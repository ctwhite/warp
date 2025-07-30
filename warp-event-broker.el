;;; warp-event-broker.el --- Dedicated Event Broker Worker for Warp -*- lexical-binding: t; -*-

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
;;   delivery guarantees for distributed events, isolating this complexity.
;; - **Automatic Backpressure**: Leverages an internal `warp-thread-pool`
;;   (which uses `warp-stream`) to automatically apply backpressure to
;;   producers if events arrive faster than they can be processed.
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig event-broker-config
  "Configuration for the Warp Event Broker worker.
This defines operational parameters for its internal thread pool, retries,
and fault tolerance, allowing performance and resilience to be tuned.

Fields:
- `max-queue-size` (integer): Max number of events in the internal task
  queue before applying backpressure.
- `processing-threads` (integer): Number of threads for fanning out events.
- `max-retry-attempts` (integer): Max retries for sending an event to a
  single failing worker.
- `retry-backoff-ms` (integer): Base backoff time in milliseconds for the
  first retry. Subsequent retries use exponential backoff.
- `circuit-breaker-threshold` (integer): Number of consecutive failures to a
  worker before its circuit breaker trips.
- `health-check-interval-ms` (integer): Interval in milliseconds for the
  broker to perform internal health checks."
  (max-queue-size          10000 :type integer :validate (> $ 0))
  (processing-threads      4     :type integer :validate (> $ 0))
  (max-retry-attempts      3     :type integer :validate (>= $ 0))
  (retry-backoff-ms        100   :type integer :validate (> $ 0))
  (circuit-breaker-threshold 5   :type integer :validate (>= $ 0))
  (health-check-interval-ms 30000 :type integer :validate (> $ 0)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------------
;;; Event Broker Specific Component Definitions
;;----------------------------------------------------------------------------

(defun warp-event-broker--get-component-definitions (broker-config base-worker)
  "Returns a list of specialized component definitions for an event broker.
These definitions override or extend the standard worker components to
implement event brokering logic.

Arguments:
- `broker-config` (event-broker-config): The event broker's configuration.
- `base-worker` (warp-worker): The worker instance that this broker is.

Returns:
- (list): A list of plists, each defining a specialized component."
  (list
   ;; Override connection manager to connect to ALL workers (peers) for event
   ;; fan-out, not just the master.
   `(:name :connection-manager
     :deps (:event-system :rpc-system)
     :factory (lambda (es rpc-system)
                (warp:connection-manager
                 :endpoints nil ;; Dynamically populated
                 :event-system es
                 :on-message-fn
                 (lambda (msg-string conn)
                   (let ((rpc-msg (warp:marshal-from-string msg-string)))
                     (pcase (warp-rpc-message-type rpc-msg)
                       (:request
                        (let ((router
                               (warp:component-system-get
                                (warp-worker-component-system base-worker)
                                :command-router)))
                          (warp:rpc-handle-request rpc-system rpc-msg conn)))
                       (:response
                        (warp:rpc-handle-response rpc-system rpc-msg)))))))
     :start (lambda (cm _) (loom:await (warp:connection-manager-connect cm)))
     :stop (lambda (cm _) (loom:await (warp:connection-manager-shutdown cm))))

   ;; Dedicated thread pool for processing event fan-out.
   `(:name :event-processing-pool
     :factory (lambda ()
                (let ((w-id (warp-worker-worker-id base-worker)))
                  (warp:thread-pool-create
                   :name ,(format "%s-event-proc-pool" w-id)
                   :pool-size (warp-event-broker-config-processing-threads
                               broker-config)
                   :max-queue-size (warp-event-broker-config-max-queue-size
                                    broker-config)
                   :overflow-policy :block)))
     :stop (lambda (pool _) (loom:await (warp:thread-pool-shutdown pool))))

   ;; The core event system for the broker.
   `(:name :event-system
     :factory (lambda ()
                (let ((w-id (warp-worker-worker-id base-worker)))
                  (warp:event-system-create
                   :id ,(format "%s-events" w-id)
                   :max-queue-size (warp-event-broker-config-max-queue-size
                                    broker-config)
                   :connected-peer-map (make-hash-table :test 'equal)))))

   ;; Specialized RPC Service to handle event propagation.
   `(:name :rpc-service
     :deps (:command-router :event-processing-pool :event-system :rpc-system)
     :priority 10 ; Start early
     :start (lambda (_rpc-svc system)
              (let ((router (warp:component-system-get system :command-router))
                    (pool (warp:component-system-get system :event-processing-pool))
                    (broker-es (warp:component-system-get system :event-system)))
                (warp:defrpc-handlers router
                  (:propagate-event .
                   (lambda (cmd ctx)
                     (warp-event-broker--handle-propagate-event
                      cmd ctx base-worker pool broker-es)))))))

   ;; Dynamically add/remove workers from the connection manager.
   `(:name :worker-discovery-service
     :deps (:event-system :connection-manager)
     :start (lambda (_ system)
              (let ((es (warp:component-system-get system :event-system))
                    (cm (warp:component-system-get system :connection-manager))
                    (b-id (warp:worker-id base-worker)))
                (warp:subscribe
                 es :worker-ready-signal-received
                 (lambda (event)
                   (braid! (warp-event-broker--handle-worker-registered
                            base-worker cm event)
                     (:catch (lambda (err)
                               (warp:log! :error b-id
                                "Failed to handle worker registered: %S" err))))))
                (warp:subscribe
                 es :worker-deregistered
                 (lambda (event)
                   (braid! (warp-event-broker--handle-worker-deregistered
                            base-worker cm event)
                     (:catch (lambda (err)
                               (warp:log! :error b-id
                                "Failed to handle worker deregistered: %S" err)))))))))

   ;; Standard worker health checks and heartbeat service.
   `(:name :health-orchestrator
     :factory (lambda ()
                (let ((w-id (warp-worker-worker-id base-worker)))
                  (warp:health-orchestrator-create :name ,(format "%s-health" w-id))))
     :start (lambda (ho _) (warp:health-orchestrator-start ho))
     :stop (lambda (ho _) (loom:await (warp:health-orchestrator-stop ho))))

   `(:name :heartbeat-service
     :deps (:connection-manager :system-monitor :health-orchestrator
            :event-system :service-registry :key-manager :rpc-system)
     :priority 60
     :factory (lambda () (make-hash-table)) ; Holder for the timer
     :start (lambda (holder system)
              (let ((interval (warp-worker-config-heartbeat-interval
                               (warp-worker-config base-worker))))
                (puthash :timer
                         (run-at-time t interval
                          (lambda () (loom:await
                                      (warp-worker--send-heartbeat
                                       base-worker system))))
                         holder)))
     :stop (lambda (holder _)
             (when-let (timer (gethash :timer holder)) (cancel-timer timer))))

   ;; Component for initial sync with master to get active workers.
   `(:name :event-broker-initial-sync-service
     :deps (:connection-manager :rpc-system :event-system)
     :priority 70 ; Run after core systems are up
     :start (lambda (_sync-svc system)
              (let* ((cm (warp:component-system-get system :connection-manager))
                     (rpc-sys (warp:component-system-get system :rpc-system))
                     (broker-id (warp:worker-id base-worker)))
                (warp:log! :info broker-id "Initiating sync for active workers...")
                (braid!
                 (warp:protocol-get-all-active-workers
                  rpc-sys (warp:connection-manager-get-connection cm)
                  broker-id "master"
                  :origin-instance-id (warp-component-system-id system))
                 (:then
                  (lambda (response)
                    (let* ((payload (warp-rpc-command-args response))
                           (workers (warp-get-all-active-workers-response-payload-active-workers
                                     payload)))
                      (warp:log! :info broker-id "Received %d active workers."
                                 (length workers))
                      (dolist (info workers)
                        (let ((w-id (plist-get info :worker-id))
                              (addr (plist-get info :inbox-address)))
                          (braid!
                           (let ((event
                                  (make-warp-event
                                   :type :worker-ready-signal-received
                                   :data `(:worker-id ,w-id
                                           :inbox-address ,addr))))
                             (warp-event-broker--handle-worker-registered
                              base-worker cm event))
                           (:catch
                            (lambda (err)
                              (warp:log! :warn broker-id
                               "Failed to add worker '%s': %S" w-id err)))))))))
                 (:catch (lambda (err)
                           (warp:log! :error broker-id
                                      "Initial worker sync failed: %S" err)))))))))

;;----------------------------------------------------------------------------
;;; Event Handling for Broker Functionality
;;----------------------------------------------------------------------------

(defun warp-event-broker--handle-propagate-event
    (command context broker-worker event-proc-pool broker-event-system)
  "Handles `:propagate-event` RPCs by submitting event to a thread pool.
This provides backpressure and concurrency for event fan-out.

Arguments:
- `command` (warp-rpc-command): The incoming RPC command.
- `context` (plist): The RPC context from the command router.
- `broker-worker` (warp-worker): The event broker worker instance.
- `event-proc-pool` (warp-thread-pool): The broker's thread pool.
- `broker-event-system` (warp-event-system): The broker's event system.

Returns:
- (loom-promise): A promise that resolves when the event is submitted."
  (let* ((event (warp-propagate-event-payload-event
                 (warp-rpc-command-args command)))
         (broker-id (warp:worker-id broker-worker)))
    (warp:log! :debug broker-id "Queuing event '%S' (ID: %s) for propagation."
               (warp-event-type event) (warp-event-id event))
    ;; Submit the fan-out task to the dedicated thread pool.
    (warp:thread-pool-submit
     event-proc-pool
     (lambda ()
       (loom:await (warp-event-broker--process-event-task
                    broker-worker event broker-event-system)))
     nil)))

(defun warp-event-broker--handle-worker-registered (broker-worker cm event)
  "Event handler to add a newly registered worker to the broadcast list.
Establishes a connection and maps the worker ID to the connection object.

Arguments:
- `broker-worker` (warp-worker): The event broker instance.
- `cm` (warp-connection-manager): The broker's connection manager.
- `event` (warp-event): The `:worker-ready-signal-received` event.

Returns:
- (loom-promise): A promise that resolves when the worker is added."
  (let* ((broker-id (warp:worker-id broker-worker))
         (data (warp-event-data event))
         (new-worker-id (plist-get data :worker-id))
         (address (plist-get data :inbox-address))
         (es (warp:component-system-get
              (warp-worker-component-system broker-worker) :event-system))
         (peer-map (warp-event-system-connected-peer-map es)))
    (braid!
     (warp:log! :info broker-id "Adding worker '%s' to broadcast list."
                new-worker-id)
     (:then (lambda (_)
              (if (and new-worker-id address
                       (not (string= new-worker-id broker-id))
                       (not (string= new-worker-id "master")))
                  (braid!
                   (warp:connection-manager-add-endpoint cm address)
                   (:then (lambda (_)
                            ;; Loop until connection becomes active.
                            (loom:loop!
                             (let ((conn (warp:connection-manager-get-connection
                                          cm address)))
                               (if conn
                                   (loom:break! conn)
                                 (loom:delay! 0.1 (loom:continue!)))))))
                   (:then (lambda (connection)
                            (loom:with-mutex! (warp-event-system-lock es)
                              (puthash new-worker-id connection peer-map))
                            (warp:log! :info broker-id "Mapped worker '%s'."
                                       new-worker-id)
                            connection))
                   (:catch (lambda (err)
                             (warp:log! :error broker-id
                                        "Failed to connect to '%s': %S"
                                        new-worker-id err)
                             (loom:rejected! err))))
                (loom:resolved! nil))))
     (:catch (lambda (err)
               (warp:log! :error broker-id
                          "Failed to handle worker registered event: %S" err)
               (loom:rejected! err))))))

(defun warp-event-broker--handle-worker-deregistered (broker-worker cm event)
  "Event handler to remove a deregistered worker from the broadcast list.

Arguments:
- `broker-worker` (warp-worker): The event broker instance.
- `cm` (warp-connection-manager): The broker's connection manager.
- `event` (warp-event): The `:worker-deregistered` event.

Returns:
- (loom-promise): A promise that resolves when the worker is removed."
  (let* ((broker-id (warp:worker-id broker-worker))
         (data (warp-event-data event))
         (old-worker-id (plist-get data :worker-id))
         (address (plist-get data :inbox-address))
         (es (warp:component-system-get
              (warp-worker-component-system broker-worker) :event-system))
         (peer-map (warp-event-system-connected-peer-map es)))
    (braid!
     (warp:log! :info broker-id "Removing worker '%s' from broadcast list."
                old-worker-id)
     (:then (lambda (_)
              (when (and old-worker-id address)
                (warp:connection-manager-remove-endpoint cm address)
                (loom:with-mutex! (warp-event-system-lock es)
                  (remhash old-worker-id peer-map))
                (warp:log! :info broker-id
                           "Removed worker '%s' from connected peers."
                           old-worker-id)
                t)))
     (:catch (lambda (err)
               (warp:log! :error broker-id "Failed to remove worker '%s': %S"
                          old-worker-id err)
               (loom:rejected! err))))))

(defun warp-event-broker--send-event-to-worker
    (broker-worker target-worker-id target-connection event)
  "Sends an event to a target worker with retry and circuit breaking.

Arguments:
- `broker-worker` (warp-worker): The event broker instance.
- `target-worker-id` (string): The logical ID of the target worker.
- `target-connection` (warp-transport-connection): The connection.
- `event` (warp-event): The `warp-event` object to send.

Returns:
- (loom-promise): Resolves on success or rejects on failure."
  (let* ((broker-id (warp:worker-id broker-worker))
         (system (warp-worker-component-system broker-worker))
         (rpc-system (warp:component-system-get system :rpc-system))
         (broker-cfg (warp-event-broker-config (warp-worker-config broker-worker)))
         (max-retries (warp-event-broker-config-max-retry-attempts broker-cfg))
         (backoff-ms (warp-event-broker-config-retry-backoff-ms broker-cfg))
         (cb-thresh (warp-event-broker-config-circuit-breaker-threshold broker-cfg))
         (cb-id (format "broker-fanout-%s" target-worker-id))
         (cb (warp:circuit-breaker-get
              cb-id
              :config-options `(:failure-threshold ,cb-thresh
                                :recovery-timeout 60.0))))
    (braid!
     (warp:circuit-breaker-execute cb
       (lambda ()
         (loom:retry
          (braid!
           (unless target-connection
             (signal (warp:error!
                      :type 'warp-errors-no-connection
                      :message (format "No active connection to %s."
                                       target-worker-id))))
           (warp:protocol-send-distributed-event
            rpc-system target-connection (warp-event-source-id event)
            target-worker-id event
            :origin-instance-id (warp-component-system-id system)))
          :retries max-retries
          :delay (lambda (n _) (/ (* backoff-ms (expt 2 (1- n))) 1000.0))
          :pred (lambda (err)
                  (or (cl-typep err 'warp-errors-no-connection)
                      (cl-typep err 'warp-rpc-error)
                      (cl-typep err 'loom-timeout-error))))))
     (:then (lambda (res)
              (warp:log! :trace broker-id "Sent event %S to %s."
                         (warp-event-id event) target-worker-id)
              res))
     (:catch (lambda (err)
               (warp:log! :warn broker-id
                          "Failed to send event %S to %s: %S"
                          (warp-event-id event) target-worker-id err)
               (loom:rejected!
                (warp:error!
                 :type 'warp-error-internal-error
                 :message (format "Delivery failed to %s" target-worker-id)
                 :cause err)))))))

(defun warp-event-broker--process-event-task (broker-worker event es)
  "Core task to re-emit an event locally and fan it out to other workers.

Arguments:
- `broker-worker` (warp-worker): The event broker instance.
- `event` (warp-event): The `warp-event` object to propagate.
- `es` (warp-event-system): The broker's local event system.

Returns:
- (loom-promise): A promise that resolves when fan-out is complete."
  (let* ((broker-id (warp:worker-id broker-worker))
         (sender-id (warp-event-source-id event))
         (peer-map (warp-event-system-connected-peer-map es)))
    (braid!
     ;; Step 1: Re-emit event locally (e.g., for metrics).
     (warp:emit-event-with-options
      es (warp-event-type event) (warp-event-data event)
      :source-id sender-id
      :correlation-id (warp-event-correlation-id event)
      :priority (warp-event-priority event)
      :metadata (warp-event-metadata event)
      :distribution-scope :local) ; Crucial to prevent loops.

     ;; Step 2: Determine fan-out targets.
     (:then (lambda (_)
              (let ((targets (cl-set-difference (hash-table-keys peer-map)
                                                (list sender-id broker-id)
                                                :test #'string=)))
                (warp:log! :debug broker-id "Fanning out event '%S' to %d peers."
                           (warp-event-type event) (length targets))
                targets)))

     ;; Step 3: Fan out to all targets in parallel.
     (:map (lambda (target-id)
             (if-let (conn (gethash target-id peer-map))
                 (warp-event-broker--send-event-to-worker
                  broker-worker target-id conn event)
               (progn
                 (warp:log! :warn broker-id "Skipping %s: No connection found."
                            target-id)
                 (loom:resolved! nil)))))
     (:all-settled)
     (:then (lambda (_results)
              (warp:log! :debug broker-id "Fan-out complete for event '%S'."
                         (warp-event-type event))
              t)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:event-broker-create (&rest options)
  "Creates and configures a dedicated Event Broker worker instance.
This factory configures a `warp-worker` with custom components for
high-performance, resilient event brokering.

Arguments:
- `&rest OPTIONS` (plist, optional): Configuration for `event-broker-config`
  and the underlying `warp-worker`.

Returns:
- (warp-worker): A new, configured but unstarted event broker worker."
  (let* ((broker-config (apply #'make-event-broker-config options))
         (worker-id (format "event-broker-%s"
                            (warp-worker--generate-worker-id)))
         (worker-options (append options (list :name "event-broker"
                                               :config broker-config
                                               :worker-id worker-id)))
         (broker-worker (apply #'warp:worker-create worker-options))
         (cs (warp-worker-component-system broker-worker)))
    ;; Override/add broker-specific component definitions.
    (setf (warp-component-system-definitions cs)
          (append (warp-event-broker--get-component-definitions
                   broker-config broker-worker)
                  (warp-component-system-definitions cs)))
    (warp:log! :info (warp:worker-id broker-worker)
               "Event Broker Worker created.")
    broker-worker))

(provide 'warp-event-broker)
;;; warp-event-broker.el ends here