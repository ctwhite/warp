;;; warp-system-monitor.el --- System-Wide Metric Collection -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a custom, Emacs Lisp-based system monitoring
;; solution. It leverages `list-system-processes` to periodically
;; collect comprehensive CPU and memory utilization metrics for both the
;; entire system and specific running processes.
;;
;; Designed to be non-intrusive and self-contained, this solution avoids
;; external system utilities, making it highly portable. It's valuable
;; for Warp workers running as Emacs processes, enabling them to query
;; their own detailed performance characteristics.
;;
;; Key Responsibilities:
;;
;; - **System-Wide Metrics**: Collects and aggregates total CPU and
;;   memory utilization across all detected processes.
;; - **Process-Specific Metrics**: Provides granular performance data
;;   for a configurable list of PIDs, defaulting to the current Emacs
;;   process.
;; - **Background Polling**: Uses a dedicated `loom-poll` instance
;;   to execute metric collection in a separate background thread.
;; - **Thread-Safe Data Access**: All reads and writes of metric data
;;   are protected by a mutex to ensure atomicity and consistency.
;; - **Data Structs**: Defines well-structured `warp:defschema` types
;;   to encapsulate collected metric data (`warp-system-metrics`,
;;   `warp-process-metrics`).
;; - **Lifecycle Management**: Implements robust startup and shutdown
;;   mechanisms, including an Emacs exit hook for proper cleanup.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-marshal)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-system-monitor-error
  "A generic error in the Warp system monitor module."
  'warp-error)

(define-error 'warp-system-monitor-uninitialized-error
  "Operation attempted on an uninitialized or stopped system monitor."
  'warp-system-monitor-error)

(define-error 'warp-system-monitor-process-not-found
  "The requested process ID could not be found by the system monitor."
  'warp-system-monitor-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Customization

(defcustom warp-system-monitor-interval 5.0
  "The interval in seconds for collecting system-wide metrics.
A lower value provides more granular data but may consume slightly
more CPU."
  :type 'number
  :group 'warp
  :safe #'numberp)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--system-monitor-global-instance nil
  "The global singleton `warp-system-monitor-instance`.
Ensures only one background polling task runs per Emacs process.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-system-metrics
    ((:constructor warp-system-metrics-create)
     (:copier nil)
     (:json-name "SystemMetrics"))
  "A structured snapshot of aggregated system-wide performance metrics.
This structure holds a snapshot of overall system resource utilization.

Fields:
- `total-cpu-utilization`: Sum of CPU utilization percentages.
- `total-memory-utilization`: Sum of memory utilization percentages.
- `cumulative-user-cpu-seconds`: Total user CPU time accumulated.
- `cumulative-system-cpu-seconds`: Total system CPU time accumulated.
- `total-cumulative-minor-page-faults`: Sum of cumulative minor faults.
- `total-cumulative-major-page-faults`: Sum of cumulative major faults.
- `total-virtual-memory-kb`: Sum of virtual memory size in KB.
- `total-resident-memory-kb`: Sum of resident set size in KB (`rss`).
- `process-count`: Total running processes detected.
- `last-collection-time`: `float-time` of last collection."
  (total-cpu-utilization 0.0 :type float :json-key "totalCpuUtil")
  (total-memory-utilization 0.0 :type float :json-key "totalMemUtil")
  (cumulative-user-cpu-seconds 0.0 :type float
                               :json-key "cumUserCpuSecs")
  (cumulative-system-cpu-seconds 0.0 :type float
                                 :json-key "cumSysCpuSecs")
  (total-cumulative-minor-page-faults 0 :type integer
                                      :json-key "totalCumMinorFaults")
  (total-cumulative-major-page-faults 0 :type integer
                                      :json-key "totalCumMajorFaults")
  (total-virtual-memory-kb 0 :type integer :json-key "totalVmKb")
  (total-resident-memory-kb 0 :type integer :json-key "totalRssKb")
  (process-count 0 :type integer :json-key "processCount")
  (last-collection-time (float-time) :type float
                        :json-key "lastCollectionTime"))

(warp:defschema warp-process-metrics
    ((:constructor warp-process-metrics-create)
     (:copier nil)
     (:json-name "ProcessMetrics"))
  "A structured snapshot of performance metrics for a single process.

Fields:
- `pid`: Process ID.
- `command-name`: Executable name (`comm`).
- `process-state`: State code (e.g., \"R\", \"S\").
- `cpu-utilization`: Instantaneous CPU usage (`pcpu`).
- `memory-utilization-mb`: Resident Set Size (RSS) in MB.
- `virtual-memory-kb`: Virtual Memory Size (VSIZE) in KB.
- `cumulative-user-cpu-seconds`: Total user CPU time (`utime`).
- `cumulative-system-cpu-seconds`: Total system CPU time (`stime`).
- `minor-page-faults`: Number of minor page faults (`minflt`).
- `major-page-faults`: Number of major page faults (`majflt`).
- `cumulative-minor-page-faults`: Total minor page faults (`cminflt`).
- `cumulative-major-page-faults`: Total major page faults (`cmajflt`).
- `elapsed-time-seconds`: Wall-clock time since start (`etime`).
- `command-line-args`: Full command line (`args`).
- `last-collection-time`: `float-time` of last collection for this PID."
  (pid 0 :type integer :json-key "pid")
  (command-name nil :type string :json-key "cmdName")
  (process-state nil :type string :json-key "state")
  (cpu-utilization 0.0 :type float :json-key "cpuUtil")
  (memory-utilization-mb 0.0 :type float :json-key "memMb")
  (virtual-memory-kb 0 :type integer :json-key "vmKb")
  (cumulative-user-cpu-seconds 0.0 :type float
                               :json-key "cumUserCpuSecs")
  (cumulative-system-cpu-seconds 0.0 :type float
                                 :json-key "cumSysCpuSecs")
  (minor-page-faults 0 :type integer :json-key "minorFaults")
  (major-page-faults 0 :type integer :json-key "majorFaults")
  (cumulative-minor-page-faults 0 :type integer
                                :json-key "cumMinorFaults")
  (cumulative-major-page-faults 0 :type integer
                                :json-key "cumMajorFaults")
  (elapsed-time-seconds 0.0 :type float :json-key "elapsedTimeSecs")
  (command-line-args nil :type string :json-key "cmdArgs")
  (last-collection-time (float-time) :type float
                        :json-key "lastCollectionTime"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-system-monitor-instance
               (:constructor %%warp-system-monitor-instance)
               (:copier nil))
  "Manages the state and background collection of system metrics.
This is the central 'engine' of the monitoring system.

Fields:
- `poll-instance`: `loom-poll` for background collection.
- `system-metrics`: Latest `warp-system-metrics` snapshot.
- `process-metrics-registry`: Hash table mapping PIDs to metrics.
- `watched-pids`: List of PIDs to actively watch.
- `lock`: `loom-lock` protecting thread-safe access to metric data.
- `status`: Current operational status (`:active` or `:stopped`)."
  (poll-instance nil :type (or null loom-poll))
  (system-metrics nil :type (or null warp-system-metrics))
  (process-metrics-registry (make-hash-table :test 'eq) :type hash-table)
  (watched-pids (list (emacs-pid)) :type list)
  (lock (loom:lock "warp-system-monitor-data") :type loom-lock)
  (status :stopped :type keyword))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-system-monitor--time-to-seconds (time-pair)
  "Convert a `(SEC . USEC)` pair to seconds.

Arguments:
- `TIME-PAIR` (cons): A cons cell of `(seconds . microseconds)`.

Returns:
- (float): The total time in seconds."
  (if (and (consp time-pair)
           (numberp (car time-pair))
           (numberp (cdr time-pair)))
      (+ (float (car time-pair)) (/ (float (cdr time-pair)) 1000000.0))
    0.0))

(defun warp-system-monitor--get-all-process-attributes ()
  "Retrieve the raw attributes for all active system processes.
This is a robust wrapper around `list-system-processes` and
`process-attributes`.

Returns:
- (list): A list of plists, each containing process attributes.

Signals:
- `warp-system-monitor-error` if listing processes fails."
  (condition-case err
      (cl-loop for pid in (list-system-processes)
               for attrs = (condition-case nil (process-attributes pid)
                             (error nil))
               when (and attrs (plist-get attrs 'pid)) ; Ensure PID
               collect attrs)
    (error (signal 'warp-system-monitor-error
                   (list (warp:error!
                          :type 'warp-system-monitor-error
                          :message "Failed to list system processes"
                          :cause err))))))

(defun warp-system-monitor--aggregate-system-metrics
    (all-proc-attrs current-time)
  "Aggregate raw process attributes into a system-wide metrics snapshot.

Arguments:
- `ALL-PROC-ATTRS` (list): A list of attribute plists.
- `CURRENT-TIME` (float): The timestamp for this collection cycle.

Returns:
- (warp-system-metrics): A new, populated system metrics object."
  (let ((sys-cpu 0.0) (sys-mem 0.0) (user-cpu 0.0) (sys-cpu-time 0.0)
        (minor-faults 0) (major-faults 0) (vm-kb 0) (rss-kb 0)
        (proc-count 0))
    (dolist (attrs all-proc-attrs)
      (cl-incf sys-cpu (or (plist-get attrs 'pcpu) 0.0))
      (cl-incf sys-mem (or (plist-get attrs 'pmem) 0.0))
      (cl-incf user-cpu
               (warp-system-monitor--time-to-seconds
                (plist-get attrs 'utime)))
      (cl-incf sys-cpu-time
               (warp-system-monitor--time-to-seconds
                (plist-get attrs 'stime)))
      (cl-incf minor-faults (or (plist-get attrs 'cminflt) 0))
      (cl-incf major-faults (or (plist-get attrs 'cmajflt) 0))
      (cl-incf vm-kb (or (plist-get attrs 'vsize) 0))
      (cl-incf rss-kb (or (plist-get attrs 'rss) 0))
      (cl-incf proc-count))
    (warp-system-metrics-create
     :total-cpu-utilization sys-cpu
     :total-memory-utilization sys-mem
     :cumulative-user-cpu-seconds user-cpu
     :cumulative-system-cpu-seconds sys-cpu-time
     :total-cumulative-minor-page-faults minor-faults
     :total-cumulative-major-page-faults major-faults
     :total-virtual-memory-kb vm-kb
     :total-resident-memory-kb rss-kb
     :process-count proc-count
     :last-collection-time current-time)))

(defun warp-system-monitor--extract-process-metrics (attrs current-time)
  "Extract and convert raw attributes into a `warp-process-metrics` object.

Arguments:
- `ATTRS` (plist): The raw attribute plist for a single process.
- `CURRENT-TIME` (float): The timestamp for this collection cycle.

Returns:
- (warp-process-metrics): A new, populated process metrics object."
  (warp-process-metrics-create
   :pid (plist-get attrs 'pid 0)
   :command-name (plist-get attrs 'comm)
   :process-state (plist-get attrs 'state)
   :cpu-utilization (plist-get attrs 'pcpu 0.0)
   :memory-utilization-mb (/ (float (plist-get attrs 'rss 0)) 1024.0)
   :virtual-memory-kb (plist-get attrs 'vsize 0)
   :cumulative-user-cpu-seconds
   (warp-system-monitor--time-to-seconds (plist-get attrs 'utime))
   :cumulative-system-cpu-seconds
   (warp-system-monitor--time-to-seconds (plist-get attrs 'stime))
   :minor-page-faults (plist-get attrs 'minflt 0)
   :major-page-faults (plist-get attrs 'majflt 0)
   :cumulative-minor-page-faults (plist-get attrs 'cminflt 0)
   :cumulative-major-page-faults (plist-get attrs 'cmajflt 0)
   :elapsed-time-seconds
   (warp-system-monitor--time-to-seconds (plist-get attrs 'etime))
   :command-line-args (plist-get attrs 'args)
   :last-collection-time current-time))

(defun warp-system-monitor--handle-internal-error
    (error-type message &key cause details context)
  "Centralized handler for internal errors within the System Monitor.

Arguments:
- `ERROR-TYPE` (symbol): A keyword categorizing the error.
- `MESSAGE` (string): A human-readable error message.
- `:CAUSE` (error): The underlying error object.
- `:DETAILS` (any): Additional relevant data.
- `:CONTEXT` (any): The operational context.

Returns:
- `nil`."
  (warp:log! :error "sys-mon"
             "Internal error [%S] (%s): %s. Details: %S, Cause: %S"
             error-type context message details cause)
  nil)

(defun warp-system-monitor--collect-metrics-task (monitor-instance)
  "The main periodic task for collecting all system and process metrics.
This is executed by `loom-poll`. It gathers metrics and atomically
updates the monitor instance's state.

Arguments:
- `MONITOR-INSTANCE` (warp-system-monitor-instance): The instance.

Returns: `nil`."
  (let ((current-time (float-time)))
    (warp:log! :trace "sys-mon" "Collecting system metrics...")
    (braid! nil
      (:bind (lambda (_)
               (condition-case err
                   (warp-system-monitor--get-all-process-attributes)
                 (error
                  (warp-system-monitor--handle-internal-error
                   'collection-failure
                   "Failed to get process attributes"
                   :cause err :context :get-all-attrs)
                  (loom:rejected! err)))))
      (:then (lambda (all-attrs)
               (let* ((new-sys-metrics
                       (warp-system-monitor--aggregate-system-metrics
                        all-attrs current-time))
                      (new-proc-reg (make-hash-table :test 'eq))
                      (watched-pids
                       (loom:with-mutex!
                           (warp-system-monitor-instance-lock monitor-instance)
                         (copy-list (warp-system-monitor-instance-watched-pids
                                     monitor-instance)))))
                 ;; Extract metrics for all specifically watched PIDs.
                 (dolist (pid watched-pids)
                   (if-let (attrs (cl-find-if
                                   (lambda (a) (= (plist-get a 'pid) pid))
                                   all-attrs))
                       (progn
                         (puthash pid
                                  (warp-system-monitor--extract-process-metrics
                                   attrs current-time)
                                  new-proc-reg)
                         (unless (plist-get attrs 'pcpu)
                           (warp:log! :warn "sys-mon"
                                      "Incomplete metrics for PID %d." pid)))
                     (warp:log! :trace "sys-mon" "PID %d not found" pid)))
                 ;; Atomically update the shared metric objects.
                 (loom:with-mutex!
                     (warp-system-monitor-instance-lock monitor-instance)
                   (setf (warp-system-monitor-instance-system-metrics
                          monitor-instance) new-sys-metrics)
                   (setf (warp-system-monitor-instance-process-metrics-registry
                          monitor-instance) new-proc-reg))
                 (warp:log! :debug "sys-mon"
                            "System metrics updated successfully."))))
      (:catch (lambda (err) (loom:rejected! err))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:system-monitor-start ()
  "Start the global system monitor if it is not already running.
This is idempotent. It initializes the singleton monitor instance and
starts its background polling thread for metric collection.

Returns:
- (warp-system-monitor-instance): The active monitor instance."
  (loom:with-mutex! (loom:lock "warp-system-monitor-init-lock")
    (unless (and warp--system-monitor-global-instance
                 (eq (warp-system-monitor-instance-status
                      warp--system-monitor-global-instance) :active))
      (warp:log! :info "sys-mon" "Starting global system monitor.")
      (setq warp--system-monitor-global-instance
            (%%warp-system-monitor-instance
             :poll-instance (loom:poll
                             :name "warp-system-monitor-poll"
                             :interval warp-system-monitor-interval)
             :system-metrics (warp-system-metrics-create)
             :status :active))
      (let ((instance warp--system-monitor-global-instance))
        (loom:poll-register-periodic-task
         (warp-system-monitor-instance-poll-instance instance)
         'collect-system-metrics-task
         (lambda () (warp-system-monitor--collect-metrics-task instance)))
        (loom:poll-start
         (warp-system-monitor-instance-poll-instance instance)))))
  warp--system-monitor-global-instance)

;;;###autoload
(defun warp:system-monitor-stop ()
  "Stop the global system monitor.
This is idempotent and safely shuts down the background polling thread.

Returns:
- `t` if the monitor was running and is now stopped, `nil` otherwise."
  (when (and warp--system-monitor-global-instance
             (eq (warp-system-monitor-instance-status
                  warp--system-monitor-global-instance) :active))
    (warp:log! :info "sys-mon" "Stopping global system monitor.")
    (let ((instance warp--system-monitor-global-instance))
      (loom:poll-shutdown
       (warp-system-monitor-instance-poll-instance instance))
      (loom:with-mutex! (warp-system-monitor-instance-lock instance)
        (setf (warp-system-monitor-instance-poll-instance instance) nil)
        (setf (warp-system-monitor-instance-status instance) :stopped)))
    (setq warp--system-monitor-global-instance nil)
    t))

;;;###autoload
(defun warp:system-monitor-get-system-metrics ()
  "Return the latest aggregated system-wide performance metrics.
This provides thread-safe access to the latest metrics snapshot.
It will start the monitor automatically if it is not already running.

Returns:
- (warp-system-metrics): The latest system metrics snapshot.

Signals:
- `warp-system-monitor-uninitialized-error` if metrics not yet
  available (briefly after startup)."
  (let ((instance (warp:system-monitor-start)))
    (loom:with-mutex! (warp-system-monitor-instance-lock instance)
      (or (warp-system-monitor-instance-system-metrics instance)
          (signal 'warp-system-monitor-uninitialized-error
                  (list (warp:error!
                         :type 'warp-system-monitor-uninitialized-error
                         :message "System metrics not yet available.")))))))

;;;###autoload
(defun warp:system-monitor-get-process-metrics (&optional pid)
  "Return the latest performance metrics for a specific process.
This provides thread-safe access to detailed metrics for a single PID.
If `PID` is omitted, it defaults to the current Emacs process ID. This
function will start the monitor automatically if it is not running.

Arguments:
- `PID` (integer, optional): The process ID to query.

Returns:
- (warp-process-metrics): The metrics for the requested PID.

Signals:
- `warp-system-monitor-process-not-found` if the PID is not found or
  not currently watched."
  (let* ((instance (warp:system-monitor-start))
         (target-pid (or pid (emacs-pid))))
    (loom:with-mutex! (warp-system-monitor-instance-lock instance)
      (or (gethash target-pid
                   (warp-system-monitor-instance-process-metrics-registry
                    instance))
          (signal 'warp-system-monitor-process-not-found
                  (list (warp:error!
                         :type 'warp-system-monitor-process-not-found
                         :message (format "Metrics for PID %d not found."
                                          target-pid)
                         :details `(:pid ,target-pid))))))))

;;;###autoload
(defun warp:system-monitor-watch-pid (pid)
  "Add a `PID` to the list of processes to be actively monitored.

Arguments:
- `PID` (integer): The process ID to add to the watch list.

Returns: `t`."
  (let ((instance (warp:system-monitor-start)))
    (loom:with-mutex! (warp-system-monitor-instance-lock instance)
      (add-to-list 'watched-pids pid
                   (warp-system-monitor-instance-watched-pids instance)))
    (warp:log! :info "sys-mon" "Added PID %d to watch list." pid)
    t))

;;;###autoload
(defun warp:system-monitor-unwatch-pid (pid)
  "Remove a `PID` from the list of actively monitored processes.

Arguments:
- `PID` (integer): The process ID to remove from the watch list.

Returns: `t`."
  (when-let ((instance warp--system-monitor-global-instance))
    (loom:with-mutex! (warp-system-monitor-instance-lock instance)
      (setf (warp-system-monitor-instance-watched-pids instance)
            (remove pid (warp-system-monitor-instance-watched-pids
                         instance))))
    (warp:log! :info "sys-mon" "Removed PID %d from watch list." pid))
  t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global Emacs Exit Hook for Cleanup

(defun warp--system-monitor-shutdown-on-exit ()
  "A cleanup function registered with `kill-emacs-hook`.
This ensures that the background polling thread of the system monitor
is always stopped gracefully when Emacs is closed.

Returns: `nil`."
  (when warp--system-monitor-global-instance
    (warp:log! :info "sys-mon" "Emacs shutdown: Cleaning up system monitor.")
    (condition-case err (warp:system-monitor-stop)
      (error (warp:log! :error "sys-mon"
                        "Error during shutdown on exit: %S" err)))))

(add-hook 'kill-emacs-hook #'warp--system-monitor-shutdown-on-exit)

(provide 'warp-system-monitor)
;;; warp-system-monitor.el ends here