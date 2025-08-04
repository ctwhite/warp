;;; warp-system-monitor.el --- System-Wide Metric Collection -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a system monitoring component that periodically
;; collects CPU and memory utilization metrics for the entire system and
;; for specific processes.
;;
;; This version has been refactored into a self-contained component,
;; removing the global singleton pattern. The monitor is now created via a
;; factory function and managed as an injectable dependency by the
;; `warp-component` system.
;;
;; ## Key Responsibilities:
;;
;; - **System-Wide Metrics**: Collects total CPU and memory utilization.
;; - **Process-Specific Metrics**: Provides granular data for watched PIDs.
;; - **Background Polling**: Uses a dedicated `loom-poll` instance
;;   to execute metric collection in a background thread.
;; - **Thread-Safe Data Access**: All metric data is protected by a mutex.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-marshal)
(require 'warp-config)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig system-monitor-config
  "Configuration for the Warp System Monitor.

Fields:
- `collection-interval` (float): The interval in seconds for collecting
  system-wide metrics.
- `initial-watched-pids` (list): A list of process IDs to actively
  monitor from the start."
  (collection-interval 5.0 :type float :validate (> $ 0.0))
  (initial-watched-pids (list (emacs-pid)) :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-system-metrics
    ((:constructor warp-system-metrics-create)
     (:copier nil)
     (:json-name "SystemMetrics"))
  "A structured snapshot of aggregated system-wide performance metrics.

Fields:
- `total-cpu-utilization` (float): Sum of all process CPU percentages.
- `total-memory-utilization` (float): Sum of all process memory
  percentages.
- `cumulative-user-cpu-seconds` (float): Total user CPU time for all
  processes.
- `cumulative-system-cpu-seconds` (float): Total system CPU time for
  all processes.
- `total-cumulative-minor-page-faults` (integer): Sum of minor page faults.
- `total-cumulative-major-page-faults` (integer): Sum of major page faults.
- `total-virtual-memory-kb` (integer): Sum of all virtual memory usage.
- `total-resident-memory-kb` (integer): Sum of all resident set size memory.
- `process-count` (integer): The total number of processes sampled.
- `last-collection-time` (float): The timestamp of this metrics snapshot."
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
- `pid` (integer): The process identifier.
- `command-name` (string): The name of the command.
- `process-state` (string): The current state (e.g., 'R' for running).
- `cpu-utilization` (float): The process's CPU usage percentage.
- `memory-utilization-mb` (float): The resident set size in megabytes.
- `virtual-memory-kb` (integer): The virtual memory size in kilobytes.
- `cumulative-user-cpu-seconds` (float): Total user CPU time.
- `cumulative-system-cpu-seconds` (float): Total system CPU time.
- `minor-page-faults` (integer): Number of minor page faults.
- `major-page-faults` (integer): Number of major page faults.
- `cumulative-minor-page-faults` (integer): Cumulative minor page faults.
- `cumulative-major-page-faults` (integer): Cumulative major page faults.
- `elapsed-time-seconds` (float): The total elapsed time since start.
- `command-line-args` (string): The full command line arguments.
- `last-collection-time` (float): The timestamp of this snapshot."
  (pid 0 :type integer :json-key "pid")
  (command-name nil :type string :json-key "cmdName")
  (process-state nil :type string :json-key "state")
  (cpu-utilization 0.0 :type float :json-key "cpuUtil")
  (memory-utilization-mb 0.0 :type float :json-key "memMb")
  (virtual-memory-kb 0 :type integer :json-key "vmKb")
  (cumulative-user-cpu-seconds 0.0 :type float :json-key "cumUserCpuSecs")
  (cumulative-system-cpu-seconds 0.0 :type float :json-key "cumSysCpuSecs")
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-system-monitor
               (:constructor %%make-system-monitor)
               (:copier nil))
  "Manages the state and background collection of system metrics.

Fields:
- `name` (string): A unique name for this monitor instance.
- `config` (system-monitor-config): The monitor's configuration.
- `poll-instance` (loom-poll): `loom-poll` for background collection.
- `system-metrics` (warp-system-metrics): Latest system metrics snapshot.
- `process-metrics-registry` (hash-table): Maps PIDs to process metrics.
- `watched-pids` (hash-table): Hash table of PIDs to actively watch.
- `lock` (loom-lock): `loom-lock` protecting thread-safe access to data.
- `status` (keyword): Current status (`:active` or `:stopped`)."
  (name nil :type string)
  (config (cl-assert nil) :type system-monitor-config)
  (poll-instance nil :type (or null t))
  (system-metrics nil :type (or null warp-system-metrics))
  (process-metrics-registry (make-hash-table :test 'eq) :type hash-table)
  (watched-pids (make-hash-table :test 'eq) :type hash-table)
  (lock (loom:lock "warp-system-monitor-data") :type t)
  (status :stopped :type keyword))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-system-monitor--time-to-seconds (time-pair)
  "Convert a `(SEC . USEC)` pair from `process-attributes` to seconds.

Arguments:
- `TIME-PAIR` (cons): A cons cell of `(SECONDS . MICROSECONDS)`.

Returns:
- (float): The total time in seconds."
  (if (and (consp time-pair)
           (numberp (car time-pair))
           (numberp (cdr time-pair)))
      (+ (float (car time-pair)) (/ (float (cdr time-pair)) 1000000.0))
    0.0))

(defun warp-system-monitor--get-all-process-attributes ()
  "Retrieve the raw attributes for all active system processes.
This function wraps `list-system-processes` and `process-attributes`
with error handling.

Arguments: None.

Returns:
- (list): A list of plists, where each plist contains the raw
  attributes for a single process.

Signals:
- `(warp-system-monitor-error)`: If listing processes fails."
  (condition-case err
      (cl-loop for pid in (list-system-processes)
               for attrs = (ignore-errors (process-attributes pid))
               when (and attrs (plist-get attrs 'pid))
               collect attrs)
    (error (signal (warp:error! :type 'warp-system-monitor-error
                                :message "Failed to list system processes"
                                :cause err)))))

(defun warp-system-monitor--aggregate-system-metrics (all-attrs time)
  "Aggregate raw process attributes into a system-wide metrics snapshot.

Arguments:
- `ALL-ATTRS` (list): A list of raw process attribute plists.
- `TIME` (float): The timestamp for this collection cycle.

Returns:
- (warp-system-metrics): A new system metrics object."
  (let ((cpu 0.0) (mem 0.0) (ucpu 0.0) (scpu 0.0) (minf 0)
        (majf 0) (vm 0) (rss 0) (count 0))
    (dolist (attrs all-attrs)
      (cl-incf cpu (or (plist-get attrs 'pcpu) 0.0))
      (cl-incf mem (or (plist-get attrs 'pmem) 0.0))
      (cl-incf ucpu (warp-system-monitor--time-to-seconds
                     (plist-get attrs 'utime)))
      (cl-incf scpu (warp-system-monitor--time-to-seconds
                     (plist-get attrs 'stime)))
      (cl-incf minf (or (plist-get attrs 'cminflt) 0))
      (cl-incf majf (or (plist-get attrs 'cmajflt) 0))
      (cl-incf vm (or (plist-get attrs 'vsize) 0))
      (cl-incf rss (or (plist-get attrs 'rss) 0))
      (cl-incf count))
    (warp-system-metrics-create
     :total-cpu-utilization cpu :total-memory-utilization mem
     :cumulative-user-cpu-seconds ucpu :cumulative-system-cpu-seconds scpu
     :total-cumulative-minor-page-faults minf
     :total-cumulative-major-page-faults majf
     :total-virtual-memory-kb vm :total-resident-memory-kb rss
     :process-count count :last-collection-time time)))

(defun warp-system-monitor--extract-process-metrics (attrs time)
  "Extract and convert raw attributes into a `warp-process-metrics` object.

Arguments:
- `ATTRS` (plist): The raw attribute plist for a single process.
- `TIME` (float): The timestamp for this collection cycle.

Returns:
- (warp-process-metrics): A new process metrics object."
  (warp-process-metrics-create
   :pid (plist-get attrs 'pid 0)
   :command-name (plist-get attrs 'comm)
   :process-state (plist-get attrs 'state)
   :cpu-utilization (plist-get attrs 'pcpu 0.0)
   :memory-utilization-mb (/ (float (plist-get attrs 'rss 0)) 1024.0)
   :virtual-memory-kb (plist-get attrs 'vsize 0)
   :cumulative-user-cpu-seconds (warp-system-monitor--time-to-seconds
                                 (plist-get attrs 'utime))
   :cumulative-system-cpu-seconds (warp-system-monitor--time-to-seconds
                                   (plist-get attrs 'stime))
   :minor-page-faults (plist-get attrs 'minflt 0)
   :major-page-faults (plist-get attrs 'majflt 0)
   :cumulative-minor-page-faults (plist-get attrs 'cminflt 0)
   :cumulative-major-page-faults (plist-get attrs 'cmajflt 0)
   :elapsed-time-seconds (warp-system-monitor--time-to-seconds
                          (plist-get attrs 'etime))
   :command-line-args (plist-get attrs 'args)
   :last-collection-time time))

(defun warp-system-monitor--collect-metrics-task (monitor)
  "The main periodic task for collecting all system and process metrics.
This function is executed by the background poller. It fetches data for
all processes, aggregates system-wide metrics, and updates the metrics
for each individually watched process.

Arguments:
- `MONITOR` (warp-system-monitor): The monitor instance.

Side Effects:
- Modifies the `system-metrics` and `process-metrics-registry` fields
  of the `MONITOR` struct.

Returns:
- (loom-promise): A promise that resolves when the collection cycle is
  complete."
  (let* ((monitor-lock (warp-system-monitor-lock monitor))
         (watched-pids (loom:with-mutex! monitor-lock
                         (hash-table-keys
                          (warp-system-monitor-watched-pids monitor)))))
    (braid! (warp-system-monitor--get-all-process-attributes)
      (:then (lambda (all-attrs)
               (let* ((current-time (float-time))
                      (new-sys-metrics
                       (warp-system-monitor--aggregate-system-metrics
                        all-attrs current-time))
                      (new-proc-reg (make-hash-table :test 'eq)))
                 (dolist (pid watched-pids)
                   (when-let (attrs (cl-find-if (lambda (a)
                                                 (= (plist-get a 'pid) pid))
                                               all-attrs))
                     (puthash pid
                              (warp-system-monitor--extract-process-metrics
                               attrs current-time)
                              new-proc-reg)))
                 (loom:with-mutex! monitor-lock
                   (setf (warp-system-monitor-system-metrics monitor)
                         new-sys-metrics)
                   (setf (warp-system-monitor-process-metrics-registry
                          monitor)
                         new-proc-reg))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:system-monitor-create (&key name config-options)
  "Create and start a new system monitor component.
This function is the factory for the component. It initializes the monitor
instance and starts its background polling thread for metric collection.

Arguments:
- `:name` (string): A unique name for this monitor instance.
- `:config-options` (plist, optional): A property list of configuration
  options for the `system-monitor-config` struct.

Side Effects:
- Starts a background `loom-poll` instance to collect metrics periodically.

Returns:
- (warp-system-monitor): The new, active monitor instance."
  (let* ((config (apply #'make-system-monitor-config config-options))
         (monitor-name (or name "default-system-monitor"))
         (monitor (%%make-system-monitor
                   :name monitor-name
                   :config config
                   :poll-instance (loom:poll :name (format "%s-poll"
                                                           monitor-name))
                   :system-metrics (warp-system-metrics-create)
                   :status :active)))
    ;; Populate initial watched PIDs from config.
    (dolist (pid (system-monitor-config-initial-watched-pids config))
      (puthash pid t (warp-system-monitor-watched-pids monitor)))
    ;; Register and start the periodic collection task.
    (loom:poll-register-periodic-task
     (warp-system-monitor-poll-instance monitor)
     'collect-metrics
     (lambda ()
       (loom:await ; Await collection task
        (warp-system-monitor--collect-metrics-task monitor)))
     :interval (system-monitor-config-collection-interval config)
     :immediate t)
    (loom:poll-start (warp-system-monitor-poll-instance monitor))
    (warp:log! :info (warp-system-monitor-name monitor)
               "System monitor started.")
    monitor))

;;;###autoload
(defun warp:system-monitor-stop (monitor)
  "Stop the system monitor.
This function is intended to be used as a `:stop` or `:destroy` hook. It
is idempotent and safely shuts down the background polling thread.

Arguments:
- `MONITOR` (warp-system-monitor): The monitor instance to stop.

Side Effects:
- Shuts down the background `loom-poll` instance.
- Sets the monitor's status to `:stopped`.

Returns:
- (loom-promise): A promise that resolves to `t` when stopped."
  (when (eq (warp-system-monitor-status monitor) :active)
    (warp:log! :info (warp-system-monitor-name monitor)
               "Stopping system monitor.")
    (braid! (loom:poll-shutdown (warp-system-monitor-poll-instance monitor))
      (:then (lambda (_)
               (loom:with-mutex! (warp-system-monitor-lock monitor)
                 (setf (warp-system-monitor-poll-instance monitor) nil)
                 (setf (warp-system-monitor-status monitor) :stopped))
               t)))))

;;;###autoload
(defun warp:system-monitor-get-system-metrics (monitor)
  "Return the latest aggregated system-wide performance metrics.

Arguments:
- `MONITOR` (warp-system-monitor): The active monitor instance.

Returns:
- (warp-system-metrics): The latest system metrics snapshot.

Signals:
- `(error)`: If the monitor is not active.
- `(warp-system-monitor-uninitialized-error)`: If metrics have not yet
  been collected."
  (unless (eq (warp-system-monitor-status monitor) :active)
    (error "Monitor is not active."))
  (loom:with-mutex! (warp-system-monitor-lock monitor)
    (or (warp-system-monitor-system-metrics monitor)
        (signal 'warp-system-monitor-uninitialized-error
                "System metrics not yet available."))))

;;;###autoload
(defun warp:system-monitor-get-process-metrics (monitor &optional pid)
  "Return the latest performance metrics for a specific process.

Arguments:
- `MONITOR` (warp-system-monitor): The active monitor instance.
- `PID` (integer, optional): The process ID to query. Defaults to the
  current Emacs process.

Returns:
- (warp-process-metrics): The metrics for the requested PID.

Signals:
- `(error)`: If the monitor is not active.
- `(warp-system-monitor-process-not-found)`: If the PID is not found."
  (unless (eq (warp-system-monitor-status monitor) :active)
    (error "Monitor is not active."))
  (let ((target-pid (or pid (emacs-pid))))
    (loom:with-mutex! (warp-system-monitor-lock monitor)
      (or (gethash target-pid
                   (warp-system-monitor-process-metrics-registry monitor))
          (signal 'warp-system-monitor-process-not-found
                  (format "Metrics for PID %d not found." target-pid))))))

;;;###autoload
(defun warp:system-monitor-watch-pid (monitor pid)
  "Add a `PID` to the list of processes to be actively monitored.

Arguments:
- `MONITOR` (warp-system-monitor): The monitor instance.
- `PID` (integer): The process ID to start watching.

Side Effects:
- Modifies the `watched-pids` hash table in the `MONITOR`.

Returns: `t`."
  (loom:with-mutex! (warp-system-monitor-lock monitor)
    (puthash pid t (warp-system-monitor-watched-pids monitor)))
  t)

;;;###autoload
(defun warp:system-monitor-unwatch-pid (monitor pid)
  "Remove a `PID` from the list of actively monitored processes.

Arguments:
- `MONITOR` (warp-system-monitor): The monitor instance.
- `PID` (integer): The process ID to stop watching.

Side Effects:
- Modifies the `watched-pids` hash table in the `MONITOR`.

Returns: `t`."
  (loom:with-mutex! (warp-system-monitor-lock monitor)
    (remhash pid (warp-system-monitor-watched-pids monitor)))
  t)

(provide 'warp-system-monitor)
;;; warp-system-monitor.el ends here ;;;