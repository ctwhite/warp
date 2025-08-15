;;; warp-security.el --- Pluggable Security Auditing and Monitoring -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a comprehensive security layer focused on auditing,
;; compliance, and real-time threat detection. It is designed to complement
;; the preventative measures in `warp-security-engine.el` by providing
;; robust detection and response capabilities.
;;
;; ## The "Why": The Need for Detection and Auditing
;;
;; While prevention (sandboxing, firewalls) is the first line of defense, it's
;; not sufficient on its own. A mature security posture requires the ability
;; to answer the questions: "What is happening on my system right now?" and
;; "What happened in the past?"
;;
;; This module provides the tools for this by focusing on:
;; - **Detection**: Identifying suspicious or malicious activity that may
;;   indicate an active threat or a system misconfiguration.
;; - **Auditing**: Creating a permanent, immutable record of critical
;;   system events. This is essential for meeting compliance requirements
;;   (like SOC 2 or HIPAA) and for performing forensic analysis after a
;;   security incident.
;;
;; ## The "How": An Extensible Rule Engine and Audit Trail
;;
;; 1.  **The Strategy Pattern for Rules**: Security threats are diverse, so
;;     the system must be extensible. This module uses the Strategy
;;     Pattern: each type of threat detection (e.g., checking a network
;;     whitelist, looking for a suspicious command pattern) is a separate
;;     "rule strategy". The `warp:defsecurity-rule` macro allows new rules
;;     to be plugged into the system at any time without changing core code.
;;
;; 2.  **The Runtime Security Monitor**: This component acts as a "security
;;     guard" that periodically "patrols" the system. On each patrol, it
;;     gathers recent activity logs (telemetry) and shows them to each
;;     registered security rule. If a rule spots something suspicious, it
;;     raises an alarm by emitting a `:security-finding-detected` event.
;;
;; 3.  **The Audit Trail**: This component acts as a "secure flight recorder".
;;     It subscribes to a configured list of critical system events (e.g.,
;;     `:user-logged-in`, `:firewall-rule-changed`) and writes every
;;     occurrence to a durable, append-only log via the `state-manager`.
;;     This provides a permanent record for compliance and forensics.

;;; Code:

(require 'cl-lib)
(require 'loom)

(require 'warp-component)
(require 'warp-plugin)
(require 'warp-event)
(require 'warp-log)
(require 'warp-state-manager)
(require 'warp-telemetry)
(require 'warp-registry)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-security-error
  "A generic error for security operations."
  'warp-error)

(define-error 'warp-security-rule-evaluation-error
  "An error occurred during the evaluation of a security rule."
  'warp-security-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--security-rule-registry
  (warp:registry-create :name "security-rules" :event-system nil)
  "A central registry for all discoverable security rules.
`warp:defsecurity-rule` populates this registry, and the
`runtime-security-monitor` consumes it.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct network-whitelist-rule
  "A concrete rule strategy that checks for unauthorized network connections.
This is used to enforce network policies by ensuring that the runtime
only communicates with an approved set of hosts.

Fields:
- `name` (keyword): The unique identifier for this rule instance.
- `severity` (keyword): The severity of a violation, e.g., `:warning` or `:critical`.
- `whitelist` (list): A list of strings, where each string is an allowed
  destination hostname or IP address."
  (name nil :type keyword)
  (severity :warning :type keyword)
  (whitelist '() :type list))

(cl-defstruct process-pattern-rule
  "A concrete rule strategy that scans executed commands for malicious patterns.
This is useful for detecting common attack vectors like privilege escalation
or the use of known malicious tools.

Fields:
- `name` (keyword): The unique identifier for this rule instance.
- `severity` (keyword): The severity of a violation.
- `pattern` (string): The regular expression to match against the full
  command line of executed processes."
  (name nil :type keyword)
  (severity :warning :type keyword)
  (pattern "" :type string))

(cl-defstruct statistical-anomaly-rule
  "A concrete rule strategy that checks for statistical deviations from a baseline.
This rule is used for anomaly detection, identifying when a system metric
(like CPU or memory usage) behaves unusually compared to its historical norm.

Fields:
- `name` (keyword): The unique identifier for this rule instance.
- `severity` (keyword): The severity of a violation.
- `metric` (string): The name of the telemetry metric to analyze (e.g., \"cpu.utilization\").
- `z-score-threshold` (float): The number of standard deviations from
  the historical mean that constitutes an anomaly. A common value is 3.0."
  (name nil :type keyword)
  (severity :warning :type keyword)
  (metric "" :type string)
  (z-score-threshold 3.0 :type float))

(cl-defstruct custom-security-rule
  "A generic rule strategy that encapsulates its evaluation logic in a lambda.
This provides maximum flexibility for defining complex or highly specific rules
that don't fit the other predefined strategy types.

Fields:
- `name` (keyword): The unique identifier for this rule instance.
- `severity` (keyword): The severity of a violation.
- `logic-fn` (function): A lambda that accepts one argument (the telemetry data
  plist) and returns a 'finding' plist if triggered, otherwise nil."
  (name nil :type keyword)
  (severity :warning :type keyword)
  (logic-fn nil :type (or null function)))

(cl-defstruct (warp-security-audit-trail (:constructor %%make-security-audit-trail))
  "The component that creates a durable, append-only audit trail.
It acts as a secure flight recorder by subscribing to critical system
events and persisting them to a durable state backend, providing a
permanent record for compliance and forensics.

Fields:
- `event-system` (t): The system event bus, used to subscribe to auditable events.
- `state-manager` (t): The durable state store for persisting audit logs.
- `audit-events` (list): A list of event keywords (e.g., `:user-login`) to be captured.
- `compliance-frameworks` (list): A list of compliance standards this trail helps
  satisfy (e.g., '(:soc2 :hipaa)).
- `subscription-ids` (list): A list of IDs for the active event subscriptions,
  used for cleanup on shutdown."
  (event-system nil :type t)
  (state-manager nil :type t)
  (audit-events nil :type list)
  (compliance-frameworks nil :type list)
  (subscription-ids nil :type list))

(cl-defstruct (warp-runtime-security-monitor (:constructor %%make-runtime-security-monitor))
  "The component that periodically scans for security violations.
This component is the engine for real-time threat detection. It runs on a timer,
fetches recent system activity from the telemetry pipeline, and evaluates all
registered security rules against that data.

Fields:
- `telemetry-pipeline` (t): The service for querying recent system activity data.
- `event-system` (t): The system event bus, used for emitting security finding events.
- `poller` (loom-poller): The background task scheduler used to run the periodic scan."
  (telemetry-pipeline nil :type t)
  (event-system nil :type t)
  (poller (loom:poll :name "runtime-security-monitor") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Security Rule Strategy Pattern

(cl-defgeneric warp:security-rule-evaluate (rule telemetry-data)
  "Evaluate a security `RULE` against a snapshot of `TELEMETRY-DATA`.
This is the generic function for all security rule strategies, forming the
core contract for the Strategy Pattern. Each concrete rule type must implement
a method for this function.

Arguments:
- `RULE` (t): The concrete rule strategy instance to evaluate.
- `TELEMETRY-DATA` (plist): A property list containing recent system
  activity, such as network connections and executed commands.

Returns:
- (plist or nil): A 'finding' plist containing details of the violation
  if the rule is triggered, otherwise returns nil."
  (:documentation "Evaluate a security RULE against TELEMETRY-DATA."))

(cl-defmethod warp:security-rule-evaluate ((rule network-whitelist-rule) data)
  "Find the first connection event where the destination host is not in
the rule's whitelist."
  (cl-find-if (lambda (conn)
                (not (member (plist-get conn :destination-host)
                             (network-whitelist-rule-whitelist rule))))
              (plist-get data :connections)))

(cl-defmethod warp:security-rule-evaluate ((rule process-pattern-rule) data)
  "Find the first shell command event that matches the rule's regexp."
  (cl-find-if (lambda (cmd)
                (string-match-p (process-pattern-rule-pattern rule)
                                (plist-get cmd :command-line)))
              (plist-get data :commands)))

(cl-defmethod warp:security-rule-evaluate ((rule statistical-anomaly-rule) data)
  "Check if a metric's current value is a statistical outlier."
  (let* ((metric-name (statistical-anomaly-rule-metric rule))
         (metrics-with-baseline (plist-get data :metrics-with-baseline)))
    (when-let (metrics (gethash metric-name metrics-with-baseline))
      (let* ((current (plist-get metrics :value))
             (mean (plist-get metrics :mean))
             (std-dev (plist-get metrics :std-dev)))
        ;; Ensure std-dev is non-zero to avoid division errors.
        (when (and current mean std-dev (> std-dev 1e-6))
          (let ((z-score (abs (/ (- current mean) std-dev))))
            (when (> z-score (statistical-anomaly-rule-z-score-threshold
                              rule))
              `(:details "Metric deviates from baseline" :metric ,metric-name
                :value ,current :mean ,mean :z-score ,z-score))))))))

(cl-defmethod warp:security-rule-evaluate ((rule custom-security-rule) data)
  "Evaluate a custom rule by invoking its stored logic function."
  (funcall (custom-security-rule-logic-fn rule) data))

;;;###autoload
(defmacro warp:defsecurity-rule (name &key type severity spec evaluate)
  "Define and register a new security monitoring rule.
This macro is the primary API for extending the runtime security monitor.
It supports two modes of definition:
1.  Type-based: Uses a predefined strategy (e.g., `:network-whitelist`)
    and configures it with the `:spec` property list.
2.  Logic-based: Uses the `:evaluate` keyword to provide a custom
    lambda function for maximum flexibility.

Arguments:
- `NAME` (keyword): A unique, namespaced name for the rule.
- `:type` (keyword, optional): The predefined strategy type, e.g.,
  `:network-whitelist`, `:process-pattern`, `:statistical-anomaly`.
- `:severity` (keyword): The severity of a finding, e.g., `:warning`, `:critical`.
- `:spec` (plist, optional): A property list of arguments for a type-based rule.
  (e.g., `'(:whitelist (\"host1\"))`).
- `:evaluate` (form, optional): A lambda of the form `(lambda (telemetry-data) ...)`
  for a custom rule.

Returns: The `NAME` of the rule."
  `(if ,evaluate
       ;; Branch 1: Logic-based rule. Create a `custom-security-rule`
       ;; instance with the provided inline lambda.
       (warp:registry-add warp--security-rule-registry ',name
                          (make-custom-security-rule
                           :name ',name :severity ',severity
                           :logic-fn ,evaluate)
                          :overwrite-p t)
     ;; Branch 2: Type-based rule. Instantiate a predefined strategy struct.
     (let ((rule-struct-name
            (cl-case ,type
              (:network-whitelist 'network-whitelist-rule)
              (:process-pattern 'process-pattern-rule)
              (:statistical-anomaly 'statistical-anomaly-rule)))
           (constructor-args `(:name ',name :severity ',',severity)))
       ;; Assemble the constructor arguments based on the rule type's spec.
       (cl-case ,type
         (:network-whitelist (setq constructor-args
                                   (append `(:whitelist
                                             ,(plist-get spec :whitelist))
                                           constructor-args)))
         (:process-pattern (setq constructor-args
                                 (append `(:pattern
                                           ,(plist-get spec :pattern))
                                         constructor-args)))
         (:statistical-anomaly (setq constructor-args
                                     (append `(:metric
                                               ,(plist-get spec :metric)
                                               :z-score-threshold
                                               ,(plist-get spec :threshold))
                                             constructor-args))))
       ;; Dynamically call the correct `make-*` constructor and register the rule.
       (warp:registry-add warp--security-rule-registry ',name
                          (apply #',(intern (format "make-%s"
                                                    rule-struct-name))
                                 constructor-args)
                          :overwrite-p t))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Component Logic

(defun warp-runtime-security--run-scan (monitor)
  "Run a single security scan cycle.
This fetches recent telemetry, evaluates all registered security rules
against it, and emits an event for any violations (findings).

Arguments:
- `MONITOR` (warp-runtime-security-monitor): The monitor instance.

Side Effects: Emits a `:security-finding-detected` event for each violation."
  (let* ((pipeline (warp-runtime-security-monitor-telemetry-pipeline monitor))
         (event-system (warp-runtime-security-monitor-event-system monitor))
         (rules (warp:registry-list-values warp--security-rule-registry)))
    (warp:log! :debug "security-monitor" "Running scan with %d rules."
               (length rules))
    ;; Step 1: Fetch a summary of recent activity from the telemetry pipeline.
    (let ((telemetry-data (loom:await
                           (warp:telemetry-pipeline-get-activity-summary
                            pipeline "1m"))))
      ;; Step 2: Evaluate each registered rule against the telemetry data.
      (dolist (rule rules)
        (condition-case err
            (when-let (finding (warp:security-rule-evaluate rule
                                                            telemetry-data))
              (let ((rule-plist (cl-struct-to-plist rule)))
                (warp:log! :warn "security-monitor" "FINDING: Rule '%s' triggered."
                           (plist-get rule-plist :name))
                ;; Step 3: If a rule is violated, emit a standardized event.
                (warp:emit-event event-system :security-finding-detected
                                 `(:rule ,(plist-get rule-plist :name)
                                   :severity ,(plist-get rule-plist
                                                         :severity)
                                   :details ,finding
                                   :timestamp ,(float-time)))))
          ;; Gracefully handle errors during rule evaluation to prevent
          ;; one bad rule from crashing the entire scan.
          (error
           (warp:log! :error "security-monitor" "Error evaluating rule '%s': %S"
                      (plist-get (cl-struct-to-plist rule) :name) err)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Plugin Definition and Components

(warp:defplugin :security
  "Provides comprehensive security, audit trails, and compliance monitoring."
  :version "2.2.0"
  :dependencies '(cluster-orchestrator telemetry event-system state-manager)
  :profiles
  `((:cluster-worker
     :components
     `(security-audit-trail runtime-security-monitor)))

  :components
  `((security-audit-trail
     :doc "Creates a durable audit trail by subscribing to critical events
and persisting them to the state manager. This provides a permanent,
append-only log for compliance and forensics."
     :requires '(event-system state-manager)
     :factory (lambda (es sm)
                (%%make-security-audit-trail
                 :event-system es :state-manager sm
                 ;; In a real system, this list would be dynamically configurable.
                 :audit-events '(:user-login :firewall-rule-change
                                 :critical-error)))
     :start (lambda (self _ctx)
              "Start the audit trail by subscribing to configured events."
              (let ((es (warp-security-audit-trail-event-system self))
                    (sm (warp-security-audit-trail-state-manager self))
                    (sub-ids '()))
                (dolist (evt-type (warp-security-audit-trail-audit-events
                                   self))
                  (push (warp:subscribe
                         es evt-type
                         (lambda (event)
                           ;; Create a unique, time-based key for the audit log entry.
                           (let ((log-key (format "audit-%s-%s" (float-time)
                                                  (warp-event-id event))))
                             (warp:state-manager-update
                              sm `(:security :audit ,log-key) event)))
                         :priority :low)
                        sub-ids))
                (setf (warp-security-audit-trail-subscription-ids self)
                      sub-ids)))
     :stop (lambda (self _ctx)
             "Stop the audit trail by unsubscribing from all events."
             (let ((es (warp-security-audit-trail-event-system self)))
               (dolist (sub-id (warp-security-audit-trail-subscription-ids
                                self))
                 (warp:unsubscribe es sub-id)))))

    (runtime-security-monitor
     :doc "Periodically scans for security violations by running all
registered rules against recent telemetry data. This component acts as the
runtime detection engine."
     :requires '(telemetry-pipeline event-system)
     :factory (lambda (tp es)
                (%%make-runtime-security-monitor :telemetry-pipeline tp
                                                 :event-system es))
     :start (lambda (self _ctx)
              "Start the monitor's periodic security scan."
              (let ((poller (warp-runtime-security-monitor-poller self)))
                (loom:poll-register-periodic-task
                 poller 'security-scan
                 (lambda () (warp-runtime-security--run-scan self))
                 :interval 60.0) ; Run scan every 60 seconds.
                (loom:poll-start poller)))
     :stop (lambda (self _ctx)
             "Stop the monitor's periodic scan."
             (loom:poll-shutdown (warp-runtime-security-monitor-poller
                                  self))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Register Default Security Rules

;; These are registered when the module is loaded.
;; Users or other plugins can add more rules using `warp:defsecurity-rule`.

(warp:defsecurity-rule :unexpected-network-connections
  :type :network-whitelist
  :severity :critical
  :spec '(:whitelist ("127.0.0.1" "warp-registry.local")))

(warp:defsecurity-rule :privilege-escalation-attempt
  :type :process-pattern
  :severity :critical
  :spec '(:pattern "sudo|su -"))

(warp:defsecurity-rule :unusual-resource-consumption
  :type :statistical-anomaly
  :severity :warning
  :spec '(:metric "cpu.utilization" :threshold 3.0))

(provide 'warp-security)
;;; warp-security.el ends here