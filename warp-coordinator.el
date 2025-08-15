;;; warp-coordinator.el --- Distributed Coordinator Service -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a distributed coordination service for the Warp
;; framework, offering essential primitives for building resilient,
;; high-availability systems, including leader election and distributed
;; locks.
;;
;; ## The "Why": The Need for Distributed Consensus
;;
;; In a distributed system, how do a group of equal nodes agree on which
;; one should be in charge of a critical task? How can you ensure that a
;; shared resource is only accessed by one process at a time across
;; multiple machines? These are fundamental problems of distributed
;; consensus.
;;
;; This module solves these problems by providing:
;; - **Leader Election**: An automatic, fault-tolerant process for one node
;;   in a cluster to become the "leader," which can then act as a central
;;   authority for specific tasks.
;; - **Distributed Locking**: A mechanism for processes across the cluster
;;   to acquire and release exclusive locks, coordinated by the current
;;   leader.
;;
;; ## The "How": A Raft-Inspired, Event-Sourced Protocol
;;
;; The coordinator implements a simplified, heartbeat-based protocol inspired
;; by Raft to achieve consensus.
;;
;; 1.  **Leader Election**: Nodes start as **Followers**. If a follower doesn't
;;     hear from a leader for a random "election timeout," it transitions to
;;     a **Candidate**. As a candidate, it increments the global `term`, votes
;;     for itself, and requests votes from its peers. If it receives votes
;;     from a majority of the cluster (a quorum), it becomes the **Leader**.
;;
;; 2.  **Heartbeats**: The leader maintains its authority by sending periodic
;;     "heartbeat" messages to all followers. When a follower receives a
;;     heartbeat from the legitimate leader, it resets its election timer,
;;     preventing it from starting a new election.
;;
;; 3.  **Event Sourcing for Consensus State**: The core state of the protocol
;;     (`role`, `currentTerm`, `votedFor`) is managed by a `warp:defaggregate`.
;;     This is a critical design choice. Every state change is an immutable,
;;     atomic event that is durably stored. This provides the strong
;;     consistency and auditability guarantees required for a correct
;;     consensus algorithm.
;;
;; 4.  **Durability**: The system relies on the `warp-state-manager` for
;;     persistence. For the consensus state to survive a full cluster
;;     restart, the state manager **must** be configured with a durable
;;     backend (like Redis).

;;; Code:

(require 'cl-lib)
(require 'subr-x)
(require 'loom)
(require 'braid)
(require 's)

(require 'warp-log)
(require 'warp-error)
(require 'warp-state-manager)
(require 'warp-rpc)
(require 'warp-protocol)
(require 'warp-event)
(require 'warp-command-router)
(require 'warp-dialer)
(require 'warp-config)
(require 'warp-component)
(require 'warp-service)
(require 'warp-plugin)
(require 'warp-patterns)
(require 'warp-aggregate)

;; Forward declaration for the internal protocol client
(cl-deftype coordinator-protocol-client () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-coordinator-error
  "A generic error for `warp-coordinator` operations."
  'warp-error)

(define-error 'warp-coordinator-not-leader
  "Operation requires leader role, but this node is a follower."
  'warp-coordinator-error)

(define-error 'warp-coordinator-lock-failed
  "Failed to acquire or release a distributed lock."
  'warp-coordinator-error)

(define-error 'warp-coordinator-timeout
  "A coordinator operation timed out."
  'warp-coordinator-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig (coordinator-config (:extends '(warp-worker-config)))
  "Configuration for a `warp-coordinator-instance`.
This config inherits from `warp-worker-config` and adds tunables
specific to the Raft-like consensus protocol.

Fields:
- `election-timeout-min` (float): Minimum random election timeout in ms.
  The timeout is randomized to prevent split-vote scenarios.
- `election-timeout-max` (float): Maximum random election timeout in ms.
- `heartbeat-interval` (float): Interval in ms at which a leader sends
  heartbeats to followers.
- `lock-lease-time` (float): Default duration in seconds that a
  distributed lock is held before it can expire.
- `rpc-timeout` (float): Timeout in seconds for internal peer RPCs.
- `listen-address` (string): Network address this node listens on.
- `cluster-members` (list): A list of network addresses for all other
  coordinator nodes in the cluster."
  (election-timeout-min 150.0 :type float)
  (election-timeout-max 300.0 :type float)
  (heartbeat-interval 50.0 :type float)
  (lock-lease-time 300.0 :type float)
  (rpc-timeout 5.0 :type float)
  (listen-address nil :type (or null string))
  (cluster-members nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-coordinator-engine (:constructor %%make-coordinator-engine))
  "The internal engine that drives the consensus protocol.
This struct encapsulates the runtime state of a coordinator, including
its dependencies, timers, and a reference to its state aggregate.

Fields:
- `id` (string): The unique identifier for this coordinator instance.
- `config` (coordinator-config): The configuration for this instance.
- `state-aggregate` (warp-aggregate-instance): The event-sourced
  aggregate holding the durable consensus state.
- `dialer-service` (t): The dialer for establishing peer connections.
- `protocol-client` (t): The RPC client for the internal protocol.
- `event-system` (t): The event bus for emitting leadership changes.
- `election-timer` (timer): Timer used by followers/candidates to
  trigger elections.
- `heartbeat-timer` (timer): Timer used only by the leader to send
  heartbeats."
  (id (cl-assert nil) :type string)
  (config (cl-assert nil) :type coordinator-config)
  (state-aggregate (cl-assert nil) :type (or null t))
  (dialer-service (cl-assert nil) :type (or null t))
  (protocol-client nil :type (or null t))
  (event-system (cl-assert nil) :type (or null t))
  (election-timer nil :type (or null timer))
  (heartbeat-timer nil :type (or null timer)))
  
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; State Aggregate Definition

(warp:defschema coordinator-state
  "The durable, event-sourced state of a single coordinator node."
  (id nil :type string)
  (role :follower :type keyword)
  (current-term 0 :type integer)
  (voted-for nil :type (or null string))
  (leader-id nil :type (or null string)))

(warp:defaggregate coordinator-state
  "The event-sourced aggregate for a coordinator's consensus state."
  :state-schema 'coordinator-state

  ;; --- Commands (Intent to change state) ---

  (:command :initialize (state id)
    "Initialize the state for a new coordinator instance."
    (produce-event :initialized `(:id ,id)))

  (:command :start-election (state)
    "Begin a new leader election. This increments the term, transitions
the role to candidate, and makes the node vote for itself."
    (let ((new-term (1+ (coordinator-state-current-term state))))
      (produce-event :election-started `(:new-term ,new-term
                                         :candidate-id ,(coordinator-state-id
                                                         state)))))

  (:command :receive-vote-request (state candidate-id term)
    "Process a vote request from a peer, following Raft rules."
    (let ((current-term (coordinator-state-current-term state))
          (voted-for (coordinator-state-voted-for state))
          (events '()))
      ;; Rule: If request term is higher, update our term and become follower.
      (when (> term current-term)
        (setq current-term term)
        (setq voted-for nil)
        (push `(:term-updated . (:new-term ,term)) events))
      ;; Rule: Grant vote if term is current and we haven't voted for anyone else.
      (if (and (= term current-term) (null voted-for))
          (push `(:voted-for-candidate . (:candidate-id ,candidate-id)) events)
        (push '(:vote-denied . nil) events))
      events))

  (:command :become-leader (state)
    "Transition this node to the leader role after winning an election."
    (produce-event :became-leader `(:leader-id ,(coordinator-state-id state))))

  ;; --- Events (Facts that have occurred) ---

  (:event :initialized (state data)
    "Apply the initial state."
    (let ((new-state (make-coordinator-state)))
      (setf (coordinator-state-id new-state) (plist-get data :id))
      new-state))

  (:event :election-started (state data)
    "Apply state changes for starting an election."
    (let ((new-state (copy-coordinator-state state)))
      (setf (coordinator-state-role new-state) :candidate)
      (setf (coordinator-state-current-term new-state) (plist-get data
                                                                  :new-term))
      (setf (coordinator-state-voted-for new-state) (plist-get data
                                                               :candidate-id))
      new-state))

  (:event :term-updated (state data)
    "Apply a term update, reverting to follower state."
    (let ((new-state (copy-coordinator-state state)))
      (setf (coordinator-state-role new-state) :follower)
      (setf (coordinator-state-current-term new-state) (plist-get data
                                                                  :new-term))
      (setf (coordinator-state-voted-for new-state) nil)
      new-state))

  (:event :voted-for-candidate (state data)
    "Apply the state change for casting a vote for a candidate."
    (let ((new-state (copy-coordinator-state state)))
      (setf (coordinator-state-voted-for new-state) (plist-get data
                                                               :candidate-id))
      new-state))

  (:event :became-leader (state data)
    "Apply the state changes for becoming the leader."
    (let ((new-state (copy-coordinator-state state)))
      (setf (coordinator-state-role new-state) :leader)
      (setf (coordinator-state-leader-id new-state) (plist-get data
                                                               :leader-id))
      new-state)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;---------------------------------------------------------------------
;;; Timers and State Transitions
;;;---------------------------------------------------------------------

(defun warp-coordinator--reset-election-timer (engine)
  "Reset the election timer with a random timeout.
This is called by followers. If no heartbeat is received from a leader,
this timer will expire and trigger a new election. The randomized
timeout is crucial for preventing split-vote scenarios.

Arguments:
- `ENGINE` (warp-coordinator-engine): The coordinator engine instance.

Side Effects:
- Cancels any existing election timer and schedules a new one."
  (when (warp-coordinator-engine-election-timer engine)
    (cancel-timer (warp-coordinator-engine-election-timer engine)))
  (let* ((config (warp-coordinator-engine-config engine))
         (min-ms (coordinator-config-election-timeout-min config))
         (max-ms (coordinator-config-election-timeout-max config))
         (timeout-s (/ (float (+ min-ms (random (- max-ms min-ms))))
                       1000.0)))
    (setf (warp-coordinator-engine-election-timer engine)
          (run-at-time timeout-s nil #'warp-coordinator--start-election
                       engine))
    (warp:log! :debug (warp-coordinator-engine-id engine)
               "Election timer reset to %.3fs." timeout-s)))

(defun warp-coordinator--start-timers (engine)
  "Start timers for a coordinator based on its role.
A follower starts an election timer; a leader starts a heartbeat timer.

Arguments:
- `ENGINE` (warp-coordinator-engine): The instance to manage timers for.

Side Effects:
- Schedules a timer for either elections or heartbeats."
  (let ((role (coordinator-state-role
               (warp:aggregate-get-state
                (warp-coordinator-engine-state-aggregate engine)))))
    (pcase role
      (:follower (warp-coordinator--reset-election-timer engine))
      (:leader (let ((interval-s (/ (coordinator-config-heartbeat-interval
                                     (warp-coordinator-engine-config engine))
                                    1000.0)))
                 (setf (warp-coordinator-engine-heartbeat-timer engine)
                       (run-at-time
                        interval-s interval-s
                        #'warp-coordinator--send-heartbeats engine)))))))

(defun warp-coordinator--stop-timers (engine)
  "Cancel all timers for a coordinator instance.

Arguments:
- `ENGINE` (warp-coordinator-engine): The instance to manage timers for.

Side Effects:
- Cancels any active timers."
  (when (warp-coordinator-engine-election-timer engine)
    (cancel-timer (warp-coordinator-engine-election-timer engine))
    (setf (warp-coordinator-engine-election-timer engine) nil))
  (when (warp-coordinator-engine-heartbeat-timer engine)
    (cancel-timer (warp-coordinator-engine-heartbeat-timer engine))
    (setf (warp-coordinator-engine-heartbeat-timer engine) nil)))

;;;---------------------------------------------------------------------
;;; Election and Heartbeat Logic
;;;---------------------------------------------------------------------

(defun warp-coordinator--start-election (engine)
  "Initiate a new leader election.
The coordinator dispatches an `:start-election` command to its state
aggregate, which increments the term and transitions its role to
`:candidate`. It then requests votes from all peers.

Arguments:
- `ENGINE` (warp-coordinator-engine): The coordinator engine instance.

Returns:
- (loom-promise): A promise that resolves when the election concludes."
  (warp:log! :info (warp-coordinator-engine-id engine) "Starting election.")
  (braid!
    ;; 1. Atomically update state to become a candidate for a new term.
    (warp:aggregate-dispatch-command
     (warp-coordinator-engine-state-aggregate engine) :start-election)
    (:then (new-state)
      ;; 2. After state is durably updated, request votes from peers.
      (let* ((config (warp-coordinator-engine-config engine))
             (members (coordinator-config-cluster-members config))
             (term (coordinator-state-current-term new-state))
             (votes-received 1) ; Vote for self.
             (quorum (1+ (floor (1+ (length members)) 2)))
             (dialer (warp-coordinator-engine-dialer-service engine))
             (client (warp-coordinator-engine-protocol-client engine)))
        (warp:log! :debug (warp-coordinator-engine-id engine)
                   "Requesting votes for term %d (quorum: %d)." term quorum)
        (let ((promises
               (cl-loop for member-id in members
                        collect
                        (braid! (warp:dialer-dial dialer member-id)
                          (:then (conn)
                            (if conn
                                (coordinator-protocol-client-request-vote
                                 client conn (warp-coordinator-engine-id
                                              engine)
                                 member-id term 0 0)
                              (loom:rejected! "no connection")))))))
          (braid! (loom:all-settled promises)
            (:then (outcomes)
              ;; 3. Tally all granted votes.
              (dolist (outcome outcomes)
                (when (eq (plist-get outcome :status) 'fulfilled)
                  (let* ((response (plist-get outcome :value))
                         (payload (warp-rpc-command-payload response)))
                    (when (plist-get payload :vote-granted-p)
                      (cl-incf votes-received)))))
              ;; 4. If quorum is reached, attempt to become the leader.
              (let ((current-role (coordinator-state-role
                                   (warp:aggregate-get-state
                                    (warp-coordinator-engine-state-aggregate
                                     engine)))))
                (when (and (eq current-role :candidate)
                           (>= votes-received quorum))
                  (loom:await (warp-coordinator--become-leader
                               engine)))))))))))

(defun warp-coordinator--send-heartbeats (engine)
  "Send periodic heartbeats to all peers if this node is the leader.

Arguments:
- `ENGINE` (warp-coordinator-engine): The coordinator engine instance.

Returns:
- (loom-promise): A promise resolving when all heartbeats are sent."
  (let ((state (warp:aggregate-get-state
                (warp-coordinator-engine-state-aggregate engine))))
    (when (eq (coordinator-state-role state) :leader)
      (let* ((config (warp-coordinator-engine-config engine))
             (members (coordinator-config-cluster-members config))
             (term (coordinator-state-current-term state))
             (leader-id (warp-coordinator-engine-id engine))
             (dialer (warp-coordinator-engine-dialer-service engine))
             (client (warp-coordinator-engine-protocol-client engine)))
        (warp:log! :trace leader-id "Sending heartbeats (term %d)." term)
        (dolist (member-id members)
          (braid! (warp:dialer-dial dialer member-id)
            (:then (conn)
              (when conn
                (coordinator-protocol-client-append-entries
                 client conn leader-id member-id term leader-id 0 0 nil 0)))
            (:catch (err)
              (warp:log! :warn leader-id "Heartbeat to %s failed: %S"
                         member-id err))))))))

(defun warp-coordinator--become-leader (engine)
  "Transition the coordinator's role to leader.
This dispatches a `:become-leader` command, adjusts timers, and emits a
`:leader-elected` event to the rest of the system.

Arguments:
- `ENGINE` (warp-coordinator-engine): The coordinator engine instance.

Returns:
- (loom-promise): A promise resolving when the transition is complete."
  (let ((state-agg (warp-coordinator-engine-state-aggregate engine)))
    (braid! (warp:aggregate-dispatch-command state-agg :become-leader)
      (:then (new-state)
        (warp:log! :info (warp-coordinator-engine-id engine)
                   "Becoming leader for term %d."
                   (coordinator-state-current-term new-state))
        ;; As leader, stop the election timer and start the heartbeat timer.
        (warp-coordinator--stop-timers engine)
        (warp-coordinator--start-timers engine)
        ;; Announce the new leader to the rest of the local system.
        (warp:emit-event
         (warp-coordinator-engine-event-system engine)
         :leader-elected
         `(:leader-id ,(coordinator-state-leader-id new-state)
           :term ,(coordinator-state-current-term new-state)))
        (loom:await (warp-coordinator--send-heartbeats engine))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------
;;; Coordinator Engine Lifecycle
;;;---------------------------------------------------------------------

;;;###autoload
(cl-defun warp:coordinator-create (&key name id cluster-id
                                       state-manager event-system
                                       command-router dialer-service
                                       rpc-system config-options)
  "Create and initialize a new `warp-coordinator-engine` instance.
This factory assembles a single node within a coordinator cluster,
linking it to all necessary system components.

Arguments:
- `:name` (string): A unique name for this coordinator instance.
- `:id` (string): The unique identifier for this coordinator instance.
- `:cluster-id` (string): The logical ID of the cluster.
- `:state-manager` (t): The component for shared state.
- `:event-system` (t): The component for emitting events.
- `:command-router` (t): The router for RPC handlers.
- `:dialer-service` (t): The dialer for establishing peer connections.
- `:rpc-system` (t): The component for sending RPC messages.
- `:config-options` (plist): Configuration for this instance.

Returns:
- (warp-coordinator-engine): A new, initialized but unstarted engine."
  (let* ((final-config (apply #'make-coordinator-config config-options))
         (client (make-coordinator-protocol-client :rpc-system rpc-system))
         (state-agg (make-coordinator-state-aggregate
                     nil event-system state-manager))
         (engine (%%make-coordinator-engine
                  :id id
                  :config final-config
                  :state-aggregate state-agg
                  :dialer-service dialer-service
                  :protocol-client client
                  :event-system event-system)))
    (warp:log! :info id "Coordinator engine '%s' created for cluster '%s'."
               id cluster-id)
    engine))

;;;###autoload
(defun warp:coordinator-start (engine)
  "Start the coordinator node's operation.
This makes the coordinator active, allowing it to participate in leader
elections and maintain cluster consensus. Intended to be called as a
component's `:start` hook.

Arguments:
- `ENGINE` (warp-coordinator-engine): The instance to start.

Returns:
- (loom-promise): A promise that resolves when startup is complete."
  (warp:log! :info (warp-coordinator-engine-id engine)
             "Starting coordinator engine.")
  (braid!
    ;; Initialize the durable state aggregate for this node.
    (warp:aggregate-dispatch-command
     (warp-coordinator-engine-state-aggregate engine)
     :initialize (warp-coordinator-engine-id engine))
    (:then (lambda (_)
             ;; Start the timers that drive the consensus protocol.
             (warp-coordinator--start-timers engine)
             t))))

;;;###autoload
(defun warp:coordinator-stop (engine)
  "Stop the coordinator node's operation gracefully.

Arguments:
- `ENGINE` (warp-coordinator-engine): The instance to stop.

Returns:
- (loom-promise): A promise resolving to `t` on successful shutdown."
  (warp:log! :info (warp-coordinator-engine-id engine)
             "Stopping coordinator engine.")
  (warp-coordinator--stop-timers engine)
  (loom:resolved! t))

;;;---------------------------------------------------------------------
;;; Public-Facing Coordinator Service & Plugin
;;;---------------------------------------------------------------------

(warp:defservice-interface :coordinator-service
  "The public-facing API for cluster coordination primitives.
This service provides high-level functions for interacting with the
cluster, such as getting the current leader or acquiring a lock.

Methods:
- `get-leader-id`: Returns the ID of the current cluster leader.
- `get-role`: Returns the role of the local coordinator node.
- `acquire-lock`: Acquires a distributed lock.
- `release-lock`: Releases a distributed lock."
  :methods '((get-leader-id ()) (get-role ())
             (acquire-lock (lock-name &key timeout))
             (release-lock (lock-name))))

(warp:defplugin :coordinator
  "Provides the distributed coordination service for leader election
and distributed locking."
  :version "1.3.0"
  :dependencies '(state-manager redis-service dialer-service rpc-system)

  :profiles
  `((:coordinator-worker
     :doc "The profile for a dedicated coordinator runtime."
     :components '(coordinator-engine coordinator-service)
     :hooks `((after-cluster-leader-elected
               ,(lambda (cluster-id leader-id)
                  (let ((system
                         (warp:component-system-get-host-system
                          (loom-current-component))))
                    (warp:log! :info "coordinator"
                               "Leader elected, starting services."))))
              (after-cluster-shutdown
               ,(lambda (cluster-id reason)
                  (warp:log! :info "coordinator"
                             "Cluster shutdown, stopping services."))))))

  :components
  `((coordinator-engine
     :doc "The internal engine that drives the consensus protocol."
     :requires '(runtime-instance config-service event-system command-router
                 rpc-system dialer-service state-manager)
     :factory (lambda (runtime cfg es router rpc dialer sm)
                (warp:coordinator-create
                 :id (warp-runtime-instance-id runtime)
                 :cluster-id (warp-worker-cluster-id runtime)
                 :config-options (cl-struct-to-plist cfg)
                 :state-manager sm :event-system es :command-router router
                 :dialer-service dialer :rpc-system rpc))
     :start (lambda (engine ctx) (loom:await (warp:coordinator-start engine)))
     :stop (lambda (engine ctx) (loom:await (warp:coordinator-stop engine))))
    (coordinator-service
     :doc "The public-facing API for the coordination service."
     :requires '(coordinator-engine state-manager)
     :factory (lambda (engine sm) `(:engine ,engine :state-manager ,sm)))))

(provide 'warp-coordinator)
;;; warp-coordinator.el ends here