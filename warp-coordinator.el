;;; warp-coordinator.el --- Distributed Coordinator Service -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a distributed coordination service for the Warp
;; framework, offering essential primitives for building resilient,
;; high-availability systems, including leader election and distributed
;; locks.
;;
;; ## Architectural Philosophy
;;
;; Why: The coordinator is the heart of the cluster's control plane. This
;; version has been refactored to use a formal, internal service, the
;; `:coordinator-protocol-service`, for all its peer-to-peer
;; communication. This brings the coordinator's design in line with the
;; modern, service-oriented architecture of the framework.
;;
;; It implements a simplified, heartbeat-based, **Raft-like protocol** to
;; ensure that within a cluster, only one node acts as the "leader" for
;; critical, centralized services.
;;
;; **Important Note on Durability**: This implementation relies on the
;; `warp-state-manager` for its distributed state persistence (e.g.,
;; current term, leader ID, and locks). For true durability that
;; survives a full cluster shutdown, the state manager *must* be
;; configured with a persistent backend like Redis.
;;

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

;; Forward declaration for the internal protocol client
(cl-deftype coordinator-protocol-client () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-coordinator-error
  "A generic error for `warp-coordinator` operations."
  'warp-error)

(define-error 'warp-coordinator-not-leader
  "Operation requires leader role, but this node is a follower.

This error is often used to signal a need for client-side redirection
to the currently known leader."
  'warp-coordinator-error)

(define-error 'warp-coordinator-lock-failed
  "Failed to acquire or release a distributed lock.

This can be due to the lock being held by another process, a network
failure communicating with the leader, or a timeout."
  'warp-coordinator-error)

(define-error 'warp-coordinator-timeout
  "A coordinator operation timed out.

Signaled when a high-level operation like acquiring a lock fails to
complete within its total allotted time, including all retries."
  'warp-coordinator-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig (coordinator-config (:extends '(warp-worker-config)))
  "Configuration for a `warp-coordinator-instance`.

This config inherits from `warp-worker-config` and adds tunables
specific to the Raft-like consensus protocol.

:Fields:
- `election-timeout-min` (float): The minimum random election timeout
  in milliseconds. The timeout is randomized between min and max to
  prevent split-vote scenarios where multiple nodes start elections
  simultaneously.
- `election-timeout-max` (float): The maximum random election timeout
  in milliseconds.
- `heartbeat-interval` (float): The interval in milliseconds at which a
  leader sends heartbeats to followers to maintain its authority.
- `lock-lease-time` (float): The default duration in seconds that a
  distributed lock is held before it can be considered expired.
- `rpc-timeout` (float): The timeout in seconds for individual,
  internal RPCs between coordinator peers.
- `listen-address` (string): The network address this coordinator node
  listens on for peer communication.
- `cluster-members` (list): A list of network addresses (strings) for
  all other coordinator nodes in the cluster."
  (election-timeout-min 150.0 :type float)
  (election-timeout-max 300.0 :type float)
  (heartbeat-interval 50.0 :type float)
  (lock-lease-time 300.0 :type float)
  (rpc-timeout 5.0 :type float)
  (listen-address nil :type (or null string))
  (cluster-members nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-coordinator-instance
               (:constructor %%make-coordinator-instance)
               (:copier nil))
  "Represents a single node in the distributed coordination service.

Why: This struct encapsulates the complete runtime state of a
coordinator, including its role in the cluster, its view of the
consensus state, and its dependencies on other core framework components.

:Fields:
- `id` (string): The unique identifier for this coordinator instance.
- `cluster-id` (string): The ID of the cluster it belongs to.
- `config` (coordinator-config): The configuration object for this
  instance.
- `state-manager` (t): The component for storing shared state durably.
- `event-system` (t): The event bus for emitting leadership changes.
- `command-router` (t): The router for handling incoming RPCs from
  peers.
- `dialer-service` (t): The dialer for establishing peer connections.
- `rpc-system` (t): The RPC system for making calls to peers.
- `protocol-client` (t): The auto-generated RPC client for the internal
  `:coordinator-protocol-service`, used for peer communication.
- `role` (keyword): The current Raft-like role of this node.
- `current-term` (integer): The current election term number.
- `voted-for` (string): The candidate ID this node voted for in the
  `current-term`.
- `leader-id` (string): The ID of the node that this instance believes
  is the current leader.
- `election-timer` (timer): A timer used by followers/candidates.
- `heartbeat-timer` (timer): A timer used only by the leader.
- `lock-registry` (hash-table): A local cache of distributed locks."
  (id                   (cl-assert nil) :type string)
  (cluster-id           (cl-assert nil) :type string)
  (config               (cl-assert nil) :type coordinator-config)
  (state-manager        (cl-assert nil) :type (or null t))
  (event-system         (cl-assert nil) :type (or null t))
  (command-router       (cl-assert nil) :type (or null t))
  (dialer-service       (cl-assert nil) :type (or null t))
  (rpc-system           (cl-assert nil) :type (or null t))
  (protocol-client      nil :type (or null t))
  (role                 :follower :type keyword)
  (current-term         0 :type integer)
  (voted-for            nil :type (or null string))
  (leader-id            nil :type (or null string))
  (election-timer       nil :type (or null timer))
  (heartbeat-timer      nil :type (or null timer))
  (lock-registry        (make-hash-table :test 'equal) :type hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Service Interface (Internal Protocol)

(warp:defservice-interface :coordinator-protocol-service
  "Defines the internal Raft-like protocol for peer communication.

Why: This is not a public-facing service. It formalizes the contract used
exclusively by coordinator nodes to perform leader election and maintain
cluster consensus.

:Methods:
- `request-vote`: Called by candidates to request votes from peers
  during an election.
- `append-entries`: Called by the leader to send heartbeats and
  replicate state to followers."
  :methods
  '((request-vote (candidate-id term last-log-index last-log-term)
     "Handles a vote request from a candidate during an election.")
    (append-entries (term leader-id prev-log-index prev-log-term entries leader-commit)
     "Handles a heartbeat or log replication from the current leader.")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;---------------------------------------------------------------------
;;; State Management & Timers
;;;---------------------------------------------------------------------



;;;---------------------------------------------------------------------
;;; Election and Heartbeat Logic
;;;---------------------------------------------------------------------

(defun warp-coordinator--start-election (coordinator)
  "Initiate a new leader election.

Why: This function implements the core election logic. The coordinator
increments its term, transitions to the `:candidate` role, votes for
itself, and then requests votes from all peers using the auto-generated
protocol client.

How: It now uses the high-level `dialer-service` to establish connections
to its peers. This simplifies the logic by abstracting away the details
of connection management, allowing this function to focus solely on the
Raft-like election protocol.

Arguments:
- `COORDINATOR` (warp-coordinator-instance): The coordinator instance.

Returns:
- (loom-promise): A promise that resolves when the election concludes."
  (warp:log! :info (warp-coordinator-instance-id coordinator)
             "Starting election.")
  ;; 1. Transition to candidate state for the new term.
  (cl-incf (warp-coordinator-instance-current-term coordinator))
  (setf (warp-coordinator-instance-role coordinator) :candidate)
  ;; Vote for self.
  (setf (warp-coordinator-instance-voted-for coordinator)
        (warp-coordinator-instance-id coordinator))
  ;; Reset the election timer; if this election fails, another will start.
  (warp-coordinator--reset-election-timer coordinator)

  (let* ((config (warp-coordinator-instance-config coordinator))
         (members (coordinator-config-cluster-members config))
         (term (warp-coordinator-instance-current-term coordinator))
         ;; Start with 1 vote (for self).
         (votes-received 1)
         ;; A majority is needed to win the election.
         (quorum (1+ (floor (1+ (length members)) 2)))
         (dialer (warp-coordinator-instance-dialer-service coordinator))
         (client (warp-coordinator-instance-protocol-client coordinator)))

    (warp:log! :debug (warp-coordinator-instance-id coordinator)
               "Requesting votes for term %d (quorum: %d)." term quorum)

    ;; 2. Concurrently send vote requests to all other cluster members.
    (let ((promises
           (cl-loop for member-id in members
                    collect
                    ;; Use the dialer to get a connection to the peer.
                    (braid! (warp:dialer-dial dialer member-id)
                      (:then (conn)
                        (if conn
                            ;; If connected, send the RPC vote request.
                            (coordinator-protocol-client-request-vote
                             client conn
                             (warp-coordinator-instance-id coordinator)
                             member-id term 0 0)
                          (loom:rejected! "no connection")))))))
      ;; 3. Wait for all vote requests to either succeed or fail.
      (braid! (loom:all-settled promises)
        (:then (outcomes)
          ;; 4. Tally all granted votes from successful responses.
          (dolist (outcome outcomes)
            (when (eq (plist-get outcome :status) 'fulfilled)
              (let* ((response (plist-get outcome :value))
                     (payload (warp-rpc-command-payload response)))
                (when (plist-get payload :vote-granted-p)
                  (cl-incf votes-received)))))
          ;; 5. If we have a majority and are still a candidate, become leader.
          (when (and (eq (warp-coordinator-instance-role coordinator) :candidate)
                     (>= votes-received quorum))
            (loom:await (warp-coordinator--become-leader coordinator))))))))

(defun warp-coordinator--send-heartbeats (coordinator)
  "Send periodic heartbeats to all peers if this node is the leader.

Why: This function is called by a timer on the leader node to assert its
authority and prevent followers from starting new elections.

How: It iterates through all known cluster members and uses the
`dialer-service` to establish a connection to each one before sending a
heartbeat RPC. This simplifies the logic by removing the need for manual
connection state management.

Arguments:
- `COORDINATOR` (warp-coordinator-instance): The coordinator instance.

Returns:
- (loom-promise): A promise that resolves when all heartbeats are sent."
  ;; Only the leader should send heartbeats.
  (when (eq (warp-coordinator-instance-role coordinator) :leader)
    (let* ((config (warp-coordinator-instance-config coordinator))
           (members (coordinator-config-cluster-members config))
           (term (warp-coordinator-instance-current-term coordinator))
           (leader-id (warp-coordinator-instance-id coordinator))
           (dialer (warp-coordinator-instance-dialer-service coordinator))
           (client (warp-coordinator-instance-protocol-client coordinator)))
      (warp:log! :trace leader-id "Sending heartbeats (term %d)." term)
      ;; Send a heartbeat to every other member of the cluster.
      (dolist (member-id members)
        (braid! (warp:dialer-dial dialer member-id)
          (:then (conn)
            (when conn
              ;; Send the append-entries RPC, which acts as a heartbeat.
              (coordinator-protocol-client-append-entries
               client conn leader-id member-id term leader-id 0 0 nil 0)))
          (:catch (err)
            ;; Log failures but don't stop; continue trying to contact other members.
            (warp:log! :warn leader-id "Heartbeat to %s failed: %S"
                       member-id err)))))))

(defun warp-coordinator--become-leader (coordinator)
  "Transition the coordinator's role to leader.

Why: This is called after winning an election. It cancels the election
timer, starts the heartbeat timer, updates the shared leader state
in the state manager, and emits a `:leader-elected` event.

Arguments:
- `COORDINATOR` (warp-coordinator-instance): The coordinator instance.

Returns:
- (loom-promise): A promise resolving when the transition is complete."
  (warp:log! :info (warp-coordinator-instance-id coordinator)
             "Becoming leader for term %d."
             (warp-coordinator-instance-current-term coordinator))
  ;; 1. Update local state to reflect the new role.
  (setf (warp-coordinator-instance-role coordinator) :leader)
  (setf (warp-coordinator-instance-leader-id coordinator)
        (warp-coordinator-instance-id coordinator))

  ;; 2. A leader no longer needs an election timer.
  (when (warp-coordinator-instance-election-timer coordinator)
    (cancel-timer (warp-coordinator-instance-election-timer coordinator))
    (setf (warp-coordinator-instance-election-timer coordinator) nil))
  (when (warp-coordinator-instance-heartbeat-timer coordinator)
    (cancel-timer (warp-coordinator-instance-heartbeat-timer coordinator)))

  ;; 3. Start the periodic heartbeat timer to maintain leadership.
  (let ((interval-s (/ (coordinator-config-heartbeat-interval
                        (warp-coordinator-instance-config coordinator))
                       1000.0)))
    (setf (warp-coordinator-instance-heartbeat-timer coordinator)
          (run-at-time interval-s interval-s
                       #'warp-coordinator--send-heartbeats coordinator)))

  ;; 4. Update the durable, shared state to announce leadership to the cluster.
  (loom:await (warp-coordinator--set-leader-state
               coordinator `(:id ,(warp-coordinator-instance-id coordinator)
                             :term ,(warp-coordinator-instance-current-term coordinator))))

  ;; 5. Emit a local event to notify all components on this node of the change.
  (warp:emit-event
   (warp-coordinator-instance-event-system coordinator)
   :leader-elected
   `(:leader-id ,(warp-coordinator-instance-id coordinator)
     :term ,(warp-coordinator-instance-current-term coordinator)))

  ;; 6. Send an immediate heartbeat to establish authority with followers.
  (loom:await (warp-coordinator--send-heartbeats coordinator)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------
;;; State Management & Timers
;;;---------------------------------------------------------------------

(defun warp-coordinator--get-leader-state (coordinator)
  "Retrieve the current leader state from `warp-state-manager`.

:Arguments:
- `COORDINATOR` (warp-coordinator-instance): The coordinator instance.

:Returns:
- (loom-promise): A promise that resolves to a plist of leader state
  or `nil`."
  ;; Delegate the read operation to the state manager component.
  (warp:state-manager-get
   (warp-coordinator-instance-state-manager coordinator)
   ;; Use a structured path to access the specific piece of state.
   `(:coordinator :leader-state)))

(defun warp-coordinator--set-leader-state (coordinator leader-state)
  "Update the leader state in `warp-state-manager`.

Why: This is called by a new leader to assert its leadership to the
cluster by writing to the durable, shared state.

:Arguments:
- `COORDINATOR` (warp-coordinator-instance): The coordinator instance.
- `LEADER-STATE` (plist): The new leader state to write.

:Returns:
- (loom-promise): A promise resolving to `t` on success."
  ;; Delegate the write operation to the state manager.
  (warp:state-manager-update
   (warp-coordinator-instance-state-manager coordinator)
   `(:coordinator :leader-state)
   leader-state))

(defun warp-coordinator--update-term (coordinator new-term &optional leader-id)
  "Update the coordinator's term, a core Raft-like safety mechanism.

Why: If `NEW-TERM` is strictly greater than the current term, this node
reverts to a follower, as the higher term indicates a more up-to-date
view of the cluster is available elsewhere. This is a critical rule
that prevents stale nodes from disrupting the cluster.

:Arguments:
- `COORDINATOR` (warp-coordinator-instance): The coordinator instance.
- `NEW-TERM` (integer): The new term number.
- `LEADER-ID` (string, optional): The ID of the new leader, if known.

:Returns:
- `nil`.

:Side Effects:
- Modifies the coordinator's `role`, `current-term`, and `voted-for`
  fields."
  ;; This condition is the core safety rule: a node always yields to a higher term.
  (when (> new-term (warp-coordinator-instance-current-term coordinator))
    (warp:log! :info (warp-coordinator-instance-id coordinator)
               "Term updated: %d -> %d. Reverting to follower."
               (warp-coordinator-instance-current-term coordinator) new-term)
    ;; Update the local term number.
    (setf (warp-coordinator-instance-current-term coordinator) new-term)
    ;; Revert to the follower state.
    (setf (warp-coordinator-instance-role coordinator) :follower)
    ;; Clear the vote for the current (now old) term.
    (setf (warp-coordinator-instance-voted-for coordinator) nil)
    ;; Accept the new leader associated with the higher term.
    (setf (warp-coordinator-instance-leader-id coordinator) leader-id)
    ;; As a new follower, cancel any pending election or heartbeat timers.
    (when (warp-coordinator-instance-election-timer coordinator)
      (cancel-timer (warp-coordinator-instance-election-timer coordinator))
      (setf (warp-coordinator-instance-election-timer coordinator) nil))))

(defun warp-coordinator--reset-election-timer (coordinator)
  "Reset the election timer with a random timeout.

Why: This is called by followers and candidates. If no valid heartbeat
is received from a leader, this timer will expire and trigger a new
election. The randomized timeout (jitter) is crucial for preventing
split-vote scenarios where all nodes start elections simultaneously.

:Arguments:
- `COORDINATOR` (warp-coordinator-instance): The coordinator instance.

:Returns:
- `nil`.

:Side Effects:
- Cancels any existing `election-timer` and schedules a new one."
  ;; Always cancel any existing timer to prevent duplicates.
  (when (warp-coordinator-instance-election-timer coordinator)
    (cancel-timer (warp-coordinator-instance-election-timer coordinator)))
  (let* ((config (warp-coordinator-instance-config coordinator))
         ;; Calculate a random timeout within the configured range.
         ;; This "jitter" is critical to prevent multiple nodes from
         ;; starting an election at the exact same time.
         (min-ms (coordinator-config-election-timeout-min config))
         (max-ms (coordinator-config-election-timeout-max config))
         (timeout-s (/ (float (+ min-ms (random (- max-ms min-ms))))
                       1000.0)))
    ;; Schedule the next election attempt to run after the timeout.
    (setf (warp-coordinator-instance-election-timer coordinator)
          (run-at-time timeout-s nil
                       #'warp-coordinator--start-election coordinator))
    (warp:log! :debug (warp-coordinator-instance-id coordinator)
               "Election timer reset to %.3fs." timeout-s)))

;;;----------------------------------------------------------------------
;;; Coordinator Initialization & Lifecycle
;;;----------------------------------------------------------------------

;;;###autoload
(cl-defun warp:coordinator-create (&key name id cluster-id
                                       state-manager event-system
                                       command-router dialer-service
                                       rpc-system config-options)
  "Creates and initializes a new `warp-coordinator-instance`.

Why: This factory function assembles a single node within a distributed
coordinator cluster, linking it to all necessary system components.
It is the designated constructor for the coordinator.

How: It takes all its dependencies as keyword arguments (which are
injected by the component system), creates the internal protocol client,
and then instantiates the main `warp-coordinator-instance` struct.

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
- (warp-coordinator-instance): A new, initialized but unstarted
  coordinator instance."
  (let* ((final-config (apply #'make-coordinator-config config-options))
         ;; The protocol client for peer-to-peer communication is created here.
         (client (make-coordinator-protocol-client :rpc-system rpc-system))
         (coordinator (%%make-coordinator-instance
                       :id id
                       :cluster-id cluster-id
                       :config final-config
                       :state-manager state-manager
                       :event-system event-system
                       :command-router command-router
                       :dialer-service dialer-service
                       :rpc-system rpc-system
                       :protocol-client client)))
    (warp:log! :info id "Coordinator instance '%s' created for cluster '%s'."
               id cluster-id)
    coordinator))

;;;###autoload
(defun warp:coordinator-start (coordinator)
  "Starts the coordinator node's operation.

Why: This function makes the coordinator active. It allows the
node to participate in the leader election process and begin
maintaining cluster consensus.

How: This function is intended to be called as a component's `:start`
hook. It first establishes connections to its configured peers via
private helpers and then starts the crucial election timer, which
will trigger a new election if no leader heartbeat is heard within
the configured timeout.

Arguments:
- `COORDINATOR` (warp-coordinator-instance): The instance to start.

Returns:
- (loom-promise): A promise that resolves when startup is complete."
  (warp:log! :info (warp-coordinator-instance-id coordinator)
             "Starting coordinator.")
  ;; Internally, this will add all peer addresses to the dialer service.
  (braid! (warp-coordinator--connect-to-peers coordinator)
    (:then (lambda (_)
             ;; This starts the election timer for followers/candidates.
             (warp-coordinator--start-timers coordinator)
             t))))

;;;###autoload
(defun warp:coordinator-stop (coordinator)
  "Stops the coordinator node's operation gracefully.

Why: This function provides a clean shutdown mechanism for the
coordinator node, ensuring it leaves the cluster cleanly and
releases all system resources.

How: It is intended to be called as a component's `:stop` hook. It
cancels all active timers (both election and heartbeat timers) and
disconnects from all peers.

Arguments:
- `COORDINATOR` (warp-coordinator-instance): The instance to stop.

Returns:
- (loom-promise): A promise that resolves to `t` on successful
  shutdown."
  (warp:log! :info (warp-coordinator-instance-id coordinator)
             "Stopping coordinator.")
  ;; Cancel all timers to prevent further elections or heartbeats.
  (warp-coordinator--stop-timers coordinator)
  ;; Disconnect from all peers.
  (braid! (warp-coordinator--disconnect-from-peers coordinator)
    (:then (lambda (_) t))))

;;;----------------------------------------------------------------------
;;; Status and Distributed Primitives
;;;----------------------------------------------------------------------

;;;###autoload
(defun warp:coordinator-get-leader (coordinator)
  "Retrieves the currently known leader's ID.

Why: This is the primary way for components to find the current
leader for directing leader-specific requests, such as acquiring a
distributed lock or submitting a new job.

How: It performs a simple, local read of the `leader-id` field on
the coordinator instance. This field is kept up-to-date by the
background Raft-like protocol as leadership changes occur.

Arguments:
- `COORDINATOR` (warp-coordinator-instance): The coordinator instance.

Returns:
- (string or nil): The ID of the known leader, or `nil` if no
  leader is currently known."
  ;; Directly access the struct slot, which is maintained by the consensus protocol.
  (warp-coordinator-instance-leader-id coordinator))

;;;###autoload
(defun warp:coordinator-get-role (coordinator)
  "Retrieves the current role of this coordinator instance.

Why: This function is useful for introspection, health checks, and
debugging, allowing a node to report its current status in the
consensus protocol (e.g., whether it is a leader or a follower).

Arguments:
- `COORDINATOR` (warp-coordinator-instance): The coordinator instance.

Returns:
- (keyword): The current role (`:follower`, `:candidate`, or `:leader`)."
  ;; Directly access the struct slot.
  (warp-coordinator-instance-role coordinator))

;;;###autoload
(cl-defun warp:coordinator-get-lock (coordinator lock-name &key timeout)
  "Acquires a distributed lock.

Why: This function provides the core distributed locking primitive,
which is essential for ensuring that critical, non-idempotent
operations are performed by only one process at a time across the
entire cluster, preventing race conditions.

How: The request is always directed to the currently known leader.
It uses the `dialer-service` to establish a connection and then
sends an RPC to the leader requesting the lock. The leader manages
the lock's state centrally.

Arguments:
- `COORDINATOR` (warp-coordinator-instance): The coordinator instance.
- `LOCK-NAME` (string): The unique name of the lock to acquire.
- `:timeout` (number, optional): Max seconds to wait for the lock.

Returns:
- (loom-promise): A promise that resolves to `t` if the lock is
  acquired, or rejects with `warp-coordinator-lock-failed`."
  (let* ((req-timeout (or timeout 30.0))
         (holder-id (warp-coordinator-instance-id coordinator)))
    (braid!
        ;; First, get the ID of the current leader.
        (let ((leader-id (warp:coordinator-get-leader coordinator)))
          (when leader-id
            ;; Use the dialer to establish a connection to the leader.
            (braid! (warp:dialer-dial (warp-coordinator-instance-dialer-service coordinator)
                                      leader-id)
              (:then (conn)
                ;; Once connected, send the RPC to the leader to acquire the lock.
                (coordinator-protocol-client-acquire-lock
                 (warp-coordinator-instance-protocol-client coordinator)
                 conn holder-id leader-id lock-name req-timeout)))))
      (:then (response)
        ;; If the leader grants the lock, update local state and resolve.
        (if (plist-get (warp-rpc-command-payload response) :granted-p)
            (progn
              (puthash lock-name t
                       (warp-coordinator-instance-lock-registry coordinator))
              t)
          ;; If the lock is not granted, reject the promise.
          (loom:rejected! (warp:error! :type 'warp-coordinator-lock-failed))))
      (:catch (err)
        ;; If dialing or the RPC fails, reject the promise.
        (loom:rejected! (warp:error! :type 'warp-coordinator-lock-failed
                                     :cause err))))))

;;;###autoload
(defun warp:coordinator-release-lock (coordinator lock-name)
  "Releases a previously acquired distributed lock.

Why: This function provides the mechanism for a node to gracefully
release a distributed lock it has acquired, allowing other processes
to acquire it and proceed with their work.

How: The request is sent to the current leader, which is the central
authority for all locks. It uses the `dialer-service` to connect and
then sends an RPC to release the lock.

Arguments:
- `COORDINATOR` (warp-coordinator-instance): The coordinator instance.
- `LOCK-NAME` (string): The name of the lock to release.

Returns:
- (loom-promise): A promise that resolves to `t` on successful release."
  (let ((holder-id (warp-coordinator-instance-id coordinator))
        (leader-id (warp:coordinator-get-leader coordinator)))
    (when leader-id
      ;; Use the dialer to establish a connection to the leader.
      (braid! (warp:dialer-dial (warp-coordinator-instance-dialer-service coordinator)
                                leader-id)
        (:then (conn)
          ;; Send the RPC to the leader to release the lock.
          (coordinator-protocol-client-release-lock
           (warp-coordinator-instance-protocol-client coordinator)
           conn holder-id leader-id lock-name))
        (:then (response)
          ;; If the release was successful, update local state.
          (if (plist-get (warp-rpc-command-payload response) :success-p)
              (progn
                (remhash lock-name
                         (warp-coordinator-instance-lock-registry coordinator))
                t)
            (loom:rejected! (warp:error! :type 'warp-coordinator-lock-failed))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Component & Plugin Definitions

(warp:defplugin :coordinator
  "Provides the distributed coordination service for leader election
and distributed locking."
  :version "1.3.0"
  :dependencies '(state-manager redis-service dialer-service rpc-system)

  :profiles
  `((:coordinator-worker
     :doc "The profile for a dedicated coordinator runtime."
     :components '(coordinator)
     :hooks `((after-cluster-leader-elected
               ,(lambda (cluster-id leader-id)
                  (let ((system (warp:component-system-get-host-system (loom-current-component))))
                    (warp:log! :info "coordinator" "Leader elected, starting services."))))
              (after-cluster-shutdown
               ,(lambda (cluster-id reason)
                  (warp:log! :info "coordinator" "Cluster shutdown, stopping services."))))))

  :components
  `((coordinator
     :doc "The core coordination service component."
     :requires '(runtime-instance config event-system command-router
                 rpc-system dialer-service state-manager)
     :factory (lambda (runtime cfg es router rpc dialer sm)
                (let* ((client (make-coordinator-protocol-client :rpc-system rpc))
                       (coordinator (%%make-coordinator-instance
                                     :id (warp-runtime-instance-id runtime)
                                     :cluster-id (warp-worker-cluster-id runtime)
                                     :config cfg
                                     :state-manager sm
                                     :event-system es
                                     :command-router router
                                     :dialer-service dialer
                                     :rpc-system rpc
                                     :protocol-client client)))
                  (warp:log! :info (warp-runtime-instance-id runtime)
                             "Coordinator instance '%s' created for cluster '%s'."
                             (warp-runtime-instance-id runtime)
                             (warp-worker-cluster-id runtime))
                  coordinator))
     :start (lambda (coord ctx)
              (loom:await (warp:coordinator-start coord)))
     :stop (lambda (coord ctx)
             (loom:await (warp:coordinator-stop coord))))))

(provide 'warp-coordinator)
;;; warp-coordinator.el ends here