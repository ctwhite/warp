;;; warp-coordinator.el --- Distributed Coordinator Service -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a simplified distributed coordination service
;; for the Warp framework. It offers abstractions for distributed
;; locks, leader election, and basic consensus-driven state
;; synchronization, enabling robust distributed operations.
;;
;; **Important Architectural Note and Limitations**:
;; This implementation relies heavily on `warp-state-manager` for its
;; distributed state persistence. For true, fault-tolerant distributed
;; consensus (e.g., a full Raft implementation that survives power loss
;; of a majority of nodes), `warp-state-manager` would need to be
;; backed by a truly persistent, replicated, and highly available
;; storage system (like Redis, ZooKeeper, or a distributed database)
;; that handles data durability and consistency across restarts.
;;
;; In its current form, if the underlying `warp-state-manager` is
;; purely in-memory and all coordinator nodes restart simultaneously,
;; the distributed state (locks, leader, barriers) will be lost. This
;; coordinator provides *consistency* and *synchronization* for
;; *currently running* distributed operations, but not full
;; *durability* without external persistent state for
;; `warp-state-manager`.
;;
;; ## Key Features:
;;
;; - **Distributed Locks**: Acquire and release named locks across the
;;   cluster. Uses a leader-centric approach built on
;;   `warp-state-manager` for state.
;; - **Leader Election**: A simplified heartbeat-based election
;;   mechanism. Nodes contend for leadership by writing to
;;   `warp-state-manager`.
;; - **Consensus Operations (Simplified)**: Leader-driven state changes.
;;   The leader proposes an update, and followers acknowledge.
;;   (Note: This is NOT a full Raft/Paxos log replication and
;;   consistency).
;; - **Distributed Barriers**: Synchronize multiple workers at a
;;   specific point in a distributed operation.
;;
;; ## Integration Points:
;;
;; - **`warp-state-manager`**: Fundamental for storing shared
;;   distributed state (locks, leader info, barrier counts).
;; - **`warp-rpc` / `warp-transport`**: Used for inter-coordinator
;;   communication (e.g., vote requests, heartbeats, lock/barrier RPCs).
;; - **`warp-event`**: For emitting notifications about leadership
;;   changes, lock status, etc.
;; - **`warp-command-router`**: For registering RPC handlers that process
;;   coordinator-specific commands from other nodes.

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
(require 'warp-connection-manager)
(require 'warp-config)
(require 'warp-component)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-coordinator-error
  "Generic error for `warp-coordinator` operations."
  'warp-error)

(define-error 'warp-coordinator-not-leader
  "Operation requires leader role, but this node is a follower."
  'warp-coordinator-error)

(define-error 'warp-coordinator-election-failed
  "Leader election failed to complete successfully."
  'warp-coordinator-error)

(define-error 'warp-coordinator-lock-failed
  "Failed to acquire or release a distributed lock."
  'warp-coordinator-error)

(define-error 'warp-coordinator-barrier-failed
  "Distributed barrier operation failed."
  'warp-coordinator-error)

(define-error 'warp-coordinator-consensus-failed
  "Distributed consensus operation failed."
  'warp-coordinator-error)

(define-error 'warp-coordinator-timeout
  "A coordinator operation timed out."
  'warp-coordinator-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig coordinator-config
  "Configuration for a `warp-coordinator-instance`.

Fields:
- `election-timeout-min` (float): Minimum random election timeout (ms).
- `election-timeout-max` (float): Maximum random election timeout (ms).
- `heartbeat-interval` (float): Leader heartbeat interval (ms).
- `lock-lease-time` (float): Default duration a lock is held (seconds).
- `rpc-timeout` (float): Timeout for internal coordinator RPCs.
- `cluster-members` (list): List of other coordinator node IDs (strings)."
  (election-timeout-min 150.0 :type float :validate (> $ 0.0))
  (election-timeout-max 300.0 :type float :validate (> $ 0.0))
  (heartbeat-interval 50.0 :type float :validate (> $ 0.0))
  (lock-lease-time 300.0 :type float :validate (> $ 0.0))
  (rpc-timeout 5.0 :type float :validate (> $ 0.0))
  (cluster-members nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-coordinator-log-entry
               (:constructor make-warp-coordinator-log-entry)
               (:copier nil))
  "A simplified log entry for the coordinator's state changes.
(Note: This is NOT a full Raft log, but a simplified in-memory representation
for basic state propagation and leader-driven consistency in this model).

Fields:
- `index` (integer): The index of this entry in the log.
- `term` (integer): The term in which this entry was created.
- `command` (keyword): The command type (e.g., `:set-key`,
  `:acquire-lock`).
- `data` (t): The data associated with the command."
  (index 0 :type integer)
  (term 0 :type integer)
  (command nil :type keyword)
  (data nil :type t))

(cl-defstruct (warp-coordinator-instance
               (:constructor %%make-coordinator-instance)
               (:copier nil))
  "Represents a single node in the distributed coordination service.

Fields:
- `id` (string): Unique identifier for this coordinator instance.
- `cluster-id` (string): The ID of the cluster it belongs to.
- `config` (coordinator-config): Configuration for this coordinator.
- `state-manager` (warp-state-manager): Reference to the shared state
  manager component, used for storing distributed state (locks, leader, etc.).
- `event-system` (warp-event-system): Reference to the event system,
  used for emitting leadership changes and other coordination events.
- `command-router` (warp-command-router): The internal command router
  for dispatching RPC handlers specific to coordinator operations.
- `connection-manager` (warp-connection-manager): Manages network connections
  to other coordinator nodes and potentially the master.
- `rpc-system` (warp-rpc-system): Reference to the RPC system for
  making outgoing RPC calls to peers.
- `component-system-id` (string): The ID of the parent `warp-component-system`
  instance, used for `warp-rpc`'s `origin-instance-id`.
- `role` (keyword): Current role in the consensus (`:follower`, `:candidate`,
  `:leader`).
- `current-term` (integer): Current term number this coordinator
  believes. Increments during elections.
- `voted-for` (string or nil): Candidate ID voted for in `current-term`.
- `leader-id` (string or nil): ID of the known leader (if any).
- `log` (list): In-memory list of committed log entries (simplified Raft log).
- `election-timer` (timer or nil): Timer for election timeouts; triggers
  new elections if no leader heartbeat is seen.
- `heartbeat-timer` (timer or nil): Timer for leader heartbeats; leaders
  send these periodically to maintain their status.
- `next-index` (hash-table): For leader, next log index to send to
  each follower. (Not fully implemented in this simplified version).
- `match-index` (hash-table): For leader, highest log index known to
  be replicated on each follower. (Not fully implemented).
- `lock-registry` (hash-table): Local cache of distributed locks held
  by this node, mapping lock-name to `lock-info` (e.g., expiry time).
- `barrier-registry` (hash-table): Local cache of active distributed
  barriers it's participating in."
  (id (cl-assert nil) :type string)
  (cluster-id (cl-assert nil) :type string)
  (config (cl-assert nil) :type coordinator-config)
  (state-manager (cl-assert nil) :type (satisfies warp-state-manager-p))
  (event-system (cl-assert nil) :type (satisfies warp-event-system-p))
  (command-router (cl-assert nil) :type (satisfies warp-command-router-p))
  (connection-manager (cl-assert nil)
                      :type (satisfies warp-connection-manager-p))
  (rpc-system (cl-assert nil) :type (satisfies warp-rpc-system-p))
  (component-system-id (cl-assert nil) :type string)
  (role :follower :type keyword)
  (current-term 0 :type integer)
  (voted-for nil :type (or null string))
  (leader-id nil :type (or null string))
  (log nil :type list) ; Simplified in-memory log
  (election-timer nil :type (or null timer))
  (heartbeat-timer nil :type (or null timer))
  (next-index (make-hash-table :test 'equal) :type hash-table)
  (match-index (make-hash-table :test 'equal) :type hash-table)
  (lock-registry (make-hash-table :test 'equal) :type hash-table)
  (barrier-registry (make-hash-table :test 'equal) :type hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; State Management (Local & Distributed via warp-state-manager)
;;----------------------------------------------------------------------

(defun warp-coordinator--get-leader-state (coordinator)
  "Retrieves the current leader state from `warp-state-manager`.
The leader state typically includes the current leader's ID, the term
they are serving, and the last heartbeat timestamp. This information is
stored in a well-known path within the shared state manager.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns:
- (loom-promise): A promise that resolves to a plist like
  `(:id \"leader-x\" :term 5 :last-heartbeat 123.4)` or `nil` if no
  leader info is present in the state manager.

Side Effects:
- Queries the `warp-state-manager`."
  (warp:state-manager-get
   (warp-coordinator-state-manager coordinator)
   `(:coordinator :leader-state)))

(defun warp-coordinator--set-leader-state (coordinator leader-state)
  "Updates the leader state in `warp-state-manager`.
This is typically called by a candidate becoming a leader, or a leader
sending heartbeats to assert its leadership. It performs an atomic
update to ensure consistency in the shared distributed state.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `leader-state` (plist): The new leader state to write. Must be a plist
  containing at least `:id` and `:term`.

Returns:
- (loom-promise): A promise resolving to `t` on success of the state
  manager update.

Side Effects:
- Updates the `warp-state-manager`."
  (warp:state-manager-update
   (warp-coordinator-state-manager coordinator)
   `(:coordinator :leader-state)
   leader-state))

(defun warp-coordinator--update-term (coordinator new-term &optional leader-id)
  "Updates the coordinator's current term and potentially its leader.
If `new-term` is strictly greater than the `coordinator`'s
`current-term`, the coordinator transitions to follower role, clears its
vote for the old term, and updates its term and known leader ID. This is a
critical Raft rule for ensuring term monotonicity and leader-follower
consistency across the cluster.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `new-term` (integer): The new term number to transition to.
- `leader-id` (string or nil, optional): The ID of the new leader,
  if known (e.g., from an AppendEntries RPC).

Returns:
- `nil`.

Side Effects:
- Modifies `coordinator`'s `current-term`, `role`, `voted-for`, and
  `leader-id` fields.
- Cancels `election-timer` if it's active, as a higher term implies
  a new leader or election, resetting the timer implicitly."
  (when (> new-term (warp-coordinator-current-term coordinator))
    (warp:log! :info (warp-coordinator-id coordinator)
                (format "Term updated: %d -> %d. Transitioning to follower."
                        (warp-coordinator-current-term coordinator) new-term))
    (setf (warp-coordinator-current-term coordinator) new-term)
    (setf (warp-coordinator-role coordinator) :follower)
    ;; Clear vote for old term, as per Raft rules.
    (setf (warp-coordinator-voted-for coordinator) nil)
    (setf (warp-coordinator-leader-id coordinator) leader-id)
    ;; Cancel election timer to stop current election/waiting period.
    (when (warp-coordinator-election-timer coordinator)
      (cancel-timer (warp-coordinator-election-timer coordinator))
      (setf (warp-coordinator-election-timer coordinator) nil))))

(defun warp-coordinator--log-append (coordinator entries)
  "Appends new log `entries` to the coordinator's in-memory log.
(Simplified: this is not a persistent or replicated log in this basic
implementation, merely an in-memory list for tracking).

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `entries` (list): A list of `warp-coordinator-log-entry` objects to append.

Returns:
- `nil`.

Side Effects:
- Modifies `coordinator`'s `log` list by appending `entries`."
  (cl-loop for entry in entries do
            (setf (warp-coordinator-log coordinator)
                  (append (warp-coordinator-log coordinator) (list entry))))
  (warp:log! :trace (warp-coordinator-id coordinator)
              (format "Appended %d log entries. New log length: %d"
                      (length entries) (length (warp-coordinator-log coordinator)))))

(defun warp-coordinator--get-last-log-info (coordinator)
  "Retrieves the term and index of the last entry in the coordinator's
local in-memory log. This information is crucial for Raft's log matching
consistency check during leader election (`RequestVote` RPCs) and log
replication (`AppendEntries` RPCs).

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns:
- (plist): A plist `(:index <int> :term <int>)` representing the index
  and term of the last log entry. Returns `(:index 0 :term 0)` if the
  log is empty, which represents the initial state before any entries."
  (if-let (last-entry (car (last (warp-coordinator-log coordinator))))
      `(:index ,(warp-coordinator-log-entry-index last-entry)
        :term ,(warp-coordinator-log-entry-term last-entry))
    `(:index 0 :term 0))) ; Initial state for empty log

;;----------------------------------------------------------------------
;;; Timers and Role Management
;;----------------------------------------------------------------------

(defun warp-coordinator--reset-election-timer (coordinator)
  "Resets the election timer with a random timeout.
This function is called by followers and candidates. If no valid
heartbeats are received from a leader, or no votes from other members
(for candidates), this timer will expire, triggering a new election.
The random timeout (jitter) helps prevent all nodes from timing out at
once and initiating elections simultaneously, reducing network contention.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns:
- `nil`.

Side Effects:
- Cancels any existing `election-timer` associated with the coordinator.
- Schedules a new `election-timer` to call
  `warp-coordinator--start-election` after a randomized delay."
  (when (warp-coordinator-election-timer coordinator)
    (cancel-timer (warp-coordinator-election-timer coordinator)))
  (let* ((config (warp-coordinator-config coordinator))
         (min-ms (warp-coordinator-config-election-timeout-min config))
         (max-ms (warp-coordinator-config-election-timeout-max config))
         ;; Add random jitter to election timeout to prevent split-brain
         ;; elections where multiple candidates repeatedly time out together.
         (timeout-ms (+ min-ms (random (- max-ms min-ms))))
         (timeout-s (/ (float timeout-ms) 1000.0)))
    (setf (warp-coordinator-election-timer coordinator)
          (run-at-time timeout-s nil
                       #'warp-coordinator--start-election coordinator))
    (warp:log! :debug (warp-coordinator-id coordinator)
                (format "Election timer reset to %.3fs." timeout-s))))

(defun warp-coordinator--process-vote-response (coordinator
                                                response
                                                current-term
                                                vote-counter-lock)
  "Processes a single vote response received during an election.
This private helper function updates the local `votes-received` count
and ensures the coordinator steps down immediately if a higher term is
discovered in the response (a critical Raft safety rule). It's designed
to be called asynchronously for each `RequestVote` RPC response.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `response` (warp-rpc-command): The RPC response containing vote
  information (specifically, the `warp-protocol-coordinator-request-vote-response-payload`).
- `current-term` (integer): The term of the election this node is
  currently participating in. Used to validate the response.
- `vote-counter-lock` (loom-lock): A mutex to protect the shared
  `votes-received` counter, ensuring atomic updates in a concurrent
  environment.

Returns:
- `nil`.

Side Effects:
- May increment internal `votes-received` count (in caller's lexical scope).
- May update `coordinator`'s term and role if a higher term is seen,
  causing it to step down to `:follower`."
  (let* ((response-payload (warp-rpc-command-args response))
         (response-term
          (warp-protocol-coordinator-request-vote-response-payload-term
           response-payload))
         (granted-p
          (warp-protocol-coordinator-request-vote-response-payload-vote-granted-p
           response-payload)))
    (loom:with-mutex! vote-counter-lock
      ;; Only process the response if this node is still a candidate
      ;; for the *same* term as the election it initiated. This avoids
      ;; stale responses affecting new elections.
      (when (eq (warp-coordinator-role coordinator) :candidate)
        ;; Raft Rule: If a higher term is seen, immediately step down.
        ;; This ensures that a node always defers to a more up-to-date cluster state.
        (when (> response-term current-term)
          (warp:log! :info (warp-coordinator-id coordinator)
                      (format "Received vote response with higher term %d.
                               Stepping down." response-term))
          (warp-coordinator--update-term
           coordinator response-term nil))
        ;; If the vote is granted and for the current term, count it.
        (when (and granted-p
                   (= response-term current-term))
          ;; `votes-received` is expected to be a variable in the outer
          ;; `start-election`'s lexical scope, incremented here.
          (cl-incf votes-received))))))

(defun warp-coordinator--send-vote-request (coordinator
                                            member-id
                                            current-term
                                            last-log-info
                                            rpc-timeout)
  "Sends a single `:coordinator-request-vote` RPC to a peer coordinator.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance
  initiating the vote request.
- `member-id` (string): The ID of the peer to send the RPC to.
- `current-term` (integer): The current term of the candidate.
- `last-log-info` (plist): The last log index and term of the candidate's
  log, used by Raft to determine vote eligibility. `(:index <int> :term <int>)`.
- `rpc-timeout` (number): Timeout for this specific RPC call.

Returns:
- (loom-promise): A promise that resolves to the RPC response (a
  `warp-rpc-command` carrying the `RequestVoteResponse` payload) on
  success, or rejects on failure. This promise's outcome is then handled
  by `loom:all-settled` in the calling function (`start-election`)."
  (let* ((cm (warp-coordinator-connection-manager coordinator))
         (rpc-system (warp-coordinator-rpc-system coordinator))
         (conn (warp:connection-manager-get-connection cm))
         ;; Use the component system's ID as the origin for RPC, for proper
         ;; cross-process promise resolution.
         (origin-instance-id (warp-coordinator-component-system-id coordinator))
         (command-payload
          (warp-protocol-make-command
           :coordinator-request-vote
           :candidate-id (warp-coordinator-id coordinator)
           :term current-term
           :last-log-index (plist-get last-log-info :index)
           :last-log-term (plist-get last-log-info :term))))
    (unless conn
      (warp:log! :warn (warp-coordinator-id coordinator)
                  (format "No active connection to %s for vote request."
                          member-id))
      (loom:rejected! (warp:error! :type 'warp-coordinator-error
                                   :message "No connection to send vote request.")))
    (braid! (warp:protocol-coordinator-request-vote
             rpc-system conn (warp-coordinator-id coordinator) ; Sender is self ID
             member-id ; Recipient is peer ID
             (warp-protocol-coordinator-request-vote-payload-candidate-id
              (warp-rpc-command-args command-payload))
             (warp-protocol-coordinator-request-vote-payload-term
              (warp-rpc-command-args command-payload))
             (warp-protocol-coordinator-request-vote-payload-last-log-index
              (warp-rpc-command-args command-payload))
             (warp-protocol-coordinator-request-vote-payload-last-log-term
              (warp-rpc-command-args command-payload))
             :timeout rpc-timeout
             :origin-instance-id origin-instance-id)
      (:then (lambda (response) response)) ; Just pass the response through
      (:catch (lambda (err)
                (warp:log! :warn (warp-coordinator-id coordinator)
                            (format "Vote request to %s failed: %S"
                                    member-id err))
                (loom:rejected! err))))))

(defun warp-coordinator--start-election (coordinator)
  "Initiates a new leader election.
This function is the core of the leader election process. The coordinator
increments its current term, transitions to the `:candidate` role, votes
for itself, and then requests votes from all other cluster members via
RPCs. It waits for all vote responses (or timeouts) to determine if it
has achieved a majority (quorum).

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns:
- (loom-promise): A promise that resolves when the election process
  has concluded (either by becoming leader, stepping down, or timing out).

Side Effects:
- Increments `current-term` and updates `role` to `:candidate`.
- Sets `voted-for` to self, `leader-id` to `nil`.
- Resets `election-timer`.
- Sends `:coordinator-request-vote` RPCs to other cluster members."
  (warp:log! :info (warp-coordinator-id coordinator) "Starting election.")
  (cl-incf (warp-coordinator-current-term coordinator))
  (setf (warp-coordinator-role coordinator) :candidate)
  (setf (warp-coordinator-voted-for coordinator)
        (warp-coordinator-id coordinator))
  (setf (warp-coordinator-leader-id coordinator) nil) ; No leader known during election
  (warp-coordinator--reset-election-timer coordinator) ; Reset timer for this election

  (let* ((config (warp-coordinator-config coordinator))
         (members (warp-coordinator-config-cluster-members config))
         (current-term (warp-coordinator-current-term coordinator))
         (last-log-info (warp-coordinator--get-last-log-info coordinator))
         ;; Initialize vote count: 1 for self-vote.
         (votes-received 1)
         (total-members (1+ (length members))) ; Include self in total
         (quorum (1+ (floor total-members 2))) ; Majority rule
         (rpc-timeout (warp-coordinator-config-rpc-timeout config))
         ;; Mutex to protect `votes-received` counter for concurrent updates
         ;; from RPC response handlers.
         (vote-counter-lock (loom:make-lock "vote-counter-lock"))

         ;; Create a list of promises for each vote request RPC to peers.
         (vote-request-promises
          (cl-loop for member-id in members
                   collect (warp-coordinator--send-vote-request
                            coordinator member-id current-term
                            last-log-info rpc-timeout))))

    (warp:log! :debug (warp-coordinator-id coordinator)
                (format "Requesting votes for term %d (quorum needed: %d)."
                        current-term quorum))

    ;; Use `loom:all-settled` to wait for all vote requests to complete,
    ;; regardless of individual success or failure (e.g., peer is down).
    ;; This prevents a single failed RPC from prematurely stopping the
    ;; entire election attempt.
    (braid! (loom:all-settled vote-request-promises)
      (:then (lambda (outcomes)
               ;; Process each individual vote response outcome.
               (dolist (outcome outcomes)
                 (when (eq (plist-get outcome :status) 'fulfilled)
                   ;; Only process fulfilled promises (successful RPC responses).
                   ;; The actual logic for updating vote count and stepping down
                   ;; is in `warp-coordinator--process-vote-response`.
                   (warp-coordinator--process-vote-response
                    coordinator (plist-get outcome :value) current-term
                    vote-counter-lock)))
               ;; After all responses are processed, check if this candidate
               ;; has received enough votes to become leader.
               (loom:with-mutex! vote-counter-lock
                 (when (and (eq (warp-coordinator-role coordinator) :candidate)
                            (>= votes-received quorum)
                            ;; Crucially, ensure we are still in the same term
                            ;; we started the election in. If a higher term
                            ;; appeared during the election (e.g., from an
                            ;; `AppendEntries` RPC or another `RequestVote` response),
                            ;; we would have already stepped down via
                            ;; `warp-coordinator--update-term`.
                            (= (warp-coordinator-current-term coordinator)
                               current-term))
                   (loom:await (warp-coordinator--become-leader coordinator)))))))))

(defun warp-coordinator--process-heartbeat-response (coordinator
                                                      response
                                                      current-term
                                                      leader-id)
  "Processes a single heartbeat response from a follower.
This function is called by the leader after sending an `AppendEntries` RPC
(even empty heartbeats). Its primary role is to ensure the leader steps
down immediately if it receives a response from a follower indicating a
higher term. This is a critical Raft safety rule, as it means a new
leader has been elected, and the current leader is now stale.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance
  (currently acting as leader).
- `response` (warp-rpc-command): The RPC response (carrying the
  `AppendEntriesResponse` payload) from a follower.
- `current-term` (integer): The term of the leader (the current node).
- `leader-id` (string): The ID of the leader (the current node's ID).

Returns:
- `nil`.

Side Effects:
- May update `coordinator`'s `current-term` and `role` (to `:follower`)
  if a higher term is observed."
  (let* ((response-payload (warp-rpc-command-args response))
         (response-term
          (warp-protocol-coordinator-append-entries-response-payload-term
           response-payload))
         (sender-id (plist-get (warp-rpc-command-metadata response) :sender-id)))
    ;; Raft Rule: If leader receives response with higher term, it must
    ;; immediately step down. This handles cases where a new leader has
    ;; been elected while this node was still considering itself leader.
    (when (> response-term current-term)
      (warp:log! :info leader-id
                  (format "Received higher term %d from %s. Stepping down."
                          response-term (or sender-id "unknown-peer")))
      (warp-coordinator--update-term
       coordinator response-term nil))))

(defun warp-coordinator--send-single-heartbeat (coordinator
                                                member-id
                                                current-term
                                                leader-id
                                                rpc-timeout)
  "Sends a single heartbeat (`AppendEntries` RPC with no new log entries)
to a specific peer coordinator.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance
  (the current leader) sending the heartbeat.
- `member-id` (string): The ID of the peer to send the heartbeat to.
- `current-term` (integer): The current term of the leader.
- `leader-id` (string): The ID of the leader (the current node's ID).
- `rpc-timeout` (number): Timeout for the RPC.

Returns:
- (loom-promise): A promise that resolves on successful RPC response,
  or rejects on failure. The outcome is processed by `loom:all-settled`
  in `warp-coordinator--send-heartbeats`. This promise returns `nil`
  on internal errors to not halt `loom:all-settled` immediately.

Side Effects:
- Uses `warp-connection-manager` to get a connection.
- Sends an `AppendEntries` RPC via `warp-rpc-request`."
  (let* ((cm (warp-coordinator-connection-manager coordinator))
         (rpc-system (warp-coordinator-rpc-system coordinator))
         (conn (warp:connection-manager-get-connection cm))
         (origin-instance-id (warp-coordinator-component-system-id coordinator))
         (command-payload
          (warp-protocol-make-command
           :coordinator-append-entries
           :term current-term
           :leader-id leader-id
           :prev-log-index 0 ; Simplified for heartbeats (no log entries)
           :prev-log-term 0  ; Simplified for heartbeats
           :entries nil      ; No new log entries for heartbeat
           :leader-commit 0))) ; Simplified
    (unless conn
      (warp:log! :warn leader-id
                  (format "No connection to %s for heartbeat." member-id))
      (loom:rejected! (warp:error! :type 'warp-coordinator-error
                                   :message "No connection for heartbeat.")))
    (braid! (warp:protocol-coordinator-append-entries
             rpc-system conn leader-id member-id
             (warp-protocol-coordinator-append-entries-payload-term
              (warp-rpc-command-args command-payload))
             (warp-protocol-coordinator-append-entries-payload-leader-id
              (warp-rpc-command-args command-payload))
             (warp-protocol-coordinator-append-entries-payload-prev-log-index
              (warp-rpc-command-args command-payload))
             (warp-protocol-coordinator-append-entries-payload-prev-log-term
              (warp-rpc-command-args command-payload))
             (warp-protocol-coordinator-append-entries-payload-entries
              (warp-rpc-command-args command-payload))
             (warp-protocol-coordinator-append-entries-payload-leader-commit
              (warp-rpc-command-args command-payload))
             :timeout rpc-timeout
             :origin-instance-id origin-instance-id) ; Pass origin ID
      (:then (lambda (response)
               (warp-coordinator--process-heartbeat-response
                coordinator response current-term leader-id)))
      (:catch (lambda (err)
                (warp:log! :warn leader-id
                            (format "Heartbeat to %s failed: %S"
                                    member-id err))
                (loom:rejected! err))))))

(defun warp-coordinator--send-heartbeats (coordinator)
  "Sends periodic heartbeats (`AppendEntries` RPCs with no new entries)
to all other cluster members if this node is currently the leader.
Heartbeats are a core part of the Raft consensus algorithm, used by
the leader to maintain its leadership and prevent followers from timing
out and starting new elections.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns:
- (loom-promise): A promise that resolves when all heartbeats are sent
  (or attempted to be sent, even if some individual sends fail).

Side Effects:
- Sends `:coordinator-append-entries` RPCs to other members.
- Logs heartbeat sending."
  (when (eq (warp-coordinator-role coordinator) :leader)
    (let* ((config (warp-coordinator-config coordinator))
           (members (warp-coordinator-config-cluster-members config))
           (current-term (warp-coordinator-current-term coordinator))
           (leader-id (warp-coordinator-id coordinator))
           (rpc-timeout (warp-coordinator-config-rpc-timeout config))
           (heartbeat-promises nil))
      (warp:log! :trace leader-id
                  (format "Sending heartbeats (term %d)." current-term))
      (dolist (member-id members)
        (push (warp-coordinator--send-single-heartbeat
               coordinator member-id current-term leader-id rpc-timeout)
              heartbeat-promises))
      ;; Use `loom:all-settled` to ensure all heartbeats are attempted,
      ;; and the promise resolves even if some individual sends fail
      ;; (e.g., a follower is temporarily unreachable). This is non-blocking.
      (loom:all-settled heartbeat-promises))))

(defun warp-coordinator--become-leader (coordinator)
  "Transitions the coordinator's role to leader.
This function is called when a candidate successfully wins an election.
It cancels the election timer, updates the coordinator's role and
leader ID, initializes log replication indices (`next-index`,
`match-index`) for all followers, and starts sending periodic
heartbeats. It also updates the shared leader state in
`warp-state-manager` and emits a `leader-elected` event.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns:
- (loom-promise): A promise that resolves when the transition to leader
  is complete and initial heartbeats are sent.

Side Effects:
- Sets `role` to `:leader`, `leader-id` to self.
- Cancels `election-timer` and any existing `heartbeat-timer`.
- Initializes `next-index` and `match-index` for all followers.
- Starts a new `heartbeat-timer` for periodic heartbeats.
- Updates leader state in `warp-state-manager`.
- Emits `:leader-elected` event."
  (warp:log! :info (warp-coordinator-id coordinator)
              (format "Becoming leader for term %d."
                      (warp-coordinator-current-term coordinator)))
  (setf (warp-coordinator-role coordinator) :leader)
  (setf (warp-coordinator-leader-id coordinator)
        (warp-coordinator-id coordinator))
  ;; Ensure all old timers are stopped as a leader has different timers.
  (when (warp-coordinator-election-timer coordinator)
    (cancel-timer (warp-coordinator-election-timer coordinator))
    (setf (warp-coordinator-election-timer coordinator) nil))
  (when (warp-coordinator-heartbeat-timer coordinator)
    (cancel-timer (warp-coordinator-heartbeat-timer coordinator))
    (setf (warp-coordinator-heartbeat-timer coordinator) nil))

  ;; Initialize nextIndex and matchIndex for all followers (Raft concept).
  ;; `nextIndex` is the index of the next log entry to send to that follower.
  ;; `matchIndex` is the highest log entry index known to be replicated on
  ;; that follower.
  (let* ((config (warp-coordinator-config coordinator))
         (members (warp-coordinator-config-cluster-members config))
         (last-log-index (plist-get
                          (warp-coordinator--get-last-log-info coordinator)
                          :index)))
    (dolist (member-id members)
      ;; Leader typically starts by assuming followers are behind its whole log.
      (puthash member-id (1+ last-log-index)
               (warp-coordinator-next-index coordinator))
      ;; Match index is initially 0, meaning no log entries matched yet.
      (puthash member-id 0 (warp-coordinator-match-index coordinator))))

  ;; Start periodic heartbeats. The interval is converted from ms to seconds.
  (setf (warp-coordinator-heartbeat-timer coordinator)
        (run-at-time (/ (warp-coordinator-config-heartbeat-interval
                         (warp-coordinator-config coordinator)) 1000.0)
                     (/ (warp-coordinator-config-heartbeat-interval
                         (warp-coordinator-config coordinator)) 1000.0)
                     #'warp-coordinator--send-heartbeats coordinator))

  ;; Update the shared leader state in `warp-state-manager`.
  (loom:await (warp-coordinator--set-leader-state
               coordinator `(:id ,(warp-coordinator-id coordinator)
                             :term ,(warp-coordinator-current-term coordinator)
                             :last-heartbeat ,(float-time))))

  ;; Emit leader elected event for external listeners (e.g., metrics, logs).
  (warp:emit-event
   (warp-coordinator-event-system coordinator)
   :leader-elected
   `(:leader-id ,(warp-coordinator-id coordinator)
     :term ,(warp-coordinator-current-term coordinator)))

  ;; Send an immediate heartbeat to quickly establish leadership with followers.
  (loom:await (warp-coordinator--send-heartbeats coordinator)))

;;----------------------------------------------------------------------
;;; RPC Handlers (Received from other Coordinator Nodes)
;;----------------------------------------------------------------------

(defun warp-coordinator-handle-request-vote (coordinator command context)
  "Handles incoming `:coordinator-request-vote` RPCs.
This is a core Raft handler. A coordinator (usually a follower) responds
to a `RequestVote` RPC from a candidate. It grants its vote if the
candidate's term is higher or equal to its own, and its log is at least
as up-to-date. It also updates its term and steps down if the candidate's
term is higher.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `command` (warp-rpc-command): The incoming RPC command, containing the
  `RequestVote` payload.
- `context` (plist): The RPC context from the router, providing sender ID.

Returns:
- (loom-promise): A promise that resolves with a `warp-protocol-coordinator-request-vote-response-payload`.

Side Effects:
- May update `coordinator`'s `current-term`, `role`, `voted-for`.
- May reset the `election-timer`."
  (let* ((args (warp-rpc-command-args command))
         (candidate-id (warp-protocol-coordinator-request-vote-payload-candidate-id
                        args))
         (candidate-term (warp-protocol-coordinator-request-vote-payload-term
                           args))
         (last-log-index (warp-protocol-coordinator-request-vote-payload-last-log-index
                           args))
         (last-log-term (warp-protocol-coordinator-request-vote-payload-last-log-term
                          args))
         (current-term (warp-coordinator-current-term coordinator))
         (voted-for (warp-coordinator-voted-for coordinator))
         (log-info (warp-coordinator--get-last-log-info coordinator))
         (vote-granted-p nil)
         (error-msg nil))

    (warp:log! :debug (warp-coordinator-id coordinator)
                (format "Received RequestVote from %s for term %d. My term: %d."
                        candidate-id candidate-term current-term))

    (cond
      ;; Raft Rule 1: If RPC request or response contains term T > currentTerm,
      ;; then set currentTerm = T, convert to follower.
      ((> candidate-term current-term)
       (warp-coordinator--update-term coordinator candidate-term nil))

      ;; Raft Rule 2: If `candidate-term < current-term`, deny vote.
      ((< candidate-term current-term)
       (setq error-msg (format "Candidate term %d is less than my term %d."
                               candidate-term current-term)))

      ;; Raft Rule 3: If `candidate-term == current-term`
      (t
       ;; If already voted for another candidate in this term, deny.
       (when (and voted-for (not (string= voted-for candidate-id)))
         (setq error-msg (format "Already voted for %s in term %d."
                                 voted-for current-term)))

       ;; Raft Rule 4: Candidate's log must be at least as up-to-date as receiver's.
       ;; Log is more up-to-date if (lastTerm > myLastTerm) or (lastTerm == myLastTerm and lastIndex >= myLastIndex).
       (unless (or (> last-log-term (plist-get log-info :term))
                   (and (= last-log-term (plist-get log-info :term))
                        (>= last-log-index (plist-get log-info :index))))
         (setq error-msg "Candidate's log is not as up-to-date as mine."))

       ;; If all checks pass, grant vote.
       (unless error-msg
         (setf (warp-coordinator-voted-for coordinator) candidate-id)
         (warp-coordinator--reset-election-timer coordinator) ; Reset timer on valid vote
         (setq vote-granted-p t)
         (warp:log! :info (warp-coordinator-id coordinator)
                     (format "Granted vote to %s for term %d."
                             candidate-id candidate-term))))))

    (loom:resolved! (warp-protocol-make-command
                     :coordinator-request-vote-response
                     :term (warp-coordinator-current-term coordinator)
                     :vote-granted-p vote-granted-p
                     :error error-msg))))

(defun warp-coordinator-handle-append-entries (coordinator command context)
  "Handles incoming `:coordinator-append-entries` RPCs (Leader Heartbeats).
This is a core Raft handler. Followers (and candidates) respond to
`AppendEntries` RPCs from the current leader. This RPC is used for both
heartbeats (empty `entries`) and log replication.
A follower/candidate must:
1. Update its term and step down if the leader's term is higher.
2. Reset its election timer upon receiving a valid heartbeat.
3. Potentially append new log entries (simplified).

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `command` (warp-rpc-command): The incoming RPC command, containing the
  `AppendEntries` payload.
- `context` (plist): The RPC context from the router.

Returns:
- (loom-promise): A promise that resolves with a `warp-protocol-coordinator-append-entries-response-payload`.

Side Effects:
- May update `coordinator`'s `current-term`, `role`, `leader-id`, and `log`.
- Resets the `election-timer`."
  (let* ((args (warp-rpc-command-args command))
         (leader-term (warp-protocol-coordinator-append-entries-payload-term
                       args))
         (leader-id (warp-protocol-coordinator-append-entries-payload-leader-id
                      args))
         (prev-log-index (warp-protocol-coordinator-append-entries-payload-prev-log-index
                           args))
         (prev-log-term (warp-protocol-coordinator-append-entries-payload-prev-log-term
                          args))
         (entries (warp-protocol-coordinator-append-entries-payload-entries
                   args))
         (leader-commit (warp-protocol-coordinator-append-entries-payload-leader-commit
                          args))
         (current-term (warp-coordinator-current-term coordinator))
         (success-p nil)
         (error-msg nil))

    (warp:log! :debug (warp-coordinator-id coordinator)
                (format "Received AppendEntries from %s for term %d. My term: %d."
                        leader-id leader-term current-term))

    (cond
      ;; Raft Rule 1: If `leader-term < current-term`, respond `false`.
      ((< leader-term current-term)
       (setq error-msg (format "Leader term %d is less than my term %d."
                               leader-term current-term)))

      ;; Raft Rule 2: If `leader-term >= current-term`, it's a valid leader
      ;; or new term. Update our state and reset election timer.
      (t
       ;; Update our term and step down to follower.
       (warp-coordinator--update-term coordinator leader-term leader-id)
       (warp-coordinator--reset-election-timer coordinator) ; Always reset timer on valid heartbeat

       ;; If this is a heartbeat (no entries), respond true.
       (when (null entries)
         (setq success-p t)
         (warp:log! :trace (warp-coordinator-id coordinator)
                     (format "Heartbeat from %s acknowledged." leader-id)))

       ;; Simplified Log Matching Raft Rule (for demonstration):
       ;; Check if log contains an entry at `prev-log-index` whose term
       ;; matches `prev-log-term`. If not, respond false.
       ;; For full Raft, this is complex log truncation/consistency.
       (let ((my-prev-log-info (warp-coordinator--get-last-log-info coordinator)))
         (unless (and (= prev-log-index (plist-get my-prev-log-info :index))
                      (= prev-log-term (plist-get my-prev-log-info :term)))
           ;; This is a log inconsistency. In full Raft, followers would
           ;; tell the leader what index they last matched. Here, we simplify.
           (setq error-msg "Log inconsistency (simplified check).")
           (setq success-p nil)))

       (when (and (not error-msg) entries)
         ;; If no log inconsistency and entries are present, append them.
         ;; This is a highly simplified log append logic.
         (warp-coordinator--log-append coordinator entries)
         (setq success-p t)
         (warp:log! :info (warp-coordinator-id coordinator)
                     (format "Appended %d entries from leader %s."
                             (length entries) leader-id)))))

    (loom:resolved! (warp-protocol-make-command
                     :coordinator-append-entries-response
                     :term (warp-coordinator-current-term coordinator)
                     :success-p success-p
                     :error error-msg))))

(defun warp-coordinator-handle-acquire-lock (coordinator command context)
  "Handles incoming `:coordinator-acquire-lock` RPCs.
Only the cluster leader processes these requests by performing an atomic
transaction on the `warp-state-manager` to acquire the lock. If the local
node is a follower, it directs the requesting client to the known leader.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `command` (warp-rpc-command): The incoming RPC command. Its `args`
  contain `lock-name`, `holder-id`, and `expiry-time`.
- `context` (plist): The RPC context from the router, providing access
  to the original `warp-rpc-message`.

Returns:
- (loom-promise): A promise resolving with a
  `warp-protocol-coordinator-lock-response-payload` (indicating
  `granted-p`, `leader-id`, and `error` if any).

Side Effects:
- If leader: May modify the lock state in `warp-state-manager` via a
  transaction. Logs lock acquisition/renewal.
- If follower: Updates `coordinator`'s `leader-id` if redirecting."
  (let* ((args (warp-rpc-command-args command))
         (lock-name (warp-protocol-coordinator-lock-request-payload-lock-name
                     args))
         (holder-id (warp-protocol-coordinator-lock-request-payload-holder-id
                     args))
         (expiry-time (warp-protocol-coordinator-lock-request-payload-expiry-time
                       args)))

    (warp:log! :debug (warp-coordinator-id coordinator)
                (format "Received acquire-lock request for '%s' from %s."
                        lock-name holder-id))

    (if (eq (warp-coordinator-role coordinator) :leader)
        ;; If this node is the leader, process the lock acquisition request.
        ;; The core logic is encapsulated in `warp-coordinator--handle-acquire-lock-logic`.
        (braid! (warp-coordinator--handle-acquire-lock-logic
                 coordinator lock-name holder-id expiry-time)
          (:then (lambda (result)
                   (loom:resolved! (warp-protocol-make-command
                                    :coordinator-lock-response
                                    :granted-p (plist-get result :granted-p)
                                    :leader-id (warp-coordinator-id coordinator)
                                    :error (plist-get result :error))))))
      ;; If this node is a follower, it cannot grant the lock.
      ;; Respond with a redirect message, indicating the current leader (if known).
      (let ((current-leader-id (warp-coordinator-leader-id coordinator)))
        (if current-leader-id
            (loom:resolved! (warp-protocol-make-command
                             :coordinator-lock-response
                             :granted-p nil
                             :leader-id current-leader-id
                             :error "Not leader. Redirect to current leader."))
          (loom:resolved! (warp-protocol-make-command
                           :coordinator-lock-response
                           :granted-p nil
                           :leader-id nil
                           :error "Not leader and no leader known.")))))))

(defun warp-coordinator--handle-acquire-lock-logic (coordinator lock-name holder-id expiry-time)
  "Internal helper for processing the core logic of an acquire lock request.
This function performs the atomic check-and-set operation for a
distributed lock within a `warp-state-manager` transaction. This ensures
that lock acquisition is an all-or-nothing operation across the
cluster's shared state, preventing race conditions between concurrent
lock attempts. It handles new acquisitions, re-acquisitions by the same
holder, and expiration of existing locks.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `lock-name` (string): The name of the lock to acquire.
- `holder-id` (string): The ID of the requester (the client trying to acquire).
- `expiry-time` (float): The requested expiry timestamp for the lock.

Returns:
- (loom-promise): A promise that resolves to a plist
  `(:granted-p <boolean> :error <string or nil>)` indicating the outcome.

Side Effects:
- May modify the lock state in `warp-state-manager` via a transaction.
- Logs lock acquisition/renewal."
  (let* ((sm (warp-coordinator-state-manager coordinator))
         (granted-p nil)
         (error-msg nil)
         (current-time (float-time)))
    (braid! (warp:state-manager-transaction
             sm
             (lambda (tx)
               (let* ((current-lock-entry (warp:state-manager-get tx
                                                                  `(:locks ,lock-name)))
                      ;; Check if a lock entry exists and is not marked as deleted (tombstone).
                      (lock-is-held (and current-lock-entry
                                         (not (warp:state-entry-deleted
                                               current-lock-entry))))
                      ;; Get current holder and expiry if lock is held.
                      (lock-data (when lock-is-held
                                   (warp:state-entry-value
                                    current-lock-entry)))
                      (lock-holder (when lock-is-held
                                     (plist-get lock-data :holder-id)))
                      (lock-expired-at (when lock-is-held
                                         (plist-get lock-data :expiry-time))))
                 (cond
                   ;; Case 1: Lock is currently held but has expired.
                   ;; Grant the lock to the new requester, effectively taking over.
                   ((and lock-is-held (>= current-time lock-expired-at))
                    (warp:state-tx-update
                     tx `(:locks ,lock-name)
                     `(:holder-id ,holder-id :expiry-time ,expiry-time))
                    (setq granted-p t)
                    (warp:log! :info (warp-coordinator-id coordinator)
                               (format "Lock '%s' acquired by %s (expired previous holder)."
                                       lock-name holder-id)))
                   ;; Case 2: Lock is already held by the requesting client.
                   ;; This is a re-acquisition or renewal. Update the lease time.
                   ((and lock-is-held (string= lock-holder holder-id))
                    (warp:state-tx-update
                     tx `(:locks ,lock-name)
                     `(:holder-id ,holder-id :expiry-time ,expiry-time))
                    (setq granted-p t)
                    (warp:log! :info (warp-coordinator-id coordinator)
                               (format "Lock '%s' renewed by %s."
                                       lock-name holder-id)))
                   ;; Case 3: Lock is not currently held by anyone.
                   ;; Acquire it for the requester.
                   ((not lock-is-held)
                    (warp:state-tx-update
                     tx `(:locks ,lock-name)
                     `(:holder-id ,holder-id :expiry-time ,expiry-time))
                    (setq granted-p t)
                    (warp:log! :info (warp-coordinator-id coordinator)
                               (format "Lock '%s' acquired by %s."
                                       lock-name holder-id)))
                   ;; Case 4: Lock is held by another client and is not expired.
                   ;; Deny the request.
                   (t
                    (setq error-msg
                          (format "Lock '%s' held by %s (expires %.1f)."
                                  lock-name lock-holder lock-expired-at)))))))
      (:then (lambda (_res)
               `(:granted-p ,granted-p :error ,error-msg)))
      (:catch (lambda (err)
                (warp:log! :error (warp-coordinator-id coordinator)
                            (format "Transaction for lock '%s' failed: %S"
                                    lock-name err))
                `(:granted-p nil :error (format "Transaction error: %S"
                                                 err)))))))

(defun warp-coordinator-handle-release-lock (coordinator command context)
  "Handles incoming `:coordinator-release-lock` RPCs.
Only the cluster leader processes these requests by performing an atomic
transaction on the `warp-state-manager` to release the lock. If the local
node is a follower, it directs the requesting client to the known leader.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `command` (warp-rpc-command): The incoming RPC command. Its `args`
  contain `lock-name` and `holder-id`.
- `context` (plist): The RPC context from the router.

Returns:
- (loom-promise): A promise resolving with a
  `warp-protocol-coordinator-lock-response-payload` (indicating
  `success-p`, `leader-id`, and `error` if any).

Side Effects:
- If leader: May modify the lock state in `warp-state-manager` via a
  transaction. Logs lock release.
- If follower: Updates `coordinator`'s `leader-id` if redirecting."
  (let* ((args (warp-rpc-command-args command))
         (lock-name (warp-protocol-coordinator-lock-request-payload-lock-name
                     args))
         (holder-id (warp-protocol-coordinator-lock-request-payload-holder-id
                     args)))

    (warp:log! :debug (warp-coordinator-id coordinator)
                (format "Received release-lock request for '%s' from %s."
                        lock-name holder-id))

    (if (eq (warp-coordinator-role coordinator) :leader)
        ;; If this node is the leader, process the lock release request.
        (braid! (warp-coordinator--handle-release-lock-logic
                 coordinator lock-name holder-id)
          (:then (lambda (result)
                   (loom:resolved! (warp-protocol-make-command
                                    :coordinator-lock-response
                                    :granted-p (plist-get result :success-p) ; Re-using granted-p as success-p
                                    :leader-id (warp-coordinator-id coordinator)
                                    :error (plist-get result :error))))))
      ;; If this node is a follower, redirect to leader.
      (let ((current-leader-id (warp-coordinator-leader-id coordinator)))
        (if current-leader-id
            (loom:resolved! (warp-protocol-make-command
                             :coordinator-lock-response
                             :granted-p nil
                             :leader-id current-leader-id
                             :error "Not leader. Redirect to current leader."))
          (loom:resolved! (warp-protocol-make-command
                           :coordinator-lock-response
                           :granted-p nil
                           :leader-id nil
                           :error "Not leader and no leader known.")))))))

(defun warp-coordinator--handle-release-lock-logic (coordinator lock-name holder-id)
  "Internal helper for processing the core logic of a release lock request.
This function performs the atomic deletion of a distributed lock within
a `warp-state-manager` transaction, but only if the requesting `holder-id`
is the current owner of the lock. It ensures that locks are released only
by their rightful owner or if they are not held.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `lock-name` (string): The name of the lock to release.
- `holder-id` (string): The ID of the requester (the client trying to release).

Returns:
- (loom-promise): A promise that resolves to a plist
  `(:success-p <boolean> :error <string or nil>)` indicating the outcome.

Side Effects:
- May modify the lock state in `warp-state-manager` via a transaction.
- Logs lock release."
  (let* ((sm (warp-coordinator-state-manager coordinator))
         (success-p nil)
         (error-msg nil))
    (braid! (warp:state-manager-transaction
             sm
             (lambda (tx)
               (let* ((current-lock-entry (warp:state-manager-get tx
                                                                  `(:locks ,lock-name)))
                      ;; Check if a lock entry exists and is not marked as deleted (tombstone).
                      (lock-is-held (and current-lock-entry
                                         (not (warp:state-entry-deleted
                                               current-lock-entry))))
                      (lock-data (when lock-is-held
                                   (warp:state-entry-value
                                    current-lock-entry)))
                      (lock-holder (when lock-is-held
                                     (plist-get lock-data :holder-id)))
                      (lock-expired-at (when lock-is-held
                                         (plist-get lock-data :expiry-time))))
                 (cond
                   ;; Case 1: Lock is held by requester, so release it.
                   ((and lock-is-held (string= lock-holder holder-id))
                    (warp:state-tx-delete tx `(:locks ,lock-name))
                    (setq success-p t)
                    (warp:log! :info (warp-coordinator-id coordinator)
                               (format "Lock '%s' released by %s."
                                       lock-name holder-id)))
                   ;; Case 2: Lock is held by someone else. Deny release.
                   ((and lock-is-held
                         (not (string= lock-holder holder-id)))
                    (setq error-msg
                          (format "Lock '%s' held by %s, not %s."
                                  lock-name lock-holder holder-id)))
                   ;; Case 3: Lock not held by anyone. Treat as successful
                   ;; release (idempotent operation).
                   (t
                    (setq success-p t)
                    (warp:log! :info (warp-coordinator-id coordinator)
                               (format "Lock '%s' not held, treating release
                                        by %s as successful."
                                       lock-name holder-id)))))))
      (:then (lambda (_res)
               `(:success-p ,success-p :error ,error-msg)))
      (:catch (lambda (err)
                (warp:log! :error (warp-coordinator-id coordinator)
                            (format "Transaction for release lock '%s' failed: %S"
                                    lock-name err))
                `(:success-p nil :error (format "Transaction error: %S"
                                                 err)))))))

(defun warp-coordinator-handle-barrier-increment (coordinator command context)
  "Handles incoming `:coordinator-barrier-increment` RPCs.
Only the cluster leader processes these requests by performing an atomic
transaction on the `warp-state-manager` to increment the barrier count.
If the local node is a follower, it directs the requesting client to
the known leader.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `command` (warp-rpc-command): The incoming RPC command. Its `args`
  contain `barrier-name`, `participant-id`, and `total-participants`.
- `context` (plist): The RPC context from the router.

Returns:
- (loom-promise): A promise resolving with a
  `warp-protocol-coordinator-barrier-response-payload` (indicating
  `success-p`, `current-count`, `is-met-p`, and `error` if any).

Side Effects:
- If leader: May modify the barrier state in `warp-state-manager` via a
  transaction. Logs barrier increments and completion.
- If follower: Updates `coordinator`'s `leader-id` if redirecting."
  (let* ((args (warp-rpc-command-args command))
         (barrier-name (warp-protocol-coordinator-barrier-increment-payload-barrier-name
                        args))
         (participant-id (warp-protocol-coordinator-barrier-increment-payload-participant-id
                          args))
         (total-participants (warp-protocol-coordinator-barrier-increment-payload-total-participants
                              args)))

    (warp:log! :debug (warp-coordinator-id coordinator)
                (format "Received barrier increment for '%s' from %s."
                        barrier-name participant-id))

    (if (eq (warp-coordinator-role coordinator) :leader)
        ;; If this node is the leader, process the barrier increment.
        (braid! (warp-coordinator--handle-barrier-increment-logic
                 coordinator barrier-name participant-id total-participants)
          (:then (lambda (result)
                   (loom:resolved! (warp-protocol-make-command
                                    :coordinator-barrier-response
                                    :success-p (plist-get result :success-p)
                                    :current-count (plist-get result :current-count)
                                    :is-met-p (plist-get result :is-met-p)
                                    :error (plist-get result :error))))))
      ;; If this node is a follower, redirect to leader.
      (let ((current-leader-id (warp-coordinator-leader-id coordinator)))
        (if current-leader-id
            (loom:resolved! (warp-protocol-make-command
                             :coordinator-barrier-response
                             :success-p nil
                             :current-count 0
                             :is-met-p nil
                             :error "Not leader. Redirect to current leader."))
          (loom:resolved! (warp-protocol-make-command
                           :coordinator-barrier-response
                           :success-p nil
                           :current-count 0
                           :is-met-p nil
                           :error "Not leader and no leader known.")))))))

(defun warp-coordinator--handle-barrier-increment-logic (coordinator barrier-name
                                                         participant-id total-participants)
  "Internal helper for processing the core logic of a barrier increment.
This function atomically updates the barrier's count and participant list
in `warp-state-manager` via a transaction. It checks if the barrier is
met after the increment. It also handles duplicate participant
registrations by returning success without incrementing if the participant
has already registered.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `barrier-name` (string): The name of the barrier.
- `participant-id` (string): The ID of the participant incrementing the barrier.
- `total-participants` (integer): The required count to meet the barrier.

Returns:
- (loom-promise): A promise that resolves to a plist
  `(:success-p <boolean> :current-count <int> :is-met-p <boolean> :error <string or nil>)`.

Side Effects:
- May modify the barrier state in `warp-state-manager` via a transaction.
- Logs barrier increments and completion."
  (let* ((sm (warp-coordinator-state-manager coordinator))
         (success-p nil)
         (current-count 0)
         (is-met-p nil)
         (error-msg nil))
    (braid! (warp:state-manager-transaction
             sm
             (lambda (tx)
               (let* ((current-barrier-entry
                       (warp:state-manager-get tx `(:barriers ,barrier-name)))
                      (barrier-data (when (and current-barrier-entry
                                               (not (warp:state-entry-deleted
                                                     current-barrier-entry)))
                                      (warp:state-entry-value
                                       current-barrier-entry)))
                      (current-count-in-state (plist-get barrier-data
                                                         :current-count 0))
                      (participants-list (plist-get barrier-data
                                                    :participants-list nil)))

                 (unless (integerp total-participants)
                   (signal (warp:error!
                            :type 'warp-coordinator-barrier-failed
                            :message "Total participants must be an integer.")))

                 ;; Check if this participant has already registered for this barrier.
                 (when (member participant-id participants-list
                               :test #'string=)
                   (setq success-p t)
                   (setq current-count current-count-in-state)
                   (setq is-met-p (>= current-count total-participants))
                   (setq error-msg
                         "Participant already registered for this barrier."))

                 (unless error-msg ; Only proceed if not already registered
                   (cl-incf current-count-in-state)
                   (setq participants-list
                         (cons participant-id participants-list))

                   (warp:state-tx-update
                    tx `(:barriers ,barrier-name)
                    `(:current-count ,current-count-in-state
                      :total-participants ,total-participants
                      :participants-list ,participants-list
                      :last-update ,(float-time)))
                   (setq success-p t)
                   (setq current-count current-count-in-state)
                   (setq is-met-p (>= current-count total-participants))
                   (when is-met-p
                     (warp:log! :info (warp-coordinator-id coordinator)
                                (format "Barrier '%s' met with %d participants."
                                        barrier-name current-count)))))))
      (:then (lambda (_res)
               `(:success-p ,success-p :current-count ,current-count
                 :is-met-p ,is-met-p :error ,error-msg)))
      (:catch (lambda (err)
                (warp:log! :error (warp-coordinator-id coordinator)
                            (format "Transaction for barrier '%s' failed: %S"
                                    barrier-name err))
                `(:success-p nil :current-count ,current-count
                  :is-met-p nil :error (format "Transaction error: %S"
                                                err)))))))

(defun warp-coordinator-handle-propose-change (coordinator command context)
  "Handles incoming `:coordinator-propose-change` RPCs.
Only the cluster leader processes these requests. It attempts to apply the
proposed state change (a key-value update) using `warp-state-manager`'s
update function. This is a simplified consensus model; it doesn't involve
full Raft log replication or quorum voting for each change, but rather
assumes the leader's direct manipulation of the shared state manager
reflects its consensus.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `command` (warp-rpc-command): The incoming RPC command. Its `args`
  contain the `key` (path) and `value` to be updated.
- `context` (plist): The RPC context from the router.

Returns:
- (loom-promise): A promise resolving with a
  `warp-protocol-coordinator-propose-change-response-payload` (indicating
  `success-p` and `error` if any).

Side Effects:
- If leader: Calls `warp:state-manager-update` to modify the shared state.
  Logs the change application.
- If follower: Updates `coordinator`'s `leader-id` if redirecting."
  (let* ((args (warp-rpc-command-args command))
         (key (warp-protocol-coordinator-propose-change-payload-key args))
         (value (warp-protocol-coordinator-propose-change-payload-value args))
         (sm (warp-coordinator-state-manager coordinator))
         (success-p nil)
         (error-msg nil))

    (warp:log! :debug (warp-coordinator-id coordinator)
                (format "Received propose-change request for key '%S'." key))

    (if (eq (warp-coordinator-role coordinator) :leader)
        ;; If this node is the leader, apply the state change directly.
        ;; This is the simplified "consensus" step for the leader.
        (braid! (warp:state-manager-update sm key value)
          (:then (lambda (_res)
                   (setq success-p t)
                   (warp:log! :info (warp-coordinator-id coordinator)
                               (format "Proposed change for key '%S' applied by leader."
                                       key))
                   (loom:resolved! (warp-protocol-make-command
                                    :coordinator-propose-change-response
                                    :success-p success-p
                                    :error error-msg))))
          (:catch (lambda (err)
                    (setq error-msg (format "Failed to apply change: %S" err))
                    (warp:log! :error (warp-coordinator-id coordinator)
                               (format "Failed to apply proposed change for key '%S': %S"
                                       key err))
                    (loom:resolved! (warp-protocol-make-command
                                     :coordinator-propose-change-response
                                     :success-p nil
                                     :error error-msg)))))
      ;; If not leader, redirect.
      (let ((current-leader-id (warp-coordinator-leader-id coordinator)))
        (if current-leader-id
            (loom:resolved! (warp-protocol-make-command
                             :coordinator-propose-change-response
                             :success-p nil
                             :error "Not leader. Redirect to current leader."))
          (loom:resolved! (warp-protocol-make-command
                           :coordinator-propose-change-response
                           :success-p nil
                           :error "Not leader and no leader known.")))))))

;;----------------------------------------------------------------------
;;; Lifecycle Management
;;----------------------------------------------------------------------

(defun warp-coordinator--initialize-rpc-handlers (coordinator)
  "Registers all RPC handlers that this coordinator node will respond to.
This function is called during the coordinator's startup lifecycle.
It uses `warp:defrpc-handlers` to declaratively connect the RPC command
names (as defined in `warp-protocol`) to their respective handling functions
within this module. These handlers will be dispatched by the
`command-router` component.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns:
- `nil`.

Side Effects:
- Registers handler functions on the `command-router` associated with
  this `coordinator`."
  (warp:log! :debug (warp-coordinator-id coordinator)
              "Registering RPC handlers.")
  (warp:defrpc-handlers (warp-coordinator-command-router coordinator)
    (:coordinator-request-vote . (lambda (command context)
                                   (warp-coordinator-handle-request-vote
                                    coordinator command context)))
    (:coordinator-append-entries . (lambda (command context)
                                     (warp-coordinator-handle-append-entries
                                      coordinator command context)))
    (:coordinator-acquire-lock . (lambda (command context)
                                   (warp-coordinator-handle-acquire-lock
                                    coordinator command context)))
    (:coordinator-release-lock . (lambda (command context)
                                   (warp-coordinator-handle-release-lock
                                    coordinator command context)))
    (:coordinator-barrier-increment . (lambda (command context)
                                        (warp-coordinator-handle-barrier-increment
                                         coordinator command context)))
    (:coordinator-propose-change . (lambda (command context)
                                      (warp-coordinator-handle-propose-change
                                       coordinator command context)))))

(defun warp-coordinator--start-timers (coordinator)
  "Starts the necessary timers for coordinator operation.
This includes the initial election timer for followers. Leaders will
start their own heartbeat timers upon election.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns:
- `nil`.

Side Effects:
- Schedules the initial `election-timer` via `warp-coordinator--reset-election-timer`."
  (warp:log! :debug (warp-coordinator-id coordinator) "Starting timers.")
  (warp-coordinator--reset-election-timer coordinator))

(defun warp-coordinator--connect-to-peers (coordinator)
  "Establishes connections to other coordinator nodes in the cluster.
This ensures the coordinator can send RPCs for leader election and
state propagation. It uses the `connection-manager` component to handle
the transport, setting up endpoints for all `cluster-members` defined
in the coordinator's configuration.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns:
- (loom-promise): A promise that resolves when connections are
  established (or at least attempted by the Connection Manager).

Side Effects:
- Calls `warp:connection-manager-add-endpoint` for each peer.
- Calls `warp:connection-manager-connect` to start the connection
  manager's internal connection process."
  (let* ((config (warp-coordinator-config coordinator))
         (members (warp-coordinator-config-cluster-members config))
         (cm (warp-coordinator-connection-manager coordinator)))
    (warp:log! :info (warp-coordinator-id coordinator)
                (format "Connecting to %d coordinator peers via Connection Manager."
                        (length members)))
    ;; Add all cluster members as endpoints to the connection manager.
    (dolist (member-address members)
      (warp:connection-manager-add-endpoint cm member-address))
    ;; The `warp:connection-manager-connect` with no arguments starts the
    ;; connection manager's process of establishing and maintaining
    ;; connections to all its configured endpoints. It returns a promise that
    ;; resolves when the CM is actively trying to connect. Individual peer RPCs
    ;; will then leverage the CM's established connections.
    (loom:await (warp:connection-manager-connect cm))))

(defun warp-coordinator--stop-timers (coordinator)
  "Stops all active timers for the coordinator instance.
This is called during shutdown to prevent lingering background tasks
(like election timeouts or heartbeat sends) and to gracefully release
associated timer resources.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns:
- `nil`.

Side Effects:
- Cancels `election-timer` and `heartbeat-timer` if they are active."
  (when (warp-coordinator-election-timer coordinator)
    (cancel-timer (warp-coordinator-election-timer coordinator))
    (setf (warp-coordinator-election-timer coordinator) nil))
  (when (warp-coordinator-heartbeat-timer coordinator)
    (cancel-timer (warp-coordinator-heartbeat-timer coordinator))
    (setf (warp-coordinator-heartbeat-timer coordinator) nil))
  (warp:log! :debug (warp-coordinator-id coordinator) "All timers stopped."))

(defun warp-coordinator--disconnect-from-peers (coordinator)
  "Disconnects from all peer coordinator nodes.
This function is called during shutdown to gracefully close network
connections and release associated resources.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns:
- (loom-promise): A promise resolving when disconnections are complete.

Side Effects:
- Calls `warp:connection-manager-shutdown` to close all active connections."
  (warp:log! :info (warp-coordinator-id coordinator) "Disconnecting from peers.")
  ;; `warp:connection-manager-shutdown` closes all active connections.
  (loom:await (warp:connection-manager-shutdown
               (warp-coordinator-connection-manager coordinator))))

(defun warp-coordinator--send-client-rpc (coordinator target-id command-payload)
  "Helper to send an RPC request from a client (this coordinator instance
acting as a client) to a target coordinator (likely the leader).
This function handles getting the connection and making the RPC call with
the correct sender/recipient IDs and timeout.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance
  acting as the client.
- `target-id` (string): The ID of the coordinator to send the RPC to.
- `command-payload` (warp-protocol-command-payload): The specific
  command payload to send (e.g., lock request, barrier increment).

Returns:
- (loom-promise): A promise that resolves to the `warp-rpc-command`
  (containing the response payload) from the target, or rejects if the
  RPC fails (e.g., no connection, timeout, or remote error).

Signals:
- `warp-coordinator-error`: If no active connection is available to peers.
- `warp-rpc-error`: If the RPC request itself fails (e.g., timeout on
  the `warp-rpc-request` level, or remote exception).

Side Effects:
- Uses `warp-connection-manager` to obtain a connection.
- Calls `warp:rpc-request`."
  (let* ((sender-id (warp-coordinator-id coordinator))
         (rpc-system (warp-coordinator-rpc-system coordinator))
         (rpc-timeout (warp-coordinator-config-rpc-timeout
                       (warp-coordinator-config coordinator)))
         (conn (warp:connection-manager-get-connection
                (warp-coordinator-connection-manager coordinator)))
         (origin-instance-id (warp-coordinator-component-system-id coordinator)))
    ;; Critical: If no connection is active, RPCs cannot be sent.
    (unless conn
      (signal (warp:error! :type 'warp-coordinator-error
                            :message "No active connection to peers for RPC.")))
    (braid! (warp:rpc-request
             rpc-system
             conn
             sender-id
             target-id
             (make-warp-rpc-command
              :name (warp-protocol-command-name command-payload)
              :args (warp-protocol-command-args command-payload))
             :timeout rpc-timeout
             :origin-instance-id origin-instance-id) ; Pass origin ID
      (:then (lambda (response) response))
      (:catch (lambda (err)
                (warp:log! :warn sender-id
                            (format "RPC to %s failed: %S. Retrying..."
                                    target-id err))
                (loom:rejected! (warp:error! :type 'warp-rpc-error
                                             :message (format "RPC to %s failed." target-id)
                                             :cause err)))))))

(defun warp-coordinator--handle-client-rpc-response (coordinator
                                                      response
                                                      operation-name)
  "Helper to process the generic response from a client-initiated RPC.
This function extracts the success status, potential error message, and
leader redirection information from the `warp-rpc-command` payload (which
holds the actual response payload from the coordinator protocol). It also
updates the local knowledge of the leader ID if the response indicates a
new leader.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `response` (warp-rpc-command): The `warp-rpc-command` received,
  containing the response payload from the target coordinator.
- `operation-name` (string): A descriptive name for the operation
  (e.g., 'Lock acquisition', 'Barrier Increment') for logging.

Returns:
- (plist): A plist indicating `(:success-p <boolean> :error <string>)`
  and potentially `:new-leader-id` (if redirection is needed).

Side Effects:
- May update `coordinator`'s `leader-id` based on response if a new
  leader is indicated.
- Logs the outcome of the operation."
  (let* ((payload (warp-rpc-command-args response)) ; Response is args of a command
         ;; Using `:granted-p` or `:success-p` as a generic success indicator,
         ;; common in coordinator protocols for various responses.
         (success-p (or (plist-get payload :granted-p)
                        (plist-get payload :success-p)))
         (leader-from-response (plist-get payload :leader-id))
         (error-msg (plist-get payload :error))
         (client-id (warp-coordinator-id coordinator)))

    (if success-p
        (progn
          (warp:log! :info client-id (format "%s successful." operation-name))
          `(:success-p t))
      (progn
        ;; If not successful, check if the response indicates a different
        ;; leader. If so, update our local knowledge of the leader.
        (when (and leader-from-response
                   (not (string= leader-from-response client-id))
                   (not (string= leader-from-response
                                 (warp-coordinator-leader-id coordinator))))
          (warp:log! :info client-id
                      (format "%s denied. New leader: %s. Updating leader ID."
                              operation-name leader-from-response))
          (setf (warp-coordinator-leader-id coordinator)
                leader-from-response))
        `(:success-p nil
          :error (format "%s failed: %S"
                         operation-name (or error-msg "unknown error"))
          :new-leader-id leader-from-response)))))

(cl-defun warp-coordinator--rpc-call-with-leader-retry (coordinator
                                                        command-fn
                                                        &key timeout
                                                             client-id
                                                             op-name)
  "Generic helper to make an RPC call to the leader with retry logic.
This function determines the current leader (or itself if leadership is
unknown), sends the RPC (`command-fn` generates the payload), and retries
if the call fails or indicates a new leader. This forms an outer retry
loop for client-initiated distributed operations, aiming for eventual
consistency and successful operation despite transient network issues or
leader changes.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator.
- `command-fn` (function): A nullary function `(lambda ())` that, when
  called, returns the `warp-protocol-command-payload` for the RPC.
- `:timeout` (number, optional): RPC timeout for the overall operation
  attempt. If the total time spent retrying exceeds this, the operation
  will fail with a `warp-coordinator-timeout`.
- `:client-id` (string): The ID of the client initiating this request
  (usually `(warp-coordinator-id coordinator)`).
- `:op-name` (string): A descriptive name for the operation (e.g.,
  'Lock acquisition') for logging and error messages.

Returns:
- (loom-promise): A promise that resolves to the final `warp-rpc-command`
  (containing the RPC response payload) from the leader on success, or
  rejects with a `warp-coordinator-error` type if all retries fail or a
  timeout occurs.

Signals:
- `warp-coordinator-timeout`: If retries for the overall operation exceed
  the specified `timeout`.
- `warp-coordinator-error`: For other non-transient RPC failures."
  (let* ((req-timeout (or timeout
                          (warp-coordinator-config-rpc-timeout
                           (warp-coordinator-config coordinator))))
         (start-time (float-time)))
    ;; The outer `loom:loop!` manages the overall retry attempts,
    ;; including delays, timeouts, and leader redirection.
    (loom:loop!
      ;; First, check the overall timeout for the entire operation.
      (when (> (- (float-time) start-time) req-timeout)
        (warp:log! :error client-id (format "%s timed out after %.2fs."
                                            op-name req-timeout))
        ;; If timed out, break from the loop with a rejection.
        (loom:break! (warp:error! :type 'warp-coordinator-timeout
                                  :message (format "%s timed out." op-name))))

      (let* ((target-id (or (warp-coordinator-leader-id coordinator)
                            client-id)) ; Target leader, or self if leader unknown.
             ;; Define the RPC sending attempt function for `loom:retry`.
             (rpc-attempt-fn
              (lambda ()
                (warp-coordinator--send-client-rpc
                 coordinator target-id (funcall command-fn)))))

        ;; Use `loom:retry` for the RPC call itself to handle transient network
        ;; failures (e.g., dropped packets, temporary connection issues).
        (braid! (loom:retry
                 rpc-attempt-fn
                 :retries 2    ; Short retry count for the inner RPC call.
                 :delay 0.5    ; Small delay between inner RPC retries.
                 :pred (lambda (err) (cl-typep err 'warp-rpc-error)))
          (:then (lambda (response)
                   ;; Process the RPC response and check for success/redirection.
                   (let ((result (warp-coordinator--handle-client-rpc-response
                                  coordinator response op-name)))
                     (if (plist-get result :success-p)
                         (loom:break! response) ; Success, exit `loom:loop!` with response.
                       (progn
                         ;; If the operation failed, and a new leader was identified,
                         ;; or it's a generic failure, retry the outer loop after a delay.
                         (loom:delay! 0.1 (loom:continue!)))))))
          (:catch (lambda (err)
                    ;; If the inner `loom:retry` for the RPC fails (all its retries exhausted),
                    ;; it means the RPC couldn't be sent or consistently failed.
                    ;; Log a warning and signal `loom:continue!` to re-enter the outer loop,
                    ;; allowing time for network recovery or leader re-election.
                    (warp:log! :warn client-id
                               (format "%s RPC to %s failed (after retries): %S.
                                        Retrying main loop..."
                                       op-name target-id err))
                    (loom:delay! 0.5 (loom:continue!)))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:coordinator-create (id
                                   cluster-id
                                   state-manager
                                   event-system
                                   command-router
                                   connection-manager
                                   rpc-system
                                   &key config)
  "Creates and initializes a new `warp-coordinator-instance`.
This factory function sets up a single node within a distributed
coordinator cluster. It links to essential Warp components (state manager,
event system, RPC system, connection manager) and initializes its internal
Raft-like state for leader election and basic consensus.

Arguments:
- `id` (string): A unique identifier for this coordinator instance within its cluster.
- `cluster-id` (string): The logical ID of the cluster this coordinator belongs to.
- `state-manager` (warp-state-manager): The `warp-state-manager` component,
  used for storing and synchronizing distributed state (locks, leader, etc.).
- `event-system` (warp-event-system): The `warp-event-system` component,
  used for emitting and subscribing to cluster-wide events (e.g., leader changes).
- `command-router` (warp-command-router): The `warp-command-router`
  component where this coordinator will register its RPC handlers
  (e.g., for `RequestVote`, `AppendEntries`).
- `connection-manager` (warp-connection-manager): The `warp-connection-manager`
  component, used for establishing and maintaining network connections to
  other cluster members.
- `rpc-system` (warp-rpc-system): The `warp-rpc-system` component, used
  for sending and receiving RPC messages between coordinator peers.
- `:config` (coordinator-config, optional): Configuration for this
  coordinator instance. If not provided, a default config will be used.

Returns:
- (warp-coordinator-instance): A new, initialized coordinator instance.

Signals:
- `cl-assert`: If any required component (e.g., `state-manager`, `rpc-system`)
  is not provided.

Side Effects:
- Initializes internal state, including `current-term`, `role`, and empty logs.
- Registers its specific RPC handlers on the provided `command-router`.
- Logs the creation of the coordinator."
  (let* ((final-config (or config (make-coordinator-config)))
         ;; Infer the ID of the component system running this coordinator.
         ;; This is needed for `warp:rpc-request`'s `origin-instance-id`.
         (component-system-id (if (functionp 'warp-component-system-id) ; Check if function exists (warp-component dependency)
                                 (warp-component-system-id (warp-rpc-system-component-system rpc-system))
                               "unknown-component-system"))) ; Fallback or error if not in component system

    (let ((coordinator (%%make-coordinator-instance
                        :id id
                        :cluster-id cluster-id
                        :config final-config
                        :state-manager state-manager
                        :event-system event-system
                        :command-router command-router
                        :connection-manager connection-manager
                        :rpc-system rpc-system
                        :component-system-id component-system-id))) 
      (warp:log! :info id (format "Coordinator instance '%s' created for cluster '%s'."
                                  id cluster-id))
      ;; Initialize RPC handlers specific to coordinator operations.
      (warp-coordinator--initialize-rpc-handlers coordinator)
      coordinator)))

;;;###autoload
(defun warp:coordinator-start (coordinator)
  "Starts the coordinator node's operation, initiating leader election
and peer communication.
This function makes the coordinator active within its cluster. It
establishes connections to peer nodes and starts the election timer,
allowing the node to participate in the leader election process.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance to start.

Returns:
- (loom-promise): A promise that resolves when the coordinator has
  successfully connected to peers and its timers are initialized.

Side Effects:
- Calls `warp-coordinator--connect-to-peers` to establish network connections.
- Calls `warp-coordinator--start-timers` to begin the election process."
  (warp:log! :info (warp-coordinator-id coordinator) "Starting coordinator.")
  (braid! (warp-coordinator--connect-to-peers coordinator)
    (:then (lambda (_)
             (warp-coordinator--start-timers coordinator)
             t))))

;;;###autoload
(defun warp:coordinator-stop (coordinator)
  "Stops the coordinator node's operation, stopping timers and
disconnecting from peers.
This function performs a graceful shutdown of the coordinator. It cancels
all active timers (election and heartbeat) and closes network connections
to other cluster members, releasing resources.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance to stop.

Returns:
- (loom-promise): A promise that resolves to `t` on successful shutdown.

Side Effects:
- Calls `warp-coordinator--stop-timers`.
- Calls `warp-coordinator--disconnect-from-peers`."
  (warp:log! :info (warp-coordinator-id coordinator) "Stopping coordinator.")
  (warp-coordinator--stop-timers coordinator)
  (braid! (warp-coordinator--disconnect-from-peers coordinator)
    (:then (lambda (_) t))))

;;;###autoload
(defun warp:coordinator-get-leader (coordinator)
  "Retrieves the currently known leader's ID for this coordinator cluster.
This function provides immediate access to the coordinator's local view
of who the current leader is. This information might be slightly stale if
a leader change has just occurred but not yet propagated.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns:
- (string or nil): The ID of the known leader, or `nil` if no leader
  is currently known or the coordinator is in election."
  (warp-coordinator-leader-id coordinator))

;;;###autoload
(defun warp:coordinator-get-role (coordinator)
  "Retrieves the current role of this coordinator instance.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns:
- (keyword): The current role (`:follower`, `:candidate`, or `:leader`)."
  (warp-coordinator-role coordinator))

;;;###autoload
(cl-defun warp:coordinator-get-lock (coordinator lock-name &key timeout)
  "Acquires a distributed lock.
This function attempts to obtain a named lock across the cluster. The request
is always directed to the known leader. If the current node is a follower,
it internally attempts to redirect the request to the known leader. It will
wait if necessary until the lock is available or the `timeout` expires,
retrying automatically on transient errors or leader changes.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `lock-name` (string): The unique name of the lock to acquire.
- `:timeout` (number, optional): Maximum total time (in seconds) to wait
  for the lock acquisition process to complete (including retries and
  leader redirection). Defaults to `warp-coordinator-config-rpc-timeout`.

Returns:
- (loom-promise): A promise that resolves to `t` if the lock is successfully
  acquired, or rejects with a `warp-coordinator-lock-failed` or
  `warp-coordinator-timeout` error on failure.

Side Effects:
- Sends RPCs to the cluster leader (via `warp-rpc`).
- Updates the local `lock-registry` on successful acquisition.
- Logs operation progress and errors."
  (let* ((req-timeout (or timeout
                          (warp-coordinator-config-rpc-timeout
                           (warp-coordinator-config coordinator))))
         (expiry-time (+ (float-time)
                         (warp-coordinator-config-lock-lease-time
                          (warp-coordinator-config coordinator))))
         (holder-id (warp-coordinator-id coordinator)))

    (braid! (warp-coordinator--rpc-call-with-leader-retry
             coordinator
             (lambda () ;; Command payload function for acquire-lock RPC.
               (warp-protocol-make-command
                :coordinator-acquire-lock
                :lock-name lock-name
                :holder-id holder-id
                :expiry-time expiry-time))
             :timeout req-timeout
             :client-id holder-id
             :op-name (format "Lock acquisition for '%s'" lock-name))
      (:then (lambda (_response)
               ;; On successful acquisition, update local cache of held locks.
               (warp:log! :info holder-id (format "Lock '%s' acquired." lock-name))
               (puthash lock-name expiry-time
                        (warp-coordinator-lock-registry coordinator))
               t))
      (:catch (lambda (err)
                (warp:log! :error holder-id
                           (format "Failed to acquire lock '%s': %S"
                                   lock-name err))
                ;; Re-signal as a specific lock-failed error for consumers.
                (signal (warp:error! :type 'warp-coordinator-lock-failed
                                     :message (format "Failed to acquire lock %S"
                                                      lock-name)
                                     :cause err)))))))

;;;###autoload
(defun warp:coordinator-release-lock (coordinator lock-name)
  "Releases a previously acquired distributed lock.
This function directs the release request to the known leader. If this
node is a follower, it internally attempts to redirect the request to
the leader. The operation retries automatically on transient errors or
leader changes.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `lock-name` (string): The name of the lock to release.

Returns:
- (loom-promise): A promise that resolves to `t` if the lock is
  successfully released, or rejects with a `warp-coordinator-lock-failed`
  error on failure.

Side Effects:
- Sends RPCs to the cluster leader.
- Removes the lock from the local `lock-registry` on successful release.
- Logs operation progress and errors."
  (let* ((req-timeout (warp-coordinator-config-rpc-timeout
                       (warp-coordinator-config coordinator)))
         (holder-id (warp-coordinator-id coordinator)))
    (braid! (warp-coordinator--rpc-call-with-leader-retry
             coordinator
             (lambda () ;; Command payload function for release-lock RPC.
               (warp-protocol-make-command
                :coordinator-release-lock
                :lock-name lock-name
                :holder-id holder-id))
             :timeout req-timeout
             :client-id holder-id
             :op-name (format "Lock release for '%s'" lock-name))
      (:then (lambda (_response)
               ;; On successful release, remove from local cache.
               (warp:log! :info holder-id (format "Lock '%s' released." lock-name))
               (remhash lock-name
                        (warp-coordinator-lock-registry coordinator))
               t))
      (:catch (lambda (err)
                (warp:log! :error holder-id
                           (format "Failed to release lock '%s': %S"
                                   lock-name err))
                ;; Re-signal as a specific lock-failed error for consumers.
                (signal (warp:error! :type 'warp-coordinator-lock-failed
                                     :message (format "Failed to release lock %S"
                                                      lock-name)
                                     :cause err)))))))

;;;###autoload
(cl-defun warp:coordinator-propose-state-change (coordinator key value &key timeout)
  "Proposes a distributed state change.
This operation can only be initiated by the current leader (this node
must be in the `:leader` role). It attempts to update the shared state
in `warp-state-manager` via an RPC to itself (as the leader).
This is a simplified consensus model; it doesn't involve full log
replication or quorum voting for each change, but rather assumes the
leader's direct manipulation of the shared state manager (which is
CRDT-based) reflects its consensus.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `key` (list): The path to the state key (e.g., `(:config :my-setting)`).
- `value` (any): The new value for the state key.
- `:timeout` (number, optional): Maximum time (in seconds) to wait
  for the proposal to be applied by the leader. Defaults to
  `warp-coordinator-config-rpc-timeout`.

Returns:
- (loom-promise): A promise that resolves to `t` if the state change is
  successfully proposed and applied by the leader, or rejects with a
  `warp-coordinator-consensus-failed` or `warp-coordinator-timeout`
  error.

Signals:
- `warp-coordinator-not-leader`: If this coordinator is not the leader.
  (This is a client-side check for this simplified model).

Side Effects:
- Sends an RPC to the cluster leader (which is itself if this node is leader).
- Calls `warp:state-manager-update` on the leader to apply the change.
- Logs proposal progress and errors."
  (let* ((req-timeout (or timeout
                          (warp-coordinator-config-rpc-timeout
                           (warp-coordinator-config coordinator))))
         (proposer-id (warp-coordinator-id coordinator)))
    ;; Client-side check: This node *must* be the leader to propose changes.
    ;; In a full Raft, followers would forward client requests to the leader.
    (unless (eq (warp-coordinator-role coordinator) :leader)
      (signal (warp:error! :type 'warp-coordinator-not-leader
                           :message "Only the leader can propose state changes.")))

    (braid! (warp-coordinator--rpc-call-with-leader-retry
             coordinator
             (lambda () ;; Command payload function for propose-change RPC.
               (warp-protocol-make-command
                :coordinator-propose-change
                :key key
                :value value))
             :timeout req-timeout
             :client-id proposer-id
             :op-name (format "Propose state change for key '%S'" key))
      (:then (lambda (_response)
               ;; No local state update needed here, as `warp-state-manager`
               ;; already handles the authoritative state and its propagation.
               (warp:log! :info proposer-id
                          (format "Proposed change for key '%S' successfully applied."
                                  key))
               t))
      (:catch (lambda (err)
                (warp:log! :error proposer-id
                           (format "Failed to propose change for key '%S': %S"
                                   key err))
                (signal (warp:error! :type 'warp-coordinator-consensus-failed
                                     :message (format "Failed to propose change for key %S"
                                                      key)
                                     :cause err)))))))

;;;###autoload
(cl-defun warp:coordinator-wait-for-barrier (coordinator barrier-name
                                          total-participants &key timeout)
  "Waits for a distributed barrier to be met.
This function repeatedly attempts to increment a named barrier's count on
the current leader and waits until the `current-count` reaches
`total-participants` or a global `timeout` expires. It handles potential
leader changes and RPC failures by retrying the operation, making it
resilient to transient cluster issues.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `barrier-name` (string): The unique name of the barrier to wait on.
- `total-participants` (integer): The total number of participants
  required to meet the barrier.
- `:timeout` (number, optional): Maximum total time (in seconds) to wait
  for the barrier to be met. Defaults to `warp-coordinator-config-rpc-timeout`.

Returns:
- (loom-promise): A promise that resolves to `t` if the barrier is met
  within the timeout, or rejects with a `warp-coordinator-barrier-failed`
  or `warp-coordinator-timeout` error.

Side Effects:
- Sends RPCs to increment the barrier's count on the cluster leader.
- Logs attempts and status updates.
- May update the known leader ID if a redirect response is received."
  (let* ((req-timeout (or timeout
                          (warp-coordinator-config-rpc-timeout
                           (warp-coordinator-config coordinator))))
         (participant-id (warp-coordinator-id coordinator))
         (start-time (float-time)))

    ;; Use `loom:loop!` to repeatedly attempt incrementing the barrier
    ;; until it's met or a timeout occurs.
    (loom:loop!
      ;; First, check the overall timeout for the entire barrier wait.
      (when (> (- (float-time) start-time) req-timeout)
        (warp:log! :error participant-id
                    (format "Barrier '%S' timed out after %.2fs."
                            barrier-name req-timeout))
        ;; If timed out, break from the loop with a rejection.
        (loom:break! (warp:error! :type 'warp-coordinator-timeout
                                  :message (format "Barrier '%S' timed out."
                                                   barrier-name))))

      (warp:log! :debug participant-id
                  (format "Attempting to increment barrier '%s' (current role: %S)."
                          barrier-name (warp-coordinator-role coordinator)))

      ;; Attempt to increment the barrier and handle the response.
      (braid! (warp-coordinator--rpc-call-with-leader-retry
               coordinator
               (lambda () ;; Command payload function for barrier-increment RPC.
                 (warp-protocol-make-command
                  :coordinator-barrier-increment
                  :barrier-name barrier-name
                  :participant-id participant-id
                  :total-participants total-participants))
               :timeout req-timeout ; Overall timeout handled by outer loop
               :client-id participant-id
               :op-name (format "Barrier increment for '%s'" barrier-name))
        (:then (lambda (response)
                 ;; Process the RPC response from the leader.
                 (let* ((payload (warp-rpc-command-args response)) ; Barrier response payload
                        (current-count (plist-get payload :current-count))
                        (is-met-p (plist-get payload :is-met-p)))
                   (cond
                     (is-met-p
                      (warp:log! :info participant-id
                                 (format "Barrier '%s' met. Count: %d/%d."
                                         barrier-name current-count
                                         total-participants))
                      (loom:break! t)) ; Barrier met, exit `loom:loop!` successfully.
                     (t
                      (warp:log! :debug participant-id
                                 (format "Barrier '%s' incremented to %d/%d. Waiting..."
                                         barrier-name current-count
                                         total-participants))
                      ;; If not met yet, continue the outer loop after a small delay.
                      (loom:delay! 0.1 (loom:continue!)))))))
        (:catch (lambda (err)
                  ;; If the `warp-coordinator--rpc-call-with-leader-retry` itself
                  ;; signals an error (e.g., all its retries failed, or a timeout
                  ;; specific to the RPC call occurred), log and continue the
                  ;; outer `loom:loop!`. This allows the overall barrier wait
                  ;; to continue trying, potentially with a new leader.
                  (warp:log! :warn participant-id
                              (format "Barrier increment attempt failed for '%s': %S.
                                       Retrying main loop..."
                                      barrier-name err))
                  (loom:delay! 0.5 (loom:continue!))))))))

(provide 'warp-coordinator)
;;; warp-coordinator.el ends here