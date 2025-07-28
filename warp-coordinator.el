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
(Note: Not a full Raft log, used for basic state propagation.)

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
  manager.
- `event-system` (warp-event-system): Reference to the event system.
- `command-router` (warp-command-router): Internal command router for
  RPC handlers.
- `connection-manager` (warp-connection-manager): Manages connections
  to other coordinator nodes and the master.
- `role` (keyword): Current role: `:follower`, `:candidate`, `:leader`.
- `current-term` (integer): Current term number this coordinator
  believes.
- `voted-for` (string or nil): Candidate ID voted for in `current-term`.
- `leader-id` (string or nil): ID of the known leader (if any).
- `log` (list): In-memory list of committed log entries (simplified).
- `election-timer` (timer or nil): Timer for election timeouts.
- `heartbeat-timer` (timer or nil): Timer for leader heartbeats.
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
they are serving, and the last heartbeat timestamp.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns: (loom-promise): A promise that resolves to a plist like
    `(:id \"leader-x\" :term 5 :last-heartbeat 123.4)` or `nil` if no
    leader info is present."
  (loom:await (warp:state-manager-get
               (warp-coordinator-state-manager coordinator)
               `(:coordinator :leader-state))))

(defun warp-coordinator--set-leader-state (coordinator leader-state)
  "Updates the leader state in `warp-state-manager`.
This is typically called by a candidate becoming a leader, or a leader
sending heartbeats. It's an atomic update to ensure consistency.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `leader-state` (plist): The new leader state to write.

Returns: (loom-promise): A promise resolving to `t` on success."
  (loom:await (warp:state-manager-update
               (warp-coordinator-state-manager coordinator)
               `(:coordinator :leader-state)
               leader-state)))

(defun warp-coordinator--update-term (coordinator new-term &optional leader-id)
  "Updates the coordinator's current term and potentially its leader.
If `new-term` is greater than `current-term`, the coordinator
transitions to follower, clears its vote, and updates its term.
This is a critical Raft rule for ensuring term monotonicity and
leader-follower consistency.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `new-term` (integer): The new term number.
- `leader-id` (string or nil, optional): The ID of the new leader,
  if known.

Returns: `nil`.

Side Effects:
- Modifies `coordinator`'s `current-term`, `role`, `voted-for`,
  `leader-id`.
- Cancels `election-timer` if active."
  (when (> new-term (warp-coordinator-current-term coordinator))
    (warp:log! :info (warp-coordinator-id coordinator)
               "Term updated: %d -> %d. Transitioning to follower."
               (warp-coordinator-current-term coordinator) new-term)
    (setf (warp-coordinator-current-term coordinator) new-term)
    (setf (warp-coordinator-role coordinator) :follower)
    ;; Clear vote for old term, as per Raft rules.
    (setf (warp-coordinator-voted-for coordinator) nil)
    (setf (warp-coordinator-leader-id coordinator) leader-id)
    (when (warp-coordinator-election-timer coordinator)
      (cancel-timer (warp-coordinator-election-timer coordinator))
      (setf (warp-coordinator-election-timer coordinator) nil))))

(defun warp-coordinator--log-append (coordinator entries)
  "Appends new log `entries` to the coordinator's in-memory log.
(Simplified: this is not a persistent or replicated log).

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `entries` (list): A list of `warp-coordinator-log-entry` objects.

Returns: `nil`.

Side Effects:
- Modifies `coordinator`'s `log`."
  (cl-loop for entry in entries do
           (setf (warp-coordinator-log coordinator)
                 (append (warp-coordinator-log coordinator) (list entry))))
  (warp:log! :trace (warp-coordinator-id coordinator)
             "Appended %d log entries. New log length: %d"
             (length entries) (length (warp-coordinator-log coordinator))))

(defun warp-coordinator--get-last-log-info (coordinator)
  "Retrieves the term and index of the last entry in the coordinator's
log. Used for Raft's log matching consistency check during elections
and AppendEntries RPCs.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns: (plist): A plist `(:index <int> :term <int>)` of the last log
  entry, or `(:index 0 :term 0)` if the log is empty (representing the
  initial state before any entries)."
  (if-let (last-entry (car (last (warp-coordinator-log coordinator))))
      `(:index ,(warp-coordinator-log-entry-index last-entry)
        :term ,(warp-coordinator-log-entry-term last-entry))
    `(:index 0 :term 0)))

;;----------------------------------------------------------------------
;;; Timers and Role Management
;;----------------------------------------------------------------------

(defun warp-coordinator--reset-election-timer (coordinator)
  "Resets the election timer with a random timeout.
This function is called by followers and candidates. If no valid
heartbeats are received from a leader, or no votes from other members
(for candidates), this timer will expire, triggering a new election.
The random timeout helps prevent all nodes from timing out at once.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns: `nil`.

Side Effects:
- Cancels any existing `election-timer`.
- Schedules a new `election-timer` to call
  `warp-coordinator--start-election`."
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
               "Election timer reset to %.3fs." timeout-s)))

(defun warp-coordinator--process-vote-response (coordinator response current-term
                                                vote-counter-lock)
  "Processes a single vote response received during an election.
This helper updates the local vote count and steps down if a higher
term is discovered. It's designed to be called asynchronously for
each vote RPC response.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `response` (warp-rpc-command): The RPC response containing vote
    information.
- `current-term` (integer): The term of the election this node is
    participating in.
- `vote-counter-lock` (loom-lock): A mutex to protect the
    `votes-received` counter, ensuring atomic updates in a concurrent
    environment.

Returns: `nil`.

Side Effects:
- May increment internal `votes-received` count (in caller's scope).
- May update `coordinator`'s term and role if a higher term is seen.
- This function expects `votes-received` to be bound in the calling
    context (`warp-coordinator--start-election`) and passed implicitly
    via closure for modification."
  (let* ((response-term
          (warp-protocol-coordinator-request-vote-response-payload-term
           response))
         (granted-p
          (warp-protocol-coordinator-request-vote-response-payload-vote-granted-p
           response)))
    (loom:with-mutex! vote-counter-lock
      ;; Only process the response if this node is still a candidate
      ;; for the *same* term as the election it initiated.
      (when (eq (warp-coordinator-role coordinator) :candidate)
        ;; Raft Rule: If a higher term is seen, immediately step down.
        (when (> response-term current-term)
          (warp-coordinator--update-term
           coordinator response-term nil))
        ;; If the vote is granted and for the current term, count it.
        (when (and granted-p
                   (= response-term current-term))
          ;; This relies on `votes-received` being mutable in the
          ;; outer function's lexical scope (`start-election`).
          (cl-incf votes-received))))))

(defun warp-coordinator--send-vote-request (coordinator member-id current-term
                                            last-log-info rpc-timeout)
  "Sends a single `:request-vote` RPC to a peer.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `member-id` (string): The ID of the peer to send the RPC to.
- `current-term` (integer): The current term of the candidate.
- `last-log-info` (plist): The last log index and term of the candidate.
- `rpc-timeout` (number): Timeout for the RPC.

Returns: (loom-promise): A promise that resolves to the RPC response on
    success, or rejects on failure. This promise's outcome is then
    handled by `loom:all-settled` in the calling function."
  (braid! (warp:rpc-request
           ;; `warp:rpc-request` uses the connection manager to find a
           ;; suitable connection to the `recipient-id`.
           (warp:connection-manager-get-connection
            (warp-coordinator-connection-manager coordinator))
           (warp-coordinator-id coordinator) ; Sender ID
           member-id                          ; Recipient ID
           (warp-protocol-make-command
            :coordinator-request-vote
            :candidate-id (warp-coordinator-id coordinator)
            :term current-term
            :last-log-index (plist-get last-log-info :index)
            :last-log-term (plist-get last-log-info :term)))
    (:timeout rpc-timeout)
    (:then (lambda (response) response)) ;; Just pass the response through
    (:catch (lambda (err)
              (warp:log! :warn (warp-coordinator-id coordinator)
                         "Vote request to %s failed: %S"
                         member-id err)
              (loom:rejected! err))))) ;; Propagate the error for loom:all-settled

(defun warp-coordinator--start-election (coordinator)
  "Initiates a new leader election.
The coordinator increments its term, becomes a candidate, votes for
itself, and requests votes from other cluster members.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns: (loom-promise): A promise that resolves when the election
    process has concluded (either by becoming leader, stepping down,
    or timing out).

Side Effects:
- Increments `current-term`.
- Sets `role` to `:candidate`, `voted-for` to self.
- Resets `election-timer`.
- Sends `:request-vote` RPCs to other members."
  (warp:log! :info (warp-coordinator-id coordinator) "Starting election.")
  (cl-incf (warp-coordinator-current-term coordinator))
  (setf (warp-coordinator-role coordinator) :candidate)
  (setf (warp-coordinator-voted-for coordinator)
        (warp-coordinator-id coordinator))
  (setf (warp-coordinator-leader-id coordinator) nil)
  (warp-coordinator--reset-election-timer coordinator)

  (let* ((config (warp-coordinator-config coordinator))
         (members (warp-coordinator-config-cluster-members config))
         (current-term (warp-coordinator-current-term coordinator))
         (last-log-info (warp-coordinator--get-last-log-info coordinator))
         (votes-received 1) ; Vote for self initially
         (total-members (1+ (length members))) ; Include self
         (quorum (1+ (floor total-members 2)))
         (rpc-timeout (warp-coordinator-config-rpc-timeout config))
         (vote-counter-lock (loom:make-lock "vote-counter-lock")) ;; Mutex for `votes-received`

         ;; Create a list of promises for each vote request RPC to peers.
         (vote-request-promises
          (cl-loop for member-id in members
                   collect (warp--send-vote-request
                            coordinator member-id current-term
                            last-log-info rpc-timeout))))

    (warp:log! :debug (warp-coordinator-id coordinator)
               "Requesting votes for term %d (quorum %d)."
               current-term quorum)

    ;; Use `loom:all-settled` to wait for all vote requests to complete,
    ;; regardless of individual success/failure. This prevents a single
    ;; failed RPC from prematurely stopping the election attempt.
    (braid! (loom:all-settled vote-request-promises)
      (:then (lambda (outcomes)
               ;; Process each individual vote response outcome.
               (dolist (outcome outcomes)
                 (when (eq (plist-get outcome :status) 'fulfilled)
                   (warp-coordinator--process-vote-response
                    coordinator (plist-get outcome :value) current-term
                    vote-counter-lock)))
               ;; After all responses are processed, check if this candidate
               ;; has received enough votes to become leader.
               (loom:with-mutex! vote-counter-lock
                 (when (and (eq (warp-coordinator-role coordinator)
                                :candidate)
                            (>= votes-received quorum)
                            ;; Crucially, ensure we are still in the same term
                            ;; we started the election in. If a higher term
                            ;; appeared during the election, we would have
                            ;; stepped down already.
                            (= (warp-coordinator-current-term coordinator)
                               current-term))
                   (warp-coordinator--become-leader coordinator))))))))

(defun warp-coordinator--process-heartbeat-response (coordinator response
                                                     current-term leader-id)
  "Processes a single heartbeat response from a follower.
If a higher term is discovered, the leader steps down.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `response` (warp-rpc-command): The RPC response.
- `current-term` (integer): The term of the leader.
- `leader-id` (string): The ID of the leader (current node).

Returns: `nil`.

Side Effects:
- May update `coordinator`'s term and role."
  (let ((response-term
         (warp-protocol-coordinator-append-entries-response-payload-term
          response)))
    ;; Raft Rule: If leader receives response with higher term, it must
    ;; step down. This handles cases where a new leader has been elected.
    (when (> response-term current-term)
      (warp:log! :info leader-id
                 "Received higher term %d from %s. Stepping down."
                 response-term (warp-rpc-command-source-id response))
      (warp-coordinator--update-term
       coordinator response-term nil))))

(defun warp-coordinator--send-single-heartbeat (coordinator member-id
                                                current-term leader-id rpc-timeout)
  "Sends a single heartbeat (AppendEntries RPC with no entries) to a peer.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `member-id` (string): The ID of the peer to send the heartbeat to.
- `current-term` (integer): The current term of the leader.
- `leader-id` (string): The ID of the leader (current node).
- `rpc-timeout` (number): Timeout for the RPC.

Returns: (loom-promise): A promise that resolves on successful RPC response,
    or rejects on failure. The outcome is processed by `loom:all-settled`."
  (braid! (warp:rpc-request
           ;; Uses the connection manager and RPC layer to route by `recipient-id`.
           (warp:connection-manager-get-connection
            (warp-coordinator-connection-manager coordinator))
           leader-id    ; Sender ID
           member-id    ; Recipient ID
           (warp-protocol-make-command
            :coordinator-append-entries
            :term current-term
            :leader-id leader-id
            :prev-log-index 0 ; Simplified for heartbeats (no log entries)
            :prev-log-term 0  ; Simplified for heartbeats
            :entries nil      ; No new log entries for heartbeat
            :leader-commit 0)) ; Simplified
          (:timeout rpc-timeout)
          (:then (lambda (response)
                   (warp-coordinator--process-heartbeat-response
                    coordinator response current-term leader-id)))
          (:catch (lambda (err)
                    (warp:log! :warn leader-id
                               "Heartbeat to %s failed: %S"
                               member-id err)
                    nil)))) ; Return nil for failed heartbeats (don't propagate rejection)

(defun warp-coordinator--send-heartbeats (coordinator)
  "Sends periodic heartbeats (AppendEntries RPCs with no new entries)
to all other cluster members if this node is the leader.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns: (loom-promise): A promise that resolves when all heartbeats
    are sent (or attempted).

Side Effects:
- Sends `:append-entries` RPCs to other members."
  (when (eq (warp-coordinator-role coordinator) :leader)
    (let* ((config (warp-coordinator-config coordinator))
           (members (warp-coordinator-config-cluster-members config))
           (current-term (warp-coordinator-current-term coordinator))
           (leader-id (warp-coordinator-id coordinator))
           (rpc-timeout (warp-coordinator-config-rpc-timeout config))
           (heartbeat-promises nil))
      (warp:log! :trace leader-id "Sending heartbeats (term %d)." current-term)
      (dolist (member-id members)
        (push (warp--send-single-heartbeat
               coordinator member-id current-term leader-id rpc-timeout)
              heartbeat-promises))
      ;; Use `loom:all-settled` to ensure all heartbeats are attempted,
      ;; and the promise resolves even if some fail (non-blocking).
      (loom:all-settled heartbeat-promises))))

(defun warp-coordinator--become-leader (coordinator)
  "Transitions the coordinator's role to leader.
Cancels the election timer, sets role to leader, and starts sending
periodic heartbeats. It also initializes `next-index` for all
followers (a Raft concept for log replication).

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns: (loom-promise): A promise that resolves when the transition
    to leader is complete and initial heartbeats are sent.

Side Effects:
- Sets `role` to `:leader`, `leader-id` to self.
- Cancels `election-timer`.
- Starts `heartbeat-timer`.
- Initializes `next-index` and `match-index` for all followers.
- Emits `:leader-elected` event."
  (warp:log! :info (warp-coordinator-id coordinator)
             "Becoming leader for term %d."
             (warp-coordinator-current-term coordinator))
  (setf (warp-coordinator-role coordinator) :leader)
  (setf (warp-coordinator-leader-id coordinator)
        (warp-coordinator-id coordinator))
  ;; Ensure all old timers are stopped as a leader has different timers.
  (when (warp-coordinator-election-timer coordinator)
    (cancel-timer (warp-coordinator-election-timer coordinator))
    (setf (warp-coordinator-election-timer coordinator) nil))
  (when (warp-coordinator-heartbeat-timer coordinator)
    (cancel-timer (warp-coordinator-heartbeat-timer coordinator)))

  ;; Initialize nextIndex and matchIndex for all followers (Raft concept).
  ;; These are used by the leader to track replication progress for each follower.
  (let* ((config (warp-coordinator-config coordinator))
         (members (warp-coordinator-config-cluster-members config))
         (last-log-index (plist-get
                          (warp-coordinator--get-last-log-info coordinator)
                          :index)))
    (dolist (member-id members)
      ;; Leader typically starts by assuming followers are behind its whole log.
      (puthash member-id (1+ last-log-index)
               (warp-coordinator-next-index coordinator))
      ;; Match index is initially 0, meaning no log entries matched.
      (puthash member-id 0 (warp-coordinator-match-index coordinator))))

  ;; Start periodic heartbeats.
  (setf (warp-coordinator-heartbeat-timer coordinator)
        (run-at-time (/ (warp-coordinator-config-heartbeat-interval
                         (warp-coordinator-config coordinator)) 1000.0)
                     (/ (warp-coordinator-config-heartbeat-interval
                         (warp-coordinator-config coordinator)) 1000.0)
                     #'warp-coordinator--send-heartbeats coordinator))

  ;; Emit leader elected event for external listeners (e.g., metrics, logs).
  (warp:emit-event
   (warp-coordinator-event-system coordinator)
   :leader-elected
   `(:leader-id ,(warp-coordinator-id coordinator)
     :term ,(warp-coordinator-current-term coordinator)))

  ;; Send an immediate heartbeat to quickly establish leadership with followers.
  (warp-coordinator--send-heartbeats coordinator))

;;----------------------------------------------------------------------
;;; RPC Handlers (Received from other Coordinator Nodes)
;;----------------------------------------------------------------------

(defun warp-coordinator--handle-acquire-lock-logic (coordinator lock-name holder-id expiry-time)
  "Internal helper for processing the core logic of an acquire lock request.
This function performs the atomic check-and-set operation for a
distributed lock within a state manager transaction. This ensures that
lock acquisition is an atomic operation across the cluster's shared state.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `lock-name` (string): The name of the lock.
- `holder-id` (string): The ID of the requester.
- `expiry-time` (float): The requested expiry timestamp for the lock.

Returns: (loom-promise): A promise that resolves to a plist
    `(:granted-p <boolean> :error <string or nil>)`.

Side Effects:
- May modify the lock state in `warp-state-manager` via a transaction.
- Logs lock acquisition/renewal."
  (let* ((sm (warp-coordinator-state-manager coordinator))
         (granted-p nil)
         (error-msg nil))
    (braid! (warp:state-manager-transaction
             sm
             (lambda (tx)
               (let* ((current-lock (warp:state-manager-get tx
                                                            `(:locks ,lock-name)))
                      ;; Check if a lock entry exists and is not marked as deleted.
                      (lock-is-held (and current-lock
                                         (not (warp:state-entry-deleted
                                               current-lock))))
                      ;; Get current holder and expiry if lock is held.
                      (lock-holder (when lock-is-held
                                     (plist-get
                                      (warp:state-entry-value current-lock)
                                      :holder-id)))
                      (lock-expired-at (when lock-is-held
                                         (plist-get
                                          (warp:state-entry-value
                                           current-lock)
                                          :expiry-time))))
                 (cond
                  ((and lock-is-held (>= (float-time) lock-expired-at))
                   ;; Case 1: Lock is currently held but has expired.
                   ;; Grant the lock to the new requester.
                   (warp:state-tx-update
                    tx `(:locks ,lock-name)
                    `(:holder-id ,holder-id :expiry-time ,expiry-time))
                   (setq granted-p t)
                   (warp:log! :info (warp-coordinator-id coordinator)
                              "Lock '%s' acquired by %s (expired previous)."
                              lock-name holder-id))
                  ((and lock-is-held (string= lock-holder holder-id))
                   ;; Case 2: Lock is already held by the requesting client.
                   ;; This is a re-acquisition or renewal. Update the lease.
                   (warp:state-tx-update
                    tx `(:locks ,lock-name)
                    `(:holder-id ,holder-id :expiry-time ,expiry-time))
                   (setq granted-p t)
                   (warp:log! :info (warp-coordinator-id coordinator)
                              "Lock '%s' renewed by %s." lock-name holder-id))
                  ((not lock-is-held)
                   ;; Case 3: Lock is not currently held by anyone.
                   ;; Acquire it for the requester.
                   (warp:state-tx-update
                    tx `(:locks ,lock-name)
                    `(:holder-id ,holder-id :expiry-time ,expiry-time))
                   (setq granted-p t)
                   (warp:log! :info (warp-coordinator-id coordinator)
                              "Lock '%s' acquired by %s." lock-name holder-id))
                  (t
                   ;; Case 4: Lock is held by another client and not expired.
                   ;; Deny the request.
                   (setq error-msg
                         (format "Lock '%s' held by %s."
                                 lock-name lock-holder)))))))
      (:then (lambda (_res)
               `(:granted-p ,granted-p :error ,error-msg)))
      (:catch (lambda (err)
                (warp:log! :error (warp-coordinator-id coordinator)
                           "Transaction for lock %s failed: %S"
                           lock-name err)
                `(:granted-p nil :error (format "Transaction error: %S"
                                                err)))))))

(defun warp-coordinator-handle-acquire-lock (command context)
  "Handles incoming `:coordinator-acquire-lock` RPCs.
Only the cluster leader processes these requests by performing an atomic
transaction on the `warp-state-manager`. If the local node is a follower,
it directs the requesting client to the known leader.

Arguments:
- `command` (warp-rpc-command): The incoming RPC command.
- `context` (warp-request-pipeline-context): The pipeline context,
    providing access to the `coordinator-instance`.

Returns: (loom-promise): A promise resolving with a
    `warp-protocol-coordinator-lock-response-payload`."
  (let* ((coordinator (plist-get
                       (warp-request-pipeline-context-worker context)
                       :coordinator-instance))
         (args (warp-rpc-command-args command))
         (lock-name (warp-protocol-coordinator-lock-request-payload-lock-name
                     args))
         (holder-id (warp-protocol-coordinator-lock-request-payload-holder-id
                     args))
         (expiry-time (warp-protocol-coordinator-lock-request-payload-expiry-time
                       args)))

    (warp:log! :debug (warp-coordinator-id coordinator)
               "Received acquire-lock request for '%s' from %s."
               lock-name holder-id)

    (if (eq (warp-coordinator-role coordinator) :leader)
        ;; If this node is the leader, process the lock acquisition request.
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

(defun warp-coordinator--handle-release-lock-logic (coordinator lock-name holder-id)
  "Internal helper for processing the core logic of a release lock request.
This function performs the atomic deletion of a distributed lock within
a state manager transaction, but only if the requesting holder is the
current owner.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `lock-name` (string): The name of the lock.
- `holder-id` (string): The ID of the requester.

Returns: (loom-promise): A promise that resolves to a plist
    `(:success-p <boolean> :error <string or nil>)`.

Side Effects:
- May modify the lock state in `warp-state-manager` via a transaction.
- Logs lock release."
  (let* ((sm (warp-coordinator-state-manager coordinator))
         (success-p nil)
         (error-msg nil))
    (braid! (warp:state-manager-transaction
             sm
             (lambda (tx)
               (let* ((current-lock (warp:state-manager-get tx
                                                            `(:locks ,lock-name)))
                      (lock-is-held (and current-lock
                                         (not (warp:state-entry-deleted
                                               current-lock))))
                      (lock-holder (when lock-is-held
                                     (plist-get
                                      (warp:state-entry-value current-lock)
                                      :holder-id))))
                 (cond
                  ((and lock-is-held (string= lock-holder holder-id))
                   ;; Case 1: Lock is held by requester, so release it.
                   (warp:state-tx-delete tx `(:locks ,lock-name))
                   (setq success-p t)
                   (warp:log! :info (warp-coordinator-id coordinator)
                              "Lock '%s' released by %s."
                              lock-name holder-id))
                  ((and lock-is-held
                        (not (string= lock-holder holder-id)))
                   ;; Case 2: Lock is held by someone else. Deny release.
                   (setq error-msg
                         (format "Lock '%s' held by %s, not %s."
                                 lock-name lock-holder holder-id)))
                  (t
                   ;; Case 3: Lock not held by anyone. Treat as successful
                   ;; release (idempotent operation).
                   (setq success-p t)
                   (warp:log! :info (warp-coordinator-id coordinator)
                              "Lock '%s' not held, treating release by %s as successful."
                              lock-name holder-id))))))
      (:then (lambda (_res)
               `(:success-p ,success-p :error ,error-msg)))
      (:catch (lambda (err)
                (warp:log! :error (warp-coordinator-id coordinator)
                           "Transaction for release lock %s failed: %S"
                           lock-name err)
                `(:success-p nil :error (format "Transaction error: %S"
                                                err)))))))

(defun warp-coordinator-handle-release-lock (command context)
  "Handles incoming `:coordinator-release-lock` RPCs.
Only the cluster leader processes these requests by performing an atomic
transaction on the `warp-state-manager`. If the local node is a follower,
it directs the requesting client to the known leader.

Arguments:
- `command` (warp-rpc-command): The incoming RPC command.
- `context` (warp-request-pipeline-context): The pipeline context.

Returns: (loom-promise): A promise resolving with a
    `warp-protocol-coordinator-lock-response-payload`."
  (let* ((coordinator (plist-get
                       (warp-request-pipeline-context-worker context)
                       :coordinator-instance))
         (args (warp-rpc-command-args command))
         (lock-name (warp-protocol-coordinator-lock-request-payload-lock-name
                     args))
         (holder-id (warp-protocol-coordinator-lock-request-payload-holder-id
                     args)))

    (warp:log! :debug (warp-coordinator-id coordinator)
               "Received release-lock request for '%s' from %s."
               lock-name holder-id)

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

(defun warp-coordinator--handle-barrier-increment-logic (coordinator barrier-name
                                                        participant-id total-participants)
  "Internal helper for processing the core logic of a barrier increment.
This function atomically updates the barrier's count in `warp-state-manager`.
It checks if the barrier is met after the increment. It also handles
duplicate participant registrations by returning success if the
participant already registered.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `barrier-name` (string): The name of the barrier.
- `participant-id` (string): The ID of the participant incrementing.
- `total-participants` (integer): The required count to meet the barrier.

Returns: (loom-promise): A promise that resolves to a plist
    `(:success-p <boolean> :current-count <int>
    :is-met-p <boolean> :error <string or nil>)`.

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
                            :message "Total participants must be integer.")))

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
                                "Barrier '%s' met with %d participants."
                                barrier-name current-count))))))
      (:then (lambda (_res)
               `(:success-p ,success-p :current-count ,current-count
                 :is-met-p ,is-met-p :error ,error-msg)))
      (:catch (lambda (err)
                (warp:log! :error (warp-coordinator-id coordinator)
                           "Transaction for barrier %s failed: %S"
                           barrier-name err)
                `(:success-p nil :current-count ,current-count
                  :is-met-p nil :error (format "Transaction error: %S"
                                               err)))))))

(defun warp-coordinator-handle-barrier-increment (command context)
  "Handles incoming `:coordinator-barrier-increment` RPCs.
Only the cluster leader processes these requests by performing an atomic
transaction on the `warp-state-manager`. If the local node is a follower,
it directs the requesting client to the known leader.

Arguments:
- `command` (warp-rpc-command): The incoming RPC command.
- `context` (warp-request-pipeline-context): The pipeline context.

Returns: (loom-promise): A promise resolving with a
    `warp-protocol-coordinator-barrier-response-payload`."
  (let* ((coordinator (plist-get
                       (warp-request-pipeline-context-worker context)
                       :coordinator-instance))
         (args (warp-rpc-command-args command))
         (barrier-name (warp-protocol-coordinator-barrier-increment-payload-barrier-name
                        args))
         (participant-id (warp-protocol-coordinator-barrier-increment-payload-participant-id
                          args))
         (total-participants (warp-protocol-coordinator-barrier-increment-payload-total-participants
                              args)))

    (warp:log! :debug (warp-coordinator-id coordinator)
               "Received barrier increment for '%s' from %s."
               barrier-name participant-id)

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

(defun warp-coordinator-handle-propose-change (command context)
  "Handles incoming `:coordinator-propose-change` RPCs.
Only the leader can process these requests. It attempts to apply the
proposed state change using `warp-state-manager`'s update function.
This is a simplified consensus model; it doesn't involve full log
replication or quorum voting for each change, but rather assumes the
leader's direct manipulation of the shared state manager reflects its
consensus.

Arguments:
- `command` (warp-rpc-command): The incoming RPC command.
- `context` (warp-request-pipeline-context): The pipeline context.

Returns: (loom-promise): A promise resolving with a
    `warp-protocol-coordinator-propose-change-response-payload`."
  (let* ((coordinator (plist-get
                       (warp-request-pipeline-context-worker context)
                       :coordinator-instance))
         (args (warp-rpc-command-args command))
         (key (warp-protocol-coordinator-propose-change-payload-key args))
         (value (warp-protocol-coordinator-propose-change-payload-value args))
         (sm (warp-coordinator-state-manager coordinator))
         (success-p nil)
         (error-msg nil))

    (warp:log! :debug (warp-coordinator-id coordinator)
               "Received propose-change request for key '%S'." key)

    (if (eq (warp-coordinator-role coordinator) :leader)
        ;; If this node is the leader, apply the state change directly.
        ;; This is the simplified "consensus" step for the leader.
        (braid! (warp:state-manager-update sm key value)
          (:then (lambda (_res)
                   (setq success-p t)
                   (warp:log! :info (warp-coordinator-id coordinator)
                              "Proposed change for key '%S' applied by leader."
                              key)
                   (loom:resolved! (warp-protocol-make-command
                                    :coordinator-propose-change-response
                                    :success-p success-p
                                    :error error-msg))))
          (:catch (lambda (err)
                    (setq error-msg (format "Failed to apply change: %S" err))
                    (warp:log! :error (warp-coordinator-id coordinator)
                               "Failed to apply proposed change for key '%S': %S"
                               key err)
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

;; Define RPC handlers using warp:defrpc-handlers. These map incoming
;; RPC command types to the functions that handle them.
(warp:defrpc-handlers
 (:coordinator-request-vote . #'warp-coordinator-handle-request-vote)
 (:coordinator-append-entries . #'warp-coordinator-handle-append-entries)
 (:coordinator-acquire-lock . #'warp-coordinator-handle-acquire-lock)
 (:coordinator-release-lock . #'warp-coordinator-handle-release-lock)
 (:coordinator-barrier-increment . #'warp-coordinator-handle-barrier-increment)
 (:coordinator-propose-change . #'warp-coordinator-handle-propose-change))

(defun warp-coordinator--initialize-rpc-handlers (coordinator)
  "Registers all RPC handlers that this coordinator node will respond to.
This function is called during the coordinator's startup lifecycle.
It connects the RPC command names (as defined in `warp-protocol`) to
their respective handling functions within this module.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns: (loom-promise): A promise that resolves when handlers are
    registered.

Side Effects:
- Registers handler functions on the `command-router`."
  (warp:log! :debug (warp-coordinator-id coordinator)
             "Registering RPC handlers.")
  (warp:rpc-register-all-handlers
   (warp-coordinator-command-router coordinator)))

(defun warp-coordinator--start-timers (coordinator)
  "Starts the necessary timers for coordinator operation.
This includes the initial election timer for followers.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns: `nil`.

Side Effects:
- Schedules the initial `election-timer`."
  (warp:log! :debug (warp-coordinator-id coordinator) "Starting timers.")
  (warp-coordinator--reset-election-timer coordinator))

(defun warp-coordinator--connect-to-peers (coordinator)
  "Establishes connections to other coordinator nodes in the cluster.
This ensures the coordinator can send RPCs for leader election and
state propagation. It uses the `connection-manager` to handle the
transport.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns: (loom-promise): A promise that resolves when connections are
    established (or attempted).

Side Effects:
- Calls `warp:connection-manager-connect` to start the connection
    manager's internal connection process for its configured endpoints."
  (let* ((config (warp-coordinator-config coordinator))
         (members (warp-coordinator-config-cluster-members config))
         (cm (warp-coordinator-connection-manager coordinator)))
    (warp:log! :info (warp-coordinator-id coordinator)
               "Connecting to %d coordinator peers via Connection Manager."
               (length members))
    ;; The `warp:connection-manager-connect` with no arguments starts the
    ;; connection manager's process of establishing and maintaining
    ;; connections to all its configured endpoints (from `cluster-members`).
    ;; It returns a promise that resolves when the CM is actively trying
    ;; to connect. Individual peer RPCs will then leverage the CM's
    ;; established connections.
    (loom:await (warp:connection-manager-connect cm))
    (loom:resolved! t)))

(defun warp-coordinator--stop-timers (coordinator)
  "Stops all active timers for the coordinator instance.
This is called during shutdown to prevent lingering background tasks.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns: `nil`.

Side Effects:
- Cancels `election-timer` and `heartbeat-timer`."
  (when (warp-coordinator-election-timer coordinator)
    (cancel-timer (warp-coordinator-election-timer coordinator))
    (setf (warp-coordinator-election-timer coordinator) nil))
  (when (warp-coordinator-heartbeat-timer coordinator)
    (cancel-timer (warp-coordinator-heartbeat-timer coordinator))
    (setf (warp-coordinator-heartbeat-timer coordinator) nil))
  (warp:log! :debug (warp-coordinator-id coordinator) "All timers stopped."))

(defun warp-coordinator--disconnect-from-peers (coordinator)
  "Disconnects from all peer coordinator nodes.
Called during shutdown to release network resources.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.

Returns: (loom-promise): A promise resolving when disconnections are
    complete.

Side Effects:
- Calls `warp:connection-manager-shutdown` to close all active connections."
  (warp:log! :info (warp-coordinator-id coordinator) "Disconnecting from peers.")
  ;; `warp:connection-manager-shutdown` closes all active connections.
  (loom:await (warp:connection-manager-shutdown
               (warp-coordinator-connection-manager coordinator))))

(defun warp-coordinator--send-client-rpc (coordinator target-id command-payload)
  "Helper to send an RPC request from a client (this coordinator instance
acting as a client) to a target coordinator (likely the leader).

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance
    acting as the client.
- `target-id` (string): The ID of the coordinator to send the RPC to.
- `command-payload` (warp-protocol-command-payload): The specific
    command payload to send (e.g., lock request, barrier increment).

Returns: (loom-promise): A promise that resolves to the RPC response
    from the target, or rejects if the RPC fails (e.g., no connection,
    timeout).

Signals:
- `warp-coordinator-error`: If no active connection is available.
- `warp-rpc-error`: If the RPC request itself fails."
  (let* ((sender-id (warp-coordinator-id coordinator))
         (rpc-timeout (warp-coordinator-config-rpc-timeout
                       (warp-coordinator-config coordinator)))
         (conn (warp:connection-manager-get-connection
                (warp-coordinator-connection-manager coordinator))))
    ;; Critical: If no connection is active, RPCs cannot be sent.
    ;; This indicates a fundamental network or configuration issue.
    (unless conn
      (signal (warp:error! :type 'warp-coordinator-error
                           :message "No active connection to peers for RPC.")))
    (braid! (warp:rpc-request
             conn
             sender-id
             target-id
             command-payload
             :timeout rpc-timeout)
      (:then (lambda (response) response))
      (:catch (lambda (err)
                (warp:log! :warn sender-id
                           "RPC to %s failed: %S. Retrying..."
                           target-id err)
                (loom:rejected! (warp:error! :type 'warp-rpc-error
                                             :cause err)))))))

(defun warp-coordinator--handle-client-rpc-response (coordinator response
                                                    operation-name)
  "Helper to process the generic response from a client-initiated RPC.
This handles success/failure and leader redirection logic based on the
RPC response payload. It assumes the payload has `:granted-p` (or
`:success-p`), `:leader-id`, and `:error` fields.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `response` (warp-rpc-command): The RPC response received.
- `operation-name` (string): A descriptive name for the operation
    (e.g., 'Lock acquisition', 'Barrier Increment') for logging.

Returns: (plist): A plist indicating `(:success-p <boolean> :error <string>)`
    and potentially `:new-leader-id` (if redirection is needed).

Side Effects:
- May update `coordinator`'s `leader-id` based on response if a new
    leader is indicated.
- Logs the outcome of the operation."
  (let* ((payload (warp-rpc-command-payload response))
         ;; Using `:granted-p` as a generic success indicator, common in
         ;; coordinator protocols.
         (success-p (or (plist-get payload :granted-p)
                        (plist-get payload :success-p)))
         (leader-from-response (plist-get payload :leader-id))
         (error-msg (plist-get payload :error))
         (client-id (warp-coordinator-id coordinator)))

    (if success-p
        (progn
          (warp:log! :info client-id "%s successful." operation-name)
          `(:success-p t))
      (progn
        ;; If not successful, check if the response indicates a different
        ;; leader. If so, update our local knowledge of the leader.
        (when (and leader-from-response
                   (not (string= leader-from-response client-id))
                   (not (string= leader-from-response
                                 (warp-coordinator-leader-id coordinator))))
          (warp:log! :info client-id
                     "%s denied. New leader: %s. Updating leader ID."
                     operation-name leader-from-response)
          (setf (warp-coordinator-leader-id coordinator)
                leader-from-response))
        `(:success-p nil
          :error (format "%s failed: %S"
                         operation-name (or error-msg "unknown error"))
          :new-leader-id leader-from-response)))))

(defun warp-coordinator--rpc-call-with-leader-retry (coordinator command-fn
                                                     &key timeout client-id op-name)
  "Generic helper to make an RPC call to the leader with retry logic.
This function determines the current leader (or itself if leader is
unknown), sends the RPC, and retries if the call fails or indicates
a new leader. This forms an outer retry loop for client-initiated
distributed operations.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator.
- `command-fn` (function): A nullary function `(lambda ())` that
    returns the `warp-protocol-command` payload for the RPC.
- `:timeout` (number): RPC timeout for the overall operation attempt.
- `:client-id` (string): The ID of the client initiating this (usually
    `warp-coordinator-id coordinator`).
- `:op-name` (string): A descriptive name for the operation (e.g.,
    'Lock acquisition') for logging and error messages.

Returns: (loom-promise): A promise that resolves to the final RPC response
    from the leader on success, or rejects with a `warp-coordinator-error`
    type if all retries fail or a timeout occurs.

Signals:
- `warp-coordinator-timeout`: If retries exceed global timeout.
- `warp-coordinator-error`: For other failures."
  (let* ((req-timeout (or timeout
                          (warp-coordinator-config-rpc-timeout
                           (warp-coordinator-config coordinator))))
         (start-time (float-time)))
    ;; The outer `loom:loop!` manages the overall retry attempts,
    ;; including delays and timeouts, and leader redirection.
    (loom:loop!
      ;; First, check the overall timeout for the entire operation.
      (loom:when! (> (- (float-time) start-time) req-timeout)
        (warp:log! :error client-id "%s timed out after %.2fs."
                   op-name req-timeout)
        ;; If timed out, break from the loop with a rejection.
        (loom:break! (warp:error! :type 'warp-coordinator-timeout
                                  :message (format "%s timed out." op-name))))

      (let* ((target-id (or (warp-coordinator-leader-id coordinator)
                            client-id)) ;; Target leader, or self if leader unknown.
             ;; Define the RPC sending attempt function for `loom:retry`.
             (rpc-attempt-fn
              (lambda ()
                (warp-coordinator--send-client-rpc
                 coordinator target-id (funcall command-fn)))))

        ;; Use `loom:retry` for the RPC call itself to handle transient network
        ;; failures (e.g., dropped packets, temporary connection issues).
        (braid! (loom:retry
                 rpc-attempt-fn
                 :retries 2     ;; Short retry count for the inner RPC call.
                 :delay 0.5     ;; Small delay between inner RPC retries.
                 :pred (lambda (err) (cl-typep err 'warp-rpc-error)))
          (:then (lambda (response)
                   ;; Process the RPC response and check for success/redirection.
                   (let ((result (warp-coordinator--handle-client-rpc-response
                                  coordinator response op-name)))
                     (if (plist-get result :success-p)
                         (loom:break! response) ;; Success, exit `loom:loop!` with response.
                       (progn
                         ;; If the operation failed, and a new leader was identified,
                         ;; or it's a generic failure, retry the outer loop after a delay.
                         (loom:delay! 0.1 (loom:continue!)))))))
          (:catch (lambda (err)
                    ;; If the inner `loom:retry` for the RPC fails (all RPC retries exhausted),
                    ;; it means the RPC couldn't be sent or consistently failed.
                    ;; Log a warning and signal `loom:continue!` to re-enter the outer loop,
                    ;; allowing time for network recovery or leader re-election.
                    (warp:log! :warn client-id
                               "%s RPC to %s failed (after retries): %S. Retrying main loop..."
                               op-name target-id err)
                    (loom:delay! 0.5 (loom:continue!)))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:coordinator-get-lock (coordinator lock-name &key timeout)
  "Acquires a distributed lock. This function will attempt to obtain
a named lock, waiting if necessary until the lock is available or the
`timeout` expires. The request is always directed to the known leader.
If the current node is a follower, it attempts to redirect the request.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `lock-name` (string): The unique name of the lock to acquire.
- `:timeout` (number, optional): Maximum time (in seconds) to wait
    for the lock. Defaults to `warp-coordinator-config-rpc-timeout`.

Returns: (loom-promise): A promise that resolves to `t` if the lock is
    successfully acquired, or rejects with a `warp-coordinator-lock-failed`
    or `warp-coordinator-timeout` error on failure.

Side Effects:
- Sends RPCs to the cluster leader.
- Updates local `lock-registry` on successful acquisition.
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
               (warp:log! :info holder-id "Lock '%s' acquired." lock-name)
               (puthash lock-name expiry-time
                        (warp-coordinator-lock-registry coordinator))
               t))
      (:catch (lambda (err)
                (warp:log! :error holder-id
                           "Failed to acquire lock '%s': %S"
                           lock-name err)
                ;; Re-signal as a specific lock-failed error for consumers.
                (signal (warp:error! :type 'warp-coordinator-lock-failed
                                     :message (format "Failed to acquire lock %S"
                                                      lock-name)
                                     :cause err)))))))

;;;###autoload
(defun warp:coordinator-release-lock (coordinator lock-name)
  "Releases a previously acquired distributed lock.
The request is directed to the known leader. If this node is a follower,
it attempts to redirect the request to the leader.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `lock-name` (string): The name of the lock to release.

Returns: (loom-promise): A promise that resolves to `t` if the lock is
    successfully released, or rejects with a `warp-coordinator-lock-failed`
    error on failure."
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
               (warp:log! :info holder-id "Lock '%s' released." lock-name)
               (remhash lock-name
                        (warp-coordinator-lock-registry coordinator))
               t))
      (:catch (lambda (err)
                (warp:log! :error holder-id
                           "Failed to release lock '%s': %S"
                           lock-name err)
                ;; Re-signal as a specific lock-failed error for consumers.
                (signal (warp:error! :type 'warp-coordinator-lock-failed
                                     :message (format "Failed to release lock %S"
                                                      lock-name)
                                     :cause err)))))))

;;;###autoload
(defun warp:coordinator-propose-state-change (coordinator key value &key timeout)
  "Proposes a distributed state change. This operation can only be
initiated by the current leader (this node must be the leader). It
attempts to update the shared state in `warp-state-manager` via RPC to
itself (as the leader).

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `key` (list): The path to the state key (e.g., `(:config :my-setting)`).
- `value` (any): The new value for the state key.
- `:timeout` (number, optional): Maximum time (in seconds) to wait
    for the proposal to be applied. Defaults to
    `warp-coordinator-config-rpc-timeout`.

Returns: (loom-promise): A promise that resolves to `t` if the state
    change is successfully proposed and applied by the leader, or rejects
    with a `warp-coordinator-consensus-failed` or `warp-coordinator-timeout`
    error.

Signals:
- `warp-coordinator-not-leader`: If this coordinator is not the leader."
  (let* ((req-timeout (or timeout
                          (warp-coordinator-config-rpc-timeout
                           (warp-coordinator-config coordinator))))
         (proposer-id (warp-coordinator-id coordinator)))
    ;; Client-side check: This node *must* be the leader to propose changes.
    ;; This is a design choice for this simplified coordinator;
    ;; in a full Raft, followers would forward client requests to the leader.
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
               ;; already handles the authoritative state.
               (warp:log! :info proposer-id
                          "Proposed change for key '%S' successfully applied."
                          key)
               t))
      (:catch (lambda (err)
                (warp:log! :error proposer-id
                           "Failed to propose change for key '%S': %S"
                           key err)
                (signal (warp:error! :type 'warp-coordinator-consensus-failed
                                     :message (format "Failed to propose change for key %S"
                                                      key)
                                     :cause err)))))))

;;;###autoload
(defun warp:coordinator-wait-for-barrier (coordinator barrier-name
                                         total-participants &key timeout)
  "Waits for a distributed barrier to be met.
This function repeatedly attempts to increment a named barrier's count on
the current leader and waits until the `current-count` reaches
`total-participants` or a global `timeout` expires. It handles potential
leader changes and RPC failures by retrying the operation.

Arguments:
- `coordinator` (warp-coordinator-instance): The coordinator instance.
- `barrier-name` (string): The unique name of the barrier.
- `total-participants` (integer): The total number of participants
    required to meet the barrier.
- `:timeout` (number, optional): Maximum time (in seconds) to wait for
    the barrier to be met. Defaults to
    `warp-coordinator-config-rpc-timeout`.

Returns: (loom-promise): A promise that resolves to `t` if the barrier
    is met, or rejects with a `warp-coordinator-barrier-failed` or
    `warp-coordinator-timeout` error.

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
      (loom:when! (> (- (float-time) start-time) req-timeout)
        (warp:log! :error participant-id
                   "Barrier '%S' timed out after %.2fs."
                   barrier-name req-timeout)
        ;; If timed out, break from the loop with a rejection.
        (loom:break! (warp:error! :type 'warp-coordinator-timeout
                                  :message (format "Barrier '%S' timed out after %.2fs."
                                                   barrier-name req-timeout))))

      (warp:log! :debug participant-id
                 "Attempting to increment barrier '%s' (current role: %S)."
                 barrier-name (warp-coordinator-role coordinator))

      ;; Attempt to increment the barrier and handle the response.
      (braid! (warp-coordinator--rpc-call-with-leader-retry
               coordinator
               (lambda () ;; Command payload function for barrier-increment RPC.
                 (warp-protocol-make-command
                  :coordinator-barrier-increment
                  :barrier-name barrier-name
                  :participant-id participant-id
                  :total-participants total-participants))
               :timeout req-timeout ;; The outer loop handles overall timeout
               :client-id participant-id
               :op-name (format "Barrier increment for '%s'" barrier-name))
        (:then (lambda (response)
                 ;; Process the RPC response from the leader.
                 (let ((current-count
                        (warp-protocol-coordinator-barrier-response-payload-current-count
                         (warp-rpc-command-payload response)))
                       (is-met-p
                        (warp-protocol-coordinator-barrier-response-payload-is-met-p
                         (warp-rpc-command-payload response))))
                   (cond
                    (is-met-p
                     (warp:log! :info participant-id
                                "Barrier '%s' met. Count: %d/%d."
                                barrier-name current-count total-participants)
                     (loom:break! t)) ;; Barrier met, exit `loom:loop!` successfully.
                    (t
                     (warp:log! :debug participant-id
                                "Barrier '%s' incremented to %d/%d. Waiting..."
                                barrier-name current-count total-participants)
                     ;; If not met yet, continue the outer loop after a small delay.
                     (loom:delay! 0.1 (loom:continue!)))))))
        (:catch (lambda (err)
                  ;; If the `warp-coordinator--rpc-call-with-leader-retry` itself
                  ;; signals an error (e.g., all its retries failed, or a timeout
                  ;; specific to the RPC call occurred), log and continue the
                  ;; outer `loom:loop!`. This allows the overall barrier wait
                  ;; to continue trying, potentially with a new leader.
                  (warp:log! :warn participant-id
                             "Barrier increment attempt failed for '%s': %S. Retrying main loop..."
                             barrier-name err)
                  (loom:delay! 0.5 (loom:continue!))))))))

(provide 'warp-coordinator)
;;; warp-coordinator.el ends here