;;; warp-protocol.el --- Warp Component Communication Protocol -*- lexical-binding: t; -*-

;;; Commentary:
;; This module defines the high-level **protocol schemas** and provides a
;; **protocol client** for communication between Warp components. It acts
;; as a shared vocabulary or data contract layer, defining the specific
;; content of RPC calls and their binary serialization format via
;; Protobuf mappings.
;;
;; ## Architectural Role:
;;
;; 1.  **Shared Schemas**: It contains all the `warp:defschema` and
;;     `warp:defprotobuf-mapping` definitions for the payloads used in
;;     operational RPCs. This ensures components agree on both the
;;     structure and the wire format of the data they exchange.
;; 2.  **Protocol Client**: It introduces a `warp-protocol-client`
;;     component that encapsulates the logic for sending all standard
;;     protocol RPCs, providing a clean, high-level API.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-marshal)
(require 'warp-rpc)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-protocol-error
  "A generic error for `warp-protocol` operations."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

;;----------------------------------------------------------------------
;;; Worker RPC Payloads
;;----------------------------------------------------------------------

(warp:defschema warp-worker-ready-payload
    ((:constructor make-warp-worker-ready-payload))
  "Payload for the `:worker-ready` RPC sent to the control plane leader.
This message is critical for the initial handshake and registration.

Fields:
- `worker-id` (string): The unique identifier of the worker process.
- `rank` (integer): The numerical rank assigned to the worker.
- `status` (symbol): The initial status of the worker (e.g., `:starting`).
- `launch-id` (string): A one-time ID for this launch, for security.
- `leader-challenge-token` (string): The token provided by the leader.
- `worker-signature` (string): The worker's signature over the token.
- `worker-public-key` (string): The worker's public key for verification.
- `inbox-address` (string): The IPC address the worker is listening on.
- `pool-name` (string): The name of the pool this worker belongs to."
  (worker-id nil :type string)
  (rank nil :type integer)
  (status nil :type symbol)
  (launch-id nil :type (or null string))
  (leader-challenge-token nil :type (or null string))
  (worker-signature nil :type (or null string))
  (worker-public-key nil :type (or null string))
  (inbox-address nil :type (or null string))
  (pool-name nil :type (or null string)))

(warp:defprotobuf-mapping warp-worker-ready-payload
  "Protobuf mapping for `warp-worker-ready-payload` for efficient
binary serialization."
  `((worker-id 1 :string)
    (rank 2 :int32)
    (status 3 :string)
    (launch-id 4 :string)
    (leader-challenge-token 5 :string)
    (worker-signature 6 :string)
    (worker-public-key 7 :string)
    (inbox-address 8 :string)
    (pool-name 9 :string)))

(warp:defschema warp-worker-heartbeat-payload
    ((:constructor make-warp-worker-heartbeat-payload))
  "Payload for the periodic `:heartbeat` RPC to the control plane leader.
Note: The worker's public key is NOT sent with every heartbeat; it's
typically exchanged during the initial `:worker-ready` handshake.

Fields:
- `worker-id` (string): ID of the worker sending the heartbeat.
- `status` (symbol): Current operational status (e.g., `:running`).
- `timestamp` (float): `float-time` when the heartbeat was created.
- `services` (list): Lisp objects describing services on the worker.
- `metrics` (bytes): Serialized metrics hash-table (e.g., `warp-worker-metrics`)."
  (worker-id nil :type string)
  (status nil :type symbol)
  (timestamp (float-time) :type float)
  (services nil :type list)
  (metrics nil :type bytes)) ;; Added metrics field to schema and docstring

(warp:defprotobuf-mapping warp-worker-heartbeat-payload
  "Protobuf mapping for `warp-worker-heartbeat-payload`."
  `((worker-id 1 :string)
    (status 2 :string)
    (timestamp 3 :double)
    (services 4 :bytes) ;; Services (list of plists) are marshaled to bytes
    (metrics 5 :bytes))) ;; Metrics (hash-table) are marshaled to bytes

(warp:defschema warp-get-init-payload-args
    ((:constructor make-warp-get-init-payload-args))
  "Arguments for the `:get-init-payload` RPC sent from a worker.
Allows a worker to request custom, rank-specific configuration.

Fields:
- `worker-id` (string): The unique ID of the worker making the request.
- `rank` (integer): The rank for which the payload is being requested."
  (worker-id nil :type string)
  (rank nil :type integer))

(warp:defprotobuf-mapping warp-get-init-payload-args
  "Protobuf mapping for `warp-get-init-payload-args`."
  `((worker-id 1 :string)
    (rank 2 :int32)))

(warp:defschema warp-ping-payload
    ((:constructor make-warp-ping-payload))
  "Payload for `:ping` requests and their responses.
Used for basic liveness checks and round-trip time measurements.

Fields:
- `message` (string): Optional message, e.g., \"ping\" or \"pong\".
- `timestamp` (float): The `float-time` of message creation."
  (message nil :type (or null string))
  (timestamp (float-time) :type float))

(warp:defprotobuf-mapping warp-ping-payload
  "Protobuf mapping for `warp-ping-payload`."
  `((message 1 :string)
    (timestamp 2 :double)))

;;----------------------------------------------------------------------
;;; Map-Reduce RPC Payloads
;;----------------------------------------------------------------------

(warp:defschema warp-cluster-map-payload
    ((:constructor make-warp-cluster-map-payload))
  "Payload for `:evaluate-map-chunk` RPC for a map-reduce operation.

Fields:
- `function-form` (t): A Lisp form (code) that evaluates to the map fn.
- `chunk-data` (list): The list of data items for the worker to process."
  (function-form nil :type t)
  (chunk-data nil :type list))

(warp:defprotobuf-mapping warp-cluster-map-payload
  "Protobuf mapping for `warp-cluster-map-payload`."
  `((function-form 1 :bytes) ;; Lisp form marshaled to bytes
    (chunk-data 2 :bytes)))  ;; List data marshaled to bytes

(warp:defschema warp-cluster-map-result
    ((:constructor make-warp-cluster-map-result))
  "Result payload for an `:evaluate-map-chunk` RPC.

Fields:
- `results` (list): Results after applying the map function."
  (results nil :type list))

(warp:defprotobuf-mapping warp-cluster-map-result
  "Protobuf mapping for `warp-cluster-map-result`."
  `((results 1 :bytes))) ;; List of results marshaled to bytes

;;----------------------------------------------------------------------
;;; Provisioning RPC Payloads
;;----------------------------------------------------------------------

(warp:defschema warp-provision-request-payload
    ((:constructor make-warp-provision-request-payload))
  "Payload for an RPC from a worker requesting its provision.

Fields:
- `worker-id` (string): ID of the worker requesting the provision.
- `current-version` (string): Version of the provision the worker has.
- `provision-type` (keyword): Type of provision requested."
  (worker-id nil :type string)
  (current-version nil :type (or null string))
  (provision-type :worker-provision :type keyword))

(warp:defprotobuf-mapping warp-provision-request-payload
  "Protobuf mapping for `warp-provision-request-payload`."
  `((worker-id 1 :string)
    (current-version 2 :string)
    (provision-type 3 :string)))

(warp:defschema warp-provision-response-payload
    ((:constructor make-warp-provision-response-payload))
  "Payload for the leader's RPC response containing the provision.

Fields:
- `version` (string): The version identifier of the returned provision.
- `provision` (t): The provision object itself."
  (version nil :type string)
  (provision nil :type t))

(warp:defprotobuf-mapping warp-provision-response-payload
  "Protobuf mapping for `warp-provision-response-payload`."
  `((version 1 :string)
    (provision 2 :bytes))) ;; Provision data (arbitrary Lisp object) marshaled to bytes

(warp:defschema warp-provision-update-payload
    ((:constructor make-warp-provision-update-payload))
  "Payload for a provision update event pushed from the leader to workers.

Fields:
- `version` (string): The new version identifier of the provision.
- `provision` (t): The new provision object.
- `provision-type` (keyword): The type of provision being updated.
- `target-ids` (list): Optional list of specific worker IDs to target."
  (version nil :type string)
  (provision nil :type t)
  (provision-type :worker-provision :type keyword)
  (target-ids nil :type (or null list)))

(warp:defprotobuf-mapping warp-provision-update-payload
  "Protobuf mapping for `warp-provision-update-payload`."
  `((version 1 :string)
    (provision 2 :bytes) ;; Provision data marshaled to bytes
    (provision-type 3 :string)
    (target-ids 4 :bytes))) ;; List of strings marshaled to bytes

(warp:defschema warp-provision-jwt-keys-payload
    ((:constructor make-warp-provision-jwt-keys-payload))
  "Provision payload for distributing trusted JWT public keys.

Fields:
- `trusted-keys` (list): An alist or plist mapping key IDs to
  PEM-encoded public keys."
  (trusted-keys nil :type (or null list)))

(warp:defprotobuf-mapping warp-provision-jwt-keys-payload
  "Protobuf mapping for `warp-provision-jwt-keys-payload`."
  `((trusted-keys 1 :bytes))) ;; List of (key . value) pairs marshaled to bytes

;;----------------------------------------------------------------------
;;; Coordination RPC Payloads
;;----------------------------------------------------------------------

(warp:defschema warp-propagate-event-payload
    ((:constructor make-warp-propagate-event-payload))
  "Payload for `propagate-event` RPC sent to the event broker.

Fields:
- `event` (warp-event): The `warp-event` object to be propagated."
  (event nil :type warp-event))

(warp:defprotobuf-mapping warp-propagate-event-payload
  "Protobuf mapping for `warp-propagate-event-payload`."
  `((event 1 :bytes))) ;; Event struct marshaled to bytes

(warp:defschema warp-coordinator-get-leader-response
    ((:constructor make-warp-coordinator-get-leader-response))
  "Response payload for `:get-coordinator-leader` RPC.
This allows a node to query a coordinator peer for the current leader's
advertised address.

Fields:
- `leader-id` (string): The ID of the currently elected leader.
- `leader-address` (string): The public contact address of the leader."
  (leader-id nil :type (or null string))
  (leader-address nil :type (or null string)))

(warp:defprotobuf-mapping warp-coordinator-get-leader-response
  "Protobuf mapping for `warp-coordinator-get-leader-response`."
  `((leader-id 1 :string)
    (leader-address 2 :string)))

(warp:defschema warp-coordinator-request-vote-payload
    ((:constructor make-warp-coordinator-request-vote-payload))
  "Payload for `RequestVote` RPC in Raft-like consensus.

Fields:
- `term` (integer): Candidate's term.
- `candidate-id` (string): Candidate requesting vote.
- `last-log-index` (integer): Index of candidate's last log entry.
- `last-log-term` (integer): Term of candidate's last log entry."
  (term nil :type integer)
  (candidate-id nil :type string)
  (last-log-index nil :type integer)
  (last-log-term nil :type integer))

(warp:defprotobuf-mapping warp-coordinator-request-vote-payload
  "Protobuf mapping for `warp-coordinator-request-vote-payload`."
  `((term 1 :int32)
    (candidate-id 2 :string)
    (last-log-index 3 :int32)
    (last-log-term 4 :int32)))

(warp:defschema warp-coordinator-request-vote-response-payload
    ((:constructor make-warp-coordinator-request-vote-response-payload))
  "Response payload for `RequestVote` RPC.

Fields:
- `term` (integer): Current term of responder.
- `vote-granted-p` (boolean): `t` if candidate received vote.
- `error` (string): Optional error message if vote not granted."
  (term nil :type integer)
  (vote-granted-p nil :type boolean)
  (error nil :type (or null string)))

(warp:defprotobuf-mapping warp-coordinator-request-vote-response-payload
  "Protobuf mapping for `warp-coordinator-request-vote-response-payload`."
  `((term 1 :int32)
    (vote-granted-p 2 :bool)
    (error 3 :string)))

(warp:defschema warp-coordinator-append-entries-payload
    ((:constructor make-warp-coordinator-append-entries-payload))
  "Payload for `AppendEntries` RPC (heartbeats and log replication).

Fields:
- `term` (integer): Leader's term.
- `leader-id` (string): Leader's ID.
- `prev-log-index` (integer): Index of log entry immediately preceding
  new ones.
- `prev-log-term` (integer): Term of `prev-log-index` entry.
- `entries` (list): Log entries to append (empty for heartbeats).
- `leader-commit` (integer): Leader's commit index."
  (term nil :type integer)
  (leader-id nil :type string)
  (prev-log-index nil :type integer)
  (prev-log-term nil :type integer)
  (entries nil :type list)
  (leader-commit nil :type integer))

(warp:defprotobuf-mapping warp-coordinator-append-entries-payload
  "Protobuf mapping for `warp-coordinator-append-entries-payload`."
  `((term 1 :int32)
    (leader-id 2 :string)
    (prev-log-index 3 :int32)
    (prev-log-term 4 :int32)
    (entries 5 :bytes) ; Log entries themselves are marshaled as bytes
    (leader-commit 6 :int32)))

(warp:defschema warp-coordinator-append-entries-response-payload
    ((:constructor make-warp-coordinator-append-entries-response-payload))
  "Response payload for `AppendEntries` RPC.

Fields:
- `term` (integer): Current term of follower.
- `success-p` (boolean): `t` if follower contained `prev-log-index` and
  `prev-log-term`.
- `error` (string): Optional error message."
  (term nil :type integer)
  (success-p nil :type boolean)
  (error nil :type (or null string)))

(warp:defprotobuf-mapping warp-coordinator-append-entries-response-payload
  "Protobuf mapping for `warp-coordinator-append-entries-response-payload`."
  `((term 1 :int32)
    (success-p 2 :bool)
    (error 3 :string)))

(warp:defschema warp-coordinator-lock-request-payload
    ((:constructor make-warp-coordinator-lock-request-payload))
  "Payload for `acquire-lock` or `release-lock` RPC.

Fields:
- `lock-name` (string): Name of the distributed lock.
- `holder-id` (string): ID of the client trying to acquire/release.
- `expiry-time` (float): Desired expiry for acquisition."
  (lock-name nil :type string)
  (holder-id nil :type string)
  (expiry-time nil :type float))

(warp:defprotobuf-mapping warp-coordinator-lock-request-payload
  "Protobuf mapping for `warp-coordinator-lock-request-payload`."
  `((lock-name 1 :string)
    (holder-id 2 :string)
    (expiry-time 3 :double)))

(warp:defschema warp-coordinator-lock-response-payload
    ((:constructor make-warp-coordinator-lock-response-payload))
  "Response payload for `acquire-lock` or `release-lock` RPC.

Fields:
- `granted-p` (boolean): `t` if lock was acquired/released, `nil` if denied.
- `leader-id` (string): Current leader ID, for redirection.
- `error` (string): Optional error message."
  (granted-p nil :type boolean)
  (leader-id nil :type (or null string))
  (error nil :type (or null string)))

(warp:defprotobuf-mapping warp-coordinator-lock-response-payload
  "Protobuf mapping for `warp-coordinator-lock-response-payload`."
  `((granted-p 1 :bool)
    (leader-id 2 :string)
    (error 3 :string)))

(warp:defschema warp-coordinator-barrier-increment-payload
    ((:constructor make-warp-coordinator-barrier-increment-payload))
  "Payload for `barrier-increment` RPC.

Fields:
- `barrier-name` (string): Name of the distributed barrier.
- `participant-id` (string): ID of the participant.
- `total-participants` (integer): Target count for the barrier."
  (barrier-name nil :type string)
  (participant-id nil :type string)
  (total-participants nil :type integer))

(warp:defprotobuf-mapping warp-coordinator-barrier-increment-payload
  "Protobuf mapping for `warp-coordinator-barrier-increment-payload`."
  `((barrier-name 1 :string)
    (participant-id 2 :string)
    (total-participants 3 :int32)))

(warp:defschema warp-coordinator-barrier-response-payload
    ((:constructor make-warp-coordinator-barrier-response-payload))
  "Response payload for `barrier-increment` RPC.

Fields:
- `success-p` (boolean): `t` if increment was successful.
- `current-count` (integer): Current participants at the barrier.
- `is-met-p` (boolean): `t` if barrier is now met.
- `error` (string): Optional error message."
  (success-p nil :type boolean)
  (current-count nil :type integer)
  (is-met-p nil :type boolean)
  (error nil :type (or null string)))

(warp:defprotobuf-mapping warp-coordinator-barrier-response-payload
  "Protobuf mapping for `warp-coordinator-barrier-response-payload`."
  `((success-p 1 :bool)
    (current-count 2 :int32)
    (is-met-p 3 :bool)
    (error 4 :string)))

(warp:defschema warp-coordinator-propose-change-payload
    ((:constructor make-warp-coordinator-propose-change-payload))
  "Payload for `propose-change` RPC to the leader.

Fields:
- `key` (list): The state path to change.
- `value` (t): The new value for the state path."
  (key nil :type list)
  (value nil :type t))

(warp:defprotobuf-mapping warp-coordinator-propose-change-payload
  "Protobuf mapping for `warp-coordinator-propose-change-payload`."
  `((key 1 :bytes) ; Key (path list) marshaled as bytes.
    (value 2 :bytes))) ; Value (arbitrary Lisp data) marshaled as bytes.

(warp:defschema warp-coordinator-propose-change-response-payload
    ((:constructor make-warp-coordinator-propose-change-response-payload))
  "Response payload for `propose-change` RPC.

Fields:
- `success-p` (boolean): `t` if change was applied.
- `error` (string): Optional error message."
  (success-p nil :type boolean)
  (error nil :type (or null string)))

(warp:defprotobuf-mapping warp-coordinator-propose-change-response-payload
  "Protobuf mapping for `warp-coordinator-propose-change-response-payload`."
  `((success-p 1 :bool)
    (error 2 :string)))

(warp:defschema warp-get-all-active-workers-args
    ((:constructor make-warp-get-all-active-workers-args))
  "Empty args payload for `:get-all-active-workers` RPC from event broker.
This RPC requests a list of all active worker IDs and their addresses
from the master's registry.

Fields: None.")

(warp:defprotobuf-mapping warp-get-all-active-workers-args
  "Protobuf mapping for `warp-get-all-active-workers-args`. No fields."
  `())

(warp:defschema warp-get-all-active-workers-response-payload
    ((:constructor make-warp-get-all-active-workers-response-payload))
  "Response payload for `:get-all-active-workers` RPC.

Fields:
- `active-workers` (list): A list of plists, where each plist has
  `:worker-id` (string) and `:inbox-address` (string)."
  (active-workers nil :type list))

(warp:defprotobuf-mapping warp-get-all-active-workers-response-payload
  "Protobuf mapping for `warp-get-all-active-workers-response-payload`."
  `((active-workers 1 :bytes))) ; List of plists marshaled as bytes.

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-protocol-client
               (:constructor %%make-protocol-client))
  "A component providing a high-level API for sending protocol RPCs.
This acts as a 'client facade' over the lower-level `rpc-system`,
offering convenience functions for sending specific, protocol-defined
messages without needing to manually construct RPC commands.

Fields:
- `rpc-system` (warp-rpc-system): The underlying RPC system used to
  actually send the requests."
  (rpc-system (cl-assert nil) :type (or null t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-protocol--make-command
    (name args-constructor &rest args-plist)
  "Constructs a `warp-rpc-command` object with its payload.
This helper standardizes the creation of RPC command objects by taking
a command name, a schema constructor for its arguments, and a plist of
those arguments, then wrapping them into a `warp-rpc-command` struct. The
payload is automatically serialized to bytes via `warp:marshal` if needed
(e.g., for `t` or `list` types in the schema).

Arguments:
- `NAME` (keyword): The symbolic name of the RPC command (e.g., `:ping`).
- `ARGS-CONSTRUCTOR` (function): The constructor for the payload schema
  (e.g., `#'make-warp-ping-payload`).
- `&rest ARGS-PLIST` (plist): A property list of arguments for the
  payload, passed directly to `ARGS-CONSTRUCTOR`.

Returns:
- (warp-rpc-command): A new, fully-formed RPC command object, ready to
  be included in a `warp-rpc-message`."
  (let ((payload (apply args-constructor args-plist)))
    (make-warp-rpc-command :name name :args payload)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:protocol-client-create (&key rpc-system)
  "Creates a new `warp-protocol-client` component.
This factory function should be used within a `warp:defcomponent`
definition.

Arguments:
- `:rpc-system` (warp-rpc-system): The `rpc-system` component that
  this client will use for sending all requests.

Returns:
- (warp-protocol-client): A new protocol client instance."
  (%%make-protocol-client :rpc-system rpc-system))

;;;###autoload
(cl-defun warp:protocol-send-worker-ready
    (rpc-system connection worker-id rank status leader-id
                &key launch-id
                challenge-token
                signature
                public-key
                inbox-address
                (expect-response t)
                origin-instance-id
                pool-name)
  "Sends a `:worker-ready` notification from a worker to the leader.
This is the initial handshake RPC used by a worker to announce its
presence and status to the control plane, and perform a secure challenge.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system managing the request.
- `CONNECTION` (t): The active `warp-transport` connection to the leader.
- `WORKER-ID` (string): The unique identifier of the worker.
- `RANK` (integer): The worker's assigned rank.
- `STATUS` (keyword): The worker's current status (e.g., `:starting`).
- `LEADER-ID` (string): The ID of the leader node to send the message to.
- `:launch-id` (string, optional): A one-time ID for this launch, for security.
- `:challenge-token` (string, optional): The cryptographic challenge token.
- `:signature` (string, optional): The worker's digital signature over the token.
- `:public-key` (string, optional): The worker's public key for verification.
- `:inbox-address` (string, optional): The IPC address the worker is listening on.
- `:pool-name` (string, optional): The name of the worker pool this worker belongs to.
- `:expect-response` (boolean, optional): Whether a response is expected.
- `:origin-instance-id` (string, optional): ID of the component system instance.

Returns:
- (loom-promise or nil): A promise for the response if `expect-response`
  is `t`, otherwise `nil`."
  (let ((cmd (warp-protocol--make-command
              :worker-ready #'make-warp-worker-ready-payload
              :worker-id worker-id :rank rank :status status
              :launch-id launch-id
              :leader-challenge-token challenge-token
              :worker-signature signature
              :worker-public-key public-key
              :inbox-address inbox-address
              :pool-name pool-name)))
    (warp:rpc-request rpc-system connection worker-id leader-id cmd
                      :expect-response expect-response
                      :origin-instance-id origin-instance-id)))

;;;###autoload
(cl-defun warp:protocol-send-heartbeat
    (rpc-system connection worker-id status services leader-id
                &key metrics (expect-response nil) origin-instance-id) ;; Added metrics, public-key was removed.
  "Sends a periodic `:heartbeat` from a worker to the leader.
Heartbeats are used to report the worker's liveness and current metrics.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system managing the request.
- `CONNECTION` (t): The active `warp-transport` connection to the leader.
- `WORKER-ID` (string): The ID of the worker sending the heartbeat.
- `STATUS` (keyword): The worker's current operational status.
- `SERVICES` (list): A list describing services hosted by the worker.
- `LEADER-ID` (string): The ID of the leader node to send the message to.
- `:metrics` (bytes): Serialized `warp-worker-metrics` data.
- `:expect-response` (boolean, optional): Whether a response is expected.
- `:origin-instance-id` (string, optional): ID of the component system instance.

Returns:
- (loom-promise or nil): A promise for the response if `expect-response`
  is `t`, otherwise `nil`."
  (let ((cmd (warp-protocol--make-command
              :heartbeat #'make-warp-worker-heartbeat-payload
              :worker-id worker-id :status status :services services :metrics metrics)))
    (warp:rpc-request rpc-system connection worker-id leader-id cmd
                      :expect-response expect-response
                      :origin-instance-id origin-instance-id)))

;;;###autoload
(cl-defun warp:protocol-ping (rpc-system
                              connection
                              sender-id
                              recipient-id
                              &key (expect-response t)
                              origin-instance-id)
  "Sends a `:ping` RPC to check liveness between two nodes.
This is a basic liveness probe or round-trip time measurement.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system managing the request.
- `CONNECTION` (t): The `warp-transport` connection to the target node.
- `SENDER-ID` (string): The unique ID of the node sending the ping.
- `RECIPIENT-ID` (string): The unique ID of the node being pinged.
- `:expect-response` (boolean, optional): Whether a response is expected.
- `:origin-instance-id` (string, optional): ID of the component system instance.

Returns:
- (loom-promise or nil): A promise for the response (e.g., \"pong\") if
  `expect-response` is `t`, otherwise `nil`."
  (let ((cmd (warp-protocol--make-command
              :ping #'make-warp-ping-payload
              :message "ping")))
    (warp:rpc-request rpc-system connection sender-id recipient-id cmd
                      :expect-response expect-response
                      :origin-instance-id origin-instance-id)))

;;;###autoload
(cl-defun warp:protocol-request-map-chunk (client
                                            connection
                                            control-plane-id
                                            worker-id
                                            function-form
                                            chunk-data
                                            &key (expect-response t)
                                            origin-instance-id)
  "Requests a worker to evaluate a map function over a chunk of data.
This RPC is part of a distributed map-reduce pattern, allowing a leader
to distribute computation to workers.

Arguments:
- `CLIENT` (warp-protocol-client): The protocol client instance.
- `CONNECTION` (t): The connection to the target worker.
- `CONTROL-PLANE-ID` (string): The leader's unique ID (acting as the sender).
- `WORKER-ID` (string): The target worker's unique ID.
- `FUNCTION-FORM` (form): S-expression that evaluates to the map function.
- `CHUNK-DATA` (list): The list of data items for the worker to process.
- `:expect-response` (boolean, optional): Whether a response is expected.
- `:origin-instance-id` (string, optional): ID of the component system instance.

Returns:
- (loom-promise or nil): A promise for the `warp-cluster-map-result` if
  `expect-response` is `t`, otherwise `nil`."
  (let ((cmd (warp-protocol--make-command
              :evaluate-map-chunk #'make-warp-cluster-map-payload
              :function-form function-form :chunk-data chunk-data)))
    (warp:rpc-request (warp-protocol-client-rpc-system client)
                      connection control-plane-id worker-id cmd
                      :expect-response expect-response
                      :origin-instance-id origin-instance-id)))

;;;###autoload
(cl-defun warp:protocol-request-provision (client
                                            connection
                                            worker-id
                                            leader-id
                                            current-version
                                            provision-type
                                            &key (expect-response t)
                                            origin-instance-id)
  "Sends a `:provision-request` RPC from a worker to the leader.
Workers use this to fetch their configuration or other provisioning data.

Arguments:
- `CLIENT` (warp-protocol-client): The protocol client instance.
- `CONNECTION` (t): The active `warp-transport` connection to the leader.
- `WORKER-ID` (string): The ID of the worker requesting the provision.
- `LEADER-ID` (string): The ID of the leader node to send the message to.
- `CURRENT-VERSION` (string): The version of the provision the worker has.
- `PROVISION-TYPE` (keyword): The type of provision requested.
- `:expect-response` (boolean, optional): Whether a response is expected.
- `:origin-instance-id` (string, optional): ID of the component system instance.

Returns:
- (loom-promise or nil): A promise for the `warp-provision-response-payload`
  if `expect-response` is `t`, otherwise `nil`."
  (let ((cmd (warp-protocol--make-command
              :get-provision #'make-warp-provision-request-payload
              :worker-id worker-id
              :current-version current-version
              :provision-type provision-type)))
    (warp:rpc-request (warp-protocol-client-rpc-system client)
                      connection worker-id leader-id cmd
                      :expect-response expect-response
                      :origin-instance-id origin-instance-id)))

;;;###autoload
(cl-defun warp:protocol-publish-provision (rpc-system
                                            connection
                                            sender-id
                                            recipient-id
                                            version
                                            provision-obj
                                            provision-type
                                            &key target-ids
                                            (expect-response nil)
                                            origin-instance-id)
  "Sends a `:provision-update` event from the leader to workers.
The leader uses this to push configuration updates to workers.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system managing the request.
- `CONNECTION` (t): The `warp-transport` connection to the target node.
- `SENDER-ID` (string): ID of the leader sending the update.
- `RECIPIENT-ID` (string): ID of the target worker (or \"cluster\").
- `VERSION` (string): The new version identifier of the provision.
- `PROVISION-OBJ` (any): The new provision object.
- `PROVISION-TYPE` (keyword): The type of provision being updated.
- `:target-ids` (list, optional): Optional list of specific worker IDs to target.
- `:expect-response` (boolean, optional): Whether a response is expected.
- `:origin-instance-id` (string, optional): ID of the component system instance.

Returns:
- (loom-promise or nil): A promise for the response if `expect-response`
  is `t`, otherwise `nil`."
  (let ((cmd (warp-protocol--make-command
              :provision-update #'make-warp-provision-update-payload
              :version version
              :provision provision-obj
              :provision-type provision-type
              :target-ids target-ids)))
    (warp:rpc-request rpc-system connection sender-id recipient-id cmd
                      :expect-response expect-response
                      :origin-instance-id origin-instance-id)))

;;;###autoload
(cl-defun warp:protocol-send-distributed-event
    (rpc-system connection sender-id recipient-id event &key origin-instance-id)
  "Sends a distributed event to a target event broker worker.
This is used by event systems to propagate events with `:cluster` or
`:global` scope.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system.
- `CONNECTION` (t): The `warp-transport` connection to the broker.
- `SENDER-ID` (string): The ID of the event's original emitter.
- `RECIPIENT-ID` (string): The ID of the event broker (target).
- `EVENT` (warp-event): The `warp-event` object to propagate.
- `:origin-instance-id` (string, optional): ID of the component system instance.

Returns:
- (loom-promise): A promise that resolves when the event is sent to the broker."
  (let ((cmd (warp-protocol--make-command
              :propagate-event #'make-warp-propagate-event-payload
              :event event)))
    (warp:rpc-request rpc-system connection sender-id recipient-id cmd
                      :expect-response nil
                      :origin-instance-id origin-instance-id)))

;;;###autoload
(cl-defun warp:protocol-get-coordinator-leader (rpc-system 
                                                connection 
                                                sender-id 
                                                recipient-id 
                                                &key origin-instance-id)
  "Requests the current leader's address from a coordinator peer.
This RPC is used by workers to dynamically discover the active leader
for the control plane.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system.
- `CONNECTION` (t): The `warp-transport` connection to the coordinator peer.
- `SENDER-ID` (string): The ID of the requesting node (e.g., worker ID).
- `RECIPIENT-ID` (string): The ID of the coordinator peer to query.
- `:origin-instance-id` (string, optional): ID of the component system instance.

Returns:
- (loom-promise): A promise that resolves with a `warp-coordinator-get-leader-response`
  containing the leader's ID and address, or rejects on error."
  (let ((cmd (make-warp-rpc-command :name :get-coordinator-leader))) ;; No args needed for this request
    (warp:rpc-request rpc-system connection sender-id recipient-id cmd
                      :expect-response t
                      :origin-instance-id origin-instance-id)))

;;;###autoload
(cl-defun warp:protocol-coordinator-request-vote (rpc-system
                                                  connection
                                                  sender-id
                                                  recipient-id
                                                  candidate-id
                                                  term
                                                  last-log-index
                                                  last-log-term
                                                  &key origin-instance-id)
  "Sends a `RequestVote` RPC in Raft-like consensus.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system.
- `CONNECTION` (t): The `warp-transport` connection to the peer.
- `SENDER-ID` (string): The ID of the candidate sending the request.
- `RECIPIENT-ID` (string): The ID of the peer to request vote from.
- `CANDIDATE-ID` (string): The ID of the candidate requesting vote.
- `TERM` (integer): Candidate's current term.
- `LAST-LOG-INDEX` (integer): Index of candidate's last log entry.
- `LAST-LOG-TERM` (integer): Term of candidate's last log entry.
- `:origin-instance-id` (string, optional): ID of the component system instance.

Returns:
- (loom-promise): A promise that resolves with the response payload."
  (let ((cmd (warp-protocol--make-command
              :coordinator-request-vote #'make-warp-coordinator-request-vote-payload
              :term term :candidate-id candidate-id
              :last-log-index last-log-index :last-log-term last-log-term)))
    (warp:rpc-request rpc-system connection sender-id recipient-id cmd
                      :expect-response t
                      :origin-instance-id origin-instance-id)))

;;;###autoload
(cl-defun warp:protocol-coordinator-append-entries (rpc-system
                                                    connection
                                                    sender-id
                                                    recipient-id
                                                    term
                                                    leader-id
                                                    prev-log-index
                                                    prev-log-term
                                                    entries
                                                    leader-commit
                                                    &key origin-instance-id)
  "Sends an `AppendEntries` RPC (heartbeats and log replication) in Raft-like consensus.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system.
- `CONNECTION` (t): The `warp-transport` connection to the peer.
- `SENDER-ID` (string): The ID of the leader sending the request.
- `RECIPIENT-ID` (string): The ID of the peer.
- `TERM` (integer): Leader's current term.
- `LEADER-ID` (string): Leader's ID.
- `PREV-LOG-INDEX` (integer): Index of log entry immediately preceding new ones.
- `PREV-LOG-TERM` (integer): Term of `prev-log-index` entry.
- `ENTRIES` (list): Log entries to append (empty for heartbeats).
- `LEADER-COMMIT` (integer): Leader's commit index.
- `:origin-instance-id` (string, optional): ID of the component system instance.

Returns:
- (loom-promise): A promise that resolves with the response payload."
  (let ((cmd (warp-protocol--make-command
              :coordinator-append-entries #'make-warp-coordinator-append-entries-payload
              :term term :leader-id leader-id
              :prev-log-index prev-log-index :prev-log-term prev-log-term
              :entries entries :leader-commit leader-commit)))
    (warp:rpc-request rpc-system connection sender-id recipient-id cmd
                      :expect-response t
                      :origin-instance-id origin-instance-id)))

;;;###autoload
(cl-defun warp:protocol-coordinator-acquire-lock (rpc-system
                                                  connection
                                                  sender-id
                                                  recipient-id
                                                  lock-name
                                                  holder-id
                                                  expiry-time
                                                  &key origin-instance-id)
  "Sends an `acquire-lock` RPC to a coordinator.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system.
- `CONNECTION` (t): The `warp-transport` connection to the coordinator.
- `SENDER-ID` (string): The ID of the client sending the request.
- `RECIPIENT-ID` (string): The ID of the target coordinator.
- `LOCK-NAME` (string): The name of the lock.
- `HOLDER-ID` (string): The ID of the client trying to acquire.
- `EXPIRY-TIME` (float): Desired expiry for acquisition.
- `:origin-instance-id` (string, optional): ID of the component system instance.

Returns:
- (loom-promise): A promise that resolves with the response payload."
  (let ((cmd (warp-protocol--make-command
              :coordinator-acquire-lock #'make-warp-coordinator-lock-request-payload
              :lock-name lock-name :holder-id holder-id :expiry-time expiry-time)))
    (warp:rpc-request rpc-system connection sender-id recipient-id cmd
                      :expect-response t
                      :origin-instance-id origin-instance-id)))

;;;###autoload
(cl-defun warp:protocol-coordinator-release-lock (rpc-system
                                                  connection
                                                  sender-id
                                                  recipient-id
                                                  lock-name
                                                  holder-id
                                                  &key origin-instance-id)
  "Sends a `release-lock` RPC to a coordinator.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system.
- `CONNECTION` (t): The `warp-transport` connection to the coordinator.
- `SENDER-ID` (string): The ID of the client sending the request.
- `RECIPIENT-ID` (string): The ID of the target coordinator.
- `LOCK-NAME` (string): The name of the lock.
- `HOLDER-ID` (string): The ID of the client trying to release.
- `:origin-instance-id` (string, optional): ID of the component system instance.

Returns:
- (loom-promise): A promise that resolves with the response payload."
  (let ((cmd (warp-protocol--make-command
              :coordinator-release-lock #'make-warp-coordinator-lock-request-payload
              :lock-name lock-name :holder-id holder-id)))
    (warp:rpc-request rpc-system connection sender-id recipient-id cmd
                      :expect-response t
                      :origin-instance-id origin-instance-id)))

;;;###autoload
(cl-defun warp:protocol-coordinator-barrier-increment (rpc-system
                                                       connection
                                                       sender-id
                                                       recipient-id
                                                       barrier-name
                                                       participant-id
                                                       total-participants
                                                       &key origin-instance-id)
  "Sends a `barrier-increment` RPC to a coordinator.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system.
- `CONNECTION` (t): The `warp-transport` connection to the coordinator.
- `SENDER-ID` (string): The ID of the participant.
- `RECIPIENT-ID` (string): The ID of the target coordinator.
- `BARRIER-NAME` (string): The name of the barrier.
- `PARTICIPANT-ID` (string): The ID of the participant.
- `TOTAL-PARTICIPANTS` (integer): The target count for the barrier.
- `:origin-instance-id` (string, optional): ID of the component system instance.

Returns:
- (loom-promise): A promise that resolves with the response payload."
  (let ((cmd (warp-protocol--make-command
              :coordinator-barrier-increment #'make-warp-coordinator-barrier-increment-payload
              :barrier-name barrier-name :participant-id participant-id
              :total-participants total-participants)))
    (warp:rpc-request rpc-system connection sender-id recipient-id cmd
                      :expect-response t
                      :origin-instance-id origin-instance-id)))

;;;###autoload
(cl-defun warp:protocol-coordinator-propose-change
    (rpc-system connection sender-id recipient-id key value &key origin-instance-id)
  "Sends a `propose-change` RPC to a coordinator leader.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system.
- `CONNECTION` (t): The `warp-transport` connection to the coordinator.
- `SENDER-ID` (string): The ID of the client proposing the change.
- `RECIPIENT-ID` (string): The ID of the target coordinator (leader).
- `KEY` (list): The state path to change.
- `VALUE` (t): The new value for the state path.
- `:origin-instance-id` (string, optional): ID of the component system instance.

Returns:
- (loom-promise): A promise that resolves with the response payload."
  (let ((cmd (warp-protocol--make-command
              :coordinator-propose-change #'make-warp-coordinator-propose-change-payload
              :key key :value value)))
    (warp:rpc-request rpc-system connection sender-id recipient-id cmd
                      :expect-response t
                      :origin-instance-id origin-instance-id)))

;;;###autoload
(cl-defun warp:protocol-get-all-active-workers (rpc-system
                                                connection
                                                sender-id
                                                recipient-id
                                                &key origin-instance-id)
  "Sends a `:get-all-active-workers` RPC to the master.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system managing the request.
- `CONNECTION` (t): The `warp-transport` connection to the master.
- `SENDER-ID` (string): The ID of the client sending the request.
- `RECIPIENT-ID` (string): The ID of the master.
- `:origin-instance-id` (string, optional): The ID of the component system
  instance originating this RPC. Crucial for remote promise resolution.

Returns:
- (loom-promise): A promise that resolves with the response payload,
  containing a list of active workers (plist with `:worker-id`, `:inbox-address`)."
  (let ((cmd (warp-protocol--make-command
              :get-all-active-workers #'make-warp-get-all-active-workers-args)))
    (warp:rpc-request rpc-system connection sender-id recipient-id cmd
                      :expect-response t
                      :origin-instance-id origin-instance-id)))

(provide 'warp-protocol)
;;; warp-protocol.el ends here