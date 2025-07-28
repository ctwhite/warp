;;; warp-protocol.el --- Warp Component Communication Protocol -*- lexical-binding: t; -*-

;;; Commentary:
;;
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
;;; Schema Definitions

(warp:defschema warp-worker-ready-payload
    ((:constructor make-warp-worker-ready-payload))
  "Payload for the `:worker-ready` RPC sent to the master.
This message is critical for the initial handshake and registration.

Fields:
- `worker-id` (string): The unique identifier of the worker process.
- `rank` (integer): The numerical rank assigned to the worker.
- `status` (symbol): The initial status of the worker (e.g., `:starting`).
- `launch-id` (string): A one-time ID for this launch, for security.
- `master-challenge-token` (string): The token provided by the master.
- `worker-signature` (string): The worker's signature over the token.
- `worker-public-key` (string): The worker's public key for verification.
- `inbox-address` (string): The IPC address the worker is listening on."
  (worker-id nil :type string)
  (rank nil :type integer)
  (status nil :type symbol)
  (launch-id nil :type (or null string))
  (master-challenge-token nil :type (or null string))
  (worker-signature nil :type (or null string))
  (worker-public-key nil :type (or null string))
  (inbox-address nil :type (or null string)))

(warp:defprotobuf-mapping warp-worker-ready-payload
  `((worker-id 1 :string)
    (rank 2 :int32)
    (status 3 :string)
    (launch-id 4 :string)
    (master-challenge-token 5 :string)
    (worker-signature 6 :string)
    (worker-public-key 7 :string)
    (inbox-address 8 :string)))

(warp:defschema warp-worker-heartbeat-payload
    ((:constructor make-warp-worker-heartbeat-payload))
  "Payload for the periodic `:heartbeat` RPC to the master.

Fields:
- `worker-id` (string): ID of the worker sending the heartbeat.
- `status` (symbol): Current operational status (e.g., `:running`).
- `timestamp` (float): `float-time` when the heartbeat was created.
- `services` (list): Lisp objects describing services on the worker."
  (worker-id nil :type string)
  (status nil :type symbol)
  (timestamp (float-time) :type float)
  (services nil :type list))

(warp:defprotobuf-mapping warp-worker-heartbeat-payload
  `((worker-id 1 :string)
    (status 2 :string)
    (timestamp 3 :double)
    (services 4 :bytes)))

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
  `((message 1 :string)
    (timestamp 2 :double)))

(warp:defschema warp-cluster-map-payload
    ((:constructor make-warp-cluster-map-payload))
  "Payload for `:evaluate-map-chunk` RPC for a map-reduce operation.

Fields:
- `function-form` (t): A Lisp form (code) that evaluates to the map fn.
- `chunk-data` (list): The list of data items for the worker to process."
  (function-form nil :type t)
  (chunk-data nil :type list))

(warp:defprotobuf-mapping warp-cluster-map-payload
  `((function-form 1 :bytes)
    (chunk-data 2 :bytes)))

(warp:defschema warp-cluster-map-result
    ((:constructor make-warp-cluster-map-result))
  "Result payload for an `:evaluate-map-chunk` RPC.

Fields:
- `results` (list): Results after applying the map function."
  (results nil :type list))

(warp:defprotobuf-mapping warp-cluster-map-result
  `((results 1 :bytes)))

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
  `((worker-id 1 :string)
    (current-version 2 :string)
    (provision-type 3 :string)))

(warp:defschema warp-provision-response-payload
    ((:constructor make-warp-provision-response-payload))
  "Payload for the master's RPC response containing the provision.

Fields:
- `version` (string): The version identifier of the returned provision.
- `provision` (t): The provision object itself."
  (version nil :type string)
  (provision nil :type t))

(warp:defprotobuf-mapping warp-provision-response-payload
  `((version 1 :string)
    (provision 2 :bytes)))

(warp:defschema warp-provision-update-payload
    ((:constructor make-warp-provision-update-payload))
  "Payload for a provision update event pushed from master to workers.

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
  `((version 1 :string)
    (provision 2 :bytes)
    (provision-type 3 :string)
    (target-ids 4 :bytes)))

(warp:defschema warp-provision-jwt-keys-payload
    ((:constructor make-warp-provision-jwt-keys-payload))
  "Provision payload for distributing trusted JWT public keys.

Fields:
- `trusted-keys` (list): An alist or plist mapping key IDs to
  PEM-encoded public keys."
  (trusted-keys nil :type (or null list)))

(warp:defprotobuf-mapping warp-provision-jwt-keys-payload
  `((trusted-keys 1 :bytes)))

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
  "Construct a `warp-rpc-command` object with its payload.
This helper standardizes the creation of RPC command objects.

Arguments:
- `NAME` (keyword): The symbolic name of the RPC command (e.g., `:ping`).
- `ARGS-CONSTRUCTOR` (function): The constructor for the payload schema.
- `&rest ARGS-PLIST` (plist): A property list of arguments for the payload.

Returns:
- (warp-rpc-command): A new, fully-formed RPC command object."
  (let ((payload (apply args-constructor args-plist)))
    (make-warp-rpc-command :name name :args payload)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:protocol-client-create (&key rpc-system)
  "Create a new `warp-protocol-client` component.
This factory function should be used within a `warp:defcomponent`
definition.

Arguments:
- `:rpc-system` (warp-rpc-system): The `rpc-system` component that
  this client will use for sending all requests.

Returns:
- (warp-protocol-client): A new protocol client instance."
  (%%make-protocol-client :rpc-system rpc-system))

;;;###autoload
(cl-defun warp:protocol-client-send-worker-ready
    (client connection worker-id rank status &key launch-id challenge-token
                                                  signature public-key
                                                  inbox-address
                                                  (expect-response t))
  "Send a `:worker-ready` notification from a worker to the master.

Arguments:
- `CLIENT` (warp-protocol-client): The protocol client instance.
- `CONNECTION` (t): The active `warp-transport` connection to the master.
- `WORKER-ID` (string): The unique identifier of the worker.
- `RANK` (integer): The worker's assigned rank.
- `STATUS` (keyword): The worker's current status (e.g., `:starting`).
- `:launch-id` (string): The unique ID of this launch instance.
- `:challenge-token` (string): The cryptographic challenge from the master.
- `:signature` (string): The worker's digital signature for auth.
- `:public-key` (string): The worker's public key material.
- `:inbox-address` (string): The IPC address the worker is listening on.
- `:expect-response` (boolean): Whether a response is expected."
  (let ((cmd (warp-protocol--make-command
              :worker-ready #'make-warp-worker-ready-payload
              :worker-id worker-id :rank rank :status status
              :launch-id launch-id
              :master-challenge-token challenge-token
              :worker-signature signature
              :worker-public-key public-key
              :inbox-address inbox-address)))
    (warp:rpc-request (warp-protocol-client-rpc-system client)
                      connection worker-id "master" cmd
                      :expect-response expect-response)))

;;;###autoload
(cl-defun warp:protocol-client-send-heartbeat
    (client connection worker-id status services &key (expect-response nil))
  "Send a periodic `:heartbeat` from a worker to the master.

Arguments:
- `CLIENT` (warp-protocol-client): The protocol client instance.
- `CONNECTION` (t): The active `warp-transport` connection to the master.
- `WORKER-ID` (string): The ID of the worker sending the heartbeat.
- `STATUS` (keyword): The worker's current operational status.
- `SERVICES` (list): A list describing services hosted by the worker.
- `:expect-response` (boolean): Whether a response is expected."
  (let ((cmd (warp-protocol--make-command
              :heartbeat #'make-warp-worker-heartbeat-payload
              :worker-id worker-id :status status :services services)))
    (warp:rpc-request (warp-protocol-client-rpc-system client)
                      connection worker-id "master" cmd
                      :expect-response expect-response)))

;;;###autoload
(cl-defun warp:protocol-ping (rpc-system connection sender-id recipient-id
                                         &key (expect-response t))
  "Send a `:ping` RPC to check liveness between two nodes.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system managing the request.
- `CONNECTION` (t): The `warp-transport` connection to the target node.
- `SENDER-ID` (string): The unique ID of the node sending the ping.
- `RECIPIENT-ID` (string): The unique ID of the node being pinged.
- `:expect-response` (boolean): Whether a response is expected."
  (let ((cmd (warp-protocol--make-command
              :ping #'make-warp-ping-payload
              :message "ping")))
    (warp:rpc-request rpc-system connection sender-id recipient-id cmd
                      :expect-response expect-response)))

;;;###autoload
(cl-defun warp:protocol-client-request-map-chunk
    (client connection master-id worker-id function-form chunk-data
            &key (expect-response t))
  "Request a worker to evaluate a map function over a chunk of data.

Arguments:
- `CLIENT` (warp-protocol-client): The protocol client instance.
- `CONNECTION` (t): The connection to the target worker.
- `MASTER-ID` (string): The master's unique ID (acting as the sender).
- `WORKER-ID` (string): The target worker's unique ID.
- `FUNCTION-FORM` (form): S-expression that evaluates to the map function.
- `CHUNK-DATA` (list): The list of data items for the worker to process.
- `:expect-response` (boolean): Whether a response is expected."
  (let ((cmd (warp-protocol--make-command
              :evaluate-map-chunk #'make-warp-cluster-map-payload
              :function-form function-form :chunk-data chunk-data)))
    (warp:rpc-request (warp-protocol-client-rpc-system client)
                      connection master-id worker-id cmd
                      :expect-response expect-response)))

;;;###autoload
(cl-defun warp:protocol-request-provision
    (client connection worker-id current-version provision-type
            &key (expect-response t))
  "Send a `:provision-request` RPC from a worker to the master.

Arguments:
- `CLIENT` (warp-protocol-client): The protocol client instance.
- `CONNECTION` (t): The active `warp-transport` connection to the master.
- `WORKER-ID` (string): The ID of the worker requesting the provision.
- `CURRENT-VERSION` (string): The version of the provision the worker has.
- `PROVISION-TYPE` (keyword): The type of provision requested.
- `:expect-response` (boolean): Whether a response is expected."
  (let ((cmd (warp-protocol--make-command
              :get-provision #'make-warp-provision-request-payload
              :worker-id worker-id
              :current-version current-version
              :provision-type provision-type)))
    (warp:rpc-request (warp-protocol-client-rpc-system client)
                      connection worker-id "master" cmd
                      :expect-response expect-response)))

;;;###autoload
(cl-defun warp:protocol-publish-provision
    (rpc-system connection sender-id recipient-id version provision-obj
     provision-type &key target-ids (expect-response nil))
  "Send a `:provision-update` event from the master to workers.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system managing the request.
- `CONNECTION` (t): The `warp-transport` connection to the target node.
- `SENDER-ID` (string): ID of the master sending the update.
- `RECIPIENT-ID` (string): ID of the target worker (or \"cluster\").
- `VERSION` (string): The new version identifier of the provision.
- `PROVISION-OBJ` (any): The new provision object.
- `PROVISION-TYPE` (keyword): The type of provision being updated.
- `:target-ids` (list): Optional list of specific worker IDs to target.
- `:expect-response` (boolean): Whether a response is expected."
  (let ((cmd (warp-protocol--make-command
              :provision-update #'make-warp-provision-update-payload
              :version version
              :provision provision-obj
              :provision-type provision-type
              :target-ids target-ids)))
    (warp:rpc-request rpc-system connection sender-id recipient-id cmd
                      :expect-response expect-response)))

(provide 'warp-protocol)
;;; warp-protocol.el ends here