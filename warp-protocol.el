;;; warp-protocol.el --- Low-Level RPC Protocol Definition -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the low-level machinery for defining Remote
;; Procedure Call (RPC) protocols within the Warp framework.
;;
;; Its primary export is the `warp:defprotocol` macro. After the
;; introduction of the unified service architecture in `warp-service.el`,
;; this macro's role has been refined.
;;
;; ## Architectural Role
;;
;; `warp:defprotocol` is now considered a lower-level tool intended for
;; specific use cases that do not fit the standard service pattern:
;;
;; 1.  **Framework-Internal APIs**: For core control plane communication
;;     that is not a discoverable "service" (e.g., worker-to-leader
;;     heartbeats).
;;
;; 2.  **Ad-hoc or Private RPCs**: For simple, internal communication
;;     between components that does not require a formal service contract.
;;
;; For all standard application-level services, developers should use the
;; `warp:defservice-implementation` macro with the `:expose-via-rpc` key,
;; as it provides a more robust, integrated, and consistent approach.
;;

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-marshal)
(require 'warp-rpc)
(require 'warp-command-router)
(require 'warp-bridge)
(require 'warp-cluster)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-protocol-error
  "A generic error for `warp-protocol` operations."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;----------------------------------------------------------------------
;;; Low-Level Protocol Macro
;;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:defprotocol (name &rest options)
  "Define a low-level RPC protocol, generating client and server code.

This macro is a powerful tool for manually defining RPC contracts.
However, for standard services, it is **highly recommended** to use
the `warp:defservice-implementation` macro instead, as it automates
this process and ensures consistency with a formal service interface.

Use this macro for framework-internal or ad-hoc communication that
does not represent a formal, discoverable service.

:Arguments:
- `NAME` (symbol): The symbolic name of the protocol.
- `OPTIONS` (plist):
  - `:client-class` (symbol): The name for the generated client struct.
  - `:handler-group-deps` (list): Dependencies injected into handlers.
  - `:auto-schema` (boolean): If t, auto-generate schemas.
  - The rest of `OPTIONS` are RPC method definitions, including an
    inline `:handler` lambda.

:Returns:
- (list): A `(progn ...)` form containing all generated definitions.

:Side Effects:
- Defines structs, functions, commands, and variables in the current
  environment when the macro is expanded."
  (let* ((protocol-name name)
         (client-class (plist-get options :client-class))
         (handler-group-deps (plist-get options :handler-group-deps))
         (auto-schema (plist-get options :auto-schema))
         (rpc-definitions
          (cl-loop for opt in options
                   unless (keywordp (car-safe opt))
                   collect opt)))

    ;; This macro is now self-contained, with the logic of the former
    ;; private helpers integrated directly into the `cl-loop` below.
    `(progn
       ;; Generate all definitions in a single pass.
       ,@(cl-loop
          for rpc-def in rpc-definitions
          append
          (let* ((method-name (car rpc-def))
                 (args-list (cadr rpc-def))
                 (method-options (cddr rpc-def))
                 (handler-code (plist-get method-options :handler))
                 (schema-name (intern (format "warp-%s-%s-request"
                                              protocol-name method-name)))
                 ;; Update the rpc-def with the schema name if auto-generating.
                 (rpc-def-with-schema
                  (if auto-schema
                      (append rpc-def `(:request-schema ,schema-name))
                    rpc-def)))

            (append
             ;; 1. Generate Schema (optional)
             (when auto-schema
               (let ((fields (cl-loop for arg in args-list
                                      unless (memq arg '(&key &optional &rest))
                                      collect `(,arg nil :serializable-p t))))
                 `((warp:defschema ,schema-name (:generate-protobuf t) ,@fields))))

             ;; 2. Generate Client Method (optional)
             (when client-class
               (let* ((client-fn-name (intern (format "%s-%s" client-class
                                                      method-name)))
                      (command-name (intern (format ":%s/%s" protocol-name
                                                    method-name)))
                      (payload-args
                       (cl-loop for arg in args-list
                                unless (memq arg '(&key &optional &rest))
                                collect `(,(intern (symbol-name arg) :keyword)
                                          ,arg))))
                 `((cl-defun ,client-fn-name (client connection sender-id
                                                     recipient-id ,@args-list)
                     ,(format "Sends a %S RPC." command-name)
                     (let ((command (make-warp-rpc-command
                                     :name ',command-name
                                     :args (,(intern (format "make-%s"
                                                             schema-name))
                                            ,@payload-args))))
                       (warp:rpc-send (,(intern (format "%s-rpc-system"
                                                        client-class))
                                       client)
                                      connection
                                      :sender-id sender-id
                                      :recipient-id recipient-id
                                      :command command
                                      ,@(if (plist-get method-options :stream)
                                            '(:stream t))
                                      ,@(if (plist-get method-options
                                                       :fire-and-forget)
                                            '(:expect-response nil))))))))

             ;; 3. Generate Server-Side Command (optional)
             (when handler-code
               (let* ((command-name (intern (format ":%s/%s" protocol-name
                                                    method-name)))
                      (handler-doc (cadr handler-code))
                      (handler-lambda-args (caddr handler-code))
                      (handler-body (cdddr handler-code)))
                 `((warp:defcommand ,command-name ,handler-doc
                     :args ',handler-lambda-args
                     :execute (lambda ,handler-lambda-args ,@handler-body))))))))

       ;; Generate the client struct after all methods are processed.
       ,@(when client-class
           `((cl-defstruct (,client-class
                            (:constructor
                             ,(intern (format "make-%s" client-class)))
                            (:conc-name ,(format "%s-" client-class)))
               ,(format "Client for the %s protocol." protocol-name)
               (rpc-system (cl-assert nil) :type (or null t)))))

       ;; Generate the handler registration function after all commands.
       (defun ,(intern (format "register-%s-handlers" protocol-name))
           (router ,@handler-group-deps)
         ,(format "Register all server-side handlers for the %s protocol."
                  protocol-name)
         ,@(cl-loop for rpc-def in rpc-definitions
                    when (plist-get (cddr rpc-def) :handler)
                    collect
                    (let* ((method-name (car rpc-def))
                           (command-name (intern (format ":%s/%s" protocol-name
                                                         method-name)))
                           (command-var (intern (format "warp-command-%s"
                                                        command-name))))
                      `(warp:command-router-add-route
                        router ',command-name
                        :handler-fn
                        (lambda (command context)
                          (apply (warp-command-executor ,command-var)
                                 (list ,@handler-group-deps
                                       command context))))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Worker-Leader Protocol Schemas & Definition

(warp:defschema warp-worker-ready-payload
  ((:constructor make-warp-worker-ready-payload))
  "The data payload for a worker's initial handshake with the leader.
This schema defines the essential information a new worker must provide
to the leader to be officially registered and integrated into the cluster.

:Fields:
- `worker-id` (string): The unique identifier of the new worker.
- `rank` (integer): The numerical rank of this worker within its pool.
- `pool-name` (string): The name of the pool this worker belongs to.
- `inbox-address` (string): The network address the leader can use to
  send commands back to this specific worker.
- `launch-id` (string): A unique token from the launch command, used
  to prevent stale or unauthorized registration attempts.
- `leader-challenge-token` (string): The cryptographic challenge from the leader.
- `worker-signature` (string): The worker's signature over the challenge token.
- `worker-public-key` (string): The worker's public key material."
  (worker-id              nil :type string)
  (rank                   nil :type (or null integer))
  (pool-name              nil :type (or null string))
  (inbox-address          nil :type (or null string))
  (launch-id              nil :type (or null string))
  (leader-challenge-token nil :type (or null string))
  (worker-signature       nil :type (or null string))
  (worker-public-key      nil :type (or null string)))

(warp:defschema warp-worker-heartbeat-payload
  ((:constructor make-warp-worker-heartbeat-payload))
  "The data payload for a worker's periodic heartbeat.
This schema defines the data a worker sends to the leader at regular
intervals to signal that it is still alive and operational.

:Fields:
- `worker-id` (string): The ID of the worker sending the heartbeat.
- `status` (symbol): The worker's self-reported health status
  (e.g., `:healthy`, `:degraded`).
- `telemetry` (string): A serialized snapshot of the worker's metrics."
  (worker-id nil :type string)
  (status    nil :type symbol)
  (telemetry nil :type (or null string)))

(warp:defschema warp-worker-deregister-payload
  ((:constructor make-warp-worker-deregister-payload))
  "The data payload for a worker's deregistration signal.
This is used by the compensation action of the worker bootstrap saga to
inform the leader that a worker is shutting down unexpectedly and should
be removed from the active registry."
  (worker-id nil :type string))

(warp:defschema warp-worker-deployment-status-payload
  ((:constructor make-warp-worker-deployment-status-payload))
  "The data payload for a worker's plugin deployment status report.
This RPC is a callback from the worker to the leader, providing a status
update for a long-running plugin management task initiated by the leader."
  (tracking-id nil :type string)
  (worker-id nil :type string)
  (status nil :type symbol
          :validate (memq $ '(:pending :in-progress :completed :failed)))
  (details nil :type (or null string)))

(warp:defprotocol worker-leader-protocol
  "Defines the essential control plane communication from a worker to its leader.

This protocol is the primary channel for managing the worker lifecycle,
including registration and ongoing health monitoring."

  :client-class worker-leader-client
  :handler-group-deps (bridge)
  :auto-schema t

  (worker-ready (payload)
   :doc "Handles the initial handshake RPC from a newly launched worker."
   :handler (lambda (bridge command _context)
              "Processes the initial handshake signal.
:Arguments:
- `bridge`: The injected `:bridge` component.
- `command`: The incoming RPC command.
- `_context`: The RPC context.
:Returns:
- (loom-promise): A promise that resolves on successful registration."
              (let ((args (warp-rpc-command-args command)))
                (warp:bridge-handle-worker-ready-signal
                 bridge (plist-get args :payload)))))

  (send-heartbeat (payload)
   :doc "Processes a periodic heartbeat from an active worker."
   :fire-and-forget t
   :handler (lambda (bridge command _context)
              "Processes a periodic heartbeat from an active worker.
:Arguments:
- `bridge`: The injected `:bridge` component.
- `command`: The incoming RPC command.
- `_context`: The RPC context.
:Returns:
- (loom-promise): A promise that resolves after processing."
              (let ((args (warp-rpc-command-args command)))
                (warp:bridge-handle-heartbeat bridge (plist-get args :payload)))))

  (worker-deregister (payload)
    :doc "Handles a worker's deregistration signal."
    :fire-and-forget t
    :handler (lambda (bridge command _context)
               "Processes a deregistration signal from a worker."
               (let ((args (warp-rpc-command-args command)))
                 (warp:bridge-handle-worker-deregister bridge (plist-get args :payload)))))

  (report-deployment-status (payload)
    :doc "Handles a worker's status report for a plugin deployment."
    :fire-and-forget t
    :handler (lambda (bridge command _context)
               "Processes a status update for a plugin deployment from a worker."
               (let ((args (warp-rpc-command-args command)))
                 (warp:bridge-handle-deployment-status-report bridge (plist-get args :payload))))))

(provide 'warp-protocol)
;;; warp-protocol.el ends here