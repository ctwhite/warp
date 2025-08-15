;;; warp-dns-plugin.el --- DNS-based Service Discovery Plugin -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a concrete implementation for the DNS protocol
;; as a transport plugin. It is designed as a **self-contained plugin** that
;; implements the `transport-protocol-service` interface, allowing the core
;; system to delegate DNS resolution in a standardized way.
;;
;; ## Architectural Role: Pluggable Discovery Resolver
;;
;; This plugin's primary role is to be a service provider for DNS
;; resolution. It offers two main APIs:
;;
;; 1.  A low-level protocol implementation for `warp-transport` to
;;     handle DNS queries over UDP.
;; 2.  A high-level `warp:dns-resolve` function that is a thin wrapper,
;;     orchestrating the DNS query as a series of `warp-transport` calls.
;;
;; This approach ensures that all network-related tasks, including DNS
;; queries, are managed through the same central mechanism, guaranteeing
;; architectural consistency and shared resilience from the transport layer.
;;
;; ## Key Implementation Details:
;;
;; * **Asynchronous Bridging**: This plugin demonstrates how to bridge
;;     a traditional, callback-based Emacs Lisp library (`dns.el`) with a
;;     modern, promise-based asynchronous framework (`loom`). It wraps the
;;     callback-based API with a `loom:promise`, providing a clean and
;;     composable interface for higher-level components.
;; * **Encapsulation**: All of the DNS-specific details, such as packet
;;     formatting and response parsing, are fully encapsulated within this
;;     plugin. External modules simply call a high-level resolve function.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)
(require 'dns)

(require 'warp-log)
(require 'warp-error)
(require 'warp-service)
(require 'warp-transport)
(require 'warp-transport-api)
(require 'warp-plugin)
(require 'warp-registry)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-dns-error
  "A generic error related to DNS resolution."
  'warp-error)

(define-error 'warp-dns-not-found
  "The DNS query returned no valid results for the service name."
  'warp-dns-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-dns--get-a-records (dns-response service-name)
  "Private: Extract IPv4 A records from a raw DNS response and format
them as endpoints.

Arguments:
- `DNS-RESPONSE` (plist): The raw response plist from `dns.el`.
- `SERVICE-NAME` (keyword): The logical name of the service.

Returns:
- (list): A list of `warp-service-endpoint` structs."
  (let ((answers (cdr (assq 'answers dns-response))))
    (cl-loop for answer in answers
             when (eq (dns-get 'type answer) 'A)
             collect (make-warp-service-endpoint
                      :name service-name
                      :address (format "tcp://%s" (dns-get 'data answer))
                      :protocol :tcp))))

(defun warp-dns--create-query-packet (hostname)
  "Private: Create a raw DNS query packet for a given hostname.

Arguments:
- `HOSTNAME` (string): The hostname to query for.

Returns:
- (string): A raw DNS packet as a unibyte string."
  (dns-write `((id ,(random 65000))
               (opcode query)
               (queries ((,hostname (type A))))
               (recursion-desired-p t))))

(defun warp-dns--parse-response-packet (packet)
  "Private: Parse a raw DNS response packet into a plist.

Arguments:
- `PACKET` (string): The raw DNS response packet.

Returns:
- (plist): A plist of the parsed DNS response."
  (dns-read packet))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:dns-resolve (service-name &key timeout)
  "Public: Resolve a service's DNS name to a list of endpoints.
This function performs an asynchronous DNS query by orchestrating a
series of `warp-transport` calls. It is a key component of the multi-source
discovery mechanism.

Arguments:
- `SERVICE-NAME` (keyword): The logical name of the service (e.g., `:job-queue`).
- `:timeout` (float, optional): The maximum time to wait for a DNS response.

Returns:
- (loom-promise): A promise that resolves to a list of
  `warp-service-endpoint`s, or rejects if the query fails or times out."
  (let* ((hostname (symbol-name service-name))
         (server-address (or (car dns-servers) "127.0.0.1"))
         (transport-address (format "tcp://%s:53" server-address))
         (query-packet (warp-dns--create-query-packet hostname)))

    (braid! (warp:transport-connect transport-address :timeout timeout)
      (:then (connection)
        ;; The `unwind-protect` ensures the connection is always closed.
        (unwind-protect
            (braid! (warp:transport-send connection query-packet)
              (:then (sent)
                (warp:transport-receive connection timeout)))
          (loom:await (warp:transport-close connection))))
      (:then (response-packet)
        (let* ((answers (warp-dns--parse-response-packet response-packet))
               (endpoints (warp-dns--get-a-records answers service-name)))
          (if endpoints
              endpoints
            (loom:rejected!
             (warp:error! :type 'warp-dns-not-found
                          :message (format "No DNS records found for %s." hostname))))))
      (:catch (err)
        (warp:log! :error "warp-dns" "DNS query for '%s' failed: %S"
                   service-name err)
        (loom:rejected! err)))))

;;;###autoload
(defun warp:register-service-resolver (name resolver-fn)
  "Public: Register a new pluggable service discovery resolver.
This function adds a new resolver to the central registry, making it
available for use in multi-source discovery.

Arguments:
- `NAME` (keyword): The unique name of the resolver (e.g., `:dns`).
- `RESOLVER-FN` (function): The resolver function. It must accept a
  service name and return a promise that resolves to a list of
  endpoints.

Returns: `t`."
  (warp:registry-add warp--service-resolver-registry name resolver-fn :overwrite-p t)
  (warp:log! :info "warp-service" "Registered service resolver '%s'." name)
  t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Plugin Definition

(warp:defplugin :dns-discovery-plugin
  "Provides a DNS-based service discovery resolver."
  :version "1.0.0"
  :implements :service-discovery
  :init
  (lambda (_context)
    ;; Register the public function `warp:dns-resolve` as the DNS resolver.
    (warp:register-service-resolver :dns 'warp:dns-resolve)))

(provide 'warp-dns-plugin)
;;; warp-dns-plugin.el ends here