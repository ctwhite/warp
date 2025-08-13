;;; warp-cli.el --- Self-contained Command-Line Interface for the Warp Framework -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This file contains the core Emacs Lisp logic for the Warp command-line
;; interface (CLI). It is a self-contained application executed by the `warp`
;; shell wrapper.
;;
;; This version has been significantly enhanced to serve as the primary
;; control plane for the new **Centralized Manager Service**. It introduces
;; commands for dynamic, cluster-wide configuration changes (`warp config`)
;; and declarative, manifest-based deployments (`warp deploy`).
;;
;; ## Architectural Overview: A Three-Layer Design
;;
;; The CLI is built on a clear, three-layer architecture designed for
;; extensibility and maintainability.
;;
;; 1.  **Generic CLI Framework**:
;;     The foundation is a generic, reusable set of macros and functions
;;     for building command-line tools in Emacs Lisp. This layer knows
;;     nothing about Warp clusters; it only provides utilities for argument
;;     parsing, help generation, and styled output.
;;
;; 2.  **Declarative Command Definitions**:
;;     Building on the framework, all of the Warp CLI's specific commands
;;     are defined declaratively. Each definition links a command string
;;     (e.g., "list") to a handler function and its options. This makes the
;;     CLI easy to extend—adding a new command is a matter of defining it
;;     and writing its handler, with no changes to the core parsing or
;;     dispatch logic.
;;
;; 3.  **RPC Client Logic (The "Smart Client")**:
;;     The CLI acts as a pure client to a running Warp cluster. All
;;     substantive operations are performed by making RPC calls to the cluster
;;     leader's administrative services. A centralized helper
;;     (`warp-cli--get-service-client`) manages the connection and creates
;;     auto-generated client stubs for the remote services, abstracting away
;;     all network communication.

;;; Code:

(require 'cl-lib)
(require 'json)
(require 's)
(require 'subr-x)
(require 'yaml)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Customization

(defgroup warp-cli nil
  "Customization options for the Warp Command-Line Interface."
  :group 'warp
  :prefix "warp-cli-")

(defcustom warp-cli-version "3.0.0"
  "The version string for the Warp CLI tool."
  :type 'string
  :group 'warp-cli)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

;;;---------------------------------------------------------------------------
;;; CLI Options
;;;---------------------------------------------------------------------------

(defvar warp-cli--verbose-mode nil
  "A boolean indicating if the CLI is running in verbose mode.
This is set by the global `--verbose` flag.")

(defvar warp-cli--options-registry (make-hash-table :test 'equal)
  "Registry for all defined command-line options.")

(defvar warp-cli--subcommand-registry (make-hash-table :test 'equal)
  "Registry for all defined subcommands and their metadata.")

(defvar warp-cli--alias-registry (make-hash-table :test 'equal)
  "Registry for command aliases (e.g., 'ls' -> 'list').")

;;;---------------------------------------------------------------------------
;;; RPC Client Stubs
;;;---------------------------------------------------------------------------

(defvar cluster-admin-client nil
  "A client stub for the legacy `cluster-admin-service`.")
(defvar service-registry-client nil
  "A client stub for the `service-registry`.")
(defvar job-queue-client nil
  "A client stub for the `job-queue-service`.")
(defvar manager-client nil
  "A client stub for the primary `manager-service`.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Warp Framework Integration

(let ((warp-root (or (getenv "WARP_CLI_ROOT_DIR")
                     (file-name-directory
                      (or load-file-name buffer-file-name)))))
  (add-to-list 'load-path warp-root)
  (require 'warp-cluster)
  (require 'warp-rpc)
  (require 'warp-protocol)
  (require 'warp-transport)
  (require 'warp-job-queue)
  (require 'warp-manager)
  (require 'loom)
  (require 'braid))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;---------------------------------------------------------------------------
;;; Logging and Styling Utilities (Private)
;;;---------------------------------------------------------------------------

(defun warp-cli--log (message &rest args)
  "Log a `MESSAGE` to stderr only if verbose mode is enabled.
This provides a simple mechanism for debug logging that is hidden
from the user by default but can be enabled with a global flag.

Arguments:
- `MESSAGE` (string): The format string for the log message.
- `ARGS` (list): A list of arguments for the format string.

Returns:
- `nil`."
  (when warp-cli--verbose-mode
    (message "warp-cli: %s" (apply #'format message args))))

(defun warp-cli--style-bold (text)
  "Return `TEXT` formatted as bold using ANSI escape codes.

Arguments:
- `TEXT` (string): The text to format.

Returns:
- (string): The formatted string with ANSI escape codes."
  (format "\e[1m%s\e[0m" text))

(defun warp-cli--style-error (text)
  "Return `TEXT` formatted in red for errors.

Arguments:
- `TEXT` (string): The text to format.

Returns:
- (string): The formatted string with ANSI escape codes."
  (format "\e[31m%s\e[0m" text))

(defun warp-cli--style-success (text)
  "Return `TEXT` formatted in green for success.

Arguments:
- `TEXT` (string): The text to format.

Returns:
- (string): The formatted string with ANSI escape codes."
  (format "\e[32m%s\e[0m" text))

(defun warp-cli--print-error (message &rest args)
  "Print a formatted error `MESSAGE` to stderr in red.

Arguments:
- `MESSAGE` (string): The format string for the error message.
- `ARGS` (list): A list of arguments for the format string.

Returns:
- `nil`.

Side Effects:
- Prints a formatted string to `stderr`."
  (message (warp-cli--style-error (apply #'format message args))))

(defun warp-cli--print-success (message &rest args)
  "Print a formatted success `MESSAGE` to stdout in green.

Arguments:
- `MESSAGE` (string): The format string for the success message.
- `ARGS` (list): A list of arguments for the format string.

Returns:
- `nil`.

Side Effects:
- Prints a formatted string to `stdout`."
  (princ (warp-cli--style-success (apply #'format message args)))
  (princ "\n"))

(defmacro warp-cli--with-spinner ((message) &rest body)
  "Display a spinner with `MESSAGE` while executing `BODY`.
This macro provides a better user experience for long-running,
asynchronous operations like network calls.

Arguments:
- `MESSAGE` (string): The message to display next to the spinner.
- `BODY` (forms): The Lisp forms to execute. The last form must
  return a `loom-promise`.

Returns:
- The result of the promise returned by `BODY`.

Side Effects:
- Blocks until the promise from `BODY` is resolved or rejected."
  (let ((result (make-symbol "result"))
        (spinner-thread (make-symbol "spinner-thread")))
    `(let (,spinner-thread)
       ;; Use `unwind-protect` to guarantee the spinner is always cleaned up,
       ;; even if the body's promise rejects or an error occurs.
       (unwind-protect
           (progn
             ;; 1. Create a separate thread to run the spinner animation.
             ;; This prevents the UI from blocking while the main thread
             ;; performs network I/O.
             (setq ,spinner-thread
                   (make-thread
                    (lambda ()
                      (let ((chars '("⠋" "⠙" "⠹" "⠸" "⠼" "⠴" "⠦" "⠧" "⠇" "⠏"))
                            (i 0))
                        (while t
                          (princ (format "\r%s %s" (nth i chars) ,message) stderr)
                          (force-output stderr)
                          (setq i (mod (1+ i) (length chars)))
                          (sleep-for 0.08))))
                    "cli-spinner"))
             ;; 2. Execute the user's code, which must return a promise.
             (setq ,result (progn ,@body))
             ;; 3. Block until the promise is settled (resolved or rejected).
             (loom:await ,result))
         ;; 4. Cleanup: stop the spinner thread and clear the line.
         (when (and ,spinner-thread (thread-live-p ,spinner-thread))
           (thread-join ,spinner-thread))
         ;; Use ANSI escape codes to clear the line for a clean output.
         (princ "\r\033[K" stderr)
         (force-output stderr))
       ,result)))

;;;---------------------------------------------------------------------------
;;; Argument Parsing and Formatting (Private)
;;;---------------------------------------------------------------------------

(defun warp-cli--convert-value (value type-key key-str)
  "Convert a string `VALUE` to its specified `TYPE-KEY`.

Arguments:
- `VALUE` (string): The raw string value from the command line.
- `TYPE-KEY` (keyword): The target type.
- `KEY-STR` (string): The name of the option, for error reporting.

Returns:
- The converted value.

Signals:
- `user-error` if the value cannot be converted."
  (pcase type-key
    (:boolean (not (member value '("false" "0" "no") :test #'string=)))
    (:integer
     (if (string-match-p "\\`[-+]?[0-9]+\\'" value)
         (string-to-number value)
       (user-error "Invalid integer for '--%s': '%s'." key-str value)))
    (:filepath (expand-file-name value))
    (_ value)))

(defun warp-cli--parse-args (args)
  "Parse a list of command-line `ARGS` based on defined options.

Arguments:
- `ARGS` (list): A list of string arguments to parse.

Returns:
- (plist): A plist with `:options` and `:positional` keys.

Side Effects:
- Prints error messages to stderr on validation failure."
  (let ((options (make-hash-table :test 'eq))
        (positional '()))
    ;; 1. Set default values for all defined options.
    (maphash (lambda (_ def)
               (when-let (default (plist-get def :default))
                 (puthash (plist-get def :key) default options)))
             warp-cli--options-registry)
    ;; 2. Iterate through args, parsing options and populating `options`.
    (while args
      (let ((arg (pop args)))
        (cond
          ;; Case: --key=value
          ((string-match "^--\\([^=]+\\)=\\(.*\\)" arg)
           (let* ((key (match-string 1 arg)) (val (match-string 2 arg))
                  (def (gethash key warp-cli--options-registry)))
             (when def
               (puthash (plist-get def :key)
                        (warp-cli--convert-value val (plist-get def :type) key)
                        options))))
          ;; Case: --key value
          ((string-match "^--\\(.+\\)" arg)
           (let* ((key (match-string 1 arg))
                  (def (gethash key warp-cli--options-registry)))
             (when def
               (if (eq (plist-get def :type) :boolean)
                   (puthash (plist-get def :key) t options)
                 (puthash (plist-get def :key)
                          (warp-cli--convert-value
                           (pop args) (plist-get def :type) key)
                          options)))))
          ;; Case: -k value
          ((string-match "^-\\([a-zA-Z0-9]\\)" arg)
           (let* ((short-key (match-string 1 arg)) key def)
             (maphash (lambda (k v) (when (equal (plist-get v :short) short-key)
                                      (setq def v key k)))
                      warp-cli--options-registry)
             (when def
               (if (eq (plist-get def :type) :boolean)
                   (puthash (plist-get def :key) t options)
                 (puthash (plist-get def :key)
                          (warp-cli--convert-value
                           (pop args) (plist-get def :type) key)
                          options)))))
          ;; Case: positional argument
          (t (push arg positional)))))
    ;; 3. Return options as an alist and positional args as a list.
    (list :options (hash-table-alist options)
          :positional (nreverse positional))))

(defun warp-cli--format-option-help-line (opt-def)
  "Format a single line of help text for a command-line option.

Arguments:
- `OPT-DEF` (plist): The definition of the option.

Returns:
- (string): A formatted string for the help output."
  (let* ((long (plist-get opt-def :long))
         (short (plist-get opt-def :short))
         (desc (plist-get opt-def :desc))
         (default (plist-get opt-def :default))
         (flag-str (concat (if short (format "-%s, " short) "    ")
                           (format "--%s" long)))
         (desc-str (concat desc (if default (format " (default: %s)"
                                                    (prin1-to-string default))
                                  ""))))
    (format "  %-25s %s" flag-str desc-str)))

(defun warp-cli--print-auto-help (&optional for-subcommand)
  "Generate and print a help message from defined commands and options.

Arguments:
- `FOR-SUBCOMMAND` (string, optional): The subcommand for specific help.

Returns:
- `nil`.

Side Effects:
- Prints a formatted help message to `stdout`."
  (let ((tool-name "warp"))
    ;; Determine whether to print general help or command-specific help.
    (if for-subcommand
        ;; Print help for a specific subcommand.
        (if-let ((sub-def (gethash for-subcommand warp-cli--subcommand-registry)))
            (princ (format "\nUsage: %s %s [options]\n\n%s\n\nOptions:\n%s\n"
                           (warp-cli--style-bold tool-name) for-subcommand
                           (plist-get sub-def :desc)
                           (mapconcat (lambda (opt-key)
                                        (let ((opt-def
                                               (gethash (symbol-name opt-key)
                                                        warp-cli--options-registry)))
                                          (warp-cli--format-option-help-line opt-def)))
                                      (plist-get sub-def :options) "\n")))
          (warp-cli--print-error "Unknown command: '%s'." for-subcommand))
      ;; Print the main help message with all commands and global options.
      (princ (format (concat "\nUsage: %s [global-options] <command> [options]\n\n"
                             "Global Options:\n%s\n\nCommands:\n%s\n"
                             "Run '%s <command> --help' for details.\n")
                     (warp-cli--style-bold tool-name)
                     (mapconcat #'(lambda (item) (warp-cli--format-option-help-line (cdr item)))
                                (cl-sort (hash-table-alist warp-cli--options-registry)
                                         #'string< :key #'car) "\n")
                     (mapconcat (lambda (item) (format "  %-15s %s" (car item)
                                                       (plist-get (cdr item) :desc)))
                                (cl-sort (hash-table-alist warp-cli--subcommand-registry)
                                         #'string< :key #'car) "\n")
                     tool-name)))))

(defun warp-cli--format-table (headers rows)
  "Format a list of `ROWS` into a clean, aligned ASCII table.

Arguments:
- `HEADERS` (list): A list of strings for the table headers.
- `ROWS` (list): A list of lists of strings for the table data.

Returns:
- (string): A formatted string containing the ASCII table."
  (let* ((num-cols (length headers))
         (col-widths (mapcar #'length headers))
         (all-content (cons headers rows)))
    ;; 1. Calculate maximum width for each column.
    (dotimes (i num-cols)
      (setf (nth i col-widths)
            (apply #'max (mapcar (lambda (row)
                                   (length (or (nth i row) "")))
                                 all-content))))
    ;; 2. Build the table as a string.
    (with-output-to-string
      (dotimes (i num-cols)
        (princ (format "%-*s  " (nth i col-widths)
                       (warp-cli--style-bold (nth i headers)))))
      (princ "\n")
      (dotimes (i num-cols)
        (princ (make-string (nth i col-widths) ?─))
        (princ "    "))
      (princ "\n")
      (dolist (row rows)
        (dotimes (i num-cols)
          (princ (format "%-*s  " (nth i col-widths)
                         (or (nth i row) "n/a"))))
        (princ "\n")))))

(defun warp-cli--format-output (data opts headers)
  "Format and print data based on the --format option.

Arguments:
- `DATA` (list): The list of data to format.
- `OPTS` (alist): The parsed command-line options.
- `HEADERS` (list): A list of header strings for table formatting.

Returns:
- `nil`.

Side Effects:
- Prints the formatted data to stdout."
  (let ((format-type (if (alist-get 'interactive opts) "fzf"
                       (alist-get 'format opts))))
    (pcase format-type
      ("json" (princ (json-encode data)))
      ("yaml" (princ (yaml-encode data)))
      ("id" (dolist (item data) (princ (format "%s\n" (plist-get item :id)))))
      ("fzf"
       (let ((id-key (pcase (plist-get (car data) :type)
                       ('service :service-name)
                       (_ :id))))
         (dolist (item data)
           (princ (format "%s\t%s\n" (plist-get item id-key)
                          (or (plist-get item :name)
                              (plist-get item :status) ""))))))
      ("table"
       (let ((rows (mapcar (lambda (item)
                             (mapcar (lambda (h)
                                       (let* ((key-str (s-replace "-" "_"
                                                                  (s-downcase h)))
                                              (key (intern (format ":%s" key-str))))
                                         (format "%s" (or (plist-get item key) ""))))
                                     headers))
                           data)))
         (princ (warp-cli--format-table headers rows)))))))

(defun warp-cli--get-service-client (leader-addr service-key)
  "Connect to the cluster leader and return a specific service client stub.
This function is the central point of contact between the CLI and a live
Warp cluster. It handles the initial network connection and then uses the
auto-generated constructor for the requested service client.

Arguments:
- `LEADER-ADDR` (string): The network address of the cluster leader.
- `SERVICE-KEY` (keyword): The keyword identifying which service client to create.

Returns:
- (any): An initialized, auto-generated client stub for the requested service.

Side Effects:
- Establishes a network connection.
- Prints an error message and exits the CLI if the connection fails."
  (let* ((rpc-system (warp:rpc-system-create))
         (conn (warp-cli--with-spinner ("Connecting to cluster...")
                 (loom:await (warp:transport-connect leader-addr)))))
    ;; 1. Check for a successful connection.
    (unless conn
      (warp-cli--print-error "Could not connect to leader at %s" leader-addr)
      (kill-emacs 1))
    ;; 2. Return the correct auto-generated client stub, which holds the
    ;;    connection and RPC system needed to make calls.
    (pcase service-key
      (:cluster-admin-service
       (make-cluster-admin-client :rpc-system rpc-system :connection conn))
      (:service-registry-service
       (make-service-registry-client :rpc-system rpc-system :connection conn))
      (:job-queue-service
       (make-job-queue-client :rpc-system rpc-system :connection conn))
      (:manager-service
       (make-manager-client :rpc-system rpc-system :connection conn))
      (_ (warp-cli--print-error "Unknown service key '%s'." service-key)
         (kill-emacs 1)))))

;;;---------------------------------------------------------------------------
;;; Command Handler Functions (Private)
;;;---------------------------------------------------------------------------

(defun warp-cli--handle-start (opts positional)
  "Handle the 'start' subcommand logic.

Arguments:
- `OPTS` (alist): Parsed options for the subcommand.
- `POSITIONAL` (list): Positional arguments (unused).

Returns:
- `nil`.

Side Effects:
- Launches a new Warp cluster leader process."
  (let* ((config-path (alist-get 'config-file opts))
         (config-data (when config-path (yaml-read-file config-path)))
         (final-config (if-let (override (alist-get 'name opts))
                           (plist-put config-data :name override)
                         config-data)))
    (unless final-config
      (warp-cli--print-error "--config-file is required for `start`.")
      (kill-emacs 1))
    ;; Start the cluster creation process, wrapped in a spinner.
    (warp-cli--with-spinner ((format "Starting cluster '%s'..."
                                    (plist-get final-config :name)))
      (let ((cluster (apply #'warp:cluster-create final-config)))
        (warp:cluster-start cluster)))
    (warp-cli--print-success "Cluster leader '%s' is running. Press C-c to stop."
                            (plist-get final-config :name))
    ;; Block to keep the process alive.
    (while t (sleep-for 1))))

(defun warp-cli--handle-stop (opts positional leader-addr)
  "Handle the 'stop' subcommand logic.

Arguments:
- `OPTS` (alist): Parsed options for the subcommand.
- `POSITIONAL` (list): Positional arguments (unused).
- `LEADER-ADDR` (string): The network address of the cluster leader.

Returns:
- `nil`.

Side Effects:
- Sends a `:shutdown-cluster` RPC to the leader."
  (when (or (alist-get 'confirm opts)
            (y-or-n-p (format "Stop leader and all workers at %s? "
                              leader-addr)))
    (let ((client-stub (warp-cli--get-service-client
                        leader-addr :cluster-admin-service)))
      (warp-cli--with-spinner ("Sending shutdown signal...")
        (loom:await (cluster-admin-client-shutdown-cluster client-stub))))
    (warp-cli--print-success "Shutdown signal sent to leader at %s."
                            leader-addr)))

(defun warp-cli--handle-invoke (opts positional leader-addr)
  "Handle the `invoke` command.

Arguments:
- `OPTS` (alist): Parsed options for the command.
- `POSITIONAL` (list): Contains the service and function names.
- `LEADER-ADDR` (string): The network address of the cluster leader.

Returns:
- `nil`.

Side Effects:
- Sends an `:invoke-service` RPC to the leader and prints the result."
  ;; 1. Validate positional arguments.
  (when (< (length positional) 2)
    (warp-cli--print-error "Usage: warp invoke <svc> <func> [options]")
    (kill-emacs 1))
  (let* ((service-name (pop positional))
         (function-name (pop positional))
         (json-payload-str (alist-get 'json opts))
         (payload-file (alist-get 'payload opts))
         (is-async (alist-get 'async opts))
         (payload-data nil)
         (client-stub (warp-cli--get-service-client
                       leader-addr :cluster-admin-service)))

    ;; 2. Determine the payload from either a JSON string or a file.
    (cond (json-payload-str
           (setq payload-data (json-read-from-string json-payload-str)))
          (payload-file
           (setq payload-data (json-read-file payload-file))))

    ;; 3. Make the RPC call via the cluster admin facade.
    (let ((result (warp-cli--with-spinner ((format "Invoking %s->%s..."
                                                  service-name function-name))
                    (loom:await (cluster-admin-client-invoke-service
                                 client-stub nil nil nil service-name
                                 function-name payload-data)))))
      ;; 4. Print the result.
      (if is-async
          (warp-cli--print-success "Service invoked asynchronously. Job ID: %s"
                                  result)
        (princ (json-encode result))
        (princ "\n")))))

(defun warp-cli--handle-list (opts positional leader-addr)
  "Handle the `list` command.

Arguments:
- `OPTS` (alist): Parsed options.
- `POSITIONAL` (list): Contains the type of resource to list.
- `LEADER-ADDR` (string): The network address of the cluster leader.

Returns:
- `nil`.

Side Effects:
- Sends an RPC request and prints a formatted list to stdout."
  (let* ((list-type (car positional))
         (data nil)
         (client-stub (warp-cli--get-service-client
                       leader-addr :cluster-admin-service))
         ;; 1. Determine the correct table headers for the resource type.
         (headers (pcase list-type
                    ("workers" '("ID" "POOL" "HEALTH-STATUS"))
                    ("services" '("SERVICE-NAME" "WORKER-ID" "ADDRESS"))
                    ("plugins" '("NAME" "VERSION"))
                    ("events" '("TYPE" "VERSION")))))
    ;; 2. Call the appropriate RPC method based on the resource type.
    (pcase list-type
      ("workers" (setq data (loom:await
                             (cluster-admin-client-get-all-active-workers
                              client-stub))))
      ("services" (setq data (loom:await
                              (service-registry-client-list-all
                               client-stub))))
      ("plugins" (setq data (loom:await
                             (cluster-admin-client-plugin-list client-stub))))
      ("events" (setq data (loom:await
                            (cluster-admin-client-event-list client-stub))))
      (_ (warp-cli--print-error
          "Unknown resource: '%s'. Choose from: workers, services, plugins, events."
          list-type)
         (kill-emacs 1)))
    ;; 3. Format and print the retrieved data.
    (warp-cli--format-output data opts headers)))

(defun warp-cli--handle-info (opts positional leader-addr)
  "Handle the `info` command.

Arguments:
- `OPTS` (alist): Parsed options.
- `POSITIONAL` (list): Contains the entity type and its ID.
- `LEADER-ADDR` (string): The network address of the cluster leader.

Returns:
- `nil`.

Side Effects:
- Sends an RPC request and prints the raw JSON output to stdout."
  ;; 1. Validate positional arguments.
  (when (< (length positional) 2)
    (warp-cli--print-error "Usage: warp info <type> <id>")
    (kill-emacs 1))
  (let* ((entity-type (pop positional))
         (entity-id (pop positional))
         (data nil)
         (client-stub (warp-cli--get-service-client
                       leader-addr :cluster-admin-service)))
    ;; 2. Call the appropriate RPC method based on the entity type.
    (pcase entity-type
      ("worker" (setq data (loom:await
                            (cluster-admin-client-get-worker-info
                             client-stub entity-id))))
      ("service" (setq data (loom:await
                             (service-registry-client-get-service-info
                              client-stub entity-id))))
      ("plugin" (setq data (loom:await
                            (cluster-admin-client-plugin-info
                             client-stub (intern entity-id)))))
      ("event" (setq data (loom:await
                           (cluster-admin-client-event-info
                            client-stub (intern entity-id)))))
      (_ (warp-cli--print-error "Unknown info type '%s'." entity-type)
         (kill-emacs 1)))
    ;; 3. Print the raw JSON result.
    (princ (json-encode data))
    (princ "\n")))

(defun warp-cli--handle-plugin (opts positional leader-addr)
  "Handle the 'plugin' subcommand for plugin management.

Arguments:
- `OPTS` (alist): Parsed options.
- `POSITIONAL` (list): The action and a list of plugin names.
- `LEADER-ADDR` (string): The network address of the cluster leader.

Returns:
- `nil`.

Side Effects:
- Sends an RPC request to the leader to manage plugins."
  ;; 1. Validate positional arguments.
  (when (< (length positional) 2)
    (warp-cli--print-error "Usage: warp plugin <action> <plugins...>")
    (kill-emacs 1))
  (let* ((action (pop positional))
         (plugin-names (mapcar #'intern positional))
         (worker-ids-str (alist-get 'workers opts))
         (client-stub (warp-cli--get-service-client
                       leader-addr :cluster-admin-service)))
    ;; 2. Dispatch to the correct RPC method based on the action.
    (pcase action
      ("load"
       (warp-cli--with-spinner ((format "Sending 'load' for %s..." plugin-names))
         (loom:await (cluster-admin-client-manage-plugin-load
                      client-stub nil nil nil plugin-names
                      (when worker-ids-str (s-split "," worker-ids-str))))))
      ("unload"
       (warp-cli--with-spinner ((format "Sending 'unload' for %s..." plugin-names))
         (loom:await (cluster-admin-client-manage-plugin-unload
                      client-stub nil nil nil plugin-names
                      (when worker-ids-str (s-split "," worker-ids-str))))))
      (_ (warp-cli--print-error "Unknown plugin action '%s'." action)
         (kill-emacs 1)))
    ;; 3. Print a success message.
    (warp-cli--print-success "Plugin action '%s' for %s dispatched."
                            action positional)))

(defun warp-cli--handle-config (opts positional leader-addr)
  "Handle the 'config' subcommand for dynamic configuration management.

Arguments:
- `OPTS` (alist): Parsed options.
- `POSITIONAL` (list): The config action and its arguments.
- `LEADER-ADDR` (string): The network address of the cluster leader.

Returns:
- `nil`.

Side Effects:
- Sends RPC requests to the `manager-service`."
  (when (null positional)
    (warp-cli--print-error "Usage: warp config <get|set|list|history|rollback> [args]")
    (kill-emacs 1))
  (let* ((action (pop positional))
         (client-stub (warp-cli--get-service-client leader-addr :manager-service)))
    (pcase action
      ("get"
       (let* ((key (intern (car positional) :keyword))
              (result (warp-cli--with-spinner ("Fetching config...")
                        (loom:await (manager-client-get-config client-stub key)))))
         (princ (json-encode result))
         (princ "\n")))
      ("set"
       (let* ((key (intern (car positional) :keyword))
              (value (json-read-from-string (cadr positional))))
         (warp-cli--with-spinner ("Setting config...")
           (loom:await (manager-client-set-config client-stub key value)))
         (warp-cli--print-success "Configuration for '%s' updated." key)))
      ("list"
       (let ((result (warp-cli--with-spinner ("Fetching all config...")
                       (loom:await (manager-client-get-all-config client-stub)))))
         (princ (json-encode result))
         (princ "\n")))
      ("history"
       (let* ((key (intern (car positional) :keyword))
              (result (warp-cli--with-spinner ("Fetching history...")
                        (loom:await (manager-client-get-config-history client-stub key)))))
         (princ (json-encode result))
         (princ "\n")))
      ("rollback"
       (let ((key (intern (car positional) :keyword)))
         (warp-cli--with-spinner ("Rolling back config...")
           (loom:await (manager-client-rollback-config client-stub key)))
         (warp-cli--print-success "Configuration for '%s' rolled back." key)))
      (_ (warp-cli--print-error "Unknown config action '%s'." action)
         (kill-emacs 1)))))

(defun warp-cli--handle-deploy (opts positional leader-addr)
  "Handle the 'deploy' subcommand for declarative deployments.

Arguments:
- `OPTS` (alist): Parsed options.
- `POSITIONAL` (list): The deploy action and its arguments.
- `LEADER-ADDR` (string): The network address of the cluster leader.

Returns:
- `nil`.

Side Effects:
- Sends RPC requests to the `manager-service`."
  (when (null positional)
    (warp-cli--print-error "Usage: warp deploy <plan|apply|status|list|rollback> [args]")
    (kill-emacs 1))
  (let* ((action (pop positional))
         (client-stub (warp-cli--get-service-client leader-addr :manager-service)))
    (pcase action
      ("plan"
       (let* ((manifest-file (alist-get 'file opts))
              (manifest (yaml-read-file manifest-file))
              (plan (warp-cli--with-spinner ("Generating deployment plan...")
                      (loom:await (manager-client-plan-deployment client-stub manifest)))))
         (princ (json-encode plan))
         (princ "\n")))
      ("apply"
       (let* ((plan-file (alist-get 'file opts))
              (plan (json-read-file plan-file))
              (result (warp-cli--with-spinner ("Applying deployment plan...")
                        (loom:await (manager-client-apply-deployment client-stub plan)))))
         (warp-cli--print-success "Deployment started. ID: %s" (plist-get result :deployment-id))))
      ("status"
       (let* ((deploy-id (car positional))
              (result (warp-cli--with-spinner ("Fetching deployment status...")
                        (loom:await (manager-client-get-deployment-status client-stub deploy-id)))))
         (princ (json-encode result))
         (princ "\n")))
      ("list"
       (let ((result (warp-cli--with-spinner ("Fetching deployment history...")
                       (loom:await (manager-client-list-deployments client-stub)))))
         (princ (json-encode result))
         (princ "\n")))
      ("rollback"
       (let* ((deploy-id (car positional))
              (result (warp-cli--with-spinner ("Initiating rollback...")
                        (loom:await (manager-client-rollback-to-deployment client-stub deploy-id)))))
         (warp-cli--print-success "Rollback deployment started. New ID: %s"
                                 (plist-get result :deployment-id))))
      (_ (warp-cli--print-error "Unknown deploy action '%s'." action)
         (kill-emacs 1)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------------
;;; Command and Option Definition Macros (Public)
;;;---------------------------------------------------------------------------

(defmacro warp:cli-define-option (key args)
  "Define a command-line option and register it globally.

Arguments:
- `KEY` (keyword): A keyword symbol for the option (e.g., `:leader`).
- `ARGS` (plist): A plist of attributes for the option.

Returns:
- The `progn` form that registers the option.

Side Effects:
- Modifies the `warp-cli--options-registry` hash table."
  (let* ((long-name (plist-get args :long))
         (type (or (plist-get args :type) :boolean)))
    `(progn
       (puthash ,long-name (list :key ,key ,@args :type ',type)
                warp-cli--options-registry)
       (warp-cli--log "Defined option: --%s" ,long-name))))

(defmacro warp:cli-define-subcommand (key args)
  "Define a subcommand and register it globally.

Arguments:
- `KEY` (string): The name of the subcommand (e.g., \"status\").
- `ARGS` (plist): A plist of attributes for the subcommand.

Returns:
- The `progn` form that registers the subcommand.

Side Effects:
- Modifies the `warp-cli--subcommand-registry` and
  `warp-cli--alias-registry` hash tables."
  (let ((desc (plist-get args :desc))
        (options (plist-get args :options))
        (aliases (plist-get args :aliases))
        (handler (plist-get args :handler)))
    `(progn
       (puthash ,key (list :desc ,desc :options ',options
                           :aliases ',aliases :handler ',handler)
                warp-cli--subcommand-registry)
       ,@(mapcar (lambda (alias) `(puthash ,alias ,key warp-cli--alias-registry))
                 aliases)
       (warp-cli--log "Defined subcommand: %s" ,key))))

;;;---------------------------------------------------------------------------
;;; Main Entry Point
;;;---------------------------------------------------------------------------

(defun warp-cli-main ()
  "The main execution function for the Warp CLI.
This function orchestrates the entire CLI lifecycle: it parses command-line
arguments, dispatches to the appropriate subcommand handler, and manages
top-level error handling and program exit.

Arguments:
- None.

Returns:
- `nil` (this function terminates the process).

Side Effects:
- Prints to `stdout` and `stderr`.
- Terminates the Emacs process with an exit code."
  (condition-case err
      (let* (;; 1. Get raw command-line arguments.
             (cli-args (cdr command-line-args-left))
             (first-arg (car cli-args))
             ;; 2. Look up the subcommand, resolving any aliases.
             (subcommand-name (gethash first-arg warp-cli--alias-registry
                                       first-arg))
             (subcommand-def (gethash subcommand-name
                                      warp-cli--subcommand-registry)))

        ;; 3. Handle global flags like --verbose, --help, and --version.
        (setq warp-cli--verbose-mode (member "--verbose" cli-args
                                             :test 'string=))
        (when (or (null cli-args)
                  (member "--help" cli-args :test 'string=)
                  (member "-h" cli-args :test 'string='))
          (warp-cli--print-auto-help (and subcommand-def subcommand-name))
          (kill-emacs 0))
        (when (member "--version" cli-args :test 'string=)
          (princ (format "Warp CLI Version: %s\n" warp-cli-version))
          (kill-emacs 0))

        ;; 4. If a valid subcommand is found, dispatch to its handler.
        (if-let (handler (and subcommand-def (plist-get subcommand-def :handler)))
            (let* (;; 5. Parse all remaining arguments.
                   (parsed (warp-cli--parse-args (cdr cli-args)))
                   (opts (alist-get :options parsed))
                   (positional (alist-get :positional parsed))
                   (leader-addr (alist-get 'leader opts)))
              ;; 6. Enforce `--leader` option for all relevant commands.
              (when (and (not (string= subcommand-name "start"))
                         (not leader-addr))
                (warp-cli--print-error
                 "--leader <address> is required for '%s'." subcommand-name)
                (kill-emacs 1))
              ;; 7. Invoke the handler function with parsed arguments.
              (funcall handler opts positional leader-addr))
          ;; 8. If the command is not found, print an error and help.
          (warp-cli--print-error "Unknown command: '%s'." first-arg)
          (warp-cli--print-auto-help)
          (kill-emacs 1)))
    ;; 9. Global error handler: catch any unexpected Lisp errors.
    (error
     (warp-cli--print-error "%s" (error-message-string err))
     (kill-emacs 1)))
  (kill-emacs 0))

;; Execute the main function when the script is run.
(warp-cli-main)