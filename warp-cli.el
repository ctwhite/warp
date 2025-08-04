;;; warp-cli.el --- Self-contained Command-Line Interface for the Warp Framework -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This file contains the core Emacs Lisp logic for the Warp command-line
;; interface (CLI). It is designed to be executed by a small, external
;; shell (Bash/PowerShell) wrapper script, which handles locating the
;; `emacs` executable and passing control to this Lisp file.
;;
;; This architecture provides robust cross-platform execution, efficient
;; handling of top-level CLI flags (like `--help` and `--version` at the
;; shell level), and a clean separation between shell invocation logic
;; and the core Emacs Lisp application functionality.
;;
;; The file is structured into two main parts:
;; 1.  **The Warp CLI Framework Library**: A generic, reusable set of
;;     macros and functions (`warp:cli-`, `warp-cli-`) for building
;;     command-line tools in Emacs Lisp. This includes argument
;;     parsing, help generation, and basic UI elements like spinners
;;     and styled text.
;; 2.  **`warp-cli` Application Logic**: This is the
;;     application-specific code that defines the actual Warp cluster
;;     management commands (e.g., `start`, `stop`, `status`,
;;     `workers`, `scale`, `job-submit`). It leverages the CLI
;;     framework library and interacts directly with the core Warp
;;     components (e.g., `warp-cluster`, `warp-rpc`, `warp-transport`)
;;     to send commands to a running cluster leader.
;;
;; To run this CLI:
;; **You should execute the generated shell wrapper script**, not this
;; `.el` file directly.
;;
;; Example wrapper script invocation (after `chmod +x`):
;; ```bash
;; ./warp-cli <command> [options]
;; ```
;;
;; Example usage via wrapper:
;; ```bash
;; ./warp-cli start \
;;   --config /path/to/cluster.yaml --name my-cluster --log-output file --log-file-path ./cluster.log
;; ./warp-cli status \
;;   --leader tcp://127.0.0.1:9000 --log-output shell
;; ./warp-cli job-submit \
;;   --leader tcp://127.0.0.1:9000 --json '{"task": "greet", "name": "World"}'
;; ./warp-cli ping worker worker-abc123 \
;;   --leader tcp://127.0.0.1:9000
;; ./warp-cli metrics worker worker-abc123 \
;;   --leader tcp://127.0.0.1:9000 --format-output json
;; ./warp-cli list jobs \
;;   --leader tcp://127.0.0.1:9000 --format-output ids-only
;; ./warp-cli health details worker-abc123 \
;;   --leader tcp://127.0.0.1:9000 --format-output table
;; ./warp-cli worker-exec worker-abc123 "(+ 1 2)" \
;;   --leader tcp://127.0.0.1:9000 --confirm
;; ./warp-cli --repl # Launch an interactive Emacs Lisp REPL
;; ./warp-cli --version # Show CLI version
;; ```
;;
;; ## Key Features of the CLI:
;;
;; -   **Wrapper-Driven Execution**: Relies on an external shell script
;;     for initial execution, enhancing portability and robustness.
;; -   **Declarative Command Definition**: Commands and their options
;;     are defined declaratively using `warp:cli-define-subcommand` and
;;     `warp:cli-define-option`, simplifying CLI structure.
;; -   **Automatic Help Generation**: The CLI can automatically generate
;;     usage information and help messages from declared commands and
;;     options (`--help`), including subcommand-specific help.
;; -   **Advanced Argument Parsing**: Robustly parses command-line
;;     arguments, supporting long (`--option`), short (`-o`), and
;;     key-value (`--option=value`) formats, with basic type conversion
;;     and required option validation.
;; -   **Flexible Configuration Loading**: Supports loading cluster
;;     configurations from both **JSON** and **YAML** files, enhancing
;;     human readability for config files.
;; -   **Interactive REPL (`--repl`)**: Provides a powerful debugging
;;     experience by launching an Emacs Lisp REPL (`ielm`) with the
;;     CLI's environment and helper functions pre-loaded.
;; -   **Version Information (`--version`)**: Standardized display of
;;     the CLI tool's version string.
;; -   **Enhanced Logging**: Supports verbose logging (`--verbose`) for
;;     detailed debug output. **Configurable log output destination**
;;     (shell, file, or none).
;; -   **User Feedback**: Provides visual cues like spinners for
;;     long-running asynchronous operations and colored output for
;;     success/error messages, enhancing the user experience.
;; -   **Integration with Warp Core**: Directly uses Warp's RPC,
;;     transport, and cluster management APIs to communicate with a
;;     running Warp cluster leader, sending commands and displaying
;;     responses. Includes commands for **pinging workers**, **listing
;;     cluster members, services, and jobs**, and **retrieving detailed
;;     metrics** for different components.
;; -   **External Tool Chaining**: Designed to output structured data
;;     compatible with `jq`/`yq`/`fzf`/`xargs` for advanced scripting.

;;; Code:

(require 'cl-lib) 
(require 'json)   
(require 's)      
(require 'thread) 
(require 'yaml)   
(require 'custom) 
(require 'subr-x) 

;;;---------------------------------------------------------------------
;;; Customization
;;---------------------------------------------------------------------

(defgroup warp-cli nil
  "Customization options for the Warp Command-Line Interface."
  :group 'warp
  :prefix "warp-cli-")

(defcustom warp-cli-version "2.0.0"
  "The version string for the Warp CLI tool."
  :type 'string
  :group 'warp-cli
  :tag "CLI Version")

(defcustom warp-cli-verbose-mode nil
  "A boolean indicating if the CLI is running in verbose mode.
When `t`, additional debug messages from `warp-cli-log` will be
printed to stderr."
  :type 'boolean
  :group 'warp-cli
  :tag "Verbose Mode")

(defvar warp-cli-subcommand nil
  "The detected subcommand string (e.g., \"status\", \"start\")
that the user invoked. This variable is set during argument parsing."
  :type '(or null string))

(defvar warp-cli-options-registry (make-hash-table :test 'equal)
  "A hash table storing the definitions of all global and subcommand-
specific command-line options. Keys are option long names (strings),
values are plists of option attributes (e.g., `:type`, `:desc`)."
  :type 'hash-table)

(defvar warp-cli-subcommand-registry (make-hash-table :test 'equal)
  "A hash table storing the definitions of all subcommands. Keys are
subcommand names (strings), values are plists of subcommand
attributes (e.g., `:desc`, `:options` used by it)."
  :type 'hash-table)

;;;---------------------------------------------------------------------
;;; Logging and Styling Utilities
;;---------------------------------------------------------------------

(defun warp-cli-log (message &rest args)
  "Log a `MESSAGE` to stderr only if `warp-cli-verbose-mode` is `t`.
This function provides a simple internal logging mechanism for the CLI
framework itself, separate from `warp-log` which is for cluster events.

Arguments:
- `MESSAGE` (string): A format-control string.
- `ARGS` (list): Arguments for the format string.

Returns:
- This function is for side-effects and has no meaningful return value."
  (when warp-cli-verbose-mode
    (message "warp-cli: %s" (apply #'format message args))))

(defun warp:cli-style-bold (text)
  "Return `TEXT` formatted as bold using ANSI escape codes.
Used for highlighting important information in the terminal output."
  (format "\e[1m%s\e[0m" text))

(defun warp:cli-style-dim (text)
  "Return `TEXT` formatted as dim using ANSI escape codes.
Used for less prominent or secondary information."
  (format "\e[2m%s\e[0m" text))

(defun warp:cli-style-error (text)
  "Return `TEXT` formatted in red using ANSI escape codes for errors.
Ensures error messages are visually distinct and attention-grabbing."
  (format "\e[31m%s\e[0m" text))

(defun warp:cli-style-success (text)
  "Return `TEXT` formatted in green using ANSI escape codes for success.
Provides positive visual feedback for successful operations."
  (format "\e[32m%s\e[0m" text))

(defun warp:cli-style-info (text)
  "Return `TEXT` formatted in blue using ANSI escape codes for
informational text. Used for general informational messages and prompts."
  (format "\e[34m%s\e[0m" text))

(defun warp:cli-print-error (message &rest args)
  "Print a formatted error `MESSAGE` to stderr in red color.
This function is intended for user-facing error reporting and always
prints, regardless of `warp-cli-verbose-mode`.

Arguments:
- `MESSAGE` (string): A format-control string.
- `ARGS` (list): Arguments for the format string.

Returns:
- This function is for side-effects and has no meaningful return value."
  (message (warp:cli-style-error (apply #'format message args))))

(defun warp:cli-print-success (message &rest args)
  "Print a formatted success `MESSAGE` to stdout in green color.
Used for confirming successful operations to the user."
  (princ (warp:cli-style-success (apply #'format message args))
         (standard-output))
  (princ "\n" (standard-output)))

(defmacro warp:cli-with-spinner ((message) &rest body)
  "Display a spinning activity indicator with `MESSAGE` while
executing `BODY`. `BODY` is expected to contain an asynchronous
operation that returns a `loom-promise`. The spinner runs in a
background thread, preventing the CLI from appearing frozen, and is
automatically cleaned up when the promise settles (resolves or rejects).

Arguments:
- `MESSAGE` (string): The message to display next to the spinner
  (e.g., \"Connecting to leader...\").
- `BODY` (forms): The Lisp forms to execute. The last form must
  return a `loom-promise` (or a value that will be awaited).

Returns:
- The result of the promise returned by `BODY` (or the value of BODY)."
  (let ((result-sym (make-symbol "result"))
        (spinner-thread (make-symbol "spinner-thread")))
    `(let (,spinner-thread)
       (unwind-protect
           (progn
             (setq ,spinner-thread
                   (make-thread
                    (lambda ()
                      (let ((chars '("|" "/" "-" "\\"))
                            (i 0))
                        ;; Loop indefinitely, printing the spinner.
                        (while t
                          (princ (format "\r%s %s" (nth i chars) ,message)
                                 stderr)
                          (force-output stderr)
                          (setq i (mod (1+ i) (length chars)))
                          (sleep-for 0.1))))
                    "cli-spinner"))
             ;; Execute the main body, which should return a promise.
             (setq ,result-sym (progn ,@body))
             ;; Await the promise from the body to ensure it completes
             ;; before proceeding with cleanup.
             (loom:await ,result-sym))
         ;; Cleanup form ensures the spinner thread is killed and the
         ;; line is cleared.
         (when (and ,spinner-thread (thread-live-p ,spinner-thread))
           (kill-thread ,spinner-thread))
         (princ "\r" stderr)
         (dotimes (_ (+ 2 (length ,message))) (princ " " stderr))
         (princ "\r" stderr)
         (force-output stderr)
         (loom:resolved! ,result-sym)))))

;;;---------------------------------------------------------------------
;;; Command and Option Definition Macros
;;---------------------------------------------------------------------

(defmacro warp:cli-define-option (key args)
  "Define a command-line option for the CLI tool.
This macro registers a new option (e.g., `--leader`, `--config`) with
the CLI framework, making it available for parsing and help generation.

Arguments:
- `KEY` (keyword): A keyword symbol for the option (e.g., `:leader`).
  This will be converted to its string representation (e.g., \"leader\")
  for internal use.
- `ARGS` (plist): A plist of attributes for the option:
  - `:long` (string): The full name of the option (e.g., \"leader\").
  - `:short` (string, optional): A single-character short name (e.g., \"l\").
  - `:type` (keyword, optional): The expected type of the option's value
    (`:boolean`, `:string`, `:integer`, `:filepath`). Defaults to
    `:boolean`.
  - `:desc` (string): A short description of the option, used in help.
  - `:default` (any, optional): The default value for the option if not
    provided on the command line.
  - `:required` (boolean, optional): If `t`, this option must be present.

Returns:
- The `progn` form that registers the option.

Side Effects:
- Modifies the `warp-cli-options-registry` hash table, adding the
  option's definition."
  (let* ((long-name (plist-get args :long))
         (type (or (plist-get args :type) :boolean)))
    `(progn
       (puthash ,long-name (list ,@args :type ',type)
                warp-cli-options-registry)
       (warp-cli-log "Defined option: --%s" ,long-name))))

(defmacro warp:cli-define-subcommand (key args)
  "Define a subcommand for the CLI tool.
This macro registers a new subcommand (e.g., `start`, `status`) with
the CLI framework, making it available for parsing and dispatching.

Arguments:
- `KEY` (string): The name of the subcommand (e.g., \"status\").
- `ARGS` (plist): A plist of attributes for the subcommand:
  - `:desc` (string): A short description of the subcommand, used in
    the main help message.
  - `:options` (list of keywords): A list of keywords referencing the
    options that are specifically relevant or required for this
    subcommand. These options must have been previously defined using
    `warp:cli-define-option`.
  - `:required-positional-args-count` (integer, optional): The number
    of mandatory positional arguments this subcommand expects.

Returns:
- The `progn` form that registers the subcommand.

Side Effects:
- Modifies the `warp-cli-subcommand-registry` hash table, adding the
  subcommand's definition."
  (let ((desc (plist-get args :desc))
        (options (plist-get args :options))
        (required-pos-count (plist-get args :required-positional-args-count 0)))
    `(progn
       (puthash ,key (list :desc ,desc
                            :options ',(mapcar (lambda (k) (symbol-name k))
                                               options)
                            :required-positional-args-count
                            ,required-pos-count)
                warp-cli-subcommand-registry)
       (warp-cli-log "Defined subcommand: %s" ,key))))

;;;---------------------------------------------------------------------
;;; Argument Parsing Logic
;;---------------------------------------------------------------------

(defun warp-cli--convert-value (value type-key key-str)
  "Convert a string `VALUE` to its specified `TYPE-KEY`.
This helper performs type coercion for command-line arguments.

Arguments:
- `VALUE` (string): The raw string value from the command line.
- `TYPE-KEY` (keyword): The target type (`:boolean`, `:string`,
  `:integer`, `:filepath`, `:directorypath`, `:float`, `:boolean-string`).
  - `:boolean-string`: specifically for \"yes\", \"no\", \"true\", \"false\",
    \"on\", \"off\", \"1\", \"0\".
  - `:directorypath`: Validates if it's an existing directory.

Returns:
- The converted value in its proper Lisp type (`t`/`nil` for boolean,
  number, expanded file path, or original string).

Signals:
- `user-error`: If the value cannot be converted to the specified type
  or fails validation (e.g., non-numeric string for `:integer`)."
  (let ((converted-value nil))
    (pcase type-key
      (:boolean
       ;; For boolean flags, any presence means true. For --no-flag, it's false.
       ;; `value` will be the string "true", "false", or nil if just a flag.
       (setq converted-value (not (member value '("false" "0" "no")
                                           :test #'string=))))
      (:boolean-string
       ;; More explicit boolean parsing for string values like "yes"/"no".
       (setq converted-value
             (pcase (downcase value)
               ((or "y" "yes" "t" "true" "1" "on") t)
               ((or "n" "no" "f" "false" "0" "off") nil)
               (_ (user-error "Invalid boolean value for '--%s': '%s'.
                               Expected 'yes', 'no', 'true', 'false',
                               'on', 'off', '1', '0'." key-str value)))))
      (:integer
       (if (string-match-p "\\`[-+]?[0-9]+\\'" value)
           (setq converted-value (string-to-number value))
         (user-error "Invalid integer value for '--%s': '%s'."
                     key-str value)))
      (:float
       (if (string-match-p "\\`[-+]?[0-9]*\\.?[0-9]+\\'" value)
           (setq converted-value (string-to-number value))
         (user-error "Invalid float value for '--%s': '%s'."
                     key-str value)))
      (:filepath
       (setq converted-value (expand-file-name value)))
      (:directorypath
       (let ((path (expand-file-name value)))
         (unless (file-directory-p path)
           (user-error "Not a valid directory path for '--%s': '%s'."
                       key-str path))
         (setq converted-value path)))
      (_
       ;; Default to string for unknown types or if explicitly :string.
       (setq converted-value value)))
    converted-value))

(defun warp:cli-parse-args (args)
  "Parse a list of command-line `ARGS` based on defined options and
subcommands. This function processes the raw command-line arguments,
extracts options and their values, identifies the subcommand, and
performs basic validation (e.g., required options, type conversion).

Arguments:
- `ARGS` (list): A list of string arguments to parse, typically
  `command-line-args-left` after the script name.

Returns:
- (plist): A plist containing two keys:
  - `:options` (alist): An alist of `(option-name . value)` pairs for
    all parsed options, including defaults.
  - `:positional` (list): A list of remaining positional arguments.

Signals:
- Terminates the script with `(kill-emacs 1)` on validation failure.

Side Effects:
- Prints error messages to stderr on validation failure."
  (let ((options (make-hash-table :test 'equal))
        (positional '()))
    ;; Populate options hash table with default values.
    (maphash (lambda (k v)
               (when-let (default (plist-get v :default))
                 (puthash k default options)))
             warp-cli-options-registry)
    ;; Parse arguments one by one.
    (while args
      (let ((arg (pop args)))
        (cond
          ;; Long option with equals sign (e.g., `--config=/path/file`).
          ((string-match "^--\\([^=]+\\)=\\(.*\\)" arg)
           (let* ((key (match-string 1 arg)) (val (match-string 2 arg))
                  (def (gethash key warp-cli-options-registry)))
             (if def
                 (puthash key (warp-cli--convert-value val
                                                       (plist-get def :type)
                                                       key)
                            options)
               (warp-cli-log "Warning: Unknown option: %s" arg))))
          ;; Long option without equals (e.g., `--verbose` or `--leader`).
          ((string-match "^--\\(.+\\)" arg)
           (let* ((key (match-string 1 arg))
                  (def (gethash key warp-cli-options-registry)))
             (if def
                 (if (and (not (eq (plist-get def :type) :boolean))
                          (car args)
                          (not (s-starts-p "-" (car args))))
                     (puthash key (warp-cli--convert-value (pop args)
                                                           (plist-get def :type)
                                                           key)
                                options)
                   (puthash key t options))
               (warp-cli-log "Warning: Unknown option: %s" arg))))
          ;; Short option (e.g., `-v` or `-f <file>`).
          ((string-match "^-\\([^-]\\)" arg)
           (let* ((short-key (match-string 1 arg)) (key nil))
             ;; Find the long key associated with the short key.
             (maphash (lambda (k v)
                        (when (equal (plist-get v :short) short-key)
                          (setq key k)))
                      warp-cli-options-registry)
             (if-let ((def (and key (gethash key warp-cli-options-registry))))
                 (if (and (not (eq (plist-get def :type) :boolean))
                          (car args)
                          (not (s-starts-p "-" (car args))))
                     (puthash key (warp-cli--convert-value (pop args)
                                                           (plist-get def :type)
                                                           key)
                                options)
                   (puthash key t options))
               (warp-cli-log "Warning: Unknown short option: -%s" short-key))))
          ;; Positional argument.
          (t (push arg positional)))))
    ;; Validate required options after all parsing.
    (let ((sub-def (if warp-cli-subcommand
                        (gethash warp-cli-subcommand
                                 warp-cli-subcommand-registry)
                      nil))
          (sub-opts (if warp-cli-subcommand
                        (plist-get (gethash warp-cli-subcommand
                                            warp-cli-subcommand-registry)
                                   :options)
                      nil)))
      (maphash
       (lambda (key def)
         (when (and (plist-get def :required)
                    ;; Check if this required option applies to subcommand.
                    (or (not sub-opts) (member key sub-opts :test #'string=))
                    (not (gethash key options)))
           (warp:cli-print-error "Required option '--%s' is missing." key)
           (kill-emacs 1)))
       warp-cli-options-registry)
      ;; Validate positional arguments for the subcommand.
      (when sub-def
        (let* ((required-pos-count (plist-get sub-def
                                              :required-positional-args-count 0))
               (actual-pos-count (length positional)))
          (when (< actual-pos-count required-pos-count)
            (warp:cli-print-error
             "Subcommand '%s' requires %d positional argument(s) "
             "but received %d."
             warp-cli-subcommand required-pos-count actual-pos-count)
            (kill-emacs 1)))))
    (list :options (hash-table-alist options)
          :positional (nreverse positional))))

;;;---------------------------------------------------------------------
;;; Help Message Generation
;;---------------------------------------------------------------------

(defun warp-cli--format-option-help-line (opt-def)
  "Helper to format a single line of option help text.
Used by `warp:cli-print-auto-help` to create a nicely aligned and
descriptive line for each option in the help message.

Arguments:
- `OPT-DEF` (plist): The plist definition for a single option (from
  `warp-cli-options-registry`).

Returns:
- (string): A formatted string for one line in the help message."
  (let* ((long (plist-get opt-def :long))
         (short (plist-get opt-def :short))
         (desc (plist-get opt-def :desc))
         (default (plist-get opt-def :default))
         (flag-str (concat (if short (format "-%s, " short) "    ")
                           (format "--%s" long)))
         (desc-str (format "%s%s" (or desc "")
                           (if default (format " (default: %s)"
                                               (prin1-to-string default))
                             ""))))
    (format "  %-22s %s" flag-str desc-str)))

(defun warp:cli-print-auto-help (&optional for-subcommand)
  "Generate and print a help message from defined commands and options.
This function dynamically constructs a usage and options summary based
on the CLI's internal registry of commands and options. It can print
either the global help message or help specific to a given subcommand.

Arguments:
- `FOR-SUBCOMMAND` (string, optional): The name of the subcommand to
  generate detailed help for. If `nil`, prints the global help
  message showing all commands and global options.

Returns:
- This function is for side-effects and has no meaningful return value."
  (let ((tool-name (file-name-nondirectory (car command-line-args-left))))
    (if for-subcommand
        ;; Print help for a specific subcommand.
        (if-let ((sub-def (gethash for-subcommand
                                   warp-cli-subcommand-registry)))
            (princ (format "%s\n\nUsage: %s %s [options]%s\n\n%s\n\nOptions for '%s':\n%s\n"
                           (warp:cli-style-bold "Warp CLI Help")
                           (warp:cli-style-bold tool-name) for-subcommand
                           (let ((pos-count (plist-get sub-def :required-positional-args-count 0)))
                             (if (> pos-count 0) (format " <%s>" (s-join "> <" (cl-loop for i from 1 to pos-count collect (format "arg%d" i)))) "")
                           (plist-get sub-def :desc)
                           for-subcommand
                           (mapconcat
                            (lambda (opt-key)
                              (warp-cli--format-option-help-line
                               (gethash opt-key warp-cli-options-registry)))
                            (plist-get sub-def :options)
                            "\n")))
          (warp:cli-print-error "Unknown command: '%s'." for-subcommand))
      ;; Print global help message.
      (princ (format "%s\n\nUsage: %s [global-options] <command> [command-options] [args...]\n\nGlobal Options:\n%s\nCommands:\n%s\nRun '%s <command> --help' for command details.\n"
                     (warp:cli-style-bold "Warp CLI Help")
                     (warp:cli-style-bold tool-name)
                     ;; Format global options.
                     (mapconcat (lambda (item)
                                  (warp-cli--format-option-help-line (cdr item)))
                                (hash-table-alist warp-cli-options-registry)
                                "\n")
                     ;; Format subcommands list.
                     (mapconcat (lambda (item)
                                  (format "  %-18s %s\n" (car item)
                                          (plist-get (cdr item) :desc)))
                                (hash-table-alist warp-cli-subcommand-registry)
                                "\n")
                     tool-name)))))

;;;---------------------------------------------------------------------
;;; Table Formatting Utility
;;---------------------------------------------------------------------

(defun warp:cli-format-table (headers rows)
  "Format a list of `ROWS` into a string as a clean, aligned ASCII table.
This utility automatically calculates column widths based on content
to ensure proper alignment, making tabular data presentable in the
terminal.

Arguments:
- `HEADERS` (list of strings): A list of column headers (e.g.,
  `(\"Name\" \"Status\")`).
- `ROWS` (list of lists of strings): A list where each inner list
  represents a row of data, with each element being a string for a
  column.

Returns:
- (string): A single string containing the formatted ASCII table."
  (let* ((num-cols (length headers))
         ;; Initialize column widths with header lengths.
         (col-widths (cl-loop for h in headers collect (length h)))
         (all-content (cons headers rows)))
    ;; Calculate maximum width for each column.
    (dotimes (i num-cols)
      (setf (nth i col-widths)
            (apply #'max (mapcar (lambda (row) (length (nth i row)))
                                 all-content))))
    (let ((out (make-string-output-stream)))
      ;; Print headers.
      (dotimes (i num-cols)
        (princ (format "%-*s  " (nth i col-widths)
                       (warp:cli-style-bold (nth i headers))) out))
      (princ "\n" out)
      ;; Print separator line.
      (dotimes (i num-cols)
        (princ (make-string (nth i col-widths) ?-) out)
        (princ "    " out))
      (princ "\n" out)
      ;; Print rows.
      (dolist (row rows)
        (dotimes (i num-cols)
          (princ (format "%-*s  " (nth i col-widths) (nth i row)) out))
        (princ "\n" out))
      (get-output-stream-string out))))

;; Load necessary Warp libraries from the project root.
;; This ensures that the CLI has access to the full Warp API.
(let ((warp-root (file-name-directory (or load-file-name
                                           (buffer-file-name)))))
  (add-to-list 'load-path warp-root)
  (require 'warp-cluster)
  (require 'warp-rpc)
  (require 'warp-protocol)
  (require 'warp-transport)
  (require 'warp-job-queue) ; For job-related commands
  (require 'loom)
  (require 'braid)
  (require 'loom-log) ; Ensure loom-log is required for appender setup.
  (require 'warp-service) ; For listing services
  (require 'warp-connection-manager)) ; For pinging specific workers

;; --- Define Global Options for `warp-cli` ---
(warp:cli-define-option :leader
                        '(:long "leader"
                          :short "l"
                          :type :string
                          :desc "Address of the cluster leader (e.g.,
                                 tcp://127.0.0.1:9001). All commands
                                 except 'start' require this."))
(warp:cli-define-option :verbose
                        '(:long "verbose"
                          :short "v"
                          :type :boolean
                          :desc "Enable verbose logging output to stderr."))
(warp:cli-define-option :config-file
                        '(:long "config-file"
                          :short "c"
                          :type :filepath
                          :desc "Path to a global or local configuration
                                 file (e.g., ~/.warp.el or ./.warp.json)."))
(warp:cli-define-option :format
                        '(:long "format"
                          :type :string
                          :default "json"
                          :choices ("json" "yaml" "el")
                          :desc "Format of the configuration file specified
                                 by --config-file or --config."))
(warp:cli-define-option :log-output
                        '(:long "log-output"
                          :type :string
                          :default "shell"
                          :choices ("shell" "file" "none")
                          :desc "Destination for cluster logs: 'shell'
                                 (stderr), 'file', or 'none'."))
(warp:cli-define-option :log-file-path
                        '(:long "log-file-path"
                          :type :filepath
                          :desc "Path to a file for cluster logs, required if
                                 --log-output is 'file'."
                          :required (lambda (opts)
                                      (string= (alist-get 'log-output opts) "file"))))
(warp:cli-define-option :ids-only
                        '(:long "ids-only"
                          :type :boolean
                          :desc "(DEPRECATED - use --format-output ids-only)
                                 Output only IDs, one per line, without
                                 headers or tables. Useful for scripting
                                 and piping to tools like fzf."))
(warp:cli-define-option :output-json
                        '(:long "output-json"
                          :type :boolean
                          :desc "(DEPRECATED - use --format-output json)
                                 Output results as raw JSON instead of
                                 formatted tables. Useful for piping to
                                 jq or other parsers."))
(warp:cli-define-option :format-output
                        '(:long "format-output"
                          :type :string
                          :default "table"
                          :choices ("json" "yaml" "table" "ids-only" "fzf-friendly")
                          :desc "Format of the command output: 'json',
                                 'yaml', 'table', 'ids-only', or
                                 'fzf-friendly' (for piping to fzf)."))
(warp:cli-define-option :confirm
                        '(:long "confirm"
                          :short "y"
                          :type :boolean
                          :desc "Skip interactive confirmation prompts for
                                 destructive actions."))
(warp:cli-define-option :interactive
                        '(:long "interactive"
                          :type :boolean
                          :desc "Enable interactive selection mode (requires fzf)."))
(warp:cli-define-option :repl
                        '(:long "repl"
                          :desc "Launch an interactive Emacs Lisp REPL
                                 (ielm) with the CLI environment loaded.
                                 Useful for debugging."))
(warp:cli-define-option :version
                        '(:long "version"
                          :desc "Print the CLI tool's version and exit."))
(warp:cli-define-option :skip-network-check
                        '(:long "skip-network-check"
                          :type :boolean
                          :desc "Skip the pre-flight network reachability
                                 check to the leader. Use with caution."))
(warp:cli-define-option :public-key-file
                        '(:long "public-key-file"
                          :type :filepath
                          :desc "Path to the public key file (e.g., .pem,
                                 .pub) for JWT updates."))
(warp:cli-define-option :payload
                        '(:long "payload"
                          :type :filepath
                          :desc "Path to a JSON file containing the job payload."))
(warp:cli-define-option :json
                        '(:long "json"
                          :type :string
                          :desc "JSON string directly providing the job payload."))
(warp:cli-define-option :priority
                        '(:long "priority"
                          :type :string
                          :default "normal"
                          :choices ("high" "normal" "low")
                          :desc "Priority for job submission (high, normal, low)."))

;; --- Helper for loading configuration from various formats ---
(defun warp-cli--load-config-file (path format)
  "Loads configuration from `PATH` based on `FORMAT`.
Supports `.json`, `.yaml`, and `.el` files.

Arguments:
- `PATH` (string): The path to the configuration file.
- `FORMAT` (string): The explicit format (\"json\","yaml\","el\").

Returns:
- (plist): The parsed configuration data as a plist.

Signals:
- Terminates the script with an error if file not found or invalid format."
  (unless (file-exists-p path)
    (warp:cli-print-error "Config file not found: %s" path)
    (kill-emacs 1))
  (let ((data nil))
    (pcase format
      ("json"
       (condition-case err
           (setq data (json-read-file path))
         (error
          (warp:cli-print-error "Invalid JSON config file '%s': %S"
                                path err)
          (kill-emacs 1))))
      ("yaml"
       (condition-case err
           (setq data (yaml-read-file path))
         (error
          (warp:cli-print-error "Invalid YAML config file '%s': %S"
                                path err)
          (kill-emacs 1))))
      ("el"
       (condition-case err
           (with-temp-buffer
             (insert-file-contents path)
             (setq data (eval-buffer)))
         (error
          (warp:cli-print-error "Error loading Emacs Lisp config file '%s': %S"
                                path err)
          (kill-emacs 1))))
      (_
       (warp:cli-print-error "Unsupported config file format specified: %s"
                             format)
       (kill-emacs 1)))
    (unless (plistp data)
      (warp:cli-print-error "Config file '%s' did not yield a valid plist."
                            path)
      (kill-emacs 1))
    data))

;; --- Interactive Confirmation Helper ---
(defun warp-cli--confirm-action (prompt opts)
  "Prompts user for confirmation unless --confirm is used.
Arguments:
- `prompt` (string): The confirmation question.
- `opts` (alist): Parsed command-line options.
Returns: t if confirmed, nil if not.
Signals: Terminates if not confirmed."
  (if (alist-get 'confirm opts)
      t
    (unless (y-or-n-p (format "%s (y/n) " prompt))
      (message "Action cancelled.")
      (kill-emacs 0))))

;; --- Generic Output Formatting Function ---
(defun warp-cli--output-data (data opts &optional custom-headers)
  "Formats and prints data based on --format-output option.
Handles general list output (e.g., workers, jobs) and single entity
output (e.g., info commands).

Arguments:
- `data` (plist or list of plists): The data to output.
- `opts` (alist): Parsed command-line options, including format-output.
- `custom-headers` (list, optional): List of strings for table headers.
  Required if `format-output` is 'table' and `data` is a list.
Returns: nil.
Signals: Terminates if output fails."
  (let* ((format-type (alist-get 'format-output opts))
         (output-json-p (alist-get 'output-json opts))
         (ids-only-p (alist-get 'ids-only opts))
         (interactive-p (alist-get 'interactive opts)))

    ;; Resolve deprecated aliases for format-type
    (setq format-type (cond
                       (interactive-p "fzf-friendly") ; Special format for FZF
                       (output-json-p "json")
                       (ids-only-p "ids-only")
                       (t format-type)))

    (pcase format-type
      ("ids-only"
       (unless (listp data)
         (warp:cli-print-error "Output is not a list; cannot output as IDs only.")
         (kill-emacs 1))
       (dolist (item data)
         (princ (plist-get item :id) (standard-output))
         (princ "\n" (standard-output))))
      ("fzf-friendly"
       (unless (listp data)
         (warp:cli-print-error "Output is not a list; cannot format for FZF.")
         (kill-emacs 1))
       (dolist (item data)
         (princ (format "%s\t%s"
                        (plist-get item :id)
                        (or (plist-get item :name)
                            (plist-get item :service-name)
                            (plist-get item :check-name) ; For health checks
                            (plist-get item :error-message) ; For job errors
                            (format "%S" item)))
                (standard-output))
         (princ "\n" (standard-output))))
      ("json"
       (princ (json-encode data) (standard-output))
       (princ "\n" (standard-output)))
      ("yaml"
       (unless (fboundp 'yaml-write-to-string)
         (warp:cli-print-error "YAML output not available (yaml-write-to-string
                                not found). Falling back to JSON.")
         (princ (json-encode data) (standard-output))
         (princ "\n" (standard-output))
         (cl-return-from warp-cli--output-data nil))
       (princ (yaml-write-to-string data) (standard-output))
       (princ "\n" (standard-output)))
      ("table"
       (unless (listp data)
         (warp:cli-print-error "Output is not a list; cannot format as table.
                                 Use json/yaml.")
         (kill-emacs 1))
       (unless custom-headers
         (warp:cli-print-error "Table format requires custom-headers."))
       (princ (warp:cli-format-table custom-headers data)))
      (_
       (warp:cli-print-error "Invalid output format type: %s" format-type)
       (kill-emacs 1))))
  nil)

;; --- Helper for RPC calls ---
(defun warp-cli--send-rpc-command (leader-addr command-name
                                   &key args spinner-message
                                   (sender-id "cli-client")
                                   (target-id "leader")
                                   (expect-response t))
  "Helper to connect to leader (or specific worker via leader), send RPC,
and close connection. Includes spinner and error handling.

Arguments:
- `leader-addr` (string): The leader's address.
- `command-name` (keyword): The RPC command name (e.g.,
  `:cluster-metrics`).
- `args` (any, optional): Arguments for the RPC command.
- `spinner-message` (string, optional): Message for the spinner.
- `sender-id` (string, optional): ID of the RPC sender.
- `target-id` (string, optional): ID of the RPC target (defaults to
 \"leader\").
- `expect-response` (boolean, optional): If `t`, expects an RPC
  response.

Returns:
- (any): The payload of the RPC response if `expect-response` is `t`.
  Otherwise, `t` on successful command send.

Signals:
- Terminates script on connection or RPC error."
  (let* ((rpc-system (warp:rpc-system-create))
         (conn (warp:cli-with-spinner ("Connecting to leader...")
                 (loom:await (warp:transport-connect leader-addr)))))
    (unless conn
      (warp:cli-print-error "Could not connect to leader at %s"
                            leader-addr)
      (kill-emacs 1))
    (unwind-protect
        (let* ((cmd (make-warp-rpc-command :name command-name :args args))
               (response (warp:cli-with-spinner ((or spinner-message
                                                      (format "Sending '%S'
                                                                command to %s..."
                                                              command-name
                                                              target-id)))
                           (loom:await (warp:rpc-request rpc-system conn
                                                         sender-id target-id
                                                         cmd
                                                         :expect-response
                                                         expect-response)))))
          (if expect-response
              (progn
                (unless (eq (warp-rpc-response-status response) :success)
                  (warp:cli-print-error "Command '%S' failed: %S"
                                        command-name
                                        (warp-rpc-response-error-details
                                         response))
                  (kill-emacs 1))
                (warp-rpc-response-payload response))
            t))
      ;; Cleanup: ensure the RPC connection is closed.
      (loom:await (warp:transport-close conn)))))

;; --- Handler for 'start' subcommand ---
(defun warp-cli--handle-start-command (opts positional)
  "Handles the 'start' subcommand logic.
Launches a new Warp cluster leader based on a configuration file.

Arguments:
- `opts` (alist): Parsed options for the subcommand.
- `positional` (list): Positional arguments (unused for 'start')."
  (let* ((config-path (alist-get 'config-file opts))
         ;; Determine config file format from option or extension
         (file-ext (file-name-extension config-path))
         (config-format (or (alist-get 'format opts) file-ext))
         (config-data (warp-cli--load-config-file config-path config-format))
         (final-config (if-let (override-name (alist-get 'name opts))
                           (plist-put config-data :name override-name)
                         config-data)))
    ;; Start cluster leader. Use a spinner for asynchronous op.
    (warp:cli-with-spinner ((format "Starting cluster '%s'..."
                                    (plist-get final-config :name)))
      (let ((cluster (apply #'warp:cluster-create final-config)))
        (warp:cluster-start cluster)))
    (warp:cli-print-success "Cluster leader '%s' is running. "
                            "Press C-c to stop."
                            (plist-get final-config :name))
    ;; Keep the CLI script running, acting as the leader process.
    (while t (sleep-for 1))))

;; --- Handler for 'job-submit' subcommand ---
(defun warp-cli--handle-job-submit-command (opts positional leader-addr)
  "Handles the 'job-submit' subcommand logic.
Submits a new job to the distributed job queue.

Arguments:
- `opts` (alist): Parsed options for the subcommand.
- `positional` (list): Positional arguments (unused for job-submit).
- `leader-addr` (string): The leader's address."
  (let* ((priority-sym (intern (s-concat ":" (alist-get 'priority opts))))
         (payload-file (alist-get 'payload opts))
         (json-payload-str (alist-get 'json opts))
         (payload-data nil))
    ;; Validate mutual exclusivity
    (when (and payload-file json-payload-str)
      (warp:cli-print-error "Cannot use both --payload and --json.
                               Please choose one.")
      (kill-emacs 1))
    ;; Determine payload source
    (cond
      (payload-file
       (unless (file-exists-p payload-file)
         (warp:cli-print-error "Payload file not found: %s" payload-file)
         (kill-emacs 1))
       (setq payload-data (json-read-file payload-file)))
      (json-payload-str
       (condition-case json-err
           (setq payload-data (json-parse-string json-payload-str))
         (error
          (warp:cli-print-error "Invalid JSON payload string: %S (Error: %S)"
                                json-payload-str json-err)
          (kill-emacs 1))))
      (t
       (warp:cli-print-error "No job payload provided. Use --payload or --json.")
       (kill-emacs 1)))

    (let ((job-id (warp-cli--send-rpc-command
                   leader-addr :job-submit
                   :args `(:priority ,priority-sym
                           :payload ,payload-data)
                   :spinner-message "Submitting job...")))
      (warp:cli-print-success "Job submitted successfully. ID: %s" job-id))))

;; --- Handler for 'metrics' group subcommand ---
(defun warp-cli--handle-metrics-command (opts positional leader-addr)
  "Handles the 'metrics' group subcommand logic.
Dispatches to specific metrics commands (cluster, worker, job-queue).

Arguments:
- `opts` (alist): Parsed options.
- `positional` (list): Positional arguments (e.g.,"worker\","<id>\").
- `leader-addr` (string): The leader's address."
  (let* ((metrics-type (car positional))
         (target-id (cadr positional))
         (metrics nil))

    (pcase metrics-type
      ("cluster"
       (unless (null target-id)
         (warp:cli-print-error "metrics cluster takes no additional arguments.")
         (kill-emacs 1))
       (setq metrics (warp-cli--send-rpc-command leader-addr
                                                 :cluster-metrics
                                                 :spinner-message
                                                 "Fetching cluster metrics..."))
       (warp-cli--output-data metrics opts
                              `("METRIC" "VALUE")
                              `((,(warp:cli-style-info "Leader Address")
                                 ,(or (alist-get 'leader-address metrics)
                                      leader-addr))
                                (,(warp:cli-style-info "Active Workers")
                                 ,(format "%s" (plist-get metrics :total-workers)))
                                (,(warp:cli-style-info "Avg. CPU")
                                 ,(format "%.2f %%" (plist-get metrics :avg-cluster-cpu-utilization)))
                                (,(warp:cli-style-info "Total Memory")
                                 ,(format "%.2f MB" (plist-get metrics :avg-cluster-memory-utilization)))
                                (,(warp:cli-style-info "Active Requests")
                                 ,(format "%s" (plist-get metrics :total-active-requests)))
                                (,(warp:cli-style-info "Processed Requests")
                                 ,(format "%s" (plist-get metrics :total-processed-requests)))
                                (,(warp:cli-style-info "Failed Requests")
                                 ,(format "%s" (plist-get metrics :total-failed-requests))))))

      ("worker"
       (unless target-id
         (warp:cli-print-error "metrics worker requires a worker ID.")
         (kill-emacs 1))
       (setq metrics (warp-cli--send-rpc-command leader-addr
                                                 :get-worker-metrics
                                                 :args `(:worker-id ,target-id)
                                                 :spinner-message
                                                 (format "Fetching metrics
                                                          for worker %s..."
                                                          target-id)))
       (warp-cli--output-data metrics opts
                              `("METRIC" "VALUE")
                              `((,(warp:cli-style-info "Worker ID")
                                 ,(plist-get metrics :worker-id))
                                (,(warp:cli-style-info "Health Status")
                                 ,(format "%S"
                                          (plist-get metrics :health-status)))
                                (,(warp:cli-style-info "Uptime (s)")
                                 ,(format "%.1f"
                                          (plist-get metrics :uptime-seconds)))
                                (,(warp:cli-style-info "CPU Util %")
                                 ,(format "%.2f"
                                          (plist-get (plist-get metrics :process-metrics)
                                                     :cpu-utilization)))
                                (,(warp:cli-style-info "Memory MB")
                                 ,(format "%.2f"
                                          (plist-get (plist-get metrics :process-metrics)
                                                     :memory-utilization-mb)))
                                (,(warp:cli-style-info "Active Requests")
                                 ,(format "%s"
                                          (plist-get metrics :active-request-count)))
                                (,(warp:cli-style-info "Total Requests")
                                 ,(format "%s"
                                          (plist-get metrics :total-requests-processed)))))))

      ("job-queue"
       (unless (null target-id)
         (warp:cli-print-error "metrics job-queue takes no additional arguments.")
         (kill-emacs 1))
       (setq metrics (warp-cli--send-rpc-command leader-addr
                                                 :job-get-metrics
                                                 :target-id "job-queue-worker"
                                                 :spinner-message
                                                 "Fetching job queue metrics..."))
       (warp-cli--output-data metrics opts
                              `("METRIC" "VALUE")
                              (mapcar (lambda (kv)
                                        (list (format "%S" (car kv))
                                              (format "%S" (cdr kv))))
                                      (alist-sort #'string<
                                                  (hash-table-alist metrics))))))

      (_
       (warp:cli-print-error "Unknown metrics type: '%s'.
                               Choose from 'cluster', 'worker', 'job-queue'."
                             metrics-type)
       (kill-emacs 1)))))

;; --- Handler for 'list' group subcommand ---
(defun warp-cli--handle-list-command (opts positional leader-addr)
  "Handles the 'list' group subcommand logic.
Dispatches to specific list commands (workers, jobs, services, clusters).

Arguments:
- `opts` (alist): Parsed options.
- `positional` (list): Positional arguments (e.g., \"workers\").
- `leader-addr` (string): The leader's address."
  (let* ((list-type (car positional))
         (data nil))
    (pcase list-type
      ("workers"
       (setq data (warp-cli--send-rpc-command leader-addr :list-workers
                                               :spinner-message
                                               "Fetching worker list...")))
      ("jobs"
       (setq data (warp-cli--send-rpc-command leader-addr :job-list
                                               :target-id "job-queue-worker"
                                               :spinner-message
                                               "Fetching job list...")))
      ("services"
       (setq data (warp-cli--send-rpc-command leader-addr :service-list-all
                                               :spinner-message
                                               "Fetching service list...")))
      ("clusters"
       (setq data (warp-cli--send-rpc-command
                   leader-addr :cluster-list-all
                   :spinner-message "Fetching cluster list...")))
      (_
       (warp:cli-print-error "Unknown list type: '%s'.
                               Choose from 'workers', 'jobs', 'services',
                               'clusters'."
                             list-type)
       (kill-emacs 1)))

    (pcase list-type
      ("workers"
       (warp-cli--output-data data opts
                              '("WORKER ID" "POOL" "STATUS" "UPTIME (min)")
                              (mapcar (lambda (w)
                                        (list (plist-get w :id)
                                              (plist-get w :pool)
                                              (format "%S"
                                                      (plist-get w :health-status))
                                              (format "%.1f"
                                                      (/ (plist-get w :uptime) 60.0))))
                                      data)))
      ("jobs"
       (warp-cli--output-data data opts
                              '("JOB ID" "STATUS" "PRIORITY" "RETRY COUNT" "WORKER ID")
                              (mapcar (lambda (j)
                                        (list (plist-get j :id)
                                              (format "%S"
                                                      (plist-get j :status))
                                              (format "%S"
                                                      (plist-get j :priority))
                                              (format "%s"
                                                      (plist-get j :retry-count))
                                              (or (plist-get j :worker-id)
                                                  "N/A")))
                                      data)))
      ("services"
       (warp-cli--output-data data opts
                              '("SERVICE NAME" "WORKER ID" "HEALTH" "ADDRESS")
                              (mapcar (lambda (s)
                                        (list (plist-get s :service-name)
                                              (plist-get s :worker-id)
                                              (format "%S"
                                                      (plist-get s :health-status))
                                              (plist-get s :address)))
                                      data)))
      ("clusters"
       (warp-cli--output-data data opts
                              '("CLUSTER ID" "NAME" "LEADER ADDRESS" "STATUS")
                              (mapcar (lambda (c)
                                        (list (plist-get c :id)
                                              (plist-get c :name)
                                              (plist-get c :leader-address)
                                              (format "%S"
                                                      (plist-get c :status))))
                                      data))))))

;; --- Handler for 'ping' group subcommand ---
(defun warp-cli--handle-ping-command (opts positional leader-addr)
  "Handles the 'ping' group subcommand logic.
Pings the leader or a specific worker.

Arguments:
- `opts` (alist): Parsed options.
- `positional` (list): Positional arguments (e.g., \"worker\", \"<id>\").
- `leader-addr` (string): The leader's address."
  (let* ((ping-type (car positional))
         (target-id (cadr positional)))
    (pcase ping-type
      ("leader"
       (unless (null target-id)
         (warp:cli-print-error "ping leader takes no additional arguments.")
         (kill-emacs 1))
       (let ((response (warp-cli--send-rpc-command leader-addr :ping
                                                   :spinner-message
                                                   (format "Pinging leader at %s..."
                                                           leader-addr))))
         (if (equal response "pong")
             (warp:cli-print-success "Leader at %s responded: %S"
                                     leader-addr response)
           (warp:cli-print-error "Leader at %s did not respond with 'pong'."
                                 leader-addr))))

      ("worker"
       (unless target-id
         (warp:cli-print-error "ping worker requires a worker ID.")
         (kill-emacs 1))
       ;; To ping a worker directly, we need its RPC address. We ask the leader's
       ;; service registry for the worker's "worker-api" service endpoint.
       (let* ((target-worker-id target-id)
              (endpoint (warp-cli--send-rpc-command leader-addr
                                                    :get-service-info ; Changed RPC name
                                                    :args `(:service-name "worker-api"
                                                            :worker-id ,target-worker-id)
                                                    :spinner-message
                                                    (format "Discovering RPC
                                                              address for worker %s..."
                                                            target-worker-id)))
              (worker-api-address (plist-get endpoint :address)))
         (unless worker-api-address
           (warp:cli-print-error "Could not find RPC address for worker %s."
                                 target-worker-id)
           (kill-emacs 1))

         ;; Now connect directly to the worker's API address (not via leader) and ping.
         (let* ((worker-rpc-system (warp:rpc-system-create))
                (worker-conn (warp:cli-with-spinner ((format "Pinging worker %s
                                                               at %s..."
                                                              target-worker-id
                                                              worker-api-address))
                               (loom:await (warp:transport-connect
                                            worker-api-address)))))
           (unwind-protect
               (let* ((ping-cmd (make-warp-rpc-command :name :ping))
                      (response (loom:await (warp:rpc-request worker-rpc-system
                                                               worker-conn
                                                               "cli-client"
                                                               target-worker-id
                                                               ping-cmd))))
                 (if (equal response "pong")
                     (warp:cli-print-success "Worker %s responded: %S"
                                             target-worker-id response)
                   (warp:cli-print-error "Worker %s did not respond with 'pong'."
                                         target-worker-id)))
             (loom:await (warp:transport-close worker-conn))))))
      (_
       (warp:cli-print-error "Unknown ping target: '%s'.
                               Choose from 'leader' or 'worker'."
                             ping-type)
       (kill-emacs 1)))))

;; --- Handler for 'info' group subcommand ---
(defun warp-cli--handle-info-command (opts positional leader-addr)
  "Handles the 'info' group subcommand logic.
Retrieves and displays detailed information about a specific cluster
entity.

Arguments:
- `opts` (alist): Parsed options.
- `positional` (list): Positional arguments (e.g., \"worker\", \"<id>\").
- `leader-addr` (string): The leader's address."
  (let* ((entity-type (car positional))
         (entity-id (cadr positional))
         (info-data nil))
    (unless entity-id
      (warp:cli-print-error "info %s requires an ID." entity-type)
      (kill-emacs 1))

    (pcase entity-type
      ("worker"
       (setq info-data (warp-cli--send-rpc-command leader-addr
                                                   :get-worker-info
                                                   :args `(:worker-id ,entity-id)
                                                   :spinner-message
                                                   (format "Fetching info
                                                            for worker %s..."
                                                            entity-id))))
      ("job"
       (setq info-data (warp-cli--send-rpc-command leader-addr :job-status
                                                   :args `(:job-id ,entity-id)
                                                   :target-id "job-queue-worker"
                                                   :spinner-message
                                                   (format "Fetching info
                                                            for job %s..."
                                                            entity-id))))
      ("service"
       (setq info-data (warp-cli--send-rpc-command leader-addr
                                                   :get-service-info
                                                   :args `(:service-name ,entity-id)
                                                   :spinner-message
                                                   (format "Fetching info
                                                            for service %s..."
                                                            entity-id))))
      ("cluster"
       (setq info-data (warp-cli--send-rpc-command leader-addr
                                                   :get-cluster-info
                                                   :args `(:cluster-id ,entity-id)
                                                   :spinner-message
                                                   (format "Fetching info
                                                            for cluster %s..."
                                                            entity-id))))
      ("state"
       (unless (= (length positional) 2)
         (warp:cli-print-error "state get requires a <path> argument.")
         (kill-emacs 1))
       (let* ((state-path (cadr positional))
              (path-list (s-split "/" state-path)))
         (setq info-data (warp-cli--send-rpc-command leader-addr :state-get
                                                     :args `(:path ,path-list)
                                                     :spinner-message
                                                     (format "Fetching
                                                              state for
                                                              path %s..."
                                                              state-path)))))
      ("health"
       (unless (= (length positional) 2)
         (warp:cli-print-error "health details requires a <worker-id> argument.")
         (kill-emacs 1))
       (let* ((worker-id (cadr positional))
              (details (warp-cli--send-rpc-command leader-addr
                                                  :get-worker-health-details
                                                  :args `(:worker-id ,worker-id)
                                                  :spinner-message
                                                  (format "Fetching
                                                           health
                                                           details
                                                           for worker %s..."
                                                           worker-id))))
         (setq info-data details)))
      (_
       (warp:cli-print-error "Unknown info type: '%s'.
                               Choose from 'worker', 'job', 'service',
                               'cluster', 'state', 'health'."
                             entity-type)
       (kill-emacs 1)))

    ;; Always output raw JSON for info commands for easy parsing by external tools
    ;; like jq/fzf preview.
    (princ (json-encode info-data) (standard-output))))

;; --- Handler for 'state' group subcommand ---
(defun warp-cli--handle-state-command (opts positional leader-addr)
  "Handles the 'state' group subcommand logic.
Interacts with the cluster's distributed state manager.

Arguments:
- `opts` (alist): Parsed options.
- `positional` (list): Positional arguments (e.g., \"get\", \"<path>\").
- `leader-addr` (string): The leader's address."
  (let* ((state-op (car positional))
         (output-json-p (alist-get 'output-json opts))
         (confirm-p (alist-get 'confirm opts)))

    (pcase state-op
      ("get"
       (unless (= (length positional) 2)
         (warp:cli-print-error "state get requires a <path> argument.")
         (kill-emacs 1))
       (let* ((state-path (cadr positional))
              (path-list (s-split "/" state-path))
              (value (warp-cli--send-rpc-command leader-addr :state-get
                                                 :args `(:path ,path-list)
                                                 :spinner-message
                                                 (format "Getting state
                                                          for path %s..."
                                                          state-path))))
         (if output-json-p
             (princ (json-encode value) (standard-output))
           (princ (format "State at %s: %S\n" state-path value)))))

      ("set"
       (unless (= (length positional) 3)
         (warp:cli-print-error "state set requires <path> and <value> arguments.")
         (kill-emacs 1))
       (let* ((state-path (cadr positional))
              (path-list (s-split "/" state-path))
              (value-str (caddr positional))
              (value nil))
         (condition-case json-err
             (setq value (json-parse-string value-str))
           (error
            (warp:cli-print-error "Invalid JSON value string: %S (Error: %S)"
                                  value-str json-err)
            (kill-emacs 1)))
         (warp-cli--confirm-action (format "Are you sure you want to set
                                           state at '%s' to %S?"
                                           state-path value) opts)
         (warp-cli--send-rpc-command leader-addr :state-set
                                     :args `(:path ,path-list :value ,value)
                                     :spinner-message
                                     (format "Setting state at path %s..."
                                             state-path)
                                     :expect-response nil)
         (warp:cli-print-success "State at '%s' set successfully." state-path)))

      ("delete"
       (unless (= (length positional) 2)
         (warp:cli-print-error "state delete requires a <path> argument.")
         (kill-emacs 1))
       (let* ((state-path (cadr positional))
              (path-list (s-split "/" state-path)))
         (warp-cli--confirm-action (format "Are you sure you want to delete
                                           state at '%s'?"
                                           state-path) opts)
         (warp-cli--send-rpc-command leader-addr :state-delete
                                     :args `(:path ,path-list)
                                     :spinner-message
                                     (format "Deleting state at path %s..."
                                             state-path)
                                     :expect-response nil)
         (warp:cli-print-success "State at '%s' deleted successfully." state-path)))

      ("keys"
       (let* ((pattern (cadr positional))
              (keys (warp-cli--send-rpc-command leader-addr :state-keys
                                                :args `(:pattern ,pattern)
                                                :spinner-message
                                                (format "Fetching
                                                         state keys
                                                         matching %S..."
                                                         pattern))))
         (if output-json-p
             (princ (json-encode keys) (standard-output))
           (princ (format "--- %s ---\n"
                          (warp:cli-style-bold (format "State Keys (%s)"
                                                       (or pattern "all")))))
           (dolist (key keys)
             (princ (format "%s\n" key) (standard-output))))))

      ("dump"
       (let* ((file-path (cadr positional))
              (state-dump (warp-cli--send-rpc-command leader-addr :state-dump
                                                      :spinner-message
                                                      "Dumping full state...")))
         (if file-path
             (progn
               (with-temp-file file-path
                 (insert (json-encode state-dump)))
               (warp:cli-print-success "Full state dumped to %s" file-path))
           (princ (json-encode state-dump) (standard-output)))))

      ("metrics"
       (unless (null (cdr positional))
         (warp:cli-print-error "state metrics takes no additional arguments.")
         (kill-emacs 1))
       (let* ((metrics (warp-cli--send-rpc-command leader-addr :state-metrics
                                                   :spinner-message
                                                   "Fetching state
                                                    manager metrics...")))
         (if output-json-p
             (princ (json-encode metrics) (standard-output))
           (princ (format "--- %s ---\n" (warp:cli-style-bold
                                          "State Manager Metrics")))
           (princ (warp:cli-format-table
                   '("METRIC" "VALUE")
                   (mapcar (lambda (kv)
                             (list (format "%S" (car kv))
                                   (format "%S" (cdr kv))))
                           (alist-sort #'string<
                                       (hash-table-alist metrics))))))))

      (_
       (warp:cli-print-error "Unknown state operation: '%s'.
                               Choose from 'get', 'set', 'delete', 'keys',
                               'dump', 'metrics'."
                             state-op)
       (kill-emacs 1)))))

;; --- Handler for 'worker-exec' subcommand ---
(defun warp-cli--handle-worker-exec-command (opts positional leader-addr)
  "Handles the 'worker-exec' subcommand logic.
Executes an arbitrary Emacs Lisp form string on a specific worker.

Arguments:
- `opts` (alist): Parsed options.
- `positional` (list): Positional arguments (<worker-id> <lisp-form-string>).
- `leader-addr` (string): The leader's address."
  (unless (= (length positional) 2)
    (warp:cli-print-error "worker-exec requires <worker-id> and
                            <lisp-form-string> positional arguments.")
    (kill-emacs 1))
  (let* ((worker-id (car positional))
         (lisp-form-string (cadr positional)))
    (warp-cli--confirm-action (format "Are you sure you want to execute Lisp
                                         form '%s' on worker '%s'? This is a
                                         sensitive operation."
                                        lisp-form-string worker-id) opts)
    (let ((result (warp-cli--send-rpc-command leader-addr
                                               :execute-lisp-form
                                               :args `(:worker-id ,worker-id
                                                       :form-string ,lisp-form-string)
                                               :spinner-message
                                               (format "Executing Lisp
                                                        on worker %s..."
                                                        worker-id))))
      (warp:cli-print-success "Lisp execution on worker '%s' completed.
                                Result: %S"
                              worker-id result))))

;; --- Handler for 'security' group subcommand ---
(defun warp-cli--handle-security-command (opts positional leader-addr)
  "Handles the 'security' group subcommand logic.
Dispatches to specific security commands (e.g., update-jwt-keys).

Arguments:
- `opts` (alist): Parsed options.
- `positional` (list): Positional arguments (e.g., \"update-jwt-keys\").
- `leader-addr` (string): The leader's address."
  (let* ((security-op (car positional))
         (public-key-file (alist-get 'public-key-file opts))
         (public-key-material nil)))
    (pcase security-op
      ("update-jwt-keys"
       (unless public-key-file
         (warp:cli-print-error "update-jwt-keys requires --public-key-file.")
         (kill-emacs 1))
       (unless (file-exists-p public-key-file)
         (warp:cli-print-error "Public key file not found: %s" public-key-file)
         (kill-emacs 1))
       (setq public-key-material (s-trim (with-temp-buffer
                                           (insert-file-contents public-key-file)
                                           (buffer-string))))

       (warp-cli--confirm-action "Are you sure you want to update JWT trusted
                                 keys on all workers? This is a sensitive
                                 operation." opts)
       (let ((result (warp-cli--send-rpc-command leader-addr
                                                  :update-jwt-keys-cluster
                                                  :args `(:public-key-material
                                                          ,public-key-material)
                                                  :spinner-message
                                                  (format "Updating JWT keys
                                                            on workers...")
                                                  :expect-response nil)))
         (warp:cli-print-success "JWT keys update initiated on cluster.")))
      (_
       (warp:cli-print-error "Unknown security operation: '%s'.
                               Choose from 'update-jwt-keys'."
                             security-op)
       (kill-emacs 1)))))

;; --- Main execution function for the CLI ---
(defun warp-cli-main ()
  "The main execution function for the Warp CLI.
This function parses command-line arguments, dispatches to the
appropriate subcommand handler, and manages top-level error handling
and program exit. This serves as the main entry point for the CLI."
  (let* ((cli-args (cdr command-line-args-left))
         (first-arg (car cli-args)))
    ;; Set verbose mode from command-line arguments (overrides custom).
    (setq warp-cli-verbose-mode (member "--verbose" cli-args))

    ;; --- Configure loom-log appenders for shell output ---
    ;; This ensures that logs generated by Warp (especially from the
    ;; cluster leader if 'start' is used) are visible in the shell.
    (let* ((parsed-opts (alist-get :options (warp:cli-parse-args cli-args)))
           (log-output-type (alist-get 'log-output parsed-opts))
           (log-file-path (alist-get 'log-file-path parsed-opts))
           (default-log-server (loom:log-default-server))
           (current-appenders (loom-log-config-appenders
                               (loom-log-server-config default-log-server))))

      ;; Remove any existing default appenders (e.g., buffer appender).
      (setf (loom-log-config-appenders (loom-log-server-config
                                        default-log-server)) nil)

      (pcase log-output-type
        ("shell"
         ;; Add a shell appender that writes to stderr.
         (unless (seq-some (lambda (appender)
                             (eq (loom-log-appender-type appender) :shell))
                           current-appenders)
           (setf (loom-log-config-appenders
                  (loom-log-server-config default-log-server))
                 (list (loom:log-make-shell-appender :level :debug)))))
        ("file"
         ;; Add a file appender.
         (unless log-file-path
           (warp:cli-print-error "Option --log-file-path is required when
                                  --log-output is 'file'.")
           (kill-emacs 1))
         (unless (seq-some (lambda (appender)
                             (and (eq (loom-log-appender-type appender) :file)
                                  (equal (plist-get
                                          (loom-log-appender-config appender)
                                          :file-path) log-file-path)))
                           current-appenders)
           (setf (loom-log-config-appenders
                  (loom-log-server-config default-log-server))
                 (list (loom:log-make-file-appender :file-path log-file-path
                                                     :level :debug)))))
        ("none"
         ;; Do nothing, no appenders configured.
         nil)
        (_
         ;; Should not happen due to :choices validation, but for safety.
         (warp:cli-print-error "Invalid --log-output type: %s"
                               log-output-type)
         (kill-emacs 1)))

      ;; Set log level for the main log server based on verbose mode.
      (loom:log-set-level (if warp-cli-verbose-mode :debug :info)
                          default-log-server))
    ;; --- End log configuration ---

    ;; Handle global help request (no subcommand or `--help`).
    (when (or (null cli-args) (member "--help" cli-args) (member "-h" cli-args))
      (warp:cli-print-auto-help
       (when (gethash first-arg warp-cli-subcommand-registry)
         first-arg))
      (kill-emacs 0))

    ;; Handle `--version` request.
    (when (member "--version" cli-args)
      (princ (format "Warp CLI Version: %s\n" warp-cli-version)
             (standard-output))
      (kill-emacs 0))

    ;; Handle `--repl` request.
    (when (member "--repl" cli-args)
      (message "Starting interactive Emacs Lisp REPL (ielm)...")
      ;; `ielm` needs a frame, so `--batch` must be avoided.
      ;; This will launch a new Emacs instance or open a new frame.
      (call-interactively 'ielm)
      ;; Do not exit Emacs immediately after REPL.
      (cl-return-from warp-cli-main nil))

    ;; Determine the subcommand. If the first argument is a known subcommand,
    ;; consume it. Otherwise, `warp-cli-subcommand` remains `nil`.
    (setq warp-cli-subcommand (if (member first-arg
                                          (hash-table-keys
                                           warp-cli-subcommand-registry))
                                  (pop cli-args)
                                nil))

    ;; If no valid subcommand was provided, print an error and global help.
    (unless warp-cli-subcommand
      (warp:cli-print-error "No valid subcommand provided.")
      (warp:cli-print-auto-help)
      (kill-emacs 1))

    ;; Main command dispatch block. All commands are wrapped in
    ;; `condition-case` for robust error reporting.
    (condition-case err
        (let* ((parsed-args (warp:cli-parse-args cli-args))
               (opts (alist-get :options parsed-args))
               (positional (alist-get :positional parsed-args))
               (leader-addr (alist-get 'leader opts))
               (cli-config-data nil))

          ;; --- Pre-flight network check ---
          (when (and (not (alist-get 'skip-network-check opts))
                     (member warp-cli-subcommand
                             '( "stop" "status" "metrics" "workers"
                                "list" "ping" "scale" "job-submit"
                                "job-status" "processes" "worker-info"
                                "job-info" "service-info" "cluster-info"
                                "state" "worker-exec" "security" "health")))
            (unless leader-addr
              (warp:cli-print-error "The --leader <address> option is
                                     required for the '%s' command."
                                    warp-cli-subcommand)
              (kill-emacs 1))
            ;; This calls the Bash wrapper's check_leader_reachable function
            ;; via an external shell command. It relies on the wrapper
            ;; executing `kill-emacs 1` if it fails.
            (let ((check-result (shell-command-to-string
                                  (format "%s check_leader_reachable %s"
                                          (file-name-nondirectory
                                           (car command-line-args-original))
                                          (shell-quote-argument leader-addr)))))
              (unless (string-match-p "Leader is reachable" check-result)
                (warp:cli-print-error "%s" (s-trim check-result))
                (kill-emacs 1))))

          ;; Load global config file if specified via --config-file.
          (when-let (config-file-path (alist-get 'config-file opts))
            (let ((format (alist-get 'format opts)))
              (setq cli-config-data
                    (warp-cli--load-config-file config-file-path format))))

          (pcase warp-cli-subcommand
            ("start"
             (warp-cli--handle-start-command opts positional))

            ("stop"
             (warp-cli--confirm-action "Are you sure you want to stop the
                                       cluster leader and all workers?"
                                       opts)
             (warp-cli--send-rpc-command leader-addr :shutdown-cluster
                                         :spinner-message
                                         "Sending shutdown signal..."
                                         :expect-response nil)
             (warp:cli-print-success "Shutdown signal sent to leader at %s."
                                     leader-addr))

            ("status"
             (let* ((metrics (warp-cli--send-rpc-command leader-addr
                                                         :cluster-metrics
                                                         :spinner-message
                                                         "Fetching cluster
                                                           status..."))
                    (leader-addr-info (alist-get 'leader-address metrics)))
               (warp-cli--output-data metrics opts
                                      `("METRIC" "VALUE")
                                      `((,(warp:cli-style-info "Leader Address")
                                         ,(or leader-addr-info leader-addr))
                                        (,(warp:cli-style-info "Active Workers")
                                         ,(format "%s" (plist-get metrics :total-workers)))
                                        (,(warp:cli-style-info "Avg. CPU")
                                         ,(format "%.2f %%" (plist-get metrics :avg-cluster-cpu-utilization)))
                                        (,(warp:cli-style-info "Total Memory")
                                         ,(format "%.2f MB" (plist-get metrics :avg-cluster-memory-utilization)))
                                        (,(warp:cli-style-info "Active Requests")
                                         ,(format "%s" (plist-get metrics :total-active-requests)))))))

            ("metrics"
             (warp-cli--handle-metrics-command opts positional leader-addr))

            ("workers"
             (let* ((workers (warp-cli--send-rpc-command leader-addr
                                                          :list-workers
                                                          :spinner-message
                                                          "Fetching worker list...")))
               (warp-cli--output-data workers opts
                                      '("WORKER ID" "POOL" "STATUS" "UPTIME (min)")
                                      (mapcar (lambda (w)
                                                (list (plist-get w :id)
                                                      (plist-get w :pool)
                                                      (format "%S"
                                                              (plist-get w :health-status))
                                                      (format "%.1f"
                                                              (/ (plist-get w :uptime) 60.0))))
                                              workers))))

            ("list"
             (warp-cli--handle-list-command opts positional leader-addr))

            ("ping"
             (warp-cli--handle-ping-command opts positional leader-addr))

            ("scale"
             (unless (= (length positional) 2)
               (warp:cli-print-error "Scale command requires <pool-name>
                                       and <target-replicas> as positional
                                       arguments.")
               (kill-emacs 1))
             (let* ((pool-name (car positional))
                    (replicas (string-to-number (cadr positional))))
               (when (and (< replicas 1) (not (alist-get 'confirm opts)))
                 (warp-cli--confirm-action
                  (format "Are you sure you want to scale down pool '%s' to
                                  %d replicas? This might terminate workers."
                          pool-name replicas)
                  opts))
               (warp-cli--send-rpc-command leader-addr :scale-pool
                                           :args `(:pool-name ,pool-name :replicas ,replicas)
                                           :spinner-message
                                           (format "Sending scale command
                                                    for '%s' to %d replicas..."
                                                    pool-name replicas))
               (warp:cli-print-success "Scale command sent successfully for
                                         pool '%s' to %d replicas."
                                       pool-name replicas)))

            ("job-submit"
             (warp-cli--handle-job-submit-command opts positional leader-addr))

            ("job-status"
             (unless (= (length positional) 1)
               (warp:cli-print-error "job-status command requires a <job-id>
                                       positional argument.")
               (kill-emacs 1))
             (let* ((job-id (car positional))
                    (job-info (warp-cli--send-rpc-command leader-addr
                                                          :job-status
                                                          :args `(:job-id ,job-id)
                                                          :target-id "job-queue-worker"
                                                          :spinner-message
                                                          (format "Fetching status
                                                                    for job %s..."
                                                                  job-id))))
               (warp-cli--output-data job-info opts
                                      `("FIELD" "VALUE")
                                      (mapcar (lambda (kv)
                                                (list (format "%S" (car kv))
                                                      (format "%S" (cdr kv))))
                                              (alist-sort #'string<
                                                          (hash-table-alist job-info)))))))

            ("processes"
             (warp-cli--handle-processes-command opts positional))

            ("info"
             (warp-cli--handle-info-command opts positional leader-addr))

            ("health"
             (unless (= (length positional) 2)
               (warp:cli-print-error "health details requires a <worker-id>
                                       positional argument.")
               (kill-emacs 1))
             (let* ((health-op (car positional))
                    (worker-id (cadr positional)))
               (pcase health-op
                 ("details"
                  (let ((details (warp-cli--send-rpc-command leader-addr
                                                              :get-worker-health-details
                                                              :args `(:worker-id ,worker-id)
                                                              :spinner-message
                                                              (format "Fetching
                                                                       health
                                                                       details
                                                                       for worker %s..."
                                                                      worker-id))))
                    (warp-cli--output-data details opts
                                           `("CHECK" "STATUS" "MESSAGE")
                                           (cl-loop for check in details
                                                    collect (list (plist-get check :check-name)
                                                                  (format "%S"
                                                                          (plist-get check :status))
                                                                  (or (plist-get check :message)
                                                                      ""))))))
                 (_
                  (warp:cli-print-error "Unknown health operation: '%s'.
                                         Choose from 'details'."
                                        health-op)
                  (kill-emacs 1)))))

            ("state"
             (warp-cli--handle-state-command opts positional leader-addr))

            ("worker-exec"
             (warp-cli--handle-worker-exec-command opts positional leader-addr))

            ("security"
             (warp-cli--handle-security-command opts positional leader-addr))
            (_
             (warp:cli-print-error "Unknown command: '%s'." warp-cli-subcommand)
             (warp:cli-print-auto-help)
             (kill-emacs 1))))
      (error
        (warp:cli-print-error "%s" (error-message-string err))
        (kill-emacs 1)))
    (kill-emacs 0)))

;; Call the main function when the script is executed.
(warp-cli-main)