package postfixcmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// CommandResult holds the exit code and captured output streams of a Postfix CLI command.
type CommandResult struct {
	ExitCode int    `json:"exit_code"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
}

// CombinedOutput returns stdout and stderr concatenated for display in API responses.
func (r *CommandResult) CombinedOutput() string {
	if r == nil {
		return ""
	}
	if r.Stdout == "" {
		return r.Stderr
	}
	if r.Stderr == "" {
		return r.Stdout
	}
	if strings.HasSuffix(r.Stdout, "\n") {
		return r.Stdout + r.Stderr
	}
	return r.Stdout + "\n" + r.Stderr
}

// QueueRecipient represents a recipient entry in `postqueue -j` NDJSON output.
type QueueRecipient struct {
	Address     string `json:"address"`
	OrigAddress string `json:"orig_address,omitempty"`
	DelayReason string `json:"delay_reason,omitempty"`
}

// QueueEntry represents a single message object emitted by `postqueue -j`.
type QueueEntry struct {
	QueueName    string           `json:"queue_name"`
	QueueID      string           `json:"queue_id"`
	ArrivalTime  int64            `json:"arrival_time"`
	MessageSize  int64            `json:"message_size"`
	ForcedExpire bool             `json:"forced_expire"`
	Sender       string           `json:"sender"`
	Recipients   []QueueRecipient `json:"recipients"`
}

// Runner executes Postfix queue inspection and control commands.
type Runner struct {
	commandDir string
	queueDir   string
	helper     *RootHelperClient
}

// NewRunner constructs a Runner using the Postfix command_directory, queue_directory,
// and an optional privileged RootHelperClient for postsuper actions.
func NewRunner(commandDir, queueDir string, helper *RootHelperClient) *Runner {
	return &Runner{
		commandDir: commandDir,
		queueDir:   queueDir,
		helper:     helper,
	}
}

// RootHelperAlive reports whether the attached root helper client is alive.
func (r *Runner) RootHelperAlive() bool {
	return r.helper != nil && r.helper.Alive()
}

// ListQueue runs `<commandDir>/postqueue -j` and parses the NDJSON entries along with
// per-queue counts (`incoming`, `active`, `deferred`, `hold`, `maildrop`, plus any others).
func (r *Runner) ListQueue(ctx context.Context) ([]QueueEntry, map[string]int, error) {
	postqueuePath := filepath.Join(r.commandDir, "postqueue")
	cmd := exec.CommandContext(ctx, postqueuePath, "-j")

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return nil, nil, fmt.Errorf("postqueue -j failed: %w: %s", err, msg)
		}
		return nil, nil, fmt.Errorf("postqueue -j failed: %w", err)
	}

	return ParsePostqueueJSON(&stdout)
}

// ParsePostqueueJSON parses newline-delimited JSON produced by `postqueue -j` and computes
// counts grouped by queue_name.
func ParsePostqueueJSON(r io.Reader) ([]QueueEntry, map[string]int, error) {
	counts := map[string]int{
		"incoming": 0,
		"active":   0,
		"deferred": 0,
		"hold":     0,
		"maildrop": 0,
	}
	entries := make([]QueueEntry, 0)

	reader := bufio.NewReader(r)
	lineNum := 0
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			lineNum++
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) > 0 {
				var entry QueueEntry
				if uerr := json.Unmarshal(trimmed, &entry); uerr != nil {
					return nil, nil, fmt.Errorf("parse postqueue -j line %d: %w", lineNum, uerr)
				}
				entries = append(entries, entry)
				if entry.QueueName != "" {
					counts[entry.QueueName]++
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, nil, fmt.Errorf("read postqueue -j output: %w", err)
		}
	}

	return entries, counts, nil
}

// ReadContent validates queueID, executes `<commandDir>/postcat -hbq <queueID>`,
// and splits the returned message into headers and body at the first blank line.
// Returns ErrNotFound if postcat exits non-zero because the message left the queue.
func (r *Runner) ReadContent(ctx context.Context, queueID string) (headers string, body string, err error) {
	if err := ValidateQueueID(queueID); err != nil {
		return "", "", err
	}

	postcatPath := filepath.Join(r.commandDir, "postcat")
	cmd := exec.CommandContext(ctx, postcatPath, "-hbq", queueID)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return "", "", ctx.Err()
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			msg := strings.TrimSpace(stderr.String())
			if msg != "" {
				return "", "", fmt.Errorf("%w: %s", ErrNotFound, msg)
			}
			return "", "", ErrNotFound
		}
		return "", "", fmt.Errorf("postcat -hbq failed: %w", err)
	}

	headers, body = SplitHeadersAndBody(stdout.String())
	return headers, body, nil
}

// SplitHeadersAndBody splits raw RFC 5322 message content into headers and body
// at the first "\n\n" or "\r\n\r\n" boundary.
func SplitHeadersAndBody(raw string) (headers string, body string) {
	idxLF := strings.Index(raw, "\n\n")
	idxCRLF := strings.Index(raw, "\r\n\r\n")

	switch {
	case idxCRLF >= 0 && (idxLF < 0 || idxCRLF < idxLF):
		return raw[:idxCRLF], raw[idxCRLF+4:]
	case idxLF >= 0:
		return raw[:idxLF], raw[idxLF+2:]
	default:
		return raw, ""
	}
}

// FlushMessage validates queueID and executes `<commandDir>/postqueue -i <queueID>`.
func (r *Runner) FlushMessage(ctx context.Context, queueID string) (*CommandResult, error) {
	if err := ValidateQueueID(queueID); err != nil {
		return nil, err
	}
	return r.runCommand(ctx, filepath.Join(r.commandDir, "postqueue"), "-i", queueID)
}

// FlushQueue executes `<commandDir>/postqueue -f` to flush all deferred mail.
func (r *Runner) FlushQueue(ctx context.Context) (*CommandResult, error) {
	return r.runCommand(ctx, filepath.Join(r.commandDir, "postqueue"), "-f")
}

// NextRetryAt validates queueID and stats the deferred queue file under `<queueDir>/deferred`.
// It checks `<first-char>/<queueID>`, `<queueID>`, and falls back to 1- and 2-level hash
// globs (`deferred/*/<queueID>` and `deferred/*/*/<queueID>`) to support both short hex IDs
// and long base-52 queue IDs (`enable_long_queue_ids = yes`) at any `hash_queue_depth`.
// Returns the file's modification time in UTC, or (nil, nil) if the queue file does not exist.
func (r *Runner) NextRetryAt(queueID string) (*time.Time, error) {
	if err := ValidateQueueID(queueID); err != nil {
		return nil, err
	}

	candidates := []string{
		filepath.Join(r.queueDir, "deferred", queueID[:1], queueID),
		filepath.Join(r.queueDir, "deferred", queueID),
	}
	for _, p := range candidates {
		info, err := os.Stat(p)
		if err == nil {
			t := info.ModTime().UTC()
			return &t, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}

	for _, pattern := range []string{
		filepath.Join(r.queueDir, "deferred", "*", queueID),
		filepath.Join(r.queueDir, "deferred", "*", "*", queueID),
	} {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, err
		}
		for _, m := range matches {
			info, err := os.Stat(m)
			if err == nil {
				t := info.ModTime().UTC()
				return &t, nil
			}
		}
	}

	return nil, nil
}

// PostsuperAction validates action and queueIDs (1..1000) and dispatches the request
// to the privileged RootHelperClient over its private pipe.
func (r *Runner) PostsuperAction(ctx context.Context, action string, queueIDs []string) (*CommandResult, error) {
	if _, err := ActionFlag(action); err != nil {
		return nil, err
	}
	if err := ValidateBulkQueueIDs(queueIDs); err != nil {
		return nil, err
	}
	if r.helper == nil {
		return nil, ErrRootHelperUnavailable
	}
	return r.helper.Do(ctx, action, queueIDs)
}

func (r *Runner) runCommand(ctx context.Context, bin string, args ...string) (*CommandResult, error) {
	cmd := exec.CommandContext(ctx, bin, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	res := &CommandResult{
		Stdout: stdout.String(),
		Stderr: stderr.String(),
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			res.ExitCode = exitErr.ExitCode()
			return res, nil
		}
		return nil, fmt.Errorf("execute %s: %w", bin, err)
	}
	return res, nil
}
