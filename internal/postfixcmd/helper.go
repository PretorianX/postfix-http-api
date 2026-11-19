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
	"sync"
	"sync/atomic"
	"time"
)

const (
	helperMaxLineBytes = 1 << 20 // 1 MiB
	helperCmdTimeout   = 30 * time.Second
	clientCallTimeout  = 35 * time.Second
	clientCloseTimeout = 5 * time.Second
)

// HelperRequest is the newline-delimited JSON request sent over the pipe to _root-helper.
type HelperRequest struct {
	Action   string   `json:"action"`
	QueueIDs []string `json:"queue_ids"`
}

// HelperResponse is the newline-delimited JSON response returned by _root-helper.
type HelperResponse struct {
	ExitCode int    `json:"exit_code"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	Error    string `json:"error,omitempty"`
}

// RootHelperClient manages the privileged _root-helper child process over private pipes.
type RootHelperClient struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	reader *bufio.Reader

	mu        sync.Mutex
	alive     atomic.Bool
	done      chan struct{}
	closeOnce sync.Once
}

// StartRootHelper spawns `<binaryPath> _root-helper --command-dir <commandDir>` with private
// stdin/stdout pipes. It must be called while the parent still holds UID 0, after marking
// inherited master FDs O_CLOEXEC and before dropping privileges to mail_owner.
func StartRootHelper(binaryPath, commandDir string) (*RootHelperClient, error) {
	if strings.TrimSpace(binaryPath) == "" {
		return nil, errors.New("binary path must not be empty")
	}
	if strings.TrimSpace(commandDir) == "" {
		return nil, errors.New("command directory must not be empty")
	}

	cmd := exec.Command(binaryPath, "_root-helper", "--command-dir", commandDir)
	cmd.Stderr = os.Stderr
	setPdeathsig(cmd)

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("create root helper stdin pipe: %w", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdinPipe.Close()
		return nil, fmt.Errorf("create root helper stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		_ = stdinPipe.Close()
		_ = stdoutPipe.Close()
		return nil, fmt.Errorf("start root helper: %w", err)
	}

	client := &RootHelperClient{
		cmd:    cmd,
		stdin:  stdinPipe,
		reader: bufio.NewReaderSize(stdoutPipe, 64*1024),
		done:   make(chan struct{}),
	}
	client.alive.Store(true)

	go func() {
		_ = cmd.Wait()
		client.alive.Store(false)
		close(client.done)
	}()

	return client, nil
}

// Alive reports whether the _root-helper child process and pipe are still healthy.
func (c *RootHelperClient) Alive() bool {
	if c == nil {
		return false
	}
	return c.alive.Load()
}

// Do sends a validated postsuper request to _root-helper and waits for its response.
func (c *RootHelperClient) Do(ctx context.Context, action string, queueIDs []string) (*CommandResult, error) {
	if c == nil {
		return nil, ErrRootHelperUnavailable
	}
	if _, err := ActionFlag(action); err != nil {
		return nil, err
	}
	if err := ValidateBulkQueueIDs(queueIDs); err != nil {
		return nil, err
	}

	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, clientCallTimeout)
		defer cancel()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.alive.Load() {
		return nil, ErrRootHelperUnavailable
	}

	payload, err := json.Marshal(HelperRequest{
		Action:   action,
		QueueIDs: queueIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal helper request: %w", err)
	}
	payload = append(payload, '\n')

	if _, err := c.stdin.Write(payload); err != nil {
		c.alive.Store(false)
		return nil, fmt.Errorf("%w: write request: %v", ErrRootHelperUnavailable, err)
	}

	type readResult struct {
		line []byte
		err  error
	}
	ch := make(chan readResult, 1)
	go func() {
		line, rerr := c.reader.ReadBytes('\n')
		ch <- readResult{line: line, err: rerr}
	}()

	var res readResult
	select {
	case res = <-ch:
	case <-ctx.Done():
		// Terminate helper on timeout/cancellation so the pipe cannot desynchronize.
		c.alive.Store(false)
		_ = c.stdin.Close()
		if c.cmd != nil && c.cmd.Process != nil {
			_ = c.cmd.Process.Kill()
		}
		return nil, fmt.Errorf("root helper request canceled: %w", ctx.Err())
	}

	if res.err != nil {
		c.alive.Store(false)
		return nil, fmt.Errorf("%w: read response: %v", ErrRootHelperUnavailable, res.err)
	}

	var resp HelperResponse
	if err := json.Unmarshal(bytes.TrimSpace(res.line), &resp); err != nil {
		c.alive.Store(false)
		return nil, fmt.Errorf("decode helper response: %w", err)
	}

	cmdRes := &CommandResult{
		ExitCode: resp.ExitCode,
		Stdout:   resp.Stdout,
		Stderr:   resp.Stderr,
	}
	if resp.Error != "" {
		return cmdRes, errors.New(resp.Error)
	}
	return cmdRes, nil
}

// Close closes the stdin pipe and waits up to 5 seconds for _root-helper to exit,
// sending SIGKILL if it does not terminate in time.
func (c *RootHelperClient) Close() error {
	if c == nil {
		return nil
	}
	var closeErr error
	c.closeOnce.Do(func() {
		c.alive.Store(false)
		closeErr = c.stdin.Close()

		timer := time.NewTimer(clientCloseTimeout)
		defer timer.Stop()
		select {
		case <-c.done:
		case <-timer.C:
			if c.cmd != nil && c.cmd.Process != nil {
				_ = c.cmd.Process.Kill()
			}
			<-c.done
		}
	})
	return closeErr
}

// RunRootHelper runs the privileged _root-helper pipe loop.
// It reads newline-delimited JSON HelperRequests from in, re-validates every action and
// queue ID (defense in depth), executes `<commandDir>/postsuper`, and writes newline-delimited
// JSON HelperResponses to out until EOF.
func RunRootHelper(commandDir string, in io.Reader, out io.Writer) error {
	if strings.TrimSpace(commandDir) == "" {
		return errors.New("command directory must not be empty")
	}
	info, err := os.Stat(commandDir)
	if err != nil {
		return fmt.Errorf("stat command directory %q: %w", commandDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("command directory %q is not a directory", commandDir)
	}

	postsuperPath := filepath.Join(commandDir, "postsuper")
	psInfo, err := os.Stat(postsuperPath)
	if err != nil {
		return fmt.Errorf("stat postsuper at %q: %w", postsuperPath, err)
	}
	if psInfo.IsDir() {
		return fmt.Errorf("postsuper path %q is a directory", postsuperPath)
	}

	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 0, 64*1024), helperMaxLineBytes)

	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		resp := handleHelperLine(postsuperPath, line)
		encoded, err := json.Marshal(resp)
		if err != nil {
			return fmt.Errorf("marshal helper response: %w", err)
		}
		encoded = append(encoded, '\n')
		if _, err := out.Write(encoded); err != nil {
			return fmt.Errorf("write helper response: %w", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read helper request: %w", err)
	}
	return nil
}

func handleHelperLine(postsuperPath string, line []byte) HelperResponse {
	var req HelperRequest
	if err := json.Unmarshal(line, &req); err != nil {
		return HelperResponse{
			ExitCode: -1,
			Error:    fmt.Sprintf("invalid request JSON: %v", err),
		}
	}

	flag, err := ActionFlag(req.Action)
	if err != nil {
		return HelperResponse{
			ExitCode: -1,
			Error:    err.Error(),
		}
	}
	if err := ValidateBulkQueueIDs(req.QueueIDs); err != nil {
		return HelperResponse{
			ExitCode: -1,
			Error:    err.Error(),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), helperCmdTimeout)
	defer cancel()

	var cmd *exec.Cmd
	if len(req.QueueIDs) == 1 {
		cmd = exec.CommandContext(ctx, postsuperPath, flag, req.QueueIDs[0])
	} else {
		cmd = exec.CommandContext(ctx, postsuperPath, flag, "-")
		cmd.Stdin = strings.NewReader(strings.Join(req.QueueIDs, "\n") + "\n")
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	runErr := cmd.Run()
	resp := HelperResponse{
		Stdout: stdout.String(),
		Stderr: stderr.String(),
	}

	if runErr != nil {
		var exitErr *exec.ExitError
		if errors.As(runErr, &exitErr) {
			resp.ExitCode = exitErr.ExitCode()
		} else {
			resp.ExitCode = -1
			resp.Error = runErr.Error()
		}
	}
	return resp
}
