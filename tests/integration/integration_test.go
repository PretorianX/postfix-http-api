//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PretorianX/postfix-http-api/tests/integration/seqmark"
)

const (
	defaultListenAddr = "0.0.0.0:8025"
	adminToken        = "pfapi_integration_admin_token_001"
	readToken         = "pfapi_integration_read_token_002"
	writeToken        = "pfapi_integration_write_token_003"
	contentToken      = "pfapi_integration_content_token_004"
)

var (
	queuedAsRe = regexp.MustCompile(`queued as ([A-Za-z0-9]{6,32})`)
	stopSeqRe  = regexp.MustCompile(`STOP-BLOCK-(\d{5})`)
)

type testEnv struct {
	target     string
	image      string
	runID      string
	helperPath string
}

var helperBuildOnce struct {
	sync.Mutex
	path string
	err  error
}

func loadTestEnv(t *testing.T) testEnv {
	t.Helper()
	target := strings.TrimSpace(os.Getenv("POSTFIX_TEST_TARGET"))
	if target == "" {
		target = "debian12"
	}
	image := strings.TrimSpace(os.Getenv("POSTFIX_TEST_IMAGE"))
	if image == "" {
		image = "postfix-http-api-test:" + target
	}
	runID := strings.TrimSpace(os.Getenv("POSTFIX_TEST_RUN_ID"))
	helperPath := strings.TrimSpace(os.Getenv("POSTFIX_TEST_HELPER"))
	if helperPath == "" {
		var err error
		helperPath, err = ensureLinuxHelper(t, image)
		if err != nil {
			t.Fatalf("build testhelper: %v", err)
		}
	}
	return testEnv{
		target:     target,
		image:      image,
		runID:      runID,
		helperPath: helperPath,
	}
}

func ensureLinuxHelper(t *testing.T, image string) (string, error) {
	t.Helper()
	helperBuildOnce.Lock()
	defer helperBuildOnce.Unlock()

	if helperBuildOnce.path != "" || helperBuildOnce.err != nil {
		return helperBuildOnce.path, helperBuildOnce.err
	}

	arch := "amd64"
	if out, err := exec.Command("docker", "image", "inspect", "--format", "{{.Architecture}}", image).Output(); err == nil {
		a := strings.TrimSpace(string(out))
		if a == "amd64" || a == "arm64" {
			arch = a
		}
	}

	tmpDir, err := os.MkdirTemp("", "postfix-api-testhelper-*")
	if err != nil {
		helperBuildOnce.err = err
		return "", err
	}
	outPath := filepath.Join(tmpDir, "testhelper")

	cmd := exec.Command("go", "build", "-o", outPath, "./cmd/testhelper")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+arch)
	if out, err := cmd.CombinedOutput(); err != nil {
		helperBuildOnce.err = fmt.Errorf("go build testhelper: %w: %s", err, string(out))
		return "", helperBuildOnce.err
	}
	helperBuildOnce.path = outPath
	return outPath, nil
}

type tokenSpec struct {
	Name   string
	Raw    string
	Scopes []string
}

type confOptions struct {
	StateDir         string
	SegmentSizeBytes int64
	MaxTotalBytes    int64
	Retention        string
	QueueSync        string
	IndexerBatchSize int
	SubjectInOutput  bool
	AllowPlaintext   bool
	Tokens           []tokenSpec
}

func defaultConfOptions() confOptions {
	return confOptions{
		SegmentSizeBytes: 16 * 1024 * 1024,
		MaxTotalBytes:    256 * 1024 * 1024,
		Retention:        "168h",
		QueueSync:        "5m",
		IndexerBatchSize: 1000,
		SubjectInOutput:  false,
		AllowPlaintext:   true,
		Tokens: []tokenSpec{
			{Name: "admin", Raw: adminToken, Scopes: []string{"read", "write", "content"}},
		},
	}
}

func sha256Hex(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func renderConfYAML(o confOptions) string {
	var b strings.Builder
	fmt.Fprintf(&b, "state_dir: %q\n", o.StateDir)
	b.WriteString("spool:\n")
	fmt.Fprintf(&b, "  segment_size_bytes: %d\n", o.SegmentSizeBytes)
	fmt.Fprintf(&b, "  max_total_bytes: %d\n", o.MaxTotalBytes)
	fmt.Fprintf(&b, "retention_duration: %s\n", o.Retention)
	fmt.Fprintf(&b, "queue_sync_interval: %s\n", o.QueueSync)
	fmt.Fprintf(&b, "indexer_batch_size: %d\n", o.IndexerBatchSize)
	fmt.Fprintf(&b, "subject_in_output: %t\n", o.SubjectInOutput)
	b.WriteString("tls:\n")
	b.WriteString("  cert_file: \"\"\n")
	b.WriteString("  key_file: \"\"\n")
	fmt.Fprintf(&b, "allow_plaintext: %t\n", o.AllowPlaintext)
	if len(o.Tokens) == 0 {
		b.WriteString("tokens: []\n")
	} else {
		b.WriteString("tokens:\n")
		for _, tok := range o.Tokens {
			fmt.Fprintf(&b, "  - name: %q\n", tok.Name)
			fmt.Fprintf(&b, "    sha256: %q\n", sha256Hex(tok.Raw))
			b.WriteString("    scopes:\n")
			for _, sc := range tok.Scopes {
				fmt.Fprintf(&b, "      - %s\n", sc)
			}
		}
	}
	return b.String()
}

type containerOpts struct {
	NameSuffix       string
	ListenAddr       string
	Conf             confOptions
	Relayhost        string
	UseTransportMaps bool
	StartSMTPSink    bool
	SkipInstall      bool
	KeepEntrypoint   bool
	Env              map[string]string
	Mounts           []string
	ExposePorts      []string
	ShellCommand     string
}

type testContainer struct {
	t           *testing.T
	id          string
	env         testEnv
	hasPython3  bool
	helperReady bool
}

func startContainer(t *testing.T, env testEnv, opts containerOpts) *testContainer {
	t.Helper()

	if opts.ListenAddr == "" {
		opts.ListenAddr = defaultListenAddr
	}
	if len(opts.ExposePorts) == 0 {
		opts.ExposePorts = []string{"25", "8025"}
	}

	args := []string{"run", "-d"}
	if env.runID != "" {
		args = append(args, "--label", "postfix-api-test-run="+env.runID)
	}
	args = append(args, "--label", "postfix-api-test="+t.Name())
	for _, p := range opts.ExposePorts {
		args = append(args, "-p", "127.0.0.1::"+p)
	}
	for k, v := range opts.Env {
		args = append(args, "-e", k+"="+v)
	}
	for _, m := range opts.Mounts {
		args = append(args, "-v", m)
	}

	if !opts.KeepEntrypoint {
		args = append(args, "--entrypoint", "/bin/sh")
	}
	args = append(args, env.image)

	if !opts.KeepEntrypoint {
		script := opts.ShellCommand
		if script == "" {
			script = buildStartScript(opts)
		}
		args = append(args, "-c", script)
	}

	cmd := exec.Command("docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker run failed: %v\nargs: %v\noutput: %s", err, args, string(out))
	}
	cid := strings.TrimSpace(string(out))
	if cid == "" {
		t.Fatalf("docker run returned empty container id")
	}

	tc := &testContainer{t: t, id: cid, env: env}
	t.Cleanup(func() {
		if t.Failed() {
			logs, _ := tc.logs()
			t.Logf("=== Container %s (%s) logs ===\n%s\n=== End logs ===", tc.id[:12], t.Name(), logs)
		}
		_ = exec.Command("docker", "rm", "-f", tc.id).Run()
	})

	// Check python3 presence and copy static testhelper into /usr/local/bin/testhelper.
	_, _, pyErr := tc.exec("sh", "-c", "command -v python3 >/dev/null 2>&1")
	tc.hasPython3 = pyErr == nil

	if err := tc.copyFile(env.helperPath, "/usr/local/bin/testhelper"); err != nil {
		t.Fatalf("copy testhelper into container: %v", err)
	}
	if _, stderr, err := tc.exec("chmod", "0755", "/usr/local/bin/testhelper"); err != nil {
		t.Fatalf("chmod testhelper: %v (%s)", err, stderr)
	}
	tc.helperReady = true

	if opts.StartSMTPSink {
		if err := tc.execDetached("/usr/local/bin/testhelper", "smtp-sink", "--listen", "127.0.0.1:2525"); err != nil {
			t.Fatalf("start smtp-sink: %v", err)
		}
		tc.waitForContainerTCP("127.0.0.1", "2525", 10*time.Second)
	}

	return tc
}

func buildStartScript(opts containerOpts) string {
	var b strings.Builder
	b.WriteString("set -eu\n")
	b.WriteString(`postconf -e "maillog_file = /dev/stdout"` + "\n")
	b.WriteString(`postconf -e "inet_interfaces = all"` + "\n")
	b.WriteString(`postconf -e "mynetworks = 127.0.0.0/8 [::1]/128 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16"` + "\n")
	b.WriteString(`postconf -e "mydestination = localhost"` + "\n")
	b.WriteString(`postconf -e "smtputf8_enable = yes"` + "\n")
	b.WriteString(`postconf -e "smtp_connect_timeout = 2s"` + "\n")
	b.WriteString(`if [ -z "$(postconf -h myhostname 2>/dev/null || true)" ] || [ "$(postconf -h myhostname 2>/dev/null)" = "localhost" ]; then postconf -e "myhostname = test-postfix.localdomain"; fi` + "\n")
	b.WriteString(`if [ -f /etc/postfix/aliases ] && [ ! -f /etc/postfix/aliases.lmdb ] && [ ! -f /etc/postfix/aliases.db ]; then postalias /etc/postfix/aliases || true; fi` + "\n")

	if opts.UseTransportMaps {
		b.WriteString("cat <<'EOF' > /etc/postfix/transport\n")
		b.WriteString("/@blackhole\\.test$/   smtp:[192.0.2.1]:25\n")
		b.WriteString("/@sink\\.test$/        smtp:[127.0.0.1]:2525\n")
		b.WriteString("EOF\n")
		b.WriteString(`postconf -e "transport_maps = regexp:/etc/postfix/transport"` + "\n")
	}
	if opts.Relayhost != "" {
		fmt.Fprintf(&b, "postconf -e \"relayhost = %s\"\n", opts.Relayhost)
	}

	if !opts.SkipInstall {
		fmt.Fprintf(&b, "/usr/local/bin/postfix-api install --listen %q\n", opts.ListenAddr)
		confYAML := renderConfYAML(opts.Conf)
		b.WriteString("umask 077\n")
		b.WriteString("cat <<'EOF_CONF' > /etc/postfix/postfix-api.conf\n")
		b.WriteString(confYAML)
		b.WriteString("EOF_CONF\n")
		b.WriteString("chown root:root /etc/postfix/postfix-api.conf\n")
		b.WriteString("chmod 0600 /etc/postfix/postfix-api.conf\n")
	}

	b.WriteString("postfix check\n")
	b.WriteString("exec postfix start-fg\n")
	return b.String()
}

func (c *testContainer) exec(args ...string) (string, string, error) {
	cmdArgs := append([]string{"exec", c.id}, args...)
	cmd := exec.Command("docker", cmdArgs...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

func (c *testContainer) execDetached(args ...string) error {
	cmdArgs := append([]string{"exec", "-d", c.id}, args...)
	out, err := exec.Command("docker", cmdArgs...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, string(out))
	}
	return nil
}

func (c *testContainer) mustExec(args ...string) string {
	c.t.Helper()
	stdout, stderr, err := c.exec(args...)
	if err != nil {
		c.t.Fatalf("docker exec %v failed: %v\nstdout: %s\nstderr: %s", args, err, stdout, stderr)
	}
	return stdout
}

func (c *testContainer) copyFile(hostSrc, containerDst string) error {
	out, err := exec.Command("docker", "cp", hostSrc, c.id+":"+containerDst).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker cp %s -> %s: %w: %s", hostSrc, containerDst, err, string(out))
	}
	return nil
}

func (c *testContainer) writeFile(containerPath, content string, mode os.FileMode) {
	c.t.Helper()
	tmp := filepath.Join(c.t.TempDir(), "upload")
	if err := os.WriteFile(tmp, []byte(content), mode); err != nil {
		c.t.Fatalf("write temp file: %v", err)
	}
	if err := c.copyFile(tmp, containerPath); err != nil {
		c.t.Fatalf("copyFile %s: %v", containerPath, err)
	}
	c.mustExec("chown", "root:root", containerPath)
	c.mustExec("chmod", fmt.Sprintf("%04o", mode.Perm()), containerPath)
}

func (c *testContainer) readFile(containerPath string) (string, error) {
	out, stderr, err := c.exec("cat", containerPath)
	if err != nil {
		return "", fmt.Errorf("cat %s: %w (%s)", containerPath, err, strings.TrimSpace(stderr))
	}
	return out, nil
}

func (c *testContainer) logs() (string, error) {
	out, err := exec.Command("docker", "logs", c.id).CombinedOutput()
	return string(out), err
}

func (c *testContainer) hostPort(containerPort string) string {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		out, err := exec.Command("docker", "port", c.id, containerPort+"/tcp").CombinedOutput()
		if err == nil {
			line := strings.TrimSpace(strings.Split(string(out), "\n")[0])
			if line != "" {
				return line
			}
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("failed to resolve docker port %s/tcp: %v (%s)", containerPort, err, string(out))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (c *testContainer) waitForContainerTCP(host, port string, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, _, err := c.exec("sh", "-c", fmt.Sprintf("nc -z %s %s >/dev/null 2>&1", host, port))
		if err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("timed out waiting for container TCP %s:%s", host, port)
}

func (c *testContainer) postconfGet(param string) string {
	c.t.Helper()
	return strings.TrimSpace(c.mustExec("postconf", "-h", param))
}

func (c *testContainer) postconfMaster(service string) string {
	c.t.Helper()
	return strings.TrimSpace(c.mustExec("postconf", "-M", service))
}

type procInfo struct {
	PID     int
	CmdLine string
	Role    string // "receiver", "api", "root-helper", "other"
}

func (c *testContainer) findPostfixAPIProcs() ([]procInfo, error) {
	script := `for p in /proc/[0-9]*; do
		pid=${p#/proc/}
		[ -r "$p/cmdline" ] || continue
		cmd=$(tr '\0' ' ' < "$p/cmdline" 2>/dev/null || true)
		case "$cmd" in
			*postfix-api*) printf '%s\t%s\n' "$pid" "$cmd" ;;
		esac
	done`
	out, stderr, err := c.exec("sh", "-c", script)
	if err != nil {
		return nil, fmt.Errorf("scan /proc: %w (%s)", err, stderr)
	}
	var procs []procInfo
	scanner := bufio.NewScanner(strings.NewReader(out))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			continue
		}
		cmd := strings.TrimSpace(parts[1])
		if strings.HasPrefix(cmd, "sh ") || strings.HasPrefix(cmd, "/bin/sh ") {
			continue
		}
		role := "other"
		switch {
		case strings.Contains(cmd, "_root-helper"):
			role = "root-helper"
		case strings.Contains(cmd, "unix-dgram"):
			role = "receiver"
		case strings.Contains(cmd, "inet"):
			role = "api"
		}
		procs = append(procs, procInfo{PID: pid, CmdLine: cmd, Role: role})
	}
	return procs, nil
}

func (c *testContainer) findProcByRole(role string) (procInfo, bool) {
	procs, err := c.findPostfixAPIProcs()
	if err != nil {
		return procInfo{}, false
	}
	for _, p := range procs {
		if p.Role == role {
			return p, true
		}
	}
	return procInfo{}, false
}

func (c *testContainer) waitProcByRole(role string, excludePID int, timeout time.Duration) procInfo {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if p, ok := c.findProcByRole(role); ok && p.PID != excludePID {
			return p
		}
		time.Sleep(50 * time.Millisecond)
	}
	procs, _ := c.findPostfixAPIProcs()
	c.t.Fatalf("timed out waiting for role %q (exclude=%d); current procs: %+v", role, excludePID, procs)
	return procInfo{}
}

func (c *testContainer) waitPIDGone(pid int, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, _, err := c.exec("test", "-d", fmt.Sprintf("/proc/%d", pid))
		if err != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("timed out waiting for PID %d to exit", pid)
}

func (c *testContainer) procGeneration(pid int) string {
	c.t.Helper()
	out := c.mustExec("sh", "-c", fmt.Sprintf("tr '\\0' '\\n' < /proc/%d/environ | grep -a '^GENERATION=' || true", pid))
	return strings.TrimPrefix(strings.TrimSpace(out), "GENERATION=")
}

func (c *testContainer) procRSSKB(pid int) int64 {
	c.t.Helper()
	out := c.mustExec("sh", "-c", fmt.Sprintf("awk '/^VmRSS:/ {print $2}' /proc/%d/status", pid))
	val, err := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	if err != nil {
		c.t.Fatalf("parse VmRSS for pid %d (%q): %v", pid, out, err)
	}
	return val
}

func (c *testContainer) readCounter(path string) uint64 {
	c.t.Helper()
	raw, err := c.readFile(path)
	if err != nil {
		c.t.Fatalf("read counter %s: %v", path, err)
	}
	v, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		c.t.Fatalf("parse counter %s (%q): %v", path, raw, err)
	}
	return v
}

func (c *testContainer) waitPostfixReady(timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, _, err := c.exec("postfix", "status")
		if err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	c.t.Fatalf("timed out waiting for postfix status == 0")
}

// --- HTTP API Client ---

type apiClient struct {
	baseURL    string
	httpClient *http.Client
}

func newAPIClient(addr string) *apiClient {
	return &apiClient{
		baseURL: "http://" + addr,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

type apiErrorEnvelope struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type apiHealthResp struct {
	Status           string     `json:"status"`
	PostfixVersion   string     `json:"postfix_version"`
	IndexerDelayMs   int64      `json:"indexer_delay_ms"`
	SpoolGaps        uint64     `json:"spool_gaps"`
	LostSpoolRecords uint64     `json:"lost_spool_records"`
	SpoolBytes       int64      `json:"spool_bytes"`
	SpoolMaxBytes    int64      `json:"spool_max_bytes"`
	LastQueueSyncAt  *time.Time `json:"last_queue_sync_at"`
	LastQueueSyncOK  bool       `json:"last_queue_sync_ok"`
	RootHelperAlive  bool       `json:"root_helper_alive"`
}

type apiClientInfo struct {
	Name    string `json:"name"`
	Address string `json:"address"`
}

type apiRecipient struct {
	Address       string     `json:"address"`
	OrigTo        string     `json:"orig_to"`
	Status        string     `json:"status"`
	Attempts      int        `json:"attempts"`
	LastAttemptAt *time.Time `json:"last_attempt_at"`
	Relay         string     `json:"relay"`
	DSN           string     `json:"dsn"`
	Response      string     `json:"response"`
	Delay         float64    `json:"delay"`
}

type apiEvent struct {
	ID        int64     `json:"id"`
	Time      time.Time `json:"time"`
	QueueID   string    `json:"queue_id"`
	Recipient *string   `json:"recipient"`
	Event     string    `json:"event"`
	Relay     *string   `json:"relay"`
	DSN       *string   `json:"dsn"`
	Response  *string   `json:"response"`
	Delay     *float64  `json:"delay"`
	Attempt   *int      `json:"attempt"`
}

type apiMessage struct {
	QueueID      string         `json:"queue_id"`
	MessageID    string         `json:"message_id"`
	Subject      string         `json:"subject"`
	Sender       string         `json:"sender"`
	Size         int64          `json:"size"`
	NRcpt        int            `json:"nrcpt"`
	Client       apiClientInfo  `json:"client"`
	SASLUsername *string        `json:"sasl_username"`
	ReceivedAt   time.Time      `json:"received_at"`
	QueueState   string         `json:"queue_state"`
	DelayReason  string         `json:"delay_reason"`
	RemovedAt    *time.Time     `json:"removed_at"`
	OrigQueueID  *string        `json:"orig_queue_id"`
	NextRetryAt  *time.Time     `json:"next_retry_at"`
	Recipients   []apiRecipient `json:"recipients"`
	Events       []apiEvent     `json:"events"`
}

type apiMessagesResp struct {
	Messages   []apiMessage `json:"messages"`
	NextCursor *string      `json:"next_cursor"`
}

type apiEventsResp struct {
	Events  []apiEvent `json:"events"`
	LastID  int64      `json:"last_id"`
	HasMore bool       `json:"has_more"`
}

type apiQueueResp struct {
	Total   int            `json:"total"`
	Counts  map[string]int `json:"counts"`
	Entries []struct {
		QueueName   string `json:"queue_name"`
		QueueID     string `json:"queue_id"`
		Sender      string `json:"sender"`
		DelayReason string `json:"delay_reason"`
	} `json:"entries"`
}

type apiContentResp struct {
	QueueID      string `json:"queue_id"`
	Headers      string `json:"headers"`
	Body         string `json:"body"`
	BodyEncoding string `json:"body_encoding"`
}

type apiActionResp struct {
	Action   string   `json:"action"`
	QueueIDs []string `json:"queue_ids"`
	ExitCode int      `json:"exit_code"`
	Output   string   `json:"output"`
}

func (a *apiClient) doRaw(method, path, token string, body any) (int, []byte, error) {
	var bodyReader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		bodyReader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, a.baseURL+path, bodyReader)
	if err != nil {
		return 0, nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return resp.StatusCode, data, err
}

func (a *apiClient) doJSON(t *testing.T, method, path, token string, body, out any) int {
	t.Helper()
	status, raw, err := a.doRaw(method, path, token, body)
	if err != nil {
		t.Fatalf("%s %s failed: %v", method, path, err)
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("%s %s decode (%d): %v\nbody: %s", method, path, status, err, string(raw))
		}
	}
	return status
}

func (a *apiClient) waitForHealth(t *testing.T, token, wantStatus string, timeout time.Duration) apiHealthResp {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr string
	for time.Now().Before(deadline) {
		code, raw, err := a.doRaw(http.MethodGet, "/v1/health", token, nil)
		if err != nil {
			lastErr = err.Error()
		} else if code == http.StatusOK {
			var h apiHealthResp
			if uerr := json.Unmarshal(raw, &h); uerr == nil {
				if wantStatus == "" || h.Status == wantStatus {
					return h
				}
				lastErr = fmt.Sprintf("status=%s (want %s), body=%s", h.Status, wantStatus, string(raw))
			} else {
				lastErr = fmt.Sprintf("decode err: %v, body=%s", uerr, string(raw))
			}
		} else {
			lastErr = fmt.Sprintf("http=%d body=%s", code, string(raw))
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for /v1/health (%s): last=%s", wantStatus, lastErr)
	return apiHealthResp{}
}

func (a *apiClient) waitForMessage(t *testing.T, qid, token string, timeout time.Duration, pred func(apiMessage) bool) apiMessage {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last apiMessage
	var lastStatus int
	var lastBody string
	for time.Now().Before(deadline) {
		code, raw, err := a.doRaw(http.MethodGet, "/v1/messages/"+qid, token, nil)
		if err == nil {
			lastStatus = code
			lastBody = string(raw)
			if code == http.StatusOK {
				var msg apiMessage
				if uerr := json.Unmarshal(raw, &msg); uerr == nil {
					last = msg
					if pred == nil || pred(msg) {
						return msg
					}
				}
			}
		} else {
			lastBody = err.Error()
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for message %s: lastStatus=%d last=%+v raw=%s", qid, lastStatus, last, lastBody)
	return apiMessage{}
}

// --- SMTP Helper ---

func sendSMTP(t *testing.T, smtpAddr, from, to, subjectHeader, messageID, body string) string {
	t.Helper()
	conn, err := net.DialTimeout("tcp4", smtpAddr, 10*time.Second)
	if err != nil {
		t.Fatalf("dial SMTP %s: %v", smtpAddr, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))

	tp := textproto.NewConn(conn)
	defer tp.Close()

	if _, _, err := tp.ReadResponse(220); err != nil {
		t.Fatalf("SMTP banner: %v", err)
	}

	cmd := func(expect int, format string, args ...any) string {
		if err := tp.PrintfLine(format, args...); err != nil {
			t.Fatalf("SMTP write %q: %v", fmt.Sprintf(format, args...), err)
		}
		_, msg, err := tp.ReadResponse(expect)
		if err != nil {
			t.Fatalf("SMTP command %q expect %d: %v", fmt.Sprintf(format, args...), expect, err)
		}
		return msg
	}

	cmd(250, "EHLO integration.test.local")
	cmd(250, "MAIL FROM:<%s>", from)
	cmd(250, "RCPT TO:<%s>", to)
	cmd(354, "DATA")

	var raw bytes.Buffer
	fmt.Fprintf(&raw, "From: <%s>\r\n", from)
	fmt.Fprintf(&raw, "To: <%s>\r\n", to)
	fmt.Fprintf(&raw, "Subject: %s\r\n", subjectHeader)
	if messageID != "" {
		fmt.Fprintf(&raw, "Message-ID: %s\r\n", messageID)
	}
	fmt.Fprintf(&raw, "Date: %s\r\n", time.Now().UTC().Format(time.RFC1123Z))
	raw.WriteString("MIME-Version: 1.0\r\n")
	raw.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
	raw.WriteString("\r\n")
	// Dot-stuff body lines if needed and ensure CRLF endings.
	normBody := strings.ReplaceAll(strings.ReplaceAll(body, "\r\n", "\n"), "\n", "\r\n")
	for _, line := range strings.Split(normBody, "\r\n") {
		if strings.HasPrefix(line, ".") {
			raw.WriteByte('.')
		}
		raw.WriteString(line)
		raw.WriteString("\r\n")
	}
	raw.WriteString(".\r\n")

	if _, err := tp.W.Write(raw.Bytes()); err != nil {
		t.Fatalf("SMTP write DATA: %v", err)
	}
	if err := tp.W.Flush(); err != nil {
		t.Fatalf("SMTP flush DATA: %v", err)
	}
	_, queuedMsg, err := tp.ReadResponse(250)
	if err != nil {
		t.Fatalf("SMTP DATA 250 response: %v", err)
	}

	_ = tp.PrintfLine("QUIT")
	_, _, _ = tp.ReadResponse(221)

	m := queuedAsRe.FindStringSubmatch(queuedMsg)
	if m == nil {
		t.Fatalf("could not extract queue ID from SMTP response %q", queuedMsg)
	}
	return m[1]
}

// --- Tests ---

func TestInstallAndLifecycle(t *testing.T) {
	env := loadTestEnv(t)

	tc := startContainer(t, env, containerOpts{
		ShellCommand: "trap 'exit 0' TERM INT; while :; do sleep 1; done",
		ExposePorts:  []string{"25"},
	})

	// Prepare clean baseline with a conflicting Subject rule to verify conflict warning and restoration.
	tc.mustExec("sh", "-c", `
set -eu
postconf -e "maillog_file = /var/log/postfix.log"
postconf -e "inet_interfaces = all"
postconf -e "mynetworks = 127.0.0.0/8 [::1]/128 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16"
if [ -z "$(postconf -h myhostname 2>/dev/null || true)" ] || [ "$(postconf -h myhostname 2>/dev/null)" = "localhost" ]; then
  postconf -e "myhostname = test-lifecycle.localdomain"
fi
if [ -f /etc/postfix/aliases ] && [ ! -f /etc/postfix/aliases.lmdb ] && [ ! -f /etc/postfix/aliases.db ]; then
  postalias /etc/postfix/aliases || true
fi
printf '/^Subject:/ WARN\n' > /etc/postfix/pre_header_checks
postconf -e "header_checks = regexp:/etc/postfix/pre_header_checks"
postconf -e "enable_long_queue_ids = no"
`)

	daemonDir := tc.postconfGet("daemon_directory")
	dataDir := tc.postconfGet("data_directory")
	configDir := tc.postconfGet("config_directory")
	stateDir := filepath.Join(dataDir, "postfix-api")

	origMasterPostlog := normalizeWS(tc.postconfMaster("postlog/unix-dgram"))
	origHeaderChecks := tc.postconfGet("header_checks")
	origLongQIDs := tc.postconfGet("enable_long_queue_ids")

	// 1. First install run.
	out1, err1, runErr1 := tc.exec("/usr/local/bin/postfix-api", "install", "--listen", "127.0.0.1:8025")
	if runErr1 != nil {
		t.Fatalf("first install failed: %v\nstdout: %s\nstderr: %s", runErr1, out1, err1)
	}
	combined1 := out1 + "\n" + err1
	if !strings.Contains(combined1, "WARNING:") || !strings.Contains(combined1, "Subject") {
		t.Fatalf("expected Subject header_checks conflict warning on install, got:\n%s", combined1)
	}

	backupPath := filepath.Join(stateDir, "install-backup.json")
	backupBytes1, err := tc.readFile(backupPath)
	if err != nil {
		t.Fatalf("expected install-backup.json after first install: %v", err)
	}
	confPath := filepath.Join(configDir, "postfix-api.conf")
	hcPath := filepath.Join(configDir, "postfix-api-header-checks")
	binDest := filepath.Join(daemonDir, "postfix-api")

	tc.mustExec("test", "-x", binDest)
	tc.mustExec("test", "-f", hcPath)
	perm := strings.TrimSpace(tc.mustExec("stat", "-c", "%a", confPath))
	if perm != "600" {
		t.Fatalf("expected %s mode 600, got %s", confPath, perm)
	}
	if got := tc.postconfGet("enable_long_queue_ids"); got != "yes" {
		t.Fatalf("expected enable_long_queue_ids=yes, got %q", got)
	}
	hcAfter1 := tc.postconfGet("header_checks")
	if strings.Count(hcAfter1, "postfix-api-header-checks") != 1 {
		t.Fatalf("expected header_checks to contain postfix-api-header-checks once, got %q", hcAfter1)
	}

	// 2. Customize postfix-api.conf and verify second install is strictly idempotent.
	tc.mustExec("sh", "-c", fmt.Sprintf("printf '# idempotency-sentinel\\n' >> %q", confPath))
	out2, err2, runErr2 := tc.exec("/usr/local/bin/postfix-api", "install", "--listen", "127.0.0.1:8025")
	if runErr2 != nil {
		t.Fatalf("second install failed: %v\nstdout: %s\nstderr: %s", runErr2, out2, err2)
	}

	confAfter2, err := tc.readFile(confPath)
	if err != nil {
		t.Fatalf("read config after 2nd install: %v", err)
	}
	if !strings.Contains(confAfter2, "# idempotency-sentinel") {
		t.Fatalf("second install overwrote existing postfix-api.conf")
	}
	backupBytes2, err := tc.readFile(backupPath)
	if err != nil {
		t.Fatalf("read backup after 2nd install: %v", err)
	}
	if backupBytes2 != backupBytes1 {
		t.Fatalf("second install modified install-backup.json:\nbefore:\n%s\nafter:\n%s", backupBytes1, backupBytes2)
	}
	hcAfter2 := tc.postconfGet("header_checks")
	if hcAfter2 != hcAfter1 || strings.Count(hcAfter2, "postfix-api-header-checks") != 1 {
		t.Fatalf("header_checks changed/duplicated on 2nd install: %q vs %q", hcAfter1, hcAfter2)
	}

	// 3. Start Postfix and trigger both unix-dgram and inet processes.
	tc.mustExec("postfix", "start")
	tc.waitPostfixReady(15 * time.Second)
	tc.waitForContainerTCP("127.0.0.1", "8025", 10*time.Second)
	_, _, _ = tc.exec("/usr/local/bin/testhelper", "http-get", "--url", "http://127.0.0.1:8025/v1/health")

	recv1 := tc.waitProcByRole("receiver", 0, 10*time.Second)
	api1 := tc.waitProcByRole("api", 0, 10*time.Second)
	helper1 := tc.waitProcByRole("root-helper", 0, 10*time.Second)
	recvGen1 := tc.procGeneration(recv1.PID)
	apiGen1 := tc.procGeneration(api1.PID)

	// 4. Reload Postfix and verify both services exit cleanly on FD 5 EOF and restart under next GENERATION.
	tc.mustExec("postfix", "reload")
	tc.waitPIDGone(recv1.PID, 10*time.Second)
	tc.waitPIDGone(api1.PID, 10*time.Second)
	tc.waitPIDGone(helper1.PID, 10*time.Second)

	// Trigger socket activation after reload.
	tc.waitForContainerTCP("127.0.0.1", "8025", 10*time.Second)
	_, _, _ = tc.exec("/usr/local/bin/testhelper", "flood", "--count", "1", "--prefix", "RELOAD-PING-")
	_, _, _ = tc.exec("/usr/local/bin/testhelper", "http-get", "--url", "http://127.0.0.1:8025/v1/health")

	recv2 := tc.waitProcByRole("receiver", recv1.PID, 10*time.Second)
	api2 := tc.waitProcByRole("api", api1.PID, 10*time.Second)
	_ = tc.waitProcByRole("root-helper", helper1.PID, 10*time.Second)
	if recv2.PID == recv1.PID {
		t.Fatalf("expected receiver PID to change on reload: pid=%d", recv1.PID)
	}
	if api2.PID == api1.PID {
		t.Fatalf("expected api PID to change on reload: pid=%d", api1.PID)
	}
	recvGen2 := tc.procGeneration(recv2.PID)
	apiGen2 := tc.procGeneration(api2.PID)
	if recvGen1 != "" && recvGen2 != "" && recvGen1 == recvGen2 {
		t.Fatalf("expected receiver GENERATION to advance on reload: gen1=%q gen2=%q", recvGen1, recvGen2)
	}
	if apiGen1 != "" && apiGen2 != "" && apiGen1 == apiGen2 {
		t.Fatalf("expected api GENERATION to advance on reload: gen1=%q gen2=%q", apiGen1, apiGen2)
	}

	// 5. Stop Postfix and verify zero lingering postfix-api or _root-helper processes remain.
	tc.mustExec("postfix", "stop")
	deadline := time.Now().Add(15 * time.Second)
	for {
		procs, err := tc.findPostfixAPIProcs()
		if err == nil && len(procs) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lingering postfix-api processes after postfix stop: %+v", procs)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// 6. Uninstall and verify exact restoration of master.cf / main.cf while preserving <state_dir>.
	uOut, uErr, uRunErr := tc.exec("/usr/local/bin/postfix-api", "uninstall")
	if uRunErr != nil {
		t.Fatalf("uninstall failed: %v\nstdout: %s\nstderr: %s", uRunErr, uOut, uErr)
	}

	restoredPostlog := normalizeWS(tc.postconfMaster("postlog/unix-dgram"))
	if restoredPostlog != origMasterPostlog {
		t.Fatalf("master.cf postlog not restored: got %q, want %q", restoredPostlog, origMasterPostlog)
	}
	if inetLine := tc.postconfMaster("127.0.0.1:8025/inet"); inetLine != "" {
		t.Fatalf("expected 127.0.0.1:8025/inet removed from master.cf, got %q", inetLine)
	}
	if got := tc.postconfGet("header_checks"); got != origHeaderChecks {
		t.Fatalf("header_checks not restored: got %q, want %q", got, origHeaderChecks)
	}
	if got := tc.postconfGet("enable_long_queue_ids"); got != origLongQIDs {
		t.Fatalf("enable_long_queue_ids not restored: got %q, want %q", got, origLongQIDs)
	}

	if _, _, err := tc.exec("test", "-e", binDest); err == nil {
		t.Fatalf("expected installed binary %s to be removed", binDest)
	}
	if _, _, err := tc.exec("test", "-e", hcPath); err == nil {
		t.Fatalf("expected header_checks file %s to be removed", hcPath)
	}

	// State directory, SQLite DB, spool, backup JSON, and postfix-api.conf must stay intact.
	tc.mustExec("test", "-d", stateDir)
	tc.mustExec("test", "-f", backupPath)
	tc.mustExec("test", "-f", filepath.Join(stateDir, "state.db"))
	tc.mustExec("test", "-d", filepath.Join(stateDir, "spool"))
	tc.mustExec("test", "-f", confPath)
}

func TestDeliveryTrackingAndSubjectAndNextRetry(t *testing.T) {
	env := loadTestEnv(t)
	tc := startContainer(t, env, containerOpts{
		Conf:             defaultConfOptions(),
		UseTransportMaps: true,
		StartSMTPSink:    true,
	})

	smtpAddr := tc.hostPort("25")
	apiAddr := tc.hostPort("8025")
	api := newAPIClient(apiAddr)
	api.waitForHealth(t, adminToken, "OPERATIONAL", 45*time.Second)

	// 1. Send deferred email with UTF-8 MIME-encoded Subject decoding to "Привет order 42".
	msgID1 := fmt.Sprintf("<def-1-%d@example.com>", time.Now().UnixNano())
	qid1 := sendSMTP(t, smtpAddr, "a@example.com", "b@blackhole.test",
		"=?UTF-8?B?0J/RgNC40LLQtdGC?= order 42", msgID1, "Deferred UTF-8 body\n")

	msg1 := api.waitForMessage(t, qid1, adminToken, 30*time.Second, func(m apiMessage) bool {
		return m.QueueState == "deferred" && len(m.Recipients) > 0 && m.Recipients[0].Attempts >= 1
	})

	if msg1.Subject != "Привет order 42" {
		t.Fatalf("subject = %q, want %q", msg1.Subject, "Привет order 42")
	}
	if msg1.Sender != "a@example.com" {
		t.Fatalf("sender = %q, want %q", msg1.Sender, "a@example.com")
	}
	if msg1.MessageID != msgID1 {
		t.Fatalf("message_id = %q, want %q", msg1.MessageID, msgID1)
	}
	if msg1.Size <= 0 {
		t.Fatalf("size = %d, want > 0", msg1.Size)
	}
	if msg1.ReceivedAt.IsZero() {
		t.Fatalf("received_at is zero")
	}

	r := msg1.Recipients[0]
	if r.Address != "b@blackhole.test" || r.Status != "deferred" || r.Attempts < 1 {
		t.Fatalf("unexpected recipient: %+v", r)
	}
	if !strings.HasPrefix(r.DSN, "4.") {
		t.Fatalf("expected 4.x.x DSN, got %q", r.DSN)
	}
	if r.Response == "" {
		t.Fatalf("expected non-empty recipient response")
	}

	// Verify events contain processed then deferred in order.
	assertEventSequence(t, msg1.Events, []string{"processed", "deferred"})

	// Verify next_retry_at is non-null, in the future, and matches deferred queue file mtime.
	if msg1.NextRetryAt == nil {
		t.Fatalf("expected non-null next_retry_at on deferred message")
	}
	if !msg1.NextRetryAt.After(time.Now()) {
		t.Fatalf("expected next_retry_at (%s) in the future (now=%s)", msg1.NextRetryAt, time.Now().UTC())
	}
	queueDir := tc.postconfGet("queue_directory")
	statOut := tc.mustExec("sh", "-c", fmt.Sprintf(
		"find %q -name %q -exec stat -c %%Y {} \\; | head -n 1",
		filepath.Join(queueDir, "deferred"),
		qid1,
	))
	mtimeSec, err := strconv.ParseInt(strings.TrimSpace(statOut), 10, 64)
	if err != nil {
		t.Fatalf("parse deferred mtime %q: %v", statOut, err)
	}
	diffSec := msg1.NextRetryAt.Unix() - mtimeSec
	if diffSec < -1 || diffSec > 1 {
		t.Fatalf("next_retry_at=%d does not match deferred file mtime=%d", msg1.NextRetryAt.Unix(), mtimeSec)
	}

	// Verify GET /v1/messages lists the deferred message with expected fields.
	var page apiMessagesResp
	if st := api.doJSON(t, http.MethodGet, "/v1/messages?queue_id="+url.QueryEscape(qid1), adminToken, nil, &page); st != http.StatusOK {
		t.Fatalf("GET /v1/messages status=%d", st)
	}
	if len(page.Messages) != 1 || page.Messages[0].Subject != "Привет order 42" {
		t.Fatalf("unexpected GET /v1/messages result: %+v", page)
	}

	// 2. Also verify Subject containing " from " (SPEC §11.3 #1).
	msgID2 := fmt.Sprintf("<def-2-%d@example.com>", time.Now().UnixNano())
	qid2 := sendSMTP(t, smtpAddr, "a@example.com", "c@blackhole.test",
		"=?UTF-8?B?0J/RgNC40LLQtdGC?= from Paris", msgID2, "Subject with from\n")
	msg2 := api.waitForMessage(t, qid2, adminToken, 30*time.Second, func(m apiMessage) bool {
		return m.QueueState == "deferred" && m.Subject != ""
	})
	if msg2.Subject != "Привет from Paris" {
		t.Fatalf("subject with from = %q, want %q", msg2.Subject, "Привет from Paris")
	}

	// 3. Verify GET /v1/events?after=0 returns events in ascending ID order.
	var evResp apiEventsResp
	if st := api.doJSON(t, http.MethodGet, "/v1/events?after=0", adminToken, nil, &evResp); st != http.StatusOK {
		t.Fatalf("GET /v1/events status=%d", st)
	}
	if len(evResp.Events) < 4 {
		t.Fatalf("expected at least 4 events, got %d", len(evResp.Events))
	}
	for i := 1; i < len(evResp.Events); i++ {
		if evResp.Events[i].ID <= evResp.Events[i-1].ID {
			t.Fatalf("events not strictly ascending at index %d: %d <= %d", i, evResp.Events[i].ID, evResp.Events[i-1].ID)
		}
	}

	// 4. Verify GET /v1/queue lists live deferred messages and counts.
	var qResp apiQueueResp
	if st := api.doJSON(t, http.MethodGet, "/v1/queue", adminToken, nil, &qResp); st != http.StatusOK {
		t.Fatalf("GET /v1/queue status=%d", st)
	}
	if qResp.Total < 2 || qResp.Counts["deferred"] < 2 {
		t.Fatalf("unexpected /v1/queue counts: total=%d counts=%+v", qResp.Total, qResp.Counts)
	}
	if !queueContains(qResp, qid1) || !queueContains(qResp, qid2) {
		t.Fatalf("expected %s and %s in /v1/queue entries: %+v", qid1, qid2, qResp.Entries)
	}

	// 5. Verify end-to-end delivery via local SMTP sink (SPEC §11.3 #2).
	afterBeforeDelivery := evResp.LastID
	msgID3 := fmt.Sprintf("<deliv-3-%d@example.com>", time.Now().UnixNano())
	qid3 := sendSMTP(t, smtpAddr, "sender3@example.com", "sink@sink.test",
		"Delivered End-to-End Check", msgID3, "Hello local sink\n")

	msg3 := api.waitForMessage(t, qid3, adminToken, 30*time.Second, func(m apiMessage) bool {
		return m.QueueState == "removed" && len(m.Recipients) > 0 && m.Recipients[0].Status == "delivered"
	})
	if msg3.RemovedAt == nil || msg3.Subject != "Delivered End-to-End Check" {
		t.Fatalf("unexpected delivered message: %+v", msg3)
	}
	assertEventSequence(t, msg3.Events, []string{"processed", "delivered"})

	var tailEv apiEventsResp
	if st := api.doJSON(t, http.MethodGet, fmt.Sprintf("/v1/events?after=%d", afterBeforeDelivery), adminToken, nil, &tailEv); st != http.StatusOK {
		t.Fatalf("GET /v1/events tail status=%d", st)
	}
	if !eventsContainForQueue(tailEv.Events, qid3, "delivered") {
		t.Fatalf("expected delivered event for %s in /v1/events?after=%d: %+v", qid3, afterBeforeDelivery, tailEv.Events)
	}
}

func TestQueueActionsAndContentAndBulk(t *testing.T) {
	env := loadTestEnv(t)
	tc := startContainer(t, env, containerOpts{
		Conf:      defaultConfOptions(),
		Relayhost: "[192.0.2.1]:25",
	})

	smtpAddr := tc.hostPort("25")
	apiAddr := tc.hostPort("8025")
	api := newAPIClient(apiAddr)
	api.waitForHealth(t, adminToken, "OPERATIONAL", 45*time.Second)

	msgID1 := fmt.Sprintf("<qa-1-%d@example.com>", time.Now().UnixNano())
	msgID2 := fmt.Sprintf("<qa-2-%d@example.com>", time.Now().UnixNano())
	msgID3 := fmt.Sprintf("<qa-3-%d@example.com>", time.Now().UnixNano())

	qid1 := sendSMTP(t, smtpAddr, "qa@example.com", "r1@blackhole.test", "Queue Action 1", msgID1, "Body of message one\n")
	qid2 := sendSMTP(t, smtpAddr, "qa@example.com", "r2@blackhole.test", "Queue Action 2", msgID2, "Body of message two\n")
	qid3 := sendSMTP(t, smtpAddr, "qa@example.com", "r3@blackhole.test", "Queue Action 3", msgID3, "Body of message three\n")

	for _, qid := range []string{qid1, qid2, qid3} {
		api.waitForMessage(t, qid, adminToken, 30*time.Second, func(m apiMessage) bool {
			return m.QueueState == "deferred"
		})
	}

	// Verify keyset pagination across 3 messages with limit=2.
	var p1, p2 apiMessagesResp
	if st := api.doJSON(t, http.MethodGet, "/v1/messages?limit=2", adminToken, nil, &p1); st != http.StatusOK {
		t.Fatalf("page 1 status=%d", st)
	}
	if len(p1.Messages) != 2 || p1.NextCursor == nil || *p1.NextCursor == "" {
		t.Fatalf("unexpected page 1: len=%d next_cursor=%v", len(p1.Messages), p1.NextCursor)
	}
	if st := api.doJSON(t, http.MethodGet, "/v1/messages?limit=2&cursor="+url.QueryEscape(*p1.NextCursor), adminToken, nil, &p2); st != http.StatusOK {
		t.Fatalf("page 2 status=%d", st)
	}
	if len(p2.Messages) != 1 || p2.NextCursor != nil {
		t.Fatalf("unexpected page 2: len=%d next_cursor=%v", len(p2.Messages), p2.NextCursor)
	}

	// Content inspection on qid1.
	var content apiContentResp
	if st := api.doJSON(t, http.MethodGet, "/v1/messages/"+qid1+"/content", adminToken, nil, &content); st != http.StatusOK {
		t.Fatalf("GET content status=%d", st)
	}
	if content.QueueID != qid1 || content.BodyEncoding != "utf-8" ||
		!strings.Contains(content.Headers, "Subject: Queue Action 1") ||
		!strings.Contains(content.Body, "Body of message one") {
		t.Fatalf("unexpected content response: %+v", content)
	}

	// Hold qid1.
	var act apiActionResp
	if st := api.doJSON(t, http.MethodPost, "/v1/messages/"+qid1+"/hold", adminToken, nil, &act); st != http.StatusOK || act.ExitCode != 0 {
		t.Fatalf("hold failed: status=%d resp=%+v", st, act)
	}
	heldMsg := api.waitForMessage(t, qid1, adminToken, 15*time.Second, func(m apiMessage) bool {
		return m.QueueState == "hold" && hasEvent(m.Events, "held")
	})
	if heldMsg.QueueState != "hold" {
		t.Fatalf("expected hold state, got %s", heldMsg.QueueState)
	}
	var qSnap apiQueueResp
	api.doJSON(t, http.MethodGet, "/v1/queue", adminToken, nil, &qSnap)
	if qSnap.Counts["hold"] < 1 {
		t.Fatalf("expected hold count >= 1 in /v1/queue, got %+v", qSnap.Counts)
	}

	// Release qid1.
	if st := api.doJSON(t, http.MethodPost, "/v1/messages/"+qid1+"/release", adminToken, nil, &act); st != http.StatusOK || act.ExitCode != 0 {
		t.Fatalf("release failed: status=%d resp=%+v", st, act)
	}
	api.waitForMessage(t, qid1, adminToken, 15*time.Second, func(m apiMessage) bool {
		return hasEvent(m.Events, "released")
	})

	// Requeue qid1 -> old qid moves to requeued, new qid enters queue with same message_id.
	if st := api.doJSON(t, http.MethodPost, "/v1/messages/"+qid1+"/requeue", adminToken, nil, &act); st != http.StatusOK || act.ExitCode != 0 {
		t.Fatalf("requeue failed: status=%d resp=%+v", st, act)
	}
	// Nudge pickup so the requeued message in maildrop is ingested immediately rather than waiting 60s for pickup's master.cf wakeup timer.
	_, _, _ = tc.exec("postkick", "public", "pickup", "W")
	requeuedOld := api.waitForMessage(t, qid1, adminToken, 15*time.Second, func(m apiMessage) bool {
		return m.QueueState == "requeued" && hasEvent(m.Events, "requeued")
	})
	if requeuedOld.RemovedAt == nil {
		t.Fatalf("expected removed_at populated after requeue")
	}

	var newQID string
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var found apiMessagesResp
		api.doJSON(t, http.MethodGet, "/v1/messages?message_id="+url.QueryEscape(msgID1), adminToken, nil, &found)
		for _, m := range found.Messages {
			if m.QueueID != qid1 && m.QueueState == "deferred" {
				newQID = m.QueueID
				break
			}
		}
		if newQID != "" {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if newQID == "" {
		t.Fatalf("timed out waiting for requeued new queue ID for message_id=%s", msgID1)
	}

	// Expire newQID.
	if st := api.doJSON(t, http.MethodPost, "/v1/messages/"+newQID+"/expire", adminToken, nil, &act); st != http.StatusOK || act.ExitCode != 0 {
		t.Fatalf("expire failed: status=%d resp=%+v", st, act)
	}
	api.waitForMessage(t, newQID, adminToken, 15*time.Second, func(m apiMessage) bool {
		return hasEvent(m.Events, "expire_requested")
	})

	// Delete newQID before queue flush (since expire + flush would cause qmgr to expire and remove it first),
	// and confirm 404 on /content afterwards.
	if st := api.doJSON(t, http.MethodDelete, "/v1/messages/"+newQID, adminToken, nil, &act); st != http.StatusOK || act.ExitCode != 0 {
		t.Fatalf("delete %s failed: status=%d resp=%+v", newQID, st, act)
	}
	delMsg := api.waitForMessage(t, newQID, adminToken, 15*time.Second, func(m apiMessage) bool {
		return m.QueueState == "removed" && hasEvent(m.Events, "deleted")
	})
	if delMsg.RemovedAt == nil {
		t.Fatalf("expected removed_at on deleted message %s", newQID)
	}
	if code, raw, err := api.doRaw(http.MethodGet, "/v1/messages/"+newQID+"/content", adminToken, nil); err != nil || code != http.StatusNotFound {
		t.Fatalf("expected 404 for deleted message content, got code=%d err=%v body=%s", code, err, string(raw))
	}

	// Flush single message & full queue.
	if st := api.doJSON(t, http.MethodPost, "/v1/messages/"+qid2+"/flush", adminToken, nil, &act); st != http.StatusOK || act.ExitCode != 0 {
		t.Fatalf("message flush failed: status=%d resp=%+v", st, act)
	}
	if st := api.doJSON(t, http.MethodPost, "/v1/queue/flush", adminToken, nil, &act); st != http.StatusOK || act.ExitCode != 0 {
		t.Fatalf("queue flush failed: status=%d resp=%+v", st, act)
	}

	// Wait for qid2 and qid3 to settle back to deferred after flush before bulk postsuper actions.
	for _, qid := range []string{qid2, qid3} {
		api.waitForMessage(t, qid, adminToken, 20*time.Second, func(m apiMessage) bool {
			return m.QueueState == "deferred" && len(m.Recipients) > 0 && m.Recipients[0].Attempts >= 2
		})
	}

	// Bulk hold on qid2 + qid3.
	bulkHoldReq := map[string]any{
		"action":    "hold",
		"queue_ids": []string{qid2, qid3},
	}
	if st := api.doJSON(t, http.MethodPost, "/v1/queue/actions", adminToken, bulkHoldReq, &act); st != http.StatusOK || act.ExitCode != 0 {
		t.Fatalf("bulk hold failed: status=%d resp=%+v", st, act)
	}
	for _, qid := range []string{qid2, qid3} {
		api.waitForMessage(t, qid, adminToken, 15*time.Second, func(m apiMessage) bool {
			return m.QueueState == "hold" && hasEvent(m.Events, "held")
		})
	}

	// Bulk delete on qid2 + qid3.
	bulkDelReq := map[string]any{
		"action":    "delete",
		"queue_ids": []string{qid2, qid3},
	}
	if st := api.doJSON(t, http.MethodPost, "/v1/queue/actions", adminToken, bulkDelReq, &act); st != http.StatusOK || act.ExitCode != 0 {
		t.Fatalf("bulk delete failed: status=%d resp=%+v", st, act)
	}
	for _, qid := range []string{qid2, qid3} {
		api.waitForMessage(t, qid, adminToken, 15*time.Second, func(m apiMessage) bool {
			return m.QueueState == "removed" && hasEvent(m.Events, "deleted")
		})
		if code, _, err := api.doRaw(http.MethodGet, "/v1/messages/"+qid+"/content", adminToken, nil); err != nil || code != http.StatusNotFound {
			t.Fatalf("expected 404 on %s content after bulk delete, got %d (%v)", qid, code, err)
		}
	}
}

func TestAuthScopesAndPlaintextGuard(t *testing.T) {
	env := loadTestEnv(t)

	// Part A: Token auth & scopes matrix + empty tokens list.
	conf := defaultConfOptions()
	conf.Tokens = []tokenSpec{
		{Name: "reader", Raw: readToken, Scopes: []string{"read"}},
		{Name: "writer", Raw: writeToken, Scopes: []string{"write"}},
		{Name: "content-reader", Raw: contentToken, Scopes: []string{"content"}},
	}

	tc := startContainer(t, env, containerOpts{
		Conf:      conf,
		Relayhost: "[192.0.2.1]:25",
	})

	smtpAddr := tc.hostPort("25")
	api := newAPIClient(tc.hostPort("8025"))
	api.waitForHealth(t, readToken, "OPERATIONAL", 45*time.Second)

	// Enqueue one message and hold it so /content returns 200 for content-scoped token.
	qid := sendSMTP(t, smtpAddr, "auth@example.com", "u@blackhole.test",
		"Auth Scope Test", fmt.Sprintf("<auth-%d@example.com>", time.Now().UnixNano()), "Secret body\n")
	api.waitForMessage(t, qid, readToken, 30*time.Second, func(m apiMessage) bool {
		return m.QueueState == "deferred"
	})
	if code, raw, err := api.doRaw(http.MethodPost, "/v1/messages/"+qid+"/hold", writeToken, nil); err != nil || code != http.StatusOK {
		t.Fatalf("writer hold failed: code=%d err=%v body=%s", code, err, string(raw))
	}

	cases := []struct {
		name     string
		method   string
		path     string
		token    string
		wantCode int
	}{
		{"no token", http.MethodGet, "/v1/messages", "", http.StatusUnauthorized},
		{"invalid token", http.MethodGet, "/v1/messages", "invalid-secret", http.StatusUnauthorized},
		{"read token on messages", http.MethodGet, "/v1/messages", readToken, http.StatusOK},
		{"read token on health", http.MethodGet, "/v1/health", readToken, http.StatusOK},
		{"read token on content", http.MethodGet, "/v1/messages/" + qid + "/content", readToken, http.StatusForbidden},
		{"read token on queue flush", http.MethodPost, "/v1/queue/flush", readToken, http.StatusForbidden},
		{"read token on hold", http.MethodPost, "/v1/messages/" + qid + "/hold", readToken, http.StatusForbidden},
		{"content token on existing content", http.MethodGet, "/v1/messages/" + qid + "/content", contentToken, http.StatusOK},
		{"content token on missing content", http.MethodGet, "/v1/messages/AAAAAA/content", contentToken, http.StatusNotFound},
		{"content token on messages", http.MethodGet, "/v1/messages", contentToken, http.StatusForbidden},
		{"content token on flush", http.MethodPost, "/v1/queue/flush", contentToken, http.StatusForbidden},
		{"write token on flush", http.MethodPost, "/v1/queue/flush", writeToken, http.StatusOK},
		{"write token on messages", http.MethodGet, "/v1/messages", writeToken, http.StatusForbidden},
		{"write token on content", http.MethodGet, "/v1/messages/" + qid + "/content", writeToken, http.StatusForbidden},
	}

	for _, cCase := range cases {
		t.Run(cCase.name, func(t *testing.T) {
			code, raw, err := api.doRaw(cCase.method, cCase.path, cCase.token, nil)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			if code != cCase.wantCode {
				t.Fatalf("got HTTP %d, want %d; body=%s", code, cCase.wantCode, string(raw))
			}
			if code >= 400 {
				var envErr apiErrorEnvelope
				if uerr := json.Unmarshal(raw, &envErr); uerr != nil || envErr.Error.Code == "" {
					t.Fatalf("expected standard error envelope, got: %s", string(raw))
				}
			}
		})
	}

	// Non-Bearer scheme -> 401.
	req, _ := http.NewRequest(http.MethodGet, api.baseURL+"/v1/messages", nil)
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	resp, err := api.httpClient.Do(req)
	if err != nil {
		t.Fatalf("non-Bearer request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for Basic scheme, got %d", resp.StatusCode)
	}

	// Empty tokens: [] -> 401 for all requests.
	apiProcBefore := tc.waitProcByRole("api", 0, 10*time.Second)
	emptyConf := conf
	emptyConf.Tokens = nil
	tc.writeFile("/etc/postfix/postfix-api.conf", renderConfYAML(emptyConf), 0o600)
	tc.mustExec("postfix", "reload")
	tc.waitPIDGone(apiProcBefore.PID, 10*time.Second)

	deadline := time.Now().Add(15 * time.Second)
	for {
		code, _, err := api.doRaw(http.MethodGet, "/v1/messages", readToken, nil)
		if err == nil {
			if code == http.StatusUnauthorized {
				break
			}
			t.Fatalf("expected 401 after reloading empty tokens, got %d", code)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for reloaded API to return 401: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Part B: Non-loopback plaintext guard (0.0.0.0:8025 with TLS off and allow_plaintext: false refuses to serve).
	t.Run("non-loopback plaintext guard refuses to serve", func(t *testing.T) {
		guardConf := defaultConfOptions()
		guardConf.AllowPlaintext = false
		tcGuard := startContainer(t, env, containerOpts{
			ListenAddr:  "0.0.0.0:8025",
			Conf:        guardConf,
			ExposePorts: []string{"8025"},
		})
		tcGuard.waitPostfixReady(20 * time.Second)

		guardAPI := newAPIClient(tcGuard.hostPort("8025"))
		guardAPI.httpClient.Timeout = 3 * time.Second
		// Trigger master to spawn inet process; it must refuse and close without serving HTTP.
		for i := 0; i < 3; i++ {
			code, raw, err := guardAPI.doRaw(http.MethodGet, "/v1/health", adminToken, nil)
			if err == nil {
				t.Fatalf("expected non-loopback plaintext API to refuse serving, but got HTTP %d (%s)", code, string(raw))
			}
			time.Sleep(250 * time.Millisecond)
		}

		// Verify explicit plaintext refusal log entry in container logs.
		logDeadline := time.Now().Add(10 * time.Second)
		var logs string
		for {
			logs, _ = tcGuard.logs()
			lower := strings.ToLower(logs)
			if strings.Contains(lower, "allow_plaintext") || strings.Contains(lower, "plaintext") {
				break
			}
			if time.Now().After(logDeadline) {
				t.Fatalf("expected plaintext refusal message in logs, got:\n%s", logs)
			}
			time.Sleep(200 * time.Millisecond)
		}
	})

	// Part C: Loopback listener (127.0.0.1:8025) with allow_plaintext: false IS allowed.
	t.Run("loopback plaintext allowed without allow_plaintext", func(t *testing.T) {
		loopConf := defaultConfOptions()
		loopConf.AllowPlaintext = false
		tcLoop := startContainer(t, env, containerOpts{
			ListenAddr:  "127.0.0.1:8025",
			Conf:        loopConf,
			ExposePorts: []string{"25"},
		})
		tcLoop.waitPostfixReady(20 * time.Second)

		loopDeadline := time.Now().Add(20 * time.Second)
		for {
			out, _, err := tcLoop.exec("/usr/local/bin/testhelper", "http-get", "--url", "http://127.0.0.1:8025/v1/health", "--token", adminToken)
			if err == nil && strings.Contains(out, `"OPERATIONAL"`) {
				break
			}
			if time.Now().After(loopDeadline) {
				t.Fatalf("loopback HTTP request failed: err=%v out=%s", err, out)
			}
			time.Sleep(250 * time.Millisecond)
		}
	})
}

func TestLogsSubjectRedactionAndOutputParity(t *testing.T) {
	env := loadTestEnv(t)
	conf := defaultConfOptions()
	conf.SubjectInOutput = false

	tc := startContainer(t, env, containerOpts{
		Conf:      conf,
		Relayhost: "[192.0.2.1]:25",
	})

	smtpAddr := tc.hostPort("25")
	api := newAPIClient(tc.hostPort("8025"))
	api.waitForHealth(t, adminToken, "OPERATIONAL", 45*time.Second)

	const secretSubj = "SECRET-SUBJECT-TOKEN-999"
	qid1 := sendSMTP(t, smtpAddr, "sec@example.com", "u@blackhole.test",
		secretSubj, fmt.Sprintf("<sec-%d@example.com>", time.Now().UnixNano()), "Secret payload\n")
	msg1 := api.waitForMessage(t, qid1, adminToken, 30*time.Second, func(m apiMessage) bool {
		return m.QueueState == "deferred" && m.Subject != ""
	})
	if msg1.Subject != secretSubj {
		t.Fatalf("API subject = %q, want %q", msg1.Subject, secretSubj)
	}

	// Poll docker logs for normal Postfix daemon lines and confirm Subject is absent.
	var logs1 string
	logDeadline := time.Now().Add(10 * time.Second)
	for {
		logs1, _ = tc.logs()
		if strings.Contains(logs1, qid1) && strings.Contains(logs1, "qmgr[") &&
			strings.Contains(logs1, "cleanup[") && strings.Contains(logs1, "smtp[") {
			break
		}
		if time.Now().After(logDeadline) {
			t.Fatalf("expected qmgr/cleanup/smtp lines for %s in container logs:\n%s", qid1, logs1)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if strings.Contains(logs1, secretSubj) {
		t.Fatalf("secret subject %q leaked into container logs:\n%s", secretSubj, logs1)
	}
	if strings.Contains(logs1, ": info: header Subject: ") {
		t.Fatalf("found unredacted header Subject line in container logs when subject_in_output=false:\n%s", logs1)
	}

	// Enable subject_in_output: true and reload Postfix.
	recvBefore := tc.waitProcByRole("receiver", 0, 10*time.Second)
	apiBefore := tc.waitProcByRole("api", 0, 10*time.Second)
	conf.SubjectInOutput = true
	tc.writeFile("/etc/postfix/postfix-api.conf", renderConfYAML(conf), 0o600)
	tc.mustExec("postfix", "reload")
	tc.waitPIDGone(recvBefore.PID, 10*time.Second)
	tc.waitPIDGone(apiBefore.PID, 10*time.Second)

	api.waitForHealth(t, adminToken, "OPERATIONAL", 20*time.Second)

	const visibleSubj = "VISIBLE-SUBJECT-TOKEN-888"
	qid2 := sendSMTP(t, smtpAddr, "vis@example.com", "u@blackhole.test",
		visibleSubj, fmt.Sprintf("<vis-%d@example.com>", time.Now().UnixNano()), "Visible payload\n")
	msg2 := api.waitForMessage(t, qid2, adminToken, 30*time.Second, func(m apiMessage) bool {
		return m.QueueState == "deferred" && m.Subject != ""
	})
	if msg2.Subject != visibleSubj {
		t.Fatalf("API subject = %q, want %q", msg2.Subject, visibleSubj)
	}

	wantLogFrag := ": info: header Subject: " + visibleSubj
	visDeadline := time.Now().Add(15 * time.Second)
	for {
		logs2, _ := tc.logs()
		if strings.Contains(logs2, wantLogFrag) {
			if strings.Contains(logs2, secretSubj) {
				t.Fatalf("earlier secret subject %q appeared in logs", secretSubj)
			}
			break
		}
		if time.Now().After(visDeadline) {
			t.Fatalf("expected %q in container logs when subject_in_output=true:\n%s", wantLogFrag, logs2)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestLogsReceiverForcedStopUnderFlood(t *testing.T) {
	env := loadTestEnv(t)
	tc := startContainer(t, env, containerOpts{
		Conf:      defaultConfOptions(),
		Relayhost: "[192.0.2.1]:25",
	})

	smtpAddr := tc.hostPort("25")
	api := newAPIClient(tc.hostPort("8025"))
	api.waitForHealth(t, adminToken, "OPERATIONAL", 45*time.Second)

	queueDir := tc.postconfGet("queue_directory")
	dataDir := tc.postconfGet("data_directory")
	sockPath := filepath.Join(queueDir, "public", "postlog")
	spoolDir := filepath.Join(dataDir, "postfix-api", "spool")

	// Ensure log receiver is running before starting flood.
	recv := tc.waitProcByRole("receiver", 0, 10*time.Second)

	const totalRecords = 32000
	progressFile := "/tmp/flood.progress"
	pidFile := "/tmp/flood.pid"

	type floodResult struct {
		stdout string
		stderr string
		err    error
	}
	floodDone := make(chan floodResult, 1)

	go func() {
		var out, errOut string
		var err error
		if tc.hasPython3 {
			py := `import os, socket, sys, time
sock_path, count, pace_every, pace_ms, prog_path, pid_path = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4]), sys.argv[5], sys.argv[6]
with open(pid_path, "w") as f:
    f.write(str(os.getpid()))
s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
s.connect(sock_path)
for i in range(1, count + 1):
    msg = f"postfix/testflood[99999]: FLOOD-SEQ-{i:05d}".encode()
    while True:
        try:
            s.send(msg)
            break
        except InterruptedError:
            continue
        except OSError as e:
            if e.errno in (11, 105):
                time.sleep(0.001)
                continue
            raise
    if prog_path and (i % 250 == 0 or i == count):
        with open(prog_path + ".tmp", "w") as f:
            f.write(str(i))
        os.replace(prog_path + ".tmp", prog_path)
    if pace_every > 0 and i % pace_every == 0:
        time.sleep(pace_ms / 1000.0)
print(f"DONE {count}", flush=True)
`
			out, errOut, err = tc.exec("python3", "-c", py, sockPath, strconv.Itoa(totalRecords), "5", "1", progressFile, pidFile)
		} else {
			out, errOut, err = tc.exec(
				"/usr/local/bin/testhelper", "flood",
				"--socket", sockPath,
				"--count", strconv.Itoa(totalRecords),
				"--prefix", "FLOOD-SEQ-",
				"--width", "5",
				"--pace-every", "5",
				"--pace-ms", "1",
				"--progress-file", progressFile,
				"--pid-file", pidFile,
			)
		}
		floodDone <- floodResult{stdout: out, stderr: errOut, err: err}
	}()

	// Wait until the flood writer has published its pid.
	floodPID := 0
	pidDeadline := time.Now().Add(10 * time.Second)
	for {
		raw, err := tc.readFile(pidFile)
		if err == nil {
			floodPID, _ = strconv.Atoi(strings.TrimSpace(raw))
			if floodPID > 0 {
				break
			}
		}
		if time.Now().After(pidDeadline) {
			t.Fatal("timed out waiting for flood writer pid file")
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Cleanup(func() {
		_, _, _ = tc.exec("kill", "-CONT", strconv.Itoa(floodPID))
	})

	// Send SIGKILL to the log receiver 3 separate times strictly mid-flood.
	// Pause the flood writer first so the receiver can drain its socket and is
	// blocked in Read — SIGKILL then cannot land on an in-flight datagram that
	// has already been dequeued from the kernel queue.
	curPID := recv.PID
	killThresholds := []int{4000, 12000, 20000}
	for idx, targetProgress := range killThresholds {
		round := idx + 1
		deadline := time.Now().Add(20 * time.Second)
		var progress int
		for {
			raw, err := tc.readFile(progressFile)
			if err == nil {
				progress, _ = strconv.Atoi(strings.TrimSpace(raw))
				if progress >= targetProgress {
					break
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("round %d: timed out waiting for flood progress >= %d (last=%d)", round, targetProgress, progress)
			}
			time.Sleep(50 * time.Millisecond)
		}
		if progress >= totalRecords {
			t.Fatalf("round %d: flood completed (%d) before SIGKILL could fire mid-flood", round, progress)
		}

		tc.mustExec("kill", "-STOP", strconv.Itoa(floodPID))
		time.Sleep(100 * time.Millisecond)

		live := tc.waitProcByRole("receiver", 0, 5*time.Second)
		curPID = live.PID
		tc.mustExec("kill", "-KILL", strconv.Itoa(curPID))

		// Master must respawn immediately (< 5s, no 60s service_throttle_time).
		respawned := tc.waitProcByRole("receiver", curPID, 5*time.Second)
		t.Logf("SIGKILL round %d at progress=%d: oldPID=%d -> newPID=%d (flood paused)", round, progress, curPID, respawned.PID)
		curPID = respawned.PID

		tc.mustExec("kill", "-CONT", strconv.Itoa(floodPID))
	}

	res := <-floodDone
	if res.err != nil {
		t.Fatalf("flood writer failed: %v\nstdout: %s\nstderr: %s", res.err, res.stdout, res.stderr)
	}
	if !strings.Contains(res.stdout, fmt.Sprintf("DONE %d", totalRecords)) {
		t.Fatalf("unexpected flood writer output: %q", res.stdout)
	}

	// Poll container logs until all 32,000 flood markers are flushed to docker logs.
	var logs string
	logDeadline := time.Now().Add(30 * time.Second)
	var countSeen int
	var missing []int
	for {
		logs, _ = tc.logs()
		countSeen, missing = seqmark.CountUnique(logs, "FLOOD-SEQ-", 5, totalRecords)
		if countSeen == totalRecords {
			break
		}
		if time.Now().After(logDeadline) {
			t.Fatalf("expected %d/%d flood records in container logs, got %d (sample missing: %v)",
				totalRecords, totalRecords, countSeen, missing)
		}
		time.Sleep(250 * time.Millisecond)
	}

	lowerLogs := strings.ToLower(logs)
	if strings.Contains(lowerLogs, "throttling") || strings.Contains(lowerLogs, "service_throttle_time") {
		t.Fatalf("unexpected Postfix throttling warning in container logs after receiver SIGKILL")
	}

	if gaps := tc.readCounter(filepath.Join(spoolDir, "gaps.count")); gaps != 0 {
		t.Fatalf("expected gaps.count=0, got %d", gaps)
	}
	if lost := tc.readCounter(filepath.Join(spoolDir, "lost.count")); lost != 0 {
		t.Fatalf("expected lost.count=0, got %d", lost)
	}

	h := api.waitForHealth(t, adminToken, "OPERATIONAL", 15*time.Second)
	if h.SpoolGaps != 0 || h.LostSpoolRecords != 0 {
		t.Fatalf("unexpected degraded spool metrics after flood: %+v", h)
	}

	// Send a real email post-flood to prove spool segments + indexer continue smoothly after 3x SIGKILL.
	postQID := sendSMTP(t, smtpAddr, "postflood@example.com", "u@blackhole.test",
		"Post-Flood Integrity Check", fmt.Sprintf("<postflood-%d@example.com>", time.Now().UnixNano()), "After 3x SIGKILL\n")
	postMsg := api.waitForMessage(t, postQID, adminToken, 30*time.Second, func(m apiMessage) bool {
		return m.QueueState == "deferred" && m.Subject == "Post-Flood Integrity Check"
	})
	if postMsg.Subject != "Post-Flood Integrity Check" {
		t.Fatalf("unexpected post-flood message: %+v", postMsg)
	}
}

func TestLogsStoppedReceiverBlocksAndResumes(t *testing.T) {
	env := loadTestEnv(t)
	tc := startContainer(t, env, containerOpts{
		Conf: defaultConfOptions(),
	})

	api := newAPIClient(tc.hostPort("8025"))
	api.waitForHealth(t, adminToken, "OPERATIONAL", 45*time.Second)

	queueDir := tc.postconfGet("queue_directory")
	sockPath := filepath.Join(queueDir, "public", "postlog")
	recv := tc.waitProcByRole("receiver", 0, 10*time.Second)

	// Read kernel net.unix.max_dgram_qlen so we reliably exceed it whether it is 10 or 512.
	qlen := 10
	if raw, err := tc.readFile("/proc/sys/net/unix/max_dgram_qlen"); err == nil {
		if parsed, perr := strconv.Atoi(strings.TrimSpace(raw)); perr == nil && parsed > 0 {
			qlen = parsed
		}
	}
	totalCount := qlen + 50
	if totalCount < 30 {
		totalCount = 30
	}

	// SIGSTOP the log receiver.
	tc.mustExec("kill", "-STOP", strconv.Itoa(recv.PID))
	t.Cleanup(func() {
		_, _, _ = tc.exec("kill", "-CONT", strconv.Itoa(recv.PID))
	})

	doneCh := make(chan error, 1)
	go func() {
		var err error
		if tc.hasPython3 {
			py := `import socket, sys
sock_path, count = sys.argv[1], int(sys.argv[2])
s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
s.connect(sock_path)
for i in range(1, count + 1):
    msg = f"postfix/teststop[99999]: STOP-BLOCK-{i:05d}".encode()
    while True:
        try:
            s.send(msg)
            break
        except InterruptedError:
            continue
print(f"DONE {count}", flush=True)
`
			_, _, err = tc.exec("python3", "-c", py, sockPath, strconv.Itoa(totalCount))
		} else {
			_, _, err = tc.exec(
				"/usr/local/bin/testhelper", "flood",
				"--socket", sockPath,
				"--count", strconv.Itoa(totalCount),
				"--prefix", "STOP-BLOCK-",
				"--width", "5",
			)
		}
		doneCh <- err
	}()

	// Verify the writer genuinely blocks on the full AF_UNIX SOCK_DGRAM queue.
	select {
	case err := <-doneCh:
		t.Fatalf("expected writer to block while receiver is SIGSTOPed (count=%d, max_dgram_qlen=%d), but finished early with err=%v",
			totalCount, qlen, err)
	case <-time.After(1 * time.Second):
	}

	// Send SIGCONT and verify the writer unblocks and finishes.
	tc.mustExec("kill", "-CONT", strconv.Itoa(recv.PID))
	select {
	case err := <-doneCh:
		if err != nil {
			t.Fatalf("writer failed after SIGCONT: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("writer remained blocked after SIGCONT")
	}

	// Verify all records were delivered to container logs.
	deadline := time.Now().Add(15 * time.Second)
	for {
		logs, _ := tc.logs()
		seen := make([]bool, totalCount+1)
		matched := 0
		for _, m := range stopSeqRe.FindAllStringSubmatch(logs, -1) {
			n, _ := strconv.Atoi(m[1])
			if n >= 1 && n <= totalCount && !seen[n] {
				seen[n] = true
				matched++
			}
		}
		if matched == totalCount {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected %d STOP-BLOCK records in logs after SIGCONT, got %d", totalCount, matched)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestLogsStoppedIndexerBoundedMemoryAndSpoolCap(t *testing.T) {
	env := loadTestEnv(t)

	const segBytes = 65536
	const maxBytes = 262144 // 256 KiB

	conf := defaultConfOptions()
	conf.SegmentSizeBytes = segBytes
	conf.MaxTotalBytes = maxBytes
	conf.QueueSync = "5s"

	tc := startContainer(t, env, containerOpts{
		Conf:      conf,
		Relayhost: "[192.0.2.1]:25",
	})

	smtpAddr := tc.hostPort("25")
	api := newAPIClient(tc.hostPort("8025"))
	api.waitForHealth(t, adminToken, "OPERATIONAL", 45*time.Second)

	// Send a warmup email first so spool_cursor.segment advances to seg-000...001.log.
	warmQID := sendSMTP(t, smtpAddr, "warm@example.com", "w@blackhole.test",
		"Warmup Cursor Anchor", fmt.Sprintf("<warm-%d@example.com>", time.Now().UnixNano()), "Warmup\n")
	api.waitForMessage(t, warmQID, adminToken, 30*time.Second, func(m apiMessage) bool {
		return m.QueueState == "deferred"
	})

	apiProc := tc.waitProcByRole("api", 0, 10*time.Second)
	recvProc := tc.waitProcByRole("receiver", 0, 10*time.Second)
	queueDir := tc.postconfGet("queue_directory")
	dataDir := tc.postconfGet("data_directory")
	sockPath := filepath.Join(queueDir, "public", "postlog")
	spoolDir := filepath.Join(dataDir, "postfix-api", "spool")

	// SIGSTOP the API/indexer process.
	tc.mustExec("kill", "-STOP", strconv.Itoa(apiProc.PID))
	t.Cleanup(func() {
		_, _, _ = tc.exec("kill", "-CONT", strconv.Itoa(apiProc.PID))
	})

	// Send an email while API/indexer is stopped; mail flow must not stall.
	stoppedQID := sendSMTP(t, smtpAddr, "stopped@example.com", "s@blackhole.test",
		"Mail While Indexer Stopped", fmt.Sprintf("<stopped-%d@example.com>", time.Now().UnixNano()), "Unblocked mail\n")
	qDeadline := time.Now().Add(15 * time.Second)
	for {
		pqOut := tc.mustExec("postqueue", "-j")
		if strings.Contains(pqOut, stoppedQID) {
			break
		}
		if time.Now().After(qDeadline) {
			t.Fatalf("mail %s stalled when indexer was SIGSTOPed; postqueue -j=%s", stoppedQID, pqOut)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Flood > 2 MiB of log datagrams (16,000 * ~160B ≈ 2.56 MiB) through public/postlog.
	const floodCount = 16000
	_, stderr, err := tc.exec(
		"/usr/local/bin/testhelper", "flood",
		"--socket", sockPath,
		"--count", strconv.Itoa(floodCount),
		"--prefix", "CAP-SEQ-",
		"--width", "5",
		"--pad-bytes", "120",
	)
	if err != nil {
		t.Fatalf("cap flood failed: %v (%s)", err, stderr)
	}

	// Check receiver RSS < 32 MiB (32768 kB).
	rssKB := tc.procRSSKB(recvProc.PID)
	if rssKB >= 32*1024 {
		t.Fatalf("log receiver RSS too high under spool flood: %d kB (>= 32768 kB)", rssKB)
	}

	// Check total spool segment bytes <= max_total_bytes + 1 segment overhead.
	sizeOut := tc.mustExec("sh", "-c", fmt.Sprintf(
		"total=0; for f in %q/seg-*.log; do [ -e \"$f\" ] || continue; sz=$(stat -c %%s \"$f\"); total=$((total + sz)); done; echo \"$total\"",
		spoolDir,
	))
	spoolTotalBytes, err := strconv.ParseInt(strings.TrimSpace(sizeOut), 10, 64)
	if err != nil {
		t.Fatalf("parse spool total bytes %q: %v", sizeOut, err)
	}
	maxAllowed := int64(maxBytes + segBytes)
	if spoolTotalBytes > maxAllowed {
		t.Fatalf("spool size %d bytes exceeded cap %d (max_total_bytes=%d + segment=%d)",
			spoolTotalBytes, maxAllowed, maxBytes, segBytes)
	}

	gaps := tc.readCounter(filepath.Join(spoolDir, "gaps.count"))
	if gaps == 0 {
		t.Fatalf("expected gaps.count > 0 after >2 MiB flood on %d-byte spool cap", maxBytes)
	}

	// Resume API/indexer via SIGCONT.
	tc.mustExec("kill", "-CONT", strconv.Itoa(apiProc.PID))

	h := api.waitForHealth(t, adminToken, "DEGRADED", 20*time.Second)
	if h.SpoolGaps == 0 {
		t.Fatalf("expected /v1/health spool_gaps > 0, got %+v", h)
	}
	if !h.RootHelperAlive {
		t.Fatalf("expected root_helper_alive=true after resume, got %+v", h)
	}

	// Indexer must resume without crashing and reconcile live queue via postqueue -j.
	reconciled := api.waitForMessage(t, stoppedQID, adminToken, 20*time.Second, func(m apiMessage) bool {
		return m.QueueID == stoppedQID
	})
	if reconciled.QueueID != stoppedQID {
		t.Fatalf("expected reconciled message %s after resume", stoppedQID)
	}

	// Send another email after SIGCONT to verify spool ingestion actively resumes end-to-end.
	resumedQID := sendSMTP(t, smtpAddr, "resumed@example.com", "r@blackhole.test",
		"Post-Resume Indexer Check", fmt.Sprintf("<resumed-%d@example.com>", time.Now().UnixNano()), "Resumed\n")
	resumedMsg := api.waitForMessage(t, resumedQID, adminToken, 30*time.Second, func(m apiMessage) bool {
		return m.QueueState == "deferred" && m.Subject == "Post-Resume Indexer Check"
	})
	if resumedMsg.Subject != "Post-Resume Indexer Check" {
		t.Fatalf("unexpected post-resume message: %+v", resumedMsg)
	}
}

func TestBokyDropInCompatibility(t *testing.T) {
	env := loadTestEnv(t)
	if env.target != "boky" {
		t.Skipf("skipping boky drop-in test for target %q", env.target)
	}

	// Mount a custom /docker-init.db/10-custom.sh hook to verify boky init script chaining.
	tmpDir := t.TempDir()
	_ = os.Chmod(tmpDir, 0o755)
	customInit := filepath.Join(tmpDir, "10-custom.sh")
	if err := os.WriteFile(customInit, []byte("#!/bin/sh\ntouch /tmp/custom-init-ran\n"), 0o755); err != nil {
		t.Fatalf("write custom init hook: %v", err)
	}

	tc := startContainer(t, env, containerOpts{
		KeepEntrypoint: true,
		ExposePorts:    []string{"587", "8025"},
		Mounts: []string{
			customInit + ":/docker-init.db/10-custom.sh:ro",
		},
		Env: map[string]string{
			"HOSTNAME":                             "hide-mail.test.local",
			"ALLOWED_SENDER_DOMAINS":               "test.local",
			"POSTFIX_minimal_backoff_time":         "60s",
			"POSTFIX_message_size_limit":           "26214400",
			"POSTFIX_relayhost":                    "[192.0.2.1]:25",
			"POSTFIX_smtp_connect_timeout":         "2s",
			"POSTFIX_smtpd_recipient_restrictions": "permit_mynetworks,reject_unauth_destination",
			"DKIM_AUTOGENERATE":                    "true",
			"POSTFIXAPI_TOKEN":                     "test-boky-secret",
			"POSTFIXAPI_ALLOW_PLAINTEXT":           "true",
		},
	})

	const bokyToken = "test-boky-secret"
	api := newAPIClient(tc.hostPort("8025"))
	h := api.waitForHealth(t, bokyToken, "OPERATIONAL", 60*time.Second)
	if !h.RootHelperAlive || h.Status != "OPERATIONAL" {
		t.Fatalf("unexpected boky health: %+v", h)
	}

	if got := tc.postconfGet("myhostname"); got != "hide-mail.test.local" {
		t.Fatalf("myhostname = %q, want %q", got, "hide-mail.test.local")
	}
	if got := tc.postconfGet("minimal_backoff_time"); got != "60s" {
		t.Fatalf("minimal_backoff_time = %q, want %q", got, "60s")
	}
	if got := tc.postconfGet("message_size_limit"); got != "26214400" {
		t.Fatalf("message_size_limit = %q, want %q", got, "26214400")
	}
	tc.mustExec("test", "-f", "/tmp/custom-init-ran")

	// Raw POSTFIX_API_TOKEN must never be persisted verbatim in postfix-api.conf.
	confRaw, err := tc.readFile("/etc/postfix/postfix-api.conf")
	if err != nil {
		t.Fatalf("read /etc/postfix/postfix-api.conf: %v", err)
	}
	if strings.Contains(confRaw, bokyToken) {
		t.Fatalf("raw POSTFIX_API_TOKEN leaked into postfix-api.conf")
	}

	// Send an email via port 587 (or fallback to sendmail if host bridge isn't permitted by custom setup).
	const bokySubject = "Boky Drop-In Secret Subject"
	msgID := fmt.Sprintf("<boky-%d@test.local>", time.Now().UnixNano())
	smtp587 := tc.hostPort("587")
	qid := sendSMTP(t, smtp587, "bot@test.local", "dest@blackhole.test", bokySubject, msgID, "Boky body\n")

	// Verify GET /v1/messages and GET /v1/messages/{qid} using Bearer test-boky-secret.
	msg := api.waitForMessage(t, qid, bokyToken, 30*time.Second, func(m apiMessage) bool {
		return m.QueueState == "deferred" && m.Subject != "" && len(m.Recipients) > 0 && len(m.Events) >= 2
	})
	if msg.Subject != bokySubject {
		t.Fatalf("boky message subject = %q, want %q", msg.Subject, bokySubject)
	}
	if len(msg.Recipients) == 0 || msg.Recipients[0].Address != "dest@blackhole.test" {
		t.Fatalf("unexpected boky recipients: %+v", msg.Recipients)
	}
	assertEventSequence(t, msg.Events, []string{"processed", "deferred"})

	var listResp apiMessagesResp
	if st := api.doJSON(t, http.MethodGet, "/v1/messages", bokyToken, nil, &listResp); st != http.StatusOK || len(listResp.Messages) == 0 {
		t.Fatalf("GET /v1/messages failed on boky: status=%d resp=%+v", st, listResp)
	}

	// Verify container stdout receives normal Postfix logs and no redacted Subject lines.
	logDeadline := time.Now().Add(15 * time.Second)
	for {
		logs, _ := tc.logs()
		if strings.Contains(logs, qid) && strings.Contains(logs, "qmgr[") && strings.Contains(logs, "smtp[") {
			if strings.Contains(logs, bokySubject) || strings.Contains(logs, ": info: header Subject: ") {
				t.Fatalf("redacted Subject line leaked to boky container stdout:\n%s", logs)
			}
			break
		}
		if time.Now().After(logDeadline) {
			t.Fatalf("expected normal Postfix logs for %s in boky stdout:\n%s", qid, logs)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// --- Test Utilities ---

func normalizeWS(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func assertEventSequence(t *testing.T, events []apiEvent, wantOrder []string) {
	t.Helper()
	idx := 0
	for _, ev := range events {
		if idx < len(wantOrder) && ev.Event == wantOrder[idx] {
			idx++
		}
	}
	if idx != len(wantOrder) {
		t.Fatalf("expected event subsequence %v, got %+v", wantOrder, events)
	}
}

func hasEvent(events []apiEvent, name string) bool {
	for _, ev := range events {
		if ev.Event == name {
			return true
		}
	}
	return false
}

func eventsContainForQueue(events []apiEvent, qid, name string) bool {
	for _, ev := range events {
		if ev.QueueID == qid && ev.Event == name {
			return true
		}
	}
	return false
}

func queueContains(q apiQueueResp, qid string) bool {
	for _, e := range q.Entries {
		if e.QueueID == qid {
			return true
		}
	}
	return false
}
