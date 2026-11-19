package postfixcmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidateQueueID(t *testing.T) {
	valid := []string{
		"74D321034",
		"C23D82B60024",
		"4jV74M63RlzHxK3",
		"abcdef",
		"12345678901234567890123456789012", // 32 chars
	}
	for _, id := range valid {
		if err := ValidateQueueID(id); err != nil {
			t.Errorf("ValidateQueueID(%q) unexpected error: %v", id, err)
		}
	}

	invalid := []string{
		"",
		"ALL",
		"12345",                             // 5 chars (<6)
		"123456789012345678901234567890123", // 33 chars (>32)
		"-d",
		"-h123456",
		"--all",
		"../etc/passwd",
		"4jV74M/63Rl",
		"4jV74M\\63Rl",
		"4jV74M 63Rl",
		"4jV74M\t63Rl",
		"4jV74M\n63Rl",
		"4jV74M;id",
		"4jV74M_63Rl",
		"4jV74M.63Rl",
	}
	for _, id := range invalid {
		err := ValidateQueueID(id)
		if !errors.Is(err, ErrInvalidQueueID) {
			t.Errorf("ValidateQueueID(%q) = %v, want ErrInvalidQueueID", id, err)
		}
	}
}

func TestParsePostqueueJSON(t *testing.T) {
	sample := strings.Join([]string{
		`{"queue_name":"deferred","queue_id":"4jV74M63RlzHxK3","arrival_time":1763484883,"message_size":310,"forced_expire":false,"sender":"a@example.com","recipients":[{"address":"b@example.org","orig_address":"orig@example.org","delay_reason":"Connection timed out"}]}`,
		`{"queue_name":"hold","queue_id":"C23D82B60024","arrival_time":1763484900,"message_size":1024,"forced_expire":true,"sender":"c@example.com","recipients":[{"address":"d@example.org"}]}`,
		``,
	}, "\n")

	entries, counts, err := ParsePostqueueJSON(strings.NewReader(sample))
	if err != nil {
		t.Fatalf("ParsePostqueueJSON error: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("len(entries) = %d, want 2", len(entries))
	}
	if entries[0].QueueID != "4jV74M63RlzHxK3" || entries[0].QueueName != "deferred" || entries[0].MessageSize != 310 {
		t.Errorf("unexpected entry[0]: %+v", entries[0])
	}
	if len(entries[0].Recipients) != 1 || entries[0].Recipients[0].DelayReason != "Connection timed out" || entries[0].Recipients[0].OrigAddress != "orig@example.org" {
		t.Errorf("unexpected entry[0].Recipients: %+v", entries[0].Recipients)
	}
	if !entries[1].ForcedExpire || entries[1].QueueName != "hold" {
		t.Errorf("unexpected entry[1]: %+v", entries[1])
	}
	if counts["deferred"] != 1 || counts["hold"] != 1 || counts["active"] != 0 || counts["incoming"] != 0 || counts["maildrop"] != 0 {
		t.Errorf("unexpected counts: %+v", counts)
	}

	// Empty queue
	emptyEntries, emptyCounts, err := ParsePostqueueJSON(strings.NewReader(""))
	if err != nil {
		t.Fatalf("empty ParsePostqueueJSON error: %v", err)
	}
	if emptyEntries == nil || len(emptyEntries) != 0 {
		t.Errorf("expected non-nil empty slice, got %#v", emptyEntries)
	}
	if emptyCounts["deferred"] != 0 {
		t.Errorf("expected 0 deferred, got %+v", emptyCounts)
	}

	// Invalid JSON
	if _, _, err := ParsePostqueueJSON(strings.NewReader("{bad json\n")); err == nil {
		t.Error("expected error on invalid JSON")
	}
}

func TestSplitHeadersAndBody(t *testing.T) {
	tests := []struct {
		name        string
		raw         string
		wantHeaders string
		wantBody    string
	}{
		{
			name:        "LF separator",
			raw:         "From: a@example.com\nSubject: Hi\n\nHello\n\nSecond paragraph\n",
			wantHeaders: "From: a@example.com\nSubject: Hi",
			wantBody:    "Hello\n\nSecond paragraph\n",
		},
		{
			name:        "CRLF separator",
			raw:         "From: a@example.com\r\nSubject: Hi\r\n\r\nHello\r\n\r\nWorld",
			wantHeaders: "From: a@example.com\r\nSubject: Hi",
			wantBody:    "Hello\r\n\r\nWorld",
		},
		{
			name:        "headers only",
			raw:         "From: a@example.com\nTo: b@example.org",
			wantHeaders: "From: a@example.com\nTo: b@example.org",
			wantBody:    "",
		},
		{
			name:        "empty body after separator",
			raw:         "From: a@example.com\n\n",
			wantHeaders: "From: a@example.com",
			wantBody:    "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, b := SplitHeadersAndBody(tc.raw)
			if h != tc.wantHeaders {
				t.Errorf("headers = %q, want %q", h, tc.wantHeaders)
			}
			if b != tc.wantBody {
				t.Errorf("body = %q, want %q", b, tc.wantBody)
			}
		})
	}
}

func TestNextRetryAt(t *testing.T) {
	queueDir := t.TempDir()
	runner := NewRunner("", queueDir, nil)

	// 1. Primary hashed path: deferred/<first-char>/<queue_id>
	qid1 := "4jV74M63RlzHxK3"
	hashedDir := filepath.Join(queueDir, "deferred", "4")
	if err := os.MkdirAll(hashedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	f1 := filepath.Join(hashedDir, qid1)
	if err := os.WriteFile(f1, []byte("queued"), 0o600); err != nil {
		t.Fatal(err)
	}
	want1 := time.Date(2026, 11, 18, 16, 59, 45, 0, time.UTC)
	if err := os.Chtimes(f1, want1, want1); err != nil {
		t.Fatal(err)
	}

	got1, err := runner.NextRetryAt(qid1)
	if err != nil {
		t.Fatalf("NextRetryAt(%q) error: %v", qid1, err)
	}
	if got1 == nil || !got1.Equal(want1) || got1.Location() != time.UTC {
		t.Errorf("NextRetryAt(%q) = %v, want %v UTC", qid1, got1, want1)
	}

	// 2. Fallback unhashed path: deferred/<queue_id>
	qid2 := "C23D82B60024"
	f2 := filepath.Join(queueDir, "deferred", qid2)
	if err := os.WriteFile(f2, []byte("queued"), 0o600); err != nil {
		t.Fatal(err)
	}
	want2 := time.Date(2026, 11, 19, 8, 15, 0, 0, time.UTC)
	if err := os.Chtimes(f2, want2, want2); err != nil {
		t.Fatal(err)
	}

	got2, err := runner.NextRetryAt(qid2)
	if err != nil {
		t.Fatalf("NextRetryAt(%q) error: %v", qid2, err)
	}
	if got2 == nil || !got2.Equal(want2) {
		t.Errorf("NextRetryAt(%q) = %v, want %v", qid2, got2, want2)
	}

	// 3. Not found returns (nil, nil)
	got3, err := runner.NextRetryAt("ABCDEF123456")
	if err != nil {
		t.Fatalf("NextRetryAt(missing) error: %v", err)
	}
	if got3 != nil {
		t.Errorf("NextRetryAt(missing) = %v, want nil", got3)
	}

	// 4. Invalid queue ID rejected
	if _, err := runner.NextRetryAt("../bad"); !errors.Is(err, ErrInvalidQueueID) {
		t.Errorf("NextRetryAt(../bad) = %v, want ErrInvalidQueueID", err)
	}
}

func TestRunnerCommandsWithMockScripts(t *testing.T) {
	cmdDir := t.TempDir()

	// Mock postqueue
	postqueueScript := `#!/bin/sh
case "$1" in
  -j)
    printf '{"queue_name":"deferred","queue_id":"4jV74M63RlzHxK3","arrival_time":1763484883,"message_size":123,"forced_expire":false,"sender":"s@e.com","recipients":[{"address":"r@e.com"}]}\n'
    ;;
  -i)
    printf 'flushed %s\n' "$2"
    ;;
  -f)
    printf 'flushed all\n'
    ;;
  *)
    exit 2
    ;;
esac
`
	writeMockScript(t, filepath.Join(cmdDir, "postqueue"), postqueueScript)

	// Mock postcat
	postcatScript := `#!/bin/sh
if [ "$1" = "-hbq" ] && [ "$2" = "4jV74M63RlzHxK3" ]; then
  printf 'From: s@e.com\nSubject: Test\n\nBody line 1\n'
  exit 0
fi
printf 'postcat: fatal: open queue file %s: No such file or directory\n' "$2" >&2
exit 1
`
	writeMockScript(t, filepath.Join(cmdDir, "postcat"), postcatScript)

	runner := NewRunner(cmdDir, t.TempDir(), nil)
	ctx := context.Background()

	// ListQueue
	entries, counts, err := runner.ListQueue(ctx)
	if err != nil {
		t.Fatalf("ListQueue error: %v", err)
	}
	if len(entries) != 1 || counts["deferred"] != 1 {
		t.Fatalf("ListQueue unexpected result: entries=%+v counts=%+v", entries, counts)
	}

	// ReadContent (found)
	h, b, err := runner.ReadContent(ctx, "4jV74M63RlzHxK3")
	if err != nil {
		t.Fatalf("ReadContent error: %v", err)
	}
	if h != "From: s@e.com\nSubject: Test" || b != "Body line 1\n" {
		t.Errorf("ReadContent got headers=%q body=%q", h, b)
	}

	// ReadContent (not found)
	_, _, err = runner.ReadContent(ctx, "MISSING123456")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("ReadContent(missing) = %v, want ErrNotFound", err)
	}

	// ReadContent (invalid queue ID)
	_, _, err = runner.ReadContent(ctx, "ALL")
	if !errors.Is(err, ErrInvalidQueueID) {
		t.Errorf("ReadContent(ALL) = %v, want ErrInvalidQueueID", err)
	}

	// FlushMessage
	res, err := runner.FlushMessage(ctx, "4jV74M63RlzHxK3")
	if err != nil || res.ExitCode != 0 || strings.TrimSpace(res.Stdout) != "flushed 4jV74M63RlzHxK3" {
		t.Errorf("FlushMessage got res=%+v err=%v", res, err)
	}

	// FlushQueue
	res, err = runner.FlushQueue(ctx)
	if err != nil || res.ExitCode != 0 || strings.TrimSpace(res.Stdout) != "flushed all" {
		t.Errorf("FlushQueue got res=%+v err=%v", res, err)
	}
}

func TestRunRootHelperRoundTrip(t *testing.T) {
	cmdDir := t.TempDir()

	// Mock postsuper script: prints args and stdin (when $2 == "-") to stdout,
	// exits 1 if queue ID is FAIL0001.
	postsuperScript := `#!/bin/sh
flag="$1"
target="$2"
if [ "$target" = "-" ]; then
  ids=$(tr '\n' ',' | sed 's/,$//')
  printf 'bulk:%s:%s\n' "$flag" "$ids"
  exit 0
fi
if [ "$target" = "FAIL0001" ]; then
  printf 'postsuper: fatal: failed on %s\n' "$target" >&2
  exit 1
fi
printf 'single:%s:%s\n' "$flag" "$target"
`
	writeMockScript(t, filepath.Join(cmdDir, "postsuper"), postsuperScript)

	// Build a 1000-ID bulk request as well
	bulk1000 := make([]string, 1000)
	for i := range bulk1000 {
		bulk1000[i] = fmt.Sprintf("QID%06d", i)
	}
	bulk1001 := append(append([]string(nil), bulk1000...), "QID001000")

	reqs := []string{
		mustJSON(t, HelperRequest{Action: "hold", QueueIDs: []string{"4jV74M63RlzHxK3"}}),
		mustJSON(t, HelperRequest{Action: "release", QueueIDs: []string{"4jV74M63RlzHxK3", "C23D82B60024"}}),
		mustJSON(t, HelperRequest{Action: "requeue", QueueIDs: []string{"74D321034"}}),
		mustJSON(t, HelperRequest{Action: "expire", QueueIDs: []string{"74D321034"}}),
		mustJSON(t, HelperRequest{Action: "delete", QueueIDs: bulk1000}),
		mustJSON(t, HelperRequest{Action: "delete", QueueIDs: []string{"FAIL0001"}}),
		mustJSON(t, HelperRequest{Action: "flush", QueueIDs: []string{"74D321034"}}), // invalid action
		mustJSON(t, HelperRequest{Action: "delete", QueueIDs: []string{"ALL"}}),      // invalid queue ID
		mustJSON(t, HelperRequest{Action: "delete", QueueIDs: []string{}}),           // empty queue IDs
		mustJSON(t, HelperRequest{Action: "delete", QueueIDs: bulk1001}),             // >1000 queue IDs
		`{not valid json`, // malformed JSON
		mustJSON(t, HelperRequest{Action: "hold", QueueIDs: []string{"AFTERERR01"}}), // recovery after error
	}

	in := strings.NewReader(strings.Join(reqs, "\n") + "\n")
	var out bytes.Buffer

	if err := RunRootHelper(cmdDir, in, &out); err != nil {
		t.Fatalf("RunRootHelper error: %v", err)
	}

	var resps []HelperResponse
	scanner := bufio.NewScanner(&out)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		var r HelperResponse
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			t.Fatalf("unmarshal response %q: %v", scanner.Text(), err)
		}
		resps = append(resps, r)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan responses: %v", err)
	}
	if len(resps) != len(reqs) {
		t.Fatalf("got %d responses, want %d", len(resps), len(reqs))
	}

	// 0: single hold
	if resps[0].ExitCode != 0 || resps[0].Error != "" || strings.TrimSpace(resps[0].Stdout) != "single:-h:4jV74M63RlzHxK3" {
		t.Errorf("resp[0] = %+v", resps[0])
	}
	// 1: bulk release (2 IDs)
	if resps[1].ExitCode != 0 || resps[1].Error != "" || strings.TrimSpace(resps[1].Stdout) != "bulk:-H:4jV74M63RlzHxK3,C23D82B60024" {
		t.Errorf("resp[1] = %+v", resps[1])
	}
	// 2: single requeue
	if resps[2].ExitCode != 0 || strings.TrimSpace(resps[2].Stdout) != "single:-r:74D321034" {
		t.Errorf("resp[2] = %+v", resps[2])
	}
	// 3: single expire
	if resps[3].ExitCode != 0 || strings.TrimSpace(resps[3].Stdout) != "single:-e:74D321034" {
		t.Errorf("resp[3] = %+v", resps[3])
	}
	// 4: bulk 1000 delete
	if resps[4].ExitCode != 0 || !strings.HasPrefix(resps[4].Stdout, "bulk:-d:QID000000,") || !strings.HasSuffix(strings.TrimSpace(resps[4].Stdout), ",QID000999") {
		t.Errorf("resp[4] = %+v", resps[4])
	}
	// 5: non-zero exit code from postsuper
	if resps[5].ExitCode != 1 || resps[5].Error != "" || !strings.Contains(resps[5].Stderr, "failed on FAIL0001") {
		t.Errorf("resp[5] = %+v", resps[5])
	}
	// 6..10: validation & JSON errors
	for i := 6; i <= 10; i++ {
		if resps[i].ExitCode != -1 || resps[i].Error == "" {
			t.Errorf("resp[%d] expected error, got %+v", i, resps[i])
		}
	}
	// 11: recovery after errors
	if resps[11].ExitCode != 0 || resps[11].Error != "" || strings.TrimSpace(resps[11].Stdout) != "single:-h:AFTERERR01" {
		t.Errorf("resp[11] = %+v", resps[11])
	}
}

func writeMockScript(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatalf("write mock script %s: %v", path, err)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
