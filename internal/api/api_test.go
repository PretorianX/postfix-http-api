package api

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PretorianX/postfix-http-api/internal/config"
	"github.com/PretorianX/postfix-http-api/internal/indexer"
	"github.com/PretorianX/postfix-http-api/internal/parser"
	"github.com/PretorianX/postfix-http-api/internal/postfixcmd"
	"github.com/PretorianX/postfix-http-api/internal/spool"
	"github.com/PretorianX/postfix-http-api/internal/store"
)

func TestMain(m *testing.M) {
	if len(os.Args) >= 4 && os.Args[1] == "_root-helper" && os.Args[2] == "--command-dir" {
		if err := postfixcmd.RunRootHelper(os.Args[3], os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type recordingAuditLogger struct {
	mu     sync.Mutex
	events []AuditEvent
}

func (l *recordingAuditLogger) Log(ev AuditEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, ev)
}

func (l *recordingAuditLogger) Events() []AuditEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]AuditEvent, len(l.events))
	copy(out, l.events)
	return out
}

type stubSpoolSource struct {
	mu    sync.Mutex
	stats indexer.SpoolStats
}

func (s *stubSpoolSource) ReadBatch(_ context.Context, cursor store.SpoolCursor, _ int) (indexer.SpoolBatch, error) {
	return indexer.SpoolBatch{NextCursor: cursor}, nil
}

func (s *stubSpoolSource) DeleteSegmentsBefore(_ string) error { return nil }

func (s *stubSpoolSource) Stats() (indexer.SpoolStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats, nil
}

func (s *stubSpoolSource) SetStats(st indexer.SpoolStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats = st
}

type stubQueueLister struct {
	mu      sync.Mutex
	entries []store.QueueSnapshotEntry
	err     error
}

func (l *stubQueueLister) ListQueue(_ context.Context) ([]store.QueueSnapshotEntry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return nil, l.err
	}
	cp := make([]store.QueueSnapshotEntry, len(l.entries))
	copy(cp, l.entries)
	return cp, nil
}

func (l *stubQueueLister) SetError(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.err = err
}

func sha256Hex(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func writeExecScript(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script %s: %v", path, err)
	}
}

type testEnv struct {
	cfg      *config.Config
	st       *store.Store
	idx      *indexer.Indexer
	spoolSrc *stubSpoolSource
	lister   *stubQueueLister
	runner   *postfixcmd.Runner
	helper   *postfixcmd.RootHelperClient
	audit    *recordingAuditLogger
	auditBuf *bytes.Buffer
	cmdDir   string
	queueDir string
	srv      *Server
	now      time.Time
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()

	cmdDir := t.TempDir()
	queueDir := t.TempDir()
	baseNow := time.Date(2026, 11, 19, 8, 30, 0, 0, time.UTC)

	writeExecScript(t, filepath.Join(cmdDir, "postsuper"), `#!/bin/sh
flag="$1"
target="$2"
if [ "$target" = "-" ]; then
  ids=$(tr '\n' ',' | sed 's/,$//')
  if echo "$ids" | grep -q "FAIL0001"; then
    printf 'postsuper: fatal: bulk fail\n' >&2
    exit 1
  fi
  printf 'postsuper %s bulk %s\n' "$flag" "$ids"
  exit 0
fi
if [ "$target" = "FAIL0001" ]; then
  printf 'postsuper: fatal: not found %s\n' "$target" >&2
  exit 1
fi
printf 'postsuper %s %s\n' "$flag" "$target"
`)

	writeExecScript(t, filepath.Join(cmdDir, "postqueue"), `#!/bin/sh
case "$1" in
  -j)
    if [ -f "`+cmdDir+`/fail_postqueue" ]; then
      printf 'postqueue: fatal: queue down\n' >&2
      exit 1
    fi
    printf '{"queue_name":"deferred","queue_id":"4jV74M63RlzHxK3","arrival_time":1763484883,"message_size":310,"forced_expire":false,"sender":"a@example.com","recipients":[{"address":"b@example.org","orig_address":"b@example.org","delay_reason":"Connection timed out"}]}\n'
    printf '{"queue_name":"hold","queue_id":"C23D82B60024","arrival_time":1763484900,"message_size":512,"forced_expire":false,"sender":"c@example.com","recipients":[{"address":"d@example.org"}]}\n'
    ;;
  -i)
    if [ "$2" = "FAIL0001" ]; then
      printf 'postqueue: fatal: invalid queue id %s\n' "$2" >&2
      exit 1
    fi
    printf 'postqueue -i %s\n' "$2"
    ;;
  -f)
    if [ -f "`+cmdDir+`/fail_flush" ]; then
      printf 'postqueue: fatal: cannot flush\n' >&2
      exit 1
    fi
    printf 'postqueue -f ok\n'
    ;;
  *)
    exit 2
    ;;
esac
`)

	writeExecScript(t, filepath.Join(cmdDir, "postcat"), `#!/bin/sh
if [ "$1" = "-hbq" ] && [ "$2" = "4jV74M63RlzHxK3" ]; then
  printf 'From: a@example.com\nTo: b@example.org\nSubject: Test\n\nHello world\n'
  exit 0
fi
if [ "$1" = "-hbq" ] && [ "$2" = "BINARY0001" ]; then
  printf 'From: a@example.com\nContent-Type: application/octet-stream\n\n\377\376\000\001'
  exit 0
fi
printf 'postcat: fatal: open queue file %s: No such file or directory\n' "$2" >&2
exit 1
`)

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	helper, err := postfixcmd.StartRootHelper(exe, cmdDir)
	if err != nil {
		t.Fatalf("StartRootHelper: %v", err)
	}
	t.Cleanup(func() { _ = helper.Close() })

	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := config.DefaultConfig()
	cfg.Tokens = []config.Token{
		{Name: "reader", SHA256: sha256Hex("tok-read"), Scopes: []string{config.ScopeRead}},
		{Name: "writer", SHA256: sha256Hex("tok-write"), Scopes: []string{config.ScopeWrite}},
		{Name: "content-viewer", SHA256: sha256Hex("tok-content"), Scopes: []string{config.ScopeContent}},
		{Name: "admin", SHA256: sha256Hex("tok-admin"), Scopes: []string{config.ScopeRead, config.ScopeWrite, config.ScopeContent}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg.Validate: %v", err)
	}

	spoolSrc := &stubSpoolSource{}
	lister := &stubQueueLister{}
	env := &testEnv{
		cfg:      cfg,
		st:       st,
		spoolSrc: spoolSrc,
		lister:   lister,
		cmdDir:   cmdDir,
		queueDir: queueDir,
		now:      baseNow,
		audit:    &recordingAuditLogger{},
		auditBuf: &bytes.Buffer{},
	}

	idx, err := indexer.New(st, spoolSrc, lister, indexer.Config{
		Now: func() time.Time { return env.now },
	})
	if err != nil {
		t.Fatalf("indexer.New: %v", err)
	}
	env.idx = idx
	env.helper = helper
	env.runner = postfixcmd.NewRunner(cmdDir, queueDir, helper)

	auditFanout := AuditLoggerFunc(func(ev AuditEvent) {
		env.audit.Log(ev)
		NewJSONAuditLogger(env.auditBuf).Log(ev)
	})

	srv, err := NewServer(
		cfg,
		st,
		idx,
		env.runner,
		queueDir,
		WithAuditLogger(auditFanout),
		WithPostfixVersion("3.11.7"),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv.now = func() time.Time { return env.now }
	env.srv = srv

	return env
}

func seedTestMessages(t *testing.T, st *store.Store) {
	t.Helper()
	ctx := context.Background()

	t1 := time.Date(2026, 11, 18, 16, 54, 43, 123456789, time.UTC)
	t2 := t1.Add(2 * time.Second)
	t3 := time.Date(2026, 11, 18, 17, 0, 0, 0, time.UTC)

	entries := []store.BatchEntry{
		{
			Timestamp: t1,
			Record: parser.ParsedRecord{
				Timestamp:     t1,
				Program:       "smtpd",
				QueueID:       "4jV74M63RlzHxK3",
				Kind:          parser.KindSMTPDClient,
				ClientName:    "localhost",
				ClientAddress: "127.0.0.1",
			},
		},
		{
			Timestamp: t1,
			Record: parser.ParsedRecord{
				Timestamp: t1,
				Program:   "cleanup",
				QueueID:   "4jV74M63RlzHxK3",
				Kind:      parser.KindMessageID,
				MessageID: "<4jV74M63RlzHxK3@localhost>",
			},
		},
		{
			Timestamp: t1,
			Record: parser.ParsedRecord{
				Timestamp: t1,
				Program:   "cleanup",
				QueueID:   "4jV74M63RlzHxK3",
				Kind:      parser.KindSubject,
				Subject:   "Привет order 42",
			},
		},
		{
			Timestamp: t1.Add(7 * time.Millisecond),
			Record: parser.ParsedRecord{
				Timestamp:      t1.Add(7 * time.Millisecond),
				Program:        "qmgr",
				QueueID:        "4jV74M63RlzHxK3",
				Kind:           parser.KindQmgrActive,
				Sender:         "a@example.com",
				HasSender:      true,
				Size:           310,
				RecipientCount: 1,
			},
		},
		{
			Timestamp: t2,
			Record: parser.ParsedRecord{
				Timestamp:    t2,
				Program:      "smtp",
				QueueID:      "4jV74M63RlzHxK3",
				Kind:         parser.KindDelivery,
				Recipient:    "b@example.org",
				Relay:        "none",
				DelaySeconds: 2.08,
				DSN:          "4.4.1",
				Status:       "deferred",
				Response:     "connect to 192.0.2.1[192.0.2.1]:25: Connection timed out",
			},
		},
		{
			Timestamp: t3,
			Record: parser.ParsedRecord{
				Timestamp:      t3,
				Program:        "qmgr",
				QueueID:        "C23D82B60024",
				Kind:           parser.KindQmgrActive,
				Sender:         "c@example.com",
				HasSender:      true,
				Size:           512,
				RecipientCount: 1,
			},
		},
		{
			Timestamp: t3.Add(time.Second),
			Record: parser.ParsedRecord{
				Timestamp:    t3.Add(time.Second),
				Program:      "smtp",
				QueueID:      "C23D82B60024",
				Kind:         parser.KindDelivery,
				Recipient:    "d@example.org",
				Relay:        "mx.example.org[198.51.100.1]:25",
				DelaySeconds: 0.45,
				DSN:          "2.0.0",
				Status:       "delivered",
				Response:     "250 2.0.0 Ok: queued",
			},
		},
		{
			Timestamp: t3.Add(2 * time.Second),
			Record: parser.ParsedRecord{
				Timestamp: t3.Add(2 * time.Second),
				Program:   "qmgr",
				QueueID:   "C23D82B60024",
				Kind:      parser.KindQmgrRemoved,
			},
		},
	}

	err := st.ApplyBatch(ctx, entries, store.SpoolCursor{
		Segment:      spool.SegmentName(1),
		Offset:       1024,
		LastRecordAt: &t3,
	})
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
}

func doReq(t *testing.T, h http.Handler, method, target, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	var bodyReader io.Reader
	if body != "" {
		bodyReader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, bodyReader)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func assertErrorEnvelope(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantCode string) {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, wantStatus, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var env struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("unmarshal error envelope %q: %v", rec.Body.String(), err)
	}
	if env.Error.Code != wantCode {
		t.Errorf("error.code = %q, want %q (body=%s)", env.Error.Code, wantCode, rec.Body.String())
	}
	if env.Error.Message == "" {
		t.Errorf("error.message is empty (body=%s)", rec.Body.String())
	}
}

func TestAuthAndScopeMatrix(t *testing.T) {
	env := newTestEnv(t)
	seedTestMessages(t, env.st)

	// 1. Missing/invalid/empty tokens -> 401
	t.Run("401_cases", func(t *testing.T) {
		rec := doReq(t, env.srv, http.MethodGet, "/v1/health", "", "")
		assertErrorEnvelope(t, rec, http.StatusUnauthorized, "unauthorized")

		rec = doReq(t, env.srv, http.MethodGet, "/v1/health", "wrong-token", "")
		assertErrorEnvelope(t, rec, http.StatusUnauthorized, "unauthorized")

		req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
		req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
		rec = httptest.NewRecorder()
		env.srv.ServeHTTP(rec, req)
		assertErrorEnvelope(t, rec, http.StatusUnauthorized, "unauthorized")

		// Multiple Authorization headers rejected
		req = httptest.NewRequest(http.MethodGet, "/v1/health", nil)
		req.Header.Add("Authorization", "Bearer tok-read")
		req.Header.Add("Authorization", "Bearer tok-read")
		rec = httptest.NewRecorder()
		env.srv.ServeHTTP(rec, req)
		assertErrorEnvelope(t, rec, http.StatusUnauthorized, "unauthorized")

		// Empty tokens config rejects everything
		emptyCfg := config.DefaultConfig()
		emptySrv, err := NewServer(emptyCfg, env.st, env.idx, env.runner, env.queueDir)
		if err != nil {
			t.Fatalf("NewServer empty tokens: %v", err)
		}
		rec = doReq(t, emptySrv, http.MethodGet, "/v1/health", "tok-admin", "")
		assertErrorEnvelope(t, rec, http.StatusUnauthorized, "unauthorized")

		// Case-insensitive "bearer" prefix + trimmed spaces accepted
		req = httptest.NewRequest(http.MethodGet, "/v1/health", nil)
		req.Header.Set("Authorization", "  bearer   tok-read  ")
		rec = httptest.NewRecorder()
		env.srv.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("case-insensitive bearer got %d: %s", rec.Code, rec.Body.String())
		}
	})

	// 2. Scope matrix across all endpoints
	endpoints := []struct {
		name       string
		method     string
		path       string
		body       string
		wantScope  string
		wantStatus int
	}{
		{"list_messages", http.MethodGet, "/v1/messages", "", config.ScopeRead, http.StatusOK},
		{"get_message", http.MethodGet, "/v1/messages/4jV74M63RlzHxK3", "", config.ScopeRead, http.StatusOK},
		{"get_content", http.MethodGet, "/v1/messages/4jV74M63RlzHxK3/content", "", config.ScopeContent, http.StatusOK},
		{"list_events", http.MethodGet, "/v1/events", "", config.ScopeRead, http.StatusOK},
		{"list_queue", http.MethodGet, "/v1/queue", "", config.ScopeRead, http.StatusOK},
		{"health", http.MethodGet, "/v1/health", "", config.ScopeRead, http.StatusOK},
		{"hold", http.MethodPost, "/v1/messages/4jV74M63RlzHxK3/hold", "", config.ScopeWrite, http.StatusOK},
		{"release", http.MethodPost, "/v1/messages/4jV74M63RlzHxK3/release", "", config.ScopeWrite, http.StatusOK},
		{"requeue", http.MethodPost, "/v1/messages/4jV74M63RlzHxK3/requeue", "", config.ScopeWrite, http.StatusOK},
		{"expire", http.MethodPost, "/v1/messages/4jV74M63RlzHxK3/expire", "", config.ScopeWrite, http.StatusOK},
		{"flush_msg", http.MethodPost, "/v1/messages/4jV74M63RlzHxK3/flush", "", config.ScopeWrite, http.StatusOK},
		{"delete_msg", http.MethodDelete, "/v1/messages/4jV74M63RlzHxK3", "", config.ScopeWrite, http.StatusOK},
		{"bulk_actions", http.MethodPost, "/v1/queue/actions", `{"action":"hold","queue_ids":["4jV74M63RlzHxK3"]}`, config.ScopeWrite, http.StatusOK},
		{"flush_queue", http.MethodPost, "/v1/queue/flush", "", config.ScopeWrite, http.StatusOK},
	}

	tokensByScope := map[string]string{
		config.ScopeRead:    "tok-read",
		config.ScopeWrite:   "tok-write",
		config.ScopeContent: "tok-content",
	}

	for _, ep := range endpoints {
		for scope, tok := range tokensByScope {
			t.Run(ep.name+"_with_"+scope, func(t *testing.T) {
				rec := doReq(t, env.srv, ep.method, ep.path, tok, ep.body)
				if scope == ep.wantScope {
					if rec.Code != ep.wantStatus {
						t.Fatalf("got %d, want %d; body=%s", rec.Code, ep.wantStatus, rec.Body.String())
					}
				} else {
					assertErrorEnvelope(t, rec, http.StatusForbidden, "forbidden")
				}
			})
		}
	}
}

func TestReadEndpointsAndValidation(t *testing.T) {
	env := newTestEnv(t)
	seedTestMessages(t, env.st)

	// Place deferred queue file on disk for next_retry_at lookup
	deferredFile := filepath.Join(env.queueDir, "deferred", "4", "4jV74M63RlzHxK3")
	if err := os.MkdirAll(filepath.Dir(deferredFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(deferredFile, []byte("deferred-msg"), 0o600); err != nil {
		t.Fatal(err)
	}
	retryWant := time.Date(2026, 11, 18, 16, 59, 45, 0, time.UTC)
	if err := os.Chtimes(deferredFile, retryWant, retryWant); err != nil {
		t.Fatal(err)
	}

	// 1. GET /v1/messages with pagination and filters
	rec := doReq(t, env.srv, http.MethodGet, "/v1/messages?limit=1", "tok-read", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/messages status = %d: %s", rec.Code, rec.Body.String())
	}
	var page1 store.MessagePage
	if err := json.Unmarshal(rec.Body.Bytes(), &page1); err != nil {
		t.Fatal(err)
	}
	if len(page1.Messages) != 1 || page1.Messages[0].QueueID != "C23D82B60024" || page1.NextCursor == nil {
		t.Fatalf("unexpected page1: %+v", page1)
	}

	rec = doReq(t, env.srv, http.MethodGet, "/v1/messages?limit=1&cursor="+*page1.NextCursor, "tok-read", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/messages cursor status = %d: %s", rec.Code, rec.Body.String())
	}
	var page2 store.MessagePage
	if err := json.Unmarshal(rec.Body.Bytes(), &page2); err != nil {
		t.Fatal(err)
	}
	if len(page2.Messages) != 1 || page2.Messages[0].QueueID != "4jV74M63RlzHxK3" || page2.NextCursor != nil {
		t.Fatalf("unexpected page2: %+v", page2)
	}

	// Filters: status, sender, recipient, message_id, from, to
	rec = doReq(t, env.srv, http.MethodGet, "/v1/messages?status=deferred&sender=a@example.com&recipient=b@example.org&from=2026-11-18T16:00:00Z&to=2026-11-18T17:00:00Z", "tok-read", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("filtered messages failed: %d %s", rec.Code, rec.Body.String())
	}
	var filtered store.MessagePage
	if err := json.Unmarshal(rec.Body.Bytes(), &filtered); err != nil {
		t.Fatal(err)
	}
	if len(filtered.Messages) != 1 || filtered.Messages[0].QueueID != "4jV74M63RlzHxK3" {
		t.Fatalf("unexpected filtered result: %+v", filtered.Messages)
	}

	// Invalid query params -> 400
	for _, badURL := range []string{
		"/v1/messages?queue_id=ALL",
		"/v1/messages?limit=0",
		"/v1/messages?limit=1001",
		"/v1/messages?limit=abc",
		"/v1/messages?from=not-a-time",
		"/v1/messages?to=not-a-time",
		"/v1/messages?status=bogus",
		"/v1/messages?cursor=bad-cursor",
	} {
		rec = doReq(t, env.srv, http.MethodGet, badURL, "tok-read", "")
		assertErrorEnvelope(t, rec, http.StatusBadRequest, "invalid_argument")
	}

	// 2. GET /v1/messages/{queue_id}
	rec = doReq(t, env.srv, http.MethodGet, "/v1/messages/4jV74M63RlzHxK3", "tok-read", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/messages/{id} status = %d: %s", rec.Code, rec.Body.String())
	}
	var detail map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if detail["subject"] != "Привет order 42" {
		t.Errorf("subject = %v", detail["subject"])
	}
	if detail["next_retry_at"] != "2026-11-18T16:59:45Z" {
		t.Errorf("next_retry_at = %v, want 2026-11-18T16:59:45Z", detail["next_retry_at"])
	}
	if evs, ok := detail["events"].([]any); !ok || len(evs) != 2 {
		t.Errorf("events = %#v, want 2 events", detail["events"])
	}

	// Non-deferred message has next_retry_at: null
	rec = doReq(t, env.srv, http.MethodGet, "/v1/messages/C23D82B60024", "tok-read", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET non-deferred message status = %d", rec.Code)
	}
	var detail2 map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &detail2); err != nil {
		t.Fatal(err)
	}
	if val, exists := detail2["next_retry_at"]; !exists || val != nil {
		t.Errorf("expected next_retry_at: null, got exists=%v val=%v", exists, val)
	}

	// Missing message -> 404
	rec = doReq(t, env.srv, http.MethodGet, "/v1/messages/MISSING123456", "tok-read", "")
	assertErrorEnvelope(t, rec, http.StatusNotFound, "not_found")

	// Invalid queue_id -> 400
	for _, badID := range []string{"ALL", "12345", "bad%2Fslash", "..%2Fetc"} {
		rec = doReq(t, env.srv, http.MethodGet, "/v1/messages/"+badID, "tok-read", "")
		assertErrorEnvelope(t, rec, http.StatusBadRequest, "invalid_argument")
	}

	// Extra path segment -> 404
	rec = doReq(t, env.srv, http.MethodGet, "/v1/messages/4jV74M63RlzHxK3/bogus", "tok-read", "")
	assertErrorEnvelope(t, rec, http.StatusNotFound, "not_found")

	// 3. GET /v1/messages/{queue_id}/content (UTF-8, base64, 404, 400)
	rec = doReq(t, env.srv, http.MethodGet, "/v1/messages/4jV74M63RlzHxK3/content", "tok-content", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("content utf-8 status = %d: %s", rec.Code, rec.Body.String())
	}
	var cResp messageContentResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &cResp); err != nil {
		t.Fatal(err)
	}
	if cResp.BodyEncoding != "utf-8" || cResp.Body != "Hello world\n" || !strings.Contains(cResp.Headers, "Subject: Test") {
		t.Errorf("unexpected content response: %+v", cResp)
	}

	rec = doReq(t, env.srv, http.MethodGet, "/v1/messages/BINARY0001/content", "tok-content", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("content binary status = %d: %s", rec.Code, rec.Body.String())
	}
	var binResp messageContentResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &binResp); err != nil {
		t.Fatal(err)
	}
	if binResp.BodyEncoding != "base64" || binResp.Body != "//4AAQ==" {
		t.Errorf("unexpected binary content response: %+v", binResp)
	}

	rec = doReq(t, env.srv, http.MethodGet, "/v1/messages/MISSING123456/content", "tok-content", "")
	assertErrorEnvelope(t, rec, http.StatusNotFound, "not_found")

	rec = doReq(t, env.srv, http.MethodGet, "/v1/messages/bad%2Fslash/content", "tok-content", "")
	assertErrorEnvelope(t, rec, http.StatusBadRequest, "invalid_argument")

	// 4. GET /v1/events
	rec = doReq(t, env.srv, http.MethodGet, "/v1/events?after=0&limit=2", "tok-read", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/events status = %d: %s", rec.Code, rec.Body.String())
	}
	var evResp eventsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &evResp); err != nil {
		t.Fatal(err)
	}
	if len(evResp.Events) != 2 || !evResp.HasMore || evResp.LastID != 2 {
		t.Fatalf("unexpected events page 1: %+v", evResp)
	}

	rec = doReq(t, env.srv, http.MethodGet, "/v1/events?after=2&limit=10", "tok-read", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/events page 2 status = %d: %s", rec.Code, rec.Body.String())
	}
	var evResp2 eventsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &evResp2); err != nil {
		t.Fatal(err)
	}
	if len(evResp2.Events) != 2 || evResp2.HasMore || evResp2.LastID != 4 {
		t.Fatalf("unexpected events page 2: %+v", evResp2)
	}

	for _, bad := range []string{"/v1/events?after=-1", "/v1/events?after=abc", "/v1/events?limit=0", "/v1/events?limit=1001"} {
		rec = doReq(t, env.srv, http.MethodGet, bad, "tok-read", "")
		assertErrorEnvelope(t, rec, http.StatusBadRequest, "invalid_argument")
	}

	// 5. GET /v1/queue
	rec = doReq(t, env.srv, http.MethodGet, "/v1/queue", "tok-read", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/queue status = %d: %s", rec.Code, rec.Body.String())
	}
	var qResp queueResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &qResp); err != nil {
		t.Fatal(err)
	}
	if qResp.Total != 2 || qResp.Counts["deferred"] != 1 || qResp.Counts["hold"] != 1 || qResp.Counts["active"] != 0 {
		t.Fatalf("unexpected queue response: %+v", qResp)
	}
	if qResp.Entries[0].DelayReason != "Connection timed out" || qResp.Entries[0].ArrivalTime.IsZero() {
		t.Errorf("unexpected queue entry[0]: %+v", qResp.Entries[0])
	}

	// Routing 404 & 405
	rec = doReq(t, env.srv, http.MethodGet, "/v1/does-not-exist", "tok-read", "")
	assertErrorEnvelope(t, rec, http.StatusNotFound, "not_found")
	rec = doReq(t, env.srv, http.MethodPost, "/v1/health", "tok-read", "")
	assertErrorEnvelope(t, rec, http.StatusMethodNotAllowed, "method_not_allowed")
	if rec.Header().Get("Allow") != "GET" {
		t.Errorf("Allow = %q, want GET", rec.Header().Get("Allow"))
	}
}

func TestWriteEndpointsAndAuditLogging(t *testing.T) {
	env := newTestEnv(t)

	// Single-message actions
	cases := []struct {
		method     string
		path       string
		wantAction string
		wantOut    string
	}{
		{http.MethodPost, "/v1/messages/4jV74M63RlzHxK3/hold", "hold", "postsuper -h 4jV74M63RlzHxK3"},
		{http.MethodPost, "/v1/messages/4jV74M63RlzHxK3/release", "release", "postsuper -H 4jV74M63RlzHxK3"},
		{http.MethodPost, "/v1/messages/4jV74M63RlzHxK3/requeue", "requeue", "postsuper -r 4jV74M63RlzHxK3"},
		{http.MethodPost, "/v1/messages/4jV74M63RlzHxK3/expire", "expire", "postsuper -e 4jV74M63RlzHxK3"},
		{http.MethodPost, "/v1/messages/4jV74M63RlzHxK3/flush", "flush", "postqueue -i 4jV74M63RlzHxK3"},
		{http.MethodDelete, "/v1/messages/4jV74M63RlzHxK3", "delete", "postsuper -d 4jV74M63RlzHxK3"},
	}

	for _, tc := range cases {
		rec := doReq(t, env.srv, tc.method, tc.path, "tok-write", "")
		if rec.Code != http.StatusOK {
			t.Fatalf("%s %s status=%d body=%s", tc.method, tc.path, rec.Code, rec.Body.String())
		}
		var resp writeActionResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if resp.Action != tc.wantAction || len(resp.QueueIDs) != 1 || resp.QueueIDs[0] != "4jV74M63RlzHxK3" || resp.ExitCode != 0 || strings.TrimSpace(resp.Output) != tc.wantOut {
			t.Errorf("%s %s unexpected resp: %+v", tc.method, tc.path, resp)
		}
	}

	// Bulk action
	rec := doReq(t, env.srv, http.MethodPost, "/v1/queue/actions", "tok-write", `{"action":"hold","queue_ids":["4jV74M63RlzHxK3","C23D82B60024"]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("bulk actions status=%d body=%s", rec.Code, rec.Body.String())
	}
	var bulkResp writeActionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &bulkResp); err != nil {
		t.Fatal(err)
	}
	if bulkResp.Action != "hold" || len(bulkResp.QueueIDs) != 2 || !strings.Contains(bulkResp.Output, "postsuper -h bulk 4jV74M63RlzHxK3,C23D82B60024") {
		t.Errorf("unexpected bulk resp: %+v", bulkResp)
	}

	// Queue flush
	rec = doReq(t, env.srv, http.MethodPost, "/v1/queue/flush", "tok-write", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("flush queue status=%d body=%s", rec.Code, rec.Body.String())
	}
	var flushResp writeActionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &flushResp); err != nil {
		t.Fatal(err)
	}
	if flushResp.Action != "flush" || len(flushResp.QueueIDs) != 0 || strings.TrimSpace(flushResp.Output) != "postqueue -f ok" {
		t.Errorf("unexpected flush resp: %+v", flushResp)
	}

	// Failing command -> 502 command_failed AND audit log with exit_code=1
	rec = doReq(t, env.srv, http.MethodPost, "/v1/messages/FAIL0001/hold", "tok-write", "")
	assertErrorEnvelope(t, rec, http.StatusBadGateway, "command_failed")

	rec = doReq(t, env.srv, http.MethodPost, "/v1/messages/FAIL0001/flush", "tok-write", "")
	assertErrorEnvelope(t, rec, http.StatusBadGateway, "command_failed")

	// Verify audit events count & JSON lines
	evs := env.audit.Events()
	if len(evs) != 10 {
		t.Fatalf("audit event count = %d, want 10", len(evs))
	}
	if evs[0].Token != "writer" || evs[0].Action != "hold" || evs[0].ExitCode != 0 || !evs[0].Audit {
		t.Errorf("unexpected audit[0]: %+v", evs[0])
	}
	if evs[7].Action != "flush" || len(evs[7].QueueIDs) != 0 || evs[7].ExitCode != 0 {
		t.Errorf("unexpected flush audit[7]: %+v", evs[7])
	}
	if evs[8].Action != "hold" || evs[8].QueueIDs[0] != "FAIL0001" || evs[8].ExitCode != 1 {
		t.Errorf("unexpected failed audit[8]: %+v", evs[8])
	}
	if evs[9].Action != "flush" || evs[9].QueueIDs[0] != "FAIL0001" || evs[9].ExitCode != 1 {
		t.Errorf("unexpected failed audit[9]: %+v", evs[9])
	}

	// Verify JSON line formatting in auditBuf
	scanner := bufio.NewScanner(bytes.NewReader(env.auditBuf.Bytes()))
	lineCount := 0
	for scanner.Scan() {
		var obj map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &obj); err != nil {
			t.Fatalf("invalid audit JSON line %q: %v", scanner.Text(), err)
		}
		if obj["audit"] != true || obj["token"] != "writer" {
			t.Errorf("unexpected audit JSON line: %v", obj)
		}
		lineCount++
	}
	if lineCount != 10 {
		t.Errorf("auditBuf line count = %d, want 10", lineCount)
	}

	// Bulk validation & 413 payload_too_large
	ids1001 := make([]string, 1001)
	for i := range ids1001 {
		ids1001[i] = fmt.Sprintf("QID%06d", i)
	}
	tooManyBody, _ := json.Marshal(bulkActionsRequest{Action: "hold", QueueIDs: ids1001})
	badBulks := []struct {
		body       string
		wantStatus int
		wantCode   string
	}{
		{"", http.StatusBadRequest, "invalid_argument"},
		{"{not-json", http.StatusBadRequest, "invalid_argument"},
		{`{"action":"hold","queue_ids":["4jV74M63RlzHxK3"]}{"extra":1}`, http.StatusBadRequest, "invalid_argument"},
		{`{"action":"flush","queue_ids":["4jV74M63RlzHxK3"]}`, http.StatusBadRequest, "invalid_argument"},
		{`{"action":"hold","queue_ids":[]}`, http.StatusBadRequest, "invalid_argument"},
		{`{"action":"hold","queue_ids":["ALL"]}`, http.StatusBadRequest, "invalid_argument"},
		{string(tooManyBody), http.StatusBadRequest, "invalid_argument"},
		{`{"action":"hold","queue_ids":["` + strings.Repeat("A", 1<<20) + `"]}`, http.StatusRequestEntityTooLarge, "payload_too_large"},
	}
	for _, tc := range badBulks {
		rec = doReq(t, env.srv, http.MethodPost, "/v1/queue/actions", "tok-write", tc.body)
		assertErrorEnvelope(t, rec, tc.wantStatus, tc.wantCode)
	}

	// Invalid queue IDs on single-message write endpoints
	for _, p := range []string{
		"/v1/messages/ALL/hold",
		"/v1/messages/12345/release",
		"/v1/messages/bad%2Fid/flush",
	} {
		rec = doReq(t, env.srv, http.MethodPost, p, "tok-write", "")
		assertErrorEnvelope(t, rec, http.StatusBadRequest, "invalid_argument")
	}
	rec = doReq(t, env.srv, http.MethodDelete, "/v1/messages/ALL", "tok-write", "")
	assertErrorEnvelope(t, rec, http.StatusBadRequest, "invalid_argument")
}

func TestHealthOperationalAndDegraded(t *testing.T) {
	env := newTestEnv(t)

	// Run initial reconcile so LastQueueSyncAt is populated and LastQueueSyncOK is true.
	if err := env.idx.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}

	getHealth := func() healthResponse {
		t.Helper()
		rec := doReq(t, env.srv, http.MethodGet, "/v1/health", "tok-read", "")
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /v1/health status = %d: %s", rec.Code, rec.Body.String())
		}
		var h healthResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &h); err != nil {
			t.Fatal(err)
		}
		return h
	}

	// 1. Operational baseline
	h := getHealth()
	if h.Status != "OPERATIONAL" || !h.RootHelperAlive || !h.LastQueueSyncOK || h.LastQueueSyncAt == nil || h.PostfixVersion != "3.11.7" {
		t.Fatalf("expected OPERATIONAL health, got %+v", h)
	}

	// 2. Degraded when spool_gaps > 0
	env.spoolSrc.SetStats(indexer.SpoolStats{Gaps: 1, Bytes: 2048})
	h = getHealth()
	if h.Status != "DEGRADED" || h.SpoolGaps != 1 || h.SpoolBytes != 2048 {
		t.Fatalf("expected DEGRADED on spool_gaps, got %+v", h)
	}

	// 3. Degraded when lost_spool_records > 0
	env.spoolSrc.SetStats(indexer.SpoolStats{LostRecords: 3})
	h = getHealth()
	if h.Status != "DEGRADED" || h.LostSpoolRecords != 3 {
		t.Fatalf("expected DEGRADED on lost_spool_records, got %+v", h)
	}

	// 4. Degraded when indexer_delay_ms > 10000
	// Seed message & cursor first, then construct a fresh indexer so it loads LastRecordAt
	// from SQLite while spool HeadSegment is ahead of cursor.
	seedTestMessages(t, env.st)
	env.spoolSrc.SetStats(indexer.SpoolStats{
		HeadSegment: spool.SegmentName(2),
		HeadOffset:  500,
		Bytes:       500,
	})
	lagIdx, err := indexer.New(env.st, env.spoolSrc, env.lister, indexer.Config{
		Now: func() time.Time { return env.now },
	})
	if err != nil {
		t.Fatalf("indexer.New(lagIdx): %v", err)
	}
	if err := lagIdx.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("lagIdx.ReconcileOnce: %v", err)
	}
	lagSrv, err := NewServer(env.cfg, env.st, lagIdx, env.runner, env.queueDir, WithPostfixVersion("3.11.7"))
	if err != nil {
		t.Fatalf("NewServer(lagSrv): %v", err)
	}
	rec := doReq(t, lagSrv, http.MethodGet, "/v1/health", "tok-read", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/health status = %d", rec.Code)
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &h); err != nil {
		t.Fatal(err)
	}
	if h.Status != "DEGRADED" || h.IndexerDelayMs <= 10000 {
		t.Fatalf("expected DEGRADED on indexer_delay_ms > 10000, got %+v", h)
	}

	// Catch up spool stats -> back to OPERATIONAL
	env.spoolSrc.SetStats(indexer.SpoolStats{HeadSegment: spool.SegmentName(1), HeadOffset: 1024})
	rec = doReq(t, lagSrv, http.MethodGet, "/v1/health", "tok-read", "")
	if err := json.Unmarshal(rec.Body.Bytes(), &h); err != nil {
		t.Fatal(err)
	}
	if h.Status != "OPERATIONAL" || h.IndexerDelayMs != 0 {
		t.Fatalf("expected OPERATIONAL once caught up, got %+v", h)
	}

	// Reset base env spool stats to caught-up for remaining checks
	env.spoolSrc.SetStats(indexer.SpoolStats{})

	// 5. Degraded when last_queue_sync_ok == false
	env.lister.SetError(errors.New("postqueue failed"))
	_ = env.idx.ReconcileOnce(context.Background())
	h = getHealth()
	if h.Status != "DEGRADED" || h.LastQueueSyncOK {
		t.Fatalf("expected DEGRADED on failed queue sync, got %+v", h)
	}

	// Recover sync -> OPERATIONAL
	env.lister.SetError(nil)
	_ = env.idx.ReconcileOnce(context.Background())
	if h := getHealth(); h.Status != "OPERATIONAL" {
		t.Fatalf("expected OPERATIONAL after sync recovery, got %+v", h)
	}

	// 6. Degraded when root_helper_alive == false
	_ = env.helper.Close()
	h = getHealth()
	if h.Status != "DEGRADED" || h.RootHelperAlive {
		t.Fatalf("expected DEGRADED when root_helper_alive=false, got %+v", h)
	}
}

func TestCheckTransportSecurity(t *testing.T) {
	plainCfg := config.DefaultConfig()
	allowPlainCfg := config.DefaultConfig()
	allowPlainCfg.AllowPlaintext = true
	tlsCfg := config.DefaultConfig()
	tlsCfg.TLS = config.TLSConfig{CertFile: "/etc/ssl/cert.pem", KeyFile: "/etc/ssl/key.pem"}
	partialTLSCfg := config.DefaultConfig()
	partialTLSCfg.TLS = config.TLSConfig{CertFile: "/etc/ssl/cert.pem"}

	loopbackAddrs := []string{
		"127.0.0.1:8025",
		"127.0.0.42:8025",
		"[::1]:8025",
		"localhost:8025",
		"127.0.0.1",
		"::1",
	}
	for _, addr := range loopbackAddrs {
		if err := CheckTransportSecurity(addr, plainCfg); err != nil {
			t.Errorf("CheckTransportSecurity(%q, plain) unexpected error: %v", addr, err)
		}
	}

	nonLoopbackAddrs := []string{
		"0.0.0.0:8025",
		"[::]:8025",
		":8025",
		"10.0.0.5:8025",
		"mail.example.com:8025",
		"",
	}
	for _, addr := range nonLoopbackAddrs {
		if err := CheckTransportSecurity(addr, plainCfg); err == nil {
			t.Errorf("CheckTransportSecurity(%q, plain) = nil, want error", addr)
		}
		if err := CheckTransportSecurity(addr, allowPlainCfg); err != nil {
			t.Errorf("CheckTransportSecurity(%q, allow_plaintext) unexpected error: %v", addr, err)
		}
		if err := CheckTransportSecurity(addr, tlsCfg); err != nil {
			t.Errorf("CheckTransportSecurity(%q, tls) unexpected error: %v", addr, err)
		}
	}

	if err := CheckTransportSecurity("127.0.0.1:8025", partialTLSCfg); err == nil {
		t.Error("expected error for partial TLS config")
	}
	if err := CheckTransportSecurity("127.0.0.1:8025", nil); err == nil {
		t.Error("expected error for nil config")
	}
}

func generateSelfSignedCert(t *testing.T, certPath, keyPath string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	certOut := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certPath, certOut, 0o600); err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	keyOut := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyOut, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestServeMasterWakeupTLSAndShutdown(t *testing.T) {
	env := newTestEnv(t)

	// 1. Refuse non-loopback FD without TLS or allow_plaintext
	rawLn, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Skipf("0.0.0.0 listen not permitted: %v", err)
	}
	f0, err := rawLn.(*net.TCPListener).File()
	_ = rawLn.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer f0.Close()

	if err := env.srv.ServeMaster(context.Background(), int(f0.Fd()), "0.0.0.0:8025"); err == nil {
		t.Fatal("expected ServeMaster to refuse non-loopback plaintext listener")
	}

	// 2. Serve HTTPS over FileListener FD, handle loopback wakeup probes silently,
	//    still log non-EOF TLS errors, serve authenticated /v1/health, and shut down cleanly.
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")
	generateSelfSignedCert(t, certFile, keyFile)

	env.cfg.TLS = config.TLSConfig{CertFile: certFile, KeyFile: keyFile}
	tlsCfg, err := LoadTLSConfig(env.cfg)
	if err != nil {
		t.Fatalf("LoadTLSConfig: %v", err)
	}

	var errBuf bytes.Buffer
	var errMu sync.Mutex
	errWriter := writerFunc(func(p []byte) (int, error) {
		errMu.Lock()
		defer errMu.Unlock()
		return errBuf.Write(p)
	})

	tlsSrv, err := NewServer(
		env.cfg,
		env.st,
		env.idx,
		env.runner,
		env.queueDir,
		WithTLSConfig(tlsCfg),
		WithErrorLogger(errWriter),
		WithPostfixVersion("3.11.7"),
	)
	if err != nil {
		t.Fatalf("NewServer TLS: %v", err)
	}

	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	tcpFile, err := ln.(*net.TCPListener).File()
	_ = ln.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer tcpFile.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- tlsSrv.ServeMaster(ctx, int(tcpFile.Fd()), addr)
	}()

	// Wait briefly until port accepts connections, then simulate master wakeup probe (connect + close).
	var probe net.Conn
	for i := 0; i < 50; i++ {
		probe, err = net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("dial listener %s: %v", addr, err)
	}
	_ = probe.Close()
	time.Sleep(50 * time.Millisecond)

	errMu.Lock()
	afterWakeup := errBuf.String()
	errMu.Unlock()
	if strings.Contains(afterWakeup, "TLS handshake error") {
		t.Errorf("expected loopback wakeup EOF to be suppressed, got logs: %q", afterWakeup)
	}

	// Send non-EOF TLS error (plain HTTP bytes to TLS listener) -> MUST be logged!
	badConn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = badConn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	_ = badConn.Close()
	time.Sleep(50 * time.Millisecond)

	errMu.Lock()
	afterBadTLS := errBuf.String()
	errMu.Unlock()
	if !strings.Contains(afterBadTLS, "TLS handshake error") {
		t.Errorf("expected real TLS handshake error to be logged, got: %q", afterBadTLS)
	}

	// Valid HTTPS request with bearer token
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	req, err := http.NewRequest(http.MethodGet, "https://"+addr+"/v1/health", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer tok-read")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("HTTPS GET /v1/health: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTPS /v1/health status = %d: %s", resp.StatusCode, string(body))
	}
	var h healthResponse
	if err := json.Unmarshal(body, &h); err != nil {
		t.Fatal(err)
	}
	if h.Status != "OPERATIONAL" {
		t.Errorf("health status = %q, want OPERATIONAL", h.Status)
	}

	// Graceful shutdown on context cancellation
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ServeMaster returned error on shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ServeMaster did not shut down in time")
	}
}

type writerFunc func(p []byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func TestCommandFailuresAndAdditionalPaths(t *testing.T) {
	env := newTestEnv(t)
	seedTestMessages(t, env.st)

	// NewServer validation & WithAuditWriter
	if _, err := NewServer(nil, env.st, env.idx, env.runner, env.queueDir); err == nil {
		t.Error("expected NewServer(nil cfg) error")
	}
	if _, err := NewServer(env.cfg, nil, env.idx, env.runner, env.queueDir); err == nil {
		t.Error("expected NewServer(nil store) error")
	}
	var auditOut bytes.Buffer
	customSrv, err := NewServer(env.cfg, env.st, env.idx, env.runner, env.queueDir, WithAuditWriter(&auditOut))
	if err != nil {
		t.Fatalf("NewServer(WithAuditWriter): %v", err)
	}
	rec := doReq(t, customSrv, http.MethodPost, "/v1/messages/4jV74M63RlzHxK3/hold", "tok-write", "")
	if rec.Code != http.StatusOK || !strings.Contains(auditOut.String(), `"action":"hold"`) {
		t.Errorf("WithAuditWriter unexpected result: code=%d audit=%q", rec.Code, auditOut.String())
	}

	// GET /v1/messages with queue_id and message_id filters
	rec = doReq(t, env.srv, http.MethodGet, "/v1/messages?queue_id=4jV74M63RlzHxK3&message_id=%3C4jV74M63RlzHxK3%40localhost%3E", "tok-read", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("filtered query failed: %d %s", rec.Code, rec.Body.String())
	}
	var filtered store.MessagePage
	if err := json.Unmarshal(rec.Body.Bytes(), &filtered); err != nil {
		t.Fatal(err)
	}
	if len(filtered.Messages) != 1 || filtered.Messages[0].QueueID != "4jV74M63RlzHxK3" {
		t.Errorf("unexpected queue_id/message_id filter result: %+v", filtered.Messages)
	}

	// GET /v1/messages/{id} fallback unhashed deferred/<qid> when runner is nil
	unhashedFile := filepath.Join(env.queueDir, "deferred", "4jV74M63RlzHxK3")
	if err := os.MkdirAll(filepath.Dir(unhashedFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(unhashedFile, []byte("queued"), 0o600); err != nil {
		t.Fatal(err)
	}
	wantRetry := time.Date(2026, 11, 19, 1, 2, 3, 0, time.UTC)
	if err := os.Chtimes(unhashedFile, wantRetry, wantRetry); err != nil {
		t.Fatal(err)
	}
	noRunnerSrv, err := NewServer(env.cfg, env.st, env.idx, nil, env.queueDir)
	if err != nil {
		t.Fatal(err)
	}
	rec = doReq(t, noRunnerSrv, http.MethodGet, "/v1/messages/4jV74M63RlzHxK3", "tok-read", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET detail without runner failed: %d", rec.Code)
	}
	var detail map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if detail["next_retry_at"] != "2026-11-19T01:02:03Z" {
		t.Errorf("fallback next_retry_at = %v, want 2026-11-19T01:02:03Z", detail["next_retry_at"])
	}

	// Trigger postqueue -j, postqueue -f, and bulk postsuper failures -> 502 command_failed
	if err := os.WriteFile(filepath.Join(env.cmdDir, "fail_postqueue"), []byte("1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(env.cmdDir, "fail_flush"), []byte("1"), 0o644); err != nil {
		t.Fatal(err)
	}
	rec = doReq(t, env.srv, http.MethodGet, "/v1/queue", "tok-read", "")
	assertErrorEnvelope(t, rec, http.StatusBadGateway, "command_failed")

	rec = doReq(t, env.srv, http.MethodPost, "/v1/queue/flush", "tok-write", "")
	assertErrorEnvelope(t, rec, http.StatusBadGateway, "command_failed")

	rec = doReq(t, env.srv, http.MethodPost, "/v1/queue/actions", "tok-write", `{"action":"delete","queue_ids":["4jV74M63RlzHxK3","FAIL0001"]}`)
	assertErrorEnvelope(t, rec, http.StatusBadGateway, "command_failed")

	// Plain HTTP ServeMaster on loopback FD + wakeup probe + shutdown
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	tcpFile, err := ln.(*net.TCPListener).File()
	_ = ln.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer tcpFile.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- env.srv.ServeMaster(ctx, int(tcpFile.Fd()), addr)
	}()

	var conn net.Conn
	for i := 0; i < 50; i++ {
		conn, err = net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("dial plain ServeMaster: %v", err)
	}
	_ = conn.Close() // empty wakeup probe

	req, _ := http.NewRequest(http.MethodGet, "http://"+addr+"/v1/health", nil)
	req.Header.Set("Authorization", "Bearer tok-read")
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("plain HTTP /v1/health: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("plain HTTP /v1/health status = %d", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("plain ServeMaster shutdown error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("plain ServeMaster shutdown timed out")
	}

	// PrepareMasterListener / Serve edge errors
	if _, err := env.srv.PrepareMasterListener(-1, "127.0.0.1:8025"); err == nil {
		t.Error("expected error for invalid FD -1")
	}
	if err := env.srv.Serve(context.Background(), nil); err == nil {
		t.Error("expected error for nil listener")
	}
}
