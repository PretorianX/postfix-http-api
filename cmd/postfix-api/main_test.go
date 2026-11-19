package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PretorianX/postfix-http-api/internal/api"
	"github.com/PretorianX/postfix-http-api/internal/config"
	"github.com/PretorianX/postfix-http-api/internal/indexer"
	"github.com/PretorianX/postfix-http-api/internal/postfixcmd"
	"github.com/PretorianX/postfix-http-api/internal/spool"
	"github.com/PretorianX/postfix-http-api/internal/store"
)

func TestCLIDispatchVersionAndToken(t *testing.T) {
	var stdout, stderr bytes.Buffer

	if code := run([]string{"version"}, strings.NewReader(""), &stdout, &stderr); code != 0 {
		t.Fatalf("version exit=%d stderr=%s", code, stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != version {
		t.Errorf("version stdout = %q, want %q", stdout.String(), version)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"token", "new", "--name", "ci-ops", "--scopes", "read,write"}, strings.NewReader(""), &stdout, &stderr); code != 0 {
		t.Fatalf("token new exit=%d stderr=%s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "pfapi_") || !strings.Contains(out, `"ci-ops"`) || !strings.Contains(out, "- read\n") || !strings.Contains(out, "- write\n") {
		t.Errorf("unexpected token new output:\n%s", out)
	}

	// Missing transport in master mode exits 2
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-u", "-c"}, strings.NewReader(""), &stdout, &stderr); code != 2 {
		t.Errorf("missing -t exit = %d, want 2", code)
	}

	// Unsupported transport exits 2
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-t", "fifo"}, strings.NewReader(""), &stdout, &stderr); code != 2 {
		t.Errorf("unsupported -t exit = %d, want 2", code)
	}

	// Unknown CLI args on install without --listen exits 2
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"install"}, strings.NewReader(""), &stdout, &stderr); code != 2 {
		t.Errorf("install without --listen exit = %d, want 2", code)
	}

	// Empty args, help flags, bad subcommands
	if code := run(nil, strings.NewReader(""), &stdout, &stderr); code != 2 {
		t.Errorf("no args exit = %d, want 2", code)
	}
	for _, helpArg := range []string{"help", "--help", "-h"} {
		stdout.Reset()
		stderr.Reset()
		if code := run([]string{helpArg}, strings.NewReader(""), &stdout, &stderr); code != 0 {
			t.Errorf("%s exit = %d, want 0", helpArg, code)
		}
		if !strings.Contains(stdout.String(), "Usage:") {
			t.Errorf("%s missing Usage text: %s", helpArg, stdout.String())
		}
	}
	if code := run([]string{"version", "extra"}, strings.NewReader(""), &stdout, &stderr); code != 2 {
		t.Errorf("version extra exit = %d, want 2", code)
	}
	if code := run([]string{"token", "list"}, strings.NewReader(""), &stdout, &stderr); code != 2 {
		t.Errorf("token list exit = %d, want 2", code)
	}
	if code := run([]string{"token", "new"}, strings.NewReader(""), &stdout, &stderr); code != 2 {
		t.Errorf("token new missing flags exit = %d, want 2", code)
	}
	if code := run([]string{"uninstall", "--unknown"}, strings.NewReader(""), &stdout, &stderr); code != 2 {
		t.Errorf("uninstall bad flag exit = %d, want 2", code)
	}
	if code := run([]string{"_root-helper"}, strings.NewReader(""), &stdout, &stderr); code != 2 {
		t.Errorf("_root-helper missing command-dir exit = %d, want 2", code)
	}

	// _root-helper invocation end-to-end through run()
	cmdDir := t.TempDir()
	psPath := filepath.Join(cmdDir, "postsuper")
	if err := os.WriteFile(psPath, []byte("#!/bin/sh\nprintf 'ok:%s:%s\\n' \"$1\" \"$2\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	in := strings.NewReader(`{"action":"hold","queue_ids":["4jV74M63RlzHxK3"]}` + "\n")
	if code := run([]string{"_root-helper", "--command-dir", cmdDir}, in, &stdout, &stderr); code != 0 {
		t.Fatalf("_root-helper exit = %d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"exit_code":0`) || !strings.Contains(stdout.String(), `ok:-h:4jV74M63RlzHxK3`) {
		t.Errorf("unexpected _root-helper output: %s", stdout.String())
	}
}

func TestSpoolAdapterAndIndexerEndToEnd(t *testing.T) {
	spoolDir := t.TempDir()
	queueDir := t.TempDir()
	cmdDir := t.TempDir()

	// Write a mock postqueue returning empty JSON
	postqueuePath := filepath.Join(cmdDir, "postqueue")
	if err := os.WriteFile(postqueuePath, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	sw, err := spool.NewWriter(spoolDir, 64*1024, 256*1024)
	if err != nil {
		t.Fatalf("spool.NewWriter: %v", err)
	}
	t1 := time.Date(2026, 11, 19, 8, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)
	if err := sw.WriteRecord(t1, []byte("postfix/qmgr[10]: 4jV74M63RlzHxK3: from=<a@example.com>, size=420, nrcpt=1 (queue active)")); err != nil {
		t.Fatal(err)
	}
	if err := sw.WriteRecord(t2, []byte("postfix/smtp[11]: 4jV74M63RlzHxK3: to=<b@example.org>, relay=mx.example.org[198.51.100.1]:25, delay=0.5, delays=0.1/0/0.2/0.2, dsn=2.0.0, status=sent (250 Ok)")); err != nil {
		t.Fatal(err)
	}
	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}

	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	runner := postfixcmd.NewRunner(cmdDir, queueDir, nil)
	spoolSrc := newSpoolSource(spoolDir)
	qLister := api.NewQueueLister(runner)

	idx, err := indexer.New(st, spoolSrc, qLister, indexer.Config{
		BatchSize: 100,
		Now:       func() time.Time { return t2 },
	})
	if err != nil {
		t.Fatal(err)
	}

	n, err := idx.IngestOnce(context.Background())
	if err != nil {
		t.Fatalf("IngestOnce: %v", err)
	}
	if n != 2 {
		t.Fatalf("IngestOnce count = %d, want 2", n)
	}

	msg, err := st.GetMessage(context.Background(), "4jV74M63RlzHxK3")
	if err != nil {
		t.Fatalf("GetMessage: %v", err)
	}
	if msg.Sender != "a@example.com" || len(msg.Recipients) != 1 || msg.Recipients[0].Status != "delivered" {
		t.Fatalf("unexpected indexed message: %+v", msg)
	}

	// Verify caught-up stats give indexer_delay_ms == 0
	metrics := idx.HealthMetrics()
	if metrics.IndexerDelayMs != 0 || metrics.SpoolGaps != 0 || metrics.LostSpoolRecords != 0 {
		t.Errorf("unexpected health metrics when caught up: %+v", metrics)
	}

	// Verify API server responds with indexed message
	cfg := config.DefaultConfig()
	cfg.Tokens = []config.Token{{
		Name:   "test",
		SHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		Scopes: []string{config.ScopeRead},
	}}
	// Use a known token hash for "secret"
	cfg.Tokens[0].SHA256 = "2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	srv, err := api.NewServer(cfg, st, idx, runner, queueDir)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/messages/4jV74M63RlzHxK3", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET message status=%d body=%s", rec.Code, rec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["sender"] != "a@example.com" {
		t.Errorf("unexpected sender in API response: %v", got["sender"])
	}
}
