package config

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func TestLoadFromReaderDefaultsAndFull(t *testing.T) {
	cfg, err := LoadFromReader(strings.NewReader(""))
	if err != nil {
		t.Fatalf("LoadFromReader empty: %v", err)
	}
	if cfg.Spool.SegmentSizeBytes != 16*1024*1024 {
		t.Errorf("SegmentSizeBytes = %d; want 16MiB", cfg.Spool.SegmentSizeBytes)
	}
	if cfg.Spool.MaxTotalBytes != 256*1024*1024 {
		t.Errorf("MaxTotalBytes = %d; want 256MiB", cfg.Spool.MaxTotalBytes)
	}
	if cfg.RetentionDuration != 168*time.Hour {
		t.Errorf("RetentionDuration = %v; want 168h", cfg.RetentionDuration)
	}
	if cfg.QueueSyncInterval != 5*time.Minute {
		t.Errorf("QueueSyncInterval = %v; want 5m", cfg.QueueSyncInterval)
	}
	if cfg.IndexerBatchSize != 1000 {
		t.Errorf("IndexerBatchSize = %d; want 1000", cfg.IndexerBatchSize)
	}
	if cfg.SubjectInOutput {
		t.Errorf("SubjectInOutput = true; want false")
	}
	if cfg.AllowPlaintext {
		t.Errorf("AllowPlaintext = true; want false")
	}
	if len(cfg.Tokens) != 0 {
		t.Errorf("len(Tokens) = %d; want 0", len(cfg.Tokens))
	}

	yamlDoc := `
state_dir: "/var/lib/postfix/postfix-api"
spool:
  segment_size_bytes: 131072
  max_total_bytes: 1048576
retention_duration: 24h
queue_sync_interval: 30s
indexer_batch_size: 250
subject_in_output: true
tls:
  cert_file: "/etc/ssl/cert.pem"
  key_file: "/etc/ssl/key.pem"
allow_plaintext: true
tokens:
  - name: "ops"
    sha256: "` + strings.ToUpper(sha256Hex("secret-1")) + `"
    scopes:
      - read
      - write
      - content
`
	cfg2, err := LoadFromReader(strings.NewReader(yamlDoc))
	if err != nil {
		t.Fatalf("LoadFromReader full: %v", err)
	}
	if cfg2.StateDir != "/var/lib/postfix/postfix-api" {
		t.Errorf("StateDir = %q", cfg2.StateDir)
	}
	if cfg2.Spool.SegmentSizeBytes != 131072 || cfg2.Spool.MaxTotalBytes != 1048576 {
		t.Errorf("Spool = %+v", cfg2.Spool)
	}
	if cfg2.RetentionDuration != 24*time.Hour || cfg2.QueueSyncInterval != 30*time.Second {
		t.Errorf("durations = %v, %v", cfg2.RetentionDuration, cfg2.QueueSyncInterval)
	}
	if cfg2.IndexerBatchSize != 250 || !cfg2.SubjectInOutput || !cfg2.AllowPlaintext {
		t.Errorf("flags = %d %v %v", cfg2.IndexerBatchSize, cfg2.SubjectInOutput, cfg2.AllowPlaintext)
	}
	if cfg2.Tokens[0].SHA256 != sha256Hex("secret-1") {
		t.Errorf("SHA256 not normalized to lowercase: %q", cfg2.Tokens[0].SHA256)
	}
}

func TestLoadFromReaderValidationAndStrictFields(t *testing.T) {
	badCases := []struct {
		name string
		yaml string
	}{
		{
			name: "unknown field",
			yaml: "allow_plain_text: true\n",
		},
		{
			name: "relative state_dir",
			yaml: "state_dir: relative/path\n",
		},
		{
			name: "segment too small",
			yaml: "spool:\n  segment_size_bytes: 4096\n",
		},
		{
			name: "max_total less than segment_size",
			yaml: "spool:\n  segment_size_bytes: 131072\n  max_total_bytes: 65536\n",
		},
		{
			name: "retention_duration zero",
			yaml: "retention_duration: 0s\n",
		},
		{
			name: "queue_sync_interval too small",
			yaml: "queue_sync_interval: 1s\n",
		},
		{
			name: "indexer_batch_size out of range",
			yaml: "indexer_batch_size: 10001\n",
		},
		{
			name: "half-configured TLS cert only",
			yaml: "tls:\n  cert_file: /etc/ssl/a.pem\n",
		},
		{
			name: "half-configured TLS key only",
			yaml: "tls:\n  key_file: /etc/ssl/a.key\n",
		},
		{
			name: "duplicate token name",
			yaml: "tokens:\n  - name: a\n    sha256: " + sha256Hex("1") + "\n    scopes: [read]\n  - name: a\n    sha256: " + sha256Hex("2") + "\n    scopes: [read]\n",
		},
		{
			name: "invalid sha256 length",
			yaml: "tokens:\n  - name: a\n    sha256: deadbeef\n    scopes: [read]\n",
		},
		{
			name: "invalid scope",
			yaml: "tokens:\n  - name: a\n    sha256: " + sha256Hex("1") + "\n    scopes: [admin]\n",
		},
		{
			name: "empty scopes",
			yaml: "tokens:\n  - name: a\n    sha256: " + sha256Hex("1") + "\n    scopes: []\n",
		},
	}

	for _, tc := range badCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := LoadFromReader(strings.NewReader(tc.yaml)); err == nil {
				t.Fatalf("LoadFromReader succeeded for %s; want error", tc.name)
			}
		})
	}
}

func TestVerifyTokenAndHasScope(t *testing.T) {
	tokA := "pfapi_token_alpha"
	tokB := "pfapi_token_beta"
	cfg, err := LoadFromReader(strings.NewReader(`
tokens:
  - name: reader
    sha256: ` + sha256Hex(tokA) + `
    scopes: [read]
  - name: writer
    sha256: ` + sha256Hex(tokB) + `
    scopes: [write, content]
`))
	if err != nil {
		t.Fatalf("LoadFromReader: %v", err)
	}

	got, ok := cfg.VerifyToken(tokA)
	if !ok || got == nil || got.Name != "reader" {
		t.Fatalf("VerifyToken(tokA) = (%v, %v); want reader", got, ok)
	}
	if !got.HasScope(ScopeRead) || got.HasScope(ScopeWrite) || got.HasScope(ScopeContent) {
		t.Errorf("reader scopes unexpected: %v", got.Scopes)
	}

	gotB, ok := cfg.VerifyToken(tokB)
	if !ok || gotB == nil || gotB.Name != "writer" {
		t.Fatalf("VerifyToken(tokB) = (%v, %v); want writer", gotB, ok)
	}
	if gotB.HasScope(ScopeRead) || !gotB.HasScope(ScopeWrite) || !gotB.HasScope(ScopeContent) {
		t.Errorf("writer scopes unexpected: %v", gotB.Scopes)
	}

	if _, ok := cfg.VerifyToken("wrong"); ok {
		t.Error("VerifyToken(wrong) = true; want false")
	}
	if _, ok := cfg.VerifyToken(""); ok {
		t.Error("VerifyToken(\"\") = true; want false")
	}

	var nilTok *Token
	if nilTok.HasScope(ScopeRead) {
		t.Error("nil Token.HasScope = true; want false")
	}

	// Manually constructed Config (without LoadFromReader) also verifies correctly.
	manual := &Config{
		Tokens: []Token{{Name: "m", SHA256: sha256Hex("manual"), Scopes: []string{ScopeRead}}},
	}
	if mTok, ok := manual.VerifyToken("manual"); !ok || mTok.Name != "m" {
		t.Fatalf("manual VerifyToken = (%v, %v); want m", mTok, ok)
	}
}

func TestLoadFileAndPermissions(t *testing.T) {
	dir := t.TempDir()
	confPath := filepath.Join(dir, "postfix-api.conf")
	if err := os.WriteFile(confPath, []byte("subject_in_output: true\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	cfg, err := LoadFile(confPath)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if !cfg.SubjectInOutput {
		t.Error("SubjectInOutput = false; want true")
	}

	if _, err := LoadFile(filepath.Join(dir, "missing.conf")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("missing file err = %v; want os.ErrNotExist", err)
	}

	if err := ValidateFilePermissions(0o600, confPath); err != nil {
		t.Errorf("0600 rejected: %v", err)
	}
	if err := ValidateFilePermissions(0o400, confPath); err != nil {
		t.Errorf("0400 rejected: %v", err)
	}
	if err := ValidateFilePermissions(0o640, confPath); err == nil {
		t.Error("0640 accepted; want error")
	}
	if err := ValidateFilePermissions(0o644, confPath); err == nil {
		t.Error("0644 accepted; want error")
	}
}

func TestResolveStateDir(t *testing.T) {
	cfg := DefaultConfig()
	dir, err := cfg.ResolveStateDir("/var/lib/postfix")
	if err != nil {
		t.Fatalf("ResolveStateDir: %v", err)
	}
	if dir != "/var/lib/postfix/postfix-api" {
		t.Errorf("dir = %q; want /var/lib/postfix/postfix-api", dir)
	}

	cfg2 := DefaultConfig()
	cfg2.StateDir = "/custom/state"
	dir2, err := cfg2.ResolveStateDir("/var/lib/postfix")
	if err != nil || dir2 != "/custom/state" {
		t.Errorf("ResolveStateDir custom = (%q, %v); want /custom/state", dir2, err)
	}
}

func TestReadPostconfAndVersionValidation(t *testing.T) {
	dir := t.TempDir()
	mockBin := filepath.Join(dir, "postconf")
	script := `#!/bin/sh
# Verify -c and -h arguments
if [ "$1" = "-c" ]; then
  shift 2
fi
if [ "$1" != "-h" ]; then
  echo "expected -h" >&2
  exit 1
fi
shift
for k in "$@"; do
  case "$k" in
    command_directory) echo "/usr/sbin" ;;
    queue_directory) echo "/var/spool/postfix" ;;
    data_directory) echo "/var/lib/postfix" ;;
    daemon_directory) echo "/usr/lib/postfix/sbin" ;;
    config_directory) echo "/etc/postfix" ;;
    mail_owner) echo "postfix" ;;
    maillog_file) echo "/dev/stdout" ;;
    mail_version) echo "3.7.11~deb12u1" ;;
    *) exit 2 ;;
  esac
done
`
	if err := os.WriteFile(mockBin, []byte(script), 0o755); err != nil {
		t.Fatalf("WriteFile mock postconf: %v", err)
	}

	t.Setenv("MAIL_CONFIG", "/etc/postfix-custom")
	params, err := ReadPostconf(mockBin)
	if err != nil {
		t.Fatalf("ReadPostconf: %v", err)
	}
	if params.CommandDirectory != "/usr/sbin" ||
		params.QueueDirectory != "/var/spool/postfix" ||
		params.DataDirectory != "/var/lib/postfix" ||
		params.DaemonDirectory != "/usr/lib/postfix/sbin" ||
		params.ConfigDirectory != "/etc/postfix" ||
		params.MailOwner != "postfix" ||
		params.MaillogFile != "/dev/stdout" ||
		params.MailVersion != "3.7.11~deb12u1" {
		t.Fatalf("unexpected params: %+v", params)
	}

	for _, okVer := range []string{"3.5.0", "3.5", "3.11.7", "3.7.11~deb12u1", "3.10-20260101", "4.0.0"} {
		if err := ValidateMailVersion(okVer); err != nil {
			t.Errorf("ValidateMailVersion(%q) unexpected error: %v", okVer, err)
		}
	}
	for _, badVer := range []string{"", "3", "3.4.9", "3.4", "2.11.3", "abc.1"} {
		if err := ValidateMailVersion(badVer); err == nil {
			t.Errorf("ValidateMailVersion(%q) = nil; want error", badVer)
		}
	}
}

func TestDropPrivilegesCurrentUser(t *testing.T) {
	u, err := user.Current()
	if err != nil {
		t.Fatalf("user.Current: %v", err)
	}
	if err := DropPrivileges(u.Username); err != nil {
		t.Errorf("DropPrivileges(current user %q): %v", u.Username, err)
	}
	if err := DropPrivileges(""); err == nil {
		t.Error("DropPrivileges(\"\") = nil; want error")
	}
	if err := DropPrivileges("nonexistent_postfix_user_99999"); err == nil {
		t.Error("DropPrivileges(nonexistent) = nil; want error")
	}
}
