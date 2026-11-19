package installer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckPostfixVersion(t *testing.T) {
	accepted := []string{
		"3.5.0",
		"3.5.25",
		"3.7.11-1~deb12u1",
		"3.11.7",
		"3.11.7-20260101",
		"4.0.0",
	}
	for _, v := range accepted {
		if err := CheckPostfixVersion(v); err != nil {
			t.Errorf("CheckPostfixVersion(%q) unexpected error: %v", v, err)
		}
	}

	rejected := []string{
		"",
		"abc",
		"3",
		"2.11.1",
		"3.0.0",
		"3.4.0",
		"3.4.9",
		"3.4.23-1~deb11u1",
	}
	for _, v := range rejected {
		if err := CheckPostfixVersion(v); err == nil {
			t.Errorf("CheckPostfixVersion(%q) = nil, want error", v)
		}
	}
}

func TestValidateListenAddr(t *testing.T) {
	valid := []string{
		"0.0.0.0:8025",
		"127.0.0.1:8025",
		"[::1]:8025",
		"localhost:443",
	}
	for _, addr := range valid {
		if err := ValidateListenAddr(addr); err != nil {
			t.Errorf("ValidateListenAddr(%q) unexpected error: %v", addr, err)
		}
	}

	invalid := []string{
		"",
		"8025",
		"0.0.0.0:0",
		"0.0.0.0:65536",
		"0.0.0.0:http",
		"bad host:8025",
	}
	for _, addr := range invalid {
		if err := ValidateListenAddr(addr); err == nil {
			t.Errorf("ValidateListenAddr(%q) = nil, want error", addr)
		}
	}
}

func TestGenerateToken(t *testing.T) {
	raw1, hash1, snippet1, err := GenerateToken("ops-reader", []string{"read", "content", "read"})
	if err != nil {
		t.Fatalf("GenerateToken error: %v", err)
	}
	if !strings.HasPrefix(raw1, "pfapi_") || len(raw1) != 6+43 {
		t.Errorf("unexpected raw token format/length: %q (len=%d)", raw1, len(raw1))
	}
	sum := sha256.Sum256([]byte(raw1))
	if want := hex.EncodeToString(sum[:]); hash1 != want {
		t.Errorf("sha256 = %q, want %q", hash1, want)
	}
	if !strings.Contains(snippet1, `"ops-reader"`) || !strings.Contains(snippet1, hash1) || !strings.Contains(snippet1, "- read\n") || !strings.Contains(snippet1, "- content\n") {
		t.Errorf("unexpected YAML snippet:\n%s", snippet1)
	}
	if strings.Count(snippet1, "- read\n") != 1 {
		t.Errorf("expected deduplicated scopes in snippet:\n%s", snippet1)
	}

	raw2, _, _, err := GenerateToken("ops-reader", []string{"write"})
	if err != nil {
		t.Fatalf("second GenerateToken error: %v", err)
	}
	if raw1 == raw2 {
		t.Error("expected distinct random tokens")
	}

	// Error cases
	if _, _, _, err := GenerateToken("", []string{"read"}); err == nil {
		t.Error("expected error for empty name")
	}
	if _, _, _, err := GenerateToken("bad\nname", []string{"read"}); err == nil {
		t.Error("expected error for newline in name")
	}
	if _, _, _, err := GenerateToken("ok", nil); err == nil {
		t.Error("expected error for empty scopes")
	}
	if _, _, _, err := GenerateToken("ok", []string{"read", "admin"}); err == nil {
		t.Error("expected error for invalid scope")
	}
}

func TestScanHeaderChecksConflicts(t *testing.T) {
	dir := t.TempDir()
	existingHC := filepath.Join(dir, "existing_header_checks")
	ownHC := filepath.Join(dir, "postfix-api-header-checks")
	afterHC := filepath.Join(dir, "after_header_checks")

	content := strings.Join([]string{
		"# Comment mentioning Subject: IGNORE",
		"/^X-Spam-Flag: YES/ REJECT spam",
		"/^Subject:.*secret/ WARN sensitive subject",
		"/^subject:/i IGNORE",
	}, "\n") + "\n"
	if err := os.WriteFile(existingHC, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ownHC, []byte("/^Subject:/ INFO\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(afterHC, []byte("/^Subject:/ IGNORE\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	hcValue := "regexp:" + existingHC + ", regexp:" + ownHC + ", regexp:" + afterHC
	warnings := ScanHeaderChecksConflicts(hcValue, ownHC)
	if len(warnings) != 2 {
		t.Fatalf("got %d warnings, want 2: %#v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], existingHC+":3") || !strings.Contains(warnings[1], existingHC+":4") {
		t.Errorf("unexpected warning lines: %#v", warnings)
	}
}

func TestInstallPreflightValidation(t *testing.T) {
	// 1. Version < 3.5 rejected
	h1 := newMockPostfixHarness(t, "3.4.9", "/dev/stdout", "")
	_, err := Install(InstallOptions{
		Listen:      "127.0.0.1:8025",
		BinaryPath:  h1.srcBin,
		PostconfBin: h1.postconfBin,
		PostfixBin:  h1.postfixBin,
		SkipChown:   true,
	})
	if err == nil || !strings.Contains(err.Error(), "3.5") {
		t.Errorf("expected version error for 3.4.9, got: %v", err)
	}

	// 2. Empty maillog_file rejected
	h2 := newMockPostfixHarness(t, "3.7.11", "", "")
	_, err = Install(InstallOptions{
		Listen:      "127.0.0.1:8025",
		BinaryPath:  h2.srcBin,
		PostconfBin: h2.postconfBin,
		PostfixBin:  h2.postfixBin,
		SkipChown:   true,
	})
	if err == nil || !strings.Contains(err.Error(), "maillog_file") {
		t.Errorf("expected maillog_file error, got: %v", err)
	}
}

func TestInstallIdempotencyAndUninstall(t *testing.T) {
	h := newMockPostfixHarness(t, "3.11.7", "/dev/stdout", "")

	// Pre-populate an existing header_checks table with a conflicting Subject rule.
	existingHC := filepath.Join(h.configDir, "custom_header_checks")
	if err := os.WriteFile(existingHC, []byte("/^Subject:/ WARN existing rule\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	origHCVal := "regexp:" + existingHC
	h.setMainParam(t, "header_checks", origHCVal)

	opts := InstallOptions{
		Listen:      "127.0.0.1:8025",
		BinaryPath:  h.srcBin,
		PostconfBin: h.postconfBin,
		PostfixBin:  h.postfixBin,
		SkipChown:   true,
	}

	// --- First Install ---
	rep1, err := Install(opts)
	if err != nil {
		t.Fatalf("first Install error: %v", err)
	}
	if !rep1.BinaryUpdated || !rep1.BackupCreated || !rep1.ConfigCreated || !rep1.PostfixReloaded {
		t.Errorf("unexpected first Install report flags: %+v", rep1)
	}
	if len(rep1.Warnings) != 1 || !strings.Contains(rep1.Warnings[0], existingHC+":1") {
		t.Errorf("expected 1 Subject conflict warning, got: %#v", rep1.Warnings)
	}

	// Verify binary copied with 0755
	binInfo, err := os.Stat(filepath.Join(h.daemonDir, binaryName))
	if err != nil {
		t.Fatalf("stat installed binary: %v", err)
	}
	if binInfo.Mode().Perm() != 0o755 {
		t.Errorf("installed binary mode = %o, want 0755", binInfo.Mode().Perm())
	}

	// Verify postfix-api.conf created with 0600 and tokens: []
	confPath := filepath.Join(h.configDir, configFileName)
	confInfo, err := os.Stat(confPath)
	if err != nil {
		t.Fatalf("stat config: %v", err)
	}
	if confInfo.Mode().Perm() != 0o600 {
		t.Errorf("config mode = %o, want 0600", confInfo.Mode().Perm())
	}
	confBytes, _ := os.ReadFile(confPath)
	if !strings.Contains(string(confBytes), "tokens: []") {
		t.Errorf("default config missing tokens: []")
	}

	// Verify master.cf and main.cf state
	if got := h.getMasterService(t, "postlog/unix-dgram"); got != "postlog unix-dgram n - n - 1 postfix-api" {
		t.Errorf("postlog/unix-dgram = %q", got)
	}
	if got := h.getMasterService(t, "127.0.0.1:8025/inet"); got != "127.0.0.1:8025 inet n n n 60 1 postfix-api" {
		t.Errorf("127.0.0.1:8025/inet = %q", got)
	}
	if got := h.getMainParam(t, "enable_long_queue_ids"); got != "yes" {
		t.Errorf("enable_long_queue_ids = %q, want yes", got)
	}
	wantHC := origHCVal + ", regexp:" + filepath.Join(h.configDir, headerChecksName)
	if got := h.getMainParam(t, "header_checks"); got != wantHC {
		t.Errorf("header_checks = %q, want %q", got, wantHC)
	}

	// Simulate operator customizing postfix-api.conf
	customConf := "tokens:\n  - name: custom\n"
	if err := os.WriteFile(confPath, []byte(customConf), 0o600); err != nil {
		t.Fatal(err)
	}

	// --- Second Install (Idempotency) ---
	rep2, err := Install(opts)
	if err != nil {
		t.Fatalf("second Install error: %v", err)
	}
	if rep2.BinaryUpdated || rep2.BackupCreated || rep2.ConfigCreated {
		t.Errorf("second Install should be idempotent, got: %+v", rep2)
	}
	// Verify header_checks was not duplicated
	if got := h.getMainParam(t, "header_checks"); got != wantHC {
		t.Errorf("after 2nd install header_checks = %q, want %q", got, wantHC)
	}
	// Verify custom config preserved
	if got, _ := os.ReadFile(confPath); string(got) != customConf {
		t.Errorf("custom config was overwritten: %q", string(got))
	}
	// Verify backup still holds original pre-install values
	backupRaw, err := os.ReadFile(rep1.BackupFile)
	if err != nil {
		t.Fatal(err)
	}
	var b InstallBackup
	if err := json.Unmarshal(backupRaw, &b); err != nil {
		t.Fatal(err)
	}
	if b.EnableLongQueueIDs != "no" || b.HeaderChecks != origHCVal || b.PostlogService != "postlog unix-dgram n - n - 1 postlogd" {
		t.Errorf("backup mutated on 2nd install: %+v", b)
	}

	// --- Uninstall ---
	err = Uninstall(UninstallOptions{
		PostconfBin: h.postconfBin,
		PostfixBin:  h.postfixBin,
	})
	if err != nil {
		t.Fatalf("Uninstall error: %v", err)
	}

	if got := h.getMasterService(t, "postlog/unix-dgram"); got != "postlog unix-dgram n - n - 1 postlogd" {
		t.Errorf("restored postlog/unix-dgram = %q", got)
	}
	if got := h.getMasterService(t, "127.0.0.1:8025/inet"); got != "" {
		t.Errorf("expected 127.0.0.1:8025/inet removed, got %q", got)
	}
	if got := h.getMainParam(t, "enable_long_queue_ids"); got != "no" {
		t.Errorf("restored enable_long_queue_ids = %q, want no", got)
	}
	if got := h.getMainParam(t, "header_checks"); got != origHCVal {
		t.Errorf("restored header_checks = %q, want %q", got, origHCVal)
	}
	if _, err := os.Stat(filepath.Join(h.configDir, headerChecksName)); !os.IsNotExist(err) {
		t.Errorf("expected %s removed", headerChecksName)
	}
	if _, err := os.Stat(filepath.Join(h.daemonDir, binaryName)); !os.IsNotExist(err) {
		t.Errorf("expected %s removed", binaryName)
	}
	// State dir and postfix-api.conf must be preserved
	if _, err := os.Stat(rep1.StateDir); err != nil {
		t.Errorf("expected state_dir preserved: %v", err)
	}
	if _, err := os.Stat(confPath); err != nil {
		t.Errorf("expected postfix-api.conf preserved: %v", err)
	}

	// Verify postfix check and reload were recorded
	pfLog, _ := os.ReadFile(h.postfixLog)
	if !strings.Contains(string(pfLog), "check") || !strings.Contains(string(pfLog), "reload") {
		t.Errorf("expected postfix check & reload in log, got:\n%s", string(pfLog))
	}
}

type mockPostfixHarness struct {
	root        string
	configDir   string
	daemonDir   string
	dataDir     string
	commandDir  string
	srcBin      string
	postconfBin string
	postfixBin  string
	mainDir     string
	masterDir   string
	postfixLog  string
}

func newMockPostfixHarness(t *testing.T, version, maillogFile, headerChecks string) *mockPostfixHarness {
	t.Helper()
	root := t.TempDir()
	h := &mockPostfixHarness{
		root:       root,
		configDir:  filepath.Join(root, "etc"),
		daemonDir:  filepath.Join(root, "libexec"),
		dataDir:    filepath.Join(root, "var-lib"),
		commandDir: filepath.Join(root, "sbin"),
		mainDir:    filepath.Join(root, "state-main"),
		masterDir:  filepath.Join(root, "state-master"),
		postfixLog: filepath.Join(root, "postfix.log"),
	}
	for _, d := range []string{h.configDir, h.daemonDir, h.dataDir, h.commandDir, h.mainDir, h.masterDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	h.srcBin = filepath.Join(root, "postfix-api-src")
	if err := os.WriteFile(h.srcBin, []byte("#!/bin/sh\necho fake-binary\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	h.setMainParam(t, "mail_version", version)
	h.setMainParam(t, "maillog_file", maillogFile)
	h.setMainParam(t, "command_directory", h.commandDir)
	h.setMainParam(t, "daemon_directory", h.daemonDir)
	h.setMainParam(t, "config_directory", h.configDir)
	h.setMainParam(t, "data_directory", h.dataDir)
	h.setMainParam(t, "mail_owner", "postfix")
	h.setMainParam(t, "header_checks", headerChecks)
	h.setMainParam(t, "enable_long_queue_ids", "no")

	// Default pre-existing postlog service in master.cf
	h.setMasterService(t, "postlog/unix-dgram", "postlog unix-dgram n - n - 1 postlogd")

	// Stateful mock postconf
	h.postconfBin = filepath.Join(h.commandDir, "postconf")
	postconfScript := `#!/bin/sh
MAIN_DIR="` + h.mainDir + `"
MASTER_DIR="` + h.masterDir + `"

if [ "$1" = "-c" ]; then
  shift 2
fi

case "$1" in
  -h)
    key="$2"
    if [ -f "$MAIN_DIR/$key" ]; then
      cat "$MAIN_DIR/$key"
    fi
    ;;
  -e)
    kv="$2"
    key="${kv%%=*}"
    val="${kv#*=}"
    printf '%s\n' "$val" > "$MAIN_DIR/$key"
    ;;
  -M)
    arg="$2"
    case "$arg" in
      *=*)
        svc="${arg%%=*}"
        val="${arg#*=}"
        safe=$(printf '%s' "$svc" | tr '/:' '__')
        printf '%s\n' "$val" > "$MASTER_DIR/$safe"
        ;;
      *)
        safe=$(printf '%s' "$arg" | tr '/:' '__')
        if [ -f "$MASTER_DIR/$safe" ]; then
          cat "$MASTER_DIR/$safe"
        fi
        ;;
    esac
    ;;
  -MX)
    safe=$(printf '%s' "$2" | tr '/:' '__')
    rm -f "$MASTER_DIR/$safe"
    ;;
  *)
    echo "unsupported postconf args: $*" >&2
    exit 1
    ;;
esac
`
	if err := os.WriteFile(h.postconfBin, []byte(postconfScript), 0o755); err != nil {
		t.Fatal(err)
	}

	// Mock postfix binary
	h.postfixBin = filepath.Join(h.commandDir, "postfix")
	postfixScript := `#!/bin/sh
if [ "$1" = "-c" ]; then
  shift 2
fi
printf '%s\n' "$1" >> "` + h.postfixLog + `"
exit 0
`
	if err := os.WriteFile(h.postfixBin, []byte(postfixScript), 0o755); err != nil {
		t.Fatal(err)
	}

	return h
}

func (h *mockPostfixHarness) setMainParam(t *testing.T, key, val string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(h.mainDir, key), []byte(val+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func (h *mockPostfixHarness) getMainParam(t *testing.T, key string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(h.mainDir, key))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func (h *mockPostfixHarness) setMasterService(t *testing.T, svc, val string) {
	t.Helper()
	safe := strings.NewReplacer("/", "_", ":", "_").Replace(svc)
	if err := os.WriteFile(filepath.Join(h.masterDir, safe), []byte(val+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func (h *mockPostfixHarness) getMasterService(t *testing.T, svc string) string {
	t.Helper()
	safe := strings.NewReplacer("/", "_", ":", "_").Replace(svc)
	b, err := os.ReadFile(filepath.Join(h.masterDir, safe))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
