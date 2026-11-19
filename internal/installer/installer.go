package installer

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	binaryName        = "postfix-api"
	configFileName    = "postfix-api.conf"
	headerChecksName  = "postfix-api-header-checks"
	backupFileName    = "install-backup.json"
	headerChecksRule  = "/^Subject:/ INFO\n"
	defaultConfigYAML = `# postfix-api configuration (mode 0600, root:root)
# Optional directory for spool and SQLite state DB.
# Defaults to "<data_directory>/postfix-api" when empty.
state_dir: ""

spool:
  segment_size_bytes: 16777216    # 16 MiB
  max_total_bytes: 268435456      # 256 MiB

retention_duration: 168h          # 7 days after removal from queue
queue_sync_interval: 5m           # postqueue -j reconciliation interval
indexer_batch_size: 1000          # spool lines committed per SQLite transaction
subject_in_output: false          # keep Subject header_check lines out of maillog_file by default

tls:
  cert_file: ""                   # PEM certificate chain path
  key_file: ""                    # PEM private key path

allow_plaintext: false            # must be true to serve HTTP without TLS on non-loopback addrs

tokens: []
`
)

// InstallOptions configures the 9-step idempotent Postfix installation.
type InstallOptions struct {
	Listen      string
	ConfigDir   string
	BinaryPath  string
	PostconfBin string
	PostfixBin  string
	SkipChown   bool
}

// UninstallOptions configures Postfix uninstallation and configuration restoration.
type UninstallOptions struct {
	ConfigDir   string
	PostconfBin string
	PostfixBin  string
}

// InstallBackup records the pre-install Postfix configuration so Uninstall can restore it.
type InstallBackup struct {
	InstalledAt        string `json:"installed_at"`
	Listen             string `json:"listen"`
	PostlogService     string `json:"postlog_service"`
	PostlogExisted     bool   `json:"postlog_existed"`
	HeaderChecks       string `json:"header_checks"`
	EnableLongQueueIDs string `json:"enable_long_queue_ids"`
}

// InstallReport summarizes actions taken and any warnings detected during Install.
type InstallReport struct {
	PostfixVersion   string
	BinaryDest       string
	BinaryUpdated    bool
	StateDir         string
	ConfigFile       string
	ConfigCreated    bool
	HeaderChecksFile string
	BackupFile       string
	BackupCreated    bool
	PostfixReloaded  bool
	Warnings         []string
}

// Install performs the 9 idempotent steps from SPEC §9.2 to integrate postfix-api with Postfix.
func Install(opts InstallOptions) (*InstallReport, error) {
	if err := ValidateListenAddr(opts.Listen); err != nil {
		return nil, err
	}

	postconfBin, err := resolvePostconf(opts.PostconfBin)
	if err != nil {
		return nil, err
	}
	pc := &cliRunner{bin: postconfBin, configDir: opts.ConfigDir}

	// Step 1: Pre-flight validation (mail_version >= 3.5 and maillog_file non-empty).
	mailVersion, err := pc.postconfGet("mail_version")
	if err != nil {
		return nil, fmt.Errorf("read mail_version: %w", err)
	}
	if err := CheckPostfixVersion(mailVersion); err != nil {
		return nil, err
	}

	maillogFile, err := pc.postconfGet("maillog_file")
	if err != nil {
		return nil, fmt.Errorf("read maillog_file: %w", err)
	}
	if strings.TrimSpace(maillogFile) == "" {
		return nil, errors.New("Postfix maillog_file is empty; configure maillog_file (e.g. /dev/stdout or /var/log/postfix.log) before installing postfix-api")
	}

	commandDir, err := pc.postconfGet("command_directory")
	if err != nil {
		return nil, fmt.Errorf("read command_directory: %w", err)
	}
	daemonDir, err := pc.postconfGet("daemon_directory")
	if err != nil {
		return nil, fmt.Errorf("read daemon_directory: %w", err)
	}
	configDir, err := pc.postconfGet("config_directory")
	if err != nil {
		return nil, fmt.Errorf("read config_directory: %w", err)
	}
	dataDir, err := pc.postconfGet("data_directory")
	if err != nil {
		return nil, fmt.Errorf("read data_directory: %w", err)
	}
	mailOwner, err := pc.postconfGet("mail_owner")
	if err != nil {
		return nil, fmt.Errorf("read mail_owner: %w", err)
	}
	origHeaderChecks, err := pc.postconfGet("header_checks")
	if err != nil {
		return nil, fmt.Errorf("read header_checks: %w", err)
	}
	origLongQueueIDs, err := pc.postconfGet("enable_long_queue_ids")
	if err != nil {
		return nil, fmt.Errorf("read enable_long_queue_ids: %w", err)
	}

	report := &InstallReport{
		PostfixVersion: mailVersion,
	}

	// Step 2: Copy binary to <daemon_directory>/postfix-api (mode 0755, atomic rename).
	srcBin := opts.BinaryPath
	if srcBin == "" {
		srcBin, err = os.Executable()
		if err != nil {
			return nil, fmt.Errorf("resolve current executable: %w", err)
		}
	}
	dstBin := filepath.Join(daemonDir, binaryName)
	report.BinaryDest = dstBin
	updated, err := installBinaryAtomic(srcBin, dstBin)
	if err != nil {
		return nil, fmt.Errorf("install binary to %s: %w", dstBin, err)
	}
	report.BinaryUpdated = updated

	// Step 3 (and state_dir creation for step 8): Create <state_dir> and save idempotent backup.
	stateDir := filepath.Join(dataDir, binaryName)
	report.StateDir = stateDir
	if err := ensureStateDirs(stateDir, mailOwner, opts.SkipChown); err != nil {
		return nil, err
	}

	backupPath := filepath.Join(stateDir, backupFileName)
	report.BackupFile = backupPath

	var backup InstallBackup
	backupBytes, readErr := os.ReadFile(backupPath)
	if errors.Is(readErr, os.ErrNotExist) {
		postlogOut, mErr := pc.run("-M", "postlog/unix-dgram")
		if mErr != nil {
			return nil, fmt.Errorf("read existing master.cf postlog/unix-dgram: %w", mErr)
		}
		postlogLine := strings.TrimSpace(postlogOut)
		backup = InstallBackup{
			InstalledAt:        time.Now().UTC().Format(time.RFC3339),
			Listen:             opts.Listen,
			PostlogService:     postlogLine,
			PostlogExisted:     postlogLine != "",
			HeaderChecks:       origHeaderChecks,
			EnableLongQueueIDs: origLongQueueIDs,
		}
		if err := writeBackupFile(backupPath, backup); err != nil {
			return nil, err
		}
		if err := chownPath(backupPath, mailOwner, opts.SkipChown); err != nil {
			return nil, err
		}
		report.BackupCreated = true
	} else if readErr != nil {
		return nil, fmt.Errorf("read existing backup %s: %w", backupPath, readErr)
	} else {
		if err := json.Unmarshal(backupBytes, &backup); err != nil {
			return nil, fmt.Errorf("parse existing backup %s: %w", backupPath, err)
		}
	}

	// Step 4: Configure postlog/unix-dgram and <listen>/inet in master.cf via postconf -M.
	if backup.Listen != "" && backup.Listen != opts.Listen {
		if _, err := pc.run("-MX", backup.Listen+"/inet"); err != nil {
			return nil, fmt.Errorf("remove previous master.cf service %s/inet: %w", backup.Listen, err)
		}
		backup.Listen = opts.Listen
		if err := writeBackupFile(backupPath, backup); err != nil {
			return nil, err
		}
	}
	if _, err := pc.run("-M", "postlog/unix-dgram=postlog unix-dgram n - n - 1 postfix-api"); err != nil {
		return nil, fmt.Errorf("configure postlog/unix-dgram in master.cf: %w", err)
	}
	inetSpec := fmt.Sprintf("%s/inet=%s inet n n n 60 1 postfix-api", opts.Listen, opts.Listen)
	if _, err := pc.run("-M", inetSpec); err != nil {
		return nil, fmt.Errorf("configure %s/inet in master.cf: %w", opts.Listen, err)
	}

	// Step 5: Enable long queue IDs.
	if _, err := pc.run("-e", "enable_long_queue_ids=yes"); err != nil {
		return nil, fmt.Errorf("enable_long_queue_ids=yes: %w", err)
	}

	// Step 6: Write Subject header_checks file and append regexp:<path> if not present.
	hcFile := filepath.Join(configDir, headerChecksName)
	report.HeaderChecksFile = hcFile
	if err := ensureFileContent(hcFile, []byte(headerChecksRule), 0o644); err != nil {
		return nil, fmt.Errorf("write %s: %w", hcFile, err)
	}

	hcEntry := "regexp:" + hcFile
	finalHeaderChecks := origHeaderChecks
	if !containsHeaderCheckEntry(origHeaderChecks, hcEntry) {
		if strings.TrimSpace(origHeaderChecks) == "" {
			finalHeaderChecks = hcEntry
		} else {
			finalHeaderChecks = strings.TrimSpace(origHeaderChecks) + ", " + hcEntry
		}
		if _, err := pc.run("-e", "header_checks="+finalHeaderChecks); err != nil {
			return nil, fmt.Errorf("update header_checks: %w", err)
		}
	}

	// Step 7: Scan existing header_checks tables listed before postfix-api-header-checks for Subject conflicts.
	report.Warnings = ScanHeaderChecksConflicts(finalHeaderChecks, hcFile)

	// Step 8: Create <config_directory>/postfix-api.conf (mode 0600, tokens: []) if missing.
	confFile := filepath.Join(configDir, configFileName)
	report.ConfigFile = confFile
	if _, statErr := os.Stat(confFile); errors.Is(statErr, os.ErrNotExist) {
		if err := os.WriteFile(confFile, []byte(defaultConfigYAML), 0o600); err != nil {
			return nil, fmt.Errorf("write default config %s: %w", confFile, err)
		}
		report.ConfigCreated = true
	} else if statErr != nil {
		return nil, fmt.Errorf("stat config %s: %w", confFile, statErr)
	}

	// Step 9: Run postfix check; if postfix status exits 0, run postfix reload.
	postfixBin := opts.PostfixBin
	if postfixBin == "" {
		postfixBin = filepath.Join(commandDir, "postfix")
	}
	pf := &cliRunner{bin: postfixBin, configDir: opts.ConfigDir}
	if _, err := pf.run("check"); err != nil {
		return nil, fmt.Errorf("postfix check failed: %w", err)
	}
	if pf.checkStatus() {
		if _, err := pf.run("reload"); err != nil {
			return nil, fmt.Errorf("postfix reload failed: %w", err)
		}
		report.PostfixReloaded = true
	}

	return report, nil
}

// Uninstall restores pre-install Postfix master.cf and main.cf settings from install-backup.json,
// removes the header_checks file and installed binary, preserves <state_dir> and postfix-api.conf,
// and validates/reloads Postfix.
func Uninstall(opts UninstallOptions) error {
	postconfBin, err := resolvePostconf(opts.PostconfBin)
	if err != nil {
		return err
	}
	pc := &cliRunner{bin: postconfBin, configDir: opts.ConfigDir}

	commandDir, err := pc.postconfGet("command_directory")
	if err != nil {
		return fmt.Errorf("read command_directory: %w", err)
	}
	daemonDir, err := pc.postconfGet("daemon_directory")
	if err != nil {
		return fmt.Errorf("read daemon_directory: %w", err)
	}
	configDir, err := pc.postconfGet("config_directory")
	if err != nil {
		return fmt.Errorf("read config_directory: %w", err)
	}
	dataDir, err := pc.postconfGet("data_directory")
	if err != nil {
		return fmt.Errorf("read data_directory: %w", err)
	}

	stateDir := filepath.Join(dataDir, binaryName)
	backupPath := filepath.Join(stateDir, backupFileName)
	raw, err := os.ReadFile(backupPath)
	if err != nil {
		return fmt.Errorf("read install backup %s: %w", backupPath, err)
	}
	var backup InstallBackup
	if err := json.Unmarshal(raw, &backup); err != nil {
		return fmt.Errorf("parse install backup %s: %w", backupPath, err)
	}

	// Restore master.cf
	if backup.Listen != "" {
		if _, err := pc.run("-MX", backup.Listen+"/inet"); err != nil {
			return fmt.Errorf("remove %s/inet from master.cf: %w", backup.Listen, err)
		}
	}
	if backup.PostlogExisted {
		if _, err := pc.run("-M", "postlog/unix-dgram="+backup.PostlogService); err != nil {
			return fmt.Errorf("restore postlog/unix-dgram in master.cf: %w", err)
		}
	} else {
		if _, err := pc.run("-MX", "postlog/unix-dgram"); err != nil {
			return fmt.Errorf("remove postlog/unix-dgram from master.cf: %w", err)
		}
	}

	// Restore main.cf
	if _, err := pc.run("-e", "enable_long_queue_ids="+backup.EnableLongQueueIDs); err != nil {
		return fmt.Errorf("restore enable_long_queue_ids: %w", err)
	}
	if _, err := pc.run("-e", "header_checks="+backup.HeaderChecks); err != nil {
		return fmt.Errorf("restore header_checks: %w", err)
	}

	// Remove postfix-api-header-checks and installed binary; preserve <state_dir> and postfix-api.conf.
	if err := removeIgnoreNotExist(filepath.Join(configDir, headerChecksName)); err != nil {
		return err
	}
	if err := removeIgnoreNotExist(filepath.Join(daemonDir, binaryName)); err != nil {
		return err
	}

	// Validate and reload Postfix if running.
	postfixBin := opts.PostfixBin
	if postfixBin == "" {
		postfixBin = filepath.Join(commandDir, "postfix")
	}
	pf := &cliRunner{bin: postfixBin, configDir: opts.ConfigDir}
	if _, err := pf.run("check"); err != nil {
		return fmt.Errorf("postfix check failed: %w", err)
	}
	if pf.checkStatus() {
		if _, err := pf.run("reload"); err != nil {
			return fmt.Errorf("postfix reload failed: %w", err)
		}
	}
	return nil
}

type cliRunner struct {
	bin       string
	configDir string
}

func (c *cliRunner) buildArgs(args ...string) []string {
	if c.configDir == "" {
		return args
	}
	out := make([]string, 0, len(args)+2)
	out = append(out, "-c", c.configDir)
	out = append(out, args...)
	return out
}

func (c *cliRunner) run(args ...string) (string, error) {
	cmd := exec.Command(c.bin, c.buildArgs(args...)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = strings.TrimSpace(stdout.String())
		}
		if detail != "" {
			return "", fmt.Errorf("%s %s: %w (%s)", c.bin, strings.Join(args, " "), err, detail)
		}
		return "", fmt.Errorf("%s %s: %w", c.bin, strings.Join(args, " "), err)
	}
	return stdout.String(), nil
}

func (c *cliRunner) postconfGet(param string) (string, error) {
	out, err := c.run("-h", param)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

func (c *cliRunner) checkStatus() bool {
	cmd := exec.Command(c.bin, c.buildArgs("status")...)
	return cmd.Run() == nil
}

func resolvePostconf(explicit string) (string, error) {
	if strings.TrimSpace(explicit) != "" {
		return explicit, nil
	}
	if p, err := exec.LookPath("postconf"); err == nil {
		return p, nil
	}
	for _, candidate := range []string{"/usr/sbin/postconf", "/usr/local/sbin/postconf", "/sbin/postconf"} {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		}
	}
	return "", errors.New("postconf binary not found in PATH or standard sbin directories")
}

func installBinaryAtomic(src, dst string) (bool, error) {
	if filepath.Clean(src) == filepath.Clean(dst) {
		return false, nil
	}
	srcInfo, err := os.Stat(src)
	if err != nil {
		return false, err
	}
	if dstInfo, err := os.Stat(dst); err == nil {
		if os.SameFile(srcInfo, dstInfo) {
			return false, nil
		}
		srcHash, err1 := fileSHA256(src)
		dstHash, err2 := fileSHA256(dst)
		if err1 == nil && err2 == nil && srcHash == dstHash {
			return false, nil
		}
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return false, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".postfix-api.tmp.*")
	if err != nil {
		return false, err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	in, err := os.Open(src)
	if err != nil {
		_ = tmp.Close()
		return false, err
	}
	_, copyErr := io.Copy(tmp, in)
	inErr := in.Close()
	closeErr := tmp.Close()
	if copyErr != nil {
		return false, copyErr
	}
	if inErr != nil {
		return false, inErr
	}
	if closeErr != nil {
		return false, closeErr
	}

	if err := os.Chmod(tmpName, 0o755); err != nil {
		return false, err
	}
	if err := os.Rename(tmpName, dst); err != nil {
		return false, err
	}
	cleanup = false
	return true, nil
}

func fileSHA256(path string) ([32]byte, error) {
	var zero [32]byte
	f, err := os.Open(path)
	if err != nil {
		return zero, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return zero, err
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum, nil
}

func ensureStateDirs(stateDir, mailOwner string, skipChown bool) error {
	spoolDir := filepath.Join(stateDir, "spool")
	if err := os.MkdirAll(spoolDir, 0o700); err != nil {
		return fmt.Errorf("create state dir %s: %w", stateDir, err)
	}
	if err := os.Chmod(stateDir, 0o700); err != nil {
		return fmt.Errorf("chmod state dir %s: %w", stateDir, err)
	}
	if err := os.Chmod(spoolDir, 0o700); err != nil {
		return fmt.Errorf("chmod spool dir %s: %w", spoolDir, err)
	}

	if err := chownPath(stateDir, mailOwner, skipChown); err != nil {
		return err
	}
	if err := chownPath(spoolDir, mailOwner, skipChown); err != nil {
		return err
	}
	return nil
}

func chownPath(path, mailOwner string, skipChown bool) error {
	if skipChown || os.Geteuid() != 0 || strings.TrimSpace(mailOwner) == "" {
		return nil
	}
	u, err := user.Lookup(mailOwner)
	if err != nil {
		return fmt.Errorf("lookup mail_owner %q: %w", mailOwner, err)
	}
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		return fmt.Errorf("parse uid for %q: %w", mailOwner, err)
	}
	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		return fmt.Errorf("parse gid for %q: %w", mailOwner, err)
	}
	if err := os.Chown(path, uid, gid); err != nil {
		return fmt.Errorf("chown %s to %s: %w", path, mailOwner, err)
	}
	return nil
}

func writeBackupFile(path string, backup InstallBackup) error {
	data, err := json.MarshalIndent(backup, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal install backup: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write install backup %s: %w", path, err)
	}
	return nil
}

func ensureFileContent(path string, want []byte, perm os.FileMode) error {
	existing, err := os.ReadFile(path)
	if err == nil && bytes.Equal(existing, want) {
		return nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.WriteFile(path, want, perm)
}

func containsHeaderCheckEntry(headerChecksVal, target string) bool {
	for _, entry := range splitHeaderChecks(headerChecksVal) {
		if entry == target {
			return true
		}
	}
	return false
}

func removeIgnoreNotExist(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove %s: %w", path, err)
	}
	return nil
}
