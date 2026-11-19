// Package config loads, validates, and resolves configuration for postfix-api
// from <config_directory>/postfix-api.conf (YAML) and Postfix postconf(1).
package config

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	DefaultSegmentSizeBytes  int64         = 16 * 1024 * 1024  // 16 MiB
	DefaultMaxTotalBytes     int64         = 256 * 1024 * 1024 // 256 MiB
	MinSegmentSizeBytes      int64         = 64 * 1024         // 64 KiB
	DefaultRetentionDuration time.Duration = 168 * time.Hour   // 7 days
	DefaultQueueSyncInterval time.Duration = 5 * time.Minute
	MinQueueSyncInterval     time.Duration = 5 * time.Second
	DefaultIndexerBatchSize  int           = 1000
	MinIndexerBatchSize      int           = 1
	MaxIndexerBatchSize      int           = 10000

	ScopeRead    = "read"
	ScopeWrite   = "write"
	ScopeContent = "content"
)

// Config holds the parsed postfix-api.conf configuration.
type Config struct {
	StateDir          string        `yaml:"state_dir"`
	Spool             SpoolConfig   `yaml:"spool"`
	RetentionDuration time.Duration `yaml:"retention_duration"`
	QueueSyncInterval time.Duration `yaml:"queue_sync_interval"`
	IndexerBatchSize  int           `yaml:"indexer_batch_size"`
	SubjectInOutput   bool          `yaml:"subject_in_output"`
	TLS               TLSConfig     `yaml:"tls"`
	AllowPlaintext    bool          `yaml:"allow_plaintext"`
	Tokens            []Token       `yaml:"tokens"`
}

// SpoolConfig configures the bounded disk spool in <state_dir>/spool.
type SpoolConfig struct {
	SegmentSizeBytes int64 `yaml:"segment_size_bytes"`
	MaxTotalBytes    int64 `yaml:"max_total_bytes"`
}

// TLSConfig configures optional TLS for the HTTP API listener.
type TLSConfig struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// Token defines an authenticated API bearer token identified by its SHA-256 hex digest.
type Token struct {
	Name   string   `yaml:"name"`
	SHA256 string   `yaml:"sha256"`
	Scopes []string `yaml:"scopes"`

	digest       [32]byte
	digestParsed bool
}

// PostfixParams holds the 8 Postfix parameters read from postconf -h.
type PostfixParams struct {
	CommandDirectory string
	QueueDirectory   string
	DataDirectory    string
	DaemonDirectory  string
	ConfigDirectory  string
	MailOwner        string
	MaillogFile      string
	MailVersion      string
}

// DefaultConfig returns a Config populated with specification defaults.
func DefaultConfig() *Config {
	return &Config{
		Spool: SpoolConfig{
			SegmentSizeBytes: DefaultSegmentSizeBytes,
			MaxTotalBytes:    DefaultMaxTotalBytes,
		},
		RetentionDuration: DefaultRetentionDuration,
		QueueSyncInterval: DefaultQueueSyncInterval,
		IndexerBatchSize:  DefaultIndexerBatchSize,
		SubjectInOutput:   false,
		AllowPlaintext:    false,
		Tokens:            []Token{},
	}
}

// LoadFile reads and validates a YAML config file at path. When running as root
// (euid == 0), it refuses group- or world-accessible files (perm & 0o077 != 0).
func LoadFile(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("config: open %q: %w", path, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("config: stat %q: %w", path, err)
	}
	if os.Geteuid() == 0 {
		if err := ValidateFilePermissions(info.Mode(), path); err != nil {
			return nil, err
		}
	}

	return LoadFromReader(f)
}

// ValidateFilePermissions checks that mode has no group or other permissions
// (perm & 0o077 == 0, allowing 0600 and 0400 read-only root secret mounts).
func ValidateFilePermissions(mode fs.FileMode, path string) error {
	if mode.Perm()&0o077 != 0 {
		return fmt.Errorf("config: %q has insecure permissions %04o; run chmod 0600 %s", path, mode.Perm(), path)
	}
	return nil
}

// LoadFromReader decodes and validates YAML configuration from r using strict
// unknown-field checking (KnownFields(true)).
func LoadFromReader(r io.Reader) (*Config, error) {
	cfg := DefaultConfig()
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)

	err := dec.Decode(cfg)
	if err != nil {
		if errors.Is(err, io.EOF) {
			if err := cfg.Validate(); err != nil {
				return nil, err
			}
			return cfg, nil
		}
		return nil, fmt.Errorf("config: parse yaml: %w", err)
	}

	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("config: unexpected trailing YAML document")
		}
		return nil, fmt.Errorf("config: parse trailing yaml: %w", err)
	}

	if cfg.Tokens == nil {
		cfg.Tokens = []Token{}
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate checks all configuration invariants and normalizes token SHA-256 digests.
func (c *Config) Validate() error {
	if c == nil {
		return errors.New("config: nil config")
	}
	if c.StateDir != "" && !filepath.IsAbs(c.StateDir) {
		return fmt.Errorf("config: state_dir %q must be an absolute path", c.StateDir)
	}
	if c.Spool.SegmentSizeBytes < MinSegmentSizeBytes {
		return fmt.Errorf("config: spool.segment_size_bytes (%d) must be >= %d", c.Spool.SegmentSizeBytes, MinSegmentSizeBytes)
	}
	if c.Spool.MaxTotalBytes < c.Spool.SegmentSizeBytes {
		return fmt.Errorf("config: spool.max_total_bytes (%d) must be >= spool.segment_size_bytes (%d)", c.Spool.MaxTotalBytes, c.Spool.SegmentSizeBytes)
	}
	if c.RetentionDuration <= 0 {
		return fmt.Errorf("config: retention_duration (%s) must be > 0", c.RetentionDuration)
	}
	if c.QueueSyncInterval < MinQueueSyncInterval {
		return fmt.Errorf("config: queue_sync_interval (%s) must be >= %s", c.QueueSyncInterval, MinQueueSyncInterval)
	}
	if c.IndexerBatchSize < MinIndexerBatchSize || c.IndexerBatchSize > MaxIndexerBatchSize {
		return fmt.Errorf("config: indexer_batch_size (%d) must be in [%d, %d]", c.IndexerBatchSize, MinIndexerBatchSize, MaxIndexerBatchSize)
	}
	if (c.TLS.CertFile == "") != (c.TLS.KeyFile == "") {
		return errors.New("config: tls.cert_file and tls.key_file must both be set or both be empty")
	}

	seenNames := make(map[string]struct{}, len(c.Tokens))
	for i := range c.Tokens {
		tok := &c.Tokens[i]
		if tok.Name == "" {
			return fmt.Errorf("config: tokens[%d].name must not be empty", i)
		}
		if _, dup := seenNames[tok.Name]; dup {
			return fmt.Errorf("config: duplicate token name %q", tok.Name)
		}
		seenNames[tok.Name] = struct{}{}

		digest, normHex, err := decodeSHA256Hex(tok.SHA256)
		if err != nil {
			return fmt.Errorf("config: token %q: %w", tok.Name, err)
		}
		tok.SHA256 = normHex
		tok.digest = digest
		tok.digestParsed = true

		if len(tok.Scopes) == 0 {
			return fmt.Errorf("config: token %q must have at least one scope", tok.Name)
		}
		for _, s := range tok.Scopes {
			switch s {
			case ScopeRead, ScopeWrite, ScopeContent:
			default:
				return fmt.Errorf("config: token %q has invalid scope %q", tok.Name, s)
			}
		}
	}

	return nil
}

func decodeSHA256Hex(s string) ([32]byte, string, error) {
	var out [32]byte
	if len(s) != 64 {
		return out, "", fmt.Errorf("sha256 must be 64 hex characters, got %d", len(s))
	}
	norm := strings.ToLower(s)
	raw, err := hex.DecodeString(norm)
	if err != nil || len(raw) != 32 {
		return out, "", errors.New("sha256 must be valid hexadecimal")
	}
	copy(out[:], raw)
	return out, norm, nil
}

// ResolveStateDir sets StateDir to "<dataDir>/postfix-api" when StateDir is empty
// and verifies the resulting path is absolute.
func (c *Config) ResolveStateDir(dataDir string) (string, error) {
	if c.StateDir == "" {
		if dataDir == "" {
			return "", errors.New("config: cannot resolve state_dir: data_directory is empty")
		}
		c.StateDir = filepath.Join(dataDir, "postfix-api")
	}
	if !filepath.IsAbs(c.StateDir) {
		return "", fmt.Errorf("config: resolved state_dir %q is not absolute", c.StateDir)
	}
	return c.StateDir, nil
}

// VerifyToken checks rawToken against all configured tokens using constant-time
// SHA-256 comparison without short-circuiting.
func (c *Config) VerifyToken(rawToken string) (*Token, bool) {
	if c == nil || rawToken == "" || len(c.Tokens) == 0 {
		return nil, false
	}
	sum := sha256.Sum256([]byte(rawToken))
	var matched *Token
	for i := range c.Tokens {
		tok := &c.Tokens[i]
		digest := tok.digest
		if !tok.digestParsed {
			var err error
			digest, _, err = decodeSHA256Hex(tok.SHA256)
			if err != nil {
				continue
			}
		}
		if subtle.ConstantTimeCompare(sum[:], digest[:]) == 1 {
			matched = tok
		}
	}
	if matched == nil {
		return nil, false
	}
	return matched, true
}

// HasScope returns true if the token explicitly grants scope.
func (t *Token) HasScope(scope string) bool {
	if t == nil {
		return false
	}
	for _, s := range t.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

var postconfFallbackPaths = []string{
	"/usr/sbin/postconf",
	"/usr/local/sbin/postconf",
	"/sbin/postconf",
}

var postconfKeys = []string{
	"command_directory",
	"queue_directory",
	"data_directory",
	"daemon_directory",
	"config_directory",
	"mail_owner",
	"maillog_file",
	"mail_version",
}

// ResolvePostconfBin finds the postconf executable via PATH or standard sbin locations.
func ResolvePostconfBin() (string, error) {
	if p, err := exec.LookPath("postconf"); err == nil {
		return p, nil
	}
	for _, p := range postconfFallbackPaths {
		if info, err := os.Stat(p); err == nil && !info.IsDir() {
			return p, nil
		}
	}
	return "", errors.New("config: postconf binary not found in PATH or /usr/sbin, /usr/local/sbin, /sbin")
}

// ReadPostconf executes postconf -h to retrieve the 8 required Postfix parameters.
// If postconfBin is empty, ResolvePostconfBin is used. If MAIL_CONFIG is set, -c
// is passed to postconf.
func ReadPostconf(postconfBin string) (*PostfixParams, error) {
	bin := postconfBin
	if bin == "" {
		var err error
		bin, err = ResolvePostconfBin()
		if err != nil {
			return nil, err
		}
	}

	args := make([]string, 0, len(postconfKeys)+3)
	if mailCfg := os.Getenv("MAIL_CONFIG"); mailCfg != "" {
		args = append(args, "-c", mailCfg)
	}
	args = append(args, "-h")
	args = append(args, postconfKeys...)

	var stdout, stderr bytes.Buffer
	cmd := exec.Command(bin, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("config: postconf -h failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}

	return ParsePostconfOutput(stdout.String())
}

// ParsePostconfOutput parses the 8 lines returned by postconf -h for
// command_directory, queue_directory, data_directory, daemon_directory,
// config_directory, mail_owner, maillog_file, and mail_version.
func ParsePostconfOutput(out string) (*PostfixParams, error) {
	trimmed := strings.TrimSuffix(out, "\n")
	lines := strings.Split(trimmed, "\n")
	if len(lines) != len(postconfKeys) {
		return nil, fmt.Errorf("config: postconf output line count = %d; want %d", len(lines), len(postconfKeys))
	}
	for i := range lines {
		lines[i] = strings.TrimRight(lines[i], "\r")
	}

	params := &PostfixParams{
		CommandDirectory: lines[0],
		QueueDirectory:   lines[1],
		DataDirectory:    lines[2],
		DaemonDirectory:  lines[3],
		ConfigDirectory:  lines[4],
		MailOwner:        lines[5],
		MaillogFile:      lines[6],
		MailVersion:      lines[7],
	}

	if err := ValidateMailVersion(params.MailVersion); err != nil {
		return nil, err
	}
	return params, nil
}

// ValidateMailVersion verifies Postfix mail_version is >= 3.5 after stripping
// distribution/snapshot suffixes such as "-20260101" or "~deb12u1".
func ValidateMailVersion(version string) error {
	if version == "" {
		return errors.New("config: mail_version is empty")
	}
	base := version
	if idx := strings.IndexAny(base, "-~+ "); idx >= 0 {
		base = base[:idx]
	}
	parts := strings.Split(base, ".")
	if len(parts) < 2 {
		return fmt.Errorf("config: invalid mail_version %q", version)
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return fmt.Errorf("config: invalid mail_version major %q: %w", version, err)
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return fmt.Errorf("config: invalid mail_version minor %q: %w", version, err)
	}
	if major < 3 || (major == 3 && minor < 5) {
		return fmt.Errorf("config: Postfix version %q is unsupported; minimum required is 3.5", version)
	}
	return nil
}

// DropPrivileges permanently drops supplementary groups, GID, and UID to mailOwner.
// Safe on Linux Go >= 1.16 via runtime AllThreadsSyscall support.
func DropPrivileges(mailOwner string) error {
	if mailOwner == "" {
		return errors.New("config: mail_owner is empty")
	}
	u, err := user.Lookup(mailOwner)
	if err != nil {
		return fmt.Errorf("config: lookup mail_owner %q: %w", mailOwner, err)
	}
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		return fmt.Errorf("config: invalid uid %q for %q: %w", u.Uid, mailOwner, err)
	}
	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		return fmt.Errorf("config: invalid gid %q for %q: %w", u.Gid, mailOwner, err)
	}

	if os.Getuid() == uid && os.Geteuid() == uid && os.Getgid() == gid && os.Getegid() == gid {
		return nil
	}

	if err := syscall.Setgroups([]int{gid}); err != nil {
		return fmt.Errorf("config: setgroups([%d]): %w", gid, err)
	}
	if err := syscall.Setgid(gid); err != nil {
		return fmt.Errorf("config: setgid(%d): %w", gid, err)
	}
	if err := syscall.Setuid(uid); err != nil {
		return fmt.Errorf("config: setuid(%d): %w", uid, err)
	}

	if os.Getuid() != uid || os.Geteuid() != uid || os.Getgid() != gid || os.Getegid() != gid {
		return fmt.Errorf("config: privilege drop verification failed (got uid=%d euid=%d gid=%d egid=%d; want uid=%d gid=%d)",
			os.Getuid(), os.Geteuid(), os.Getgid(), os.Getegid(), uid, gid)
	}
	return nil
}
