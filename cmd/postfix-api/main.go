// Command postfix-api provides the Postfix master(8)-managed log receiver,
// authenticated HTTP observability & queue-control API, root helper, and
// installation CLI.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/PretorianX/postfix-http-api/internal/api"
	"github.com/PretorianX/postfix-http-api/internal/config"
	"github.com/PretorianX/postfix-http-api/internal/indexer"
	"github.com/PretorianX/postfix-http-api/internal/installer"
	"github.com/PretorianX/postfix-http-api/internal/logreceiver"
	"github.com/PretorianX/postfix-http-api/internal/masterproto"
	"github.com/PretorianX/postfix-http-api/internal/postfixcmd"
	"github.com/PretorianX/postfix-http-api/internal/spool"
	"github.com/PretorianX/postfix-http-api/internal/store"
)

// version is set at build time via -ldflags="-X main.version=..."
var version = "dev"

const configFileName = "postfix-api.conf"

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}

	switch args[0] {
	case "install":
		return runInstall(args[1:], stdout, stderr)
	case "uninstall":
		return runUninstall(args[1:], stdout, stderr)
	case "token":
		return runToken(args[1:], stdout, stderr)
	case "version":
		if len(args) > 1 {
			fmt.Fprintln(stderr, "usage: postfix-api version")
			return 2
		}
		fmt.Fprintln(stdout, version)
		return 0
	case "_root-helper":
		return runRootHelper(args[1:], stdin, stdout, stderr)
	case "help", "--help":
		printUsage(stdout)
		return 0
	case "-h":
		if len(args) == 1 {
			printUsage(stdout)
			return 0
		}
		return runMasterCmd(args, stderr)
	default:
		return runMasterCmd(args, stderr)
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage:
  postfix-api install --listen <addr:port> [--config-dir <dir>]
  postfix-api uninstall [--config-dir <dir>]
  postfix-api token new --name <name> --scopes <csv>
  postfix-api version
  postfix-api _root-helper --command-dir <dir>
  postfix-api -t <unix-dgram|inet> [master flags]`)
}

func runInstall(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("install", flag.ContinueOnError)
	fs.SetOutput(stderr)
	listen := fs.String("listen", "", "HTTP/HTTPS listen address (<addr>:<port>)")
	configDir := fs.String("config-dir", "", "Postfix config_directory override")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*listen) == "" {
		fmt.Fprintln(stderr, "usage: postfix-api install --listen <addr:port> [--config-dir <dir>]")
		return 2
	}

	report, err := installer.Install(installer.InstallOptions{
		Listen:    *listen,
		ConfigDir: *configDir,
	})
	if err != nil {
		fmt.Fprintf(stderr, "postfix-api install: %v\n", err)
		return 1
	}

	for _, w := range report.Warnings {
		fmt.Fprintln(stderr, w)
	}
	fmt.Fprintf(stdout, "Installed postfix-api (Postfix %s)\n", report.PostfixVersion)
	fmt.Fprintf(stdout, "  binary:        %s (updated=%v)\n", report.BinaryDest, report.BinaryUpdated)
	fmt.Fprintf(stdout, "  state_dir:     %s\n", report.StateDir)
	fmt.Fprintf(stdout, "  config:        %s (created=%v)\n", report.ConfigFile, report.ConfigCreated)
	fmt.Fprintf(stdout, "  header_checks: %s\n", report.HeaderChecksFile)
	fmt.Fprintf(stdout, "  backup:        %s (created=%v)\n", report.BackupFile, report.BackupCreated)
	fmt.Fprintf(stdout, "  reloaded:      %v\n", report.PostfixReloaded)
	return 0
}

func runUninstall(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("uninstall", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configDir := fs.String("config-dir", "", "Postfix config_directory override")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintln(stderr, "usage: postfix-api uninstall [--config-dir <dir>]")
		return 2
	}

	if err := installer.Uninstall(installer.UninstallOptions{ConfigDir: *configDir}); err != nil {
		fmt.Fprintf(stderr, "postfix-api uninstall: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, "Uninstalled postfix-api (state_dir and postfix-api.conf preserved)")
	return 0
}

func runToken(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "new" {
		fmt.Fprintln(stderr, "usage: postfix-api token new --name <name> --scopes <csv>")
		return 2
	}

	fs := flag.NewFlagSet("token new", flag.ContinueOnError)
	fs.SetOutput(stderr)
	name := fs.String("name", "", "token name")
	scopesCSV := fs.String("scopes", "", "comma-separated scopes (read,write,content)")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*name) == "" || strings.TrimSpace(*scopesCSV) == "" {
		fmt.Fprintln(stderr, "usage: postfix-api token new --name <name> --scopes <csv>")
		return 2
	}

	rawScopes := strings.Split(*scopesCSV, ",")
	rawToken, sha256Hex, yamlSnippet, err := installer.GenerateToken(*name, rawScopes)
	if err != nil {
		fmt.Fprintf(stderr, "postfix-api token new: %v\n", err)
		return 1
	}

	fmt.Fprintf(stdout, "Token (store securely; shown only once):\n%s\n\nSHA-256: %s\n\nAdd to postfix-api.conf under tokens:\n%s",
		rawToken, sha256Hex, yamlSnippet)
	return 0
}

func runRootHelper(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("_root-helper", flag.ContinueOnError)
	fs.SetOutput(stderr)
	commandDir := fs.String("command-dir", "", "Postfix command_directory")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || strings.TrimSpace(*commandDir) == "" {
		fmt.Fprintln(stderr, "usage: postfix-api _root-helper --command-dir <dir>")
		return 2
	}

	if err := postfixcmd.RunRootHelper(*commandDir, stdin, stdout); err != nil {
		fmt.Fprintf(stderr, "postfix-api _root-helper: %v\n", err)
		return 1
	}
	return 0
}

func runMasterCmd(args []string, stderr io.Writer) int {
	serviceType, serviceName, err := masterproto.ParseMasterArgs(args)
	if err != nil {
		fmt.Fprintf(stderr, "postfix-api: %v\n", err)
		return 2
	}
	if serviceType != "unix-dgram" && serviceType != "inet" {
		fmt.Fprintf(stderr, "postfix-api: missing or unsupported master transport -t %q (expected unix-dgram or inet)\n", serviceType)
		printUsage(stderr)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if err := runMaster(ctx, serviceType, serviceName, stderr); err != nil {
		fmt.Fprintf(stderr, "postfix-api (%s): %v\n", serviceType, err)
		return 1
	}
	return 0
}

func runMaster(ctx context.Context, serviceType, serviceName string, stderr io.Writer) error {
	gen, err := masterproto.ParseGeneration(os.Getenv("GENERATION"))
	if err != nil {
		return err
	}

	params, err := config.ReadPostconf("")
	if err != nil {
		return err
	}

	confPath := filepath.Join(params.ConfigDirectory, configFileName)
	cfg, err := config.LoadFile(confPath)
	if err != nil {
		if serviceType == "unix-dgram" && errors.Is(err, os.ErrNotExist) {
			fmt.Fprintf(stderr, "postfix-api: warning: %q not found; continuing log receiver with defaults\n", confPath)
			cfg = config.DefaultConfig()
		} else {
			return err
		}
	}

	switch serviceType {
	case "unix-dgram":
		return logreceiver.RunMaster(ctx, cfg, params, gen)
	case "inet":
		err := runMasterAPI(ctx, cfg, params, gen, serviceName)
		if err != nil {
			logToPostlog(params.QueueDirectory, fmt.Sprintf("postfix-api (inet): %v", err))
		}
		return err
	default:
		return fmt.Errorf("unsupported master transport %q", serviceType)
	}
}

// logToPostlog emits a best-effort log record to Postfix's public/postlog socket so
// fatal startup errors (e.g. plaintext refusal) are visible in container/Postfix logs
// even though master redirects child stderr to /dev/null.
func logToPostlog(queueDir, msg string) {
	if queueDir == "" || msg == "" {
		return
	}
	sockPath := filepath.Join(queueDir, "public", "postlog")
	conn, err := net.DialTimeout("unixgram", sockPath, 500*time.Millisecond)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
	host, _ := os.Hostname()
	if host == "" {
		host = "localhost"
	}
	line := fmt.Sprintf("%s %s postfix/postfix-api[%d]: fatal: %s",
		time.Now().Format("Jan _2 15:04:05"), host, os.Getpid(), msg)
	_, _ = conn.Write([]byte(line))
}

func runMasterAPI(ctx context.Context, cfg *config.Config, params *config.PostfixParams, gen uint32, serviceName string) error {
	// Mark inherited master FDs close-on-exec before spawning _root-helper.
	masterproto.SetCloseOnExec(masterproto.StatusFD, masterproto.ListenFD)

	stateDir, err := cfg.ResolveStateDir(params.DataDirectory)
	if err != nil {
		return err
	}
	spoolDir := filepath.Join(stateDir, "spool")
	if err := os.MkdirAll(stateDir, 0o750); err != nil {
		return fmt.Errorf("mkdir state_dir %q: %w", stateDir, err)
	}
	if err := os.MkdirAll(spoolDir, 0o750); err != nil {
		return fmt.Errorf("mkdir spool_dir %q: %w", spoolDir, err)
	}
	if err := logreceiver.ChownStateTree(stateDir, params.MailOwner); err != nil {
		return err
	}

	// Load TLS certificates/keys while still root (private keys are typically 0600 root:root).
	tlsCfg, err := api.LoadTLSConfig(cfg)
	if err != nil {
		return err
	}

	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable path: %w", err)
	}
	helper, err := postfixcmd.StartRootHelper(exePath, params.CommandDirectory)
	if err != nil {
		return err
	}
	defer func() { _ = helper.Close() }()

	if err := config.DropPrivileges(params.MailOwner); err != nil {
		return err
	}

	if err := api.CheckTransportSecurity(serviceName, cfg); err != nil {
		return err
	}

	st, err := store.Open(filepath.Join(stateDir, "state.db"))
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	runner := postfixcmd.NewRunner(params.CommandDirectory, params.QueueDirectory, helper)
	spoolSrc := newSpoolSource(spoolDir)
	queueLister := api.NewQueueLister(runner)

	idx, err := indexer.New(st, spoolSrc, queueLister, indexer.Config{
		BatchSize:         cfg.IndexerBatchSize,
		QueueSyncInterval: cfg.QueueSyncInterval,
		RetentionDuration: cfg.RetentionDuration,
	})
	if err != nil {
		return err
	}

	srv, err := api.NewServer(
		cfg,
		st,
		idx,
		runner,
		params.QueueDirectory,
		api.WithPostfixVersion(params.MailVersion),
		api.WithTLSConfig(tlsCfg),
	)
	if err != nil {
		return err
	}

	ln, err := srv.PrepareMasterListener(masterproto.ListenFD, serviceName)
	if err != nil {
		return err
	}

	idxCtx, idxCancel := context.WithCancel(context.Background())
	idxDone := make(chan struct{})
	go func() {
		defer close(idxDone)
		_ = idx.Run(idxCtx)
	}()

	if err := masterproto.Handshake(masterproto.StatusFD, gen); err != nil {
		_ = ln.Close()
		idxCancel()
		<-idxDone
		return err
	}

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	shutdownCh := masterproto.WatchShutdown(runCtx, masterproto.StatusFD)
	go func() {
		select {
		case <-shutdownCh:
			runCancel()
		case <-runCtx.Done():
		}
	}()

	serveErr := srv.Serve(runCtx, ln)

	// Orderly shutdown: stop HTTP (already completed by Serve), stop indexer,
	// close SQLite store, and shut down _root-helper.
	idxCancel()
	<-idxDone

	storeCloseErr := st.Close()
	helperCloseErr := helper.Close()

	if serveErr != nil {
		return serveErr
	}
	if storeCloseErr != nil {
		return storeCloseErr
	}
	return helperCloseErr
}

type spoolSourceAdapter struct {
	dir    string
	reader *spool.Reader
}

func newSpoolSource(spoolDir string) indexer.SpoolSource {
	return &spoolSourceAdapter{
		dir:    spoolDir,
		reader: spool.NewReader(spoolDir),
	}
}

func (a *spoolSourceAdapter) ReadBatch(ctx context.Context, cursor store.SpoolCursor, maxLines int) (indexer.SpoolBatch, error) {
	if err := ctx.Err(); err != nil {
		return indexer.SpoolBatch{}, err
	}
	batch, err := a.reader.ReadBatch(cursor.Segment, cursor.Offset, maxLines)
	if err != nil {
		return indexer.SpoolBatch{}, err
	}

	lines := make([]string, len(batch.Records))
	var lastTS = cursor.LastRecordAt
	for i, rec := range batch.Records {
		utcTime := rec.Time.UTC()
		lines[i] = utcTime.Format(spool.TimeLayout) + " " + rec.Raw
		t := utcTime
		lastTS = &t
	}

	return indexer.SpoolBatch{
		Lines: lines,
		NextCursor: store.SpoolCursor{
			Segment:      batch.NextSegment,
			Offset:       batch.NextOffset,
			LastRecordAt: lastTS,
		},
		Gap: batch.GapDetected,
	}, nil
}

func (a *spoolSourceAdapter) DeleteSegmentsBefore(segment string) error {
	_, err := a.reader.DeleteBefore(segment)
	return err
}

func (a *spoolSourceAdapter) Stats() (indexer.SpoolStats, error) {
	st, err := spool.Stats(a.dir)
	if err != nil {
		return indexer.SpoolStats{}, err
	}
	var headOffset int64
	if st.NewestSegment != "" {
		if info, serr := os.Stat(filepath.Join(a.dir, st.NewestSegment)); serr == nil {
			headOffset = info.Size()
		}
	}
	return indexer.SpoolStats{
		Gaps:        st.Gaps,
		LostRecords: st.Lost,
		Bytes:       st.TotalBytes,
		HeadSegment: st.NewestSegment,
		HeadOffset:  headOffset,
	}, nil
}
