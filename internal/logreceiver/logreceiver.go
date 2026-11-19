// Package logreceiver implements Postfix postlog(8) replacement over
// MASTER_LISTEN_FD (FD 6, AF_UNIX SOCK_DGRAM): opening maillog_file as root,
// preparing <state_dir>/spool ownership, dropping privileges to mail_owner,
// and forwarding datagrams with zero steady-state heap allocations.
package logreceiver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/PretorianX/postfix-http-api/internal/config"
	"github.com/PretorianX/postfix-http-api/internal/masterproto"
	"github.com/PretorianX/postfix-http-api/internal/spool"
)

const (
	// MaxDatagramSize is the 64 KiB datagram receive buffer size.
	MaxDatagramSize = 65536

	// DefaultDrainTimeout is the socket drain period after shutdown signal.
	DefaultDrainTimeout = 50 * time.Millisecond
)

var subjectMarker = []byte(": info: header Subject: ")

// DatagramReader reads single datagrams into p and supports read deadlines
// for clean shutdown draining. *net.UnixConn and *os.File satisfy this.
type DatagramReader interface {
	Read(p []byte) (int, error)
	SetReadDeadline(t time.Time) error
}

// SpoolAppender appends formatted timestamped spool lines without allocation.
// *spool.Writer satisfies this interface.
type SpoolAppender interface {
	AppendFormatted(line []byte) error
}

// Options configures Receiver behavior.
type Options struct {
	SubjectInOutput bool
	DrainTimeout    time.Duration
}

// Receiver reads Postfix log datagrams, forwards non-Subject lines (or all
// lines when SubjectInOutput is true) to maillog_file, and always appends
// UTC-nanosecond-stamped records to the disk spool.
type Receiver struct {
	sock            DatagramReader
	out             io.Writer
	spool           SpoolAppender
	subjectInOutput bool
	drainTimeout    time.Duration

	pktBuf         [MaxDatagramSize]byte
	outBuf         []byte
	spoolBuf       []byte
	outErrors      atomic.Uint64
	spoolErrors    atomic.Uint64
	lastOutLogNano atomic.Int64
	lastSplLogNano atomic.Int64
}

type packetConnReader struct {
	pc net.PacketConn
}

func (p packetConnReader) Read(b []byte) (int, error) {
	n, _, err := p.pc.ReadFrom(b)
	return n, err
}

func (p packetConnReader) SetReadDeadline(t time.Time) error {
	return p.pc.SetReadDeadline(t)
}

// NewReceiver constructs a Receiver using a DatagramReader (such as *net.UnixConn
// or *os.File) for zero-allocation datagram reads.
func NewReceiver(sock DatagramReader, out io.Writer, sp SpoolAppender, opts Options) (*Receiver, error) {
	if sock == nil {
		return nil, errors.New("logreceiver: datagram reader is nil")
	}
	if out == nil {
		return nil, errors.New("logreceiver: output writer is nil")
	}
	drain := opts.DrainTimeout
	if drain <= 0 {
		drain = DefaultDrainTimeout
	}
	return &Receiver{
		sock:            sock,
		out:             out,
		spool:           sp,
		subjectInOutput: opts.SubjectInOutput,
		drainTimeout:    drain,
		outBuf:          make([]byte, 0, MaxDatagramSize+1),
		spoolBuf:        make([]byte, 0, 64+MaxDatagramSize+1),
	}, nil
}

// NewReceiverFromPacketConn constructs a Receiver from a net.PacketConn.
// When pc implements DatagramReader (e.g. *net.UnixConn), Read() is used directly
// to avoid per-packet net.Addr heap allocations.
func NewReceiverFromPacketConn(pc net.PacketConn, out io.Writer, sp SpoolAppender, opts Options) (*Receiver, error) {
	if pc == nil {
		return nil, errors.New("logreceiver: packet conn is nil")
	}
	if dr, ok := pc.(DatagramReader); ok {
		return NewReceiver(dr, out, sp, opts)
	}
	return NewReceiver(packetConnReader{pc: pc}, out, sp, opts)
}

// OpenMaillogFile opens maillogPath for append, falling back to opening without
// O_CREATE when character devices or fd symlinks such as /dev/stdout reject O_CREATE.
func OpenMaillogFile(maillogPath string) (*os.File, error) {
	if maillogPath == "" {
		return nil, errors.New("logreceiver: maillog_file is empty")
	}
	f, err := os.OpenFile(maillogPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err == nil {
		return f, nil
	}
	if errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.EROFS) ||
		errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.EPERM) ||
		errors.Is(err, syscall.ENODEV) {
		if f2, err2 := os.OpenFile(maillogPath, os.O_WRONLY|os.O_APPEND, 0); err2 == nil {
			return f2, nil
		}
	}
	return nil, fmt.Errorf("logreceiver: open maillog_file %q: %w", maillogPath, err)
}

// ChownStateTree recursively ensures path and all its contents are owned by
// mailOwner's UID and primary GID when running as root.
func ChownStateTree(path, mailOwner string) error {
	if os.Geteuid() != 0 {
		return nil
	}
	u, err := user.Lookup(mailOwner)
	if err != nil {
		return fmt.Errorf("logreceiver: lookup mail_owner %q: %w", mailOwner, err)
	}
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		return fmt.Errorf("logreceiver: invalid uid %q: %w", u.Uid, err)
	}
	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		return fmt.Errorf("logreceiver: invalid gid %q: %w", u.Gid, err)
	}
	return filepath.WalkDir(path, func(p string, _ fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := os.Chown(p, uid, gid); err != nil {
			return fmt.Errorf("logreceiver: chown %q: %w", p, err)
		}
		return nil
	})
}

// WrapListenFD wraps inherited MASTER_LISTEN_FD (FD 6) as a net.PacketConn
// (closing the temporary *os.File wrapper) or falls back to *os.File if needed.
func WrapListenFD(fd int) (net.PacketConn, error) {
	f := os.NewFile(uintptr(fd), "postlog")
	if f == nil {
		return nil, fmt.Errorf("logreceiver: invalid listen fd %d", fd)
	}
	pc, err := net.FilePacketConn(f)
	_ = f.Close()
	if err != nil {
		return nil, fmt.Errorf("logreceiver: wrap listen fd %d: %w", fd, err)
	}
	return pc, nil
}

// RunMaster runs the full master(8)-managed log receiver lifecycle:
// open maillog_file and spool as root, chown state_dir to mail_owner, drop
// privileges permanently, wrap FD 6, perform the FD 5 handshake, monitor FD 5
// for EOF shutdown, and execute the zero-alloc datagram loop until shutdown.
func RunMaster(ctx context.Context, cfg *config.Config, params *config.PostfixParams, gen uint32) error {
	if cfg == nil {
		return errors.New("logreceiver: config is nil")
	}
	if params == nil {
		return errors.New("logreceiver: postfix params is nil")
	}
	if params.MaillogFile == "" {
		return errors.New("logreceiver: Postfix maillog_file is empty")
	}

	stateDir, err := cfg.ResolveStateDir(params.DataDirectory)
	if err != nil {
		return err
	}
	spoolDir := filepath.Join(stateDir, "spool")

	outFile, err := OpenMaillogFile(params.MaillogFile)
	if err != nil {
		return err
	}
	defer outFile.Close()

	if err := os.MkdirAll(stateDir, 0o750); err != nil {
		return fmt.Errorf("logreceiver: mkdir state_dir %q: %w", stateDir, err)
	}
	if err := os.MkdirAll(spoolDir, 0o750); err != nil {
		return fmt.Errorf("logreceiver: mkdir spool_dir %q: %w", spoolDir, err)
	}

	sw, err := spool.NewWriter(spoolDir, cfg.Spool.SegmentSizeBytes, cfg.Spool.MaxTotalBytes)
	if err != nil {
		return err
	}
	defer sw.Close()

	if err := ChownStateTree(stateDir, params.MailOwner); err != nil {
		return err
	}

	masterproto.SetCloseOnExec(masterproto.StatusFD, masterproto.ListenFD)

	if err := config.DropPrivileges(params.MailOwner); err != nil {
		return err
	}

	pc, err := WrapListenFD(masterproto.ListenFD)
	if err != nil {
		return err
	}
	defer pc.Close()

	rec, err := NewReceiverFromPacketConn(pc, outFile, sw, Options{
		SubjectInOutput: cfg.SubjectInOutput,
		DrainTimeout:    DefaultDrainTimeout,
	})
	if err != nil {
		return err
	}

	if err := masterproto.Handshake(masterproto.StatusFD, gen); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	shutdownCh := masterproto.WatchShutdown(runCtx, masterproto.StatusFD)
	go func() {
		select {
		case <-shutdownCh:
			cancel()
		case <-runCtx.Done():
		}
	}()

	return rec.Run(runCtx)
}

// OutputErrors returns the number of maillog_file write errors encountered.
func (r *Receiver) OutputErrors() uint64 {
	return r.outErrors.Load()
}

// SpoolErrors returns the number of spool append errors encountered.
func (r *Receiver) SpoolErrors() uint64 {
	return r.spoolErrors.Load()
}

// Run reads datagrams in a zero-allocation steady-state loop until ctx is
// cancelled (draining buffered datagrams up to DrainTimeout) or a fatal socket
// error occurs.
func (r *Receiver) Run(ctx context.Context) error {
	loopDone := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-ctx.Done():
			if err := r.sock.SetReadDeadline(time.Now().Add(r.drainTimeout)); err != nil {
				if c, ok := r.sock.(io.Closer); ok {
					_ = c.Close()
				}
			}
		case <-loopDone:
		}
	}()
	defer func() {
		close(loopDone)
		<-watchDone
	}()

	for {
		n, err := r.sock.Read(r.pktBuf[:])
		if n > 0 {
			r.ProcessDatagram(r.pktBuf[:n], time.Now())
		}
		if err != nil {
			if ctx.Err() != nil && (errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF)) {
				return nil
			}
			if isTransientReadErr(err) {
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("logreceiver: read datagram: %w", err)
		}
	}
}

// ProcessDatagram processes a single raw datagram payload using Receiver's
// pre-allocated buffers without heap allocations in steady state.
func (r *Receiver) ProcessDatagram(pkt []byte, now time.Time) {
	rec := bytes.TrimRight(pkt, "\r\n\x00")
	if len(rec) == 0 {
		return
	}

	if r.subjectInOutput || !bytes.Contains(rec, subjectMarker) {
		r.outBuf = append(r.outBuf[:0], rec...)
		r.outBuf = append(r.outBuf, '\n')
		if _, err := writeAllWriter(r.out, r.outBuf); err != nil {
			r.recordOutError(err)
		}
	}

	if r.spool != nil {
		r.spoolBuf = now.UTC().AppendFormat(r.spoolBuf[:0], spool.TimeLayout)
		r.spoolBuf = append(r.spoolBuf, ' ')
		r.spoolBuf = append(r.spoolBuf, rec...)
		r.spoolBuf = append(r.spoolBuf, '\n')
		if err := r.spool.AppendFormatted(r.spoolBuf); err != nil {
			r.recordSpoolError(err)
		}
	}
}

func writeAllWriter(w io.Writer, b []byte) (int, error) {
	written := 0
	for written < len(b) {
		n, err := w.Write(b[written:])
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func (r *Receiver) recordOutError(err error) {
	count := r.outErrors.Add(1)
	nowNano := time.Now().UnixNano()
	prev := r.lastOutLogNano.Load()
	if nowNano-prev >= int64(10*time.Second) && r.lastOutLogNano.CompareAndSwap(prev, nowNano) {
		fmt.Fprintf(os.Stderr, "postfix-api maillog_file write error (count=%d): %v\n", count, err)
	}
}

func (r *Receiver) recordSpoolError(err error) {
	count := r.spoolErrors.Add(1)
	nowNano := time.Now().UnixNano()
	prev := r.lastSplLogNano.Load()
	if nowNano-prev >= int64(10*time.Second) && r.lastSplLogNano.CompareAndSwap(prev, nowNano) {
		fmt.Fprintf(os.Stderr, "postfix-api spool write error (count=%d): %v\n", count, err)
	}
}

func isTransientReadErr(err error) bool {
	return errors.Is(err, syscall.EINTR) ||
		errors.Is(err, syscall.EAGAIN) ||
		errors.Is(err, syscall.EWOULDBLOCK)
}
