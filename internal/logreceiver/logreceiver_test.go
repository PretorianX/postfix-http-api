package logreceiver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/PretorianX/postfix-http-api/internal/spool"
)

func newUnixDgramPair(t *testing.T) (srvPC net.PacketConn, cli *os.File) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_DGRAM, 0)
	if err != nil {
		t.Fatalf("Socketpair(AF_UNIX, SOCK_DGRAM): %v", err)
	}
	// WrapListenFD dups fds[0] via net.FilePacketConn and closes its *os.File wrapper.
	pc, err := WrapListenFD(fds[0])
	if err != nil {
		_ = syscall.Close(fds[0])
		_ = syscall.Close(fds[1])
		t.Fatalf("WrapListenFD: %v", err)
	}
	cliFile := os.NewFile(uintptr(fds[1]), "dgram-client")
	return pc, cliFile
}

func sendDgramFD(t *testing.T, f *os.File, payload []byte) {
	t.Helper()
	fd := int(f.Fd())
	for i := 0; i < 1000; i++ {
		_, err := syscall.Write(fd, payload)
		if err == nil {
			return
		}
		if errors.Is(err, syscall.ENOBUFS) || errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EINTR) {
			runtime.Gosched()
			time.Sleep(100 * time.Microsecond)
			continue
		}
		t.Fatalf("dgram Write: %v", err)
	}
	t.Fatalf("dgram Write exhausted ENOBUFS retries")
}

func TestOpenMaillogFile(t *testing.T) {
	if _, err := OpenMaillogFile(""); err == nil {
		t.Fatal("OpenMaillogFile(\"\") = nil; want error")
	}

	logPath := filepath.Join(t.TempDir(), "mail.log")
	f, err := OpenMaillogFile(logPath)
	if err != nil {
		t.Fatalf("OpenMaillogFile regular: %v", err)
	}
	if _, err := f.WriteString("line1\n"); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	_ = f.Close()

	devNull, err := OpenMaillogFile("/dev/null")
	if err != nil {
		t.Fatalf("OpenMaillogFile(/dev/null): %v", err)
	}
	_ = devNull.Close()
}

func TestUnixDatagramEndToEndAndWrapListenFD(t *testing.T) {
	pc, cli := newUnixDgramPair(t)
	defer pc.Close()
	defer cli.Close()

	spoolDir := filepath.Join(t.TempDir(), "spool")
	sw, err := spool.NewWriter(spoolDir, 1024*1024, 8*1024*1024)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer sw.Close()

	outPath := filepath.Join(t.TempDir(), "maillog")
	outFile, err := OpenMaillogFile(outPath)
	if err != nil {
		t.Fatalf("OpenMaillogFile: %v", err)
	}
	defer outFile.Close()

	rec, err := NewReceiverFromPacketConn(pc, outFile, sw, Options{
		SubjectInOutput: false,
		DrainTimeout:    25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewReceiverFromPacketConn: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- rec.Run(ctx)
	}()

	sendDgramFD(t, cli, []byte("postfix/smtpd[10]: ABC123: client=localhost[127.0.0.1]\r\n\x00"))
	sendDgramFD(t, cli, []byte("postfix/cleanup[11]: ABC123: info: header Subject: Secret Payload from bob; from=<a@example.com>\n"))
	sendDgramFD(t, cli, []byte("\r\n\x00"))
	sendDgramFD(t, cli, []byte("postfix/qmgr[12]: ABC123: removed"))

	// Give receiver a brief moment to process, then cancel and verify clean exit.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := spool.Stats(spoolDir)
		if st.TotalBytes > 120 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()

	select {
	case runErr := <-errCh:
		if runErr != nil {
			t.Fatalf("Receiver.Run: %v", runErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Receiver.Run did not shut down cleanly")
	}

	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("ReadFile maillog: %v", err)
	}
	outStr := string(outBytes)
	if !strings.Contains(outStr, "client=localhost[127.0.0.1]") {
		t.Errorf("maillog missing smtpd line: %q", outStr)
	}
	if strings.Contains(outStr, "Secret Payload") || strings.Contains(outStr, ": info: header Subject: ") {
		t.Errorf("maillog leaked redacted Subject line: %q", outStr)
	}
	if !strings.Contains(outStr, "ABC123: removed") {
		t.Errorf("maillog missing removed line: %q", outStr)
	}

	r := spool.NewReader(spoolDir)
	batch, err := r.ReadBatch("", 0, 100)
	if err != nil {
		t.Fatalf("spool ReadBatch: %v", err)
	}
	foundSubject := false
	for _, item := range batch.Records {
		if strings.Contains(item.Raw, "Secret Payload") {
			foundSubject = true
		}
	}
	if !foundSubject {
		t.Errorf("spool missing Subject record: %+v", batch.Records)
	}
}

func TestSubjectRedactionOnAndOff(t *testing.T) {
	subjectPkt := []byte("postfix/cleanup[1]: 4jV74M63RlzHxK3: info: header Subject: Invoice 42 from ACME; from=<a@example.com>")
	normalPkt := []byte("postfix/qmgr[2]: 4jV74M63RlzHxK3: removed")
	now := time.Date(2026, 11, 19, 8, 30, 0, 0, time.UTC)

	t.Run("subject_in_output=false", func(t *testing.T) {
		var out bytes.Buffer
		spoolDir := filepath.Join(t.TempDir(), "spool")
		sw, err := spool.NewWriter(spoolDir, 65536, 262144)
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		defer sw.Close()

		sock := &sliceDatagramReader{}
		rec, err := NewReceiver(sock, &out, sw, Options{SubjectInOutput: false})
		if err != nil {
			t.Fatalf("NewReceiver: %v", err)
		}
		rec.ProcessDatagram(subjectPkt, now)
		rec.ProcessDatagram(normalPkt, now)

		if strings.Contains(out.String(), "Invoice 42") {
			t.Errorf("expected Subject redacted from output, got %q", out.String())
		}
		if !strings.Contains(out.String(), "removed") {
			t.Errorf("expected normal line in output, got %q", out.String())
		}

		b, err := spool.NewReader(spoolDir).ReadBatch("", 0, 10)
		if err != nil {
			t.Fatalf("ReadBatch: %v", err)
		}
		if len(b.Records) != 2 || !strings.Contains(b.Records[0].Raw, "Invoice 42") {
			t.Errorf("expected both records in spool, got %+v", b.Records)
		}
	})

	t.Run("subject_in_output=true", func(t *testing.T) {
		var out bytes.Buffer
		spoolDir := filepath.Join(t.TempDir(), "spool")
		sw, err := spool.NewWriter(spoolDir, 65536, 262144)
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		defer sw.Close()

		sock := &sliceDatagramReader{}
		rec, err := NewReceiver(sock, &out, sw, Options{SubjectInOutput: true})
		if err != nil {
			t.Fatalf("NewReceiver: %v", err)
		}
		rec.ProcessDatagram(subjectPkt, now)
		rec.ProcessDatagram(normalPkt, now)

		if !strings.Contains(out.String(), ": info: header Subject: Invoice 42") {
			t.Errorf("expected Subject in output when enabled, got %q", out.String())
		}
	})
}

type failingSpool struct {
	err error
}

func (f failingSpool) AppendFormatted(_ []byte) error {
	return f.err
}

func TestSpoolFailureIsolation(t *testing.T) {
	var out bytes.Buffer
	rec, err := NewReceiver(&sliceDatagramReader{}, &out, failingSpool{err: errors.New("ENOSPC")}, Options{})
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}

	now := time.Now().UTC()
	rec.ProcessDatagram([]byte("postfix/smtp[1]: ABC123: status=sent"), now)
	rec.ProcessDatagram([]byte("postfix/qmgr[2]: ABC123: removed"), now)

	if rec.SpoolErrors() != 2 {
		t.Errorf("SpoolErrors() = %d; want 2", rec.SpoolErrors())
	}
	if rec.OutputErrors() != 0 {
		t.Errorf("OutputErrors() = %d; want 0", rec.OutputErrors())
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("output lines = %d (%q); want 2", len(lines), out.String())
	}
}

func TestZeroAllocationSteadyStateAndMax64KB(t *testing.T) {
	spoolDir := filepath.Join(t.TempDir(), "spool")
	sw, err := spool.NewWriter(spoolDir, 64*1024*1024, 256*1024*1024)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer sw.Close()

	rec, err := NewReceiver(&sliceDatagramReader{}, io.Discard, sw, Options{SubjectInOutput: false})
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}

	pkt := []byte("postfix/smtp[99]: 4jV74M63RlzHxK3: to=<b@example.org>, relay=mx.example.org[192.0.2.1]:25, delay=0.42, dsn=2.0.0, status=sent (250 OK)")
	subjPkt := []byte("postfix/cleanup[88]: 4jV74M63RlzHxK3: info: header Subject: Secret; from=<a@example.com>")
	now := time.Date(2026, 11, 19, 8, 30, 0, 123456789, time.UTC)

	// Warm up.
	rec.ProcessDatagram(pkt, now)
	rec.ProcessDatagram(subjPkt, now)

	allocsNormal := testing.AllocsPerRun(500, func() {
		rec.ProcessDatagram(pkt, now)
	})
	if allocsNormal != 0 {
		t.Fatalf("ProcessDatagram (normal) allocs = %v; want 0", allocsNormal)
	}

	allocsSubj := testing.AllocsPerRun(500, func() {
		rec.ProcessDatagram(subjPkt, now)
	})
	if allocsSubj != 0 {
		t.Fatalf("ProcessDatagram (subject redacted) allocs = %v; want 0", allocsSubj)
	}

	// Verify full 64 KiB datagram allocates 0 bytes and does not overflow buffers.
	maxPkt := bytes.Repeat([]byte("A"), MaxDatagramSize)
	rec.ProcessDatagram(maxPkt, now)
	allocsMax := testing.AllocsPerRun(50, func() {
		rec.ProcessDatagram(maxPkt, now)
	})
	if allocsMax != 0 {
		t.Fatalf("ProcessDatagram (64KB) allocs = %v; want 0", allocsMax)
	}

	// Verify full Receiver.Run loop with zero-alloc DatagramReader allocates 0 per packet.
	feed := &sliceDatagramReader{
		pkts: make([][]byte, 200),
	}
	for i := range feed.pkts {
		feed.pkts[i] = pkt
	}
	recLoop, err := NewReceiver(feed, io.Discard, sw, Options{})
	if err != nil {
		t.Fatalf("NewReceiver loop: %v", err)
	}
	allocsLoop := testing.AllocsPerRun(10, func() {
		feed.idx = 0
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// When feed exhausts pkts, it cancels/returns ErrDeadlineExceeded once ctx cancelled.
		feed.onDone = cancel
		if err := recLoop.Run(ctx); err != nil {
			t.Fatalf("Run: %v", err)
		}
	})
	// Run itself spawns 2 watcher goroutines once per Run invocation (~2-4 allocs per Run,
	// NOT per datagram!). Verify total per Run across 200 datagrams is tiny (< 10 allocs, i.e., 0 per datagram).
	if allocsLoop >= 20 {
		t.Fatalf("Run across 200 datagrams allocated %v (should be O(1), 0 per datagram)", allocsLoop)
	}
}

func TestHighVolumeBurst10000Datagrams(t *testing.T) {
	pc, cli := newUnixDgramPair(t)
	defer pc.Close()
	defer cli.Close()
	if uc, ok := pc.(*net.UnixConn); ok {
		_ = uc.SetReadBuffer(4 * 1024 * 1024)
	}
	_ = syscall.SetsockoptInt(int(cli.Fd()), syscall.SOL_SOCKET, syscall.SO_SNDBUF, 1024*1024)

	spoolDir := filepath.Join(t.TempDir(), "spool")
	sw, err := spool.NewWriter(spoolDir, 256*1024, 16*1024*1024)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer sw.Close()

	outPath := filepath.Join(t.TempDir(), "maillog")
	outFile, err := OpenMaillogFile(outPath)
	if err != nil {
		t.Fatalf("OpenMaillogFile: %v", err)
	}
	defer outFile.Close()

	rec, err := NewReceiverFromPacketConn(pc, outFile, sw, Options{
		SubjectInOutput: false,
		DrainTimeout:    100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewReceiverFromPacketConn: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	var runErr error
	go func() {
		defer wg.Done()
		runErr = rec.Run(ctx)
	}()

	const total = 12000
	for i := 0; i < total; i++ {
		msg := fmt.Sprintf("postfix/smtp[1]: Q%09d: status=sent", i)
		sendDgramFD(t, cli, []byte(msg))
	}

	// Wait until all 12,000 records are flushed into the spool before cancelling.
	r := spool.NewReader(spoolDir)
	var count int
	curSeg := ""
	var curOff int64
	deadline := time.Now().Add(10 * time.Second)
	for count < total && time.Now().Before(deadline) {
		b, rerr := r.ReadBatch(curSeg, curOff, 1000)
		if rerr != nil {
			t.Fatalf("ReadBatch: %v", rerr)
		}
		count += len(b.Records)
		curSeg = b.NextSegment
		curOff = b.NextOffset
		if len(b.Records) == 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}

	cancel()
	wg.Wait()
	if runErr != nil {
		t.Fatalf("Receiver.Run: %v", runErr)
	}

	if count != total {
		t.Fatalf("spool received %d datagrams; want %d", count, total)
	}

	outData, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("ReadFile outPath: %v", err)
	}
	outLines := strings.Count(string(outData), "\n")
	if outLines != total {
		t.Fatalf("maillog lines = %d; want %d", outLines, total)
	}
}

type sliceDatagramReader struct {
	pkts   [][]byte
	idx    int
	onDone func()
}

func (s *sliceDatagramReader) Read(p []byte) (int, error) {
	if s.idx < len(s.pkts) {
		n := copy(p, s.pkts[s.idx])
		s.idx++
		return n, nil
	}
	if s.onDone != nil {
		s.onDone()
	}
	return 0, os.ErrDeadlineExceeded
}

func (s *sliceDatagramReader) SetReadDeadline(_ time.Time) error {
	return nil
}
