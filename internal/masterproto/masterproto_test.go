package masterproto

import (
	"context"
	"encoding/binary"
	"io"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestParseGeneration(t *testing.T) {
	tests := []struct {
		in      string
		want    uint32
		wantErr bool
	}{
		{in: "0", want: 0},
		{in: "1", want: 1},
		{in: "7", want: 7},
		{in: "10", want: 8},
		{in: "010", want: 8},
		{in: "77", want: 63},
		{in: "37777777777", want: ^uint32(0)},
		{in: "", wantErr: true},
		{in: "8", wantErr: true},
		{in: "9", wantErr: true},
		{in: "abc", wantErr: true},
		{in: "-1", wantErr: true},
		{in: "0x10", wantErr: true},
		{in: "40000000000", wantErr: true},
	}

	for _, tc := range tests {
		got, err := ParseGeneration(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseGeneration(%q) = %d, nil; want error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseGeneration(%q) unexpected error: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseGeneration(%q) = %d; want %d", tc.in, got, tc.want)
		}
	}
}

func TestEncodeStatus(t *testing.T) {
	frame := EncodeStatus(12345, 8, StateAvailable)
	if len(frame) != 12 {
		t.Fatalf("frame length = %d; want 12", len(frame))
	}
	gotPID := int32(binary.NativeEndian.Uint32(frame[0:4]))
	gotGen := binary.NativeEndian.Uint32(frame[4:8])
	gotState := int32(binary.NativeEndian.Uint32(frame[8:12]))

	if gotPID != 12345 || gotGen != 8 || gotState != StateAvailable {
		t.Fatalf("decoded (%d, %d, %d); want (12345, 8, %d)", gotPID, gotGen, gotState, StateAvailable)
	}

	frameTaken := EncodeStatus(-1, 0xffffffff, StateTaken)
	if got := int32(binary.NativeEndian.Uint32(frameTaken[0:4])); got != -1 {
		t.Fatalf("got pid %d; want -1", got)
	}
	if got := binary.NativeEndian.Uint32(frameTaken[4:8]); got != 0xffffffff {
		t.Fatalf("got gen %x; want 0xffffffff", got)
	}
	if got := int32(binary.NativeEndian.Uint32(frameTaken[8:12])); got != StateTaken {
		t.Fatalf("got state %d; want %d", got, StateTaken)
	}
}

func TestHandshakeAndNotifyPipe(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	defer r.Close()
	defer w.Close()

	if err := Handshake(int(w.Fd()), 8); err != nil {
		t.Fatalf("Handshake: %v", err)
	}

	buf := make([]byte, 24)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}

	wantPID := int32(os.Getpid())
	for idx, wantState := range []int32{StateTaken, StateAvailable} {
		off := idx * 12
		gotPID := int32(binary.NativeEndian.Uint32(buf[off : off+4]))
		gotGen := binary.NativeEndian.Uint32(buf[off+4 : off+8])
		gotState := int32(binary.NativeEndian.Uint32(buf[off+8 : off+12]))
		if gotPID != wantPID || gotGen != 8 || gotState != wantState {
			t.Errorf("frame %d = (%d, %d, %d); want (%d, 8, %d)", idx, gotPID, gotGen, gotState, wantPID, wantState)
		}
	}

	if err := Notify(-1, wantPID, 1, StateTaken); err == nil {
		t.Errorf("Notify(-1) = nil; want error")
	}
}

func TestWatchShutdownEOF(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	defer r.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := WatchShutdown(ctx, int(r.Fd()))

	select {
	case <-ch:
		t.Fatal("WatchShutdown closed before pipe write end closed")
	case <-time.After(25 * time.Millisecond):
	}

	if err := w.Close(); err != nil {
		t.Fatalf("w.Close: %v", err)
	}

	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("WatchShutdown did not close channel after pipe EOF")
	}
}

func TestWatchShutdownContextCancel(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	defer r.Close()
	defer w.Close()

	ctx, cancel := context.WithCancel(context.Background())
	ch := WatchShutdown(ctx, int(r.Fd()))

	cancel()

	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("WatchShutdown did not close channel after context cancellation")
	}
}

func TestWatchShutdownInvalidFD(t *testing.T) {
	ch := WatchShutdown(context.Background(), -1)
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("WatchShutdown(-1) did not return closed channel")
	}
}

func TestParseMasterArgs(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantType string
		wantName string
		wantErr  bool
	}{
		{
			name:     "unix-dgram standard flags",
			args:     []string{"-t", "unix-dgram", "-n", "postlog", "-u", "-c", "-o", "syslog_name=postfix", "-l", "-z"},
			wantType: "unix-dgram",
			wantName: "postlog",
		},
		{
			name:     "attached flags and extra master options",
			args:     []string{"-tinet", "-n0.0.0.0:8025", "-v", "-S", "-D", "-m", "-s", "1", "-i", "-Z", "-r", "-d"},
			wantType: "inet",
			wantName: "0.0.0.0:8025",
		},
		{
			name:     "arbitrary future and value flags",
			args:     []string{"-T", "inet", "-f", "/etc/postfix", "-h", "localhost", "-m", "5", "-s", "-i", "100", "-x", "--extra", "-t", "inet", "-n", "[::1]:8025"},
			wantType: "inet",
			wantName: "[::1]:8025",
		},
		{
			name:     "no -t flag returns empty type without error",
			args:     []string{"-u", "-c", "-v", "-n", "postlog"},
			wantType: "",
			wantName: "postlog",
		},
		{
			name:    "missing -t value",
			args:    []string{"-u", "-t"},
			wantErr: true,
		},
		{
			name:    "missing -n value",
			args:    []string{"-t", "inet", "-n"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotType, gotName, err := ParseMasterArgs(tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseMasterArgs(%v) err = nil; want error", tc.args)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseMasterArgs(%v) unexpected err: %v", tc.args, err)
			}
			if gotType != tc.wantType || gotName != tc.wantName {
				t.Fatalf("ParseMasterArgs(%v) = (%q, %q); want (%q, %q)", tc.args, gotType, gotName, tc.wantType, tc.wantName)
			}
		})
	}
}

func TestWatchShutdownSocketpairEOFAndCancel(t *testing.T) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("Socketpair: %v", err)
	}
	parentFD, childFD := fds[0], fds[1]
	defer syscall.Close(childFD)

	// Handshake from child side, read from parent side.
	if err := Handshake(childFD, 0o12); err != nil {
		_ = syscall.Close(parentFD)
		t.Fatalf("Handshake: %v", err)
	}

	var buf [24]byte
	n, err := syscall.Read(parentFD, buf[:])
	if err != nil || n != 24 {
		_ = syscall.Close(parentFD)
		t.Fatalf("Read handshake = (%d, %v); want (24, nil)", n, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := WatchShutdown(ctx, childFD)

	// Closing parentFD triggers EOF on childFD.
	if err := syscall.Close(parentFD); err != nil {
		t.Fatalf("Close parentFD: %v", err)
	}

	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("WatchShutdown did not close on socketpair EOF")
	}

	// Test context cancellation unblocks socketpair immediately.
	fds2, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("Socketpair 2: %v", err)
	}
	defer syscall.Close(fds2[0])
	defer syscall.Close(fds2[1])

	ctx2, cancel2 := context.WithCancel(context.Background())
	ch2 := WatchShutdown(ctx2, fds2[1])
	cancel2()

	select {
	case <-ch2:
	case <-time.After(2 * time.Second):
		t.Fatal("WatchShutdown did not close on socketpair cancel")
	}
}

func TestSetCloseOnExec(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	defer r.Close()
	defer w.Close()

	// Clear FD_CLOEXEC first to verify SetCloseOnExec sets it.
	for _, fd := range []int{int(r.Fd()), int(w.Fd())} {
		if _, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), uintptr(syscall.F_SETFD), 0); errno != 0 {
			t.Fatalf("fcntl F_SETFD 0: %v", errno)
		}
	}

	SetCloseOnExec(int(r.Fd()), int(w.Fd()), -1)

	for _, fd := range []int{int(r.Fd()), int(w.Fd())} {
		flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), uintptr(syscall.F_GETFD), 0)
		if errno != 0 {
			t.Fatalf("fcntl F_GETFD: %v", errno)
		}
		if flags&syscall.FD_CLOEXEC == 0 {
			t.Errorf("fd %d missing FD_CLOEXEC (flags=%#x)", fd, flags)
		}
	}
}
