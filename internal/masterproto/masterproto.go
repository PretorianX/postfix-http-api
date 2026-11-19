// Package masterproto implements the Postfix master(8) child protocol:
// inherited file descriptors (FD 5 status pipe, FD 6 listen socket),
// octal GENERATION parsing, 12-byte native-endian MASTER_STATUS frames,
// startup readiness handshakes, FD 5 EOF shutdown detection, and tolerant
// Postfix daemon CLI flag scanning.
package masterproto

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	// StatusFD is MASTER_STATUS_FD (FD 5) inherited from Postfix master(8).
	StatusFD = 5
	// ListenFD is MASTER_LISTEN_FD (FD 6) inherited from Postfix master(8).
	ListenFD = 6

	// StateTaken indicates the service is busy/initializing (MASTER_STAT_TAKEN).
	StateTaken int32 = 0
	// StateAvailable indicates the service is ready for work (MASTER_STAT_AVAIL).
	StateAvailable int32 = 1

	// Spec-named aliases for compatibility across internal packages.
	MasterStatusFD        = StatusFD
	MasterListenFD        = ListenFD
	MasterStatTaken int32 = StateTaken
	MasterStatAvail int32 = StateAvailable
)

// ParseGeneration parses Postfix master(8)'s GENERATION environment variable,
// which is formatted in octal ASCII (base 8, e.g. "10" == 8).
func ParseGeneration(envVal string) (uint32, error) {
	if envVal == "" {
		return 0, errors.New("masterproto: GENERATION is empty")
	}
	v, err := strconv.ParseUint(envVal, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("masterproto: invalid octal GENERATION %q: %w", envVal, err)
	}
	return uint32(v), nil
}

// EncodeStatus encodes a 12-byte native-endian MASTER_STATUS frame matching
// Postfix's struct MASTER_STATUS { int pid; unsigned gen; int avail; }.
func EncodeStatus(pid int32, gen uint32, state int32) [12]byte {
	var frame [12]byte
	binary.NativeEndian.PutUint32(frame[0:4], uint32(pid))
	binary.NativeEndian.PutUint32(frame[4:8], gen)
	binary.NativeEndian.PutUint32(frame[8:12], uint32(state))
	return frame
}

// Notify writes a single 12-byte MASTER_STATUS frame atomically to statusFD,
// retrying on EINTR.
func Notify(statusFD int, pid int32, gen uint32, state int32) error {
	if statusFD < 0 {
		return fmt.Errorf("masterproto: invalid status fd %d", statusFD)
	}
	frame := EncodeStatus(pid, gen, state)
	buf := frame[:]
	for len(buf) > 0 {
		n, err := syscall.Write(statusFD, buf)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return fmt.Errorf("masterproto: write status fd %d: %w", statusFD, err)
		}
		if n == 0 {
			return fmt.Errorf("masterproto: write status fd %d: short write", statusFD)
		}
		buf = buf[n:]
	}
	return nil
}

// Handshake sends StateTaken followed immediately by StateAvailable on statusFD
// for the current process PID and generation. Sending both completes one child
// service cycle inside master(8) so rapid restarts do not trigger 60s throttling.
func Handshake(statusFD int, gen uint32) error {
	pid := int32(os.Getpid())
	if err := Notify(statusFD, pid, gen, StateTaken); err != nil {
		return err
	}
	return Notify(statusFD, pid, gen, StateAvailable)
}

// WatchShutdown duplicates statusFD with FD_CLOEXEC and monitors it via a
// blocking read in a background goroutine. When master(8) closes FD 5 (on
// postfix reload or postfix stop) yielding EOF/error, or when ctx is cancelled,
// the returned channel is closed. Duplicating avoids closing caller-owned FDs.
func WatchShutdown(ctx context.Context, statusFD int) <-chan struct{} {
	ch := make(chan struct{})
	if statusFD < 0 {
		close(ch)
		return ch
	}

	syscall.ForkLock.RLock()
	dupFD, err := syscall.Dup(statusFD)
	if err == nil {
		syscall.CloseOnExec(dupFD)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		close(ch)
		return ch
	}

	f := os.NewFile(uintptr(dupFD), "master-status")
	if f == nil {
		_ = syscall.Close(dupFD)
		close(ch)
		return ch
	}

	var closeOnce sync.Once
	closeCh := func() { closeOnce.Do(func() { close(ch) }) }

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		defer closeCh()
		defer f.Close()
		var buf [1]byte
		for {
			n, rerr := f.Read(buf[:])
			if rerr != nil {
				return
			}
			if n == 0 {
				return
			}
		}
	}()

	go func() {
		select {
		case <-ctx.Done():
			closeCh()
			// Unblock pollable FDs (e.g. AF_UNIX socketpair or Linux pipe) immediately.
			_ = f.SetReadDeadline(time.Now())
			<-readDone
		case <-readDone:
		}
	}()

	return ch
}

// SetCloseOnExec marks each non-negative file descriptor with FD_CLOEXEC so
// inherited master(8) descriptors never leak into child subprocesses.
func SetCloseOnExec(fds ...int) {
	for _, fd := range fds {
		if fd >= 0 {
			syscall.CloseOnExec(fd)
		}
	}
}

// ParseMasterArgs scans Postfix master(8)-supplied daemon CLI arguments without
// failing on standard or future Postfix flags. It extracts the transport type
// (-t) and service name (-n).
func ParseMasterArgs(args []string) (serviceType string, serviceName string, err error) {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			break
		}
		switch {
		case arg == "-t":
			if i+1 >= len(args) {
				return "", "", errors.New("masterproto: flag -t requires an argument")
			}
			serviceType = args[i+1]
			i++
		case strings.HasPrefix(arg, "-t") && len(arg) > 2:
			serviceType = arg[2:]
		case arg == "-n":
			if i+1 >= len(args) {
				return "", "", errors.New("masterproto: flag -n requires an argument")
			}
			serviceName = args[i+1]
			i++
		case strings.HasPrefix(arg, "-n") && len(arg) > 2:
			serviceName = arg[2:]
		case arg == "-o" || arg == "-T" || arg == "-f":
			// Value-taking Postfix daemon flags: consume following argument if present.
			if i+1 < len(args) {
				i++
			}
		case arg == "-s" || arg == "-h" || arg == "-m" || arg == "-i":
			// Flags that may take an argument in some Postfix daemons or tests;
			// consume next token only if it does not look like another flag.
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				i++
			}
		default:
			// Tolerate standalone flags (-u, -c, -l, -z, -v, -d, -S, -r, -D, -Z,
			// unknown flags, attached forms like -okey=val, etc.) without error.
		}
	}
	return serviceType, serviceName, nil
}
