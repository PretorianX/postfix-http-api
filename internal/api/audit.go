// Package api implements the authenticated HTTP/HTTPS REST API for
// postfix-api (/v1/*), including Bearer token SHA-256 verification, scope
// enforcement, non-loopback plaintext protection, wakeup-probe handling,
// and structured JSON audit logging.
package api

import (
	"encoding/json"
	"io"
	"os"
	"sync"
)

const auditTimeLayout = "2006-01-02T15:04:05.000000Z"

// AuditEvent is the structured JSON record emitted for every write endpoint invocation.
type AuditEvent struct {
	Timestamp  string   `json:"ts"`
	Audit      bool     `json:"audit"`
	Token      string   `json:"token"`
	Action     string   `json:"action"`
	QueueIDs   []string `json:"queue_ids"`
	ExitCode   int      `json:"exit_code"`
	RemoteAddr string   `json:"remote_addr"`
}

// AuditLogger records structured audit events for mutating API endpoints.
type AuditLogger interface {
	Log(event AuditEvent)
}

// AuditLoggerFunc adapts a function to the AuditLogger interface.
type AuditLoggerFunc func(event AuditEvent)

// Log calls f(event).
func (f AuditLoggerFunc) Log(event AuditEvent) {
	f(event)
}

type jsonAuditLogger struct {
	mu  sync.Mutex
	out io.Writer
}

// NewJSONAuditLogger returns a thread-safe AuditLogger that writes one JSON line
// per event to out (defaulting to os.Stderr when out is nil).
func NewJSONAuditLogger(out io.Writer) AuditLogger {
	if out == nil {
		out = os.Stderr
	}
	return &jsonAuditLogger{out: out}
}

func (l *jsonAuditLogger) Log(event AuditEvent) {
	event.Audit = true
	if event.QueueIDs == nil {
		event.QueueIDs = []string{}
	}
	line, err := json.Marshal(event)
	if err != nil {
		return
	}
	line = append(line, '\n')

	l.mu.Lock()
	_, _ = l.out.Write(line)
	l.mu.Unlock()
}
