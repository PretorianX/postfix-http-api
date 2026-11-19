package api

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/PretorianX/postfix-http-api/internal/config"
)

// CheckTransportSecurity verifies that plaintext HTTP is never exposed on a
// non-loopback address unless TLS is configured or cfg.AllowPlaintext is true.
func CheckTransportSecurity(listenAddr string, cfg *config.Config) error {
	if cfg == nil {
		return errors.New("api: config must not be nil")
	}

	hasCert := cfg.TLS.CertFile != ""
	hasKey := cfg.TLS.KeyFile != ""
	if hasCert != hasKey {
		return errors.New("api: tls.cert_file and tls.key_file must both be set or both be empty")
	}
	if hasCert && hasKey {
		return nil
	}
	if cfg.AllowPlaintext {
		return nil
	}

	if isLoopbackListenAddr(listenAddr) {
		return nil
	}
	return fmt.Errorf(
		"api: refusing to serve plaintext HTTP on non-loopback address %q without TLS "+
			"(configure tls.cert_file/tls.key_file or set allow_plaintext: true)",
		listenAddr,
	)
}

// LoadTLSConfig loads the configured X.509 certificate and private key into memory
// (enforcing TLS 1.2+). Intended to be called while still running as root prior to
// dropping privileges, since private keys are typically mode 0600 root:root.
// Returns (nil, nil) when TLS is not configured.
func LoadTLSConfig(cfg *config.Config) (*tls.Config, error) {
	if cfg == nil {
		return nil, errors.New("api: config must not be nil")
	}
	hasCert := cfg.TLS.CertFile != ""
	hasKey := cfg.TLS.KeyFile != ""
	if hasCert != hasKey {
		return nil, errors.New("api: tls.cert_file and tls.key_file must both be set or both be empty")
	}
	if !hasCert {
		return nil, nil
	}

	cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("api: load TLS keypair (%q, %q): %w", cfg.TLS.CertFile, cfg.TLS.KeyFile, err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

func checkListenerSecurity(ln net.Listener, cfg *config.Config) error {
	if cfg == nil {
		return errors.New("api: config must not be nil")
	}
	if (cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "") || cfg.AllowPlaintext {
		return nil
	}

	switch addr := ln.Addr().(type) {
	case *net.TCPAddr:
		if addr.IP != nil && addr.IP.IsLoopback() {
			return nil
		}
		return fmt.Errorf(
			"api: refusing to serve plaintext HTTP on non-loopback socket %q without TLS "+
				"(configure tls.cert_file/tls.key_file or set allow_plaintext: true)",
			addr.String(),
		)
	case *net.UnixAddr:
		return nil
	default:
		return CheckTransportSecurity(ln.Addr().String(), cfg)
	}
}

func isLoopbackListenAddr(listenAddr string) bool {
	trimmed := strings.TrimSpace(listenAddr)
	if trimmed == "" {
		return false
	}

	host := trimmed
	if h, _, err := net.SplitHostPort(trimmed); err == nil {
		host = h
	} else {
		host = strings.TrimSuffix(strings.TrimPrefix(trimmed, "["), "]")
	}

	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") || strings.EqualFold(host, "localhost.") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func extractBearerToken(r *http.Request) (string, bool) {
	vals := r.Header.Values("Authorization")
	if len(vals) != 1 {
		return "", false
	}
	raw := strings.TrimSpace(vals[0])
	idx := strings.IndexAny(raw, " \t")
	if idx <= 0 {
		return "", false
	}
	scheme := raw[:idx]
	if !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token := strings.TrimSpace(raw[idx+1:])
	if token == "" {
		return "", false
	}
	return token, true
}

const tlsHandshakePrefix = "http: TLS handshake error from "

type wakeupFilterWriter struct {
	out io.Writer
}

func (w *wakeupFilterWriter) Write(p []byte) (int, error) {
	if isBenignLoopbackWakeupLog(string(p)) {
		return len(p), nil
	}
	return w.out.Write(p)
}

func isBenignLoopbackWakeupLog(msg string) bool {
	trimmed := strings.TrimSpace(msg)
	idx := strings.Index(trimmed, tlsHandshakePrefix)
	if idx < 0 {
		return false
	}
	rest := trimmed[idx+len(tlsHandshakePrefix):]

	var peer string
	switch {
	case strings.HasSuffix(rest, ": EOF"):
		peer = strings.TrimSuffix(rest, ": EOF")
	case strings.HasSuffix(rest, ": io.EOF"):
		peer = strings.TrimSuffix(rest, ": io.EOF")
	default:
		return false
	}

	host, _, err := net.SplitHostPort(peer)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func newFilteredErrorLog(out io.Writer) *log.Logger {
	if out == nil {
		out = os.Stderr
	}
	return log.New(&wakeupFilterWriter{out: out}, "", log.LstdFlags)
}
