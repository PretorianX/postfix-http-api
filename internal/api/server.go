package api

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/PretorianX/postfix-http-api/internal/config"
	"github.com/PretorianX/postfix-http-api/internal/indexer"
	"github.com/PretorianX/postfix-http-api/internal/postfixcmd"
	"github.com/PretorianX/postfix-http-api/internal/store"
)

const (
	defaultLimit          = 100
	maxLimit              = 1000
	maxRequestBodyBytes   = 1 << 20 // 1 MiB
	httpReadHeaderTimeout = 5 * time.Second
	httpReadTimeout       = 30 * time.Second
	httpWriteTimeout      = 60 * time.Second
	httpIdleTimeout       = 60 * time.Second
	httpShutdownTimeout   = 5 * time.Second
)

// Server serves the authenticated /v1/* HTTP/HTTPS API over Postfix master FD 6
// or a caller-provided net.Listener.
type Server struct {
	cfg            *config.Config
	store          *store.Store
	indexer        *indexer.Indexer
	runner         *postfixcmd.Runner
	queueDir       string
	auditLogger    AuditLogger
	tlsConfig      *tls.Config
	postfixVersion string
	errOut         io.Writer
	now            func() time.Time
}

// Option configures optional Server parameters.
type Option func(*Server)

// WithAuditLogger sets a custom AuditLogger for mutating API requests.
func WithAuditLogger(logger AuditLogger) Option {
	return func(s *Server) {
		s.auditLogger = logger
	}
}

// WithAuditWriter sets an io.Writer for JSON-line audit logging.
func WithAuditWriter(w io.Writer) Option {
	return func(s *Server) {
		if w != nil {
			s.auditLogger = NewJSONAuditLogger(w)
		}
	}
}

// WithTLSConfig sets a preloaded *tls.Config (typically loaded while running as root
// before dropping privileges).
func WithTLSConfig(tlsCfg *tls.Config) Option {
	return func(s *Server) {
		s.tlsConfig = tlsCfg
	}
}

// WithPostfixVersion records the Postfix mail_version reported by GET /v1/health.
func WithPostfixVersion(version string) Option {
	return func(s *Server) {
		s.postfixVersion = version
	}
}

// WithErrorLogger sets the destination writer for non-suppressed HTTP server error logs.
func WithErrorLogger(w io.Writer) Option {
	return func(s *Server) {
		if w != nil {
			s.errOut = w
		}
	}
}

// NewServer constructs an API Server wired with configuration, SQLite store,
// spool indexer, Postfix command runner, queue directory, and optional settings.
func NewServer(
	cfg *config.Config,
	st *store.Store,
	idx *indexer.Indexer,
	runner *postfixcmd.Runner,
	queueDir string,
	opts ...Option,
) (*Server, error) {
	if cfg == nil {
		return nil, errors.New("api: config must not be nil")
	}
	if st == nil {
		return nil, errors.New("api: store must not be nil")
	}

	s := &Server{
		cfg:      cfg,
		store:    st,
		indexer:  idx,
		runner:   runner,
		queueDir: queueDir,
		errOut:   os.Stderr,
		now:      time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	if s.auditLogger == nil {
		s.auditLogger = NewJSONAuditLogger(os.Stderr)
	}
	return s, nil
}

// PrepareMasterListener wraps inherited Postfix master FD 6 via net.FileListener,
// enforces non-loopback plaintext protection, and applies TLS if configured.
// Calling this before signaling MasterStatAvail ensures insecure configurations
// fail closed before Postfix treats the service as available.
func (s *Server) PrepareMasterListener(listenFD int, listenAddr string) (net.Listener, error) {
	if listenFD < 0 {
		return nil, fmt.Errorf("api: invalid listen fd %d", listenFD)
	}
	if listenAddr != "" {
		if err := CheckTransportSecurity(listenAddr, s.cfg); err != nil {
			return nil, err
		}
	}

	f := os.NewFile(uintptr(listenFD), "master-listen")
	if f == nil {
		return nil, fmt.Errorf("api: wrap listen fd %d: invalid descriptor", listenFD)
	}
	ln, err := net.FileListener(f)
	_ = f.Close()
	if err != nil {
		return nil, fmt.Errorf("api: file listener on fd %d: %w", listenFD, err)
	}

	if err := s.wrapAndCheckListener(&ln); err != nil {
		_ = ln.Close()
		return nil, err
	}
	return ln, nil
}

func (s *Server) wrapAndCheckListener(ln *net.Listener) error {
	if err := checkListenerSecurity(*ln, s.cfg); err != nil {
		return err
	}

	tlsCfg := s.tlsConfig
	if tlsCfg == nil && s.cfg.TLS.CertFile != "" {
		var err error
		tlsCfg, err = LoadTLSConfig(s.cfg)
		if err != nil {
			return err
		}
	}
	if tlsCfg != nil {
		*ln = tls.NewListener(*ln, tlsCfg)
	}
	return nil
}

// ServeMaster wraps listenFD via net.FileListener, validates transport security,
// applies TLS if configured, and serves HTTP until ctx is canceled.
func (s *Server) ServeMaster(ctx context.Context, listenFD int, listenAddr string) error {
	ln, err := s.PrepareMasterListener(listenFD, listenAddr)
	if err != nil {
		return err
	}
	return s.Serve(ctx, ln)
}

// Serve runs the HTTP server on ln with master wakeup-probe timeouts/log filtering
// and performs a bounded graceful shutdown when ctx is canceled.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	if ln == nil {
		return errors.New("api: listener must not be nil")
	}

	httpSrv := &http.Server{
		Handler:           s,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
		MaxHeaderBytes:    maxRequestBodyBytes,
		ErrorLog:          newFilteredErrorLog(s.errOut),
	}

	serveErrCh := make(chan error, 1)
	go func() {
		err := httpSrv.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErrCh <- err
			return
		}
		serveErrCh <- nil
	}()

	select {
	case err := <-serveErrCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
		return <-serveErrCh
	}
}

// ServeHTTP authenticates every request with constant-time Bearer token comparison,
// enforces per-endpoint scope requirements, and dispatches /v1/* routes.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rawToken, ok := extractBearerToken(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid bearer token")
		return
	}
	tok, valid := s.cfg.VerifyToken(rawToken)
	if !valid {
		writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid bearer token")
		return
	}

	escaped := strings.TrimPrefix(r.URL.EscapedPath(), "/")
	if escaped == "" {
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
		return
	}
	segs := strings.Split(escaped, "/")
	if len(segs) < 2 || segs[0] != "v1" {
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
		return
	}

	switch segs[1] {
	case "messages":
		s.dispatchMessages(w, r, tok, segs)
	case "events":
		if len(segs) != 2 {
			writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
			return
		}
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		if !requireScope(w, tok, config.ScopeRead) {
			return
		}
		s.handleListEvents(w, r)
	case "queue":
		s.dispatchQueue(w, r, tok, segs)
	case "health":
		if len(segs) != 2 {
			writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
			return
		}
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		if !requireScope(w, tok, config.ScopeRead) {
			return
		}
		s.handleHealth(w, r)
	default:
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
	}
}

func (s *Server) dispatchMessages(w http.ResponseWriter, r *http.Request, tok *config.Token, segs []string) {
	switch len(segs) {
	case 2:
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		if !requireScope(w, tok, config.ScopeRead) {
			return
		}
		s.handleListMessages(w, r)

	case 3:
		queueID, err := unescapeSegment(segs[2])
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_argument", "invalid queue_id encoding")
			return
		}
		switch r.Method {
		case http.MethodGet:
			if !requireScope(w, tok, config.ScopeRead) {
				return
			}
			s.handleGetMessage(w, r, queueID)
		case http.MethodDelete:
			if !requireScope(w, tok, config.ScopeWrite) {
				return
			}
			s.handleSingleMessagePostsuper(w, r, tok, queueID, "delete")
		default:
			writeMethodNotAllowed(w, http.MethodGet, http.MethodDelete)
		}

	case 4:
		queueID, err := unescapeSegment(segs[2])
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_argument", "invalid queue_id encoding")
			return
		}
		sub := segs[3]
		switch sub {
		case "content":
			if r.Method != http.MethodGet {
				writeMethodNotAllowed(w, http.MethodGet)
				return
			}
			if !requireScope(w, tok, config.ScopeContent) {
				return
			}
			s.handleGetMessageContent(w, r, queueID)
		case "hold", "release", "requeue", "expire":
			if r.Method != http.MethodPost {
				writeMethodNotAllowed(w, http.MethodPost)
				return
			}
			if !requireScope(w, tok, config.ScopeWrite) {
				return
			}
			s.handleSingleMessagePostsuper(w, r, tok, queueID, sub)
		case "flush":
			if r.Method != http.MethodPost {
				writeMethodNotAllowed(w, http.MethodPost)
				return
			}
			if !requireScope(w, tok, config.ScopeWrite) {
				return
			}
			s.handleFlushMessage(w, r, tok, queueID)
		default:
			writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
		}

	default:
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
	}
}

func (s *Server) dispatchQueue(w http.ResponseWriter, r *http.Request, tok *config.Token, segs []string) {
	switch len(segs) {
	case 2:
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		if !requireScope(w, tok, config.ScopeRead) {
			return
		}
		s.handleListQueue(w, r)

	case 3:
		switch segs[2] {
		case "actions":
			if r.Method != http.MethodPost {
				writeMethodNotAllowed(w, http.MethodPost)
				return
			}
			if !requireScope(w, tok, config.ScopeWrite) {
				return
			}
			s.handleBulkQueueActions(w, r, tok)
		case "flush":
			if r.Method != http.MethodPost {
				writeMethodNotAllowed(w, http.MethodPost)
				return
			}
			if !requireScope(w, tok, config.ScopeWrite) {
				return
			}
			s.handleFlushQueue(w, r, tok)
		default:
			writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
		}

	default:
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
	}
}

func unescapeSegment(seg string) (string, error) {
	if seg == "" {
		return "", errors.New("empty path segment")
	}
	return url.PathUnescape(seg)
}

func requireScope(w http.ResponseWriter, tok *config.Token, scope string) bool {
	if !tok.HasScope(scope) {
		writeError(w, http.StatusForbidden, "forbidden", fmt.Sprintf("token lacks required %q scope", scope))
		return false
	}
	return true
}

type apiErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type apiErrorEnvelope struct {
	Error apiErrorBody `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, apiErrorEnvelope{
		Error: apiErrorBody{
			Code:    code,
			Message: message,
		},
	})
}

func writeMethodNotAllowed(w http.ResponseWriter, allowed ...string) {
	if len(allowed) > 0 {
		w.Header().Set("Allow", strings.Join(allowed, ", "))
	}
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "HTTP method not allowed for this endpoint")
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":"internal_error","message":"failed to encode response"}}` + "\n"))
		return
	}
	data = append(data, '\n')
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}
