package store

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"net/url"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schemaSQL string

var memSeq atomic.Uint64

// Store manages the embedded SQLite WAL delivery state database.
type Store struct {
	writerDB *sql.DB
	readerDB *sql.DB
	now      func() time.Time
}

// Open opens or initializes the SQLite database at path with separate writer
// (MaxOpenConns=1, WAL, synchronous=NORMAL, foreign_keys=ON) and reader
// (MaxOpenConns=4, query_only=ON) connection pools.
func Open(path string) (*Store, error) {
	return OpenWithClock(path, time.Now)
}

// OpenWithClock opens the SQLite store using a custom clock function (primarily for tests).
func OpenWithClock(path string, nowFn func() time.Time) (*Store, error) {
	if path == "" {
		return nil, errors.New("store path must not be empty")
	}
	if nowFn == nil {
		nowFn = time.Now
	}

	writerDSN, readerDSN := buildDSNs(path)

	writerDB, err := sql.Open("sqlite", writerDSN)
	if err != nil {
		return nil, fmt.Errorf("open sqlite writer: %w", err)
	}
	writerDB.SetMaxOpenConns(1)
	writerDB.SetMaxIdleConns(1)

	if err := initWriter(writerDB, path == ":memory:"); err != nil {
		_ = writerDB.Close()
		return nil, err
	}

	readerDB, err := sql.Open("sqlite", readerDSN)
	if err != nil {
		_ = writerDB.Close()
		return nil, fmt.Errorf("open sqlite reader: %w", err)
	}
	readerDB.SetMaxOpenConns(4)
	readerDB.SetMaxIdleConns(4)

	if err := readerDB.Ping(); err != nil {
		_ = readerDB.Close()
		_ = writerDB.Close()
		return nil, fmt.Errorf("ping sqlite reader: %w", err)
	}

	return &Store{
		writerDB: writerDB,
		readerDB: readerDB,
		now:      nowFn,
	}, nil
}

func buildDSNs(path string) (writerDSN, readerDSN string) {
	writerQuery := "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)"
	readerQuery := "_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)&_pragma=query_only(ON)"

	if path == ":memory:" {
		id := memSeq.Add(1)
		base := fmt.Sprintf("file:postfix_api_mem_%d?mode=memory&cache=shared", id)
		return base + "&" + writerQuery, base + "&" + readerQuery
	}

	u := &url.URL{
		Scheme:   "file",
		Path:     path,
		RawQuery: writerQuery,
	}
	writerDSN = u.String()
	u.RawQuery = readerQuery
	readerDSN = u.String()
	return writerDSN, readerDSN
}

func initWriter(db *sql.DB, isMemory bool) error {
	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping sqlite writer: %w", err)
	}
	pragmas := []string{
		"PRAGMA busy_timeout = 5000;",
		"PRAGMA synchronous = NORMAL;",
		"PRAGMA foreign_keys = ON;",
	}
	if !isMemory {
		pragmas = append([]string{"PRAGMA journal_mode = WAL;"}, pragmas...)
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return fmt.Errorf("exec %q: %w", p, err)
		}
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}

// Close closes both reader and writer SQLite connection pools.
func (s *Store) Close() error {
	var errs []error
	if s.readerDB != nil {
		if err := s.readerDB.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.writerDB != nil {
		if err := s.writerDB.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// CheckpointWAL runs a non-blocking passive WAL checkpoint so the WAL file stays bounded.
func (s *Store) CheckpointWAL(ctx context.Context) error {
	_, err := s.writerDB.ExecContext(ctx, "PRAGMA wal_checkpoint(PASSIVE);")
	return err
}

func (s *Store) beginImmediate(ctx context.Context) (*sql.Conn, func() error, func(), error) {
	conn, err := s.writerDB.Conn(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("acquire writer conn: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("begin immediate: %w", err)
	}

	done := false
	commit := func() error {
		if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
			return fmt.Errorf("commit transaction: %w", err)
		}
		done = true
		return nil
	}
	cleanup := func() {
		if !done {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
		_ = conn.Close()
	}
	return conn, commit, cleanup, nil
}
