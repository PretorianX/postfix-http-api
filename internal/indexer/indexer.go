package indexer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/PretorianX/postfix-http-api/internal/parser"
	"github.com/PretorianX/postfix-http-api/internal/store"
)

const (
	defaultBatchSize         = 1000
	defaultPollInterval      = 100 * time.Millisecond
	defaultQueueSyncInterval = 5 * time.Minute
	defaultRetentionDuration = 168 * time.Hour
)

var validQueueIDRe = regexp.MustCompile(`^[A-Za-z0-9]{6,32}$`)

// SpoolStats summarizes the disk spool state for health reporting and lag calculation.
type SpoolStats struct {
	Gaps        uint64
	LostRecords uint64
	Bytes       int64
	HeadSegment string
	HeadOffset  int64
}

// SpoolBatch holds lines read from the spool alongside the resulting cursor position.
type SpoolBatch struct {
	Lines      []string
	NextCursor store.SpoolCursor
	Gap        bool
}

// SpoolSource decouples the indexer from the concrete disk spool implementation.
type SpoolSource interface {
	ReadBatch(ctx context.Context, cursor store.SpoolCursor, maxLines int) (SpoolBatch, error)
	DeleteSegmentsBefore(segment string) error
	Stats() (SpoolStats, error)
}

// QueueLister decouples the indexer from the concrete `postqueue -j` command runner.
type QueueLister interface {
	ListQueue(ctx context.Context) ([]store.QueueSnapshotEntry, error)
}

// Config configures the Indexer loops and intervals.
type Config struct {
	BatchSize         int
	PollInterval      time.Duration
	QueueSyncInterval time.Duration
	RetentionDuration time.Duration
	Now               func() time.Time
}

// Metrics contains health and lag telemetry reported by the indexer for /v1/health.
type Metrics struct {
	IndexerDelayMs     int64
	SpoolGaps          uint64
	LostSpoolRecords   uint64
	SpoolBytes         int64
	Degraded           bool
	LastProcessedTime  *time.Time
	LastQueueSyncAt    *time.Time
	LastQueueSyncOK    bool
	LastQueueSyncError string
}

// Indexer consumes spool batches into SQLite and periodically reconciles the live queue
// and enforces retention.
type Indexer struct {
	store  *store.Store
	spool  SpoolSource
	lister QueueLister
	cfg    Config

	reconcileNowCh chan struct{}

	ingestMu sync.Mutex

	mu                 sync.RWMutex
	cursorLoaded       bool
	cursor             store.SpoolCursor
	caughtUp           bool
	lastProcessedTime  *time.Time
	cursorGapCount     int64
	lastQueueSyncAt    *time.Time
	lastQueueSyncOK    bool
	lastQueueSyncError string
	lastSpoolStats     SpoolStats
}

// New constructs an Indexer with defaults applied and loads initial cursor/reconcile telemetry.
func New(st *store.Store, spool SpoolSource, lister QueueLister, cfg Config) (*Indexer, error) {
	if st == nil {
		return nil, errors.New("store must not be nil")
	}
	if spool == nil {
		return nil, errors.New("spool source must not be nil")
	}
	if lister == nil {
		return nil, errors.New("queue lister must not be nil")
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.QueueSyncInterval <= 0 {
		cfg.QueueSyncInterval = defaultQueueSyncInterval
	}
	if cfg.RetentionDuration <= 0 {
		cfg.RetentionDuration = defaultRetentionDuration
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	idx := &Indexer{
		store:           st,
		spool:           spool,
		lister:          lister,
		cfg:             cfg,
		reconcileNowCh:  make(chan struct{}, 1),
		lastQueueSyncOK: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if cur, err := st.GetSpoolCursor(ctx); err == nil {
		idx.cursor = cur
		idx.cursorLoaded = true
		if cur.LastRecordAt != nil {
			t := *cur.LastRecordAt
			idx.lastProcessedTime = &t
		}
	}
	if rs, err := st.GetReconcileState(ctx); err == nil {
		idx.cursorGapCount = rs.CursorGapCount
		idx.lastQueueSyncOK = rs.LastSyncOK
		idx.lastQueueSyncError = rs.LastSyncError
		if rs.LastSyncAt != nil {
			t := *rs.LastSyncAt
			idx.lastQueueSyncAt = &t
		}
	}
	if stats, err := spool.Stats(); err == nil {
		idx.lastSpoolStats = stats
		idx.caughtUp = isCaughtUpWithCursor(idx.cursor, stats, true)
	}

	return idx, nil
}

// Run starts both the spool ingestion loop and the periodic queue reconciliation + retention loop
// and blocks until ctx is canceled and active commits finish.
func (idx *Indexer) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		idx.runIngestionLoop(ctx)
	}()

	go func() {
		defer wg.Done()
		idx.runReconcileLoop(ctx)
	}()

	wg.Wait()
	return nil
}

func (idx *Indexer) runIngestionLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		n, err := idx.IngestOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			timer := time.NewTimer(idx.cfg.PollInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			continue
		}

		// When a full batch was consumed, immediately pull the next batch with zero delay.
		if n >= idx.cfg.BatchSize {
			continue
		}

		timer := time.NewTimer(idx.cfg.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (idx *Indexer) runReconcileLoop(ctx context.Context) {
	// Run an initial reconciliation + retention cycle immediately at startup.
	if ctx.Err() == nil {
		_ = idx.ReconcileOnce(ctx)
	}

	ticker := time.NewTicker(idx.cfg.QueueSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = idx.ReconcileOnce(ctx)
		case <-idx.reconcileNowCh:
			_ = idx.ReconcileOnce(ctx)
		}
	}
}

// IngestOnce reads up to cfg.BatchSize spool lines, parses them, commits them atomically
// with the updated spool cursor, and cleans up fully consumed older segments.
func (idx *Indexer) IngestOnce(ctx context.Context) (int, error) {
	idx.ingestMu.Lock()
	defer idx.ingestMu.Unlock()

	idx.mu.RLock()
	cur := idx.cursor
	loaded := idx.cursorLoaded
	idx.mu.RUnlock()

	if !loaded {
		var err error
		cur, err = idx.store.GetSpoolCursor(ctx)
		if err != nil {
			return 0, err
		}
		idx.mu.Lock()
		idx.cursor = cur
		idx.cursorLoaded = true
		idx.mu.Unlock()
	}

	batch, err := idx.spool.ReadBatch(ctx, cur, idx.cfg.BatchSize)
	if err != nil {
		return 0, err
	}

	if batch.Gap {
		txCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		err := idx.store.RecordCursorGap(txCtx, batch.NextCursor)
		cancel()
		if err != nil {
			return 0, err
		}
		idx.mu.Lock()
		idx.cursorGapCount++
		idx.cursor = batch.NextCursor
		idx.mu.Unlock()

		// Trigger immediate postqueue -j reconciliation after a spool gap.
		select {
		case idx.reconcileNowCh <- struct{}{}:
		default:
		}
		cur = batch.NextCursor
	}

	if len(batch.Lines) == 0 {
		if batch.NextCursor.Segment != cur.Segment || batch.NextCursor.Offset != cur.Offset {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			txCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			err := idx.store.ApplyBatch(txCtx, nil, batch.NextCursor)
			cancel()
			if err != nil {
				return 0, err
			}
			if batch.NextCursor.Segment != "" {
				_ = idx.spool.DeleteSegmentsBefore(batch.NextCursor.Segment)
			}
			cur = batch.NextCursor
		}

		idx.mu.Lock()
		idx.cursor = cur
		idx.caughtUp = true
		idx.mu.Unlock()
		return 0, nil
	}

	entries := make([]store.BatchEntry, 0, len(batch.Lines))
	var lastTS time.Time
	for _, line := range batch.Lines {
		ts, rec, ok := parser.ParseSpoolLine(line)
		if ts.IsZero() && !ok {
			if r, rok := parser.ParseLine(line); rok {
				rec = r
				ok = true
				ts = r.Timestamp
			}
		}
		if !ts.IsZero() {
			lastTS = ts
		}
		if ok && rec != nil {
			entryTS := ts
			if entryTS.IsZero() {
				entryTS = idx.cfg.Now().UTC()
			}
			entries = append(entries, store.BatchEntry{
				Record:    *rec,
				Timestamp: entryTS,
			})
		}
	}

	nextCursor := batch.NextCursor
	if nextCursor.LastRecordAt == nil && !lastTS.IsZero() {
		t := lastTS.UTC()
		nextCursor.LastRecordAt = &t
	}

	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	// Finish committing an active batch cleanly even if shutdown starts mid-commit.
	txCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	err = idx.store.ApplyBatch(txCtx, entries, nextCursor)
	cancel()
	if err != nil {
		return 0, err
	}

	// Delete older spool segments ONLY after ApplyBatch committed.
	if nextCursor.Segment != "" {
		_ = idx.spool.DeleteSegmentsBefore(nextCursor.Segment)
	}

	idx.mu.Lock()
	idx.cursor = nextCursor
	idx.cursorLoaded = true
	idx.caughtUp = len(batch.Lines) < idx.cfg.BatchSize
	if nextCursor.LastRecordAt != nil {
		t := *nextCursor.LastRecordAt
		idx.lastProcessedTime = &t
	} else if !lastTS.IsZero() {
		t := lastTS.UTC()
		idx.lastProcessedTime = &t
	}
	idx.mu.Unlock()

	return len(batch.Lines), nil
}

// ReconcileOnce runs one queue reconciliation (`postqueue -j`), updates spool gap telemetry,
// purges expired removed messages, and performs a passive WAL checkpoint.
func (idx *Indexer) ReconcileOnce(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	syncStart := idx.cfg.Now().UTC()
	entries, listErr := idx.lister.ListQueue(ctx)

	txCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	if listErr != nil {
		errMsg := listErr.Error()
		_ = idx.store.RecordReconcileError(txCtx, errMsg, idx.cfg.Now().UTC())
		idx.mu.Lock()
		t := idx.cfg.Now().UTC()
		idx.lastQueueSyncAt = &t
		idx.lastQueueSyncOK = false
		idx.lastQueueSyncError = errMsg
		idx.mu.Unlock()
		return listErr
	}

	if err := idx.store.ReconcileQueue(txCtx, entries, syncStart); err != nil {
		return err
	}

	idx.mu.Lock()
	idx.lastQueueSyncAt = &syncStart
	idx.lastQueueSyncOK = true
	idx.lastQueueSyncError = ""
	idx.mu.Unlock()

	if stats, err := idx.spool.Stats(); err == nil {
		if stats.Gaps > 0 {
			_ = idx.store.SyncSpoolGapCount(txCtx, stats.Gaps)
		}
		idx.mu.Lock()
		idx.lastSpoolStats = stats
		if int64(stats.Gaps) > idx.cursorGapCount {
			idx.cursorGapCount = int64(stats.Gaps)
		}
		idx.mu.Unlock()
	}

	cutoff := idx.cfg.Now().UTC().Add(-idx.cfg.RetentionDuration)
	if _, err := idx.store.PurgeExpired(txCtx, cutoff); err != nil {
		return err
	}
	_ = idx.store.CheckpointWAL(txCtx)
	return nil
}

// HealthMetrics returns current indexer lag, spool gap/loss/size counters, and degraded status.
func (idx *Indexer) HealthMetrics() Metrics {
	stats, err := idx.spool.Stats()

	idx.mu.Lock()
	if err == nil {
		idx.lastSpoolStats = stats
	} else {
		stats = idx.lastSpoolStats
	}
	cur := idx.cursor
	caughtUp := isCaughtUpWithCursor(cur, stats, idx.caughtUp)
	var lastProc *time.Time
	if idx.lastProcessedTime != nil {
		t := *idx.lastProcessedTime
		lastProc = &t
	}
	cursorGapCount := idx.cursorGapCount
	var lastSync *time.Time
	if idx.lastQueueSyncAt != nil {
		t := *idx.lastQueueSyncAt
		lastSync = &t
	}
	syncOK := idx.lastQueueSyncOK
	syncErr := idx.lastQueueSyncError
	idx.mu.Unlock()

	var delayMs int64
	if !caughtUp && lastProc != nil {
		delayMs = idx.cfg.Now().UTC().Sub(*lastProc).Milliseconds()
		if delayMs < 0 {
			delayMs = 0
		}
	}

	gaps := stats.Gaps
	if cursorGapCount > 0 && uint64(cursorGapCount) > gaps {
		gaps = uint64(cursorGapCount)
	}

	degraded := gaps > 0 || stats.LostRecords > 0 || !syncOK || delayMs > 10000

	return Metrics{
		IndexerDelayMs:     delayMs,
		SpoolGaps:          gaps,
		LostSpoolRecords:   stats.LostRecords,
		SpoolBytes:         stats.Bytes,
		Degraded:           degraded,
		LastProcessedTime:  lastProc,
		LastQueueSyncAt:    lastSync,
		LastQueueSyncOK:    syncOK,
		LastQueueSyncError: syncErr,
	}
}

func isCaughtUpWithCursor(cur store.SpoolCursor, stats SpoolStats, fallbackCaughtUp bool) bool {
	if stats.HeadSegment != "" {
		if cur.Segment > stats.HeadSegment {
			return true
		}
		if cur.Segment == stats.HeadSegment && cur.Offset >= stats.HeadOffset {
			return true
		}
		return false
	}
	if stats.Bytes == 0 {
		return true
	}
	return fallbackCaughtUp
}

// NextRetryAt stats `<queueDir>/deferred/<first_char>/<queueID>` (falling back to
// `<queueDir>/deferred/<queueID>` when hash_queue_depth=0) and returns the mtime in UTC,
// or (nil, nil) when the queue file is absent.
func NextRetryAt(queueDir, queueID string) (*time.Time, error) {
	if !validQueueIDRe.MatchString(queueID) {
		return nil, fmt.Errorf("invalid queue_id %q", queueID)
	}

	primary := filepath.Join(queueDir, "deferred", queueID[:1], queueID)
	info, err := os.Stat(primary)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		fallback := filepath.Join(queueDir, "deferred", queueID)
		info, err = os.Stat(fallback)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, nil
			}
			return nil, err
		}
	}

	t := info.ModTime().UTC()
	return &t, nil
}
