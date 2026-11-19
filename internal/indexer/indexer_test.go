package indexer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PretorianX/postfix-http-api/internal/store"
)

type mockSpool struct {
	mu           sync.Mutex
	segments     map[string][]string // segment name -> lines
	segOrder     []string
	deletedCalls []string
	stats        SpoolStats
	gapOnCursor  func(cur store.SpoolCursor) (bool, store.SpoolCursor)
}

func newMockSpool() *mockSpool {
	return &mockSpool{
		segments: make(map[string][]string),
	}
}

func (m *mockSpool) addSegment(name string, lines ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.segments[name]; !exists {
		m.segOrder = append(m.segOrder, name)
	}
	m.segments[name] = append(m.segments[name], lines...)
	m.recomputeHeadLocked()
}

func (m *mockSpool) recomputeHeadLocked() {
	var totalBytes int64
	var headSeg string
	var headOff int64
	for _, seg := range m.segOrder {
		lines := m.segments[seg]
		var segBytes int64
		for _, l := range lines {
			segBytes += int64(len(l)) + 1
		}
		totalBytes += segBytes
		headSeg = seg
		headOff = segBytes
	}
	m.stats.Bytes = totalBytes
	m.stats.HeadSegment = headSeg
	m.stats.HeadOffset = headOff
}

func (m *mockSpool) ReadBatch(_ context.Context, cursor store.SpoolCursor, maxLines int) (SpoolBatch, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.gapOnCursor != nil {
		if isGap, jumped := m.gapOnCursor(cursor); isGap {
			m.gapOnCursor = nil
			return SpoolBatch{
				NextCursor: jumped,
				Gap:        true,
			}, nil
		}
	}

	if len(m.segOrder) == 0 {
		return SpoolBatch{NextCursor: cursor}, nil
	}

	segIdx := 0
	lineIdx := int(cursor.Offset)
	if cursor.Segment != "" {
		found := false
		for i, s := range m.segOrder {
			if s == cursor.Segment {
				segIdx = i
				found = true
				break
			}
		}
		if !found {
			// Segment evicted while indexer was offline -> jump to oldest available.
			return SpoolBatch{
				NextCursor: store.SpoolCursor{Segment: m.segOrder[0], Offset: 0},
				Gap:        true,
			}, nil
		}
	}

	var out []string
	curSeg := m.segOrder[segIdx]
	curOff := lineIdx

	for segIdx < len(m.segOrder) && len(out) < maxLines {
		seg := m.segOrder[segIdx]
		lines := m.segments[seg]
		if seg != curSeg {
			curSeg = seg
			lineIdx = 0
		}
		for lineIdx < len(lines) && len(out) < maxLines {
			out = append(out, lines[lineIdx])
			lineIdx++
			curOff = lineIdx
		}
		if lineIdx >= len(lines) {
			segIdx++
			if segIdx < len(m.segOrder) && len(out) < maxLines {
				curSeg = m.segOrder[segIdx]
				lineIdx = 0
				curOff = 0
			}
		}
	}

	// If we reached exact end of segments, compute byte offset parity or line index offset cleanly.
	nextCur := store.SpoolCursor{
		Segment:      curSeg,
		Offset:       int64(curOff),
		LastRecordAt: cursor.LastRecordAt,
	}
	return SpoolBatch{
		Lines:      out,
		NextCursor: nextCur,
	}, nil
}

func (m *mockSpool) DeleteSegmentsBefore(segment string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deletedCalls = append(m.deletedCalls, segment)
	var remaining []string
	for _, s := range m.segOrder {
		if s < segment {
			delete(m.segments, s)
			continue
		}
		remaining = append(remaining, s)
	}
	m.segOrder = remaining
	m.recomputeHeadLocked()
	return nil
}

func (m *mockSpool) Stats() (SpoolStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stats, nil
}

type mockQueueLister struct {
	mu      sync.Mutex
	entries []store.QueueSnapshotEntry
	err     error
	calls   atomic.Int64
}

func (q *mockQueueLister) ListQueue(_ context.Context) ([]store.QueueSnapshotEntry, error) {
	q.calls.Add(1)
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.err != nil {
		return nil, q.err
	}
	cp := make([]store.QueueSnapshotEntry, len(q.entries))
	copy(cp, q.entries)
	return cp, nil
}

func TestIndexer_BatchIngestionCursorRestartAndCleanup(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	now := time.Date(2026, 11, 18, 17, 0, 0, 0, time.UTC)

	spool := newMockSpool()
	spool.addSegment("seg-00000000000000000001.log",
		"2026-11-18T16:54:43.000000000Z postfix/smtpd[1]: connect from localhost[127.0.0.1]",
		"2026-11-18T16:54:43.100000000Z postfix/smtpd[1]: 4jV74M63RlzHxK3: client=localhost[127.0.0.1]",
		"2026-11-18T16:54:43.200000000Z postfix/cleanup[2]: 4jV74M63RlzHxK3: info: header Subject: =?UTF-8?B?0J/RgNC40LLQtdGC?= order 42 from localhost[127.0.0.1]; from=<a@example.com> to=<b@example.org> proto=ESMTP",
	)
	spool.addSegment("seg-00000000000000000002.log",
		"2026-11-18T16:54:43.300000000Z postfix/qmgr[3]: 4jV74M63RlzHxK3: from=<a@example.com>, size=310, nrcpt=1 (queue active)",
		"2026-11-18T16:54:44.000000000Z postfix/smtp[4]: 4jV74M63RlzHxK3: to=<b@example.org>, relay=mx.example.org[192.0.2.2]:25, delay=0.9, delays=0.1/0/0.3/0.5, dsn=2.0.0, status=sent (250 Ok)",
	)

	// Phase 1: open store, ingest batch of 3 lines (spans seg-1 into seg-2? No, exactly seg-1's 3 lines!).
	st1, err := store.OpenWithClock(dbPath, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	lister := &mockQueueLister{}
	idx1, err := New(st1, spool, lister, Config{
		BatchSize:         3,
		PollInterval:      10 * time.Millisecond,
		QueueSyncInterval: time.Hour,
		Now:               func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	n, err := idx1.IngestOnce(ctx)
	if err != nil || n != 3 {
		t.Fatalf("IngestOnce #1 n=%d err=%v", n, err)
	}
	if err := st1.Close(); err != nil {
		t.Fatal(err)
	}

	// Phase 2: simulate restart by opening a fresh Store and Indexer on the same DB & spool.
	st2, err := store.OpenWithClock(dbPath, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()

	// Note: In our mockSpool, Offset counts lines within segment; seg-1 finished so cursor is (seg-00002, 0).
	idx2, err := New(st2, spool, lister, Config{
		BatchSize:         3,
		PollInterval:      10 * time.Millisecond,
		QueueSyncInterval: time.Hour,
		Now:               func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}

	n2, err := idx2.IngestOnce(ctx)
	if err != nil || n2 != 2 {
		t.Fatalf("IngestOnce #2 after restart n=%d err=%v", n2, err)
	}

	msg, err := st2.GetMessage(ctx, "4jV74M63RlzHxK3")
	if err != nil {
		t.Fatal(err)
	}
	if msg.Subject != "Привет order 42" || msg.QueueState != "active" || len(msg.Events) != 2 {
		t.Fatalf("unexpected message state after restart ingestion: %+v", msg)
	}

	// Segment 1 should have been cleaned up after advancing into segment 2.
	spool.mu.Lock()
	_, seg1Exists := spool.segments["seg-00000000000000000001.log"]
	spool.mu.Unlock()
	if seg1Exists {
		t.Fatal("expected consumed seg-1 deleted after commit")
	}
}

func TestIndexer_ReconcileRetentionAndHealthMetrics(t *testing.T) {
	now := time.Date(2026, 11, 18, 17, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	getNow := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(d)
	}

	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.OpenWithClock(dbPath, getNow)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	spool := newMockSpool()
	oldTS := now.Add(-10 * 24 * time.Hour).Format("2006-01-02T15:04:05.000000000Z")
	recentTS := now.Add(-1 * time.Minute).Format("2006-01-02T15:04:05.000000000Z")

	spool.addSegment("seg-00000000000000000001.log",
		fmt.Sprintf("%s postfix/qmgr[1]: OLDMSG1: from=<a@ex.com>, size=100, nrcpt=1 (queue active)", oldTS),
		fmt.Sprintf("%s postfix/qmgr[1]: OLDMSG1: removed", oldTS),
		fmt.Sprintf("%s postfix/qmgr[1]: LIVEMSG: from=<a@ex.com>, size=200, nrcpt=1 (queue active)", recentTS),
	)

	lister := &mockQueueLister{}
	idx, err := New(st, spool, lister, Config{
		BatchSize:         1000,
		PollInterval:      10 * time.Millisecond,
		QueueSyncInterval: time.Hour,
		RetentionDuration: 7 * 24 * time.Hour,
		Now:               getNow,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if _, err := idx.IngestOnce(ctx); err != nil {
		t.Fatal(err)
	}
	// Advance mock head to match line-offset convention for caught-up check.
	spool.mu.Lock()
	spool.stats.HeadSegment = "seg-00000000000000000001.log"
	spool.stats.HeadOffset = 3
	spool.mu.Unlock()

	// Cycle 1: LIVEMSG missing (1st miss), OLDMSG1 purged by retention!
	advance(1 * time.Minute)
	if err := idx.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetMessage(ctx, "OLDMSG1"); err != store.ErrNotFound {
		t.Fatalf("expected OLDMSG1 purged, got err=%v", err)
	}
	mLive, err := st.GetMessage(ctx, "LIVEMSG")
	if err != nil || mLive.ReconcileMisses != 1 || mLive.QueueState != "active" {
		t.Fatalf("expected LIVEMSG at 1 miss, got m=%+v err=%v", mLive, err)
	}

	m := idx.HealthMetrics()
	if m.Degraded || m.IndexerDelayMs != 0 || !m.LastQueueSyncOK {
		t.Fatalf("expected healthy metrics when caught up and sync ok, got %+v", m)
	}

	// Simulate postqueue -j failure -> Degraded = true, LastQueueSyncOK = false.
	lister.mu.Lock()
	lister.err = errors.New("postqueue timeout")
	lister.mu.Unlock()
	advance(1 * time.Minute)
	if err := idx.ReconcileOnce(ctx); err == nil {
		t.Fatal("expected ReconcileOnce error")
	}
	m = idx.HealthMetrics()
	if !m.Degraded || m.LastQueueSyncOK {
		t.Fatalf("expected Degraded=true when postqueue failed, got %+v", m)
	}

	// Recover postqueue -> 2nd miss closes LIVEMSG to unknown, LastQueueSyncOK = true.
	lister.mu.Lock()
	lister.err = nil
	lister.mu.Unlock()
	advance(1 * time.Minute)
	if err := idx.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	mLive, _ = st.GetMessage(ctx, "LIVEMSG")
	if mLive.QueueState != "unknown" || mLive.RemovedAt == nil {
		t.Fatalf("expected LIVEMSG closed to unknown on 2nd miss, got %+v", mLive)
	}

	// Simulate spool behind by > 10s -> IndexerDelayMs > 10000 -> Degraded = true.
	spool.mu.Lock()
	spool.stats.HeadSegment = "seg-00000000000000000002.log"
	spool.stats.HeadOffset = 100
	spool.stats.Bytes = 4096
	spool.mu.Unlock()
	advance(30 * time.Second)
	m = idx.HealthMetrics()
	if m.IndexerDelayMs <= 10000 || !m.Degraded || m.SpoolBytes != 4096 {
		t.Fatalf("expected lag > 10000ms and Degraded=true, got %+v", m)
	}

	// Simulate lost records -> Degraded = true even if caught up.
	spool.mu.Lock()
	spool.stats.HeadSegment = "seg-00000000000000000001.log"
	spool.stats.HeadOffset = 3
	spool.stats.LostRecords = 2
	spool.mu.Unlock()
	m = idx.HealthMetrics()
	if !m.Degraded || m.LostSpoolRecords != 2 {
		t.Fatalf("expected Degraded=true when LostSpoolRecords>0, got %+v", m)
	}
}

func TestIndexer_RunLoopsAndGapTrigger(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	spool := newMockSpool()
	// Simulate an initial cursor pointing to evicted seg-1 while disk only has seg-2.
	spool.addSegment("seg-00000000000000000002.log",
		"2026-11-18T16:54:43.000000000Z postfix/qmgr[1]: GAPMSG1: from=<a@ex.com>, size=100, nrcpt=1 (queue active)",
	)
	spool.gapOnCursor = func(cur store.SpoolCursor) (bool, store.SpoolCursor) {
		if cur.Segment == "" {
			return true, store.SpoolCursor{Segment: "seg-00000000000000000002.log", Offset: 0}
		}
		return false, cur
	}

	lister := &mockQueueLister{}
	idx, err := New(st, spool, lister, Config{
		BatchSize:         100,
		PollInterval:      15 * time.Millisecond,
		QueueSyncInterval: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- idx.Run(ctx)
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := st.GetMessage(context.Background(), "GAPMSG1"); err == nil && lister.calls.Load() >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop cleanly after context cancel")
	}

	if _, err := st.GetMessage(context.Background(), "GAPMSG1"); err != nil {
		t.Fatalf("expected GAPMSG1 ingested after gap recovery, got %v", err)
	}
	if lister.calls.Load() < 2 {
		t.Fatalf("expected startup + gap-triggered reconcile (>=2 calls), got %d", lister.calls.Load())
	}
	hm := idx.HealthMetrics()
	if hm.SpoolGaps == 0 || !hm.Degraded {
		t.Fatalf("expected SpoolGaps>0 and Degraded=true after cursor gap, got %+v", hm)
	}
}

func TestNextRetryAt(t *testing.T) {
	queueDir := t.TempDir()
	hashedDir := filepath.Join(queueDir, "deferred", "4")
	flatDir := filepath.Join(queueDir, "deferred")
	if err := os.MkdirAll(hashedDir, 0o755); err != nil {
		t.Fatal(err)
	}

	primaryFile := filepath.Join(hashedDir, "4jV74M63RlzHxK3")
	if err := os.WriteFile(primaryFile, []byte("deferred"), 0o600); err != nil {
		t.Fatal(err)
	}
	wantPrimary := time.Date(2026, 11, 18, 16, 59, 45, 0, time.UTC)
	if err := os.Chtimes(primaryFile, wantPrimary, wantPrimary); err != nil {
		t.Fatal(err)
	}

	got, err := NextRetryAt(queueDir, "4jV74M63RlzHxK3")
	if err != nil || got == nil || !got.Equal(wantPrimary) {
		t.Fatalf("primary NextRetryAt got=%v err=%v, want %v", got, err, wantPrimary)
	}

	// Fallback when hash_queue_depth=0 (flat deferred/<queue_id>).
	fallbackFile := filepath.Join(flatDir, "74D321034")
	if err := os.WriteFile(fallbackFile, []byte("deferred"), 0o600); err != nil {
		t.Fatal(err)
	}
	wantFallback := time.Date(2026, 11, 18, 17, 5, 0, 0, time.UTC)
	if err := os.Chtimes(fallbackFile, wantFallback, wantFallback); err != nil {
		t.Fatal(err)
	}

	got2, err := NextRetryAt(queueDir, "74D321034")
	if err != nil || got2 == nil || !got2.Equal(wantFallback) {
		t.Fatalf("fallback NextRetryAt got=%v err=%v, want %v", got2, err, wantFallback)
	}

	// Missing file returns (nil, nil).
	got3, err := NextRetryAt(queueDir, "MISSING123")
	if err != nil || got3 != nil {
		t.Fatalf("missing NextRetryAt got=%v err=%v, want (nil, nil)", got3, err)
	}

	// Invalid queue ID returns error.
	if _, err := NextRetryAt(queueDir, "../evil"); err == nil {
		t.Fatal("expected error for invalid queue id")
	}
}
