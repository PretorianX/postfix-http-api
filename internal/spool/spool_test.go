package spool

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSegmentNaming(t *testing.T) {
	if got := SegmentName(1); got != "seg-00000000000000000001.log" {
		t.Fatalf("SegmentName(1) = %q", got)
	}
	if seq, ok := ParseSegmentName("seg-00000000000000000042.log"); !ok || seq != 42 {
		t.Fatalf("ParseSegmentName valid = (%d, %v)", seq, ok)
	}
	for _, bad := range []string{
		"",
		"seg-00000000000000000000.log",
		"seg-0000000000000000001.log",
		"seg-00000000000000000001.txt",
		"foo-00000000000000000001.log",
		"seg-0000000000000000000a.log",
	} {
		if _, ok := ParseSegmentName(bad); ok {
			t.Errorf("ParseSegmentName(%q) = true; want false", bad)
		}
	}
}

func TestWriteReadRoundtripAndResume(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spool")
	w, err := NewWriter(dir, 4096, 16384)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	ts1 := time.Date(2026, 11, 19, 8, 30, 1, 123456789, time.FixedZone("EET", 2*3600))
	ts2 := ts1.Add(250 * time.Millisecond)
	ts3 := ts2.Add(500 * time.Millisecond)

	if err := w.WriteRecord(ts1, []byte("postfix/smtpd[1]: ABC123: client=localhost[127.0.0.1]\r\n")); err != nil {
		t.Fatalf("WriteRecord 1: %v", err)
	}
	if err := w.WriteRecord(ts2, []byte("postfix/cleanup[2]: ABC123: message-id=<1@example.com>")); err != nil {
		t.Fatalf("WriteRecord 2: %v", err)
	}
	if err := w.WriteRecord(ts3, []byte("postfix/qmgr[3]: ABC123: removed")); err != nil {
		t.Fatalf("WriteRecord 3: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}

	r := NewReader(dir)
	b1, err := r.ReadBatch("", 0, 2)
	if err != nil {
		t.Fatalf("ReadBatch 1: %v", err)
	}
	if b1.GapDetected || len(b1.Records) != 2 {
		t.Fatalf("b1 = %+v; want 2 records, no gap", b1)
	}
	if !b1.Records[0].Time.Equal(ts1.UTC()) || b1.Records[0].Raw != "postfix/smtpd[1]: ABC123: client=localhost[127.0.0.1]" {
		t.Errorf("record 0 = %+v", b1.Records[0])
	}
	if !b1.Records[1].Time.Equal(ts2.UTC()) || b1.Records[1].Raw != "postfix/cleanup[2]: ABC123: message-id=<1@example.com>" {
		t.Errorf("record 1 = %+v", b1.Records[1])
	}

	// Resume from b1 cursor.
	b2, err := r.ReadBatch(b1.NextSegment, b1.NextOffset, 10)
	if err != nil {
		t.Fatalf("ReadBatch 2: %v", err)
	}
	if len(b2.Records) != 1 || b2.Records[0].Raw != "postfix/qmgr[3]: ABC123: removed" {
		t.Fatalf("b2 = %+v", b2)
	}

	// Reading again at end yields 0 records without error.
	b3, err := r.ReadBatch(b2.NextSegment, b2.NextOffset, 10)
	if err != nil {
		t.Fatalf("ReadBatch 3: %v", err)
	}
	if len(b3.Records) != 0 {
		t.Fatalf("b3 = %+v; want 0 records", b3)
	}
}

func TestRotationEvictionGapCountingAndCleanup(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spool")
	// Each line is ~45 bytes; segmentSize=120 (~2 lines/seg), maxTotal=300 (~2-3 segs max).
	w, err := NewWriter(dir, 120, 300)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	baseTS := time.Date(2026, 11, 19, 9, 0, 0, 0, time.UTC)
	for i := 0; i < 20; i++ {
		rec := fmt.Sprintf("rec-%02d-payload", i)
		if err := w.WriteRecord(baseTS.Add(time.Duration(i)*time.Millisecond), []byte(rec)); err != nil {
			t.Fatalf("WriteRecord %d: %v", i, err)
		}
	}
	gapsBeforeClose := w.Gaps()
	if gapsBeforeClose == 0 {
		t.Fatal("expected eviction gaps > 0")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	st, err := Stats(dir)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.Gaps != gapsBeforeClose {
		t.Errorf("Stats.Gaps = %d; want %d", st.Gaps, gapsBeforeClose)
	}
	if st.TotalBytes > 300+120 {
		t.Errorf("TotalBytes = %d exceeds bounded cap 420", st.TotalBytes)
	}

	// Verify gaps.count is exact 21 bytes and persists across Writer restart.
	gapsRaw, err := os.ReadFile(filepath.Join(dir, GapsCounterFile))
	if err != nil || len(gapsRaw) != CounterFileSize {
		t.Fatalf("gaps.count len=%d err=%v; want %d", len(gapsRaw), err, CounterFileSize)
	}

	w2, err := NewWriter(dir, 120, 300)
	if err != nil {
		t.Fatalf("NewWriter restart: %v", err)
	}
	if w2.Gaps() != gapsBeforeClose {
		t.Errorf("restarted Writer.Gaps() = %d; want %d", w2.Gaps(), gapsBeforeClose)
	}
	_ = w2.Close()

	// Reader starting from evicted segment seg-1 detects gap and jumps to oldest remaining segment.
	r := NewReader(dir)
	batch, err := r.ReadBatch(SegmentName(1), 0, 100)
	if err != nil {
		t.Fatalf("ReadBatch from evicted segment: %v", err)
	}
	if !batch.GapDetected || r.Gaps() != 1 {
		t.Errorf("expected GapDetected=true and r.Gaps()=1; got %v, %d", batch.GapDetected, r.Gaps())
	}
	if len(batch.Records) == 0 {
		t.Fatal("expected remaining records after evicted gap")
	}
	// Last record must be rec-19-payload.
	last := batch.Records[len(batch.Records)-1]
	if last.Raw != "rec-19-payload" {
		t.Errorf("last record = %q; want rec-19-payload", last.Raw)
	}

	// DeleteBefore(batch.NextSegment) deletes all older closed segments while keeping NextSegment.
	deleted, err := r.DeleteBefore(batch.NextSegment)
	if err != nil {
		t.Fatalf("DeleteBefore: %v", err)
	}
	if deleted == 0 {
		t.Errorf("expected deleted > 0")
	}
	stAfter, err := Stats(dir)
	if err != nil {
		t.Fatalf("Stats after cleanup: %v", err)
	}
	if stAfter.OldestSegment != batch.NextSegment {
		t.Errorf("OldestSegment = %q; want %q", stAfter.OldestSegment, batch.NextSegment)
	}
}

func TestLostCounterOnWriteError(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spool")
	w, err := NewWriter(dir, 80, 400)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	ts := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	if err := w.WriteRecord(ts, []byte("first-line-fills-segment-beyond-80-bytes-limit-now")); err != nil {
		t.Fatalf("WriteRecord 1: %v", err)
	}

	// Close underlying activeFile directly to simulate EBADF/write failure while
	// keeping lostFile open for pwrite at offset 0.
	w.mu.Lock()
	_ = w.activeFile.Close()
	w.mu.Unlock()

	// Next write attempts write on closed activeFile -> fails and pwrites lost.count!
	// Wait: activeSize >= 80 will trigger rotateLocked; to test BOTH direct write failure
	// AND rotation failure:
	// 1. Force a smaller activeSize so direct write runs against closed activeFile:
	w.mu.Lock()
	w.activeSize = 10
	w.mu.Unlock()
	if err := w.WriteRecord(ts, []byte("short")); err == nil {
		t.Fatal("expected write error on closed activeFile")
	}
	if w.Lost() != 1 {
		t.Fatalf("Lost() = %d; want 1", w.Lost())
	}

	// 2. Make directory read-only (for non-root) or remove dir to test rotation open failure.
	// Note: when running inside container as root, chmod 0555 wouldn't block root;
	// verify via Stats and Writer restart persistence instead, plus chmod when non-root.
	if os.Geteuid() != 0 {
		if err := os.Chmod(dir, 0o555); err != nil {
			t.Fatalf("Chmod 0555: %v", err)
		}
		defer os.Chmod(dir, 0o750)

		w.mu.Lock()
		w.activeSize = 80 // trigger rotateLocked -> open new segment in 0555 dir fails!
		w.mu.Unlock()

		if err := w.WriteRecord(ts, []byte("trigger-rotate-fail")); err == nil {
			t.Fatal("expected rotation open error in 0555 directory")
		}
		if w.Lost() != 2 {
			t.Fatalf("Lost() = %d; want 2", w.Lost())
		}
	}

	_ = os.Chmod(dir, 0o750)
	if err := w.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Close: %v", err)
	}

	lostRaw, err := os.ReadFile(filepath.Join(dir, LostCounterFile))
	if err != nil || len(lostRaw) != CounterFileSize {
		t.Fatalf("lost.count len=%d err=%v; want %d", len(lostRaw), err, CounterFileSize)
	}
	st, err := Stats(dir)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	wantLost := uint64(1)
	if os.Geteuid() != 0 {
		wantLost = 2
	}
	if st.Lost != wantLost {
		t.Errorf("Stats.Lost = %d; want %d", st.Lost, wantLost)
	}

	// Verify lost.count persists across restart.
	w2, err := NewWriter(dir, 80, 400)
	if err != nil {
		t.Fatalf("NewWriter restart: %v", err)
	}
	if w2.Lost() != wantLost {
		t.Errorf("restarted Writer.Lost() = %d; want %d", w2.Lost(), wantLost)
	}
	_ = w2.Close()
}

func TestCrashTailAndCorruptLineResilience(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spool")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	// Segment 1: valid line, empty line, corrupt timestamp, missing space, and partial tail without '\n'.
	seg1Content := strings.Join([]string{
		"2026-11-19T08:00:00.000000000Z good-1",
		"",
		"   ",
		"not-a-timestamp bad-line",
		"nospaceatall",
		"2026-11-19T08:00:01.000000000Z good-2",
		"2026-11-19T08:00:02.000000000Z partial-tail-no-newline",
	}, "\n") // note: no trailing '\n'

	seg1Path := filepath.Join(dir, SegmentName(1))
	if err := os.WriteFile(seg1Path, []byte(seg1Content), 0o640); err != nil {
		t.Fatalf("WriteFile seg1: %v", err)
	}

	r := NewReader(dir)

	// While seg1 is the ONLY (newest) segment, partial tail must be paused on, not yielded.
	b1, err := r.ReadBatch("", 0, 100)
	if err != nil {
		t.Fatalf("ReadBatch active partial: %v", err)
	}
	if len(b1.Records) != 2 || b1.Records[0].Raw != "good-1" || b1.Records[1].Raw != "good-2" {
		t.Fatalf("b1 = %+v; want [good-1, good-2]", b1)
	}
	if b1.NextSegment != SegmentName(1) {
		t.Fatalf("b1.NextSegment = %q; want seg-1", b1.NextSegment)
	}

	// Re-reading before newline or rotation yields 0 records.
	bRetry, err := r.ReadBatch(b1.NextSegment, b1.NextOffset, 100)
	if err != nil {
		t.Fatalf("ReadBatch retry: %v", err)
	}
	if len(bRetry.Records) != 0 || bRetry.NextOffset != b1.NextOffset {
		t.Fatalf("bRetry = %+v; want 0 records at same offset %d", bRetry, b1.NextOffset)
	}

	// Now simulate prior SIGKILL + receiver restart creating seg2.
	seg2Path := filepath.Join(dir, SegmentName(2))
	if err := os.WriteFile(seg2Path, []byte("2026-11-19T08:00:03.000000000Z good-3\n"), 0o640); err != nil {
		t.Fatalf("WriteFile seg2: %v", err)
	}

	// Now seg1's partial tail must be discarded and reading advances cleanly to seg2.
	b2, err := r.ReadBatch(b1.NextSegment, b1.NextOffset, 100)
	if err != nil {
		t.Fatalf("ReadBatch after rotation: %v", err)
	}
	if len(b2.Records) != 1 || b2.Records[0].Raw != "good-3" || b2.Records[0].Segment != SegmentName(2) {
		t.Fatalf("b2 = %+v; want [good-3 in seg-2]", b2)
	}

	// DeleteBefore(seg2) removes seg1 cleanly.
	del, err := r.DeleteBefore(b2.NextSegment)
	if err != nil {
		t.Fatalf("DeleteBefore: %v", err)
	}
	if del != 1 {
		t.Errorf("deleted = %d; want 1", del)
	}
}

func TestConcurrentWriterAndReader(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spool")
	w, err := NewWriter(dir, 512, 64*1024)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer w.Close()

	const total = 500
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		base := time.Now().UTC()
		for i := 0; i < total; i++ {
			if err := w.WriteRecord(base.Add(time.Duration(i)*time.Microsecond), []byte(fmt.Sprintf("msg-%04d", i))); err != nil {
				t.Errorf("WriteRecord %d: %v", i, err)
				return
			}
		}
	}()

	r := NewReader(dir)
	var got []string
	curSeg := ""
	var curOff int64
	deadline := time.Now().Add(5 * time.Second)
	for len(got) < total && time.Now().Before(deadline) {
		b, err := r.ReadBatch(curSeg, curOff, 37)
		if err != nil {
			t.Fatalf("ReadBatch: %v", err)
		}
		if b.GapDetected {
			t.Fatalf("unexpected gap in large spool: %+v", b)
		}
		for _, rec := range b.Records {
			got = append(got, rec.Raw)
		}
		curSeg = b.NextSegment
		curOff = b.NextOffset
		if curSeg != "" {
			if _, err := r.DeleteBefore(curSeg); err != nil {
				t.Fatalf("DeleteBefore: %v", err)
			}
		}
		if len(b.Records) == 0 {
			time.Sleep(2 * time.Millisecond)
		}
	}
	wg.Wait()

	if len(got) != total {
		t.Fatalf("read %d records; want %d", len(got), total)
	}
	for i := 0; i < total; i++ {
		want := fmt.Sprintf("msg-%04d", i)
		if got[i] != want {
			t.Fatalf("got[%d] = %q; want %q", i, got[i], want)
		}
	}
}

func TestWriterZeroAllocSteadyState(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spool")
	w, err := NewWriter(dir, 16*1024*1024, 256*1024*1024)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer w.Close()

	rec := []byte("postfix/qmgr[123]: 4jV74M63RlzHxK3: removed")
	ts := time.Date(2026, 11, 19, 8, 30, 0, 123456789, time.UTC)

	// Warm up once.
	if err := w.WriteRecord(ts, rec); err != nil {
		t.Fatalf("warm WriteRecord: %v", err)
	}

	allocs := testing.AllocsPerRun(100, func() {
		if err := w.WriteRecord(ts, rec); err != nil {
			t.Fatalf("WriteRecord: %v", err)
		}
	})
	if allocs != 0 {
		t.Fatalf("WriteRecord allocs = %v; want 0", allocs)
	}

	preformatted := []byte("2026-11-19T08:30:00.123456789Z postfix/qmgr[123]: 4jV74M63RlzHxK3: removed\n")
	allocs2 := testing.AllocsPerRun(100, func() {
		if err := w.AppendFormatted(preformatted); err != nil {
			t.Fatalf("AppendFormatted: %v", err)
		}
	})
	if allocs2 != 0 {
		t.Fatalf("AppendFormatted allocs = %v; want 0", allocs2)
	}
}
