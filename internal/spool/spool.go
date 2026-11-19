// Package spool implements an append-only, bounded, segmented disk spool in
// <state_dir>/spool/ with zero-alloc steady-state writes, segment rotation,
// oldest-segment eviction with persistent gaps.count, and pwrite-based
// persistent lost.count for ENOSPC resilience.
package spool

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// TimeLayout is the fixed-width UTC RFC3339Nano timestamp layout used on every spool line.
	TimeLayout = "2006-01-02T15:04:05.000000000Z"

	SegmentPrefix   = "seg-"
	SegmentSuffix   = ".log"
	SegmentSeqWidth = 20

	GapsCounterFile = "gaps.count"
	LostCounterFile = "lost.count"
	CounterFileSize = 21 // 20 zero-padded digits + '\n'
)

// Record represents a single timestamped line read from the disk spool.
type Record struct {
	Time    time.Time
	Raw     string
	Segment string
	Offset  int64
}

// Batch is the result of Reader.ReadBatch. NextSegment and NextOffset represent
// the cursor position right after the last processed byte in the batch.
type Batch struct {
	Records     []Record
	NextSegment string
	NextOffset  int64
	GapDetected bool
}

// SpoolStats summarizes persistent counters and current disk usage in the spool directory.
type SpoolStats struct {
	Gaps          uint64
	Lost          uint64
	TotalBytes    int64
	SegmentCount  int
	OldestSegment string
	NewestSegment string
}

type segEntry struct {
	seq  uint64
	name string
	size int64
}

// SegmentName formats a 1-based sequence number as seg-00000000000000000001.log.
func SegmentName(seq uint64) string {
	return fmt.Sprintf("seg-%020d.log", seq)
}

// ParseSegmentName parses a segment filename matching seg-%020d.log (seq >= 1).
func ParseSegmentName(name string) (uint64, bool) {
	if len(name) != len(SegmentPrefix)+SegmentSeqWidth+len(SegmentSuffix) {
		return 0, false
	}
	if !strings.HasPrefix(name, SegmentPrefix) || !strings.HasSuffix(name, SegmentSuffix) {
		return 0, false
	}
	digits := name[len(SegmentPrefix) : len(SegmentPrefix)+SegmentSeqWidth]
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return 0, false
		}
	}
	seq, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || seq == 0 {
		return 0, false
	}
	return seq, true
}

func listSegments(dir string) ([]segEntry, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	segs := make([]segEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		seq, ok := ParseSegmentName(entry.Name())
		if !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		segs = append(segs, segEntry{
			seq:  seq,
			name: entry.Name(),
			size: info.Size(),
		})
	}
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].seq < segs[j].seq
	})
	return segs, nil
}

// Writer appends timestamped lines to segmented spool files, enforcing
// segment rotation, total spool size bounds, and persistent counters.
type Writer struct {
	dir              string
	segmentSizeBytes int64
	maxTotalBytes    int64

	mu             sync.Mutex
	activeFile     *os.File
	activeSeq      uint64
	activeName     string
	activeSize     int64
	totalBytes     int64
	closed         bool
	gaps           uint64
	lost           uint64
	gapsFile       *os.File
	lostFile       *os.File
	lineBuf        []byte
	counterBuf     [CounterFileSize]byte
	lastErrLogNano int64
}

// NewWriter opens or creates the spool directory, initializes persistent 21-byte
// gaps.count and lost.count files, and opens the active write segment.
func NewWriter(spoolDir string, segmentSizeBytes, maxTotalBytes int64) (*Writer, error) {
	if spoolDir == "" {
		return nil, errors.New("spool: spool directory is empty")
	}
	if segmentSizeBytes <= 0 {
		return nil, fmt.Errorf("spool: segment_size_bytes (%d) must be > 0", segmentSizeBytes)
	}
	if maxTotalBytes < segmentSizeBytes {
		return nil, fmt.Errorf("spool: max_total_bytes (%d) must be >= segment_size_bytes (%d)", maxTotalBytes, segmentSizeBytes)
	}
	if err := os.MkdirAll(spoolDir, 0o750); err != nil {
		return nil, fmt.Errorf("spool: mkdir %q: %w", spoolDir, err)
	}

	gapsFile, gapsVal, err := openCounterFile(filepath.Join(spoolDir, GapsCounterFile))
	if err != nil {
		return nil, err
	}
	lostFile, lostVal, err := openCounterFile(filepath.Join(spoolDir, LostCounterFile))
	if err != nil {
		_ = gapsFile.Close()
		return nil, err
	}

	segs, err := listSegments(spoolDir)
	if err != nil {
		_ = gapsFile.Close()
		_ = lostFile.Close()
		return nil, fmt.Errorf("spool: scan segments: %w", err)
	}

	var totalBytes int64
	for _, s := range segs {
		totalBytes += s.size
	}

	var activeSeq uint64 = 1
	var activeSize int64
	if len(segs) > 0 {
		last := segs[len(segs)-1]
		if last.size == 0 {
			activeSeq = last.seq
		} else {
			// Start a fresh segment so any partial tail left by a prior SIGKILL
			// cannot glue onto subsequent records.
			activeSeq = last.seq + 1
		}
	}

	activeName := SegmentName(activeSeq)
	activeFile, err := os.OpenFile(filepath.Join(spoolDir, activeName), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o640)
	if err != nil {
		_ = gapsFile.Close()
		_ = lostFile.Close()
		return nil, fmt.Errorf("spool: open active segment %q: %w", activeName, err)
	}

	return &Writer{
		dir:              spoolDir,
		segmentSizeBytes: segmentSizeBytes,
		maxTotalBytes:    maxTotalBytes,
		activeFile:       activeFile,
		activeSeq:        activeSeq,
		activeName:       activeName,
		activeSize:       activeSize,
		totalBytes:       totalBytes,
		gaps:             gapsVal,
		lost:             lostVal,
		gapsFile:         gapsFile,
		lostFile:         lostFile,
		lineBuf:          make([]byte, 0, 64+65537),
	}, nil
}

func openCounterFile(path string) (*os.File, uint64, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o640)
	if err != nil {
		return nil, 0, fmt.Errorf("spool: open counter %q: %w", path, err)
	}

	var raw [64]byte
	n, rerr := f.ReadAt(raw[:], 0)
	if rerr != nil && !errors.Is(rerr, io.EOF) {
		_ = f.Close()
		return nil, 0, fmt.Errorf("spool: read counter %q: %w", path, rerr)
	}

	val := parseCounterBytes(raw[:n])
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, fmt.Errorf("spool: stat counter %q: %w", path, err)
	}

	// Pre-allocate exact 21 bytes so subsequent pwrite at offset 0 succeeds on ENOSPC.
	if info.Size() < CounterFileSize {
		var buf [CounterFileSize]byte
		formatCounter21(&buf, val)
		if _, err := f.WriteAt(buf[:], 0); err != nil {
			_ = f.Close()
			return nil, 0, fmt.Errorf("spool: initialize counter %q: %w", path, err)
		}
	}
	return f, val, nil
}

func parseCounterBytes(b []byte) uint64 {
	s := strings.TrimSpace(string(b))
	if s == "" {
		return 0
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func formatCounter21(dst *[CounterFileSize]byte, val uint64) {
	for i := CounterFileSize - 2; i >= 0; i-- {
		dst[i] = byte('0' + val%10)
		val /= 10
	}
	dst[CounterFileSize-1] = '\n'
}

// ActiveSegment returns the filename of the currently open write segment.
func (w *Writer) ActiveSegment() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.activeName
}

// Gaps returns the accumulated segment eviction gap counter.
func (w *Writer) Gaps() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.gaps
}

// Lost returns the accumulated failed spool write counter.
func (w *Writer) Lost() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lost
}

// Append writes record stamped with the current UTC time to the spool.
func (w *Writer) Append(record []byte) error {
	return w.WriteRecord(time.Now().UTC(), record)
}

// WriteRecord formats "<RFC3339Nano-UTC> <record>\n" into a reusable buffer
// and writes it to the spool with zero steady-state heap allocations.
func (w *Writer) WriteRecord(ts time.Time, record []byte) error {
	rec := bytes.TrimRight(record, "\r\n\x00")
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("spool: writer is closed")
	}

	w.lineBuf = ts.UTC().AppendFormat(w.lineBuf[:0], TimeLayout)
	w.lineBuf = append(w.lineBuf, ' ')
	w.lineBuf = append(w.lineBuf, rec...)
	w.lineBuf = append(w.lineBuf, '\n')

	return w.writeLineLocked(w.lineBuf)
}

// AppendFormatted writes an already-formatted "<RFC3339Nano-UTC> <record>\n"
// byte slice without copying or heap allocation.
func (w *Writer) AppendFormatted(line []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("spool: writer is closed")
	}
	return w.writeLineLocked(line)
}

func (w *Writer) writeLineLocked(line []byte) error {
	if len(line) == 0 {
		return nil
	}

	if w.activeFile == nil || (w.activeSize > 0 && (w.activeSize >= w.segmentSizeBytes || w.activeSize+int64(len(line)) > w.segmentSizeBytes)) {
		if err := w.rotateLocked(); err != nil {
			w.recordLostLocked(err)
			return err
		}
	}

	if w.totalBytes+int64(len(line)) > w.maxTotalBytes {
		w.evictLocked(int64(len(line)))
	}

	n, err := writeAll(w.activeFile, line)
	if n > 0 {
		w.activeSize += int64(n)
		w.totalBytes += int64(n)
	}
	if err != nil {
		if n > 0 {
			// A partial line may have been written; close so next write rotates
			// rather than gluing a subsequent record onto an incomplete line.
			_ = w.activeFile.Close()
			w.activeFile = nil
			w.activeSeq++
			w.activeName = SegmentName(w.activeSeq)
			w.activeSize = 0
		}
		w.recordLostLocked(err)
		return fmt.Errorf("spool: write segment %s: %w", w.activeName, err)
	}

	return nil
}

func writeAll(f *os.File, b []byte) (int, error) {
	written := 0
	for written < len(b) {
		n, err := f.Write(b[written:])
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func (w *Writer) rotateLocked() error {
	if w.activeFile != nil {
		_ = w.activeFile.Close()
		w.activeFile = nil
		w.activeSeq++
		w.activeName = SegmentName(w.activeSeq)
		w.activeSize = 0
	}

	path := filepath.Join(w.dir, w.activeName)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o640)
	if err != nil {
		return fmt.Errorf("spool: open segment %s: %w", w.activeName, err)
	}
	var size int64
	if info, serr := f.Stat(); serr == nil {
		size = info.Size()
	}
	w.activeFile = f
	w.activeSize = size
	return nil
}

func (w *Writer) evictLocked(incomingBytes int64) {
	segs, err := listSegments(w.dir)
	if err != nil {
		return
	}

	var total int64
	for _, s := range segs {
		total += s.size
	}

	for _, s := range segs {
		if total+incomingBytes <= w.maxTotalBytes {
			break
		}
		if s.seq >= w.activeSeq {
			break
		}
		err := os.Remove(filepath.Join(w.dir, s.name))
		if err == nil {
			total -= s.size
			w.gaps++
			if w.gapsFile != nil {
				formatCounter21(&w.counterBuf, w.gaps)
				_, _ = w.gapsFile.WriteAt(w.counterBuf[:], 0)
			}
		} else if errors.Is(err, os.ErrNotExist) {
			total -= s.size
		}
	}
	if total < 0 {
		total = 0
	}
	w.totalBytes = total
}

func (w *Writer) recordLostLocked(writeErr error) {
	w.lost++
	if w.lostFile != nil {
		formatCounter21(&w.counterBuf, w.lost)
		_, _ = w.lostFile.WriteAt(w.counterBuf[:], 0)
	}
	nowNano := time.Now().UnixNano()
	if nowNano-w.lastErrLogNano >= int64(10*time.Second) {
		w.lastErrLogNano = nowNano
		fmt.Fprintf(os.Stderr, "postfix-api spool error (lost=%d): %v\n", w.lost, writeErr)
	}
}

// Close closes the active segment file and persistent counter descriptors.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true

	var firstErr error
	for _, f := range []*os.File{w.activeFile, w.gapsFile, w.lostFile} {
		if f != nil {
			if err := f.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	w.activeFile = nil
	w.gapsFile = nil
	w.lostFile = nil
	return firstErr
}

// Reader consumes spool segments in monotonic sequence order starting from a
// cursor (segmentName, offset).
type Reader struct {
	dir  string
	mu   sync.Mutex
	gaps uint64
}

// NewReader creates a Reader over spoolDir (<state_dir>/spool).
func NewReader(spoolDir string) *Reader {
	return &Reader{dir: spoolDir}
}

// Gaps returns the cumulative number of cursor gaps detected by this Reader
// when a requested segment had already been evicted.
func (r *Reader) Gaps() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.gaps
}

// ReadBatch reads up to maxRecords complete records starting at (segment, offset).
// If segment was evicted, Batch.GapDetected is true and reading resumes at the
// oldest available segment. Partial trailing lines without '\n' are discarded if
// a newer segment already exists on disk, or paused on if reading the newest segment.
func (r *Reader) ReadBatch(segment string, offset int64, maxRecords int) (Batch, error) {
	if maxRecords <= 0 {
		maxRecords = 1000
	}
	if offset < 0 {
		offset = 0
	}

	segs, err := listSegments(r.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if segment != "" {
				r.recordGap()
				return Batch{GapDetected: true}, nil
			}
			return Batch{}, nil
		}
		return Batch{}, fmt.Errorf("spool: list segments: %w", err)
	}

	if len(segs) == 0 {
		if segment != "" {
			r.recordGap()
			return Batch{GapDetected: true}, nil
		}
		return Batch{}, nil
	}

	startIdx := 0
	curOffset := offset
	gapDetected := false

	if segment == "" {
		curOffset = 0
	} else {
		targetSeq, ok := ParseSegmentName(segment)
		if !ok {
			return Batch{}, fmt.Errorf("spool: invalid cursor segment %q", segment)
		}
		foundIdx := -1
		jumpIdx := -1
		for i, s := range segs {
			if s.seq == targetSeq {
				foundIdx = i
				break
			}
			if s.seq > targetSeq && jumpIdx == -1 {
				jumpIdx = i
			}
		}
		if foundIdx >= 0 {
			startIdx = foundIdx
		} else {
			gapDetected = true
			r.recordGap()
			curOffset = 0
			if jumpIdx >= 0 {
				startIdx = jumpIdx
			} else {
				// All existing segments have seq < targetSeq (e.g. spool reset).
				startIdx = 0
			}
		}
	}

	batch := Batch{
		Records:     make([]Record, 0, minInt(maxRecords, 64)),
		NextSegment: segs[startIdx].name,
		NextOffset:  curOffset,
		GapDetected: gapDetected,
	}

	for i := startIdx; i < len(segs) && len(batch.Records) < maxRecords; i++ {
		seg := segs[i]
		path := filepath.Join(r.dir, seg.name)
		f, err := os.Open(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// Segment evicted concurrently right before open.
				batch.GapDetected = true
				r.recordGap()
				curOffset = 0
				continue
			}
			return Batch{}, fmt.Errorf("spool: open segment %s: %w", seg.name, err)
		}

		info, serr := f.Stat()
		if serr != nil {
			_ = f.Close()
			return Batch{}, fmt.Errorf("spool: stat segment %s: %w", seg.name, serr)
		}
		if curOffset > info.Size() {
			curOffset = info.Size()
		}
		if _, err := f.Seek(curOffset, io.SeekStart); err != nil {
			_ = f.Close()
			return Batch{}, fmt.Errorf("spool: seek segment %s: %w", seg.name, err)
		}

		batch.NextSegment = seg.name
		batch.NextOffset = curOffset

		br := bufio.NewReaderSize(f, 64*1024)
		lineStart := curOffset
		reachedEOF := false
		discardedTail := false

		for len(batch.Records) < maxRecords {
			lineBytes, rerr := br.ReadBytes('\n')
			if len(lineBytes) > 0 && lineBytes[len(lineBytes)-1] == '\n' {
				nextOff := lineStart + int64(len(lineBytes))
				lineStart = nextOff
				batch.NextSegment = seg.name
				batch.NextOffset = nextOff

				rec, valid := parseSpoolLine(lineBytes, seg.name, nextOff)
				if valid {
					batch.Records = append(batch.Records, rec)
				}
				if rerr == io.EOF {
					reachedEOF = true
					break
				}
				continue
			}

			if len(lineBytes) > 0 {
				// Partial trailing line without '\n'. Check on-disk segments to see
				// whether a newer segment exists (meaning this segment is closed).
				newer, nerr := r.nextSegmentAfter(seg.seq)
				if nerr == nil && newer != "" {
					// Crash-tail on closed segment: discard partial slice and move on.
					discardedTail = true
					batch.NextSegment = newer
					batch.NextOffset = 0
				}
				// Otherwise pause at lineStart (before the partial bytes).
				break
			}

			if rerr == io.EOF {
				reachedEOF = true
				break
			}
			if rerr != nil {
				_ = f.Close()
				return Batch{}, fmt.Errorf("spool: read segment %s: %w", seg.name, rerr)
			}
		}

		// If we stopped exactly on maxRecords at the end of a segment, check EOF so
		// fully consumed closed segments immediately advance NextSegment.
		if !reachedEOF && !discardedTail && len(batch.Records) >= maxRecords {
			if _, perr := br.Peek(1); perr == io.EOF {
				reachedEOF = true
			}
		}

		_ = f.Close()

		if reachedEOF {
			newer, nerr := r.nextSegmentAfter(seg.seq)
			if nerr == nil && newer != "" {
				batch.NextSegment = newer
				batch.NextOffset = 0
				curOffset = 0
				continue
			}
			break
		}

		if discardedTail {
			curOffset = 0
			continue
		}

		break
	}

	return batch, nil
}

func (r *Reader) recordGap() {
	r.mu.Lock()
	r.gaps++
	r.mu.Unlock()
}

func (r *Reader) nextSegmentAfter(seq uint64) (string, error) {
	segs, err := listSegments(r.dir)
	if err != nil {
		return "", err
	}
	for _, s := range segs {
		if s.seq > seq {
			return s.name, nil
		}
	}
	return "", nil
}

func parseSpoolLine(lineBytes []byte, segName string, nextOff int64) (Record, bool) {
	trimmed := strings.TrimRight(string(lineBytes), "\r\n")
	if strings.TrimSpace(trimmed) == "" {
		return Record{}, false
	}
	tsStr, raw, ok := strings.Cut(trimmed, " ")
	if !ok || tsStr == "" {
		return Record{}, false
	}
	ts, err := time.Parse(time.RFC3339Nano, tsStr)
	if err != nil {
		return Record{}, false
	}
	return Record{
		Time:    ts.UTC(),
		Raw:     raw,
		Segment: segName,
		Offset:  nextOff,
	}, true
}

// DeleteBefore removes all closed seg-*.log files strictly older than segment,
// never deleting segment itself or newer segments. Returns the number of
// segments removed by this call (ignoring os.ErrNotExist).
func (r *Reader) DeleteBefore(segment string) (int, error) {
	if segment == "" {
		return 0, nil
	}
	cutoffSeq, ok := ParseSegmentName(segment)
	if !ok {
		return 0, fmt.Errorf("spool: invalid segment name %q", segment)
	}
	segs, err := listSegments(r.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("spool: list segments for cleanup: %w", err)
	}
	deleted := 0
	for _, s := range segs {
		if s.seq >= cutoffSeq {
			break
		}
		if err := os.Remove(filepath.Join(r.dir, s.name)); err == nil {
			deleted++
		} else if !errors.Is(err, os.ErrNotExist) {
			return deleted, fmt.Errorf("spool: remove consumed segment %s: %w", s.name, err)
		}
	}
	return deleted, nil
}

// Stats reads gaps.count, lost.count, and current total bytes of seg-*.log files
// in spoolDir. Missing counter files report 0.
func Stats(spoolDir string) (SpoolStats, error) {
	var st SpoolStats
	segs, err := listSegments(spoolDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return st, nil
		}
		return st, fmt.Errorf("spool: stats read dir: %w", err)
	}
	st.SegmentCount = len(segs)
	if len(segs) > 0 {
		st.OldestSegment = segs[0].name
		st.NewestSegment = segs[len(segs)-1].name
		for _, s := range segs {
			st.TotalBytes += s.size
		}
	}

	if gapsBytes, err := os.ReadFile(filepath.Join(spoolDir, GapsCounterFile)); err == nil {
		st.Gaps = parseCounterBytes(gapsBytes)
	} else if !errors.Is(err, os.ErrNotExist) {
		return st, fmt.Errorf("spool: read %s: %w", GapsCounterFile, err)
	}

	if lostBytes, err := os.ReadFile(filepath.Join(spoolDir, LostCounterFile)); err == nil {
		st.Lost = parseCounterBytes(lostBytes)
	} else if !errors.Is(err, os.ErrNotExist) {
		return st, fmt.Errorf("spool: read %s: %w", LostCounterFile, err)
	}

	return st, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
