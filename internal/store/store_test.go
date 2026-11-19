package store

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/PretorianX/postfix-http-api/internal/parser"
)

func openTestStore(t *testing.T, clock *time.Time) *Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	nowFn := func() time.Time {
		if clock != nil {
			return *clock
		}
		return time.Now().UTC()
	}
	st, err := OpenWithClock(dbPath, nowFn)
	if err != nil {
		t.Fatalf("OpenWithClock failed: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("Close failed: %v", err)
		}
	})
	return st
}

func TestOpen_PragmasAndConcurrentReadersWriter(t *testing.T) {
	st := openTestStore(t, nil)
	ctx := context.Background()

	var journalMode string
	if err := st.writerDB.QueryRowContext(ctx, "PRAGMA journal_mode;").Scan(&journalMode); err != nil {
		t.Fatal(err)
	}
	if journalMode != "wal" {
		t.Fatalf("journal_mode = %q, want wal", journalMode)
	}

	var fk, busy, syncMode int
	if err := st.writerDB.QueryRowContext(ctx, "PRAGMA foreign_keys;").Scan(&fk); err != nil {
		t.Fatal(err)
	}
	if err := st.writerDB.QueryRowContext(ctx, "PRAGMA busy_timeout;").Scan(&busy); err != nil {
		t.Fatal(err)
	}
	if err := st.writerDB.QueryRowContext(ctx, "PRAGMA synchronous;").Scan(&syncMode); err != nil {
		t.Fatal(err)
	}
	if fk != 1 || busy != 5000 || syncMode != 1 {
		t.Fatalf("unexpected pragmas: fk=%d busy=%d sync=%d", fk, busy, syncMode)
	}

	// Reader handle must reject writes (query_only=ON).
	if _, err := st.readerDB.ExecContext(ctx, "DELETE FROM spool_cursor;"); err == nil {
		t.Fatal("expected readerDB to reject write query under query_only=ON")
	}

	// Concurrent writer + readers under -race.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		base := time.Date(2026, 11, 19, 8, 0, 0, 0, time.UTC)
		for i := 0; i < 30; i++ {
			ts := base.Add(time.Duration(i) * time.Second)
			qid := "4jV74M63RlzHx" + string(rune('A'+i%26)) + string(rune('0'+i/26))
			err := st.ApplyBatch(ctx, []BatchEntry{
				{
					Timestamp: ts,
					Record: parser.ParsedRecord{
						Timestamp:     ts,
						Program:       "smtpd",
						QueueID:       qid,
						Kind:          parser.KindSMTPDClient,
						ClientName:    "localhost",
						ClientAddress: "127.0.0.1",
					},
				},
			}, SpoolCursor{
				Segment:      "seg-00000000000000000001.log",
				Offset:       int64((i + 1) * 100),
				LastRecordAt: &ts,
			})
			if err != nil {
				t.Errorf("concurrent ApplyBatch %d failed: %v", i, err)
				return
			}
		}
	}()

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				if _, err := st.ListMessages(ctx, MessageFilter{Limit: 10}); err != nil {
					t.Errorf("concurrent ListMessages failed: %v", err)
					return
				}
				if _, err := st.GetSpoolCursor(ctx); err != nil {
					t.Errorf("concurrent GetSpoolCursor failed: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestApplyBatch_LifecycleFirstSubjectAndIdempotency(t *testing.T) {
	now := time.Date(2026, 11, 18, 17, 0, 0, 0, time.UTC)
	st := openTestStore(t, &now)
	ctx := context.Background()

	t0 := time.Date(2026, 11, 18, 16, 54, 43, 100000000, time.UTC)
	lines := []struct {
		offset time.Duration
		line   string
	}{
		{0, "postfix/submission/smtpd[10]: 4jV74M63RlzHxK3: client=localhost[127.0.0.1], sasl_method=PLAIN, sasl_username=alice@example.com"},
		{10 * time.Millisecond, "postfix/cleanup[11]: 4jV74M63RlzHxK3: message-id=<4jV74M63RlzHxK3@localhost>"},
		{20 * time.Millisecond, "postfix/cleanup[11]: 4jV74M63RlzHxK3: info: header Subject: =?UTF-8?B?0J/RgNC40LLQtdGC?= order 42 from localhost[127.0.0.1]; from=<a@example.com> to=<b@example.org> proto=ESMTP"},
		// Second Subject from attached .eml must NOT overwrite the first Subject.
		{25 * time.Millisecond, "postfix/cleanup[11]: 4jV74M63RlzHxK3: info: header Subject: Inner Attached Email Subject from localhost[127.0.0.1]; from=<a@example.com> to=<b@example.org> proto=ESMTP"},
		{30 * time.Millisecond, "postfix/qmgr[12]: 4jV74M63RlzHxK3: from=<a@example.com>, size=310, nrcpt=1 (queue active)"},
		{2 * time.Second, "postfix/smtp[13]: 4jV74M63RlzHxK3: to=<b@example.org>, relay=none, delay=2.08, delays=0.01/0.02/2.05/0, dsn=4.4.1, status=deferred (connect to 192.0.2.1[192.0.2.1]:25: Connection timed out)"},
		// Second qmgr active (retry) must NOT create a duplicate 'processed' event.
		{5 * time.Minute, "postfix/qmgr[12]: 4jV74M63RlzHxK3: from=<a@example.com>, size=310, nrcpt=1 (queue active)"},
		{5*time.Minute + time.Second, "postfix/smtp[14]: 4jV74M63RlzHxK3: to=<b@example.org>, relay=mx.example.org[192.0.2.2]:25, delay=301.1, delays=300/0.01/0.5/0.59, dsn=2.0.0, status=sent (250 2.0.0 Ok)"},
		{5*time.Minute + 2*time.Second, "postfix/qmgr[12]: 4jV74M63RlzHxK3: removed"},
	}

	var batch []BatchEntry
	var lastTS time.Time
	for _, item := range lines {
		rec, ok := parser.ParseLine(item.line)
		if !ok {
			t.Fatalf("failed to parse %q", item.line)
		}
		ts := t0.Add(item.offset)
		lastTS = ts
		batch = append(batch, BatchEntry{Record: *rec, Timestamp: ts})
	}

	cur := SpoolCursor{Segment: "seg-00000000000000000001.log", Offset: 1234, LastRecordAt: &lastTS}
	if err := st.ApplyBatch(ctx, batch, cur); err != nil {
		t.Fatalf("ApplyBatch failed: %v", err)
	}

	msg, err := st.GetMessage(ctx, "4jV74M63RlzHxK3")
	if err != nil {
		t.Fatalf("GetMessage failed: %v", err)
	}
	if msg.Subject != "Привет order 42" {
		t.Errorf("Subject = %q, want %q", msg.Subject, "Привет order 42")
	}
	if msg.MessageID != "<4jV74M63RlzHxK3@localhost>" {
		t.Errorf("MessageID = %q", msg.MessageID)
	}
	if msg.Sender != "a@example.com" || msg.Size != 310 || msg.RecipientCount != 1 {
		t.Errorf("unexpected envelope metadata: %+v", msg)
	}
	if msg.SASLUsername == nil || *msg.SASLUsername != "alice@example.com" {
		t.Errorf("SASLUsername = %v", msg.SASLUsername)
	}
	if msg.QueueState != "removed" || msg.RemovedAt == nil || !msg.RemovedAt.Equal(lastTS) {
		t.Errorf("QueueState=%q RemovedAt=%v, want removed at %v", msg.QueueState, msg.RemovedAt, lastTS)
	}
	if len(msg.Recipients) != 1 {
		t.Fatalf("len(Recipients) = %d, want 1", len(msg.Recipients))
	}
	r := msg.Recipients[0]
	if r.Status != "delivered" || r.Attempts != 2 || r.DSN != "2.0.0" || r.Relay != "mx.example.org[192.0.2.2]:25" {
		t.Errorf("unexpected recipient: %+v", r)
	}

	// Expect exactly 3 events: processed, deferred (attempt 1), delivered (attempt 2).
	if len(msg.Events) != 3 {
		t.Fatalf("len(Events) = %d, want 3: %+v", len(msg.Events), msg.Events)
	}
	if msg.Events[0].Event != "processed" || msg.Events[1].Event != "deferred" || msg.Events[2].Event != "delivered" {
		t.Errorf("unexpected event sequence: %+v", msg.Events)
	}
	if *msg.Events[1].Attempt != 1 || *msg.Events[2].Attempt != 2 {
		t.Errorf("unexpected attempts in events: %+v", msg.Events)
	}

	// Cursor committed atomically.
	gotCur, err := st.GetSpoolCursor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if gotCur.Segment != cur.Segment || gotCur.Offset != cur.Offset || gotCur.LastRecordAt == nil || !gotCur.LastRecordAt.Equal(lastTS) {
		t.Errorf("unexpected cursor: %+v", gotCur)
	}
}

func TestApplyBatch_RollbackLeavesCursorUnchanged(t *testing.T) {
	st := openTestStore(t, nil)
	ctx := context.Background()

	ts := time.Date(2026, 11, 18, 17, 0, 0, 0, time.UTC)
	err := st.ApplyBatch(ctx, []BatchEntry{
		{
			Timestamp: ts,
			Record: parser.ParsedRecord{
				QueueID:       "4jV74M63RlzHxK3",
				Kind:          parser.KindSMTPDClient,
				ClientName:    "localhost",
				ClientAddress: "127.0.0.1",
			},
		},
		{
			Timestamp: ts,
			Record: parser.ParsedRecord{
				QueueID: "4jV74M63RlzHxK3",
				Kind:    parser.RecordKind("bogus_kind_triggers_error"),
			},
		},
	}, SpoolCursor{Segment: "seg-00000000000000000009.log", Offset: 999, LastRecordAt: &ts})
	if err == nil {
		t.Fatal("expected ApplyBatch error on bogus kind")
	}

	if _, err := st.GetMessage(ctx, "4jV74M63RlzHxK3"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after rollback, got %v", err)
	}
	cur, err := st.GetSpoolCursor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cur.Segment != "" || cur.Offset != 0 {
		t.Fatalf("expected cursor unchanged after rollback, got %+v", cur)
	}
}

func TestApplyBatch_AliasesBouncePostsuperAndQmgrExpire(t *testing.T) {
	now := time.Date(2026, 11, 18, 18, 0, 0, 0, time.UTC)
	st := openTestStore(t, &now)
	ctx := context.Background()

	t0 := time.Date(2026, 11, 18, 17, 30, 0, 0, time.UTC)
	entries := []BatchEntry{
		// Message 1: two aliases expanding to same final address, plus one already delivered and one deferred.
		{Timestamp: t0, Record: parser.ParsedRecord{QueueID: "QID0001", Kind: parser.KindPickup, Sender: "s@example.com", HasSender: true}},
		{Timestamp: t0.Add(1 * time.Second), Record: parser.ParsedRecord{QueueID: "QID0001", Kind: parser.KindDelivery, Recipient: "dest@example.org", OrigTo: "alias1@example.com", Relay: "local", DSN: "2.0.0", Status: "delivered", Response: "ok"}},
		{Timestamp: t0.Add(2 * time.Second), Record: parser.ParsedRecord{QueueID: "QID0001", Kind: parser.KindDelivery, Recipient: "dest@example.org", OrigTo: "alias2@example.com", Relay: "none", DSN: "4.4.1", Status: "deferred", Response: "temp fail"}},
		{Timestamp: t0.Add(3 * time.Second), Record: parser.ParsedRecord{QueueID: "QID0001", Kind: parser.KindPostsuperHold}},
		{Timestamp: t0.Add(4 * time.Second), Record: parser.ParsedRecord{QueueID: "QID0001", Kind: parser.KindPostsuperRelease}},
		{Timestamp: t0.Add(5 * time.Second), Record: parser.ParsedRecord{QueueID: "QID0001", Kind: parser.KindPostsuperExpire}},
		// force-expired only closes open (deferred/queued) recipients, NOT already delivered ones.
		{Timestamp: t0.Add(6 * time.Second), Record: parser.ParsedRecord{QueueID: "QID0001", Kind: parser.KindQmgrExpired}},
		{Timestamp: t0.Add(7 * time.Second), Record: parser.ParsedRecord{QueueID: "QID0001", Kind: parser.KindBounce, BounceQueueID: "BOUNCE1"}},

		// Message 2: postsuper requeue followed by qmgr removed must preserve 'requeued'.
		{Timestamp: t0, Record: parser.ParsedRecord{QueueID: "QID0002", Kind: parser.KindQmgrActive, Sender: "s2@example.com", HasSender: true, Size: 100, RecipientCount: 1}},
		{Timestamp: t0.Add(1 * time.Second), Record: parser.ParsedRecord{QueueID: "QID0002", Kind: parser.KindPostsuperRequeue}},
		{Timestamp: t0.Add(2 * time.Second), Record: parser.ParsedRecord{QueueID: "QID0002", Kind: parser.KindQmgrRemoved}},

		// Message 3: postsuper removed closes open recipients to 'deleted'.
		{Timestamp: t0, Record: parser.ParsedRecord{QueueID: "QID0003", Kind: parser.KindDelivery, Recipient: "u@example.org", Relay: "none", DSN: "4.4.1", Status: "deferred", Response: "timeout"}},
		{Timestamp: t0.Add(1 * time.Second), Record: parser.ParsedRecord{QueueID: "QID0003", Kind: parser.KindPostsuperRemove}},
	}

	if err := st.ApplyBatch(ctx, entries, SpoolCursor{Segment: "seg-00000000000000000001.log", Offset: 500, LastRecordAt: &t0}); err != nil {
		t.Fatalf("ApplyBatch failed: %v", err)
	}

	m1, err := st.GetMessage(ctx, "QID0001")
	if err != nil {
		t.Fatal(err)
	}
	if len(m1.Recipients) != 2 {
		t.Fatalf("expected 2 distinct orig_to recipients, got %d", len(m1.Recipients))
	}
	if m1.Recipients[0].Status != "delivered" || m1.Recipients[1].Status != "expired" {
		t.Errorf("unexpected recipient statuses: %+v", m1.Recipients)
	}

	bounceMsg, err := st.GetMessage(ctx, "BOUNCE1")
	if err != nil {
		t.Fatal(err)
	}
	if bounceMsg.OrigQueueID == nil || *bounceMsg.OrigQueueID != "QID0001" {
		t.Errorf("bounce OrigQueueID = %v, want QID0001", bounceMsg.OrigQueueID)
	}

	m2, err := st.GetMessage(ctx, "QID0002")
	if err != nil {
		t.Fatal(err)
	}
	if m2.QueueState != "requeued" {
		t.Errorf("m2 QueueState = %q, want requeued", m2.QueueState)
	}

	m3, err := st.GetMessage(ctx, "QID0003")
	if err != nil {
		t.Fatal(err)
	}
	if m3.QueueState != "removed" || m3.Recipients[0].Status != "deleted" {
		t.Errorf("m3 state=%q recip=%+v", m3.QueueState, m3.Recipients)
	}
	if len(m3.Events) != 2 || m3.Events[1].Event != "deleted" {
		t.Errorf("m3 events = %+v", m3.Events)
	}
}

func TestListMessages_FiltersPaginationAndEventFeed(t *testing.T) {
	now := time.Date(2026, 11, 18, 19, 0, 0, 0, time.UTC)
	st := openTestStore(t, &now)
	ctx := context.Background()

	// Insert 5 messages; 2 share the exact same timestamp to test keyset tie-breaking.
	tTie := time.Date(2026, 11, 18, 18, 0, 0, 0, time.UTC)
	tOlder := tTie.Add(-1 * time.Minute)
	tNewer := tTie.Add(1 * time.Minute)

	batch := []BatchEntry{
		{Timestamp: tOlder, Record: parser.ParsedRecord{QueueID: "QID_A", Kind: parser.KindQmgrActive, Sender: "alice@example.com", HasSender: true}},
		{Timestamp: tOlder, Record: parser.ParsedRecord{QueueID: "QID_A", Kind: parser.KindMessageID, MessageID: "<id-a@ex>"}},
		{Timestamp: tOlder, Record: parser.ParsedRecord{QueueID: "QID_A", Kind: parser.KindDelivery, Recipient: "r1@example.org", Status: "delivered", Relay: "mx", DSN: "2.0.0"}},
		{Timestamp: tOlder, Record: parser.ParsedRecord{QueueID: "QID_A", Kind: parser.KindDelivery, Recipient: "r2@example.org", Status: "deferred", Relay: "none", DSN: "4.4.1"}},

		{Timestamp: tTie, Record: parser.ParsedRecord{QueueID: "QID_B1", Kind: parser.KindQmgrActive, Sender: "bob@example.com", HasSender: true}},
		{Timestamp: tTie, Record: parser.ParsedRecord{QueueID: "QID_B2", Kind: parser.KindQmgrActive, Sender: "bob@example.com", HasSender: true}},
		{Timestamp: tTie, Record: parser.ParsedRecord{QueueID: "QID_B3", Kind: parser.KindQmgrActive, Sender: "bob@example.com", HasSender: true}},
		{Timestamp: tNewer, Record: parser.ParsedRecord{QueueID: "QID_C", Kind: parser.KindQmgrActive, Sender: "carol@example.com", HasSender: true}},
	}
	if err := st.ApplyBatch(ctx, batch, SpoolCursor{Segment: "seg-00000000000000000001.log", Offset: 10}); err != nil {
		t.Fatal(err)
	}

	// Combined recipient + status must NOT cross-match different recipients on QID_A.
	p, err := st.ListMessages(ctx, MessageFilter{Recipient: "r1@example.org", Status: "deferred"})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Messages) != 0 {
		t.Fatalf("expected 0 messages matching r1=deferred, got %d", len(p.Messages))
	}

	p, err = st.ListMessages(ctx, MessageFilter{Recipient: "r1@example.org", Status: "delivered"})
	if err != nil || len(p.Messages) != 1 || p.Messages[0].QueueID != "QID_A" {
		t.Fatalf("expected QID_A for r1=delivered, got err=%v msgs=%+v", err, p.Messages)
	}

	// MessageID and Sender and time window filters.
	p, err = st.ListMessages(ctx, MessageFilter{MessageID: "<id-a@ex>", Sender: "alice@example.com"})
	if err != nil || len(p.Messages) != 1 || p.Messages[0].QueueID != "QID_A" {
		t.Fatalf("message_id filter failed: err=%v msgs=%+v", err, p.Messages)
	}

	p, err = st.ListMessages(ctx, MessageFilter{From: &tTie, To: &tTie})
	if err != nil || len(p.Messages) != 3 {
		t.Fatalf("time window filter failed: err=%v count=%d", err, len(p.Messages))
	}

	// Keyset pagination across ties: page size 2 across 5 total rows.
	var seen []string
	cursor := ""
	for {
		page, err := st.ListMessages(ctx, MessageFilter{Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range page.Messages {
			seen = append(seen, m.QueueID)
		}
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}
	wantOrder := []string{"QID_C", "QID_B3", "QID_B2", "QID_B1", "QID_A"}
	if len(seen) != len(wantOrder) {
		t.Fatalf("seen=%v, want %v", seen, wantOrder)
	}
	for i := range wantOrder {
		if seen[i] != wantOrder[i] {
			t.Errorf("page order[%d] = %q, want %q (all=%v)", i, seen[i], wantOrder[i], seen)
		}
	}

	// Invalid cursor rejected.
	if _, err := st.ListMessages(ctx, MessageFilter{Cursor: "not-valid-base64!!!"}); err == nil {
		t.Fatal("expected error for malformed cursor")
	}

	// Event feed pagination.
	evs1, lastID, hasMore, err := st.ListEvents(ctx, 0, 3)
	if err != nil || len(evs1) != 3 || !hasMore || lastID != evs1[2].ID {
		t.Fatalf("ListEvents page 1 failed: len=%d lastID=%d hasMore=%v err=%v", len(evs1), lastID, hasMore, err)
	}
	evs2, lastID2, hasMore2, err := st.ListEvents(ctx, lastID, 10)
	if err != nil || hasMore2 || len(evs2) == 0 || lastID2 != evs2[len(evs2)-1].ID {
		t.Fatalf("ListEvents page 2 failed: len=%d lastID2=%d hasMore2=%v err=%v", len(evs2), lastID2, hasMore2, err)
	}
}

func TestReconcileQueue_BackfillTwoMissesErrorAndPurge(t *testing.T) {
	clock := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	st := openTestStore(t, &clock)
	ctx := context.Background()

	// Seed an open message and a removed message.
	tOld := clock.Add(-10 * 24 * time.Hour)
	if err := st.ApplyBatch(ctx, []BatchEntry{
		{Timestamp: clock.Add(-2 * time.Minute), Record: parser.ParsedRecord{QueueID: "OPEN001", Kind: parser.KindQmgrActive, Sender: "a@ex.com", HasSender: true}},
		{Timestamp: clock.Add(-1 * time.Minute), Record: parser.ParsedRecord{QueueID: "OPEN001", Kind: parser.KindDelivery, Recipient: "r@ex.org", Status: "deferred", Relay: "none", DSN: "4.4.1", Response: "timeout"}},
		{Timestamp: tOld, Record: parser.ParsedRecord{QueueID: "OLD0001", Kind: parser.KindQmgrActive, Sender: "old@ex.com", HasSender: true}},
		{Timestamp: tOld.Add(time.Minute), Record: parser.ParsedRecord{QueueID: "OLD0001", Kind: parser.KindQmgrRemoved}},
	}, SpoolCursor{Segment: "seg-00000000000000000001.log", Offset: 200}); err != nil {
		t.Fatal(err)
	}

	// Reconcile 1: OPEN001 is missing (1st miss), NEW0001 (maildrop) and DEF0001 (deferred, Postfix >=3.11 orig_address==address) backfilled.
	sync1 := clock.Add(1 * time.Minute)
	err := st.ReconcileQueue(ctx, []QueueSnapshotEntry{
		{
			QueueName:   "maildrop",
			QueueID:     "NEW0001",
			ArrivalTime: sync1.Add(-10 * time.Second),
			MessageSize: 256,
			Sender:      "local@ex.com",
			Recipients:  []QueueRecipient{{Address: "dest@ex.org"}},
		},
		{
			QueueName:   "deferred",
			QueueID:     "DEF0001",
			ArrivalTime: sync1.Add(-20 * time.Second),
			MessageSize: 512,
			Sender:      "a@ex.com",
			Recipients: []QueueRecipient{
				{Address: "b@ex.org", OrigAddress: "b@ex.org", DelayReason: "connection refused"},
			},
		},
	}, sync1)
	if err != nil {
		t.Fatal(err)
	}

	mOpen, err := st.GetMessage(ctx, "OPEN001")
	if err != nil {
		t.Fatal(err)
	}
	if mOpen.ReconcileMisses != 1 || mOpen.QueueState != "deferred" || mOpen.RemovedAt != nil {
		t.Fatalf("after 1 miss OPEN001 should stay open: %+v", mOpen)
	}

	mNew, err := st.GetMessage(ctx, "NEW0001")
	if err != nil {
		t.Fatal(err)
	}
	if mNew.QueueState != "incoming" || len(mNew.Recipients) != 1 || mNew.Recipients[0].Status != "queued" {
		t.Fatalf("backfilled maildrop message mismatch: %+v", mNew)
	}

	mDef, err := st.GetMessage(ctx, "DEF0001")
	if err != nil {
		t.Fatal(err)
	}
	if mDef.QueueState != "deferred" || mDef.DelayReason != "connection refused" || mDef.Recipients[0].OrigTo != "" {
		t.Fatalf("backfilled deferred message mismatch: %+v", mDef)
	}

	// Simulate subsequent SMTP delivery on DEF0001 (no orig_to): must reuse existing recipient row cleanly!
	clock = sync1.Add(10 * time.Second)
	if err := st.ApplyBatch(ctx, []BatchEntry{
		{Timestamp: clock, Record: parser.ParsedRecord{QueueID: "DEF0001", Kind: parser.KindDelivery, Recipient: "b@ex.org", Relay: "mx", DSN: "2.0.0", Status: "delivered", Response: "250 Ok"}},
	}, SpoolCursor{Segment: "seg-00000000000000000001.log", Offset: 300}); err != nil {
		t.Fatal(err)
	}
	mDef, _ = st.GetMessage(ctx, "DEF0001")
	if len(mDef.Recipients) != 1 || mDef.Recipients[0].Status != "delivered" || mDef.Recipients[0].Attempts != 1 {
		t.Fatalf("expected DEF0001 recipient merged cleanly, got %+v", mDef.Recipients)
	}

	// Failed postqueue -j must record error and NOT increment miss counts!
	syncErrAt := clock.Add(1 * time.Minute)
	if err := st.RecordReconcileError(ctx, "postqueue exited 1", syncErrAt); err != nil {
		t.Fatal(err)
	}
	mOpen, _ = st.GetMessage(ctx, "OPEN001")
	if mOpen.ReconcileMisses != 1 {
		t.Fatalf("expected ReconcileMisses still 1 after error, got %d", mOpen.ReconcileMisses)
	}
	rs, err := st.GetReconcileState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rs.LastSyncOK || rs.LastSyncError != "postqueue exited 1" {
		t.Fatalf("unexpected reconcile state: %+v", rs)
	}

	// Reconcile 2: OPEN001 still absent -> 2nd miss -> closes to unknown! NEW0001 absent -> 1st miss.
	sync2 := syncErrAt.Add(1 * time.Minute)
	if err := st.ReconcileQueue(ctx, nil, sync2); err != nil {
		t.Fatal(err)
	}
	mOpen, _ = st.GetMessage(ctx, "OPEN001")
	if mOpen.QueueState != "unknown" || mOpen.RemovedAt == nil || !mOpen.RemovedAt.Equal(sync2) {
		t.Fatalf("expected OPEN001 closed to unknown on 2nd miss: %+v", mOpen)
	}
	if mOpen.Recipients[0].Status != "unknown" {
		t.Fatalf("expected open recipient status=unknown, got %+v", mOpen.Recipients[0])
	}
	// DEF0001 had delivered recipient; its 1st miss keeps delivered status intact.
	mNew, _ = st.GetMessage(ctx, "NEW0001")
	if mNew.ReconcileMisses != 1 || mNew.RemovedAt != nil {
		t.Fatalf("expected NEW0001 at 1 miss, got %+v", mNew)
	}

	// Retention purge: 7 days before clock deletes OLD0001 (10 days old), keeps OPEN001 (just closed).
	cutoff := clock.Add(-7 * 24 * time.Hour)
	purged, err := st.PurgeExpired(ctx, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if purged != 1 {
		t.Fatalf("purged = %d, want 1", purged)
	}
	if _, err := st.GetMessage(ctx, "OLD0001"); err != ErrNotFound {
		t.Fatalf("expected OLD0001 deleted, got err=%v", err)
	}
	if _, err := st.GetMessage(ctx, "OPEN001"); err != nil {
		t.Fatalf("expected OPEN001 retained, got err=%v", err)
	}

	// Verify cascade deletion cleared OLD0001's events too.
	evs, _, _, err := st.ListEvents(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range evs {
		if ev.QueueID == "OLD0001" {
			t.Fatalf("expected OLD0001 events cascaded away, found %+v", ev)
		}
	}
}
