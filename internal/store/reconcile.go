package store

import (
	"context"
	"fmt"
	"time"
)

// RecordCursorGap atomically increments reconcile_state.cursor_gap_count and
// advances spool_cursor to newCursor inside a single BEGIN IMMEDIATE transaction.
func (s *Store) RecordCursorGap(ctx context.Context, newCursor SpoolCursor) error {
	conn, commit, cleanup, err := s.beginImmediate(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	nowStr := formatTime(s.now())

	if _, err := conn.ExecContext(ctx, `
		UPDATE reconcile_state
		SET cursor_gap_count = cursor_gap_count + 1
		WHERE id = 1
	`); err != nil {
		return fmt.Errorf("increment cursor_gap_count: %w", err)
	}

	var lastRecAt any
	if newCursor.LastRecordAt != nil && !newCursor.LastRecordAt.IsZero() {
		lastRecAt = formatTime(*newCursor.LastRecordAt)
	}
	if _, err := conn.ExecContext(ctx, `
		UPDATE spool_cursor
		SET segment = ?,
		    offset = ?,
		    last_record_at = COALESCE(?, last_record_at),
		    updated_at = ?
		WHERE id = 1
	`, newCursor.Segment, newCursor.Offset, lastRecAt, nowStr); err != nil {
		return fmt.Errorf("advance spool_cursor on gap: %w", err)
	}

	return commit()
}

// SyncSpoolGapCount ensures reconcile_state.cursor_gap_count is at least fileGapCount.
func (s *Store) SyncSpoolGapCount(ctx context.Context, fileGapCount uint64) error {
	conn, commit, cleanup, err := s.beginImmediate(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	if _, err := conn.ExecContext(ctx, `
		UPDATE reconcile_state
		SET cursor_gap_count = MAX(cursor_gap_count, ?)
		WHERE id = 1
	`, int64(fileGapCount)); err != nil {
		return fmt.Errorf("sync cursor_gap_count: %w", err)
	}
	return commit()
}

// RecordReconcileError records a failed postqueue -j execution without touching
// reconcile_misses on open messages.
func (s *Store) RecordReconcileError(ctx context.Context, errMsg string, now time.Time) error {
	conn, commit, cleanup, err := s.beginImmediate(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	if _, err := conn.ExecContext(ctx, `
		UPDATE reconcile_state
		SET last_sync_at = ?,
		    last_sync_ok = 0,
		    last_sync_error = ?
		WHERE id = 1
	`, formatTime(now), errMsg); err != nil {
		return fmt.Errorf("record reconcile error: %w", err)
	}
	return commit()
}

// ReconcileQueue reconciles SQLite state against a live `postqueue -j` snapshot.
// Missing queued messages are backfilled, present messages have reconcile_misses
// reset and queue_state/delay_reason updated, and open messages absent from entries
// increment reconcile_misses and close to 'unknown' after 2 consecutive misses.
func (s *Store) ReconcileQueue(ctx context.Context, entries []QueueSnapshotEntry, now time.Time) error {
	conn, commit, cleanup, err := s.beginImmediate(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	syncStart := now.UTC()
	syncStartStr := formatTime(syncStart)

	present := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.QueueID == "" {
			continue
		}
		present[entry.QueueID] = struct{}{}

		queueState := normalizeQueueName(entry.QueueName)
		delayReason := entry.DelayReason
		if delayReason == "" {
			for _, r := range entry.Recipients {
				if r.DelayReason != "" {
					delayReason = r.DelayReason
					break
				}
			}
		}

		receivedAt := entry.ArrivalTime
		if receivedAt.IsZero() {
			receivedAt = syncStart
		}
		receivedAtStr := formatTime(receivedAt)

		if _, err := conn.ExecContext(ctx, `
			INSERT INTO messages (queue_id, sender, size, nrcpt, received_at, queue_state, delay_reason, reconcile_misses, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
			ON CONFLICT(queue_id) DO UPDATE SET
				sender = CASE WHEN messages.sender = '' THEN excluded.sender ELSE messages.sender END,
				size = CASE WHEN messages.size = 0 THEN excluded.size ELSE messages.size END,
				nrcpt = CASE WHEN messages.nrcpt = 0 THEN excluded.nrcpt ELSE messages.nrcpt END,
				queue_state = excluded.queue_state,
				delay_reason = CASE WHEN excluded.delay_reason != '' THEN excluded.delay_reason ELSE messages.delay_reason END,
				reconcile_misses = 0,
				updated_at = excluded.updated_at
		`, entry.QueueID, entry.Sender, entry.MessageSize, len(entry.Recipients), receivedAtStr, queueState, delayReason, syncStartStr); err != nil {
			return fmt.Errorf("upsert reconciled message %q: %w", entry.QueueID, err)
		}

		recipStatus := "queued"
		if queueState == "deferred" {
			recipStatus = "deferred"
		}
		for _, r := range entry.Recipients {
			origTo := r.OrigAddress
			if origTo == r.Address {
				origTo = ""
			}
			if origTo == "" {
				if _, err := conn.ExecContext(ctx, `
					INSERT INTO recipients (queue_id, address, orig_to, status)
					SELECT ?, ?, '', ?
					WHERE NOT EXISTS (
						SELECT 1 FROM recipients WHERE queue_id = ? AND address = ?
					)
				`, entry.QueueID, r.Address, recipStatus, entry.QueueID, r.Address); err != nil {
					return fmt.Errorf("insert reconciled recipient %q for %q: %w", r.Address, entry.QueueID, err)
				}
			} else {
				if _, err := conn.ExecContext(ctx, `
					INSERT OR IGNORE INTO recipients (queue_id, address, orig_to, status)
					VALUES (?, ?, ?, ?)
				`, entry.QueueID, r.Address, origTo, recipStatus); err != nil {
					return fmt.Errorf("insert reconciled aliased recipient %q (%q) for %q: %w", r.Address, origTo, entry.QueueID, err)
				}
			}
		}
	}

	// 2-consecutive-miss closure for open messages absent from the live queue snapshot.
	type missCandidate struct {
		queueID string
		misses  int
	}
	rows, err := conn.QueryContext(ctx, `
		SELECT queue_id, reconcile_misses
		FROM messages
		WHERE removed_at IS NULL AND updated_at < ?
	`, syncStartStr)
	if err != nil {
		return fmt.Errorf("query miss candidates: %w", err)
	}
	var candidates []missCandidate
	for rows.Next() {
		var c missCandidate
		if err := rows.Scan(&c.queueID, &c.misses); err != nil {
			_ = rows.Close()
			return err
		}
		if _, ok := present[c.queueID]; !ok {
			candidates = append(candidates, c)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}

	for _, c := range candidates {
		newMisses := c.misses + 1
		if newMisses >= 2 {
			if _, err := conn.ExecContext(ctx, `
				UPDATE messages
				SET reconcile_misses = ?,
				    queue_state = 'unknown',
				    removed_at = ?,
				    updated_at = ?
				WHERE queue_id = ?
			`, newMisses, syncStartStr, syncStartStr, c.queueID); err != nil {
				return fmt.Errorf("close missed message %q: %w", c.queueID, err)
			}
			if _, err := conn.ExecContext(ctx, `
				UPDATE recipients
				SET status = 'unknown'
				WHERE queue_id = ? AND status IN ('queued', 'deferred')
			`, c.queueID); err != nil {
				return fmt.Errorf("close missed recipients %q: %w", c.queueID, err)
			}
		} else {
			if _, err := conn.ExecContext(ctx, `
				UPDATE messages
				SET reconcile_misses = ?
				WHERE queue_id = ?
			`, newMisses, c.queueID); err != nil {
				return fmt.Errorf("increment reconcile_misses %q: %w", c.queueID, err)
			}
		}
	}

	if _, err := conn.ExecContext(ctx, `
		UPDATE reconcile_state
		SET last_sync_at = ?,
		    last_sync_ok = 1,
		    last_sync_error = ''
		WHERE id = 1
	`, syncStartStr); err != nil {
		return fmt.Errorf("update reconcile_state: %w", err)
	}

	return commit()
}

// PurgeExpired deletes removed messages (and cascading recipients/events) whose
// removed_at timestamp is strictly older than cutoff.
func (s *Store) PurgeExpired(ctx context.Context, cutoff time.Time) (int64, error) {
	conn, commit, cleanup, err := s.beginImmediate(ctx)
	if err != nil {
		return 0, err
	}
	defer cleanup()

	res, err := conn.ExecContext(ctx, `
		DELETE FROM messages
		WHERE removed_at IS NOT NULL AND removed_at < ?
	`, formatTime(cutoff))
	if err != nil {
		return 0, fmt.Errorf("delete expired messages: %w", err)
	}
	deleted, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected on purge: %w", err)
	}

	if _, err := conn.ExecContext(ctx, `
		UPDATE reconcile_state
		SET last_retention_at = ?
		WHERE id = 1
	`, formatTime(s.now())); err != nil {
		return 0, fmt.Errorf("update last_retention_at: %w", err)
	}

	if err := commit(); err != nil {
		return 0, err
	}
	return deleted, nil
}

func normalizeQueueName(queueName string) string {
	switch queueName {
	case "incoming", "active", "deferred", "hold":
		return queueName
	case "maildrop":
		return "incoming"
	default:
		return "incoming"
	}
}
