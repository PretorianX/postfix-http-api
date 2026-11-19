package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/PretorianX/postfix-http-api/internal/parser"
)

// ApplyBatch applies parsed log records and updates spool_cursor atomically inside
// a single BEGIN IMMEDIATE SQLite transaction. If any step fails, the entire batch
// and cursor update are rolled back together.
func (s *Store) ApplyBatch(ctx context.Context, entries []BatchEntry, newCursor SpoolCursor) error {
	conn, commit, cleanup, err := s.beginImmediate(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	now := s.now().UTC()
	nowStr := formatTime(now)

	for i := range entries {
		if err := applyEntry(ctx, conn, &entries[i], now, nowStr); err != nil {
			return fmt.Errorf("apply entry %d (queue_id=%s kind=%s): %w", i, entries[i].Record.QueueID, entries[i].Record.Kind, err)
		}
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
		return fmt.Errorf("update spool_cursor: %w", err)
	}

	return commit()
}

func applyEntry(ctx context.Context, conn *sql.Conn, entry *BatchEntry, now time.Time, nowStr string) error {
	rec := &entry.Record
	ts := entry.Timestamp
	if ts.IsZero() {
		ts = rec.Timestamp
	}
	if ts.IsZero() {
		ts = now
	}
	tsStr := formatTime(ts)

	switch rec.Kind {
	case parser.KindSMTPDClient:
		_, err := conn.ExecContext(ctx, `
			INSERT INTO messages (queue_id, client_name, client_addr, sasl_username, received_at, queue_state, updated_at)
			VALUES (?, ?, ?, NULLIF(?, ''), ?, 'incoming', ?)
			ON CONFLICT(queue_id) DO UPDATE SET
				client_name = CASE WHEN excluded.client_name != '' THEN excluded.client_name ELSE messages.client_name END,
				client_addr = CASE WHEN excluded.client_addr != '' THEN excluded.client_addr ELSE messages.client_addr END,
				sasl_username = COALESCE(excluded.sasl_username, messages.sasl_username),
				updated_at = excluded.updated_at
		`, rec.QueueID, rec.ClientName, rec.ClientAddress, rec.SASLUsername, tsStr, nowStr)
		return err

	case parser.KindPickup:
		_, err := conn.ExecContext(ctx, `
			INSERT INTO messages (queue_id, sender, received_at, queue_state, updated_at)
			VALUES (?, ?, ?, 'incoming', ?)
			ON CONFLICT(queue_id) DO UPDATE SET
				sender = CASE WHEN excluded.sender != '' THEN excluded.sender ELSE messages.sender END,
				updated_at = excluded.updated_at
		`, rec.QueueID, rec.Sender, tsStr, nowStr)
		return err

	case parser.KindMessageID:
		_, err := conn.ExecContext(ctx, `
			INSERT INTO messages (queue_id, message_id, received_at, queue_state, updated_at)
			VALUES (?, ?, ?, 'incoming', ?)
			ON CONFLICT(queue_id) DO UPDATE SET
				message_id = CASE WHEN messages.message_id = '' THEN excluded.message_id ELSE messages.message_id END,
				updated_at = excluded.updated_at
		`, rec.QueueID, rec.MessageID, tsStr, nowStr)
		return err

	case parser.KindSubject:
		if _, err := conn.ExecContext(ctx, `
			INSERT OR IGNORE INTO messages (queue_id, received_at, queue_state, updated_at)
			VALUES (?, ?, 'incoming', ?)
		`, rec.QueueID, tsStr, nowStr); err != nil {
			return err
		}
		if rec.Subject == "" {
			return nil
		}
		_, err := conn.ExecContext(ctx, `
			UPDATE messages
			SET subject = ?, updated_at = ?
			WHERE queue_id = ? AND subject = ''
		`, rec.Subject, nowStr, rec.QueueID)
		return err

	case parser.KindQmgrActive:
		if _, err := conn.ExecContext(ctx, `
			INSERT INTO messages (queue_id, sender, size, nrcpt, received_at, queue_state, processed_logged, updated_at)
			VALUES (?, ?, ?, ?, ?, 'active', 0, ?)
			ON CONFLICT(queue_id) DO UPDATE SET
				sender = CASE WHEN excluded.sender != '' THEN excluded.sender ELSE messages.sender END,
				size = excluded.size,
				nrcpt = excluded.nrcpt,
				queue_state = CASE
					WHEN messages.queue_state NOT IN ('removed', 'requeued', 'hold') THEN 'active'
					ELSE messages.queue_state
				END,
				updated_at = excluded.updated_at
		`, rec.QueueID, rec.Sender, rec.Size, rec.RecipientCount, tsStr, nowStr); err != nil {
			return err
		}
		res, err := conn.ExecContext(ctx, `
			UPDATE messages
			SET processed_logged = 1
			WHERE queue_id = ? AND processed_logged = 0
		`, rec.QueueID)
		if err != nil {
			return err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if affected > 0 {
			if _, err := conn.ExecContext(ctx, `
				INSERT INTO events (time, queue_id, event)
				VALUES (?, ?, 'processed')
			`, tsStr, rec.QueueID); err != nil {
				return err
			}
		}
		return nil

	case parser.KindDelivery:
		if _, err := conn.ExecContext(ctx, `
			INSERT OR IGNORE INTO messages (queue_id, received_at, queue_state, updated_at)
			VALUES (?, ?, 'active', ?)
		`, rec.QueueID, tsStr, nowStr); err != nil {
			return err
		}

		origTo := rec.OrigTo
		if origTo == rec.Recipient {
			origTo = ""
		}

		// If postqueue -j (<3.11 without orig_address) inserted a placeholder row with
		// orig_to='' and 0 attempts before the delivery log arrived with orig_to, upgrade it.
		if origTo != "" {
			if _, err := conn.ExecContext(ctx, `
				UPDATE recipients
				SET orig_to = ?
				WHERE queue_id = ? AND address = ? AND orig_to = '' AND attempts = 0
				  AND NOT EXISTS (
				      SELECT 1 FROM recipients r2
				      WHERE r2.queue_id = ? AND r2.address = ? AND r2.orig_to = ?
				  )
			`, origTo, rec.QueueID, rec.Recipient, rec.QueueID, rec.Recipient, origTo); err != nil {
				return err
			}
		}

		var attempt int
		if err := conn.QueryRowContext(ctx, `
			INSERT INTO recipients (queue_id, address, orig_to, status, attempts, last_attempt_at, relay, dsn, response, delay)
			VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
			ON CONFLICT(queue_id, address, orig_to) DO UPDATE SET
				status = excluded.status,
				attempts = recipients.attempts + 1,
				last_attempt_at = excluded.last_attempt_at,
				relay = excluded.relay,
				dsn = excluded.dsn,
				response = excluded.response,
				delay = excluded.delay
			RETURNING attempts
		`, rec.QueueID, rec.Recipient, origTo, rec.Status, tsStr, rec.Relay, rec.DSN, rec.Response, rec.DelaySeconds).Scan(&attempt); err != nil {
			return err
		}

		if _, err := conn.ExecContext(ctx, `
			INSERT INTO events (time, queue_id, recipient, event, relay, dsn, response, delay, attempt)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, tsStr, rec.QueueID, rec.Recipient, rec.Status, rec.Relay, rec.DSN, rec.Response, rec.DelaySeconds, attempt); err != nil {
			return err
		}

		if rec.Status == "deferred" {
			if _, err := conn.ExecContext(ctx, `
				UPDATE messages
				SET queue_state = CASE
				        WHEN queue_state NOT IN ('removed', 'requeued') THEN 'deferred'
				        ELSE queue_state
				    END,
				    delay_reason = CASE
				        WHEN queue_state NOT IN ('removed', 'requeued') THEN ?
				        ELSE delay_reason
				    END,
				    updated_at = ?
				WHERE queue_id = ?
			`, rec.Response, nowStr, rec.QueueID); err != nil {
				return err
			}
		}
		return nil

	case parser.KindQmgrExpired:
		if _, err := conn.ExecContext(ctx, `
			INSERT OR IGNORE INTO messages (queue_id, received_at, queue_state, updated_at)
			VALUES (?, ?, 'active', ?)
		`, rec.QueueID, tsStr, nowStr); err != nil {
			return err
		}
		return closeOpenRecipients(ctx, conn, rec.QueueID, tsStr, "expired", "expired")

	case parser.KindQmgrRemoved:
		if _, err := conn.ExecContext(ctx, `
			INSERT OR IGNORE INTO messages (queue_id, received_at, queue_state, removed_at, updated_at)
			VALUES (?, ?, 'removed', ?, ?)
		`, rec.QueueID, tsStr, tsStr, nowStr); err != nil {
			return err
		}
		_, err := conn.ExecContext(ctx, `
			UPDATE messages
			SET removed_at = COALESCE(removed_at, ?),
			    queue_state = CASE WHEN queue_state = 'requeued' THEN 'requeued' ELSE 'removed' END,
			    updated_at = ?
			WHERE queue_id = ?
		`, tsStr, nowStr, rec.QueueID)
		return err

	case parser.KindBounce:
		_, err := conn.ExecContext(ctx, `
			INSERT INTO messages (queue_id, received_at, queue_state, orig_queue_id, updated_at)
			VALUES (?, ?, 'incoming', ?, ?)
			ON CONFLICT(queue_id) DO UPDATE SET
				orig_queue_id = COALESCE(messages.orig_queue_id, excluded.orig_queue_id),
				updated_at = excluded.updated_at
		`, rec.BounceQueueID, tsStr, rec.QueueID, nowStr)
		return err

	case parser.KindPostsuperHold:
		if _, err := conn.ExecContext(ctx, `
			INSERT OR IGNORE INTO messages (queue_id, received_at, queue_state, updated_at)
			VALUES (?, ?, 'hold', ?)
		`, rec.QueueID, tsStr, nowStr); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `
			INSERT INTO events (time, queue_id, event) VALUES (?, ?, 'held')
		`, tsStr, rec.QueueID); err != nil {
			return err
		}
		_, err := conn.ExecContext(ctx, `
			UPDATE messages
			SET queue_state = CASE
			        WHEN queue_state NOT IN ('removed', 'requeued') THEN 'hold'
			        ELSE queue_state
			    END,
			    updated_at = ?
			WHERE queue_id = ?
		`, nowStr, rec.QueueID)
		return err

	case parser.KindPostsuperRelease:
		if _, err := conn.ExecContext(ctx, `
			INSERT OR IGNORE INTO messages (queue_id, received_at, queue_state, updated_at)
			VALUES (?, ?, 'active', ?)
		`, rec.QueueID, tsStr, nowStr); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `
			INSERT INTO events (time, queue_id, event) VALUES (?, ?, 'released')
		`, tsStr, rec.QueueID); err != nil {
			return err
		}
		_, err := conn.ExecContext(ctx, `
			UPDATE messages
			SET queue_state = CASE WHEN queue_state = 'hold' THEN 'active' ELSE queue_state END,
			    updated_at = ?
			WHERE queue_id = ?
		`, nowStr, rec.QueueID)
		return err

	case parser.KindPostsuperRequeue:
		if _, err := conn.ExecContext(ctx, `
			INSERT OR IGNORE INTO messages (queue_id, received_at, queue_state, removed_at, updated_at)
			VALUES (?, ?, 'requeued', ?, ?)
		`, rec.QueueID, tsStr, tsStr, nowStr); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `
			INSERT INTO events (time, queue_id, event) VALUES (?, ?, 'requeued')
		`, tsStr, rec.QueueID); err != nil {
			return err
		}
		_, err := conn.ExecContext(ctx, `
			UPDATE messages
			SET queue_state = 'requeued',
			    removed_at = ?,
			    updated_at = ?
			WHERE queue_id = ?
		`, tsStr, nowStr, rec.QueueID)
		return err

	case parser.KindPostsuperExpire:
		if _, err := conn.ExecContext(ctx, `
			INSERT OR IGNORE INTO messages (queue_id, received_at, queue_state, updated_at)
			VALUES (?, ?, 'active', ?)
		`, rec.QueueID, tsStr, nowStr); err != nil {
			return err
		}
		_, err := conn.ExecContext(ctx, `
			INSERT INTO events (time, queue_id, event) VALUES (?, ?, 'expire_requested')
		`, tsStr, rec.QueueID)
		return err

	case parser.KindPostsuperRemove:
		if _, err := conn.ExecContext(ctx, `
			INSERT OR IGNORE INTO messages (queue_id, received_at, queue_state, removed_at, updated_at)
			VALUES (?, ?, 'removed', ?, ?)
		`, rec.QueueID, tsStr, tsStr, nowStr); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `
			UPDATE messages
			SET queue_state = 'removed',
			    removed_at = ?,
			    updated_at = ?
			WHERE queue_id = ?
		`, tsStr, nowStr, rec.QueueID); err != nil {
			return err
		}
		return closeOpenRecipients(ctx, conn, rec.QueueID, tsStr, "deleted", "deleted")

	default:
		return fmt.Errorf("unknown parsed record kind %q", rec.Kind)
	}
}

type openRecipientTarget struct {
	id      int64
	address string
}

func closeOpenRecipients(ctx context.Context, conn *sql.Conn, queueID, tsStr, newStatus, eventName string) error {
	rows, err := conn.QueryContext(ctx, `
		SELECT id, address
		FROM recipients
		WHERE queue_id = ? AND status IN ('queued', 'deferred')
		ORDER BY id ASC
	`, queueID)
	if err != nil {
		return err
	}
	var targets []openRecipientTarget
	for rows.Next() {
		var t openRecipientTarget
		if err := rows.Scan(&t.id, &t.address); err != nil {
			_ = rows.Close()
			return err
		}
		targets = append(targets, t)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}

	for _, t := range targets {
		if _, err := conn.ExecContext(ctx, `
			UPDATE recipients SET status = ? WHERE id = ?
		`, newStatus, t.id); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `
			INSERT INTO events (time, queue_id, recipient, event)
			VALUES (?, ?, ?, ?)
		`, tsStr, queueID, t.address, eventName); err != nil {
			return err
		}
	}
	return nil
}
