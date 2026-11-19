package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// GetSpoolCursor returns the persisted spool consumer position.
func (s *Store) GetSpoolCursor(ctx context.Context) (SpoolCursor, error) {
	var cur SpoolCursor
	var lastRecAt sql.NullString
	var updatedAtStr string

	err := s.readerDB.QueryRowContext(ctx, `
		SELECT segment, offset, last_record_at, updated_at
		FROM spool_cursor
		WHERE id = 1
	`).Scan(&cur.Segment, &cur.Offset, &lastRecAt, &updatedAtStr)
	if err != nil {
		return SpoolCursor{}, fmt.Errorf("query spool_cursor: %w", err)
	}

	if lastRecAt.Valid && lastRecAt.String != "" {
		t, err := parseTimeRequired(lastRecAt.String)
		if err != nil {
			return SpoolCursor{}, fmt.Errorf("parse last_record_at %q: %w", lastRecAt.String, err)
		}
		cur.LastRecordAt = &t
	}
	t, err := parseTimeRequired(updatedAtStr)
	if err != nil {
		return SpoolCursor{}, fmt.Errorf("parse updated_at %q: %w", updatedAtStr, err)
	}
	cur.UpdatedAt = t
	return cur, nil
}

// GetReconcileState returns the persisted queue reconciliation and retention telemetry.
func (s *Store) GetReconcileState(ctx context.Context) (ReconcileState, error) {
	var rs ReconcileState
	var lastSyncAt, lastRetentionAt sql.NullString
	var syncOK int

	err := s.readerDB.QueryRowContext(ctx, `
		SELECT last_sync_at, last_sync_ok, last_sync_error, cursor_gap_count, last_retention_at
		FROM reconcile_state
		WHERE id = 1
	`).Scan(&lastSyncAt, &syncOK, &rs.LastSyncError, &rs.CursorGapCount, &lastRetentionAt)
	if err != nil {
		return ReconcileState{}, fmt.Errorf("query reconcile_state: %w", err)
	}

	rs.LastSyncOK = syncOK != 0
	if lastSyncAt.Valid && lastSyncAt.String != "" {
		t, err := parseTimeRequired(lastSyncAt.String)
		if err != nil {
			return ReconcileState{}, fmt.Errorf("parse last_sync_at %q: %w", lastSyncAt.String, err)
		}
		rs.LastSyncAt = &t
	}
	if lastRetentionAt.Valid && lastRetentionAt.String != "" {
		t, err := parseTimeRequired(lastRetentionAt.String)
		if err != nil {
			return ReconcileState{}, fmt.Errorf("parse last_retention_at %q: %w", lastRetentionAt.String, err)
		}
		rs.LastRetentionAt = &t
	}
	return rs, nil
}

// ListMessages queries messages matching filter ordered by (received_at DESC, queue_id DESC)
// using keyset pagination, and attaches each message's recipients.
func (s *Store) ListMessages(ctx context.Context, filter MessageFilter) (MessagePage, error) {
	var cursor *PageCursor
	if filter.Cursor != "" {
		var err error
		cursor, err = DecodePageCursor(filter.Cursor)
		if err != nil {
			return MessagePage{}, err
		}
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	} else if limit > 1000 {
		limit = 1000
	}

	var where []string
	var args []any

	if filter.QueueID != "" {
		where = append(where, "m.queue_id = ?")
		args = append(args, filter.QueueID)
	}
	if filter.MessageID != "" {
		where = append(where, "m.message_id = ?")
		args = append(args, filter.MessageID)
	}
	if filter.Sender != "" {
		where = append(where, "m.sender = ?")
		args = append(args, filter.Sender)
	}
	if filter.From != nil {
		where = append(where, "m.received_at >= ?")
		args = append(args, formatTime(*filter.From))
	}
	if filter.To != nil {
		where = append(where, "m.received_at <= ?")
		args = append(args, formatTime(*filter.To))
	}
	if filter.Recipient != "" || filter.Status != "" {
		var sub []string
		sub = append(sub, "r.queue_id = m.queue_id")
		if filter.Recipient != "" {
			sub = append(sub, "r.address = ?")
			args = append(args, filter.Recipient)
		}
		if filter.Status != "" {
			sub = append(sub, "r.status = ?")
			args = append(args, filter.Status)
		}
		where = append(where, "EXISTS (SELECT 1 FROM recipients r WHERE "+strings.Join(sub, " AND ")+")")
	}
	if cursor != nil {
		where = append(where, "(m.received_at < ? OR (m.received_at = ? AND m.queue_id < ?))")
		args = append(args, cursor.ReceivedAt, cursor.ReceivedAt, cursor.QueueID)
	}

	query := `
		SELECT m.queue_id, m.message_id, m.subject, m.sender, m.size, m.nrcpt,
		       m.client_name, m.client_addr, m.sasl_username, m.received_at,
		       m.queue_state, m.delay_reason, m.removed_at, m.orig_queue_id,
		       m.processed_logged, m.reconcile_misses, m.updated_at
		FROM messages m
	`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY m.received_at DESC, m.queue_id DESC LIMIT ?"
	args = append(args, limit+1)

	rows, err := s.readerDB.QueryContext(ctx, query, args...)
	if err != nil {
		return MessagePage{}, fmt.Errorf("query messages: %w", err)
	}

	msgs := make([]Message, 0)
	for rows.Next() {
		m, err := scanMessageRow(rows)
		if err != nil {
			_ = rows.Close()
			return MessagePage{}, err
		}
		m.Recipients = []Recipient{}
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return MessagePage{}, err
	}
	if err := rows.Close(); err != nil {
		return MessagePage{}, err
	}

	var nextCursor *string
	if len(msgs) > limit {
		msgs = msgs[:limit]
		last := msgs[len(msgs)-1]
		tok := EncodePageCursor(last.ReceivedAt, last.QueueID)
		nextCursor = &tok
	}

	if len(msgs) > 0 {
		if err := s.attachRecipients(ctx, msgs); err != nil {
			return MessagePage{}, err
		}
	}

	return MessagePage{
		Messages:   msgs,
		NextCursor: nextCursor,
	}, nil
}

func (s *Store) attachRecipients(ctx context.Context, msgs []Message) error {
	placeholders := make([]string, len(msgs))
	args := make([]any, len(msgs))
	byQID := make(map[string]int, len(msgs))
	for i, m := range msgs {
		placeholders[i] = "?"
		args[i] = m.QueueID
		byQID[m.QueueID] = i
	}

	query := `
		SELECT id, queue_id, address, orig_to, status, attempts, last_attempt_at, relay, dsn, response, delay
		FROM recipients
		WHERE queue_id IN (` + strings.Join(placeholders, ",") + `)
		ORDER BY id ASC
	`
	rows, err := s.readerDB.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query recipients batch: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		r, err := scanRecipientRow(rows)
		if err != nil {
			return err
		}
		if idx, ok := byQID[r.QueueID]; ok {
			msgs[idx].Recipients = append(msgs[idx].Recipients, r)
		}
	}
	return rows.Err()
}

// GetMessage loads a single message with all of its recipients and chronological events.
// Returns ErrNotFound if queueID does not exist.
func (s *Store) GetMessage(ctx context.Context, queueID string) (*Message, error) {
	row := s.readerDB.QueryRowContext(ctx, `
		SELECT queue_id, message_id, subject, sender, size, nrcpt,
		       client_name, client_addr, sasl_username, received_at,
		       queue_state, delay_reason, removed_at, orig_queue_id,
		       processed_logged, reconcile_misses, updated_at
		FROM messages
		WHERE queue_id = ?
	`, queueID)

	msg, err := scanMessageRow(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("query message %q: %w", queueID, err)
	}

	recips, err := s.loadRecipients(ctx, queueID)
	if err != nil {
		return nil, err
	}
	msg.Recipients = recips

	events, err := s.loadMessageEvents(ctx, queueID)
	if err != nil {
		return nil, err
	}
	msg.Events = events

	return &msg, nil
}

func (s *Store) loadRecipients(ctx context.Context, queueID string) ([]Recipient, error) {
	rows, err := s.readerDB.QueryContext(ctx, `
		SELECT id, queue_id, address, orig_to, status, attempts, last_attempt_at, relay, dsn, response, delay
		FROM recipients
		WHERE queue_id = ?
		ORDER BY id ASC
	`, queueID)
	if err != nil {
		return nil, fmt.Errorf("query recipients for %q: %w", queueID, err)
	}
	defer rows.Close()

	recips := make([]Recipient, 0)
	for rows.Next() {
		r, err := scanRecipientRow(rows)
		if err != nil {
			return nil, err
		}
		recips = append(recips, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return recips, nil
}

func (s *Store) loadMessageEvents(ctx context.Context, queueID string) ([]Event, error) {
	rows, err := s.readerDB.QueryContext(ctx, `
		SELECT id, time, '', recipient, event, relay, dsn, response, delay, attempt
		FROM events
		WHERE queue_id = ?
		ORDER BY id ASC
	`, queueID)
	if err != nil {
		return nil, fmt.Errorf("query events for %q: %w", queueID, err)
	}
	defer rows.Close()

	events := make([]Event, 0)
	for rows.Next() {
		ev, err := scanEventRow(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

// ListEvents returns up to limit events with id > afterID ordered by id ASC,
// along with the last returned event id (or afterID if empty) and whether more events exist.
func (s *Store) ListEvents(ctx context.Context, afterID int64, limit int) ([]Event, int64, bool, error) {
	if afterID < 0 {
		afterID = 0
	}
	if limit <= 0 {
		limit = 100
	} else if limit > 1000 {
		limit = 1000
	}

	rows, err := s.readerDB.QueryContext(ctx, `
		SELECT id, time, queue_id, recipient, event, relay, dsn, response, delay, attempt
		FROM events
		WHERE id > ?
		ORDER BY id ASC
		LIMIT ?
	`, afterID, limit+1)
	if err != nil {
		return nil, 0, false, fmt.Errorf("query events after %d: %w", afterID, err)
	}
	defer rows.Close()

	events := make([]Event, 0)
	for rows.Next() {
		ev, err := scanEventRow(rows)
		if err != nil {
			return nil, 0, false, err
		}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, false, err
	}

	hasMore := false
	if len(events) > limit {
		hasMore = true
		events = events[:limit]
	}
	lastID := afterID
	if len(events) > 0 {
		lastID = events[len(events)-1].ID
	}
	return events, lastID, hasMore, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanMessageRow(row rowScanner) (Message, error) {
	var m Message
	var sasl, removedAt, origQID sql.NullString
	var receivedAtStr, updatedAtStr string
	var procLogged int

	err := row.Scan(
		&m.QueueID,
		&m.MessageID,
		&m.Subject,
		&m.Sender,
		&m.Size,
		&m.RecipientCount,
		&m.Client.Name,
		&m.Client.Address,
		&sasl,
		&receivedAtStr,
		&m.QueueState,
		&m.DelayReason,
		&removedAt,
		&origQID,
		&procLogged,
		&m.ReconcileMisses,
		&updatedAtStr,
	)
	if err != nil {
		return Message{}, err
	}

	if sasl.Valid {
		v := sasl.String
		m.SASLUsername = &v
	}
	if origQID.Valid {
		v := origQID.String
		m.OrigQueueID = &v
	}
	if removedAt.Valid && removedAt.String != "" {
		t, err := parseTimeRequired(removedAt.String)
		if err != nil {
			return Message{}, fmt.Errorf("parse removed_at %q: %w", removedAt.String, err)
		}
		m.RemovedAt = &t
	}
	recTime, err := parseTimeRequired(receivedAtStr)
	if err != nil {
		return Message{}, fmt.Errorf("parse received_at %q: %w", receivedAtStr, err)
	}
	m.ReceivedAt = recTime

	updTime, err := parseTimeRequired(updatedAtStr)
	if err != nil {
		return Message{}, fmt.Errorf("parse updated_at %q: %w", updatedAtStr, err)
	}
	m.UpdatedAt = updTime
	m.ProcessedLogged = procLogged != 0
	return m, nil
}

func scanRecipientRow(row rowScanner) (Recipient, error) {
	var r Recipient
	var lastAttempt sql.NullString
	err := row.Scan(
		&r.ID,
		&r.QueueID,
		&r.Address,
		&r.OrigTo,
		&r.Status,
		&r.Attempts,
		&lastAttempt,
		&r.Relay,
		&r.DSN,
		&r.Response,
		&r.DelaySeconds,
	)
	if err != nil {
		return Recipient{}, err
	}
	if lastAttempt.Valid && lastAttempt.String != "" {
		t, err := parseTimeRequired(lastAttempt.String)
		if err != nil {
			return Recipient{}, fmt.Errorf("parse last_attempt_at %q: %w", lastAttempt.String, err)
		}
		r.LastAttemptAt = &t
	}
	return r, nil
}

func scanEventRow(row rowScanner) (Event, error) {
	var ev Event
	var timeStr string
	var recip, relay, dsn, resp sql.NullString
	var delay sql.NullFloat64
	var attempt sql.NullInt64

	err := row.Scan(
		&ev.ID,
		&timeStr,
		&ev.QueueID,
		&recip,
		&ev.Event,
		&relay,
		&dsn,
		&resp,
		&delay,
		&attempt,
	)
	if err != nil {
		return Event{}, err
	}
	t, err := parseTimeRequired(timeStr)
	if err != nil {
		return Event{}, fmt.Errorf("parse event time %q: %w", timeStr, err)
	}
	ev.Time = t
	if recip.Valid {
		v := recip.String
		ev.Recipient = &v
	}
	if relay.Valid {
		v := relay.String
		ev.Relay = &v
	}
	if dsn.Valid {
		v := dsn.String
		ev.DSN = &v
	}
	if resp.Valid {
		v := resp.String
		ev.Response = &v
	}
	if delay.Valid {
		v := delay.Float64
		ev.DelaySeconds = &v
	}
	if attempt.Valid {
		v := int(attempt.Int64)
		ev.Attempt = &v
	}
	return ev, nil
}

// Keep unused time import check satisfied if refactored.
var _ = time.UTC
