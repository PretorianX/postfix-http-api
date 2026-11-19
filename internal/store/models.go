package store

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/PretorianX/postfix-http-api/internal/parser"
)

// Fixed-width 9-digit fractional seconds RFC3339 UTC layout so SQLite TEXT
// lexicographical ordering always matches chronological order.
const timeFormat = "2006-01-02T15:04:05.000000000Z"

var (
	// ErrNotFound is returned when a requested message does not exist in the store.
	ErrNotFound = errors.New("message not found")
	// ErrInvalidCursor is returned when a pagination cursor token is malformed.
	ErrInvalidCursor = errors.New("invalid pagination cursor")
)

// ClientInfo identifies the SMTP client that submitted a message.
type ClientInfo struct {
	Name    string `json:"name"`
	Address string `json:"address"`
}

// Message represents the persisted delivery state of a queued Postfix message.
type Message struct {
	QueueID         string      `json:"queue_id"`
	MessageID       string      `json:"message_id"`
	Subject         string      `json:"subject"`
	Sender          string      `json:"sender"`
	Size            int64       `json:"size"`
	RecipientCount  int         `json:"nrcpt"`
	Client          ClientInfo  `json:"client"`
	SASLUsername    *string     `json:"sasl_username"`
	ReceivedAt      time.Time   `json:"received_at"`
	QueueState      string      `json:"queue_state"`
	DelayReason     string      `json:"delay_reason,omitempty"`
	RemovedAt       *time.Time  `json:"removed_at"`
	OrigQueueID     *string     `json:"orig_queue_id"`
	ProcessedLogged bool        `json:"-"`
	ReconcileMisses int         `json:"-"`
	UpdatedAt       time.Time   `json:"-"`
	Recipients      []Recipient `json:"recipients"`
	Events          []Event     `json:"events,omitempty"`
}

// Recipient represents per-recipient delivery tracking for a message.
type Recipient struct {
	ID            int64      `json:"-"`
	QueueID       string     `json:"-"`
	Address       string     `json:"address"`
	OrigTo        string     `json:"orig_to"`
	Status        string     `json:"status"`
	Attempts      int        `json:"attempts"`
	LastAttemptAt *time.Time `json:"last_attempt_at"`
	Relay         string     `json:"relay,omitempty"`
	DSN           string     `json:"dsn,omitempty"`
	Response      string     `json:"response,omitempty"`
	DelaySeconds  float64    `json:"delay,omitempty"`
}

// Event represents an immutable timeline entry emitted for a message or recipient.
type Event struct {
	ID           int64     `json:"id"`
	Time         time.Time `json:"time"`
	QueueID      string    `json:"queue_id,omitempty"`
	Recipient    *string   `json:"recipient,omitempty"`
	Event        string    `json:"event"`
	Relay        *string   `json:"relay,omitempty"`
	DSN          *string   `json:"dsn,omitempty"`
	Response     *string   `json:"response,omitempty"`
	DelaySeconds *float64  `json:"delay,omitempty"`
	Attempt      *int      `json:"attempt,omitempty"`
}

// PageCursor identifies the keyset position for ListMessages pagination.
type PageCursor struct {
	ReceivedAt string `json:"t"`
	QueueID    string `json:"q"`
}

// EncodePageCursor serializes a keyset position into an unpadded base64url token.
func EncodePageCursor(receivedAt time.Time, queueID string) string {
	b, _ := json.Marshal(PageCursor{
		ReceivedAt: receivedAt.UTC().Format(timeFormat),
		QueueID:    queueID,
	})
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodePageCursor decodes and validates an unpadded base64url pagination cursor.
func DecodePageCursor(token string) (*PageCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCursor, err)
	}
	var c PageCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCursor, err)
	}
	if c.ReceivedAt == "" || c.QueueID == "" {
		return nil, fmt.Errorf("%w: missing t or q", ErrInvalidCursor)
	}
	parsed, err := time.Parse(time.RFC3339Nano, c.ReceivedAt)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid timestamp", ErrInvalidCursor)
	}
	c.ReceivedAt = parsed.UTC().Format(timeFormat)
	return &c, nil
}

// MessageFilter specifies search criteria and keyset pagination parameters.
type MessageFilter struct {
	QueueID   string
	MessageID string
	Sender    string
	Recipient string
	Status    string
	From      *time.Time
	To        *time.Time
	Limit     int
	Cursor    string
}

// MessagePage is a single page of messages returned by ListMessages.
type MessagePage struct {
	Messages   []Message `json:"messages"`
	NextCursor *string   `json:"next_cursor"`
}

// SpoolCursor tracks the last committed position in the log spool.
type SpoolCursor struct {
	Segment      string
	Offset       int64
	LastRecordAt *time.Time
	UpdatedAt    time.Time
}

// ReconcileState holds persistent health and queue reconciliation telemetry.
type ReconcileState struct {
	LastSyncAt      *time.Time
	LastSyncOK      bool
	LastSyncError   string
	CursorGapCount  int64
	LastRetentionAt *time.Time
}

// QueueRecipient represents a recipient in a live queue snapshot.
type QueueRecipient struct {
	Address     string `json:"address"`
	OrigAddress string `json:"orig_address,omitempty"`
	DelayReason string `json:"delay_reason,omitempty"`
}

// QueueSnapshotEntry represents one live queued message from `postqueue -j`.
type QueueSnapshotEntry struct {
	QueueName   string           `json:"queue_name"`
	QueueID     string           `json:"queue_id"`
	ArrivalTime time.Time        `json:"arrival_time"`
	MessageSize int64            `json:"message_size"`
	Sender      string           `json:"sender"`
	Recipients  []QueueRecipient `json:"recipients"`
	DelayReason string           `json:"delay_reason,omitempty"`
}

// BatchEntry pairs a parsed log record with its spool/receive timestamp.
type BatchEntry struct {
	Record    parser.ParsedRecord
	Timestamp time.Time
}

func formatTime(t time.Time) string {
	return t.UTC().Format(timeFormat)
}

func parseTimeRequired(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}
