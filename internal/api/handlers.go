package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/PretorianX/postfix-http-api/internal/config"
	"github.com/PretorianX/postfix-http-api/internal/indexer"
	"github.com/PretorianX/postfix-http-api/internal/postfixcmd"
	"github.com/PretorianX/postfix-http-api/internal/store"
)

var validRecipientStatuses = map[string]bool{
	"queued":    true,
	"deferred":  true,
	"delivered": true,
	"bounced":   true,
	"expired":   true,
	"deleted":   true,
	"unknown":   true,
}

type messageDetailResponse struct {
	QueueID        string            `json:"queue_id"`
	MessageID      string            `json:"message_id"`
	Subject        string            `json:"subject"`
	Sender         string            `json:"sender"`
	Size           int64             `json:"size"`
	RecipientCount int               `json:"nrcpt"`
	Client         store.ClientInfo  `json:"client"`
	SASLUser       *string           `json:"sasl_username"`
	ReceivedAt     time.Time         `json:"received_at"`
	QueueState     string            `json:"queue_state"`
	DelayReason    string            `json:"delay_reason,omitempty"`
	RemovedAt      *time.Time        `json:"removed_at"`
	OrigQueueID    *string           `json:"orig_queue_id"`
	NextRetryAt    *string           `json:"next_retry_at"`
	Recipients     []store.Recipient `json:"recipients"`
	Events         []store.Event     `json:"events"`
}

type messageContentResponse struct {
	QueueID      string `json:"queue_id"`
	Headers      string `json:"headers"`
	Body         string `json:"body"`
	BodyEncoding string `json:"body_encoding"`
}

type eventsResponse struct {
	Events  []store.Event `json:"events"`
	LastID  int64         `json:"last_id"`
	HasMore bool          `json:"has_more"`
}

type queueResponse struct {
	Total   int                        `json:"total"`
	Counts  map[string]int             `json:"counts"`
	Entries []store.QueueSnapshotEntry `json:"entries"`
}

type bulkActionsRequest struct {
	Action   string   `json:"action"`
	QueueIDs []string `json:"queue_ids"`
}

type writeActionResponse struct {
	Action   string   `json:"action"`
	QueueIDs []string `json:"queue_ids"`
	ExitCode int      `json:"exit_code"`
	Output   string   `json:"output"`
}

type healthResponse struct {
	Status           string  `json:"status"`
	PostfixVersion   string  `json:"postfix_version"`
	IndexerDelayMs   int64   `json:"indexer_delay_ms"`
	SpoolGaps        uint64  `json:"spool_gaps"`
	LostSpoolRecords uint64  `json:"lost_spool_records"`
	SpoolBytes       int64   `json:"spool_bytes"`
	SpoolMaxBytes    int64   `json:"spool_max_bytes"`
	LastQueueSyncAt  *string `json:"last_queue_sync_at"`
	LastQueueSyncOK  bool    `json:"last_queue_sync_ok"`
	RootHelperAlive  bool    `json:"root_helper_alive"`
}

// ConvertQueueEntries converts raw `postqueue -j` entries into store.QueueSnapshotEntry
// values with UTC time.Time arrival timestamps and entry-level delay_reason.
func ConvertQueueEntries(raw []postfixcmd.QueueEntry) []store.QueueSnapshotEntry {
	entries := make([]store.QueueSnapshotEntry, 0, len(raw))
	for _, e := range raw {
		var arrival time.Time
		if e.ArrivalTime > 0 {
			arrival = time.Unix(e.ArrivalTime, 0).UTC()
		}

		recips := make([]store.QueueRecipient, 0, len(e.Recipients))
		var delayReason string
		for _, r := range e.Recipients {
			recips = append(recips, store.QueueRecipient{
				Address:     r.Address,
				OrigAddress: r.OrigAddress,
				DelayReason: r.DelayReason,
			})
			if delayReason == "" && r.DelayReason != "" {
				delayReason = r.DelayReason
			}
		}

		entries = append(entries, store.QueueSnapshotEntry{
			QueueName:   e.QueueName,
			QueueID:     e.QueueID,
			ArrivalTime: arrival,
			MessageSize: e.MessageSize,
			Sender:      e.Sender,
			Recipients:  recips,
			DelayReason: delayReason,
		})
	}
	return entries
}

// NewQueueLister adapts *postfixcmd.Runner to indexer.QueueLister.
func NewQueueLister(runner *postfixcmd.Runner) indexer.QueueLister {
	return queueListerAdapter{runner: runner}
}

type queueListerAdapter struct {
	runner *postfixcmd.Runner
}

func (a queueListerAdapter) ListQueue(ctx context.Context) ([]store.QueueSnapshotEntry, error) {
	if a.runner == nil {
		return nil, errors.New("postfixcmd runner is nil")
	}
	raw, _, err := a.runner.ListQueue(ctx)
	if err != nil {
		return nil, err
	}
	return ConvertQueueEntries(raw), nil
}

func (s *Server) handleListMessages(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := store.MessageFilter{
		QueueID:   q.Get("queue_id"),
		MessageID: q.Get("message_id"),
		Sender:    q.Get("sender"),
		Recipient: q.Get("recipient"),
		Status:    q.Get("status"),
		Cursor:    q.Get("cursor"),
		Limit:     defaultLimit,
	}

	if filter.QueueID != "" {
		if err := postfixcmd.ValidateQueueID(filter.QueueID); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_argument", "invalid queue_id format")
			return
		}
	}
	if filter.Status != "" && !validRecipientStatuses[filter.Status] {
		writeError(w, http.StatusBadRequest, "invalid_argument", fmt.Sprintf("invalid status filter %q", filter.Status))
		return
	}

	if fromRaw := q.Get("from"); fromRaw != "" {
		t, err := time.Parse(time.RFC3339Nano, fromRaw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_argument", "invalid 'from' timestamp (expected RFC3339)")
			return
		}
		u := t.UTC()
		filter.From = &u
	}
	if toRaw := q.Get("to"); toRaw != "" {
		t, err := time.Parse(time.RFC3339Nano, toRaw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_argument", "invalid 'to' timestamp (expected RFC3339)")
			return
		}
		u := t.UTC()
		filter.To = &u
	}

	limit, err := parseLimitParam(q.Get("limit"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	filter.Limit = limit

	page, err := s.store.ListMessages(r.Context(), filter)
	if err != nil {
		if errors.Is(err, store.ErrInvalidCursor) {
			writeError(w, http.StatusBadRequest, "invalid_argument", "invalid pagination cursor")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to query messages")
		return
	}
	if page.Messages == nil {
		page.Messages = []store.Message{}
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleGetMessage(w http.ResponseWriter, r *http.Request, queueID string) {
	if err := postfixcmd.ValidateQueueID(queueID); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_argument", "invalid queue_id format")
		return
	}

	msg, err := s.store.GetMessage(r.Context(), queueID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("message %q not found", queueID))
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to load message")
		return
	}

	var nextRetry *string
	if msg.QueueState == "deferred" {
		var retryTime *time.Time
		if s.runner != nil {
			retryTime, _ = s.runner.NextRetryAt(queueID)
		} else if s.queueDir != "" {
			retryTime, _ = indexer.NextRetryAt(s.queueDir, queueID)
		}
		if retryTime != nil {
			formatted := retryTime.UTC().Format(time.RFC3339)
			nextRetry = &formatted
		}
	}

	recips := msg.Recipients
	if recips == nil {
		recips = []store.Recipient{}
	}
	events := msg.Events
	if events == nil {
		events = []store.Event{}
	}

	writeJSON(w, http.StatusOK, messageDetailResponse{
		QueueID:        msg.QueueID,
		MessageID:      msg.MessageID,
		Subject:        msg.Subject,
		Sender:         msg.Sender,
		Size:           msg.Size,
		RecipientCount: msg.RecipientCount,
		Client:         msg.Client,
		SASLUser:       msg.SASLUsername,
		ReceivedAt:     msg.ReceivedAt,
		QueueState:     msg.QueueState,
		DelayReason:    msg.DelayReason,
		RemovedAt:      msg.RemovedAt,
		OrigQueueID:    msg.OrigQueueID,
		NextRetryAt:    nextRetry,
		Recipients:     recips,
		Events:         events,
	})
}

func (s *Server) handleGetMessageContent(w http.ResponseWriter, r *http.Request, queueID string) {
	if err := postfixcmd.ValidateQueueID(queueID); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_argument", "invalid queue_id format")
		return
	}
	if s.runner == nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "command runner is unavailable")
		return
	}

	headers, body, err := s.runner.ReadContent(r.Context(), queueID)
	if err != nil {
		switch {
		case errors.Is(err, postfixcmd.ErrInvalidQueueID):
			writeError(w, http.StatusBadRequest, "invalid_argument", "invalid queue_id format")
		case errors.Is(err, postfixcmd.ErrNotFound):
			writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("queued message %q not found", queueID))
		default:
			writeError(w, http.StatusBadGateway, "command_failed", fmt.Sprintf("postcat failed: %v", err))
		}
		return
	}

	bodyEncoding := "utf-8"
	outBody := body
	if !utf8.ValidString(body) {
		bodyEncoding = "base64"
		outBody = base64.StdEncoding.EncodeToString([]byte(body))
	}

	writeJSON(w, http.StatusOK, messageContentResponse{
		QueueID:      queueID,
		Headers:      strings.ToValidUTF8(headers, "\uFFFD"),
		Body:         outBody,
		BodyEncoding: bodyEncoding,
	})
}

func (s *Server) handleListEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	var after int64
	if rawAfter := q.Get("after"); rawAfter != "" {
		v, err := strconv.ParseInt(rawAfter, 10, 64)
		if err != nil || v < 0 {
			writeError(w, http.StatusBadRequest, "invalid_argument", "invalid 'after' parameter (must be non-negative integer)")
			return
		}
		after = v
	}

	limit, err := parseLimitParam(q.Get("limit"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}

	events, lastID, hasMore, err := s.store.ListEvents(r.Context(), after, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to query events")
		return
	}
	if events == nil {
		events = []store.Event{}
	}

	writeJSON(w, http.StatusOK, eventsResponse{
		Events:  events,
		LastID:  lastID,
		HasMore: hasMore,
	})
}

func (s *Server) handleListQueue(w http.ResponseWriter, r *http.Request) {
	if s.runner == nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "command runner is unavailable")
		return
	}

	rawEntries, counts, err := s.runner.ListQueue(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, "command_failed", fmt.Sprintf("postqueue -j failed: %v", err))
		return
	}
	if counts == nil {
		counts = map[string]int{}
	}
	for _, k := range []string{"incoming", "active", "deferred", "hold", "maildrop"} {
		if _, ok := counts[k]; !ok {
			counts[k] = 0
		}
	}

	entries := ConvertQueueEntries(rawEntries)
	writeJSON(w, http.StatusOK, queueResponse{
		Total:   len(entries),
		Counts:  counts,
		Entries: entries,
	})
}

func (s *Server) handleSingleMessagePostsuper(w http.ResponseWriter, r *http.Request, tok *config.Token, queueID, action string) {
	if err := postfixcmd.ValidateQueueID(queueID); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_argument", "invalid queue_id format")
		return
	}
	if s.runner == nil {
		s.emitAudit(tok, action, []string{queueID}, -1, r.RemoteAddr)
		writeError(w, http.StatusInternalServerError, "internal_error", "command runner is unavailable")
		return
	}

	qids := []string{queueID}
	res, err := s.runner.PostsuperAction(r.Context(), action, qids)
	s.respondWriteCommand(w, r, tok, action, qids, res, err)
}

func (s *Server) handleFlushMessage(w http.ResponseWriter, r *http.Request, tok *config.Token, queueID string) {
	if err := postfixcmd.ValidateQueueID(queueID); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_argument", "invalid queue_id format")
		return
	}
	if s.runner == nil {
		s.emitAudit(tok, "flush", []string{queueID}, -1, r.RemoteAddr)
		writeError(w, http.StatusInternalServerError, "internal_error", "command runner is unavailable")
		return
	}

	qids := []string{queueID}
	res, err := s.runner.FlushMessage(r.Context(), queueID)
	s.respondWriteCommand(w, r, tok, "flush", qids, res, err)
}

func (s *Server) handleFlushQueue(w http.ResponseWriter, r *http.Request, tok *config.Token) {
	if s.runner == nil {
		s.emitAudit(tok, "flush", []string{}, -1, r.RemoteAddr)
		writeError(w, http.StatusInternalServerError, "internal_error", "command runner is unavailable")
		return
	}

	qids := []string{}
	res, err := s.runner.FlushQueue(r.Context())
	s.respondWriteCommand(w, r, tok, "flush", qids, res, err)
}

func (s *Server) handleBulkQueueActions(w http.ResponseWriter, r *http.Request, tok *config.Token) {
	bodyBytes, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBodyBytes))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large", "request body exceeds 1 MiB limit")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_argument", "failed to read request body")
		return
	}
	if len(bytes.TrimSpace(bodyBytes)) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_argument", "request body must not be empty")
		return
	}

	var req bulkActionsRequest
	dec := json.NewDecoder(bytes.NewReader(bodyBytes))
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_argument", fmt.Sprintf("malformed JSON body: %v", err))
		return
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_argument", "unexpected trailing JSON data")
		return
	}

	if _, err := postfixcmd.ActionFlag(req.Action); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	if err := postfixcmd.ValidateBulkQueueIDs(req.QueueIDs); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}

	if s.runner == nil {
		s.emitAudit(tok, req.Action, req.QueueIDs, -1, r.RemoteAddr)
		writeError(w, http.StatusInternalServerError, "internal_error", "command runner is unavailable")
		return
	}

	res, err := s.runner.PostsuperAction(r.Context(), req.Action, req.QueueIDs)
	s.respondWriteCommand(w, r, tok, req.Action, req.QueueIDs, res, err)
}

func (s *Server) respondWriteCommand(
	w http.ResponseWriter,
	r *http.Request,
	tok *config.Token,
	action string,
	queueIDs []string,
	res *postfixcmd.CommandResult,
	err error,
) {
	exitCode := -1
	if res != nil {
		exitCode = res.ExitCode
		if err != nil && exitCode == 0 {
			exitCode = -1
		}
	}

	s.emitAudit(tok, action, queueIDs, exitCode, r.RemoteAddr)

	if err != nil {
		if errors.Is(err, postfixcmd.ErrInvalidQueueID) || errors.Is(err, postfixcmd.ErrInvalidAction) {
			writeError(w, http.StatusBadRequest, "invalid_argument", err.Error())
			return
		}
		writeError(w, http.StatusBadGateway, "command_failed", fmt.Sprintf("%s failed: %v", action, err))
		return
	}

	if res == nil {
		writeError(w, http.StatusBadGateway, "command_failed", fmt.Sprintf("%s returned no result", action))
		return
	}

	output := res.CombinedOutput()
	if res.ExitCode != 0 {
		msg := fmt.Sprintf("%s exited with status %d", action, res.ExitCode)
		if detail := strings.TrimSpace(output); detail != "" {
			msg = fmt.Sprintf("%s: %s", msg, detail)
		}
		writeError(w, http.StatusBadGateway, "command_failed", msg)
		return
	}

	outIDs := queueIDs
	if outIDs == nil {
		outIDs = []string{}
	}
	writeJSON(w, http.StatusOK, writeActionResponse{
		Action:   action,
		QueueIDs: outIDs,
		ExitCode: res.ExitCode,
		Output:   output,
	})
}

func (s *Server) emitAudit(tok *config.Token, action string, queueIDs []string, exitCode int, remoteAddr string) {
	if s.auditLogger == nil {
		return
	}
	tokName := ""
	if tok != nil {
		tokName = tok.Name
	}
	if queueIDs == nil {
		queueIDs = []string{}
	}
	s.auditLogger.Log(AuditEvent{
		Timestamp:  s.now().UTC().Format(auditTimeLayout),
		Audit:      true,
		Token:      tokName,
		Action:     action,
		QueueIDs:   queueIDs,
		ExitCode:   exitCode,
		RemoteAddr: remoteAddr,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	rootHelperAlive := s.runner != nil && s.runner.RootHelperAlive()

	var metrics indexer.Metrics
	if s.indexer != nil {
		metrics = s.indexer.HealthMetrics()
	} else {
		metrics.Degraded = true
	}

	degraded := metrics.Degraded || !rootHelperAlive
	status := "OPERATIONAL"
	if degraded {
		status = "DEGRADED"
	}

	var lastSync *string
	if metrics.LastQueueSyncAt != nil {
		formatted := metrics.LastQueueSyncAt.UTC().Format(time.RFC3339)
		lastSync = &formatted
	}

	writeJSON(w, http.StatusOK, healthResponse{
		Status:           status,
		PostfixVersion:   s.postfixVersion,
		IndexerDelayMs:   metrics.IndexerDelayMs,
		SpoolGaps:        metrics.SpoolGaps,
		LostSpoolRecords: metrics.LostSpoolRecords,
		SpoolBytes:       metrics.SpoolBytes,
		SpoolMaxBytes:    s.cfg.Spool.MaxTotalBytes,
		LastQueueSyncAt:  lastSync,
		LastQueueSyncOK:  metrics.LastQueueSyncOK,
		RootHelperAlive:  rootHelperAlive,
	})
}

func parseLimitParam(raw string) (int, error) {
	if raw == "" {
		return defaultLimit, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 || v > maxLimit {
		return 0, fmt.Errorf("invalid 'limit' parameter (must be integer in [1, %d])", maxLimit)
	}
	return v, nil
}
