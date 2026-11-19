package parser

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

// RecordKind identifies the structured event kind parsed from a Postfix log line.
type RecordKind string

const (
	KindSMTPDClient      RecordKind = "smtpd_client"
	KindPickup           RecordKind = "pickup"
	KindMessageID        RecordKind = "message_id"
	KindSubject          RecordKind = "subject"
	KindQmgrActive       RecordKind = "qmgr_active"
	KindQmgrExpired      RecordKind = "qmgr_expired"
	KindQmgrRemoved      RecordKind = "qmgr_removed"
	KindDelivery         RecordKind = "delivery"
	KindBounce           RecordKind = "bounce"
	KindPostsuperHold    RecordKind = "postsuper_hold"
	KindPostsuperRelease RecordKind = "postsuper_release"
	KindPostsuperRequeue RecordKind = "postsuper_requeue"
	KindPostsuperExpire  RecordKind = "postsuper_expire"
	KindPostsuperRemove  RecordKind = "postsuper_remove"
)

// ParsedRecord holds structured data extracted from a single Postfix log line.
type ParsedRecord struct {
	Timestamp time.Time
	Program   string
	QueueID   string
	Kind      RecordKind

	// smtpd client fields
	ClientName    string
	ClientAddress string
	SASLUsername  string

	// pickup / qmgr fields
	Sender         string
	HasSender      bool
	Size           int64
	RecipientCount int

	// cleanup fields
	MessageID string
	Subject   string

	// delivery agent fields (smtp, lmtp, local, virtual, pipe, error)
	Recipient    string
	OrigTo       string
	Relay        string
	DelaySeconds float64
	DSN          string
	Status       string // "delivered", "deferred", "bounced", "expired"
	Response     string

	// bounce fields: QueueID is the original message's queue ID; BounceQueueID is the NDN message's queue ID.
	BounceQueueID string
}

var (
	spoolHeaderPrefixRe = regexp.MustCompile(`^(?:[A-Z][a-z]{2}\s+\d+\s+\d{2}:\d{2}:\d{2}\s+)?(?:[^/\s:\[]+\s+)?(?:[a-z0-9]+\.[a-z]+\s+)?(?:[^/\s:]+/)*([A-Za-z0-9_-]+)\[\d+\]:\s+(.*)$`)
	rawHeaderPrefixRe   = regexp.MustCompile(`^(?:[A-Z][a-z]{2}\s+\d+\s+\d{2}:\d{2}:\d{2}\s+(?:[^/\s:\[]+\s+)?(?:[a-z0-9]+\.[a-z]+\s+)?)?(?:[^/\s:]+/)*([A-Za-z0-9_-]+)\[\d+\]:\s+(.*)$`)
	queueIDPrefixRe     = regexp.MustCompile(`^([A-Za-z0-9]{6,32}):\s+(.*)$`)

	smtpdClientRe    = regexp.MustCompile(`^client=([^,\s]+)(?:,(.*))?$`)
	saslUsernameRe   = regexp.MustCompile(`(?:^|[,\s])sasl_username=([^\s,]+)`)
	pickupRe         = regexp.MustCompile(`^uid=\d+\s+from=<([^>]*)>`)
	cleanupMsgIDRe   = regexp.MustCompile(`^message-id=(.*)$`)
	cleanupSubjectRe = regexp.MustCompile(`(?i)^info:\s+header\s+Subject:\s*(.*)$`)
	cleanupUIDFromRe = regexp.MustCompile(`;\s+uid=\d+\s+from=<`)
	qmgrActiveRe     = regexp.MustCompile(`^from=<([^>]*)>,\s+size=(\d+),\s+nrcpt=(\d+)\s+\(queue active\)\s*$`)
	qmgrExpiredRe    = regexp.MustCompile(`^from=<([^>]*)>,\s+status=(expired|force-expired)(?:,|$)`)
	bounceRe         = regexp.MustCompile(`^sender non-delivery notification:\s+([A-Za-z0-9]{6,32})\s*$`)
)

// ParseLine parses either a spool log line ("<RFC3339Nano> <raw_log_line>") or a
// raw Postfix syslog/postlogd line. Unrecognized lines return (nil, false).
func ParseLine(line string) (*ParsedRecord, bool) {
	line = strings.TrimRight(line, "\r\n\x00")
	if line == "" {
		return nil, false
	}

	var ts time.Time
	raw := line
	hasTS := false
	if sp := strings.IndexByte(line, ' '); sp > 0 {
		if parsed, err := time.Parse(time.RFC3339Nano, line[:sp]); err == nil {
			ts = parsed.UTC()
			raw = strings.TrimLeft(line[sp+1:], " ")
			hasTS = true
		}
	}

	return parseRaw(ts, raw, hasTS)
}

// ParseSpoolLine parses a line in the spool format "<RFC3339Nano> <raw_log_line>".
// It returns the parsed spool timestamp (even for recognized-format skipped lines),
// the parsed record if recognized, and ok=true when a record was extracted.
func ParseSpoolLine(line string) (time.Time, *ParsedRecord, bool) {
	line = strings.TrimRight(line, "\r\n\x00")
	sp := strings.IndexByte(line, ' ')
	if sp <= 0 {
		return time.Time{}, nil, false
	}
	ts, err := time.Parse(time.RFC3339Nano, line[:sp])
	if err != nil {
		return time.Time{}, nil, false
	}
	ts = ts.UTC()
	raw := strings.TrimLeft(line[sp+1:], " ")
	rec, ok := parseRaw(ts, raw, true)
	return ts, rec, ok
}

func parseRaw(ts time.Time, raw string, hasTS bool) (*ParsedRecord, bool) {
	var hdrMatch []string
	if hasTS {
		hdrMatch = spoolHeaderPrefixRe.FindStringSubmatch(raw)
	} else {
		hdrMatch = rawHeaderPrefixRe.FindStringSubmatch(raw)
	}
	if hdrMatch == nil {
		return nil, false
	}

	program := hdrMatch[1]
	body := hdrMatch[2]

	qidMatch := queueIDPrefixRe.FindStringSubmatch(body)
	if qidMatch == nil {
		return nil, false
	}
	queueID := qidMatch[1]
	if isNonQueueWord(queueID) {
		return nil, false
	}
	msg := qidMatch[2]

	rec := &ParsedRecord{
		Timestamp: ts,
		Program:   program,
		QueueID:   queueID,
	}

	switch program {
	case "smtpd":
		if !parseSMTPD(msg, rec) {
			return nil, false
		}
		return rec, true
	case "pickup":
		if !parsePickup(msg, rec) {
			return nil, false
		}
		return rec, true
	case "cleanup":
		if !parseCleanup(msg, rec) {
			return nil, false
		}
		return rec, true
	case "qmgr":
		if !parseQmgr(msg, rec) {
			return nil, false
		}
		return rec, true
	case "smtp", "lmtp", "local", "virtual", "pipe", "error":
		if !parseDelivery(msg, rec) {
			return nil, false
		}
		return rec, true
	case "bounce":
		if !parseBounce(msg, rec) {
			return nil, false
		}
		return rec, true
	case "postsuper":
		if !parsePostsuper(msg, rec) {
			return nil, false
		}
		return rec, true
	default:
		return nil, false
	}
}

func isNonQueueWord(s string) bool {
	switch strings.ToLower(s) {
	case "warning", "statistics", "fatal", "panic", "connect", "disconnect", "timeout":
		return true
	default:
		return false
	}
}

func parseSMTPD(msg string, rec *ParsedRecord) bool {
	m := smtpdClientRe.FindStringSubmatch(msg)
	if m == nil {
		return false
	}
	clientVal := m[1]
	openIdx := strings.LastIndexByte(clientVal, '[')
	if openIdx <= 0 || !strings.HasSuffix(clientVal, "]") {
		return false
	}
	rec.Kind = KindSMTPDClient
	rec.ClientName = clientVal[:openIdx]
	rec.ClientAddress = clientVal[openIdx+1 : len(clientVal)-1]
	if len(m) > 2 && m[2] != "" {
		if sm := saslUsernameRe.FindStringSubmatch(m[2]); sm != nil {
			rec.SASLUsername = sm[1]
		}
	}
	return true
}

func parsePickup(msg string, rec *ParsedRecord) bool {
	m := pickupRe.FindStringSubmatch(msg)
	if m == nil {
		return false
	}
	rec.Kind = KindPickup
	rec.Sender = m[1]
	rec.HasSender = true
	return true
}

func parseCleanup(msg string, rec *ParsedRecord) bool {
	if m := cleanupMsgIDRe.FindStringSubmatch(msg); m != nil {
		rec.Kind = KindMessageID
		rec.MessageID = strings.TrimSpace(m[1])
		return true
	}
	if m := cleanupSubjectRe.FindStringSubmatch(msg); m != nil {
		rest := m[1]
		idx := strings.LastIndex(rest, "; from=<")
		if idx < 0 {
			if loc := cleanupUIDFromRe.FindStringIndex(rest); loc != nil {
				idx = loc[0]
			}
		}
		if idx < 0 {
			return false
		}
		prefix := rest[:idx]
		lastFrom := strings.LastIndex(prefix, " from ")
		if lastFrom < 0 {
			return false
		}
		rawSubj := prefix[:lastFrom]
		rec.Kind = KindSubject
		rec.Subject = DecodeSubject(rawSubj)
		return true
	}
	return false
}

func parseQmgr(msg string, rec *ParsedRecord) bool {
	if m := qmgrActiveRe.FindStringSubmatch(msg); m != nil {
		size, err := strconv.ParseInt(m[2], 10, 64)
		if err != nil {
			return false
		}
		nrcpt, err := strconv.Atoi(m[3])
		if err != nil {
			return false
		}
		rec.Kind = KindQmgrActive
		rec.Sender = m[1]
		rec.HasSender = true
		rec.Size = size
		rec.RecipientCount = nrcpt
		return true
	}
	if m := qmgrExpiredRe.FindStringSubmatch(msg); m != nil {
		rec.Kind = KindQmgrExpired
		rec.Sender = m[1]
		rec.HasSender = true
		rec.Status = "expired"
		return true
	}
	if strings.TrimSpace(msg) == "removed" {
		rec.Kind = KindQmgrRemoved
		return true
	}
	return false
}

func parseDelivery(msg string, rec *ParsedRecord) bool {
	if !strings.HasPrefix(msg, "to=<") {
		return false
	}
	statusIdx := strings.Index(msg, ", status=")
	if statusIdx < 0 {
		return false
	}
	prefix := msg[:statusIdx]
	statusRest := strings.TrimSpace(msg[statusIdx+len(", status="):])

	var statusWord, response string
	if sp := strings.IndexByte(statusRest, ' '); sp >= 0 {
		statusWord = statusRest[:sp]
		respPart := strings.TrimSpace(statusRest[sp+1:])
		if strings.HasPrefix(respPart, "(") && strings.HasSuffix(respPart, ")") {
			response = respPart[1 : len(respPart)-1]
		} else {
			response = respPart
		}
	} else {
		statusWord = statusRest
	}

	var mappedStatus string
	switch statusWord {
	case "sent":
		mappedStatus = "delivered"
	case "deferred":
		mappedStatus = "deferred"
	case "bounced":
		mappedStatus = "bounced"
	case "expired":
		mappedStatus = "expired"
	default:
		return false
	}

	var (
		recipient string
		origTo    string
		relay     string
		delayStr  string
		dsn       string
		hasTo     bool
		hasRelay  bool
		hasDelay  bool
	)

	for _, part := range strings.Split(prefix, ", ") {
		part = strings.TrimSpace(part)
		eq := strings.IndexByte(part, '=')
		if eq <= 0 {
			continue
		}
		k := part[:eq]
		v := part[eq+1:]
		switch k {
		case "to":
			if !strings.HasPrefix(v, "<") || !strings.HasSuffix(v, ">") {
				return false
			}
			recipient = v[1 : len(v)-1]
			hasTo = true
		case "orig_to":
			if strings.HasPrefix(v, "<") && strings.HasSuffix(v, ">") {
				origTo = v[1 : len(v)-1]
			}
		case "relay":
			relay = v
			hasRelay = true
		case "delay":
			delayStr = v
			hasDelay = true
		case "dsn":
			dsn = v
		}
	}

	if !hasTo || !hasRelay || !hasDelay {
		return false
	}
	delay, err := strconv.ParseFloat(delayStr, 64)
	if err != nil {
		return false
	}

	rec.Kind = KindDelivery
	rec.Recipient = recipient
	rec.OrigTo = origTo
	rec.Relay = relay
	rec.DelaySeconds = delay
	rec.DSN = dsn
	rec.Status = mappedStatus
	rec.Response = response
	return true
}

func parseBounce(msg string, rec *ParsedRecord) bool {
	m := bounceRe.FindStringSubmatch(msg)
	if m == nil {
		return false
	}
	rec.Kind = KindBounce
	rec.BounceQueueID = m[1]
	return true
}

func parsePostsuper(msg string, rec *ParsedRecord) bool {
	switch strings.TrimSpace(msg) {
	case "placed on hold":
		rec.Kind = KindPostsuperHold
	case "released from hold":
		rec.Kind = KindPostsuperRelease
	case "requeued":
		rec.Kind = KindPostsuperRequeue
	case "expired":
		rec.Kind = KindPostsuperExpire
	case "removed":
		rec.Kind = KindPostsuperRemove
	default:
		return false
	}
	return true
}
