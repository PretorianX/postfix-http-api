package postfixcmd

import (
	"errors"
	"fmt"
	"regexp"
)

const (
	// MaxBulkQueueIDs is the maximum number of queue IDs allowed in a single postsuper request.
	MaxBulkQueueIDs = 1000
)

var (
	// ErrInvalidQueueID is returned when a queue ID fails strict format validation.
	ErrInvalidQueueID = errors.New("invalid queue_id format")
	// ErrInvalidAction is returned when a postsuper action is not in the strict allowlist.
	ErrInvalidAction = errors.New("invalid postsuper action")
	// ErrNotFound is returned when a requested message is no longer in the Postfix queue.
	ErrNotFound = errors.New("message not found in queue")
	// ErrRootHelperUnavailable is returned when the privileged root helper is not running.
	ErrRootHelperUnavailable = errors.New("root helper is unavailable")
)

var queueIDPattern = regexp.MustCompile(`^[A-Za-z0-9]{6,32}$`)

var allowedActions = map[string]string{
	"hold":    "-h",
	"release": "-H",
	"requeue": "-r",
	"expire":  "-e",
	"delete":  "-d",
}

// ValidateQueueID checks that id matches the strict Postfix queue ID format ^[A-Za-z0-9]{6,32}$.
// It accepts both classic short hex IDs and long base-52 IDs, and rejects ALL, flags,
// path separators, whitespace, and control characters.
func ValidateQueueID(id string) error {
	if !queueIDPattern.MatchString(id) {
		return fmt.Errorf("%w: %q", ErrInvalidQueueID, id)
	}
	return nil
}

// ActionFlag validates a postsuper action name and returns its corresponding CLI flag.
func ActionFlag(action string) (string, error) {
	flag, ok := allowedActions[action]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrInvalidAction, action)
	}
	return flag, nil
}

// ValidateBulkQueueIDs verifies that queueIDs has length 1..MaxBulkQueueIDs and every ID is valid.
func ValidateBulkQueueIDs(queueIDs []string) error {
	if len(queueIDs) < 1 || len(queueIDs) > MaxBulkQueueIDs {
		return fmt.Errorf("%w: queue_ids count %d must be between 1 and %d", ErrInvalidQueueID, len(queueIDs), MaxBulkQueueIDs)
	}
	for _, id := range queueIDs {
		if err := ValidateQueueID(id); err != nil {
			return err
		}
	}
	return nil
}
