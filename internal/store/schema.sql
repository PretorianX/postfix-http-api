CREATE TABLE IF NOT EXISTS messages (
    queue_id          TEXT PRIMARY KEY,
    message_id        TEXT NOT NULL DEFAULT '',
    subject           TEXT NOT NULL DEFAULT '',
    sender            TEXT NOT NULL DEFAULT '',
    size              INTEGER NOT NULL DEFAULT 0,
    nrcpt             INTEGER NOT NULL DEFAULT 0,
    client_name       TEXT NOT NULL DEFAULT '',
    client_addr       TEXT NOT NULL DEFAULT '',
    sasl_username     TEXT,
    received_at       TEXT NOT NULL,
    queue_state       TEXT NOT NULL DEFAULT 'incoming'
                      CHECK (queue_state IN ('incoming','active','deferred','hold','removed','requeued','unknown')),
    delay_reason      TEXT NOT NULL DEFAULT '',
    removed_at        TEXT,
    orig_queue_id     TEXT,
    processed_logged  INTEGER NOT NULL DEFAULT 0,
    reconcile_misses  INTEGER NOT NULL DEFAULT 0,
    updated_at        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_received_qid
    ON messages(received_at DESC, queue_id DESC);
CREATE INDEX IF NOT EXISTS idx_messages_message_id
    ON messages(message_id) WHERE message_id != '';
CREATE INDEX IF NOT EXISTS idx_messages_sender
    ON messages(sender) WHERE sender != '';
CREATE INDEX IF NOT EXISTS idx_messages_removed_at
    ON messages(removed_at) WHERE removed_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_open
    ON messages(queue_state) WHERE removed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_messages_orig_queue_id
    ON messages(orig_queue_id) WHERE orig_queue_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS recipients (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id        TEXT NOT NULL REFERENCES messages(queue_id) ON DELETE CASCADE,
    address         TEXT NOT NULL,
    orig_to         TEXT NOT NULL DEFAULT '',
    status          TEXT NOT NULL DEFAULT 'queued'
                    CHECK (status IN ('queued','deferred','delivered','bounced','expired','deleted','unknown')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TEXT,
    relay           TEXT NOT NULL DEFAULT '',
    dsn             TEXT NOT NULL DEFAULT '',
    response        TEXT NOT NULL DEFAULT '',
    delay           REAL NOT NULL DEFAULT 0,
    UNIQUE(queue_id, address, orig_to)
);

CREATE INDEX IF NOT EXISTS idx_recipients_queue_id
    ON recipients(queue_id);
CREATE INDEX IF NOT EXISTS idx_recipients_address
    ON recipients(address);
CREATE INDEX IF NOT EXISTS idx_recipients_status
    ON recipients(status);
CREATE INDEX IF NOT EXISTS idx_recipients_address_status
    ON recipients(address, status);

CREATE TABLE IF NOT EXISTS events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    time      TEXT NOT NULL,
    queue_id  TEXT NOT NULL REFERENCES messages(queue_id) ON DELETE CASCADE,
    recipient TEXT,
    event     TEXT NOT NULL
              CHECK (event IN ('processed','delivered','deferred','bounced','expired','deleted','held','released','requeued','expire_requested')),
    relay     TEXT,
    dsn       TEXT,
    response  TEXT,
    delay     REAL,
    attempt   INTEGER
);

CREATE INDEX IF NOT EXISTS idx_events_queue_id_id
    ON events(queue_id, id);
CREATE INDEX IF NOT EXISTS idx_events_time
    ON events(time);

CREATE TABLE IF NOT EXISTS spool_cursor (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    segment        TEXT NOT NULL DEFAULT '',
    offset         INTEGER NOT NULL DEFAULT 0,
    last_record_at TEXT,
    updated_at     TEXT NOT NULL
);

INSERT OR IGNORE INTO spool_cursor (id, segment, offset, updated_at)
VALUES (1, '', 0, '1970-01-01T00:00:00.000000000Z');

CREATE TABLE IF NOT EXISTS reconcile_state (
    id                INTEGER PRIMARY KEY CHECK (id = 1),
    last_sync_at      TEXT,
    last_sync_ok      INTEGER NOT NULL DEFAULT 1,
    last_sync_error   TEXT NOT NULL DEFAULT '',
    cursor_gap_count  INTEGER NOT NULL DEFAULT 0,
    last_retention_at TEXT
);

INSERT OR IGNORE INTO reconcile_state (id, last_sync_ok, cursor_gap_count)
VALUES (1, 1, 0);
