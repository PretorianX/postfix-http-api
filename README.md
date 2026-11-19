# postfix-http-api

**HTTP delivery-status tracking and queue management for Postfix — with zero external dependencies.**

Postfix only shows mail that is *still waiting in the queue*. The moment a message is delivered or bounced, Postfix removes it and the only trace of what happened lives as unstructured text in the mail log.

`postfix-http-api` solves this by adding a **SendGrid-style HTTP API** directly inside Postfix (`>= 3.5`):

- **Track every email end to end:** query by `queue_id`, `message_id`, sender, recipient, status, or time range to see the envelope, decoded `Subject`, every delivery attempt, relay, DSN code, remote SMTP response, final outcome, and exact `next_retry_at` time.
- **Stream delivery events:** consume an ordered event feed (`GET /v1/events?after=<id>`) to sync delivery status into your own app or database.
- **Manage the live queue over HTTP:** list queue counts, inspect queued headers/body (`postcat`), and run single or bulk `hold`, `release`, `requeue`, `expire`, `flush`, and `delete` actions.
- **Drop-in packaging:** ships as a single static Linux binary (`CGO_ENABLED=0`, `amd64`/`arm64`, glibc & musl) **and** a drop-in replacement Docker image for [`boky/postfix:v4.4.0`](docker/boky/Dockerfile).

No Postfix source patches. No sidecar containers. No external database. Postfix `master(8)` starts and supervises everything itself.

---

## How It Works (Architecture & Log-Processing Flow)

Instead of tailing log files or adding a second supervisor, `postfix-api` integrates directly into Postfix `master.cf` as two services and splits the **hot log path** from **indexing & HTTP serving** via a bounded disk spool:

```text
                        Postfix daemons (smtpd, cleanup, qmgr, smtp, bounce, postsuper)
                                                    │
                                     unix-dgram (public/postlog)
                                                    ▼
                    ┌───────────────────────────────────────────────────────────┐
                    │  1. Log Receiver  (master.cf: postlog unix-dgram)         │
                    │     Replaces stock postlogd; runs as mail_owner           │
                    │     Fixed 64 KB buffer · zero-alloc hot loop              │
                    └───────────────┬───────────────────────────┬───────────────┘
                                    │                           │
                   normal logs      │                           │ all records + UTC ns timestamp
                   (Subject lines   │                           │ (segmented, bounded to 256 MiB)
                   redacted by      ▼                           ▼
                    default)   maillog_file              Disk Spool (spool/)
                               (/dev/stdout)                    │
                                                                │ async batch read (1000/tx)
                                                                ▼
                    ┌───────────────────────────────────────────────────────────┐
                    │  2. API + Indexer  (master.cf: <addr>:<port> inet)        │
                    │     Runs unprivileged as mail_owner                       │
                    │     • Parses logs + RFC 2047 Subjects → SQLite (WAL)      │
                    │     • Reconciles live queue via postqueue -j              │
                    │     • Reads next_retry_at from deferred queue file mtime  │
                    │     • Serves authenticated /v1/* HTTP(S) API              │
                    └───────────────────────────────┬───────────────────────────┘
                                                    │ private pipe (validated queue IDs only)
                                                    ▼
                    ┌───────────────────────────────────────────────────────────┐
                    │  3. Root Helper  (_root-helper, spawned pre-drop)         │
                    │     Only root process · runs postsuper (-h/-H/-r/-e/-d)   │
                    └───────────────────────────────────────────────────────────┘
```

### Why this log-processing pipeline matters

1. **Mail flow is never blocked by the API or database:** Postfix's log socket (`public/postlog`) is blocking. Because the log receiver never parses logs, never opens SQLite, and never waits on the indexer, an indexer slowdown or API crash cannot stall mail delivery.
2. **Fixed memory under any log rate:** Backlog lives on disk in segmented spool files (`16 MiB` segments, `256 MiB` total cap). If the indexer is paused or overwhelmed, the receiver evicts the oldest spool segment, increments `spool_gaps`, and keeps mail flowing.
3. **Instant recovery on crash:** Both processes speak the Postfix `master(8)` child protocol (FD `5` status handshake and EOF shutdown), so `master` restarts a killed receiver in milliseconds without triggering the 60-second `service_throttle_time` delay, and stops both cleanly on `postfix reload` / `postfix stop`.
4. **Subject capture without leaking PII to container logs:** A `header_checks` rule (`/^Subject:/ INFO`) logs the subject into `postlog`. By default (`subject_in_output: false`), the receiver writes it only to the spool for the API and strips it from `maillog_file` (`/dev/stdout`).

---

## Example API Usage

Query a message by its Postfix queue ID:

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:8025/v1/messages/4jV74M63RlzHxK3 | jq .
```

```json
{
  "queue_id": "4jV74M63RlzHxK3",
  "message_id": "<4jV74M63RlzHxK3@mail.example.com>",
  "subject": "Привет order 42",
  "sender": "a@example.com",
  "size": 310,
  "client": { "name": "app-1", "address": "10.0.0.12" },
  "sasl_username": null,
  "received_at": "2026-11-18T16:54:43.123456789Z",
  "queue_state": "deferred",
  "next_retry_at": "2026-11-18T16:59:45Z",
  "recipients": [
    {
      "address": "b@example.org",
      "status": "deferred",
      "attempts": 1,
      "last_attempt_at": "2026-11-18T16:54:45.201Z",
      "relay": "none",
      "dsn": "4.4.1",
      "response": "connect to 192.0.2.1[192.0.2.1]:25: Connection timed out"
    }
  ],
  "events": [
    { "id": 101, "time": "2026-11-18T16:54:43.130Z", "event": "processed" },
    { "id": 102, "time": "2026-11-18T16:54:45.201Z", "event": "deferred", "recipient": "b@example.org", "dsn": "4.4.1", "attempt": 1 }
  ]
}
```

### HTTP API Endpoints

All endpoints require `Authorization: Bearer <token>`. Tokens are stored as SHA-256 hashes with least-privilege scopes (`read`, `write`, `content`).

| Method & Path | Scope | Purpose |
|---|---|---|
| `GET /v1/messages` | `read` | Search messages (`queue_id`, `message_id`, `sender`, `recipient`, `status`, `from`, `to`, `limit`, `cursor`). |
| `GET /v1/messages/{queue_id}` | `read` | Full message details, per-recipient attempts, events, and live `next_retry_at`. |
| `GET /v1/messages/{queue_id}/content` | `content` | Queued headers and body via `postcat -hbq` (`404` once it leaves the queue). |
| `GET /v1/events` | `read` | Ordered delivery event feed (`after`, `limit`) for downstream polling/sync. |
| `GET /v1/queue` | `read` | Live queue entries and per-queue counts via `postqueue -j`. |
| `POST /v1/messages/{queue_id}/{hold,release,requeue,expire,flush}` | `write` | Single-message queue control (`postsuper` / `postqueue -i`). |
| `DELETE /v1/messages/{queue_id}` | `write` | Delete a queued message (`postsuper -d`). |
| `POST /v1/queue/actions` | `write` | Bulk queue action (`hold`, `release`, `requeue`, `expire`, `delete`; up to 1000 IDs). |
| `POST /v1/queue/flush` | `write` | Flush the entire queue (`postqueue -f`). |
| `GET /v1/health` | `read` | `OPERATIONAL` / `DEGRADED`, indexer lag, spool gaps, lost spool records, and helper status. |

---

## Quick Start: Drop-In Replacement for `boky/postfix:v4.4.0`

Swap `boky/postfix:v4.4.0` for `ghcr.io/pretorianx/postfix-http-api:boky-v4.4.0`. Every existing `boky/postfix` variable (`HOSTNAME`, `RELAYHOST*`, `ALLOWED_SENDER_DOMAINS`, `POSTFIX_*`, `DKIM_*`, `SASL_*`, `TZ`), volume, and `/docker-init.db/*.sh` hook continues to work unchanged:

```diff
 services:
   postfix:
-    image: boky/postfix:v4.4.0
+    image: ghcr.io/pretorianx/postfix-http-api:boky-v4.4.0
     ports:
       - "587:587"
+      - "127.0.0.1:8025:8025"
     environment:
       HOSTNAME: mail.example.com
       RELAYHOST: "[smtp.relay.example.com]:587"
       ALLOWED_SENDER_DOMAINS: "example.com"
+      POSTFIXAPI_ALLOW_PLAINTEXT: "true"
+      POSTFIXAPI_TOKEN_SHA256: "<64-char-hex-sha256>"
```

Generate a token hash with:

```bash
docker run --rm ghcr.io/pretorianx/postfix-http-api:boky-v4.4.0 \
  postfix-api token new --name my-app --scopes read,write,content
```

See [docker/compose.example.yml](docker/compose.example.yml) for a full Docker Compose setup.

### Container Configuration Precedence

`/docker-init.db/99-postfix-api.sh` runs last during `boky/postfix` startup (after `boky` applies all `POSTFIX_*` settings and before `supervisord` starts Postfix):

1. **Explicit config file:** `POSTFIXAPI_CONFIG_FILE` (or `POSTFIXAPI_CONFIG`) copied to `/etc/postfix/postfix-api.conf` (`0600`).
2. **Mounted config file:** Pre-existing `/etc/postfix/postfix-api.conf` (supports read-only Docker/Kubernetes secrets with mode `0400`/`0600`).
3. **Environment variables:** Materializes `/etc/postfix/postfix-api.conf` (`0600`) from `POSTFIXAPI_*` variables (this prefix avoids colliding with `boky/postfix`'s `POSTFIX_*` → `main.cf` mapper):

| Environment Variable | Default | Description |
|---|---|---|
| `POSTFIXAPI_LISTEN` | `0.0.0.0:8025` | `master.cf` inet listen address (`addr:port`). |
| `POSTFIXAPI_MAILLOG_FILE` | `/dev/stdout` (if unset in `main.cf`) | Target `maillog_file` path. |
| `POSTFIXAPI_TOKEN_SHA256` / `POSTFIXAPI_TOKEN_HASH` | — | Pre-hashed 64-char hex SHA-256 of the Bearer token (recommended). |
| `POSTFIXAPI_TOKEN` / `POSTFIXAPI_TOKEN_FILE` | — | Raw token (or secret file path); hashed via `sha256sum` during init and unset before Postfix starts. |
| `POSTFIXAPI_TOKEN_NAME` | `default` | Token name recorded in write-action audit logs. |
| `POSTFIXAPI_TOKEN_SCOPES` | `read,write,content` | Comma-separated scopes for the generated token. |
| `POSTFIXAPI_ALLOW_PLAINTEXT` | `false` | Must be `true` to serve HTTP without TLS on non-loopback addresses. |
| `POSTFIXAPI_SUBJECT_IN_OUTPUT` | `false` | Emit `Subject:` `header_checks` lines to `maillog_file`. |
| `POSTFIXAPI_RETENTION_DURATION` / `POSTFIXAPI_RETENTION` | `168h` | Retention window after a message leaves the queue. |
| `POSTFIXAPI_TLS_CERT_FILE` / `POSTFIXAPI_TLS_KEY_FILE` | `""` | PEM certificate and key paths for native HTTPS. |
| `POSTFIXAPI_STATE_DIR` | `<data_directory>/postfix-api` | Directory for SQLite `state.db` and disk spool. |

---

## Bare-Host Installation (Postfix `>= 3.5`)

1. Download the static binary (`postfix-api-linux-amd64` or `postfix-api-linux-arm64`) from GitHub Releases to `/usr/local/bin/postfix-api`.
2. Ensure Postfix has `maillog_file` enabled (e.g. `sudo postconf -e "maillog_file = /var/log/postfix.log"`).
3. Install idempotently into `master.cf` and `main.cf`:
   ```bash
   sudo postfix-api install --listen 127.0.0.1:8025
   ```
4. Generate a token and paste the printed YAML block into `/etc/postfix/postfix-api.conf` (`0600`, `root:root`):
   ```bash
   postfix-api token new --name operator --scopes read,write,content
   ```
5. Reload Postfix: `sudo postfix reload`.

To restore the original `master.cf` and `main.cf` settings (keeping `<state_dir>` and `postfix-api.conf` intact):

```bash
sudo postfix-api uninstall
```

---

## Security Model

- **Deny by default:** All endpoints reject requests with `401` until at least one token hash is configured. Token comparison uses constant-time SHA-256 matching.
- **Transport guard:** Binding a non-loopback address without TLS refuses to start unless `allow_plaintext: true` is explicitly set (e.g., behind a TLS reverse proxy or service mesh).
- **Privilege separation:** Both `master.cf` services drop privileges to `mail_owner` after opening root-only resources (`maillog_file`, TLS key, config). Only the isolated `_root-helper` child retains root, and it only executes `postsuper` on strictly validated queue IDs (`^[A-Za-z0-9]{6,32}$`).
- **Audit logging:** Every write action logs the token name, action, queue IDs, and exit code.

---

## Development, Testing & Specification

Requires Go `1.27.4` and Docker (for real-Postfix integration tests).

```bash
make lint                                # gofmt check + go vet
make test                                # go test -race -cover ./...
make build                               # build local binary into bin/postfix-api
make build-linux                         # build static linux/amd64 and linux/arm64 into dist/
make docker-boky                         # build docker/boky/Dockerfile locally
make test-integration TARGET=boky        # run Docker integration tests (debian12|alpine|boky|latest)
```

### Real-Postfix Test Matrix

CI runs unit/race tests plus end-to-end Docker integration tests (including 3× `SIGKILL` of the log receiver during a 32,000-record flood, `SIGSTOP`/`SIGCONT` backpressure, stopped-indexer memory & spool-cap checks, subject redaction, reload/stop, auth/scopes, and `boky/postfix:v4.4.0` drop-in compatibility) across:

- [docker/test/Dockerfile.debian12](docker/test/Dockerfile.debian12) — Debian 12 (`bookworm-slim`), stock Postfix (`glibc`)
- [docker/test/Dockerfile.alpine](docker/test/Dockerfile.alpine) — Alpine `3.24`, stock Postfix (`musl`)
- [docker/test/Dockerfile.latest](docker/test/Dockerfile.latest) — Postfix `3.11.7` compiled from source on Debian 12
- [docker/boky/Dockerfile](docker/boky/Dockerfile) — Drop-in `boky/postfix:v4.4.0` production image

For full protocol details, SQLite DDL, wire formats, and design trade-offs, see [docs/SPEC.md](docs/SPEC.md).
