# `postfix-api` (`postfix-http-api`)

Self-contained HTTP observability and queue-management daemon for Postfix `>= 3.5`, packaged as a single static Linux binary (`CGO_ENABLED=0`, `linux/amd64` and `linux/arm64`) and a drop-in [`boky/postfix:v4.4.0`](docker/boky/Dockerfile) container image.

## Key Capabilities

- **Zero External Dependencies:** Single static Go binary using embedded SQLite (WAL mode via `modernc.org/sqlite`), supervised directly by Postfix `master(8)` — no sidecars, no external database, no Postfix source patches.
- **Full Delivery Lifecycle Tracking:** Replaces `postlogd` on `master.cf` `postlog` (`unix-dgram`), forwarding logs to `maillog_file` (e.g. `/dev/stdout`) while buffering into a bounded disk spool indexed asynchronously into SQLite.
- **Subject Capture Without Log Leaks:** Uses a `header_checks` `INFO` rule (`/^Subject:/ INFO`) with RFC 2047 MIME decoding and strips Subject lines from `maillog_file` output by default (`subject_in_output: false`).
- **Authenticated HTTP API:** Scoped Bearer token authentication (`read`, `write`, `content`) with SHA-256 hashes stored in `0600` `root:root` YAML config, TLS enforcement on non-loopback addresses, and strict privilege separation (`mail_owner` for HTTP/log loops, isolated `_root-helper` pipe for `postsuper`).
- **Drop-In `boky/postfix:v4.4.0` Container Image:** Preserves `/scripts/run.sh`, `HOSTNAME`, `RELAYHOST*`, `ALLOWED_SENDER_DOMAINS`, `POSTFIX_*`, `DKIM_*`, `SASL_*`, and `/docker-init.db/` hooks for workloads such as `hide-mail`.

See [docs/SPEC.md](docs/SPEC.md) for the complete architecture, protocol, schema, and verification specification.

---

## Drop-In Migration from `boky/postfix:v4.4.0`

Replace the base image with `ghcr.io/pretorianx/postfix-http-api:boky-v4.4.0` — all existing `boky/postfix` environment variables, volumes, and `/docker-init.db/*.sh` hooks continue to work unchanged:

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

See [docker/compose.example.yml](docker/compose.example.yml) for a complete Docker Compose example.

### Container Configuration Precedence

`/docker-init.db/99-postfix-api.sh` runs last during `boky/postfix` initialization (after `boky` applies all `POSTFIX_*` settings and before `supervisord` starts `master`):

1. **Explicit config path:** `POSTFIXAPI_CONFIG_FILE` (or `POSTFIXAPI_CONFIG`) copied to `/etc/postfix/postfix-api.conf` (`0600`).
2. **Mounted config file:** Pre-existing `/etc/postfix/postfix-api.conf` (supports read-only Docker/Kubernetes secrets with mode `0400`/`0600`).
3. **Environment variables:** Materializes `/etc/postfix/postfix-api.conf` (`0600`) from `POSTFIXAPI_*` variables (using `POSTFIXAPI_*` avoids colliding with `boky/postfix`'s `POSTFIX_*` `main.cf` mapper):

| Environment Variable | Default | Description |
|---|---|---|
| `POSTFIXAPI_LISTEN` | `0.0.0.0:8025` | `master.cf` inet listen address (`addr:port`). |
| `POSTFIXAPI_MAILLOG_FILE` | `/dev/stdout` (if unset in `main.cf`) | Target `maillog_file` path. |
| `POSTFIXAPI_TOKEN_SHA256` / `POSTFIXAPI_TOKEN_HASH` | — | Pre-hashed 64-char hex SHA-256 of the Bearer token (recommended). |
| `POSTFIXAPI_TOKEN` / `POSTFIXAPI_TOKEN_FILE` | — | Raw token (or secret file path); hashed in memory via `sha256sum` and unset before Postfix starts. |
| `POSTFIXAPI_TOKEN_NAME` | `default` | Token name recorded in audit logs. |
| `POSTFIXAPI_TOKEN_SCOPES` | `read,write,content` | Comma-separated scopes for the generated token. |
| `POSTFIXAPI_ALLOW_PLAINTEXT` | `false` | Must be `true` to serve HTTP without TLS on non-loopback addresses. |
| `POSTFIXAPI_SUBJECT_IN_OUTPUT` | `false` | Emit `Subject:` `header_checks` lines to `maillog_file`. |
| `POSTFIXAPI_RETENTION_DURATION` / `POSTFIXAPI_RETENTION` | `168h` | Retention window after a message leaves the queue. |
| `POSTFIXAPI_TLS_CERT_FILE` / `POSTFIXAPI_TLS_KEY_FILE` | `""` | PEM certificate and key paths for native HTTPS. |
| `POSTFIXAPI_STATE_DIR` | `<data_directory>/postfix-api` | Directory for SQLite `state.db` and disk spool. |

---

## Bare-Host Installation (Postfix `>= 3.5`)

1. Download the static binary for your architecture (`postfix-api-linux-amd64` or `postfix-api-linux-arm64`) from GitHub Releases and place it at `/usr/local/bin/postfix-api`.
2. Ensure Postfix has `maillog_file` enabled (e.g. `postconf -e "maillog_file = /var/log/postfix.log"`).
3. Install idempotently into `master.cf` and `main.cf`:
   ```bash
   sudo postfix-api install --listen 127.0.0.1:8025
   ```
4. Generate an API token and add the printed YAML block to `/etc/postfix/postfix-api.conf` (`0600`, `root:root`):
   ```bash
   postfix-api token new --name operator --scopes read,write,content
   ```
5. Reload Postfix (`sudo postfix reload`).

To revert `master.cf` and `main.cf` to their pre-install state (preserving `<state_dir>` and `postfix-api.conf`):

```bash
sudo postfix-api uninstall
```

---

## HTTP API Summary

All endpoints require `Authorization: Bearer <token>`.

| Method & Path | Scope | Purpose |
|---|---|---|
| `GET /v1/messages` | `read` | Search messages (`queue_id`, `message_id`, `sender`, `recipient`, `status`, `from`, `to`, `limit`, `cursor`). |
| `GET /v1/messages/{queue_id}` | `read` | Full message details, per-recipient attempts, events, and live `next_retry_at`. |
| `GET /v1/messages/{queue_id}/content` | `content` | Queued headers and body via `postcat -hbq`. |
| `GET /v1/events` | `read` | Ordered event feed (`after`, `limit`) for downstream sync. |
| `GET /v1/queue` | `read` | Live queue snapshot and per-queue counts via `postqueue -j`. |
| `POST /v1/messages/{queue_id}/{hold,release,requeue,expire,flush}` | `write` | Single-message queue control. |
| `DELETE /v1/messages/{queue_id}` | `write` | Delete a queued message (`postsuper -d`). |
| `POST /v1/queue/actions` | `write` | Bulk queue action (`hold`, `release`, `requeue`, `expire`, `delete`; up to 1000 IDs). |
| `POST /v1/queue/flush` | `write` | Flush the entire queue (`postqueue -f`). |
| `GET /v1/health` | `read` | Health (`OPERATIONAL` / `DEGRADED`), indexer lag, spool gaps, and helper status. |

---

## Development & Testing

Requires Go `1.27.4` and Docker (for integration tests).

```bash
make lint                                # gofmt check + go vet
make test                                # go test -race -cover ./...
make build                               # build local binary into bin/postfix-api
make build-linux                         # build static linux/amd64 and linux/arm64 into dist/
make docker-boky                         # build docker/boky/Dockerfile locally
make test-integration TARGET=boky        # run Docker integration tests (debian12|alpine|boky|latest)
```

### Docker Test Matrix

- [docker/test/Dockerfile.debian12](docker/test/Dockerfile.debian12) — Debian 12 (`bookworm-slim`), stock Postfix (`glibc`)
- [docker/test/Dockerfile.alpine](docker/test/Dockerfile.alpine) — Alpine `3.24`, stock Postfix (`musl`)
- [docker/test/Dockerfile.latest](docker/test/Dockerfile.latest) — Postfix `3.11.7` compiled from source on Debian 12
- [docker/boky/Dockerfile](docker/boky/Dockerfile) — Drop-in `boky/postfix:v4.4.0` production image
