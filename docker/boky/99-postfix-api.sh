#!/bin/sh
# Post-init hook for boky/postfix:v4.4.0 drop-in compatibility.
# Note: boky/postfix's /scripts/run.sh *sources* /docker-init.db/*.sh, so all work
# runs inside a subshell to avoid leaking shell options (e.g. `set -u`) or variables
# into /scripts/run.sh.
#
# Use the `POSTFIXAPI_*` prefix (recommended) rather than `POSTFIX_API_*`, because
# boky/postfix maps every `POSTFIX_*` env var into /etc/postfix/main.cf. If legacy
# `POSTFIX_API_*` vars are provided, this hook still reads them and strips any
# `API_*` parameters boky wrote to main.cf.

(
set -eu

# Remove any API_* parameters that boky's postfix_custom_commands may have written to main.cf
# if the operator passed legacy POSTFIX_API_* env vars.
postconf -X \
    API_LISTEN API_CONFIG API_CONFIG_FILE API_MAILLOG_FILE \
    API_TOKEN API_TOKEN_FILE API_TOKEN_HASH API_TOKEN_SHA256 API_TOKEN_NAME API_TOKEN_SCOPES \
    API_ALLOW_PLAINTEXT API_SUBJECT_IN_OUTPUT API_RETENTION API_RETENTION_DURATION \
    API_STATE_DIR API_SPOOL_SEGMENT_SIZE_BYTES API_SPOOL_MAX_TOTAL_BYTES \
    API_QUEUE_SYNC_INTERVAL API_INDEXER_BATCH_SIZE API_TLS_CERT_FILE API_TLS_KEY_FILE \
    2>/dev/null || true

CONFIG_DIR="$(postconf -h config_directory 2>/dev/null || echo /etc/postfix)"
CONF_PATH="${CONFIG_DIR}/postfix-api.conf"
LISTEN_ADDR="${POSTFIXAPI_LISTEN:-${POSTFIX_API_LISTEN:-0.0.0.0:8025}}"
CONFIG_SRC="${POSTFIXAPI_CONFIG_FILE:-${POSTFIXAPI_CONFIG:-${POSTFIX_API_CONFIG_FILE:-${POSTFIX_API_CONFIG:-}}}}"
MAILLOG_OVERRIDE="${POSTFIXAPI_MAILLOG_FILE:-${POSTFIX_API_MAILLOG_FILE:-}}"

# 1. Ensure maillog_file is configured so postfix-api log receiver can replace postlogd.
if [ -n "${MAILLOG_OVERRIDE}" ]; then
    postconf -e "maillog_file = ${MAILLOG_OVERRIDE}"
else
    CURRENT_MAILLOG="$(postconf -h maillog_file 2>/dev/null || true)"
    if [ -z "${CURRENT_MAILLOG}" ]; then
        postconf -e "maillog_file = /dev/stdout"
    fi
fi

# Track whether the operator already mounted/provided postfix-api.conf before install.
PREEXISTING_CONF=0
if [ -f "${CONF_PATH}" ] && [ -s "${CONF_PATH}" ]; then
    PREEXISTING_CONF=1
fi

# 2. Idempotently install postfix-api into Postfix master.cf / main.cf.
/usr/local/bin/postfix-api install --listen "${LISTEN_ADDR}"

# Resolve config env vars (prefer POSTFIXAPI_*, fall back to POSTFIX_API_*).
V_TOKEN="${POSTFIXAPI_TOKEN:-${POSTFIX_API_TOKEN:-}}"
V_TOKEN_FILE="${POSTFIXAPI_TOKEN_FILE:-${POSTFIX_API_TOKEN_FILE:-}}"
V_TOKEN_HASH="${POSTFIXAPI_TOKEN_HASH:-${POSTFIX_API_TOKEN_HASH:-}}"
V_TOKEN_SHA256="${POSTFIXAPI_TOKEN_SHA256:-${POSTFIX_API_TOKEN_SHA256:-}}"
V_ALLOW_PLAINTEXT="${POSTFIXAPI_ALLOW_PLAINTEXT:-${POSTFIX_API_ALLOW_PLAINTEXT:-}}"
V_SUBJECT_IN_OUTPUT="${POSTFIXAPI_SUBJECT_IN_OUTPUT:-${POSTFIX_API_SUBJECT_IN_OUTPUT:-}}"
V_RETENTION="${POSTFIXAPI_RETENTION_DURATION:-${POSTFIXAPI_RETENTION:-${POSTFIX_API_RETENTION_DURATION:-${POSTFIX_API_RETENTION:-}}}}"
V_STATE_DIR="${POSTFIXAPI_STATE_DIR:-${POSTFIX_API_STATE_DIR:-}}"
V_SPOOL_SEG="${POSTFIXAPI_SPOOL_SEGMENT_SIZE_BYTES:-${POSTFIX_API_SPOOL_SEGMENT_SIZE_BYTES:-}}"
V_SPOOL_MAX="${POSTFIXAPI_SPOOL_MAX_TOTAL_BYTES:-${POSTFIX_API_SPOOL_MAX_TOTAL_BYTES:-}}"
V_QUEUE_SYNC="${POSTFIXAPI_QUEUE_SYNC_INTERVAL:-${POSTFIX_API_QUEUE_SYNC_INTERVAL:-}}"
V_BATCH_SIZE="${POSTFIXAPI_INDEXER_BATCH_SIZE:-${POSTFIX_API_INDEXER_BATCH_SIZE:-}}"
V_TLS_CERT="${POSTFIXAPI_TLS_CERT_FILE:-${POSTFIX_API_TLS_CERT_FILE:-}}"
V_TLS_KEY="${POSTFIXAPI_TLS_KEY_FILE:-${POSTFIX_API_TLS_KEY_FILE:-}}"

# 3. Provision /etc/postfix/postfix-api.conf.
if [ -n "${CONFIG_SRC}" ]; then
    if [ ! -r "${CONFIG_SRC}" ]; then
        echo "99-postfix-api.sh: ERROR: config file not readable: ${CONFIG_SRC}" >&2
        exit 1
    fi
    if [ "${CONFIG_SRC}" != "${CONF_PATH}" ]; then
        umask 077
        cp "${CONFIG_SRC}" "${CONF_PATH}.tmp"
        chown root:root "${CONF_PATH}.tmp" 2>/dev/null || true
        chmod 0600 "${CONF_PATH}.tmp"
        mv "${CONF_PATH}.tmp" "${CONF_PATH}"
    else
        chown root:root "${CONF_PATH}" 2>/dev/null || true
        chmod 0600 "${CONF_PATH}" 2>/dev/null || true
    fi
elif [ "${PREEXISTING_CONF}" -eq 1 ]; then
    chown root:root "${CONF_PATH}" 2>/dev/null || true
    chmod 0600 "${CONF_PATH}" 2>/dev/null || true
elif [ -n "${V_TOKEN}" ] \
  || [ -n "${V_TOKEN_FILE}" ] \
  || [ -n "${V_TOKEN_HASH}" ] \
  || [ -n "${V_TOKEN_SHA256}" ] \
  || [ -n "${V_ALLOW_PLAINTEXT}" ] \
  || [ -n "${V_SUBJECT_IN_OUTPUT}" ] \
  || [ -n "${V_RETENTION}" ] \
  || [ -n "${V_STATE_DIR}" ] \
  || [ -n "${V_SPOOL_SEG}" ] \
  || [ -n "${V_SPOOL_MAX}" ] \
  || [ -n "${V_QUEUE_SYNC}" ] \
  || [ -n "${V_BATCH_SIZE}" ] \
  || [ -n "${V_TLS_CERT}" ] \
  || [ -n "${V_TLS_KEY}" ]; then

    STATE_DIR="${V_STATE_DIR}"
    SPOOL_SEG_BYTES="${V_SPOOL_SEG:-16777216}"
    SPOOL_MAX_BYTES="${V_SPOOL_MAX:-268435456}"
    RETENTION_DURATION="${V_RETENTION:-168h}"
    QUEUE_SYNC_INTERVAL="${V_QUEUE_SYNC:-5m}"
    INDEXER_BATCH_SIZE="${V_BATCH_SIZE:-1000}"
    SUBJECT_IN_OUTPUT="${V_SUBJECT_IN_OUTPUT:-false}"
    TLS_CERT_FILE="${V_TLS_CERT}"
    TLS_KEY_FILE="${V_TLS_KEY}"
    ALLOW_PLAINTEXT="${V_ALLOW_PLAINTEXT:-false}"
    TOKEN_NAME="${POSTFIXAPI_TOKEN_NAME:-${POSTFIX_API_TOKEN_NAME:-default}}"
    TOKEN_SCOPES="${POSTFIXAPI_TOKEN_SCOPES:-${POSTFIX_API_TOKEN_SCOPES:-read,write,content}}"

    TOKEN_SHA256=""
    if [ -n "${V_TOKEN_SHA256}" ]; then
        TOKEN_SHA256="$(printf '%s' "${V_TOKEN_SHA256}" | tr 'A-F' 'a-f')"
    elif [ -n "${V_TOKEN_HASH}" ]; then
        TOKEN_SHA256="$(printf '%s' "${V_TOKEN_HASH}" | tr 'A-F' 'a-f')"
    elif [ -n "${V_TOKEN_FILE}" ]; then
        if [ ! -r "${V_TOKEN_FILE}" ]; then
            echo "99-postfix-api.sh: ERROR: token file not readable: ${V_TOKEN_FILE}" >&2
            exit 1
        fi
        RAW_TOKEN="$(tr -d '\r\n' < "${V_TOKEN_FILE}")"
        if [ -n "${RAW_TOKEN}" ]; then
            TOKEN_SHA256="$(printf '%s' "${RAW_TOKEN}" | sha256sum | cut -d' ' -f1)"
        fi
        unset RAW_TOKEN
    elif [ -n "${V_TOKEN}" ]; then
        TOKEN_SHA256="$(printf '%s' "${V_TOKEN}" | sha256sum | cut -d' ' -f1)"
    fi

    umask 077
    TMP_CONF="${CONF_PATH}.tmp"
    {
        printf 'state_dir: "%s"\n' "${STATE_DIR}"
        printf 'spool:\n'
        printf '  segment_size_bytes: %s\n' "${SPOOL_SEG_BYTES}"
        printf '  max_total_bytes: %s\n' "${SPOOL_MAX_BYTES}"
        printf 'retention_duration: %s\n' "${RETENTION_DURATION}"
        printf 'queue_sync_interval: %s\n' "${QUEUE_SYNC_INTERVAL}"
        printf 'indexer_batch_size: %s\n' "${INDEXER_BATCH_SIZE}"
        printf 'subject_in_output: %s\n' "${SUBJECT_IN_OUTPUT}"
        printf 'tls:\n'
        printf '  cert_file: "%s"\n' "${TLS_CERT_FILE}"
        printf '  key_file: "%s"\n' "${TLS_KEY_FILE}"
        printf 'allow_plaintext: %s\n' "${ALLOW_PLAINTEXT}"
        if [ -n "${TOKEN_SHA256}" ]; then
            printf 'tokens:\n'
            printf '  - name: "%s"\n' "${TOKEN_NAME}"
            printf '    sha256: "%s"\n' "${TOKEN_SHA256}"
            printf '    scopes:\n'
            OLD_IFS="${IFS}"
            IFS=','
            for scope in ${TOKEN_SCOPES}; do
                trimmed="$(printf '%s' "${scope}" | tr -d '[:space:]')"
                if [ -n "${trimmed}" ]; then
                    printf '      - %s\n' "${trimmed}"
                fi
            done
            IFS="${OLD_IFS}"
        else
            printf 'tokens: []\n'
        fi
    } > "${TMP_CONF}"

    chown root:root "${TMP_CONF}"
    chmod 0600 "${TMP_CONF}"
    mv "${TMP_CONF}" "${CONF_PATH}"
fi

postfix check
)

# Clear raw secrets from parent shell environment after materializing hashed config on disk.
unset POSTFIXAPI_TOKEN POSTFIX_API_TOKEN 2>/dev/null || true
