# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.27.4 AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /src

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/postfix-api ./cmd/postfix-api

FROM boky/postfix:v4.4.0

LABEL org.opencontainers.image.source="https://github.com/PretorianX/postfix-http-api" \
      org.opencontainers.image.description="Drop-in boky/postfix:v4.4.0 image with embedded postfix-api HTTP observability and queue management" \
      org.opencontainers.image.base.name="boky/postfix:v4.4.0"

COPY --from=builder /out/postfix-api /usr/local/bin/postfix-api
RUN chmod 0755 /usr/local/bin/postfix-api

# Patch [program:postfix] in /etc/supervisord.conf so maillog_file=/dev/stdout
# written by postfix-api's log receiver reaches container stdout without
# supervisord log rotation (ESPIPE) or stderr merging.
RUN awk ' \
    BEGIN { in_postfix = 0; found = 0 } \
    /^\[.*\]/ { \
        in_postfix = ($0 == "[program:postfix]"); \
        print; \
        if (in_postfix) { \
            found = 1; \
            print "stdout_logfile = /dev/stdout"; \
            print "stdout_logfile_maxbytes = 0"; \
            print "stderr_logfile = /dev/stderr"; \
            print "stderr_logfile_maxbytes = 0"; \
            print "redirect_stderr = false"; \
        } \
        next; \
    } \
    in_postfix && /^[[:space:]]*(stdout_logfile|stdout_logfile_maxbytes|stderr_logfile|stderr_logfile_maxbytes|redirect_stderr)[[:space:]]*=/ { \
        next; \
    } \
    { print } \
    END { if (!found) exit 1 } \
    ' /etc/supervisord.conf > /etc/supervisord.conf.tmp \
    && mv /etc/supervisord.conf.tmp /etc/supervisord.conf

COPY docker/boky/99-postfix-api.sh /docker-init.db/99-postfix-api.sh
RUN chmod 0755 /docker-init.db/99-postfix-api.sh

EXPOSE 587 8025
