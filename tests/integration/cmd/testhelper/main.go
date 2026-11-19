// Command testhelper provides portable in-container utilities (AF_UNIX SOCK_DGRAM
// flood writer and minimal RFC 5321 SMTP sink) for Docker integration tests.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: testhelper <flood|smtp-sink|http-get> [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "flood":
		if err := runFlood(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "flood: %v\n", err)
			os.Exit(1)
		}
	case "smtp-sink":
		if err := runSMTPSink(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "smtp-sink: %v\n", err)
			os.Exit(1)
		}
	case "http-get":
		if err := runHTTPGet(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "http-get: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		os.Exit(2)
	}
}

func runFlood(args []string) error {
	fs := flag.NewFlagSet("flood", flag.ContinueOnError)
	sockPath := fs.String("socket", "/var/spool/postfix/public/postlog", "unix-dgram socket path")
	count := fs.Int("count", 1, "number of datagrams to send")
	prefix := fs.String("prefix", "FLOOD-SEQ-", "sequence marker prefix")
	width := fs.Int("width", 5, "zero-padded sequence width")
	padBytes := fs.Int("pad-bytes", 0, "optional trailing X padding bytes per datagram")
	paceEvery := fs.Int("pace-every", 0, "sleep every N datagrams (0 disables)")
	paceMs := fs.Int("pace-ms", 0, "sleep duration in milliseconds")
	progressFile := fs.String("progress-file", "", "optional file updated with sent count")
	if err := fs.Parse(args); err != nil {
		return err
	}

	fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_DGRAM, 0)
	if err != nil {
		return fmt.Errorf("socket: %w", err)
	}
	defer syscall.Close(fd)

	if err := syscall.Connect(fd, &syscall.SockaddrUnix{Name: *sockPath}); err != nil {
		return fmt.Errorf("connect %q: %w", *sockPath, err)
	}

	pad := ""
	if *padBytes > 0 {
		pad = " " + strings.Repeat("X", *padBytes)
	}
	format := fmt.Sprintf("postfix/testflood[99999]: %%s%%0%dd%%s", *width)

	for i := 1; i <= *count; i++ {
		msg := []byte(fmt.Sprintf(format, *prefix, i, pad))
		if err := writeBlocking(fd, msg); err != nil {
			return fmt.Errorf("write #%d: %w", i, err)
		}
		if *progressFile != "" && (i%250 == 0 || i == *count) {
			writeProgress(*progressFile, i)
		}
		if *paceEvery > 0 && *paceMs > 0 && i%*paceEvery == 0 {
			time.Sleep(time.Duration(*paceMs) * time.Millisecond)
		}
	}
	fmt.Printf("DONE %d\n", *count)
	return nil
}

func writeBlocking(fd int, msg []byte) error {
	for {
		_, err := syscall.Write(fd, msg)
		if err == nil {
			return nil
		}
		if err == syscall.EINTR {
			continue
		}
		if err == syscall.EAGAIN || err == syscall.ENOBUFS {
			time.Sleep(time.Millisecond)
			continue
		}
		return err
	}
}

func writeProgress(path string, count int) {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.Itoa(count)), 0o644); err == nil {
		_ = os.Rename(tmp, path)
	}
}

func runSMTPSink(args []string) error {
	fs := flag.NewFlagSet("smtp-sink", flag.ContinueOnError)
	listenAddr := fs.String("listen", "127.0.0.1:2525", "TCP listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ln, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		return fmt.Errorf("listen %q: %w", *listenAddr, err)
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go handleSMTPConn(conn)
	}
}

func handleSMTPConn(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	writeLine := func(s string) bool {
		if _, err := w.WriteString(s + "\r\n"); err != nil {
			return false
		}
		return w.Flush() == nil
	}

	if !writeLine("220 localhost ESMTP test-sink") {
		return
	}

	inData := false
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				return
			}
			return
		}
		trimmed := strings.TrimRight(line, "\r\n")
		if inData {
			if trimmed == "." {
				inData = false
				if !writeLine("250 2.0.0 Ok: queued as TESTSINK") {
					return
				}
			}
			continue
		}

		upper := strings.ToUpper(trimmed)
		switch {
		case strings.HasPrefix(upper, "EHLO"):
			if !writeLine("250-localhost") || !writeLine("250 SIZE 52428800") {
				return
			}
		case strings.HasPrefix(upper, "HELO"):
			if !writeLine("250 localhost") {
				return
			}
		case strings.HasPrefix(upper, "MAIL FROM:"):
			if !writeLine("250 2.1.0 Ok") {
				return
			}
		case strings.HasPrefix(upper, "RCPT TO:"):
			if !writeLine("250 2.1.5 Ok") {
				return
			}
		case upper == "DATA":
			inData = true
			if !writeLine("354 End data with <CR><LF>.<CR><LF>") {
				return
			}
		case upper == "RSET" || upper == "NOOP":
			if !writeLine("250 2.0.0 Ok") {
				return
			}
		case upper == "QUIT":
			_ = writeLine("221 2.0.0 Bye")
			return
		default:
			if !writeLine("502 5.5.2 Command not recognized") {
				return
			}
		}
	}
}

func runHTTPGet(args []string) error {
	fs := flag.NewFlagSet("http-get", flag.ContinueOnError)
	targetURL := fs.String("url", "", "URL to fetch")
	token := fs.String("token", "", "Bearer token")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *targetURL == "" {
		return fmt.Errorf("--url is required")
	}
	req, err := http.NewRequest(http.MethodGet, *targetURL, nil)
	if err != nil {
		return err
	}
	if *token != "" {
		req.Header.Set("Authorization", "Bearer "+*token)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("%d\n%s\n", resp.StatusCode, strings.TrimSpace(string(body)))
	if resp.StatusCode >= 400 {
		return fmt.Errorf("unexpected HTTP status %d", resp.StatusCode)
	}
	return nil
}
