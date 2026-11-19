package parser

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestParseLine_EnvelopeFormats(t *testing.T) {
	ts := time.Date(2026, 11, 18, 16, 54, 43, 123456789, time.UTC)

	tests := []struct {
		name     string
		line     string
		wantTS   time.Time
		wantProg string
		wantQID  string
		wantKind RecordKind
		wantOK   bool
	}{
		{
			name:     "spool postlogd without syslog prefix",
			line:     "2026-11-18T16:54:43.123456789Z postfix/smtpd[123]: 4jV74M63RlzHxK3: client=localhost[127.0.0.1]",
			wantTS:   ts,
			wantProg: "smtpd",
			wantQID:  "4jV74M63RlzHxK3",
			wantKind: KindSMTPDClient,
			wantOK:   true,
		},
		{
			name:     "spool debian syslog format",
			line:     "2026-11-18T16:54:43.123456789Z Nov 18 16:54:43 mail01 postfix/smtpd[456]: 74D321034: client=localhost[127.0.0.1]",
			wantTS:   ts,
			wantProg: "smtpd",
			wantQID:  "74D321034",
			wantKind: KindSMTPDClient,
			wantOK:   true,
		},
		{
			name:     "raw syslog submission multi-level tag single-digit day",
			line:     "Nov  8 04:05:06 mail.example.com postfix/submission/smtpd[789]: C23D82B60024: client=client.example[192.0.2.10]",
			wantProg: "smtpd",
			wantQID:  "C23D82B60024",
			wantKind: KindSMTPDClient,
			wantOK:   true,
		},
		{
			name:     "raw alpine busybox syslog facility.level",
			line:     "Nov 18 16:54:43 alpine-mx mail.info postfix/qmgr[12]: 4jV74M63RlzHxK3: from=<a@example.com>, size=310, nrcpt=1 (queue active)",
			wantProg: "qmgr",
			wantQID:  "4jV74M63RlzHxK3",
			wantKind: KindQmgrActive,
			wantOK:   true,
		},
		{
			name:     "raw syslog without hostname",
			line:     "Nov 18 16:54:43 postfix/qmgr[12]: 4jV74M63RlzHxK3: removed",
			wantProg: "qmgr",
			wantQID:  "4jV74M63RlzHxK3",
			wantKind: KindQmgrRemoved,
			wantOK:   true,
		},
		{
			name:     "raw postlogd bare postsuper",
			line:     "postsuper[99]: 4jV74M63RlzHxK3: placed on hold",
			wantProg: "postsuper",
			wantQID:  "4jV74M63RlzHxK3",
			wantKind: KindPostsuperHold,
			wantOK:   true,
		},
		{
			name:     "RFC5424 timestamp syslog",
			line:     "2026-11-18T16:54:43.123456789Z mail01 postfix/cleanup[55]: 4jV74M63RlzHxK3: message-id=<abc@mail01>",
			wantTS:   ts,
			wantProg: "cleanup",
			wantQID:  "4jV74M63RlzHxK3",
			wantKind: KindMessageID,
			wantOK:   true,
		},
		{
			name:   "bad leading token rejected",
			line:   "not-a-ts postfix/smtpd[123]: 4jV74M63RlzHxK3: client=localhost[127.0.0.1]",
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec, ok := ParseLine(tc.line)
			if ok != tc.wantOK {
				t.Fatalf("ParseLine ok=%v, want %v (rec=%+v)", ok, tc.wantOK, rec)
			}
			if !tc.wantOK {
				return
			}
			if !tc.wantTS.IsZero() && !rec.Timestamp.Equal(tc.wantTS) {
				t.Errorf("Timestamp=%v, want %v", rec.Timestamp, tc.wantTS)
			}
			if rec.Program != tc.wantProg {
				t.Errorf("Program=%q, want %q", rec.Program, tc.wantProg)
			}
			if rec.QueueID != tc.wantQID {
				t.Errorf("QueueID=%q, want %q", rec.QueueID, tc.wantQID)
			}
			if rec.Kind != tc.wantKind {
				t.Errorf("Kind=%q, want %q", rec.Kind, tc.wantKind)
			}
		})
	}
}

func TestParseLine_SMTPD(t *testing.T) {
	t.Run("ipv4 without sasl", func(t *testing.T) {
		rec, ok := ParseLine("postfix/smtpd[10]: 4jV74M63RlzHxK3: client=localhost[127.0.0.1]")
		if !ok {
			t.Fatal("expected match")
		}
		if rec.ClientName != "localhost" || rec.ClientAddress != "127.0.0.1" || rec.SASLUsername != "" {
			t.Fatalf("unexpected record: %+v", rec)
		}
	})

	t.Run("ipv6 with sasl", func(t *testing.T) {
		rec, ok := ParseLine("postfix/submission/smtpd[10]: 4jV74M63RlzHxK3: client=sender.example.com[2001:db8:1::42], sasl_method=PLAIN, sasl_username=alice@example.com")
		if !ok {
			t.Fatal("expected match")
		}
		if rec.ClientName != "sender.example.com" || rec.ClientAddress != "2001:db8:1::42" || rec.SASLUsername != "alice@example.com" {
			t.Fatalf("unexpected record: %+v", rec)
		}
	})

	t.Run("unknown client name and address", func(t *testing.T) {
		rec, ok := ParseLine("postfix/smtpd[10]: 4jV74M63RlzHxK3: client=unknown[unknown]")
		if !ok {
			t.Fatal("expected match")
		}
		if rec.ClientName != "unknown" || rec.ClientAddress != "unknown" {
			t.Fatalf("unexpected record: %+v", rec)
		}
	})
}

func TestParseLine_PickupAndCleanup(t *testing.T) {
	t.Run("pickup normal sender", func(t *testing.T) {
		rec, ok := ParseLine("postfix/pickup[12]: 4jV74M63RlzHxK3: uid=1000 from=<alice@example.com>")
		if !ok || rec.Kind != KindPickup || !rec.HasSender || rec.Sender != "alice@example.com" {
			t.Fatalf("unexpected record: ok=%v rec=%+v", ok, rec)
		}
	})

	t.Run("pickup empty bounce sender", func(t *testing.T) {
		rec, ok := ParseLine("postfix/pickup[12]: 4jV74M63RlzHxK3: uid=0 from=<>")
		if !ok || rec.Kind != KindPickup || !rec.HasSender || rec.Sender != "" {
			t.Fatalf("unexpected record: ok=%v rec=%+v", ok, rec)
		}
	})

	t.Run("cleanup message-id", func(t *testing.T) {
		rec, ok := ParseLine("postfix/cleanup[15]: 4jV74M63RlzHxK3: message-id=<4jV74M63RlzHxK3@localhost>  ")
		if !ok || rec.Kind != KindMessageID || rec.MessageID != "<4jV74M63RlzHxK3@localhost>" {
			t.Fatalf("unexpected record: ok=%v rec=%+v", ok, rec)
		}
	})

	t.Run("cleanup subject containing from", func(t *testing.T) {
		line := "2026-11-18T16:54:43.100000000Z postfix/cleanup[15]: 4jV74M63RlzHxK3: info: header Subject: Report from Paris from Alice from mail.example.com[192.0.2.1]; from=<a@example.com> to=<b@example.org> proto=ESMTP helo=<mail.example.com>"
		rec, ok := ParseLine(line)
		if !ok || rec.Kind != KindSubject {
			t.Fatalf("unexpected result: ok=%v rec=%+v", ok, rec)
		}
		if rec.Subject != "Report from Paris from Alice" {
			t.Fatalf("Subject=%q, want %q", rec.Subject, "Report from Paris from Alice")
		}
	})

	t.Run("cleanup subject containing semicolon from=<", func(t *testing.T) {
		line := "postfix/cleanup[15]: 4jV74M63RlzHxK3: info: header Subject: Hello; from=<spoof@evil> world from localhost[127.0.0.1]; from=<a@example.com> to=<b@example.org> proto=ESMTP"
		rec, ok := ParseLine(line)
		if !ok || rec.Subject != "Hello; from=<spoof@evil> world" {
			t.Fatalf("Subject=%q, ok=%v", rec.Subject, ok)
		}
	})

	t.Run("cleanup subject from local pickup", func(t *testing.T) {
		line := "postfix/cleanup[15]: 4jV74M63RlzHxK3: info: header Subject: Cron output from host from local; uid=0 from=<root@localhost>"
		rec, ok := ParseLine(line)
		if !ok || rec.Subject != "Cron output from host" {
			t.Fatalf("Subject=%q, ok=%v", rec.Subject, ok)
		}
	})

	t.Run("cleanup case-insensitive subject header name", func(t *testing.T) {
		line := "postfix/cleanup[15]: 4jV74M63RlzHxK3: info: header SUBJECT: Alert from localhost[127.0.0.1]; from=<a@b> to=<c@d>"
		rec, ok := ParseLine(line)
		if !ok || rec.Subject != "Alert" {
			t.Fatalf("Subject=%q, ok=%v", rec.Subject, ok)
		}
	})

	t.Run("cleanup subject utf8 base64 Privet order 42", func(t *testing.T) {
		line := "postfix/cleanup[15]: 4jV74M63RlzHxK3: info: header Subject: =?UTF-8?B?0J/RgNC40LLQtdGC?= order 42 from localhost[127.0.0.1]; from=<a@example.com> to=<b@example.org> proto=ESMTP helo=<localhost>"
		rec, ok := ParseLine(line)
		if !ok || rec.Subject != "Привет order 42" {
			t.Fatalf("Subject=%q, ok=%v", rec.Subject, ok)
		}
	})
}

func TestDecodeSubject_MIMEEncodingsAndTruncation(t *testing.T) {
	// Helper to encode single-byte bytes into base64 encoded-word.
	bWord := func(charset string, data []byte) string {
		return "=?" + charset + "?B?" + base64.StdEncoding.EncodeToString(data) + "?="
	}

	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "plain ascii",
			raw:  "Order #42 confirmation",
			want: "Order #42 confirmation",
		},
		{
			name: "utf8 base64 Privet order 42",
			raw:  "=?UTF-8?B?0J/RgNC40LLQtdGC?= order 42",
			want: "Привет order 42",
		},
		{
			name: "utf8 q-encoding Privet order 42",
			raw:  "=?utf-8?Q?=D0=9F=D1=80=D0=B8=D0=B2=D0=B5=D1=82_order_42?=",
			want: "Привет order 42",
		},
		{
			name: "adjacent encoded words drop intervening space",
			raw:  "=?UTF-8?B?0J/RgNC4?= =?UTF-8?B?0LLQtdGC?= order 42",
			want: "Привет order 42",
		},
		{
			name: "iso-8859-1",
			raw:  "=?ISO-8859-1?Q?Caf=E9_cr=E8me?=",
			want: "Café crème",
		},
		{
			name: "windows-1251",
			// "Привет" in CP1251: CF F0 E8 E2 E5 F2
			raw:  bWord("windows-1251", []byte{0xCF, 0xF0, 0xE8, 0xE2, 0xE5, 0xF2}) + " order 42",
			want: "Привет order 42",
		},
		{
			name: "windows-1252 euro and smart quotes",
			raw:  bWord("windows-1252", []byte{0x80, ' ', 0x93, 'o', 'k', 0x94}),
			want: "€ “ok”",
		},
		{
			name: "koi8-r",
			// "Привет" in KOI8-R: F0 D2 C9 D7 C5 D4
			raw:  bWord("koi8-r", []byte{0xF0, 0xD2, 0xC9, 0xD7, 0xC5, 0xD4}) + " order 42",
			want: "Привет order 42",
		},
		{
			name: "us-ascii",
			raw:  "=?US-ASCII?Q?Hello_World?=",
			want: "Hello World",
		},
		{
			name: "200-byte truncated trailing encoded word preserves decoded prefix and raw tail",
			raw:  "=?UTF-8?B?0J/RgNC40LLQtdGC?= " + strings.Repeat("A", 140) + " =?UTF-8?B?0J/RgNC4",
			want: "Привет " + strings.Repeat("A", 140) + " =?UTF-8?B?0J/RgNC4",
		},
		{
			name: "unsupported charset followed by valid word and truncated tail",
			raw:  "=?UTF-8?B?0J/RgNC40LLQtdGC?= =?ISO-2022-JP?B?GyRC?= =?UTF-8?B?dGVzdA==?= =?UTF-8?B?broken",
			want: "Привет =?ISO-2022-JP?B?GyRC?= test =?UTF-8?B?broken",
		},
		{
			name: "invalid utf-8 bytes inside encoded word replaced safely",
			raw:  "=?UTF-8?Q?=FF=FE_test?=",
			want: "\uFFFD test",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := DecodeSubject(tc.raw)
			if !utf8.ValidString(got) {
				t.Fatalf("decoded subject is not valid UTF-8: %q", got)
			}
			if got != tc.want {
				t.Errorf("DecodeSubject(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestParseLine_QmgrBouncePostsuperAndDelivery(t *testing.T) {
	t.Run("qmgr active with bounce empty sender", func(t *testing.T) {
		rec, ok := ParseLine("postfix/qmgr[22]: 4jV74P1234zHxK9: from=<>, size=1852, nrcpt=1 (queue active)")
		if !ok || rec.Kind != KindQmgrActive || !rec.HasSender || rec.Sender != "" || rec.Size != 1852 || rec.RecipientCount != 1 {
			t.Fatalf("unexpected record: ok=%v rec=%+v", ok, rec)
		}
	})

	t.Run("qmgr expired and force-expired", func(t *testing.T) {
		rec1, ok1 := ParseLine("postfix/qmgr[22]: 4jV74M63RlzHxK3: from=<a@example.com>, status=expired, returned to sender")
		rec2, ok2 := ParseLine("postfix/qmgr[22]: 4jV74M63RlzHxK3: from=<a@example.com>, status=force-expired, returned to sender")
		if !ok1 || rec1.Kind != KindQmgrExpired || rec1.Status != "expired" {
			t.Fatalf("unexpected expired: ok=%v rec=%+v", ok1, rec1)
		}
		if !ok2 || rec2.Kind != KindQmgrExpired || rec2.Status != "expired" {
			t.Fatalf("unexpected force-expired: ok=%v rec=%+v", ok2, rec2)
		}
	})

	t.Run("qmgr removed", func(t *testing.T) {
		rec, ok := ParseLine("postfix/qmgr[22]: 4jV74M63RlzHxK3: removed")
		if !ok || rec.Kind != KindQmgrRemoved {
			t.Fatalf("unexpected record: ok=%v rec=%+v", ok, rec)
		}
	})

	t.Run("bounce NDN", func(t *testing.T) {
		rec, ok := ParseLine("postfix/bounce[44]: 4jV74M63RlzHxK3: sender non-delivery notification: 4jV74P1234zHxK9")
		if !ok || rec.Kind != KindBounce || rec.QueueID != "4jV74M63RlzHxK3" || rec.BounceQueueID != "4jV74P1234zHxK9" {
			t.Fatalf("unexpected record: ok=%v rec=%+v", ok, rec)
		}
	})

	t.Run("all 6 delivery agents", func(t *testing.T) {
		lines := []struct {
			prog       string
			line       string
			wantStatus string
			wantOrigTo string
			wantRelay  string
			wantDSN    string
			wantResp   string
		}{
			{
				prog:       "smtp",
				line:       "postfix/smtp[31]: 4jV74M63RlzHxK3: to=<b@example.org>, relay=mx.example.org[192.0.2.2]:25, conn_use=2, delay=1.25, delays=0.05/0.01/0.4/0.79, dsn=2.0.0, status=sent (250 2.0.0 Ok: queued as XYZ)",
				wantStatus: "delivered",
				wantRelay:  "mx.example.org[192.0.2.2]:25",
				wantDSN:    "2.0.0",
				wantResp:   "250 2.0.0 Ok: queued as XYZ",
			},
			{
				prog:       "smtp",
				line:       "Nov 19 09:36:19 test-alpine postfix/smtp[332]: 4jVYJ16Wv6zKBCQ: to=<b@blackhole.test>, relay=none, delay=2, delays=0.01/0/2/0, tls=none?, dsn=4.4.1, status=deferred (connect to 192.0.2.1[192.0.2.1]:25: Operation timed out)",
				wantStatus: "deferred",
				wantRelay:  "none",
				wantDSN:    "4.4.1",
				wantResp:   "connect to 192.0.2.1[192.0.2.1]:25: Operation timed out",
			},
			{
				prog:       "lmtp",
				line:       "postfix/lmtp[32]: 4jV74M63RlzHxK3: to=<b@example.org>, orig_to=<alias@example.com>, relay=127.0.0.1[127.0.0.1]:24, delay=0.12, delays=0.02/0/0.01/0.09, dsn=4.2.2, status=deferred (Mailbox full)",
				wantStatus: "deferred",
				wantOrigTo: "alias@example.com",
				wantRelay:  "127.0.0.1[127.0.0.1]:24",
				wantDSN:    "4.2.2",
				wantResp:   "Mailbox full",
			},
			{
				prog:       "local",
				line:       "postfix/local[33]: 4jV74M63RlzHxK3: to=<bob@localhost>, orig_to=<root>, relay=local, delay=0.03, delays=0.01/0/0/0.02, dsn=2.0.0, status=sent (delivered to maildir)",
				wantStatus: "delivered",
				wantOrigTo: "root",
				wantRelay:  "local",
				wantDSN:    "2.0.0",
				wantResp:   "delivered to maildir",
			},
			{
				prog:       "virtual",
				line:       "postfix/virtual[34]: 4jV74M63RlzHxK3: to=<bob@virt.example>, relay=virtual, delay=0.04, delays=0.01/0/0/0.03, dsn=5.1.1, status=bounced (unknown user: \"bob@virt.example\" (in reply to check))",
				wantStatus: "bounced",
				wantRelay:  "virtual",
				wantDSN:    "5.1.1",
				wantResp:   "unknown user: \"bob@virt.example\" (in reply to check)",
			},
			{
				prog:       "pipe",
				line:       "postfix/pipe[35]: 4jV74M63RlzHxK3: to=<bob@example.org>, relay=dovecot, delay=0.22, delays=0.01/0.01/0/0.2, dsn=2.0.0, status=sent (delivered via dovecot service)",
				wantStatus: "delivered",
				wantRelay:  "dovecot",
				wantDSN:    "2.0.0",
				wantResp:   "delivered via dovecot service",
			},
			{
				prog:       "error",
				line:       "postfix/error[36]: 4jV74M63RlzHxK3: to=<bad@example.org>, relay=none, delay=0.01, delays=0.01/0/0/0, dsn=5.0.0, status=bounced (User unknown in virtual alias table)",
				wantStatus: "bounced",
				wantRelay:  "none",
				wantDSN:    "5.0.0",
				wantResp:   "User unknown in virtual alias table",
			},
		}

		for _, tc := range lines {
			rec, ok := ParseLine(tc.line)
			if !ok {
				t.Fatalf("%s: expected match for %q", tc.prog, tc.line)
			}
			if rec.Program != tc.prog || rec.Kind != KindDelivery {
				t.Fatalf("%s: unexpected prog/kind %+v", tc.prog, rec)
			}
			if rec.Status != tc.wantStatus || rec.OrigTo != tc.wantOrigTo || rec.Relay != tc.wantRelay || rec.DSN != tc.wantDSN || rec.Response != tc.wantResp {
				t.Errorf("%s: got %+v", tc.prog, rec)
			}
		}
	})

	t.Run("delivery unknown status ignored cleanly", func(t *testing.T) {
		_, ok := ParseLine("postfix/smtp[31]: 4jV74M63RlzHxK3: to=<b@example.org>, relay=mx.example.org[192.0.2.2]:25, delay=0.5, delays=0/0/0.2/0.3, dsn=2.1.5, status=deliverable (250 2.1.5 Ok)")
		if ok {
			t.Fatal("expected unknown status=deliverable to be ignored")
		}
	})

	t.Run("postsuper actions", func(t *testing.T) {
		cases := map[string]RecordKind{
			"placed on hold":     KindPostsuperHold,
			"released from hold": KindPostsuperRelease,
			"requeued":           KindPostsuperRequeue,
			"expired":            KindPostsuperExpire,
			"removed":            KindPostsuperRemove,
		}
		for action, wantKind := range cases {
			rec, ok := ParseLine("postfix/postsuper[99]: 4jV74M63RlzHxK3: " + action)
			if !ok || rec.Kind != wantKind {
				t.Errorf("action %q: ok=%v rec=%+v", action, ok, rec)
			}
		}
	})
}

func TestParseLine_UnrecognizedLines(t *testing.T) {
	ignored := []string{
		"",
		"   ",
		"2026-11-18T16:54:43.123456789Z postfix/smtpd[123]: connect from localhost[127.0.0.1]",
		"2026-11-18T16:54:43.123456789Z postfix/smtpd[123]: disconnect from localhost[127.0.0.1] ehlo=1 quit=1 commands=2",
		"Nov 18 16:54:43 mail postfix/anvil[45]: statistics: max connection rate 1/60s for (smtp:127.0.0.1) at Nov 18 16:54:00",
		"Nov 18 16:54:43 mail postfix/master[1]: daemon started -- version 3.11.7, configuration /etc/postfix",
		"Nov 18 16:54:43 mail postfix/postsuper[99]: Placed on hold: 1 message.",
		"Nov 18 16:54:43 mail postfix/cleanup[15]: warning: 4jV74M63RlzHxK3: unreasonable header",
		"Nov 18 16:54:43 mail postfix/smtpd[123]: NOQUEUE: reject: RCPT from unknown[192.0.2.5]: 550 5.1.1 User unknown",
		"totally random non-postfix line",
	}

	for _, line := range ignored {
		rec, ok := ParseLine(line)
		if ok || rec != nil {
			t.Errorf("expected ignored line %q to return (nil, false), got (%+v, %v)", line, rec, ok)
		}
	}
}

func TestParseSpoolLine(t *testing.T) {
	ts, rec, ok := ParseSpoolLine("2026-11-18T16:54:43.123456789Z postfix/smtpd[12]: connect from localhost[127.0.0.1]")
	if ok || rec != nil || ts.IsZero() {
		t.Fatalf("expected valid ts with skipped record, got ts=%v rec=%v ok=%v", ts, rec, ok)
	}

	ts, rec, ok = ParseSpoolLine("2026-11-18T16:54:43.123456789Z postfix/smtpd[12]: 4jV74M63RlzHxK3: client=localhost[127.0.0.1]")
	if !ok || rec == nil || ts.IsZero() || rec.QueueID != "4jV74M63RlzHxK3" {
		t.Fatalf("expected parsed record, got ts=%v rec=%+v ok=%v", ts, rec, ok)
	}

	ts, rec, ok = ParseSpoolLine("Nov 18 16:54:43 host postfix/smtpd[12]: 4jV74M63RlzHxK3: client=localhost[127.0.0.1]")
	if ok || rec != nil || !ts.IsZero() {
		t.Fatalf("expected ParseSpoolLine to require RFC3339Nano prefix, got ts=%v rec=%v ok=%v", ts, rec, ok)
	}
}
