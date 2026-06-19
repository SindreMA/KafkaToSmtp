package main

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"strings"
	"time"
)

// Mailer builds a MIME message from an Envelope and submits it to the
// configured SMTP server. The SMTP server (Maddy) is responsible for DKIM
// signing, MX lookup, and final delivery — this worker only speaks submission.
type Mailer struct {
	cfg SMTPConfig
}

// Send renders and delivers one envelope. Parse/validation failures are
// returned as permanent errors (the message is poison and should be skipped);
// transient failures (e.g. the SMTP server being down) are returned plainly so
// the caller retries.
func (m *Mailer) Send(env *Envelope) error {
	fromStr := env.From
	if fromStr == "" {
		fromStr = m.cfg.DefaultFrom
	}
	from, err := mail.ParseAddress(fromStr)
	if err != nil {
		return permanent(fmt.Errorf("invalid from address %q: %w", fromStr, err))
	}

	rcpts, err := recipientAddrs(env)
	if err != nil {
		return permanent(err)
	}

	msg := buildMIME(env, from)

	if err := m.deliver(from.Address, rcpts, msg); err != nil {
		return classify(err)
	}
	return nil
}

func (m *Mailer) deliver(from string, rcpts []string, msg []byte) error {
	addr := net.JoinHostPort(m.cfg.Host, m.cfg.Port)
	dialer := &net.Dialer{Timeout: m.cfg.DialTimeout}

	var conn net.Conn
	var err error
	if m.cfg.TLS == "tls" {
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, m.tlsConfig())
	} else {
		conn, err = dialer.Dial("tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}

	c, err := smtp.NewClient(conn, m.cfg.Host)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("smtp handshake: %w", err)
	}
	defer c.Close()

	if err := c.Hello(m.cfg.HeloName); err != nil {
		return fmt.Errorf("helo: %w", err)
	}

	if m.cfg.TLS == "starttls" {
		if ok, _ := c.Extension("STARTTLS"); !ok {
			return errors.New("starttls requested but not advertised by server")
		}
		if err := c.StartTLS(m.tlsConfig()); err != nil {
			return fmt.Errorf("starttls: %w", err)
		}
	}

	if m.cfg.Username != "" {
		if ok, _ := c.Extension("AUTH"); ok {
			auth := smtp.PlainAuth("", m.cfg.Username, m.cfg.Password, m.cfg.Host)
			if err := c.Auth(auth); err != nil {
				return fmt.Errorf("auth: %w", err)
			}
		}
	}

	if err := c.Mail(from); err != nil {
		return fmt.Errorf("mail from <%s>: %w", from, err)
	}
	for _, r := range rcpts {
		if err := c.Rcpt(r); err != nil {
			return fmt.Errorf("rcpt to <%s>: %w", r, err)
		}
	}

	w, err := c.Data()
	if err != nil {
		return fmt.Errorf("data: %w", err)
	}
	if _, err := w.Write(msg); err != nil {
		_ = w.Close()
		return fmt.Errorf("write body: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("close body: %w", err)
	}
	return c.Quit()
}

func (m *Mailer) tlsConfig() *tls.Config {
	return &tls.Config{
		ServerName:         m.cfg.Host,
		InsecureSkipVerify: m.cfg.TLSInsecure, //nolint:gosec // opt-in for self-signed internal MTAs
	}
}

// buildMIME renders the envelope into RFC 5322 bytes with CRLF line endings.
// Bodies are base64-encoded, which is always safe regardless of content.
func buildMIME(env *Envelope, from *mail.Address) []byte {
	var b strings.Builder
	header := func(k, v string) {
		b.WriteString(k)
		b.WriteString(": ")
		b.WriteString(v)
		b.WriteString("\r\n")
	}

	header("From", from.String())
	if len(env.To) > 0 {
		header("To", joinAddrs(env.To))
	}
	if len(env.Cc) > 0 {
		header("Cc", joinAddrs(env.Cc))
	}
	if env.ReplyTo != "" {
		header("Reply-To", env.ReplyTo)
	}
	header("Subject", mime.QEncoding.Encode("utf-8", env.Subject))
	header("Date", time.Now().Format(time.RFC1123Z))
	header("Message-ID", messageID(from.Address))
	for k, v := range env.Headers {
		if isReservedHeader(k) {
			continue // never let custom headers clobber structural ones
		}
		header(k, mime.QEncoding.Encode("utf-8", v))
	}
	header("MIME-Version", "1.0")

	hasText, hasHTML := env.Text != "", env.HTML != ""
	switch {
	case hasText && hasHTML:
		boundary := "alt-" + randHex(16)
		header("Content-Type", fmt.Sprintf("multipart/alternative; boundary=%q", boundary))
		b.WriteString("\r\n")
		writePart(&b, boundary, "text/plain; charset=utf-8", env.Text)
		writePart(&b, boundary, "text/html; charset=utf-8", env.HTML)
		b.WriteString("--" + boundary + "--\r\n")
	case hasHTML:
		writeSinglePart(&b, "text/html; charset=utf-8", env.HTML)
	default:
		writeSinglePart(&b, "text/plain; charset=utf-8", env.Text)
	}

	return []byte(b.String())
}

func writeSinglePart(b *strings.Builder, ctype, body string) {
	b.WriteString("Content-Type: " + ctype + "\r\n")
	b.WriteString("Content-Transfer-Encoding: base64\r\n")
	b.WriteString("\r\n")
	b.WriteString(wrap76(base64.StdEncoding.EncodeToString([]byte(body))))
}

func writePart(b *strings.Builder, boundary, ctype, body string) {
	b.WriteString("--" + boundary + "\r\n")
	b.WriteString("Content-Type: " + ctype + "\r\n")
	b.WriteString("Content-Transfer-Encoding: base64\r\n")
	b.WriteString("\r\n")
	b.WriteString(wrap76(base64.StdEncoding.EncodeToString([]byte(body))))
}

// wrap76 splits a string into 76-character lines separated by CRLF, as required
// for base64 in email bodies, and ensures a trailing CRLF.
func wrap76(s string) string {
	var b strings.Builder
	for len(s) > 76 {
		b.WriteString(s[:76])
		b.WriteString("\r\n")
		s = s[76:]
	}
	b.WriteString(s)
	b.WriteString("\r\n")
	return b.String()
}

func joinAddrs(addrs []string) string {
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if pa, err := mail.ParseAddress(a); err == nil {
			out = append(out, pa.String())
		} else {
			out = append(out, a)
		}
	}
	return strings.Join(out, ", ")
}

// recipientAddrs returns the bare addresses for the SMTP envelope (RCPT TO),
// combining To, Cc and Bcc.
func recipientAddrs(env *Envelope) ([]string, error) {
	var rcpts []string
	for _, group := range []StringOrSlice{env.To, env.Cc, env.Bcc} {
		for _, a := range group {
			pa, err := mail.ParseAddress(a)
			if err != nil {
				return nil, fmt.Errorf("invalid recipient %q: %w", a, err)
			}
			rcpts = append(rcpts, pa.Address)
		}
	}
	if len(rcpts) == 0 {
		return nil, errors.New("no recipients")
	}
	return rcpts, nil
}

func messageID(fromAddr string) string {
	domain := "localhost"
	if i := strings.LastIndex(fromAddr, "@"); i >= 0 && i+1 < len(fromAddr) {
		domain = fromAddr[i+1:]
	}
	return fmt.Sprintf("<%s@%s>", randHex(16), domain)
}

func randHex(n int) string {
	buf := make([]byte, n)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}

func isReservedHeader(k string) bool {
	switch strings.ToLower(strings.TrimSpace(k)) {
	case "from", "to", "cc", "bcc", "reply-to", "subject", "date",
		"message-id", "mime-version", "content-type", "content-transfer-encoding":
		return true
	}
	return false
}

// permanentError marks a message that will never succeed (bad address, poison
// content, or a 5xx SMTP rejection) so the caller skips rather than retries.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

func permanent(err error) error { return &permanentError{err: err} }

func isPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// classify promotes 5xx SMTP replies to permanent errors; everything else
// (connection refused, timeouts, 4xx) stays transient and is retried.
func classify(err error) error {
	var tp *textproto.Error
	if errors.As(err, &tp) && tp.Code >= 500 && tp.Code < 600 {
		return permanent(err)
	}
	return err
}
