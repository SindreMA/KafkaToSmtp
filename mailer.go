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
	"strconv"
	"strings"
	"time"
)

// sendViaProvider renders the envelope and submits it through one provider's SMTP
// relay. A permanent error (returned via permanent()) means the message itself is
// bad and should be dropped; any other error is provider-specific and the caller
// should fail over to the next provider.
func sendViaProvider(p Provider, env *Envelope, cfg Config) error {
	fromStr := env.From
	fromFromMessage := fromStr != ""
	if fromStr == "" {
		fromStr = p.From
	}
	if fromStr == "" {
		fromStr = cfg.DefaultFrom
	}

	from, err := mail.ParseAddress(fromStr)
	if err != nil {
		if fromFromMessage {
			return permanent(fmt.Errorf("invalid from %q: %w", fromStr, err))
		}
		// Misconfigured provider/default From — treat as provider-specific so we
		// don't drop the message; another provider may have a valid From.
		return fmt.Errorf("invalid configured from %q (provider %s): %w", fromStr, p.Name, err)
	}

	rcpts, err := recipientAddrs(env)
	if err != nil {
		return permanent(err)
	}

	msg := buildMIME(env, from)
	return deliver(p, cfg, from.Address, rcpts, msg)
}

func deliver(p Provider, cfg Config, from string, rcpts []string, msg []byte) error {
	addr := net.JoinHostPort(p.Host, strconv.Itoa(p.Port))
	dialer := &net.Dialer{Timeout: cfg.DialTimeout}

	var conn net.Conn
	var err error
	if p.TLS == "tls" {
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, tlsConf(p))
	} else {
		conn, err = dialer.Dial("tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}

	c, err := smtp.NewClient(conn, p.Host)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("smtp handshake: %w", err)
	}
	defer c.Close()

	if err := c.Hello(cfg.HeloName); err != nil {
		return fmt.Errorf("helo: %w", err)
	}

	if p.TLS == "starttls" {
		if ok, _ := c.Extension("STARTTLS"); !ok {
			return errors.New("starttls requested but not advertised by server")
		}
		if err := c.StartTLS(tlsConf(p)); err != nil {
			return fmt.Errorf("starttls: %w", err)
		}
	}

	if p.Username != "" {
		if ok, _ := c.Extension("AUTH"); ok {
			auth := smtp.PlainAuth("", p.Username, p.Password, p.Host)
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

func tlsConf(p Provider) *tls.Config {
	return &tls.Config{
		ServerName:         p.Host,
		InsecureSkipVerify: p.TLSInsecure, //nolint:gosec // opt-in per provider
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
			continue
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
	b.WriteString("Content-Transfer-Encoding: base64\r\n\r\n")
	b.WriteString(wrap76(base64.StdEncoding.EncodeToString([]byte(body))))
}

func writePart(b *strings.Builder, boundary, ctype, body string) {
	b.WriteString("--" + boundary + "\r\n")
	b.WriteString("Content-Type: " + ctype + "\r\n")
	b.WriteString("Content-Transfer-Encoding: base64\r\n\r\n")
	b.WriteString(wrap76(base64.StdEncoding.EncodeToString([]byte(body))))
}

// wrap76 splits a string into 76-character CRLF-separated lines (required for
// base64 email bodies) and ensures a trailing CRLF.
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

// permanentError marks a message that will never succeed regardless of provider
// (bad address syntax / poison content), so the caller drops it instead of retrying.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

func permanent(err error) error { return &permanentError{err: err} }

func isPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
