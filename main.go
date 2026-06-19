// Command kafka-to-smtp consumes email "envelope" messages from a Kafka topic
// and submits them to an SMTP server (e.g. Maddy) for delivery.
//
// It is designed to scale to zero: when the topic is empty, no work is done,
// and KEDA's Kafka scaler can run zero replicas. When messages arrive, KEDA
// scales the Deployment up and this worker drains the backlog.
//
// Delivery is at-least-once: a message's offset is committed only after the
// SMTP server has accepted it. If the SMTP server is unavailable, the worker
// keeps retrying and never commits, so mail waits durably in Kafka instead of
// being lost.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	cfg := LoadConfig()
	setupLogging(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go serveHealth(ctx, cfg.HealthPort)

	mailer := &Mailer{cfg: cfg.SMTP}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Kafka.Brokers,
		Topic:    cfg.Kafka.Topic,
		GroupID:  cfg.Kafka.GroupID,
		MinBytes: 1,
		MaxBytes: 10 * 1024 * 1024,
	})
	defer reader.Close()

	slog.Info("kafka-to-smtp started",
		"brokers", cfg.Kafka.Brokers,
		"topic", cfg.Kafka.Topic,
		"group", cfg.Kafka.GroupID,
		"smtp", net.JoinHostPort(cfg.SMTP.Host, cfg.SMTP.Port),
		"smtp_tls", cfg.SMTP.TLS,
	)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break // shutting down
			}
			slog.Error("fetch message", "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second): // avoid a hot loop on broker errors
			}
			continue
		}

		if err := process(ctx, mailer, cfg, msg); err != nil {
			// process only returns an error when the context was cancelled
			// mid-retry (shutdown). Do NOT commit — the message is redelivered
			// on next start, preserving at-least-once delivery.
			break
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				break
			}
			slog.Error("commit offset", "offset", msg.Offset, "err", err)
		}
	}

	slog.Info("shutting down")
}

// process handles a single Kafka message. It returns nil when the message has
// been dealt with and its offset may be committed (sent successfully, or
// permanently undeliverable and therefore skipped). It returns a non-nil error
// only when the context is cancelled while retrying, signalling the caller to
// stop without committing.
func process(ctx context.Context, m *Mailer, cfg Config, msg kafka.Message) error {
	var env Envelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		slog.Error("dropping unparseable message (poison)", "offset", msg.Offset, "err", err)
		return nil // commit & skip — retrying never helps
	}
	if err := env.Validate(cfg.SMTP.DefaultFrom); err != nil {
		slog.Error("dropping invalid message (poison)", "offset", msg.Offset, "err", err)
		return nil
	}

	for attempt := 1; ; attempt++ {
		err := m.Send(&env)
		if err == nil {
			slog.Info("email sent",
				"offset", msg.Offset, "to", []string(env.To), "subject", env.Subject, "attempt", attempt)
			return nil
		}
		if isPermanent(err) {
			slog.Error("dropping message (permanent SMTP error)", "offset", msg.Offset, "err", err)
			return nil // commit & skip
		}
		if cfg.SMTP.MaxAttempts > 0 && attempt >= cfg.SMTP.MaxAttempts {
			slog.Error("giving up after max attempts (message skipped)",
				"offset", msg.Offset, "attempts", attempt, "err", err)
			return nil
		}

		backoff := computeBackoff(attempt, cfg.SMTP.RetryBase, cfg.SMTP.RetryMax)
		slog.Warn("send failed, will retry",
			"offset", msg.Offset, "attempt", attempt, "backoff", backoff.String(), "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err() // shutting down; leave uncommitted for redelivery
		case <-time.After(backoff):
		}
	}
}

// computeBackoff returns base * 2^(attempt-1), capped at max. The shift can
// overflow for large attempts; the guard catches the resulting non-positive
// value and falls back to max.
func computeBackoff(attempt int, base, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := base << (attempt - 1)
	if d <= 0 || d > max {
		return max
	}
	return d
}

func setupLogging(level string) {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn", "warning":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l})))
}
