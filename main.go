// Command kafka-to-smtp consumes email "envelope" messages from a Kafka topic
// and sends them through one of several SMTP providers, rotating between them to
// stay under each provider's free-tier daily limit.
//
// Per-provider daily counts are derived entirely from a Kafka ledger topic (no
// database): after each successful send the worker appends a SentEvent, and on
// startup it replays the current UTC day to rebuild the counts. This is a
// single-replica design (the worker is the only writer) and scales to zero.
//
// Delivery is at-least-once: the input offset is committed only after a provider
// has accepted the message. If every provider is failing or capped, the message
// is left uncommitted so it waits durably in Kafka.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
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

	providers, err := LoadProviders(cfg)
	if err != nil {
		slog.Error("failed to load providers", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go serveHealth(ctx, cfg.HealthPort)

	// Rebuild today's per-provider counts from the ledger.
	counts, err := replayToday(ctx, cfg.Kafka.Brokers, cfg.Kafka.SentTopic)
	if err != nil {
		slog.Warn("ledger replay failed; starting counts from zero", "err", err)
		counts = map[string]int{}
	}
	usage := NewUsage(counts)

	ledger := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Kafka.Brokers...),
		Topic:        cfg.Kafka.SentTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		BatchTimeout: 50 * time.Millisecond,
	}
	defer ledger.Close()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Kafka.Brokers,
		Topic:    cfg.Kafka.Topic,
		GroupID:  cfg.Kafka.GroupID,
		MinBytes: 1,
		MaxBytes: 10 * 1024 * 1024,
	})
	defer reader.Close()

	names := make([]string, len(providers))
	for i, p := range providers {
		names[i] = p.Name
	}
	slog.Info("kafka-to-smtp started",
		"brokers", cfg.Kafka.Brokers,
		"topic", cfg.Kafka.Topic,
		"sent_topic", cfg.Kafka.SentTopic,
		"group", cfg.Kafka.GroupID,
		"providers", names,
		"count_by", cfg.CountBy,
		"day", usage.Day(),
		"usage", usage.Snapshot(),
	)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			slog.Error("fetch message", "err", err)
			if !sleep(ctx, 2*time.Second) {
				break
			}
			continue
		}

		if err := process(ctx, cfg, providers, usage, ledger, msg); err != nil {
			break // context cancelled mid-retry; leave uncommitted for redelivery
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

// process handles one message. Returns nil when the offset may be committed
// (sent, or permanently undeliverable and skipped); returns an error only when
// the context is cancelled while waiting, so the caller stops without committing.
func process(ctx context.Context, cfg Config, providers []Provider, usage *Usage, ledger *kafka.Writer, msg kafka.Message) error {
	var env Envelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		slog.Error("dropping unparseable message (poison)", "offset", msg.Offset, "err", err)
		return nil
	}
	if err := env.Validate(anyFallbackFrom(providers, cfg.DefaultFrom)); err != nil {
		slog.Error("dropping invalid message (poison)", "offset", msg.Offset, "err", err)
		return nil
	}

	rcpts, perr := recipientAddrs(&env)
	if perr != nil {
		slog.Error("dropping message with bad recipients (poison)", "offset", msg.Offset, "err", perr)
		return nil
	}
	need := 1
	if cfg.CountBy == "recipient" {
		need = len(rcpts)
	}

	for attempt := 1; ; attempt++ {
		// Candidates = providers still under their daily cap for `need` more.
		var candidates []Provider
		capped := 0
		for _, p := range providers {
			if p.DailyLimit > 0 && usage.Get(p.Name)+need > p.DailyLimit {
				capped++
				continue
			}
			candidates = append(candidates, p)
		}

		if len(candidates) == 0 {
			slog.Warn("all providers at daily cap; waiting",
				"offset", msg.Offset, "day", usage.Day(), "usage", usage.Snapshot(),
				"wait", cfg.AllCappedWait.String())
			if !sleep(ctx, cfg.AllCappedWait) {
				return ctx.Err()
			}
			continue
		}

		errored := 0
		for _, p := range candidates {
			err := sendViaProvider(p, &env, cfg)
			if err == nil {
				if werr := recordSent(ctx, ledger, p.Name, need); werr != nil {
					slog.Error("sent, but failed to write ledger event (count may reset on restart)",
						"provider", p.Name, "err", werr)
				}
				usage.Add(p.Name, need)
				slog.Info("email sent",
					"provider", p.Name, "offset", msg.Offset, "to", []string(env.To),
					"subject", env.Subject, "count", need, "provider_used_today", usage.Get(p.Name))
				return nil
			}
			if isPermanent(err) {
				slog.Error("dropping message (permanent)", "offset", msg.Offset, "err", err)
				return nil
			}
			errored++
			slog.Warn("provider failed, trying next", "provider", p.Name, "offset", msg.Offset, "err", err)
		}

		// Every available provider errored this round.
		if cfg.MaxAttempts > 0 && attempt >= cfg.MaxAttempts {
			slog.Error("giving up after max attempts (message skipped)",
				"offset", msg.Offset, "attempts", attempt)
			return nil
		}
		backoff := computeBackoff(attempt, cfg.RetryBase, cfg.RetryMax)
		slog.Warn("all available providers failed; backing off",
			"offset", msg.Offset, "attempt", attempt, "failed", errored, "backoff", backoff.String())
		if !sleep(ctx, backoff) {
			return ctx.Err()
		}
	}
}

// computeBackoff returns base * 2^(attempt-1), capped at max (overflow falls back to max).
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

// sleep waits for d or context cancellation; returns false if cancelled.
func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
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
