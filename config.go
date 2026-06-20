package main

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration, sourced from environment variables.
// Provider definitions (SMTP relays + daily limits) are loaded separately from
// JSON via PROVIDERS_FILE / PROVIDERS — see provider.go.
type Config struct {
	Kafka         KafkaConfig
	DefaultFrom   string
	HeloName      string
	DialTimeout   time.Duration
	RetryBase     time.Duration
	RetryMax      time.Duration
	AllCappedWait time.Duration // how long to wait when every provider is at its daily cap
	MaxAttempts   int           // max full retry rounds when all providers error; 0 = unlimited
	CountBy       string        // "recipient" (default) or "message"
	ProvidersFile string
	ProvidersRaw  string
	LogLevel      string
	HealthPort    string
}

type KafkaConfig struct {
	Brokers   []string
	Topic     string // input: email envelopes
	GroupID   string
	SentTopic string // ledger: one event per successful send
}

func LoadConfig() Config {
	helo := env("SMTP_HELO", "")
	if helo == "" {
		if h, err := os.Hostname(); err == nil {
			helo = h
		} else {
			helo = "localhost"
		}
	}

	return Config{
		Kafka: KafkaConfig{
			Brokers:   splitCSV(env("KAFKA_BROKERS", "kafka-kafka-bootstrap.events:9092")),
			Topic:     env("KAFKA_TOPIC", "email-outbound"),
			GroupID:   env("KAFKA_GROUP_ID", "email-worker"),
			SentTopic: env("KAFKA_SENT_TOPIC", "email-sent"),
		},
		DefaultFrom:   env("DEFAULT_FROM", ""),
		HeloName:      helo,
		DialTimeout:   envDuration("SMTP_DIAL_TIMEOUT", 10*time.Second),
		RetryBase:     envDuration("SMTP_RETRY_BASE", 2*time.Second),
		RetryMax:      envDuration("SMTP_RETRY_MAX", 60*time.Second),
		AllCappedWait: envDuration("ALL_CAPPED_WAIT", 5*time.Minute),
		MaxAttempts:   envInt("SMTP_MAX_ATTEMPTS", 0),
		CountBy:       strings.ToLower(env("COUNT_BY", "recipient")),
		ProvidersFile: env("PROVIDERS_FILE", ""),
		ProvidersRaw:  env("PROVIDERS", ""),
		LogLevel:      env("LOG_LEVEL", "info"),
		HealthPort:    env("HEALTH_PORT", "8080"),
	}
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
