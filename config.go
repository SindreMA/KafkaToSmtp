package main

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration, sourced entirely from environment
// variables so the same image works in any namespace.
type Config struct {
	Kafka      KafkaConfig
	SMTP       SMTPConfig
	LogLevel   string
	HealthPort string
}

type KafkaConfig struct {
	Brokers []string
	Topic   string
	GroupID string
}

type SMTPConfig struct {
	Host        string
	Port        string
	Username    string
	Password    string
	DefaultFrom string
	HeloName    string
	TLS         string // "none" | "starttls" | "tls"
	TLSInsecure bool
	DialTimeout time.Duration
	RetryBase   time.Duration
	RetryMax    time.Duration
	MaxAttempts int // 0 = retry indefinitely (lossless backpressure)
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
			Brokers: splitCSV(env("KAFKA_BROKERS", "kafka-kafka-bootstrap.events:9092")),
			Topic:   env("KAFKA_TOPIC", "email-outbound"),
			GroupID: env("KAFKA_GROUP_ID", "email-worker"),
		},
		SMTP: SMTPConfig{
			Host:        env("SMTP_HOST", "maddy.communication.svc.cluster.local"),
			Port:        env("SMTP_PORT", "25"),
			Username:    env("SMTP_USERNAME", ""),
			Password:    env("SMTP_PASSWORD", ""),
			DefaultFrom: env("DEFAULT_FROM", ""),
			HeloName:    helo,
			TLS:         strings.ToLower(env("SMTP_TLS", "none")),
			TLSInsecure: envBool("SMTP_TLS_INSECURE", false),
			DialTimeout: envDuration("SMTP_DIAL_TIMEOUT", 10*time.Second),
			RetryBase:   envDuration("SMTP_RETRY_BASE", 2*time.Second),
			RetryMax:    envDuration("SMTP_RETRY_MAX", 60*time.Second),
			MaxAttempts: envInt("SMTP_MAX_ATTEMPTS", 0),
		},
		LogLevel:   env("LOG_LEVEL", "info"),
		HealthPort: env("HEALTH_PORT", "8080"),
	}
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
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
