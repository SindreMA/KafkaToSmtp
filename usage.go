package main

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// SentEvent is appended to the ledger topic after each successful send. The
// per-provider daily totals are derived purely from these events — there is no
// database. State is rebuilt on startup by replaying the current UTC day.
type SentEvent struct {
	Provider string `json:"provider"`
	Count    int    `json:"count"` // number of recipients (or 1, per COUNT_BY)
	Day      string `json:"day"`   // UTC date, YYYY-MM-DD
	TS       string `json:"ts"`    // RFC3339 timestamp
}

// Usage holds the current UTC day's per-provider counts in memory. Single-replica
// by design: the worker is the only writer, so in-memory counts + its own
// increments stay accurate, and a restart re-derives them from the ledger.
type Usage struct {
	mu     sync.Mutex
	day    string
	counts map[string]int
}

func NewUsage(initial map[string]int) *Usage {
	if initial == nil {
		initial = map[string]int{}
	}
	return &Usage{day: todayUTC(), counts: initial}
}

func todayUTC() string { return time.Now().UTC().Format("2006-01-02") }

func midnightUTC() time.Time {
	n := time.Now().UTC()
	return time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, time.UTC)
}

// rollover resets counts when the UTC day changes (caller holds the lock).
func (u *Usage) rollover() {
	if d := todayUTC(); d != u.day {
		u.day = d
		u.counts = map[string]int{}
	}
}

func (u *Usage) Get(provider string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.rollover()
	return u.counts[provider]
}

func (u *Usage) Add(provider string, n int) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.rollover()
	u.counts[provider] += n
}

func (u *Usage) Day() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.rollover()
	return u.day
}

func (u *Usage) Snapshot() map[string]int {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.rollover()
	out := make(map[string]int, len(u.counts))
	for k, v := range u.counts {
		out[k] = v
	}
	return out
}

// replayToday rebuilds today's per-provider counts by reading every ledger event
// since UTC midnight from the single-partition ledger topic. Returns empty counts
// (not an error) if the topic doesn't exist yet or has nothing for today.
func replayToday(ctx context.Context, brokers []string, topic string) (map[string]int, error) {
	counts := map[string]int{}
	day := todayUTC()

	conn, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, 0)
	if err != nil {
		return counts, err
	}
	defer conn.Close()

	startOff, err := conn.ReadOffset(midnightUTC())
	if err != nil {
		return counts, err
	}
	lastOff, err := conn.ReadLastOffset()
	if err != nil {
		return counts, err
	}
	if startOff < 0 || startOff >= lastOff {
		return counts, nil // nothing for today
	}

	r := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0})
	defer r.Close()
	if err := r.SetOffset(startOff); err != nil {
		return counts, err
	}
	for off := startOff; off < lastOff; off++ {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			break
		}
		var ev SentEvent
		if json.Unmarshal(m.Value, &ev) == nil && ev.Day == day {
			counts[ev.Provider] += ev.Count
		}
		if m.Offset >= lastOff-1 {
			break
		}
	}
	return counts, nil
}

// recordSent appends a send event to the ledger so the count survives restarts.
func recordSent(ctx context.Context, w *kafka.Writer, provider string, count int) error {
	ev := SentEvent{
		Provider: provider,
		Count:    count,
		Day:      todayUTC(),
		TS:       time.Now().UTC().Format(time.RFC3339),
	}
	b, _ := json.Marshal(ev)
	return w.WriteMessages(ctx, kafka.Message{Key: []byte(provider), Value: b})
}
