package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// Provider is one SMTP relay the worker can send through, with a daily free-tier
// cap. The worker tries providers in Priority order, skipping any that have hit
// their DailyLimit for the current UTC day.
type Provider struct {
	Name        string `json:"name"`
	Host        string `json:"host"`
	Port        int    `json:"port"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	TLS         string `json:"tls"`         // none | starttls | tls  (default starttls)
	TLSInsecure bool   `json:"tlsInsecure"` // skip cert verification (self-signed relays)
	DailyLimit  int    `json:"dailyLimit"`  // 0 = unlimited
	Priority    int    `json:"priority"`    // lower = preferred
	From        string `json:"from"`        // optional per-provider From override
}

// LoadProviders reads the provider list from PROVIDERS_FILE (preferred, e.g. a
// mounted Secret) or the PROVIDERS env var, fills defaults, and sorts by priority.
func LoadProviders(cfg Config) ([]Provider, error) {
	var raw []byte
	switch {
	case cfg.ProvidersFile != "":
		b, err := os.ReadFile(cfg.ProvidersFile)
		if err != nil {
			return nil, fmt.Errorf("read PROVIDERS_FILE: %w", err)
		}
		raw = b
	case cfg.ProvidersRaw != "":
		raw = []byte(cfg.ProvidersRaw)
	default:
		return nil, fmt.Errorf("no providers configured (set PROVIDERS_FILE or PROVIDERS)")
	}

	var ps []Provider
	if err := json.Unmarshal(raw, &ps); err != nil {
		return nil, fmt.Errorf("parse providers JSON: %w", err)
	}
	if len(ps) == 0 {
		return nil, fmt.Errorf("providers list is empty")
	}

	for i := range ps {
		if ps[i].Host == "" {
			return nil, fmt.Errorf("provider %d: missing host", i)
		}
		if ps[i].Port == 0 {
			ps[i].Port = 587
		}
		if ps[i].TLS == "" {
			ps[i].TLS = "starttls"
		}
		if ps[i].Name == "" {
			ps[i].Name = fmt.Sprintf("%s:%d", ps[i].Host, ps[i].Port)
		}
	}
	sort.SliceStable(ps, func(i, j int) bool { return ps[i].Priority < ps[j].Priority })
	return ps, nil
}

// anyFallbackFrom reports whether a From can be resolved without the message
// specifying one (via a provider override or the global default).
func anyFallbackFrom(providers []Provider, defaultFrom string) bool {
	if defaultFrom != "" {
		return true
	}
	for _, p := range providers {
		if p.From != "" {
			return true
		}
	}
	return false
}
