package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

// serveHealth runs a tiny HTTP server exposing /healthz and /readyz for
// Kubernetes probes. It is liveness-only (always 200 once the process is up);
// Kafka/SMTP connectivity is handled by retry logic, not by failing probes,
// so a transient broker or MTA outage never restarts the pod.
func serveHealth(ctx context.Context, port string) {
	if port == "" {
		return
	}

	mux := http.NewServeMux()
	ok := func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
	mux.HandleFunc("/healthz", ok)
	mux.HandleFunc("/readyz", ok)

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("health server stopped", "err", err)
	}
}
