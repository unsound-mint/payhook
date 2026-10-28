// Package metrics defines the Prometheus metrics shared by all binaries.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "payhook_http_request_duration_seconds",
		Help:    "HTTP request latency by route and status code.",
		Buckets: prometheus.DefBuckets,
	}, []string{"route", "method", "code"})

	Webhooks = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payhook_webhooks_total",
		Help: "Provider webhooks by outcome (applied, duplicate, rejected_transition, ...).",
	}, []string{"provider", "outcome"})

	OutboxPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payhook_outbox_published_total",
		Help: "Outbox events published to the broker.",
	})

	OutboxFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payhook_outbox_failed_total",
		Help: "Outbox rows set aside because they are not a valid event. Should stay at zero.",
	})

	OutboxBacklog = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "payhook_outbox_backlog",
		Help: "Outbox events not yet published. Should stay near zero.",
	})

	Deliveries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payhook_notifier_deliveries_total",
		Help: "Merchant callback attempts by result (ok, retry, dead_letter, duplicate, skipped).",
	}, []string{"result"})
)

// Serve exposes /metrics on addr. Every binary serves it on a separate port
// (api :9100, relay :9101, worker :9102), never on the public API listener.
func Serve(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return srv.ListenAndServe()
}
