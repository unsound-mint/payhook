// Package httpapi exposes the REST API and the provider webhook endpoint.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/unsound-mint/payhook/internal/metrics"
	"github.com/unsound-mint/payhook/internal/payment"
	"github.com/unsound-mint/payhook/internal/safehttp"
	"github.com/unsound-mint/payhook/internal/webhook"
)

const maxBody = 64 << 10 // 64 KiB is plenty for these payloads

type Server struct {
	Store          *payment.Store
	Pool           *pgxpool.Pool
	WebhookSecrets map[string][]byte // provider name -> signing secret
	// AllowPrivateCallbacks accepts http and internal callback_url targets (local/tests only).
	AllowPrivateCallbacks bool
	Log                   *slog.Logger
	Now                   func() time.Time
}

func (s *Server) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.Recoverer, s.observe)

	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	r.Get("/readyz", s.ready)
	r.Handle("/metrics", promhttp.Handler())

	r.Route("/v1", func(r chi.Router) {
		r.Post("/payments", s.createPayment)
		r.Get("/payments/{id}", s.getPayment)
		r.Post("/webhooks/{provider}", s.receiveWebhook)
	})
	return r
}

// POST /v1/payments  (header Idempotency-Key required)
func (s *Server) createPayment(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 255 {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required (max 255 chars)")
		return
	}
	var req payment.CreateRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	if req.CallbackURL != nil {
		if err := safehttp.CheckURL(*req.CallbackURL, s.AllowPrivateCallbacks); err != nil {
			writeError(w, http.StatusUnprocessableEntity, "callback_url: "+err.Error())
			return
		}
	}
	p, created, err := s.Store.Create(r.Context(), key, req)
	switch {
	case errors.Is(err, payment.ErrIdempotencyReuse):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	case err != nil:
		s.internal(w, r, err)
		return
	}
	code := http.StatusCreated
	if !created {
		code = http.StatusOK // replay of an earlier request
	}
	writeJSON(w, code, p)
}

// GET /v1/payments/{id}
func (s *Server) getPayment(w http.ResponseWriter, r *http.Request) {
	p, err := s.Store.Get(r.Context(), chi.URLParam(r, "id"))
	switch {
	case errors.Is(err, payment.ErrNotFound):
		writeError(w, http.StatusNotFound, "payment not found")
	case err != nil:
		s.internal(w, r, err)
	default:
		writeJSON(w, http.StatusOK, p)
	}
}

// POST /v1/webhooks/{provider}
//
// Returns 2xx for everything we've safely recorded, including duplicates and
// transitions we refused: the provider would otherwise keep retrying an event
// that will never apply. Non-2xx only for bad signatures and our own failures.
func (s *Server) receiveWebhook(w http.ResponseWriter, r *http.Request) {
	provider := chi.URLParam(r, "provider")
	secret, ok := s.WebhookSecrets[provider]
	if !ok {
		writeError(w, http.StatusNotFound, "unknown provider")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "body too large")
		return
	}
	if err := webhook.Verify(secret, r.Header.Get("X-Signature"), body, s.Now(), 5*time.Minute); err != nil {
		metrics.Webhooks.WithLabelValues(provider, "bad_signature").Inc()
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	var ev payment.WebhookEvent
	if err := json.Unmarshal(body, &ev); err != nil || ev.ID == "" || ev.Type == "" {
		writeError(w, http.StatusBadRequest, "event must have id and type")
		return
	}
	outcome, err := s.Store.ApplyWebhook(r.Context(), provider, ev, body)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	metrics.Webhooks.WithLabelValues(provider, outcome).Inc()
	s.Log.Info("webhook", "provider", provider, "event_id", ev.ID, "type", ev.Type, "outcome", outcome)
	writeJSON(w, http.StatusOK, map[string]string{"outcome": outcome})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.Pool.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) internal(w http.ResponseWriter, r *http.Request, err error) {
	s.Log.Error("request failed", "path", r.URL.Path, "request_id", middleware.GetReqID(r.Context()), "err", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}

// observe records latency per route pattern (not raw path, which would explode
// metric cardinality with one series per payment id).
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = "unmatched"
		}
		metrics.HTTPDuration.WithLabelValues(route, r.Method, strconv.Itoa(ww.Status())).Observe(time.Since(start).Seconds())
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
