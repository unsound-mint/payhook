// End-to-end tests against real PostgreSQL and NATS JetStream (via
// testcontainers). They exercise the API, the outbox relay and the notifier
// together. Skipped when Docker isn't available, except in CI (CI env var set),
// where a missing Docker must fail the build instead of passing silently.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/unsound-mint/payhook/internal/broker"
	"github.com/unsound-mint/payhook/internal/db"
	"github.com/unsound-mint/payhook/internal/httpapi"
	"github.com/unsound-mint/payhook/internal/notifier"
	"github.com/unsound-mint/payhook/internal/outbox"
	"github.com/unsound-mint/payhook/internal/payment"
	"github.com/unsound-mint/payhook/internal/webhook"
)

var (
	providerSecret = []byte("whsec_provider")
	callbackSecret = []byte("whsec_callback")
)

type env struct {
	ctx   context.Context // cancelled when the test ends
	api   *httptest.Server
	pool  *pgxpool.Pool
	js    *broker.JetStream
	store *payment.Store
	log   *slog.Logger
}

// setup starts everything, including a notifier that runs until the test ends.
func setup(t *testing.T) *env {
	t.Helper()
	e := setupWithoutNotifier(t)
	e.runNotifier(e.ctx, nil)
	return e
}

// runNotifier starts a notifier, optionally adjusted by configure, and returns
// a channel that receives Run's result.
func (e *env) runNotifier(ctx context.Context, configure func(*notifier.Notifier)) <-chan error {
	n := notifier.New(e.pool, e.store, callbackSecret, e.log)
	if configure != nil {
		configure(n)
	}
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx, e.js.JS) }()
	return done
}

func setupWithoutNotifier(t *testing.T) *env {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	pg, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("payhook"), tcpostgres.WithUsername("payhook"), tcpostgres.WithPassword("payhook"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)))
	if err != nil {
		noDocker(t, err)
	}
	t.Cleanup(func() { pg.Terminate(context.Background()) })
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	nc, err := tcnats.Run(ctx, "nats:2.11-alpine")
	if err != nil {
		noDocker(t, err)
	}
	t.Cleanup(func() { nc.Terminate(context.Background()) })
	natsURL, err := nc.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}

	pool, err := db.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx, pool); err != nil { // must be re-runnable
		t.Fatal(err)
	}

	js, err := broker.Connect(ctx, natsURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(js.Close)

	store := payment.NewStore(pool)
	go outbox.NewRelay(pool, js, log).Run(ctx)

	api := httptest.NewServer((&httpapi.Server{
		Store: store, Pool: pool, Log: log, Now: time.Now,
		WebhookSecrets: map[string][]byte{"halyk": providerSecret},
	}).Routes())
	t.Cleanup(api.Close)
	return &env{ctx: ctx, api: api, pool: pool, js: js, store: store, log: log}
}

func (e *env) processedCallbacks(t *testing.T, paymentID string) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), `SELECT count(*) FROM processed_messages pm
		JOIN outbox o ON o.id::text = pm.message_id WHERE o.aggregate_id = $1`, paymentID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func noDocker(t *testing.T, err error) {
	t.Helper()
	if os.Getenv("CI") != "" {
		t.Fatalf("docker unavailable in CI: %v", err)
	}
	t.Skipf("docker unavailable: %v", err)
}

// waitFor polls cond until it holds or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (e *env) createPayment(t *testing.T, key string, body map[string]any) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, e.api.URL+"/v1/payments", bytes.NewReader(b))
	req.Header.Set("Idempotency-Key", key)
	return do(t, req)
}

func (e *env) webhook(t *testing.T, ev map[string]string) (int, string) {
	t.Helper()
	code, outcome, err := e.sendWebhook(ev)
	if err != nil {
		t.Fatal(err)
	}
	return code, outcome
}

// sendWebhook is the goroutine-safe variant: it returns the error instead of
// calling t.Fatal, which must only run on the test goroutine.
func (e *env) sendWebhook(ev map[string]string) (int, string, error) {
	b, _ := json.Marshal(ev)
	req, _ := http.NewRequest(http.MethodPost, e.api.URL+"/v1/webhooks/halyk", bytes.NewReader(b))
	req.Header.Set("X-Signature", webhook.Sign(providerSecret, time.Now(), b))
	code, out, err := send(req)
	outcome, _ := out["outcome"].(string)
	return code, outcome, err
}

func do(t *testing.T, req *http.Request) (int, map[string]any) {
	t.Helper()
	code, out, err := send(req)
	if err != nil {
		t.Fatal(err)
	}
	return code, out
}

func send(req *http.Request) (int, map[string]any, error) {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out, nil
}

func TestPaymentLifecycle(t *testing.T) {
	e := setup(t)

	// Merchant endpoint: fails the first callback, accepts the rest.
	var calls, accepted atomic.Int32
	received := make(chan payment.Event, 10)
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := webhook.Verify(callbackSecret, r.Header.Get("X-Signature"), body, time.Now(), time.Minute); err != nil {
			t.Errorf("callback signature: %v", err)
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		accepted.Add(1)
		var ev payment.Event
		json.Unmarshal(body, &ev)
		received <- ev
	}))
	defer merchant.Close()

	body := map[string]any{"merchant_id": "shop-1", "amount": 1500000, "currency": "KZT", "callback_url": merchant.URL}
	code, p := e.createPayment(t, "order-42", body)
	if code != http.StatusCreated || p["status"] != "pending" {
		t.Fatalf("create: %d %v", code, p)
	}
	id := p["id"].(string)

	// Same key, same body: same payment, 200 instead of 201.
	if code, again := e.createPayment(t, "order-42", body); code != http.StatusOK || again["id"] != id {
		t.Fatalf("idempotent replay: %d %v", code, again)
	}
	// Same key, different body: refused.
	body["amount"] = 1
	if code, _ := e.createPayment(t, "order-42", body); code != http.StatusUnprocessableEntity {
		t.Fatalf("key reuse: got %d", code)
	}

	ev := map[string]string{"id": "evt_1", "type": "payment.succeeded", "payment_id": id, "provider_ref": "halyk-777"}
	if code, out := e.webhook(t, ev); code != 200 || out != payment.OutcomeApplied {
		t.Fatalf("webhook: %d %s", code, out)
	}
	if _, out := e.webhook(t, ev); out != payment.OutcomeDuplicate {
		t.Fatalf("redelivery: %s", out)
	}
	late := map[string]string{"id": "evt_2", "type": "payment.failed", "payment_id": id}
	if _, out := e.webhook(t, late); out != payment.OutcomeRejected {
		t.Fatalf("out-of-order failed after succeeded: %s", out)
	}

	req, _ := http.NewRequest(http.MethodGet, e.api.URL+"/v1/payments/"+id, nil)
	if _, got := do(t, req); got["status"] != "succeeded" || got["provider_ref"] != "halyk-777" {
		t.Fatalf("get: %v", got)
	}

	select {
	case got := <-received:
		if got.Type != "payment.succeeded" || got.PaymentID != id {
			t.Fatalf("callback: %+v", got)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("merchant never received the callback")
	}
	// processed_messages is written after the merchant accepted the callback and
	// before the ack, so once the row exists no further callback can be sent.
	waitFor(t, 5*time.Second, "callback recorded as processed", func() bool { return e.processedCallbacks(t, id) == 1 })
	if calls.Load() != 2 || accepted.Load() != 1 {
		t.Fatalf("calls=%d accepted=%d, want 2 and 1 (one retry, no duplicates)", calls.Load(), accepted.Load())
	}
}

func TestBadSignatureRejected(t *testing.T) {
	e := setup(t)
	b := []byte(`{"id":"evt_x","type":"payment.succeeded","payment_id":"00000000-0000-0000-0000-000000000000"}`)
	req, _ := http.NewRequest(http.MethodPost, e.api.URL+"/v1/webhooks/halyk", bytes.NewReader(b))
	req.Header.Set("X-Signature", webhook.Sign([]byte("attacker"), time.Now(), b))
	if code, _ := do(t, req); code != http.StatusUnauthorized {
		t.Fatalf("got %d, want 401", code)
	}
}

// Many concurrent deliveries — same event and competing events — must change
// the payment exactly once and write exactly one outbox row.
func TestConcurrentWebhooks(t *testing.T) {
	e := setup(t)
	_, p := e.createPayment(t, "order-race", map[string]any{"merchant_id": "shop-1", "amount": 100, "currency": "KZT"})
	id := p["id"].(string)

	var wg sync.WaitGroup
	var mu sync.Mutex
	outcomes := map[string]int{}
	for i := range 30 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			evID := "evt_same"
			if i%3 == 0 {
				evID = fmt.Sprintf("evt_other_%d", i) // different event, same target status
			}
			_, out, err := e.sendWebhook(map[string]string{"id": evID, "type": "payment.succeeded", "payment_id": id})
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			outcomes[out]++
			mu.Unlock()
		}()
	}
	wg.Wait()

	if outcomes[payment.OutcomeApplied] != 1 {
		t.Fatalf("applied %d times, want exactly 1: %v", outcomes[payment.OutcomeApplied], outcomes)
	}
	var n int
	if err := e.pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE aggregate_id = $1`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("outbox rows = %d, want 1", n)
	}
}

// SIGTERM in the middle of a callback: the callback must complete and be
// recorded, and Run must not return before that. Otherwise the merchant gets
// the callback again from another worker.
func TestShutdownFinishesInFlightCallback(t *testing.T) {
	e := setupWithoutNotifier(t)
	ctx, stop := context.WithCancel(e.ctx)
	defer stop()
	done := e.runNotifier(ctx, nil)

	var calls atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
	}))
	defer merchant.Close()
	defer unblock() // runs first, so Close doesn't wait on a blocked handler

	_, p := e.createPayment(t, "order-shutdown", map[string]any{"merchant_id": "shop-1", "amount": 100, "currency": "KZT", "callback_url": merchant.URL})
	id := p["id"].(string)
	if _, out := e.webhook(t, map[string]string{"id": "evt_shutdown", "type": "payment.succeeded", "payment_id": id}); out != payment.OutcomeApplied {
		t.Fatalf("webhook: %s", out)
	}
	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("callback never started")
	}

	stop() // what SIGTERM does in cmd/worker
	select {
	case <-done:
		t.Fatal("Run returned while a callback was still in flight")
	case <-time.After(300 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after the callback finished")
	}
	if n := e.processedCallbacks(t, id); n != 1 || calls.Load() != 1 {
		t.Fatalf("processed=%d calls=%d, want 1 and 1", n, calls.Load())
	}
}
