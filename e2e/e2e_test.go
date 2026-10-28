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
	// The merchant stubs are httptest servers on http://127.0.0.1.
	n := notifier.New(e.pool, e.store, callbackSecret, true, e.log)
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
		WebhookSecrets:        map[string][]byte{"halyk": providerSecret},
		AllowPrivateCallbacks: true,
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

// Many concurrent deliveries of one "succeeded" event racing distinct
// "failed" events for the same payment: exactly one of them changes the
// payment, it ends in the winner's status, and there is exactly one outbox row.
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
			ev := map[string]string{"id": "evt_same", "type": "payment.succeeded", "payment_id": id}
			if i%3 == 0 {
				ev = map[string]string{"id": fmt.Sprintf("evt_failed_%d", i), "type": "payment.failed", "payment_id": id}
			}
			_, out, err := e.sendWebhook(ev)
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
	var status, eventType string
	if err := e.pool.QueryRow(context.Background(), `SELECT count(*), max(o.event_type), max(p.status)
		FROM outbox o JOIN payments p ON p.id = o.aggregate_id WHERE o.aggregate_id = $1`, id).Scan(&n, &eventType, &status); err != nil {
		t.Fatal(err)
	}
	if n != 1 || eventType != payment.EventType(payment.Status(status)) {
		t.Fatalf("outbox rows = %d with %q for status %q, want 1 matching row", n, eventType, status)
	}
	t.Logf("winner: %s, outcomes: %v", status, outcomes)
}

// Concurrent creates with the same key and body: one payment, one 201.
func TestConcurrentCreateSameKey(t *testing.T) {
	e := setup(t)
	b, _ := json.Marshal(map[string]any{"merchant_id": "shop-1", "amount": 500, "currency": "KZT"})

	var wg sync.WaitGroup
	var mu sync.Mutex
	codes := map[int]int{}
	ids := map[string]bool{}
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodPost, e.api.URL+"/v1/payments", bytes.NewReader(b))
			req.Header.Set("Idempotency-Key", "order-concurrent")
			code, out, err := send(req)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			codes[code]++
			if id, ok := out["id"].(string); ok {
				ids[id] = true
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	if codes[http.StatusCreated] != 1 || codes[http.StatusOK] != 19 || len(ids) != 1 {
		t.Fatalf("codes %v, %d distinct ids; want one 201, nineteen 200, one id", codes, len(ids))
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

// deliveries is how many times the broker has delivered messages to the
// notifier consumer, redeliveries included.
func (e *env) deliveries(t *testing.T) uint64 {
	t.Helper()
	c, err := e.js.JS.Consumer(context.Background(), broker.Stream, "notifier")
	if err != nil {
		t.Fatal(err)
	}
	info, err := c.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info.Delivered.Consumer
}

func (e *env) deadLetters(t *testing.T, paymentID string) (n int, lastError string) {
	t.Helper()
	if err := e.pool.QueryRow(context.Background(), `SELECT count(*), COALESCE(max(d.last_error), '')
		FROM dead_letters d JOIN outbox o ON o.id::text = d.message_id WHERE o.aggregate_id = $1`, paymentID).Scan(&n, &lastError); err != nil {
		t.Fatal(err)
	}
	return n, lastError
}

// A merchant that never accepts: after the last attempt the event is parked in
// dead_letters exactly once with the merchant's error, also when the first try
// to park it fails, and the merchant isn't called again after the last attempt.
func TestDeadLetterAfterLastAttempt(t *testing.T) {
	e := setupWithoutNotifier(t)
	e.runNotifier(e.ctx, func(n *notifier.Notifier) { n.Backoff = []time.Duration{50 * time.Millisecond, 50 * time.Millisecond} })

	var calls atomic.Int32
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer merchant.Close()

	// Make the dead-letter inserts fail until the table is back.
	ctx := context.Background()
	if _, err := e.pool.Exec(ctx, `ALTER TABLE dead_letters RENAME TO dead_letters_away`); err != nil {
		t.Fatal(err)
	}
	_, p := e.createPayment(t, "order-dlq", map[string]any{"merchant_id": "shop-1", "amount": 100, "currency": "KZT", "callback_url": merchant.URL})
	id := p["id"].(string)
	if _, out := e.webhook(t, map[string]string{"id": "evt_dlq", "type": "payment.succeeded", "payment_id": id}); out != payment.OutcomeApplied {
		t.Fatalf("webhook: %s", out)
	}
	// Three deliveries make the three attempts; a fifth delivery is only possible
	// if the third and fourth were not terminated, i.e. parking failed twice.
	waitFor(t, 15*time.Second, "redelivery after a failed dead-letter insert", func() bool { return e.deliveries(t) >= 5 })
	if c := calls.Load(); c != 3 {
		t.Fatalf("merchant called %d times before parking, want 3", c)
	}
	if _, err := e.pool.Exec(ctx, `ALTER TABLE dead_letters_away RENAME TO dead_letters`); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 15*time.Second, "dead letter", func() bool { n, _ := e.deadLetters(t, id); return n == 1 })
	if _, lastErr := e.deadLetters(t, id); lastErr != "merchant responded 500" {
		t.Fatalf("last_error = %q, want the merchant's error", lastErr)
	}
	if n := e.processedCallbacks(t, id); n != 1 {
		t.Fatalf("processed = %d, want 1", n)
	}
	if c := calls.Load(); c != 3 {
		t.Fatalf("merchant called %d times, want 3 (no callbacks after the last attempt)", c)
	}
}

// Our own database failing for longer than all the retries together must not
// use up the merchant's attempts: once it's back, the callback goes out.
func TestDatabaseOutageDoesNotUseUpAttempts(t *testing.T) {
	e := setupWithoutNotifier(t)
	e.runNotifier(e.ctx, func(n *notifier.Notifier) { n.Backoff = []time.Duration{50 * time.Millisecond, 50 * time.Millisecond} })

	var calls atomic.Int32
	merchant := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer merchant.Close()

	_, p := e.createPayment(t, "order-outage", map[string]any{"merchant_id": "shop-1", "amount": 100, "currency": "KZT", "callback_url": merchant.URL})
	id := p["id"].(string)
	// Every lookup the notifier does first now fails, like a database outage.
	ctx := context.Background()
	if _, err := e.pool.Exec(ctx, `ALTER TABLE processed_messages RENAME TO processed_messages_away`); err != nil {
		t.Fatal(err)
	}
	if _, out := e.webhook(t, map[string]string{"id": "evt_outage", "type": "payment.succeeded", "payment_id": id}); out != payment.OutcomeApplied {
		t.Fatalf("webhook: %s", out)
	}
	waitFor(t, 20*time.Second, "deliveries past the attempt limit", func() bool { return e.deliveries(t) >= 6 })
	if _, err := e.pool.Exec(ctx, `ALTER TABLE processed_messages_away RENAME TO processed_messages`); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 15*time.Second, "callback after recovery", func() bool { return e.processedCallbacks(t, id) == 1 })
	if n, _ := e.deadLetters(t, id); n != 0 || calls.Load() != 1 {
		t.Fatalf("dead letters = %d, merchant calls = %d; want 0 and 1", n, calls.Load())
	}
}

// Rows that can't become an event must not block the rows behind them.
func TestRelaySkipsInvalidOutboxRows(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	var badShape, noType, good int64
	for _, row := range []struct {
		payload string
		id      *int64
	}{
		{`{"type":"payment.succeeded","amount":"not a number"}`, &badShape},
		{`{"amount":100}`, &noType},
		{`{"type":"payment.succeeded","payment_id":"00000000-0000-0000-0000-000000000000","amount":100}`, &good},
	} {
		if err := e.pool.QueryRow(ctx, `INSERT INTO outbox (aggregate_id, event_type, payload)
			VALUES (gen_random_uuid(), 'payment.succeeded', $1) RETURNING id`, row.payload).Scan(row.id); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 10*time.Second, "valid row published", func() bool {
		var published bool
		err := e.pool.QueryRow(ctx, `SELECT published_at IS NOT NULL FROM outbox WHERE id = $1`, good).Scan(&published)
		return err == nil && published
	})
	for _, id := range []int64{badShape, noType} {
		var failed, published bool
		if err := e.pool.QueryRow(ctx, `SELECT failed_at IS NOT NULL AND last_error <> '', published_at IS NOT NULL
			FROM outbox WHERE id = $1`, id).Scan(&failed, &published); err != nil {
			t.Fatal(err)
		}
		if !failed || published {
			t.Fatalf("row %d: failed=%v published=%v, want failed and not published", id, failed, published)
		}
	}
}
