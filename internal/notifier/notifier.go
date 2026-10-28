// Package notifier consumes payment events and calls the merchant's callback URL.
//
// Delivery is at least once end to end, so this consumer:
//   - skips messages it already processed (processed_messages table),
//   - retries failed callbacks with growing delays (NakWithDelay), counting
//     them in delivery_attempts; failures on our side (database errors,
//     shutdown) are retried without using up an attempt,
//   - parks a message in dead_letters after the last attempt instead of
//     retrying forever.
//
// Merchants should still dedupe on the event id: if we crash after a successful
// callback but before recording it, the callback is sent again.
package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/unsound-mint/payhook/internal/broker"
	"github.com/unsound-mint/payhook/internal/metrics"
	"github.com/unsound-mint/payhook/internal/payment"
	"github.com/unsound-mint/payhook/internal/safehttp"
	"github.com/unsound-mint/payhook/internal/webhook"
)

const consumerName = "notifier"

// DefaultBackoff is the delay between callback attempts. len(Backoff)+1 is the
// max number of attempts.
var DefaultBackoff = []time.Duration{time.Second, 5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute}

// Shutdown budget, which must fit in the pod's 30s termination grace period:
// drainTimeout + broker close (3s) + up to handleTimeout for a handler still
// holding a database connection when the pool closes = 25s.
const (
	// handleTimeout bounds one message: a 5s callback plus a few queries.
	handleTimeout = 10 * time.Second
	// drainTimeout bounds how long shutdown waits for the message in progress.
	// Anything still unacked after it is redelivered after AckWait.
	drainTimeout = 12 * time.Second
	// dbRetryDelay is the redelivery delay after our own (database) failures.
	dbRetryDelay = time.Second
)

type Notifier struct {
	pool   *pgxpool.Pool
	store  *payment.Store
	client *http.Client
	secret []byte
	log    *slog.Logger

	allowPrivate bool // see config.AllowPrivateCallbacks

	Backoff []time.Duration // DefaultBackoff unless changed before Run
}

func New(pool *pgxpool.Pool, store *payment.Store, secret []byte, allowPrivate bool, log *slog.Logger) *Notifier {
	return &Notifier{
		pool: pool, store: store, secret: secret, log: log, Backoff: DefaultBackoff,
		client: safehttp.NewClient(5*time.Second, allowPrivate), allowPrivate: allowPrivate,
	}
}

func (n *Notifier) Run(ctx context.Context, js jetstream.JetStream) error {
	cons, err := js.CreateOrUpdateConsumer(ctx, broker.Stream, jetstream.ConsumerConfig{
		Durable:   consumerName,
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   30 * time.Second,
		// Unlimited: handle caps the callback attempts itself (delivery_attempts).
		// The broker's delivery count also grows on our own failures, and with a
		// broker-side limit a message whose last delivery hit a database error
		// (or a failed dead-letter insert) would never be redelivered.
		MaxDeliver:    -1,
		FilterSubject: broker.Subjects,
	})
	if err != nil {
		return err
	}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		// Detached from ctx: a shutdown signal must not cancel a callback or the
		// DB write that records it halfway, which would send the callback again.
		hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), handleTimeout)
		defer cancel()
		n.handle(hctx, msg)
	},
		// The handler runs serially, and AckWait runs from the moment a message
		// is fetched, so prefetching more would let messages time out in the
		// buffer and be redelivered to another worker (a second callback).
		// Fetch one at a time; handle also resets AckWait when it starts.
		jetstream.PullMaxMessages(1))
	if err != nil {
		return err
	}
	<-ctx.Done()
	// Stop fetching and finish the message in progress (and the prefetched one).
	cc.Drain()
	select {
	case <-cc.Closed():
	case <-time.After(drainTimeout):
		n.log.Warn("drain timed out, unacked messages will be redelivered")
	}
	return nil
}

func (n *Notifier) handle(ctx context.Context, msg jetstream.Msg) {
	// Restart AckWait now that processing begins (see PullMaxMessages in Run).
	settle(n.log, "in_progress", msg.InProgress())

	var ev payment.Event
	if err := json.Unmarshal(msg.Data(), &ev); err != nil || ev.ID == "" {
		n.log.Error("dropping malformed message", "err", err)
		settle(n.log, "term", msg.Term())
		return
	}
	delivery := uint64(1)
	if meta, err := msg.Metadata(); err == nil {
		delivery = meta.NumDelivered
	}
	log := n.log.With("event_id", ev.ID, "type", ev.Type, "delivery", delivery)
	defer func() {
		if r := recover(); r != nil {
			n.failedAttempt(ctx, log, msg, ev.ID, fmt.Errorf("panic: %v", r), false)
		}
	}()

	// Our own failures below are retried without limit and never count as an
	// attempt: a database outage must not use up the merchant's retries.
	st, err := n.state(ctx, ev.ID)
	if err != nil {
		n.ourFailure(log, msg, fmt.Errorf("state lookup: %w", err))
		return
	}
	if st.processed {
		metrics.Deliveries.WithLabelValues("duplicate").Inc()
		settle(log, "ack", msg.Ack())
		return
	}
	if st.attempts >= n.maxAttempts() {
		// The last attempt failed but parking the message didn't succeed.
		n.giveUp(ctx, log, msg, ev.ID, errors.New(st.lastError))
		return
	}

	url, err := n.store.CallbackURL(ctx, ev.PaymentID)
	if err != nil && !errors.Is(err, payment.ErrNotFound) {
		n.ourFailure(log, msg, fmt.Errorf("callback lookup: %w", err))
		return
	}
	if url == "" {
		n.markProcessed(ctx, ev.ID)
		metrics.Deliveries.WithLabelValues("skipped").Inc()
		settle(log, "ack", msg.Ack())
		return
	}

	// Re-checked here for URLs stored before validation existed; retrying won't fix them.
	if err := safehttp.CheckURL(url, n.allowPrivate); err != nil {
		n.giveUp(ctx, log, msg, ev.ID, fmt.Errorf("callback_url: %w", err))
		return
	}
	if err := n.deliver(ctx, url, msg.Data()); err != nil {
		// A blocked address (resolved at dial time) won't change on retry.
		n.failedAttempt(ctx, log, msg, ev.ID, err, errors.Is(err, safehttp.ErrBlocked))
		return
	}
	n.markProcessed(ctx, ev.ID)
	metrics.Deliveries.WithLabelValues("ok").Inc()
	settle(log, "ack", msg.Ack())
}

func (n *Notifier) maxAttempts() int { return len(n.Backoff) + 1 }

// ourFailure redelivers after a failure on our side, without counting an attempt.
func (n *Notifier) ourFailure(log *slog.Logger, msg jetstream.Msg, err error) {
	log.Error("processing failed, will retry", "err", err, "in", dbRetryDelay)
	metrics.Deliveries.WithLabelValues("error").Inc()
	settle(log, "nak", msg.NakWithDelay(dbRetryDelay))
}

// failedAttempt records a failed callback and either schedules the next attempt
// or, after the last one (or when final), parks the message in dead_letters.
func (n *Notifier) failedAttempt(ctx context.Context, log *slog.Logger, msg jetstream.Msg, id string, cause error, final bool) {
	attempts, err := n.recordAttempt(ctx, id, cause)
	if err != nil {
		// Not counted, so the merchant may get one extra attempt; better than
		// giving up because our database was briefly unavailable.
		log.Error("recording attempt failed", "err", err, "cause", cause)
		settle(log, "nak", msg.NakWithDelay(dbRetryDelay))
		return
	}
	if final || attempts >= n.maxAttempts() {
		n.giveUp(ctx, log, msg, id, cause)
		return
	}
	delay := n.Backoff[attempts-1]
	log.Warn("callback failed, retrying", "err", cause, "attempt", attempts, "in", delay)
	metrics.Deliveries.WithLabelValues("retry").Inc()
	settle(log, "nak", msg.NakWithDelay(delay))
}

// giveUp parks the message in dead_letters. It is only terminated after the
// dead letter is stored; otherwise it is redelivered and parking is retried.
func (n *Notifier) giveUp(ctx context.Context, log *slog.Logger, msg jetstream.Msg, id string, cause error) {
	if err := n.deadLetter(ctx, id, msg.Data(), cause); err != nil {
		log.Error("dead letter insert failed, will retry", "err", err, "cause", cause)
		settle(log, "nak", msg.NakWithDelay(dbRetryDelay))
		return
	}
	log.Error("giving up, moved to dead letters", "err", cause)
	metrics.Deliveries.WithLabelValues("dead_letter").Inc()
	settle(log, "term", msg.Term())
}

// deliver POSTs the event, signed the same way providers sign webhooks to us,
// so the merchant can verify it came from us.
func (n *Notifier) deliver(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Signature", webhook.Sign(n.secret, time.Now(), body))
	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	// Read a bounded amount so the connection can be reused, then drop the rest.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("merchant responded %d", resp.StatusCode)
	}
	return nil
}

// settle logs a failed ack/nak/term. The message will then be redelivered after
// AckWait, which the dedupe check makes harmless.
func settle(log *slog.Logger, op string, err error) {
	if err != nil {
		log.Warn("settling message failed", "op", op, "err", err)
	}
}

type msgState struct {
	processed bool
	attempts  int    // failed callbacks so far
	lastError string // of the last failed callback
}

func (n *Notifier) state(ctx context.Context, id string) (msgState, error) {
	var st msgState
	err := n.pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM processed_messages WHERE consumer = $1 AND message_id = $2),
		       COALESCE(a.attempts, 0), COALESCE(a.last_error, '')
		FROM (SELECT 1) one
		LEFT JOIN delivery_attempts a ON a.consumer = $1 AND a.message_id = $2`,
		consumerName, id).Scan(&st.processed, &st.attempts, &st.lastError)
	return st, err
}

// recordAttempt counts a failed callback and returns the new total.
func (n *Notifier) recordAttempt(ctx context.Context, id string, cause error) (int, error) {
	var attempts int
	err := n.pool.QueryRow(ctx, `
		INSERT INTO delivery_attempts (consumer, message_id, attempts, last_error) VALUES ($1, $2, 1, $3)
		ON CONFLICT (consumer, message_id) DO UPDATE
		SET attempts = delivery_attempts.attempts + 1, last_error = EXCLUDED.last_error, updated_at = now()
		RETURNING attempts`,
		consumerName, id, cause.Error()).Scan(&attempts)
	return attempts, err
}

func (n *Notifier) markProcessed(ctx context.Context, id string) {
	if _, err := n.pool.Exec(ctx, `INSERT INTO processed_messages (consumer, message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		consumerName, id); err != nil {
		n.log.Error("mark processed failed", "event_id", id, "err", err)
	}
}

// deadLetter marks the message processed and stores it in one statement, so a
// crash in between can't leave a dead letter that is then delivered again. The
// dead letter is only written by whoever inserted the processed row, so two
// copies of the message racing here produce one dead letter, not two.
func (n *Notifier) deadLetter(ctx context.Context, id string, payload []byte, cause error) error {
	_, err := n.pool.Exec(ctx, `
		WITH p AS (
			INSERT INTO processed_messages (consumer, message_id) VALUES ($1, $2)
			ON CONFLICT DO NOTHING
			RETURNING consumer, message_id
		)
		INSERT INTO dead_letters (consumer, message_id, payload, last_error)
		SELECT consumer, message_id, $3::jsonb, $4::text FROM p`,
		consumerName, id, payload, cause.Error())
	return err
}
