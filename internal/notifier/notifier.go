// Package notifier consumes payment events and calls the merchant's callback URL.
//
// Delivery is at least once end to end, so this consumer:
//   - skips messages it already processed (processed_messages table),
//   - retries failed callbacks with growing delays (NakWithDelay),
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
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/unsound-mint/payhook/internal/broker"
	"github.com/unsound-mint/payhook/internal/metrics"
	"github.com/unsound-mint/payhook/internal/payment"
	"github.com/unsound-mint/payhook/internal/webhook"
)

const consumerName = "notifier"

// DefaultBackoff is the delay between callback attempts. len(Backoff)+1 is the
// max number of attempts.
var DefaultBackoff = []time.Duration{time.Second, 5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute}

const (
	// handleTimeout bounds one message. It stays below AckWait so a stuck handler
	// gives up before the broker hands the message to another worker.
	handleTimeout = 15 * time.Second
	// drainTimeout bounds shutdown; the pod's termination grace period is 30s.
	// Anything still unacked after it is redelivered after AckWait.
	drainTimeout = 20 * time.Second
	// pullBuffer is how many messages are fetched ahead. Their AckWait runs while
	// they sit in the buffer, and shutdown processes the whole buffer, so keep it small.
	pullBuffer = 10
	// dbRetryDelay is the redelivery delay after our own (database) failures.
	dbRetryDelay = time.Second
)

type Notifier struct {
	pool   *pgxpool.Pool
	store  *payment.Store
	client *http.Client
	secret []byte
	log    *slog.Logger

	Backoff []time.Duration // DefaultBackoff unless changed before Run
}

func New(pool *pgxpool.Pool, store *payment.Store, secret []byte, log *slog.Logger) *Notifier {
	return &Notifier{pool: pool, store: store, client: &http.Client{Timeout: 5 * time.Second}, secret: secret, log: log, Backoff: DefaultBackoff}
}

func (n *Notifier) Run(ctx context.Context, js jetstream.JetStream) error {
	cons, err := js.CreateOrUpdateConsumer(ctx, broker.Stream, jetstream.ConsumerConfig{
		Durable:   consumerName,
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   30 * time.Second,
		// Unlimited: handle caps the attempts itself. With a broker-side limit,
		// a message whose last attempt hit a database error (or a failed
		// dead-letter insert) would never be redelivered and would be lost.
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
	}, jetstream.PullMaxMessages(pullBuffer))
	if err != nil {
		return err
	}
	<-ctx.Done()
	// Stop fetching, finish the message in progress and the buffered ones.
	cc.Drain()
	select {
	case <-cc.Closed():
	case <-time.After(drainTimeout):
		n.log.Warn("drain timed out, unacked messages will be redelivered")
	}
	return nil
}

func (n *Notifier) handle(ctx context.Context, msg jetstream.Msg) {
	var ev payment.Event
	if err := json.Unmarshal(msg.Data(), &ev); err != nil || ev.ID == "" {
		n.log.Error("dropping malformed message", "err", err)
		settle(n.log, "term", msg.Term())
		return
	}
	attempt := uint64(1)
	if meta, err := msg.Metadata(); err == nil {
		attempt = meta.NumDelivered
	}
	log := n.log.With("event_id", ev.ID, "type", ev.Type, "attempt", attempt)

	done, err := n.alreadyProcessed(ctx, ev.ID)
	if err != nil {
		n.retry(ctx, log, msg, ev.ID, attempt, fmt.Errorf("dedupe check: %w", err), dbRetryDelay)
		return
	}
	if done {
		metrics.Deliveries.WithLabelValues("duplicate").Inc()
		settle(log, "ack", msg.Ack())
		return
	}
	if int(attempt) > len(n.Backoff)+1 {
		// Redelivered after the last attempt because parking it failed (or our
		// own failures used up the attempts): don't call the merchant again.
		n.retry(ctx, log, msg, ev.ID, attempt, errors.New("redelivered after the last attempt, earlier errors are in the logs"), dbRetryDelay)
		return
	}

	url, err := n.store.CallbackURL(ctx, ev.PaymentID)
	if err != nil && !errors.Is(err, payment.ErrNotFound) {
		n.retry(ctx, log, msg, ev.ID, attempt, fmt.Errorf("callback lookup: %w", err), dbRetryDelay)
		return
	}
	if url == "" {
		n.markProcessed(ctx, ev.ID)
		metrics.Deliveries.WithLabelValues("skipped").Inc()
		settle(log, "ack", msg.Ack())
		return
	}

	if err := n.deliver(ctx, url, msg.Data()); err != nil {
		var delay time.Duration
		if int(attempt) <= len(n.Backoff) {
			delay = n.Backoff[attempt-1]
		}
		n.retry(ctx, log, msg, ev.ID, attempt, err, delay)
		return
	}
	n.markProcessed(ctx, ev.ID)
	metrics.Deliveries.WithLabelValues("ok").Inc()
	settle(log, "ack", msg.Ack())
}

// retry settles a failed attempt: redeliver after delay or, once the attempts
// are used up, park the message in dead_letters. It is only terminated after
// the dead letter is stored; otherwise it is redelivered and parking is retried.
func (n *Notifier) retry(ctx context.Context, log *slog.Logger, msg jetstream.Msg, id string, attempt uint64, cause error, delay time.Duration) {
	if int(attempt) <= len(n.Backoff) {
		log.Warn("attempt failed, retrying", "err", cause, "in", delay)
		metrics.Deliveries.WithLabelValues("retry").Inc()
		settle(log, "nak", msg.NakWithDelay(delay))
		return
	}
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

func (n *Notifier) alreadyProcessed(ctx context.Context, id string) (bool, error) {
	var ok bool
	err := n.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM processed_messages WHERE consumer = $1 AND message_id = $2)`,
		consumerName, id).Scan(&ok)
	return ok, err
}

func (n *Notifier) markProcessed(ctx context.Context, id string) {
	if _, err := n.pool.Exec(ctx, `INSERT INTO processed_messages (consumer, message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		consumerName, id); err != nil {
		n.log.Error("mark processed failed", "event_id", id, "err", err)
	}
}

// deadLetter stores the message and marks it processed in one statement, so a
// crash in between can't leave a dead letter that is then delivered again.
func (n *Notifier) deadLetter(ctx context.Context, id string, payload []byte, cause error) error {
	_, err := n.pool.Exec(ctx, `
		WITH dl AS (
			INSERT INTO dead_letters (consumer, message_id, payload, last_error) VALUES ($1, $2, $3, $4)
		)
		INSERT INTO processed_messages (consumer, message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		consumerName, id, payload, cause.Error())
	return err
}
