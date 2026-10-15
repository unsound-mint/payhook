// Package outbox moves events from the outbox table to the message broker.
//
// Why an outbox: updating the payment and publishing to NATS are two different
// systems, so doing both directly can't be atomic. If the process crashes between
// them, either the event is lost or it's published for a change that was rolled
// back. Writing the event into the same database transaction as the change and
// publishing it afterwards turns that into "at least once" delivery.
package outbox

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/unsound-mint/payhook/internal/metrics"
	"github.com/unsound-mint/payhook/internal/payment"
)

// Publisher sends one event. msgID must be stable across retries so the broker
// can drop duplicates.
type Publisher interface {
	Publish(ctx context.Context, subject, msgID string, data []byte) error
}

type Relay struct {
	pool      *pgxpool.Pool
	pub       Publisher
	batchSize int
	interval  time.Duration
	log       *slog.Logger
}

func NewRelay(pool *pgxpool.Pool, pub Publisher, log *slog.Logger) *Relay {
	return &Relay{pool: pool, pub: pub, batchSize: 100, interval: 200 * time.Millisecond, log: log}
}

func (r *Relay) Run(ctx context.Context) error {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		// Drain while there is a full batch, then wait for the next tick.
		for {
			n, err := r.PublishBatch(ctx)
			if err != nil {
				r.log.Error("outbox batch failed", "err", err)
				break
			}
			if n < r.batchSize {
				break
			}
		}
		r.updateBacklog(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// PublishBatch publishes up to batchSize unpublished events and marks them.
// FOR UPDATE SKIP LOCKED lets several relay replicas run without publishing
// the same rows at the same time.
func (r *Relay) PublishBatch(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, payload FROM outbox
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, r.batchSize)
	if err != nil {
		return 0, err
	}
	type item struct {
		id      int64
		payload []byte
	}
	var items []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.id, &it.payload); err != nil {
			return 0, err
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	var published []int64
	for _, it := range items {
		var ev payment.Event
		if err := json.Unmarshal(it.payload, &ev); err != nil {
			return 0, err
		}
		ev.ID = strconv.FormatInt(it.id, 10)
		data, err := json.Marshal(ev)
		if err != nil {
			return 0, err
		}
		// Stop at the first failure so events keep their order; the rest are
		// retried on the next tick.
		if err := r.pub.Publish(ctx, "payments."+ev.Type, ev.ID, data); err != nil {
			r.log.Warn("publish failed, will retry", "outbox_id", it.id, "err", err)
			break
		}
		published = append(published, it.id)
	}
	if len(published) == 0 {
		return 0, tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, published); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		// The events were published but not marked: they'll be published again
		// and dropped by the broker's dedupe window (same msgID) or the consumer.
		return 0, err
	}
	metrics.OutboxPublished.Add(float64(len(published)))
	return len(published), nil
}

func (r *Relay) updateBacklog(ctx context.Context) {
	var n int64
	if err := r.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n); err == nil {
		metrics.OutboxBacklog.Set(float64(n))
	}
}
