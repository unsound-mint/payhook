package payment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Webhook outcomes, stored in webhook_events.outcome and exposed as a metric label.
const (
	OutcomeApplied        = "applied"
	OutcomeNoop           = "noop"
	OutcomeDuplicate      = "duplicate"
	OutcomeRejected       = "rejected_transition"
	OutcomeUnknownPayment = "unknown_payment"
	OutcomeIgnoredType    = "ignored_type"
)

var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

type Store struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, now: time.Now}
}

const paymentCols = `id::text, merchant_id, amount, currency, status, provider_ref, callback_url, created_at, updated_at`

func scanPayment(row pgx.Row) (Payment, error) {
	var p Payment
	err := row.Scan(&p.ID, &p.MerchantID, &p.Amount, &p.Currency, &p.Status, &p.ProviderRef, &p.CallbackURL, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, ErrNotFound
	}
	return p, err
}

func requestHash(r CreateRequest) string {
	b, _ := json.Marshal(r) // struct field order is fixed, so this is canonical
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Create inserts a payment, or returns the existing one if the merchant already
// used this idempotency key for the same request. created reports which happened.
func (s *Store) Create(ctx context.Context, idemKey string, r CreateRequest) (p Payment, created bool, err error) {
	hash := requestHash(r)
	p, err = scanPayment(s.pool.QueryRow(ctx, `
		INSERT INTO payments (merchant_id, amount, currency, callback_url, idempotency_key, request_hash)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (merchant_id, idempotency_key) DO NOTHING
		RETURNING `+paymentCols,
		r.MerchantID, r.Amount, r.Currency, r.CallbackURL, idemKey, hash))
	if err == nil {
		return p, true, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return p, false, err
	}
	// Conflict: someone already created a payment with this key.
	var existingHash string
	row := s.pool.QueryRow(ctx, `SELECT `+paymentCols+`, request_hash FROM payments
		WHERE merchant_id = $1 AND idempotency_key = $2`, r.MerchantID, idemKey)
	if err := row.Scan(&p.ID, &p.MerchantID, &p.Amount, &p.Currency, &p.Status, &p.ProviderRef, &p.CallbackURL, &p.CreatedAt, &p.UpdatedAt, &existingHash); err != nil {
		return p, false, err
	}
	if existingHash != hash {
		return Payment{}, false, ErrIdempotencyReuse
	}
	return p, false, nil
}

func (s *Store) Get(ctx context.Context, id string) (Payment, error) {
	if !uuidRe.MatchString(id) {
		return Payment{}, ErrNotFound
	}
	return scanPayment(s.pool.QueryRow(ctx, `SELECT `+paymentCols+` FROM payments WHERE id = $1`, id))
}

// WebhookEvent is the provider's notification body.
type WebhookEvent struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	PaymentID   string `json:"payment_id"`
	ProviderRef string `json:"provider_ref"`
}

var statusByEventType = map[string]Status{
	"payment.succeeded": StatusSucceeded,
	"payment.failed":    StatusFailed,
	"payment.canceled":  StatusCanceled,
	"payment.refunded":  StatusRefunded,
}

// ApplyWebhook records a provider event and, when it is a valid transition,
// updates the payment and writes an outbox event — all in one transaction.
// It is safe to call any number of times with the same event.
func (s *Store) ApplyWebhook(ctx context.Context, provider string, ev WebhookEvent, raw []byte) (string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx) // no-op after Commit

	var (
		p       Payment
		found   bool
		outcome string
		target  Status
		changed bool
	)
	if uuidRe.MatchString(ev.PaymentID) {
		// FOR UPDATE serialises concurrent webhooks for the same payment.
		p, err = scanPayment(tx.QueryRow(ctx, `SELECT `+paymentCols+` FROM payments WHERE id = $1 FOR UPDATE`, ev.PaymentID))
		switch {
		case err == nil:
			found = true
		case !errors.Is(err, ErrNotFound):
			return "", err
		}
	}

	target, knownType := statusByEventType[ev.Type]
	switch {
	case !found:
		outcome = OutcomeUnknownPayment
	case !knownType:
		outcome = OutcomeIgnoredType
	default:
		changed, err = Transition(p.Status, target)
		switch {
		case err != nil:
			outcome = OutcomeRejected
		case changed:
			outcome = OutcomeApplied
		default:
			outcome = OutcomeNoop
		}
	}

	var paymentID *string
	if found {
		paymentID = &p.ID
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO webhook_events (provider, event_id, payment_id, event_type, payload, outcome)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (provider, event_id) DO NOTHING`,
		provider, ev.ID, paymentID, ev.Type, raw, outcome)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() == 0 {
		return OutcomeDuplicate, nil // rollback: nothing else to do
	}

	if changed {
		now := s.now().UTC()
		var ref *string
		if ev.ProviderRef != "" {
			ref = &ev.ProviderRef
		}
		if _, err := tx.Exec(ctx, `UPDATE payments SET status = $2, provider_ref = COALESCE($3, provider_ref), updated_at = $4 WHERE id = $1`,
			p.ID, target, ref, now); err != nil {
			return "", err
		}
		payload, err := json.Marshal(Event{
			Type: EventType(target), PaymentID: p.ID, Status: target,
			Amount: p.Amount, Currency: p.Currency, OccurredAt: now,
		})
		if err != nil {
			return "", err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO outbox (aggregate_id, event_type, payload) VALUES ($1, $2, $3)`,
			p.ID, EventType(target), payload); err != nil {
			return "", err
		}
	}
	return outcome, tx.Commit(ctx)
}

// CallbackURL returns where to notify the merchant about a payment ("" if nowhere).
func (s *Store) CallbackURL(ctx context.Context, paymentID string) (string, error) {
	var url *string
	err := s.pool.QueryRow(ctx, `SELECT callback_url FROM payments WHERE id = $1`, paymentID).Scan(&url)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil || url == nil {
		return "", err
	}
	return *url, nil
}
