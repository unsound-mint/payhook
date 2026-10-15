// Package payment holds the payment domain model and its state machine.
package payment

import (
	"errors"
	"fmt"
	"time"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusCanceled  Status = "canceled"
	StatusRefunded  Status = "refunded"
)

// transitions lists every allowed status change. Anything not listed is rejected,
// which is what protects us from out-of-order provider webhooks
// (e.g. "succeeded" arriving after "failed").
var transitions = map[Status][]Status{
	StatusPending:   {StatusSucceeded, StatusFailed, StatusCanceled},
	StatusSucceeded: {StatusRefunded},
}

var (
	ErrInvalidTransition = errors.New("invalid status transition")
	ErrNotFound          = errors.New("payment not found")
	ErrIdempotencyReuse  = errors.New("idempotency key reused with a different request")
)

// Transition reports whether moving from -> to is allowed.
// Moving to the current status is a no-op and returns (false, nil):
// a provider retrying the same webhook must not fail.
func Transition(from, to Status) (changed bool, err error) {
	if from == to {
		return false, nil
	}
	for _, next := range transitions[from] {
		if next == to {
			return true, nil
		}
	}
	return false, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
}

type Payment struct {
	ID          string    `json:"id"`
	MerchantID  string    `json:"merchant_id"`
	Amount      int64     `json:"amount"` // minor units (tiyn, cents)
	Currency    string    `json:"currency"`
	Status      Status    `json:"status"`
	ProviderRef *string   `json:"provider_ref,omitempty"`
	CallbackURL *string   `json:"callback_url,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type CreateRequest struct {
	MerchantID  string  `json:"merchant_id"`
	Amount      int64   `json:"amount"`
	Currency    string  `json:"currency"`
	CallbackURL *string `json:"callback_url,omitempty"`
}

func (r CreateRequest) Validate() error {
	switch {
	case r.MerchantID == "":
		return errors.New("merchant_id is required")
	case r.Amount <= 0:
		return errors.New("amount must be positive (minor units)")
	case len(r.Currency) != 3:
		return errors.New("currency must be a 3-letter ISO code")
	}
	return nil
}

// Event is what gets written to the outbox and published to the broker.
type Event struct {
	ID         string    `json:"id"` // outbox id, also used as the broker dedupe id
	Type       string    `json:"type"`
	PaymentID  string    `json:"payment_id"`
	Status     Status    `json:"status"`
	Amount     int64     `json:"amount"`
	Currency   string    `json:"currency"`
	OccurredAt time.Time `json:"occurred_at"`
}

// EventType maps a status change to the public event name.
func EventType(s Status) string { return "payment." + string(s) }
