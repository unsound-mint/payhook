package payment

import (
	"errors"
	"testing"
)

func TestTransition(t *testing.T) {
	cases := []struct {
		from, to Status
		changed  bool
		wantErr  bool
	}{
		{StatusPending, StatusSucceeded, true, false},
		{StatusPending, StatusFailed, true, false},
		{StatusPending, StatusCanceled, true, false},
		{StatusSucceeded, StatusRefunded, true, false},
		{StatusSucceeded, StatusSucceeded, false, false}, // duplicate webhook
		{StatusFailed, StatusSucceeded, false, true},     // out-of-order webhook
		{StatusRefunded, StatusSucceeded, false, true},
		{StatusPending, StatusRefunded, false, true},
		{StatusCanceled, StatusFailed, false, true},
	}
	for _, c := range cases {
		changed, err := Transition(c.from, c.to)
		if changed != c.changed || (err != nil) != c.wantErr {
			t.Errorf("%s -> %s: changed=%v err=%v", c.from, c.to, changed, err)
		}
		if err != nil && !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("%s -> %s: error should wrap ErrInvalidTransition", c.from, c.to)
		}
	}
}

func TestCreateRequestValidate(t *testing.T) {
	ok := CreateRequest{MerchantID: "m1", Amount: 1000, Currency: "KZT"}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := []CreateRequest{
		{Amount: 1000, Currency: "KZT"},
		{MerchantID: "m1", Amount: 0, Currency: "KZT"},
		{MerchantID: "m1", Amount: 10, Currency: "KZ"},
	}
	for _, r := range bad {
		if r.Validate() == nil {
			t.Errorf("expected error for %+v", r)
		}
	}
}
