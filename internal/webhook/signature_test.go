package webhook

import (
	"errors"
	"testing"
	"time"
)

func TestVerify(t *testing.T) {
	secret := []byte("whsec_test")
	body := []byte(`{"id":"evt_1"}`)
	now := time.Unix(1_800_000_000, 0)
	header := Sign(secret, now, body)

	if err := Verify(secret, header, body, now.Add(time.Minute), 5*time.Minute); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
	tests := map[string]struct {
		secret []byte
		header string
		body   []byte
		now    time.Time
		want   error
	}{
		"tampered body": {secret, header, []byte(`{"id":"evt_2"}`), now, ErrBadSignature},
		"wrong secret":  {[]byte("other"), header, body, now, ErrBadSignature},
		"replayed late": {secret, header, body, now.Add(10 * time.Minute), ErrExpired},
		"from future":   {secret, header, body, now.Add(-10 * time.Minute), ErrExpired},
		"no v1":         {secret, "t=1800000000", body, now, ErrMalformedHeader},
		"garbage":       {secret, "nonsense", body, now, ErrMalformedHeader},
		"bad timestamp": {secret, "t=abc,v1=00", body, now, ErrMalformedHeader},
	}
	for name, tc := range tests {
		if err := Verify(tc.secret, tc.header, tc.body, tc.now, 5*time.Minute); !errors.Is(err, tc.want) {
			t.Errorf("%s: got %v, want %v", name, err, tc.want)
		}
	}
}
