// Package webhook verifies signed provider webhooks.
//
// Signature header format (same idea as Stripe):
//
//	X-Signature: t=<unix seconds>,v1=<hex hmac_sha256(secret, "<t>.<raw body>")>
//
// Signing the timestamp together with the body prevents replaying an old,
// validly signed request later.
package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"
)

var (
	ErrMalformedHeader = errors.New("malformed signature header")
	ErrBadSignature    = errors.New("signature mismatch")
	ErrExpired         = errors.New("signature timestamp outside tolerance")
)

func Sign(secret []byte, ts time.Time, body []byte) string {
	t := strconv.FormatInt(ts.Unix(), 10)
	return "t=" + t + ",v1=" + mac(secret, t, body)
}

func Verify(secret []byte, header string, body []byte, now time.Time, tolerance time.Duration) error {
	var t, sig string
	for _, part := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return ErrMalformedHeader
		}
		switch k {
		case "t":
			t = v
		case "v1":
			sig = v
		}
	}
	if t == "" || sig == "" {
		return ErrMalformedHeader
	}
	unix, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return ErrMalformedHeader
	}
	if d := now.Sub(time.Unix(unix, 0)); d > tolerance || d < -tolerance {
		return ErrExpired
	}
	want := mac(secret, t, body)
	// Constant-time comparison so the response time doesn't leak how many
	// leading characters matched.
	if !hmac.Equal([]byte(sig), []byte(want)) {
		return ErrBadSignature
	}
	return nil
}

func mac(secret []byte, t string, body []byte) string {
	h := hmac.New(sha256.New, secret)
	h.Write([]byte(t))
	h.Write([]byte("."))
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}
