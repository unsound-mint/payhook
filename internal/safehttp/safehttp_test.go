package safehttp

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"
)

func TestBlocked(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1":        true,
		"10.1.2.3":         true,
		"172.16.0.1":       true,
		"192.168.1.1":      true,
		"169.254.169.254":  true, // cloud metadata
		"100.64.0.1":       true,
		"0.0.0.0":          true,
		"255.255.255.255":  true,
		"::1":              true,
		"::ffff:127.0.0.1": true, // v4-mapped loopback
		"fd00:ec2::254":    true, // AWS metadata over IPv6
		"fe80::1":          true,
		"64:ff9b::a00:1":   true, // NAT64 for 10.0.0.1
		"2002:a00:1::1":    true, // 6to4 for 10.0.0.1
		"2001:0:a00:1::1":  true, // Teredo
		"::7f00:1":         true, // IPv4-compatible 127.0.0.1
		"2001:4860::8888":  false,
		"93.184.216.34":    false,
		"2606:4700::1111":  false,
	} {
		if got := Blocked(netip.MustParseAddr(addr)); got != want {
			t.Errorf("Blocked(%s) = %v, want %v", addr, got, want)
		}
	}
}

func TestCheckURL(t *testing.T) {
	for _, tc := range []struct {
		url          string
		allowPrivate bool
		ok           bool
	}{
		{"https://shop.example.com/hook", false, true},
		{"https://93.184.216.34/hook", false, true},
		{"http://shop.example.com/hook", false, false},
		{"ftp://shop.example.com/", false, false},
		{"https:///nohost", false, false},
		{"/relative", false, false},
		{"", false, false},
		{"https://user:pass@shop.example.com/", false, false},
		{"https://localhost/hook", false, false},
		{"https://api.localhost./hook", false, false},
		{"https://127.0.0.1/hook", false, false},
		{"https://169.254.169.254/latest/meta-data/", false, false},
		{"https://[::1]:8443/hook", false, false},
		{"https://10.0.0.5/hook", false, false},
		{"http://127.0.0.1:8080/hook", true, true},
		{"ftp://127.0.0.1/", true, false},
	} {
		if err := CheckURL(tc.url, tc.allowPrivate); (err == nil) != tc.ok {
			t.Errorf("CheckURL(%q, %v) = %v, want ok=%v", tc.url, tc.allowPrivate, err, tc.ok)
		}
	}
}

// The dial-time check is what stops names that resolve to internal addresses,
// which CheckURL can't see. httptest listens on 127.0.0.1.
func TestClientRefusesBlockedAddressesAtDial(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	resp, err := NewClient(time.Second, false).Get(srv.URL)
	if err == nil {
		resp.Body.Close()
	}
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("got %v, want ErrBlocked", err)
	}
	resp, err = NewClient(time.Second, true).Get(srv.URL)
	if err != nil {
		t.Fatalf("allowPrivate: %v", err)
	}
	resp.Body.Close()
}

func TestClientDoesNotFollowRedirects(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		http.Redirect(w, r, "/elsewhere", http.StatusFound)
	}))
	defer srv.Close()

	resp, err := NewClient(time.Second, true).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusFound || hits != 1 {
		t.Fatalf("status %d after %d requests, want 302 after 1", resp.StatusCode, hits)
	}
}
