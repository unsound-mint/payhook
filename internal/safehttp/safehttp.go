// Package safehttp guards outgoing requests to merchant-supplied URLs.
//
// callback_url comes from the merchant, and the worker POSTs to it from inside
// our network, so without checks it can be pointed at the cloud metadata
// endpoint, the database or anything else only we can reach (SSRF). The URL is
// checked when the payment is created, and the resolved IP is checked again
// when connecting, which also covers DNS names that point (or later rebind) to
// internal addresses.
package safehttp

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"
)

var ErrBlocked = errors.New("destination address is not allowed")

// Ranges not covered by the netip predicates in Blocked.
var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), // carrier-grade NAT, used inside some clouds
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("240.0.0.0/4"),  // reserved, includes broadcast
	netip.MustParsePrefix("64:ff9b::/96"), // NAT64: reaches arbitrary IPv4 addresses
	netip.MustParsePrefix("2002::/16"),    // 6to4: embeds an IPv4 address
	netip.MustParsePrefix("2001::/32"),    // Teredo: embeds an IPv4 address
	netip.MustParsePrefix("::/96"),        // IPv4-compatible (deprecated)
}

// Blocked reports whether a is loopback, private, link-local (which includes
// the 169.254.169.254 metadata endpoint), multicast or otherwise not a public
// unicast address. fc00::/7 (IsPrivate) covers AWS's fd00:ec2::254.
func Blocked(a netip.Addr) bool {
	a = a.Unmap()
	if a.IsLoopback() || a.IsPrivate() || a.IsLinkLocalUnicast() || a.IsLinkLocalMulticast() ||
		a.IsInterfaceLocalMulticast() || a.IsMulticast() || a.IsUnspecified() {
		return true
	}
	for _, p := range blockedPrefixes {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// CheckURL validates a callback URL: absolute https with a host, no
// credentials, and not a literal internal address or localhost. allowPrivate
// also permits http and internal targets; it exists for local runs and tests.
func CheckURL(raw string, allowPrivate bool) error {
	if len(raw) > 2048 {
		return errors.New("url is too long")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	secure := u.Scheme == "https" || (allowPrivate && u.Scheme == "http")
	switch {
	case !secure:
		return errors.New("url must use https")
	case u.Hostname() == "":
		return errors.New("url must have a host")
	case u.User != nil:
		return errors.New("url must not contain credentials")
	}
	if allowPrivate {
		return nil
	}
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return ErrBlocked
	}
	if a, err := netip.ParseAddr(host); err == nil && Blocked(a) {
		return ErrBlocked
	}
	return nil
}

// NewClient returns a client for merchant callbacks: no redirects (a 3xx is
// returned as is, so a redirect can't bypass the address check), no proxy from
// the environment, and, unless allowPrivate, connections to blocked addresses
// refused after DNS resolution.
func NewClient(timeout time.Duration, allowPrivate bool) *http.Client {
	dialer := &net.Dialer{Timeout: timeout}
	if !allowPrivate {
		dialer.Control = func(_, address string, _ syscall.RawConn) error {
			ap, err := netip.ParseAddrPort(address)
			if err != nil {
				return fmt.Errorf("%w: %s", ErrBlocked, address)
			}
			if Blocked(ap.Addr()) {
				return fmt.Errorf("%w: %s", ErrBlocked, ap.Addr())
			}
			return nil
		}
	}
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy:               nil,
			DialContext:         dialer.DialContext,
			TLSHandshakeTimeout: timeout,
			MaxIdleConns:        100,
			IdleConnTimeout:     90 * time.Second,
			ForceAttemptHTTP2:   true,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}
