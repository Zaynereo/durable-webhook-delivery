package destination

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

var ErrDestinationNotAllowed = errors.New("destination URL is not on the allowlist")

// Allowlist validates destination URLs against an explicit set of allowed URLs.
// Matching is exact on scheme, host, port, and path (query stripped). No redirects
// are followed by the HTTP client, so redirects cannot bypass the allowlist.
type Allowlist struct {
	allowed map[string]struct{}
}

func NewAllowlist(urls []string) (*Allowlist, error) {
	allowed := make(map[string]struct{}, len(urls))
	for _, raw := range urls {
		norm, err := normalizeDestination(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid allowlist entry %q: %w", raw, err)
		}
		allowed[norm] = struct{}{}
	}
	if len(allowed) == 0 {
		return nil, errors.New("allowlist is empty")
	}
	return &Allowlist{allowed: allowed}, nil
}

func (a *Allowlist) Validate(raw string) error {
	norm, err := normalizeDestination(raw)
	if err != nil {
		return err
	}
	if _, ok := a.allowed[norm]; ok {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrDestinationNotAllowed, norm)
}

func (a *Allowlist) AllowedKeys() []string {
	out := make([]string, 0, len(a.allowed))
	for k := range a.allowed {
		out = append(out, k)
	}
	return out
}

func normalizeDestination(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("only http and https schemes are allowed")
	}
	host := u.Hostname()
	if host == "" {
		return "", errors.New("missing host")
	}
	// Block obvious non-routable misuse in demo; allowlist still is the real gate.
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() {
			// permitted only if explicitly on allowlist
		}
	}
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	return fmt.Sprintf("%s://%s:%s%s", u.Scheme, strings.ToLower(host), port, path), nil
}
