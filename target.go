package dtmkitex

import (
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
	"time"
)

// Defaults and bounds for query parameters.
const (
	defaultRefreshInterval = 5 * time.Second
	defaultEmptyGrace      = 30 * time.Second
	defaultWeight          = 10

	minRefreshInterval = 100 * time.Millisecond
	maxRefreshInterval = time.Hour
	minEmptyGrace      = time.Second
	maxEmptyGrace      = time.Hour
	minDialTimeout     = 100 * time.Millisecond
	maxDialTimeout     = 5 * time.Minute
	minWeight          = 1
	maxWeight          = 10000
)

// discoveryConfig is the single normalized configuration model produced by URI
// parsing and consumed by providers. Query holds decoded, validated values.
type discoveryConfig struct {
	scheme    string
	endpoints []string
	service   string
	query     url.Values
}

// sensitiveQueryKeys never appear in cache keys or error output.
var sensitiveQueryKeys = map[string]bool{
	"user":     true,
	"password": true,
	"token":    true,
	"username": true,
}

// commonQueryKeys are accepted by every registry scheme.
var commonQueryKeys = map[string]bool{
	"refresh_interval": true,
	"empty_grace":      true,
	"default_weight":   true,
}

// schemeQueryKeys lists the scheme-specific accepted query keys.
var schemeQueryKeys = map[string]map[string]bool{
	SchemeEtcd: {
		"prefix":       true,
		"user":         true,
		"password":     true,
		"dial_timeout": true,
	},
	SchemeConsul: {
		"datacenter": true,
		"token":      true,
		"scheme":     true,
	},
	SchemeNacos: {
		"group":     true,
		"cluster":   true,
		"namespace": true,
		"username":  true,
		"password":  true,
	},
}

// branchTarget is the parsed form of a DTM branch URL.
type branchTarget struct {
	server    string
	method    string
	discovery *discoveryConfig // nil for direct connections
}

// registrationTarget is the parsed form of a DTM server registration URL.
type registrationTarget struct {
	discovery *discoveryConfig
}

// parseBranchTarget parses either a direct "host:port/Service/Method" target or
// a "kitex-<backend>://..." registry target.
func parseBranchTarget(raw string) (branchTarget, error) {
	if raw == "" {
		return branchTarget{}, fmt.Errorf("dtm-driver-kitex: empty target")
	}
	if !strings.Contains(raw, "://") {
		return parseDirectTarget(raw)
	}
	dc, method, err := parseRegistryTarget(raw, true)
	if err != nil {
		return branchTarget{}, err
	}
	return branchTarget{
		server:    dc.serverString(),
		method:    method,
		discovery: dc,
	}, nil
}

// parseRegistrationTarget parses a registry URL that identifies a service to
// register, with no gRPC service/method part.
func parseRegistrationTarget(raw string) (registrationTarget, error) {
	if raw == "" {
		return registrationTarget{}, fmt.Errorf("dtm-driver-kitex: empty target")
	}
	if !strings.Contains(raw, "://") {
		return registrationTarget{}, fmt.Errorf("dtm-driver-kitex: registration target must use a kitex-<backend> scheme")
	}
	dc, _, err := parseRegistryTarget(raw, false)
	if err != nil {
		return registrationTarget{}, err
	}
	return registrationTarget{discovery: dc}, nil
}

// parseDirectTarget parses "host:port/<grpc-service>/<method>".
func parseDirectTarget(raw string) (branchTarget, error) {
	if strings.ContainsAny(raw, "?#") {
		return branchTarget{}, fmt.Errorf("dtm-driver-kitex: direct target must not carry query or fragment")
	}
	slash := strings.IndexByte(raw, '/')
	if slash < 0 {
		return branchTarget{}, fmt.Errorf("dtm-driver-kitex: direct target %q has no method part", raw)
	}
	endpoint := raw[:slash]
	rest := raw[slash+1:]
	if err := validateEndpoint(endpoint); err != nil {
		return branchTarget{}, err
	}
	method, err := validateMethodPath(rest)
	if err != nil {
		return branchTarget{}, err
	}
	return branchTarget{server: endpoint, method: method}, nil
}

// parseRegistryTarget parses and validates a kitex-<backend>:// URL. When
// withMethod is true the path must carry "<discovery-service>/<grpc-service>/<method>";
// otherwise exactly "<discovery-service>".
func parseRegistryTarget(raw string, withMethod bool) (*discoveryConfig, string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		// url.Parse echoes the raw URL, which may carry inline secrets; never
		// surface it in an error.
		return nil, "", fmt.Errorf("dtm-driver-kitex: invalid target URL (malformed host, port, or escaping)")
	}
	if u.User != nil {
		return nil, "", fmt.Errorf("dtm-driver-kitex: target must not contain userinfo")
	}
	if u.Fragment != "" {
		return nil, "", fmt.Errorf("dtm-driver-kitex: target must not contain a fragment")
	}
	if _, known := schemeQueryKeys[u.Scheme]; !known {
		return nil, "", fmt.Errorf("dtm-driver-kitex: unsupported scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, "", fmt.Errorf("dtm-driver-kitex: target %q has no backend endpoints", u.Scheme)
	}

	endpoints, err := parseEndpoints(u.Scheme, u.Host)
	if err != nil {
		return nil, "", err
	}

	segments, err := splitPath(u.Path)
	if err != nil {
		return nil, "", err
	}
	wantSegments := 1
	if withMethod {
		wantSegments = 3
	}
	if len(segments) != wantSegments {
		if withMethod {
			return nil, "", fmt.Errorf("dtm-driver-kitex: target path must be <discovery-service>/<grpc-service>/<method>")
		}
		return nil, "", fmt.Errorf("dtm-driver-kitex: registration path must be <discovery-service>")
	}
	service := segments[0]

	method := ""
	if withMethod {
		method, err = validateMethodPath(segments[1] + "/" + segments[2])
		if err != nil {
			return nil, "", err
		}
	}

	values, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, "", fmt.Errorf("dtm-driver-kitex: invalid query encoding")
	}
	query, err := parseQuery(u.Scheme, values)
	if err != nil {
		return nil, "", err
	}

	return &discoveryConfig{
		scheme:    u.Scheme,
		endpoints: endpoints,
		service:   service,
		query:     query,
	}, method, nil
}

// splitPath splits a URL path into decoded segments, rejecting empty segments
// and any segment that decoded to content containing "/".
func splitPath(path string) ([]string, error) {
	trimmed := strings.TrimPrefix(path, "/")
	if trimmed == "" {
		return nil, fmt.Errorf("dtm-driver-kitex: target path is empty")
	}
	segments := strings.Split(trimmed, "/")
	for _, seg := range segments {
		if seg == "" {
			return nil, fmt.Errorf("dtm-driver-kitex: target path contains an empty segment")
		}
		if strings.Contains(seg, "/") {
			return nil, fmt.Errorf("dtm-driver-kitex: target path segment must not contain '/'")
		}
	}
	return segments, nil
}

// validateMethodPath validates "<grpc-service>/<method>" and returns the
// gRPC full method "/<grpc-service>/<method>".
func validateMethodPath(rest string) (string, error) {
	segments := strings.Split(rest, "/")
	if len(segments) != 2 {
		return "", fmt.Errorf("dtm-driver-kitex: method must be <grpc-service>/<method>")
	}
	for _, seg := range segments {
		if seg == "" {
			return "", fmt.Errorf("dtm-driver-kitex: method must be <grpc-service>/<method>")
		}
		if strings.ContainsAny(seg, "?#") {
			return "", fmt.Errorf("dtm-driver-kitex: method must not contain query or fragment")
		}
	}
	return "/" + rest, nil
}

// parseEndpoints validates the comma-separated backend endpoint list.
func parseEndpoints(scheme, host string) ([]string, error) {
	parts := strings.Split(host, ",")
	for _, p := range parts {
		if err := validateEndpoint(p); err != nil {
			return nil, err
		}
	}
	if scheme != SchemeEtcd && len(parts) > 1 {
		return nil, fmt.Errorf("dtm-driver-kitex: scheme %q accepts exactly one backend endpoint", scheme)
	}
	return parts, nil
}

// validateEndpoint requires a host:port pair with a numeric port.
func validateEndpoint(endpoint string) error {
	if endpoint == "" {
		return fmt.Errorf("dtm-driver-kitex: empty backend endpoint")
	}
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		return fmt.Errorf("dtm-driver-kitex: backend endpoint must be host:port")
	}
	if host == "" {
		return fmt.Errorf("dtm-driver-kitex: backend endpoint has empty host")
	}
	if port == "" {
		return fmt.Errorf("dtm-driver-kitex: backend endpoint has empty port")
	}
	for _, r := range port {
		if r < '0' || r > '9' {
			return fmt.Errorf("dtm-driver-kitex: backend endpoint has non-numeric port")
		}
	}
	return nil
}

// parseQuery validates query keys against the scheme-specific whitelist and
// validates duration and weight values.
func parseQuery(scheme string, q url.Values) (url.Values, error) {
	out := url.Values{}
	for key, values := range q {
		if !commonQueryKeys[key] && !schemeQueryKeys[scheme][key] {
			return nil, fmt.Errorf("dtm-driver-kitex: unsupported query parameter %q for scheme %q", key, scheme)
		}
		if len(values) != 1 {
			return nil, fmt.Errorf("dtm-driver-kitex: query parameter %q must appear once", key)
		}
		value := values[0]
		switch key {
		case "refresh_interval":
			if err := validateDuration(key, value, minRefreshInterval, maxRefreshInterval); err != nil {
				return nil, err
			}
		case "empty_grace":
			if err := validateDuration(key, value, minEmptyGrace, maxEmptyGrace); err != nil {
				return nil, err
			}
		case "dial_timeout":
			if err := validateDuration(key, value, minDialTimeout, maxDialTimeout); err != nil {
				return nil, err
			}
		case "default_weight":
			w, err := parseWeight(value)
			if err != nil {
				return nil, err
			}
			value = fmt.Sprintf("%d", w)
		}
		out.Set(key, value)
	}
	return out, nil
}

func validateDuration(key, value string, min, max time.Duration) error {
	d, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("dtm-driver-kitex: query parameter %q must be a duration", key)
	}
	if d < min || d > max {
		return fmt.Errorf("dtm-driver-kitex: query parameter %q must be within [%s, %s]", key, min, max)
	}
	return nil
}

func parseWeight(value string) (int, error) {
	n := 0
	if value == "" {
		return 0, fmt.Errorf("dtm-driver-kitex: query parameter \"default_weight\" must be an integer")
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("dtm-driver-kitex: query parameter \"default_weight\" must be an integer")
		}
		n = n*10 + int(r-'0')
		if n > maxWeight {
			return 0, fmt.Errorf("dtm-driver-kitex: query parameter \"default_weight\" must be <= %d", maxWeight)
		}
	}
	if n < minWeight {
		return 0, fmt.Errorf("dtm-driver-kitex: query parameter \"default_weight\" must be >= %d", minWeight)
	}
	return n, nil
}

// serverString rebuilds the canonical resolver target: the discovery service
// plus sorted query, without gRPC service/method.
func (dc *discoveryConfig) serverString() string {
	var b strings.Builder
	b.WriteString(dc.scheme)
	b.WriteString("://")
	b.WriteString(strings.Join(dc.endpoints, ","))
	b.WriteString("/")
	b.WriteString(dc.service)
	if len(dc.query) > 0 {
		b.WriteString("?")
		b.WriteString(dc.query.Encode())
	}
	return b.String()
}

var discoveryCacheQueryKeys = map[string][]string{
	SchemeEtcd:   {"dial_timeout", "prefix", "default_weight"},
	SchemeConsul: {"scheme", "datacenter"},
	SchemeNacos:  {"namespace", "group", "cluster"},
}

var registrationCacheQueryKeys = map[string][]string{
	SchemeEtcd:   {"dial_timeout", "prefix"},
	SchemeConsul: {"scheme", "datacenter"},
	SchemeNacos:  {"namespace", "group", "cluster"},
}

// canonicalKey returns the complete stable, secret-free target identity. It is
// retained for diagnostics and compatibility with existing internal tests; the
// provider caches use the narrower identities below.
func (dc *discoveryConfig) canonicalKey() string {
	endpoints := append([]string(nil), dc.endpoints...)
	sort.Strings(endpoints)

	safe := url.Values{}
	for key, values := range dc.query {
		if sensitiveQueryKeys[key] {
			safe.Set(key, "<set>")
			continue
		}
		for _, v := range values {
			safe.Add(key, v)
		}
	}
	return dc.scheme + "|" + strings.Join(endpoints, ",") + "|" + dc.service + "|" + safe.Encode()
}

func (dc *discoveryConfig) discoveryCacheKey() string {
	return dc.cacheKey(discoveryCacheQueryKeys[dc.scheme], false)
}

func (dc *discoveryConfig) registrationCacheKey() string {
	return dc.cacheKey(registrationCacheQueryKeys[dc.scheme], true)
}

func (dc *discoveryConfig) cacheKey(queryKeys []string, includeService bool) string {
	endpoints := append([]string(nil), dc.endpoints...)
	sort.Strings(endpoints)

	projected := url.Values{}
	for _, key := range queryKeys {
		for _, value := range dc.query[key] {
			projected.Add(key, value)
		}
	}

	parts := []string{dc.scheme, strings.Join(endpoints, ",")}
	if includeService {
		parts = append(parts, dc.service)
	}
	parts = append(parts, projected.Encode())
	return strings.Join(parts, "|")
}

// refreshInterval returns the configured refresh interval or the default.
func (dc *discoveryConfig) refreshInterval() time.Duration {
	return dc.durationOrDefault("refresh_interval", defaultRefreshInterval)
}

// emptyGrace returns the configured empty-result grace period or the default.
func (dc *discoveryConfig) emptyGrace() time.Duration {
	return dc.durationOrDefault("empty_grace", defaultEmptyGrace)
}

// weight returns the configured default weight or the default.
func (dc *discoveryConfig) weight() int {
	if v := dc.query.Get("default_weight"); v != "" {
		if w, err := parseWeight(v); err == nil {
			return w
		}
	}
	return defaultWeight
}

func (dc *discoveryConfig) durationOrDefault(key string, def time.Duration) time.Duration {
	if v := dc.query.Get(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
