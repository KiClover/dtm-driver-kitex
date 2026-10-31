package dtmkitex

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/cloudwego/kitex/pkg/registry"
)

// registrationKey identifies one DTM server registration. Two registrations
// with the same key are idempotent; the same service+endpoint under a
// different config is a conflict.
type registrationKey struct {
	Scheme   string
	Service  string
	Endpoint string
	ConfigID string
}

// entry is one live DTM server registration: the registry that owns it and
// the exact info used at Register time (Deregister needs the same shape).
type entry struct {
	reg  registry.Registry
	info registry.Info
}

// registrations tracks live DTM server registrations so RegisterService stays
// idempotent and Shutdown can deregister everything.
type registrations struct {
	mu      sync.Mutex
	entries map[registrationKey]entry
}

var defaultRegistrations = &registrations{entries: map[registrationKey]entry{}}

// add records e under key. It reports false when an identical registration
// already exists.
func (r *registrations) add(key registrationKey, e entry) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.entries[key]; ok {
		return false
	}
	r.entries[key] = e
	return true
}

// lookup returns the entry owning key.
func (r *registrations) lookup(key registrationKey) (entry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[key]
	return e, ok
}

// remove drops key from the table.
func (r *registrations) remove(key registrationKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, key)
}

// deregisterAll deregisters every live registration with its original info,
// collecting the first error but always continuing.
func (r *registrations) deregisterAll() error {
	r.mu.Lock()
	keys := make([]registrationKey, 0, len(r.entries))
	for key := range r.entries {
		keys = append(keys, key)
	}
	r.mu.Unlock()

	var firstErr error
	for _, key := range keys {
		e, ok := r.lookup(key)
		if !ok {
			continue
		}
		if err := e.reg.Deregister(&e.info); err != nil && firstErr == nil {
			firstErr = err
		}
		r.remove(key)
	}
	return firstErr
}

// newRegistrationInfo builds the Kitex registration for one DTM server
// endpoint. The endpoint address is authoritative: no environment lookup may
// override it, which is why SkipListenAddr is set and a fixed address is used
// at registration time. A weight of 0 keeps the info's weight unset for
// Deregister-only infos.
func newRegistrationInfo(serviceName, endpoint string, weight int, tags map[string]string) (registry.Info, error) {
	if serviceName == "" {
		return registry.Info{}, fmt.Errorf("dtm-driver-kitex: service name must not be empty")
	}
	if strings.Contains(serviceName, "/") {
		return registry.Info{}, fmt.Errorf("dtm-driver-kitex: service name %q must not contain %q", serviceName, "/")
	}
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		return registry.Info{}, fmt.Errorf("dtm-driver-kitex: endpoint %q is not host:port: %w", endpoint, err)
	}
	if host == "" || port == "" {
		return registry.Info{}, fmt.Errorf("dtm-driver-kitex: endpoint %q is not host:port", endpoint)
	}
	p, err := net.LookupPort("tcp", port)
	if err != nil || p <= 0 {
		return registry.Info{}, fmt.Errorf("dtm-driver-kitex: endpoint %q has invalid tcp port %q", endpoint, port)
	}
	if weight < 0 {
		return registry.Info{}, fmt.Errorf("dtm-driver-kitex: weight %d must not be negative", weight)
	}
	// Hosts may be IP literals or DNS names (k8s services, docker host
	// aliases). Kitex registry SDKs format the registration address from the
	// net.Addr (String/SplitHostPort), so a hostname is carried verbatim in
	// hostnameAddr instead of being eagerly resolved into a possibly
	// context-wrong IP.
	info := registry.Info{
		ServiceName:    serviceName,
		PayloadCodec:   "protobuf",
		Weight:         weight,
		Tags:           copyTags(tags),
		SkipListenAddr: true,
	}
	if ip := net.ParseIP(host); ip != nil {
		info.Addr = &net.TCPAddr{IP: ip, Port: p}
	} else {
		info.Addr = hostnameAddr{host: host, port: p}
	}
	return info, nil
}

func copyTags(tags map[string]string) map[string]string {
	if tags == nil {
		return nil
	}
	out := make(map[string]string, len(tags))
	for k, v := range tags {
		out[k] = v
	}
	return out
}

// hostnameAddr carries a DNS-name endpoint into the registry SDKs. Both
// registry-etcd and registry-consul format the registration address with
// net.SplitHostPort/String(), so implementing net.Addr with the name kept
// verbatim stores the hostname in the registry instead of an eagerly
// resolved (and possibly context-wrong) IP.
type hostnameAddr struct {
	host string
	port int
}

func (a hostnameAddr) Network() string { return "tcp" }
func (a hostnameAddr) String() string  { return net.JoinHostPort(a.host, strconv.Itoa(a.port)) }
