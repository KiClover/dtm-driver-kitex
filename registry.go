package dtmkitex

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/cloudwego/kitex/pkg/registry"
)

func newRegistrationInfo(service, endpoint string, weight int, tags map[string]string) (registry.Info, error) {
	if strings.TrimSpace(service) == "" || strings.Contains(service, "/") {
		return registry.Info{}, fmt.Errorf("dtm-driver-kitex: invalid discovery service %q", service)
	}
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil || strings.TrimSpace(host) == "" || strings.TrimSpace(portText) == "" {
		return registry.Info{}, fmt.Errorf("dtm-driver-kitex: registration endpoint %q must be host:port", endpoint)
	}
	port, err := net.LookupPort("tcp", portText)
	if err != nil {
		return registry.Info{}, fmt.Errorf("dtm-driver-kitex: invalid registration port %q", portText)
	}
	if weight < 0 {
		return registry.Info{}, fmt.Errorf("dtm-driver-kitex: registration weight %d must not be negative", weight)
	}

	var addr net.Addr = &net.TCPAddr{IP: net.ParseIP(host), Port: port}
	if net.ParseIP(host) == nil {
		addr = hostnameAddr(endpoint)
	}
	return registry.Info{
		ServiceName:    service,
		Addr:           addr,
		PayloadCodec:   "protobuf",
		Weight:         weight,
		StartTime:      time.Now(),
		Tags:           copyTags(tags),
		SkipListenAddr: true,
	}, nil
}

// hostnameAddr preserves a DNS name instead of resolving it locally. This is
// required for addresses such as host.docker.internal used by Consul agents.
type hostnameAddr string

func (a hostnameAddr) Network() string { return "tcp" }
func (a hostnameAddr) String() string  { return string(a) }

func copyTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	out := make(map[string]string, len(tags))
	for key, value := range tags {
		out[key] = value
	}
	return out
}
