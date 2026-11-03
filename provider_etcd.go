package dtmkitex

import (
	"fmt"
	"net"
	"time"

	"github.com/cloudwego/kitex/pkg/discovery"
	etcd "github.com/kitex-contrib/registry-etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// newEtcdOptions maps the normalized URI config and credentials onto
// registry-etcd's exported Config. It includes resolver default weight.
func newEtcdOptions(dc *discoveryConfig, creds credentialProfile) ([]etcd.Option, *clientv3.Config, error) {
	return buildEtcdOptions(dc, creds, true)
}

func buildEtcdOptions(dc *discoveryConfig, creds credentialProfile, includeDefaultWeight bool) ([]etcd.Option, *clientv3.Config, error) {
	etcdCfg := &clientv3.Config{
		Endpoints:   append([]string(nil), dc.endpoints...),
		DialTimeout: 5 * time.Second,
	}
	if dc.query.Has("dial_timeout") {
		if d, err := time.ParseDuration(dc.query.Get("dial_timeout")); err == nil && d > 0 {
			etcdCfg.DialTimeout = d
		}
	}
	if creds.User != "" {
		etcdCfg.Username = creds.User
		etcdCfg.Password = creds.Password
	}

	opts := []etcd.Option{func(cfg *etcd.Config) {
		cfg.EtcdConfig = etcdCfg
	}}
	if prefix := dc.query.Get("prefix"); prefix != "" {
		opts = append(opts, etcd.WithEtcdServicePrefix(prefix))
	}
	if includeDefaultWeight {
		if w := dc.weight(); w > 0 && dc.query.Has("default_weight") {
			opts = append(opts, etcd.WithDefaultWeight(w))
		}
	}
	return opts, etcdCfg, nil
}

// newEtcdDiscovery builds the etcd discovery resolver. The registry-etcd
// version used here exposes no Close, so the discovery resolver lives for the
// process lifetime and is safe to reuse across services.
func newEtcdDiscovery(dc *discoveryConfig, creds credentialProfile) (discovery.Resolver, error) {
	opts, _, err := buildEtcdOptions(dc, creds, true)
	if err != nil {
		return nil, err
	}
	resolver, err := etcd.NewEtcdResolver(append([]string(nil), dc.endpoints...), opts...)
	if err != nil {
		return nil, fmt.Errorf("dtm-driver-kitex: create etcd resolver: %w", err)
	}
	return resolver, nil
}

// newEtcdRegistration builds the etcd registration provider. The registry holds
// mutable service state (meta, lease, keepalive goroutines), so each service
// requires its own registry instance.
func newEtcdRegistration(dc *discoveryConfig, creds credentialProfile) (*registrationProvider, error) {
	opts, _, err := buildEtcdOptions(dc, creds, false)
	if err != nil {
		return nil, err
	}
	registry, err := etcd.NewEtcdRegistry(append([]string(nil), dc.endpoints...), opts...)
	if err != nil {
		return nil, fmt.Errorf("dtm-driver-kitex: create etcd registry: %w", err)
	}
	return &registrationProvider{
		registry: registry,
		pinAddress: func(addr net.Addr) error {
			etcd.SetFixedAddress(registry, addr)
			return nil
		},
	}, nil
}

// mustTCPAddr parses a fixed address for SetFixedAddress; the address is
// replaced per registration before Register runs.
func mustTCPAddr(addr string) net.Addr {
	tcp, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		panic(fmt.Sprintf("dtm-driver-kitex: bad fixed address %q: %v", addr, err))
	}
	return tcp
}

func init() {
	registerProviderFactories(SchemeEtcd, newEtcdDiscovery, newEtcdRegistration)
}
