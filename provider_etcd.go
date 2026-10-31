package dtmkitex

import (
	"fmt"
	"net"
	"time"

	etcd "github.com/kitex-contrib/registry-etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// newEtcdOptions maps the normalized URI config and credentials onto
// registry-etcd's exported Config. Options are written by hand instead of
// WithTLSOpt/WithAuthOpt so a TLS failure can never silently fall back to a
// plaintext client.
func newEtcdOptions(dc *discoveryConfig, creds credentialProfile) ([]etcd.Option, *clientv3.Config, error) {
	etcdCfg := &clientv3.Config{
		Endpoints:   append([]string(nil), dc.endpoints...),
		DialTimeout: 5 * time.Second,
	}
	if dc.query.Has("dial_timeout") {
		// The parser guarantees a valid, in-range duration; a defensive
		// parse failure keeps the 5s default instead of zeroing it.
		if d, err := time.ParseDuration(dc.query.Get("dial_timeout")); err == nil && d > 0 {
			etcdCfg.DialTimeout = d
		}
	}
	if creds.User != "" {
		etcdCfg.Username = creds.User
		etcdCfg.Password = creds.Password
	}
	tlsCfg, err := buildTLSConfig(creds)
	if err != nil {
		return nil, nil, err
	}
	if tlsCfg != nil {
		etcdCfg.TLS = tlsCfg
	}

	opts := []etcd.Option{func(cfg *etcd.Config) {
		cfg.EtcdConfig = etcdCfg
	}}
	if prefix := dc.query.Get("prefix"); prefix != "" {
		opts = append(opts, etcd.WithEtcdServicePrefix(prefix))
	}
	if w := dc.weight(); w > 0 && dc.query.Has("default_weight") {
		opts = append(opts, etcd.WithDefaultWeight(w))
	}
	return opts, etcdCfg, nil
}

// newEtcdProvider builds the etcd discovery resolver and registration
// registry. registry-etcd v0.2.6 owns one clientv3.Client per object and
// exposes no Close, so closeFn stays nil; Shutdown relies on Deregister to
// stop the keepalive goroutines. Note: registry-etcd derives the Deregister
// key from info.Addr plus the KITEX_IP_TO_REGISTRY/KITEX_PORT_TO_REGISTRY
// environment (Register uses the pinned address instead), so a deregistration
// with those variables set may target a stale key; the lease TTL then
// reclaims the registration.
func newEtcdProvider(dc *discoveryConfig, creds credentialProfile) (*provider, error) {
	opts, _, err := newEtcdOptions(dc, creds)
	if err != nil {
		return nil, err
	}
	endpoints := append([]string(nil), dc.endpoints...)
	disc, err := etcd.NewEtcdResolver(endpoints, opts...)
	if err != nil {
		return nil, fmt.Errorf("dtm-driver-kitex: create etcd resolver: %w", err)
	}
	reg, err := etcd.NewEtcdRegistry(endpoints, opts...)
	if err != nil {
		return nil, fmt.Errorf("dtm-driver-kitex: create etcd registry: %w", err)
	}
	return &provider{
		name:      "etcd",
		discovery: disc,
		registry:  reg,
		closeFn:   nil,
		pinAddress: func(addr net.Addr) error {
			etcd.SetFixedAddress(reg, addr)
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
	registerProviderFactory(SchemeEtcd, newEtcdProvider)
}
