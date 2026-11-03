package dtmkitex

import (
	"net"
	"testing"
	"time"

	"github.com/kitex-contrib/registry-etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// applyEtcdOptions runs opts against a fresh Config the way
// NewEtcdResolver/NewEtcdRegistry do, for assertion purposes.
func applyEtcdOptions(opts []etcd.Option) *etcd.Config {
	cfg := &etcd.Config{EtcdConfig: &clientv3.Config{}}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

func etcdDC(t *testing.T, raw string) *discoveryConfig {
	t.Helper()
	dc, _, err := parseRegistryTarget(raw, false)
	if err != nil {
		t.Fatalf("parseRegistryTarget(%q): %v", raw, err)
	}
	return dc
}

func TestEtcdOptionsDefaults(t *testing.T) {
	dc := etcdDC(t, "kitex-etcd://127.0.0.1:2379/svc")
	opts, etcdCfg, err := newEtcdOptions(dc, credentialProfile{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := applyEtcdOptions(opts)
	if got := cfg.EtcdConfig.Endpoints; len(got) != 1 || got[0] != "127.0.0.1:2379" {
		t.Fatalf("endpoints = %v", got)
	}
	if etcdCfg.DialTimeout != 5*time.Second {
		t.Errorf("dial timeout = %s, want 5s default", etcdCfg.DialTimeout)
	}
	if cfg.EtcdConfig.Username != "" || cfg.EtcdConfig.Password != "" {
		t.Errorf("unexpected credentials %q/%q", cfg.EtcdConfig.Username, cfg.EtcdConfig.Password)
	}
	if cfg.EtcdConfig.TLS != nil {
		t.Error("unexpected TLS config")
	}
}

func TestEtcdOptionsMultiEndpointAndDialTimeout(t *testing.T) {
	dc := etcdDC(t, "kitex-etcd://10.0.0.1:2379,10.0.0.2:2379/svc?dial_timeout=2s")
	opts, etcdCfg, err := newEtcdOptions(dc, credentialProfile{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := applyEtcdOptions(opts)
	if got := cfg.EtcdConfig.Endpoints; len(got) != 2 {
		t.Fatalf("endpoints = %v", got)
	}
	if etcdCfg.DialTimeout != 2*time.Second {
		t.Errorf("dial timeout = %s, want 2s", etcdCfg.DialTimeout)
	}
}

func TestEtcdOptionsBadDialTimeout(t *testing.T) {
	// The parser rejects malformed and out-of-range dial_timeout before any
	// factory code runs; the factory keeps the 5s default defensively.
	if _, _, err := parseRegistryTarget("kitex-etcd://127.0.0.1:2379/svc?dial_timeout=abc", false); err == nil {
		t.Fatal("parser must reject non-duration dial_timeout")
	}
	if _, _, err := parseRegistryTarget("kitex-etcd://127.0.0.1:2379/svc?dial_timeout=-1s", false); err == nil {
		t.Fatal("parser must reject out-of-range dial_timeout")
	}
}

func TestEtcdOptionsInlineCredentials(t *testing.T) {
	dc := etcdDC(t, "kitex-etcd://127.0.0.1:2379/svc?user=root&password=secret")
	opts, _, err := newEtcdOptions(dc, credentialProfile{User: "root", Password: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	cfg := applyEtcdOptions(opts)
	if cfg.EtcdConfig.Username != "root" || cfg.EtcdConfig.Password != "secret" {
		t.Errorf("credentials = %q/%q", cfg.EtcdConfig.Username, cfg.EtcdConfig.Password)
	}
}

func TestEtcdOptionsPrefixAndWeight(t *testing.T) {
	dc := etcdDC(t, "kitex-etcd://127.0.0.1:2379/svc?prefix=dtm/services&default_weight=7")
	opts, _, err := newEtcdOptions(dc, credentialProfile{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := applyEtcdOptions(opts)
	if cfg.Prefix != "dtm/services" {
		t.Errorf("prefix = %q, want dtm/services", cfg.Prefix)
	}
	if cfg.DefaultWeight != 7 {
		t.Errorf("default weight = %d, want 7", cfg.DefaultWeight)
	}
}

func TestEtcdOptionsNoWeightOptionByDefault(t *testing.T) {
	dc := etcdDC(t, "kitex-etcd://127.0.0.1:2379/svc")
	opts, _, err := newEtcdOptions(dc, credentialProfile{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := applyEtcdOptions(opts)
	if cfg.DefaultWeight != 0 {
		t.Errorf("default weight = %d, want unset 0 (backend default 10)", cfg.DefaultWeight)
	}
}

// Etcd client construction is lazy: no connection is made until the first
// RPC, so an unreachable endpoint is fine in these constructor tests.
func TestEtcdDiscoveryConstruction(t *testing.T) {
	dc := etcdDC(t, "kitex-etcd://127.0.0.1:1/svc?dial_timeout=100ms")
	resolver, err := newEtcdDiscovery(dc, credentialProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if resolver == nil {
		t.Fatal("discovery resolver must be set")
	}
}

func TestEtcdRegistrationConstruction(t *testing.T) {
	dc := etcdDC(t, "kitex-etcd://127.0.0.1:1/svc?dial_timeout=100ms")
	provider, err := newEtcdRegistration(dc, credentialProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if provider == nil || provider.registry == nil {
		t.Fatal("registration provider must be set")
	}
	if provider.pinAddress == nil {
		t.Fatal("etcd provider must pin registration addresses")
	}
	if err := provider.pinAddress(&net.TCPAddr{IP: net.ParseIP("10.0.0.1"), Port: 8080}); err != nil {
		t.Fatalf("pinAddress: %v", err)
	}
}

func TestEtcdDiscoverySharedViaCache(t *testing.T) {
	resetRegistrationState(t)
	dc := etcdDC(t, "kitex-etcd://127.0.0.1:1/svc?dial_timeout=100ms")
	first, err := defaultDiscoveries.get(dc, credentialProfile{})
	if err != nil {
		t.Fatal(err)
	}
	again, err := defaultDiscoveries.get(dc, credentialProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if first != again {
		t.Error("same effective config must share one discovery resolver")
	}
}
