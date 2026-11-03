package dtmkitex

import "testing"

func consulDC(t *testing.T, raw string) *discoveryConfig {
	t.Helper()
	dc, _, err := parseRegistryTarget(raw, false)
	if err != nil {
		t.Fatalf("parseRegistryTarget(%q): %v", raw, err)
	}
	return dc
}

func TestConsulConfigDefaults(t *testing.T) {
	dc := consulDC(t, "kitex-consul://127.0.0.1:8500/svc")
	cfg, err := newConsulConfig(dc, credentialProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Address != "127.0.0.1:8500" {
		t.Errorf("address = %q", cfg.Address)
	}
	if cfg.Scheme != "http" { // consul api.DefaultConfig() defaults to http
		t.Errorf("scheme = %q, want http", cfg.Scheme)
	}
	if cfg.Datacenter != "" {
		t.Errorf("datacenter = %q, want default", cfg.Datacenter)
	}
	if cfg.Token != "" {
		t.Errorf("token = %q, want empty", cfg.Token)
	}
	if cfg.TLSConfig.CAFile != "" {
		t.Error("unexpected TLS config")
	}
}

func TestConsulConfigDatacenterTokenScheme(t *testing.T) {
	dc := consulDC(t, "kitex-consul://127.0.0.1:8500/svc?datacenter=dc2&scheme=https")
	cfg, err := newConsulConfig(dc, credentialProfile{Token: "acl-token"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Datacenter != "dc2" {
		t.Errorf("datacenter = %q", cfg.Datacenter)
	}
	if cfg.Scheme != "https" {
		t.Errorf("scheme = %q", cfg.Scheme)
	}
	if cfg.Token != "acl-token" {
		t.Errorf("token = %q", cfg.Token)
	}
}

func TestConsulConfigBadScheme(t *testing.T) {
	// scheme=ftp is rejected by the provider, though the URI whitelist in
	// target.go only checks the key name.
	dc := consulDC(t, "kitex-consul://127.0.0.1:8500/svc?scheme=ftp")
	if _, err := newConsulConfig(dc, credentialProfile{}); err == nil {
		t.Fatal("scheme=ftp must be rejected")
	}
}

func TestConsulDiscoveryConstruction(t *testing.T) {
	dc := consulDC(t, "kitex-consul://127.0.0.1:8500/svc")
	resolver, err := newConsulDiscovery(dc, credentialProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if resolver == nil {
		t.Fatal("discovery resolver must be set")
	}
}

func TestConsulRegistrationConstruction(t *testing.T) {
	dc := consulDC(t, "kitex-consul://127.0.0.1:8500/svc")
	provider, err := newConsulRegistration(dc, credentialProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if provider == nil || provider.registry == nil {
		t.Fatal("registration provider must be set")
	}
	if provider.pinAddress != nil {
		t.Error("consul provider must not pin registration addresses")
	}
}

func TestConsulDiscoverySharedViaCache(t *testing.T) {
	resetRegistrationState(t)
	dc := consulDC(t, "kitex-consul://127.0.0.1:8500/svc")
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
