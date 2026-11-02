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

func TestConsulProviderConstruction(t *testing.T) {
	dc := consulDC(t, "kitex-consul://127.0.0.1:8500/svc")
	p, err := newConsulProvider(dc, credentialProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if p.discovery == nil || p.registry == nil {
		t.Fatal("discovery and registry must be set")
	}
	if p.pinAddress != nil {
		t.Error("consul provider must not pin registration addresses")
	}
}

func TestConsulProviderSharedViaCache(t *testing.T) {
	resetRegistrationState(t)
	dc := consulDC(t, "kitex-consul://127.0.0.1:8500/svc")
	p1, err := defaultProviders.get(dc, credentialProfile{})
	if err != nil {
		t.Fatal(err)
	}
	p2, err := defaultProviders.get(dc, credentialProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if p1 != p2 {
		t.Error("same canonical config must share one provider")
	}
}
