package dtmkitex

import (
	"os"
	"path/filepath"
	"testing"
)

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

func TestConsulConfigTLSRequiresHTTPS(t *testing.T) {
	dc := consulDC(t, "kitex-consul://127.0.0.1:8500/svc")
	if _, err := newConsulConfig(dc, credentialProfile{TLSCA: "ca.pem"}); err == nil {
		t.Fatal("TLS material without scheme=https must be rejected")
	}
}

func TestConsulConfigTLSFiles(t *testing.T) {
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	cert := filepath.Join(dir, "cert.pem")
	key := filepath.Join(dir, "key.pem")
	for _, f := range []string{ca, cert, key} {
		if err := os.WriteFile(f, []byte("placeholder"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	dc := consulDC(t, "kitex-consul://127.0.0.1:8500/svc?scheme=https")
	cfg, err := newConsulConfig(dc, credentialProfile{TLSCA: ca, TLSCert: cert, TLSKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TLSConfig.CAFile != ca || cfg.TLSConfig.CertFile != cert || cfg.TLSConfig.KeyFile != key {
		t.Fatalf("tls config = %+v", cfg.TLSConfig)
	}

	// cert without key is a deterministic construction error.
	if _, err := newConsulConfig(dc, credentialProfile{TLSCA: ca, TLSCert: cert}); err == nil {
		t.Fatal("tls_cert without tls_key must be rejected")
	}
}

func TestConsulProviderConstruction(t *testing.T) {
	dc := consulDC(t, "kitex-consul://127.0.0.1:8500/svc")
	p, err := newConsulProvider(dc, credentialProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if p.name != "consul" {
		t.Errorf("name = %q", p.name)
	}
	if p.discovery == nil || p.registry == nil {
		t.Fatal("discovery and registry must be set")
	}
	if p.pinAddress != nil {
		t.Error("consul provider must not pin registration addresses")
	}
	if p.closeFn != nil {
		t.Error("consul provider must not claim an owned close in v0.2.0")
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
	if p1.name != "consul" {
		t.Errorf("cached provider name = %q", p1.name)
	}
}
