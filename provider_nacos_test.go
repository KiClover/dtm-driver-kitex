package dtmkitex

import (
	"net/url"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/common/constant"
)

func nacosTestConfig(t *testing.T, query url.Values, creds credentialProfile) (*constant.ClientConfig, []constant.ServerConfig) {
	t.Helper()
	dc := &discoveryConfig{
		scheme:    SchemeNacos,
		endpoints: []string{"127.0.0.1:8848"},
		service:   "svc",
		query:     query,
	}
	cfg, servers, err := newNacosConfig(dc, creds)
	if err != nil {
		t.Fatalf("newNacosConfig: %v", err)
	}
	return cfg, servers
}

// TestNacosConfigDefaults pins the default mapping when no query parameters
// and no credentials are given.
func TestNacosConfigDefaults(t *testing.T) {
	cfg, servers := nacosTestConfig(t, url.Values{}, credentialProfile{})

	if cfg.NamespaceId != "" {
		t.Errorf("NamespaceId = %q, want empty (public namespace)", cfg.NamespaceId)
	}
	if !cfg.NotLoadCacheAtStart {
		t.Error("NotLoadCacheAtStart = false, want true")
	}
	if !cfg.UpdateCacheWhenEmpty {
		t.Error("UpdateCacheWhenEmpty = false, want true: server-side empties must propagate so instance loss is observed")
	}
	if cfg.Username != "" || cfg.Password != "" {
		t.Errorf("Username/Password = %q/%q, want empty", cfg.Username, cfg.Password)
	}
	if len(servers) != 1 {
		t.Fatalf("len(servers) = %d, want 1", len(servers))
	}
	if servers[0].IpAddr != "127.0.0.1" || servers[0].Port != 8848 {
		t.Errorf("server = %s:%d, want 127.0.0.1:8848", servers[0].IpAddr, servers[0].Port)
	}

	group, cluster := nacosOptions(&discoveryConfig{scheme: SchemeNacos, query: url.Values{}})
	if group != "DEFAULT_GROUP" {
		t.Errorf("group = %q, want DEFAULT_GROUP", group)
	}
	if cluster != "DEFAULT" {
		t.Errorf("cluster = %q, want DEFAULT", cluster)
	}
}

// TestNacosGroupClusterNamespace maps query parameters onto the client config
// and the registry options.
func TestNacosGroupClusterNamespace(t *testing.T) {
	q := url.Values{}
	q.Set("group", "g1")
	q.Set("cluster", "c1")
	q.Set("namespace", "ns1")

	cfg, _ := nacosTestConfig(t, q, credentialProfile{})
	if cfg.NamespaceId != "ns1" {
		t.Errorf("NamespaceId = %q, want ns1", cfg.NamespaceId)
	}

	dc := &discoveryConfig{scheme: SchemeNacos, query: q}
	group, cluster := nacosOptions(dc)
	if group != "g1" || cluster != "c1" {
		t.Errorf("group/cluster = %q/%q, want g1/c1", group, cluster)
	}
}

// TestNacosCredentialPairing rejects a lone username or password.
func TestNacosCredentialPairing(t *testing.T) {
	dc := &discoveryConfig{
		scheme:    SchemeNacos,
		endpoints: []string{"127.0.0.1:8848"},
		service:   "svc",
		query:     url.Values{},
	}
	if _, _, err := newNacosConfig(dc, credentialProfile{User: "u"}); err == nil {
		t.Error("username without password accepted")
	}
	if _, _, err := newNacosConfig(dc, credentialProfile{Password: "p"}); err == nil {
		t.Error("password without username accepted")
	}
	cfg, _, err := newNacosConfig(dc, credentialProfile{User: "u", Password: "p"})
	if err != nil {
		t.Fatalf("newNacosConfig with pair: %v", err)
	}
	if cfg.Username != "u" || cfg.Password != "p" {
		t.Errorf("Username/Password = %q/%q, want u/p", cfg.Username, cfg.Password)
	}
}

// TestNacosEndpointValidation requires host:port endpoints.
func TestNacosEndpointValidation(t *testing.T) {
	dc := &discoveryConfig{
		scheme:  SchemeNacos,
		service: "svc",
		query:   url.Values{},
	}

	dc.endpoints = []string{"127.0.0.1:8848", "127.0.0.2:8848"}
	_, servers, err := newNacosConfig(dc, credentialProfile{})
	if err != nil {
		t.Fatalf("two endpoints: %v", err)
	}
	if len(servers) != 2 {
		t.Fatalf("len(servers) = %d, want 2", len(servers))
	}

	for _, bad := range []string{"127.0.0.1", ":", "127.0.0.1:notaport", ""} {
		dc.endpoints = []string{bad}
		if _, _, err := newNacosConfig(dc, credentialProfile{}); err == nil {
			t.Errorf("endpoint %q accepted", bad)
		}
	}
}

func nacosConstructorConfig() *discoveryConfig {
	return &discoveryConfig{
		scheme:    SchemeNacos,
		endpoints: []string{"127.0.0.1:18848"},
		service:   "svc",
		query:     url.Values{},
	}
}

// SDK client construction must not dial the unreachable test endpoint.
func TestNacosDiscoveryConstruction(t *testing.T) {
	resolver, err := newNacosDiscovery(nacosConstructorConfig(), credentialProfile{})
	if err != nil {
		t.Fatalf("newNacosDiscovery: %v", err)
	}
	if resolver == nil {
		t.Fatal("discovery resolver must be set")
	}
}

func TestNacosRegistrationConstruction(t *testing.T) {
	provider, err := newNacosRegistration(nacosConstructorConfig(), credentialProfile{})
	if err != nil {
		t.Fatalf("newNacosRegistration: %v", err)
	}
	if provider == nil || provider.registry == nil {
		t.Fatal("registration provider must be set")
	}
}

func TestNacosDiscoverySharedViaCache(t *testing.T) {
	resetRegistrationState(t)
	dc := nacosConstructorConfig()
	first, err := defaultDiscoveries.get(dc, credentialProfile{})
	if err != nil {
		t.Fatalf("first get: %v", err)
	}
	again, err := defaultDiscoveries.get(dc, credentialProfile{})
	if err != nil {
		t.Fatalf("second get: %v", err)
	}
	if first != again {
		t.Error("same effective config returned distinct discovery resolvers")
	}
}
