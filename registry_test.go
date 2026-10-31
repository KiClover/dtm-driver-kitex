package dtmkitex

import (
	"context"
	"errors"
	"net"
	"testing"

	. "github.com/dtm-labs/dtmdriver-kitex/internal/testkit"
)

// resetRegistrationState clears process-global provider and registration
// state so tests are independent.
func resetRegistrationState(t *testing.T) {
	t.Helper()
	defaultRegistrations.mu.Lock()
	defaultRegistrations.entries = map[registrationKey]entry{}
	defaultRegistrations.mu.Unlock()
	defaultProviders.mu.Lock()
	defaultProviders.providers = map[string]*provider{}
	defaultProviders.mu.Unlock()
}

// installFakeProvider installs a factory returning the fake registry-backed
// provider for scheme.
func installFakeProvider(t *testing.T, scheme string, reg *Registry, pin func(net.Addr) error) {
	t.Helper()
	prev, had := providerFactories[scheme]
	providerFactories[scheme] = func(dc *discoveryConfig, creds credentialProfile) (*provider, error) {
		return &provider{
			name:       scheme,
			discovery:  NewDiscovery(scheme, dc.service),
			registry:   reg,
			pinAddress: pin,
		}, nil
	}
	t.Cleanup(func() {
		if had {
			providerFactories[scheme] = prev
		} else {
			delete(providerFactories, scheme)
		}
	})
}

func TestNewRegistrationInfoValidation(t *testing.T) {
	cases := []struct {
		name      string
		service   string
		endpooint string
		weight    int
		wantErr   bool
	}{
		{"valid", "svc", "127.0.0.1:8080", 10, false},
		{"empty service", "", "127.0.0.1:8080", 10, true},
		{"slash service", "a/b", "127.0.0.1:8080", 10, true},
		{"no port", "svc", "127.0.0.1", 10, true},
		{"bad port", "svc", "127.0.0.1:abc", 10, true},
		{"bad host", "svc", "notahost:8080", 10, true},
		{"negative weight", "svc", "127.0.0.1:8080", -1, true},
		{"zero weight tolerated", "svc", "127.0.0.1:8080", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newRegistrationInfo(tc.service, tc.endpooint, tc.weight, nil)
			if (err != nil) != tc.wantErr {
				t.Fatalf("newRegistrationInfo(%q, %q, %d) err = %v, wantErr %v", tc.service, tc.endpooint, tc.weight, err, tc.wantErr)
			}
		})
	}
}

func TestNewRegistrationInfoShape(t *testing.T) {
	tags := map[string]string{"dtm": "yes"}
	info, err := newRegistrationInfo("svc", "10.0.0.1:9090", 30, tags)
	if err != nil {
		t.Fatal(err)
	}
	if info.ServiceName != "svc" {
		t.Errorf("ServiceName = %q", info.ServiceName)
	}
	tcp, ok := info.Addr.(*net.TCPAddr)
	if !ok {
		t.Fatalf("Addr type %T, want *net.TCPAddr", info.Addr)
	}
	if tcp.String() != "10.0.0.1:9090" {
		t.Errorf("Addr = %v", tcp)
	}
	if info.PayloadCodec != "protobuf" {
		t.Errorf("PayloadCodec = %q", info.PayloadCodec)
	}
	if info.Weight != 30 {
		t.Errorf("Weight = %d", info.Weight)
	}
	if !info.SkipListenAddr {
		t.Error("SkipListenAddr must be true")
	}
	tags["mutated"] = "yes"
	if _, mutated := info.Tags["mutated"]; mutated {
		t.Error("Tags must be deep-copied")
	}
}

func TestRegisterServiceIdempotent(t *testing.T) {
	resetRegistrationState(t)
	reg := NewRegistry()
	installFakeProvider(t, SchemeEtcd, reg, nil)

	dc := mustConfig(t, SchemeEtcd+"://127.0.0.1:2379/svc")
	for i := 0; i < 3; i++ {
		if err := registerService(dc, credentialProfile{}, "127.0.0.1:8080"); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if got := len(reg.Registers()); got != 1 {
		t.Fatalf("Register called %d times, want 1", got)
	}
}

func TestRegisterServiceDistinctEndpoints(t *testing.T) {
	resetRegistrationState(t)
	reg := NewRegistry()
	installFakeProvider(t, SchemeEtcd, reg, nil)

	dc := mustConfig(t, SchemeEtcd+"://127.0.0.1:2379/svc")
	if err := registerService(dc, credentialProfile{}, "127.0.0.1:8080"); err != nil {
		t.Fatal(err)
	}
	if err := registerService(dc, credentialProfile{}, "127.0.0.1:8081"); err != nil {
		t.Fatal(err)
	}
	if got := len(reg.Registers()); got != 2 {
		t.Fatalf("Register called %d times, want 2", got)
	}
}

func TestRegisterServiceRegisterErrorPropagates(t *testing.T) {
	resetRegistrationState(t)
	reg := NewRegistry()
	installFakeProvider(t, SchemeEtcd, reg, nil)
	reg.SetRegisterError(errors.New("etcd down"))

	dc := mustConfig(t, SchemeEtcd+"://127.0.0.1:2379/svc")
	err := registerService(dc, credentialProfile{}, "127.0.0.1:8080")
	if err == nil {
		t.Fatal("want error from Register")
	}
	// A failed registration must not be remembered as live.
	if got := len(defaultRegistrations.entries); got != 0 {
		t.Fatalf("registrations = %d, want 0", got)
	}
}

func TestRegisterServicePinsAddress(t *testing.T) {
	resetRegistrationState(t)
	reg := NewRegistry()
	var pinned net.Addr
	installFakeProvider(t, SchemeEtcd, reg, func(a net.Addr) error {
		pinned = a
		return nil
	})

	dc := mustConfig(t, SchemeEtcd+"://127.0.0.1:2379/svc")
	if err := registerService(dc, credentialProfile{}, "10.1.2.3:8080"); err != nil {
		t.Fatal(err)
	}
	if pinned == nil || pinned.String() != "10.1.2.3:8080" {
		t.Fatalf("pinned = %v, want 10.1.2.3:8080", pinned)
	}
}

func TestShutdownDeregistersAll(t *testing.T) {
	resetRegistrationState(t)
	reg := NewRegistry()
	installFakeProvider(t, SchemeEtcd, reg, nil)

	dc := mustConfig(t, SchemeEtcd+"://127.0.0.1:2379/svc")
	if err := registerService(dc, credentialProfile{}, "127.0.0.1:8080"); err != nil {
		t.Fatal(err)
	}
	if err := (&Driver{}).Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(reg.Deregisters()); got != 1 {
		t.Fatalf("Deregister called %d times, want 1", got)
	}
	info, ok := reg.Deregisters()[0], true
	_ = info
	_ = ok
	if len(reg.Deregisters()) != 1 {
		t.Fatal("unreachable")
	}
	d := reg.Deregisters()[0]
	if d.ServiceName != "svc" || d.Addr.String() != "127.0.0.1:8080" {
		t.Fatalf("deregistered info = %+v", d)
	}
	// Shutdown is repeatable.
	if err := (&Driver{}).Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(reg.Deregisters()); got != 1 {
		t.Fatalf("Deregister after second Shutdown = %d, want 1", got)
	}
}

func TestShutdownContinuesOnDeregisterError(t *testing.T) {
	resetRegistrationState(t)
	reg1 := NewRegistry()
	reg2 := NewRegistry()
	installFakeProvider(t, SchemeEtcd, reg1, nil)
	reg1.SetDeregisterError(errors.New("etcd down"))

	dc := mustConfig(t, SchemeEtcd+"://127.0.0.1:2379/svc")
	if err := registerService(dc, credentialProfile{}, "127.0.0.1:8080"); err != nil {
		t.Fatal(err)
	}

	// Second registration via a second registry to see Shutdown continue.
	installFakeProvider(t, SchemeConsul, reg2, nil)
	dc2 := mustConfig(t, SchemeConsul+"://127.0.0.1:2379/svc2")
	if err := registerService(dc2, credentialProfile{}, "127.0.0.1:8081"); err != nil {
		t.Fatal(err)
	}

	err := (&Driver{}).Shutdown(context.Background())
	if err == nil {
		t.Fatal("want first deregister error surfaced")
	}
	if got := len(reg2.Deregisters()); got != 1 {
		t.Fatalf("second registry Deregister = %d, want 1 despite first failure", got)
	}
}

func mustConfig(t *testing.T, raw string) *discoveryConfig {
	t.Helper()
	dc, _, err := parseRegistryTarget(raw, false)
	if err != nil {
		t.Fatalf("parseRegistryTarget(%q): %v", raw, err)
	}
	return dc
}
