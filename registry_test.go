package dtmkitex

import (
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/discovery"
	. "github.com/dtm-labs/dtmdriver-kitex/internal/testkit"
)

// resetRegistrationState clears both process-global caches so tests are independent.
func resetRegistrationState(t *testing.T) {
	t.Helper()
	defaultDiscoveries.mu.Lock()
	defaultDiscoveries.resolvers = map[string]discovery.Resolver{}
	defaultDiscoveries.mu.Unlock()
	defaultRegistrations.mu.Lock()
	defaultRegistrations.providers = map[string]*registrationProvider{}
	defaultRegistrations.mu.Unlock()
}

func installFakeFactories(t *testing.T, scheme string, discoveryFactory discoveryFactory, registrationFactory registrationFactory) {
	t.Helper()
	previousDiscovery, hadDiscovery := discoveryFactories[scheme]
	previousRegistration, hadRegistration := registrationFactories[scheme]
	discoveryFactories[scheme] = discoveryFactory
	registrationFactories[scheme] = registrationFactory
	t.Cleanup(func() {
		if hadDiscovery {
			discoveryFactories[scheme] = previousDiscovery
		} else {
			delete(discoveryFactories, scheme)
		}
		if hadRegistration {
			registrationFactories[scheme] = previousRegistration
		} else {
			delete(registrationFactories, scheme)
		}
	})
}

func installFakeProvider(t *testing.T, scheme string, reg *Registry, pin func(net.Addr) error) {
	t.Helper()
	installFakeFactories(t, scheme,
		func(dc *discoveryConfig, _ credentialProfile) (discovery.Resolver, error) {
			return NewDiscovery(scheme, dc.service), nil
		},
		func(*discoveryConfig, credentialProfile) (*registrationProvider, error) {
			return &registrationProvider{registry: reg, pinAddress: pin}, nil
		},
	)
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
		{"hostname accepted", "svc", "host.docker.internal:8080", 10, false},
		{"empty port", "svc", "host.docker.internal:", 10, true},
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

func TestNewRegistrationInfoHostnameAddr(t *testing.T) {
	info, err := newRegistrationInfo("svc", "host.docker.internal:9090", 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Addr.String(); got != "host.docker.internal:9090" {
		t.Errorf("hostname addr = %q", got)
	}
	if info.Addr.Network() != "tcp" {
		t.Errorf("network = %q", info.Addr.Network())
	}
}

func TestRegisterServiceDelegatesEachCall(t *testing.T) {
	resetRegistrationState(t)
	reg := NewRegistry()
	installFakeProvider(t, SchemeEtcd, reg, nil)

	dc := mustConfig(t, SchemeEtcd+"://127.0.0.1:2379/svc")
	for i := 0; i < 3; i++ {
		if err := registerService(dc, credentialProfile{}, "127.0.0.1:8080"); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if got := len(reg.Registers()); got != 3 {
		t.Fatalf("Register called %d times, want 3", got)
	}
}

func TestRegisterServiceSerializesPinAndRegister(t *testing.T) {
	resetRegistrationState(t)
	reg := NewRegistry()
	firstPinned := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondPinned := make(chan struct{}, 1)
	var calls atomic.Int32
	installFakeProvider(t, SchemeEtcd, reg, func(net.Addr) error {
		if calls.Add(1) == 1 {
			close(firstPinned)
			<-releaseFirst
			return nil
		}
		secondPinned <- struct{}{}
		return nil
	})

	dc := mustConfig(t, SchemeEtcd+"://127.0.0.1:2379/svc")
	errs := make(chan error, 2)
	go func() { errs <- registerService(dc, credentialProfile{}, "127.0.0.1:8080") }()
	<-firstPinned
	go func() { errs <- registerService(dc, credentialProfile{}, "127.0.0.1:8081") }()

	concurrent := false
	select {
	case <-secondPinned:
		concurrent = true
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirst)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if concurrent {
		t.Fatal("pin and Register were not serialized per provider")
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

func mustConfig(t *testing.T, raw string) *discoveryConfig {
	t.Helper()
	dc, _, err := parseRegistryTarget(raw, false)
	if err != nil {
		t.Fatalf("parseRegistryTarget(%q): %v", raw, err)
	}
	return dc
}
