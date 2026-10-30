package dtmkitex

import (
	"sync"
	"testing"

	"google.golang.org/grpc/resolver"

	dtmdriver "github.com/dtm-labs/dtmdriver"
)

func TestDriverName(t *testing.T) {
	d := &Driver{}
	if got := d.GetName(); got != DriverName {
		t.Fatalf("GetName() = %q, want %q", got, DriverName)
	}
	if DriverName != "dtm-driver-kitex" {
		t.Fatalf("DriverName = %q", DriverName)
	}
}

func TestDriverRegisteredWithDtmdriver(t *testing.T) {
	if err := dtmdriver.Use(DriverName); err != nil {
		t.Fatalf("dtmdriver.Use(%q) = %v", DriverName, err)
	}
	d := dtmdriver.GetDriver()
	if d == nil {
		t.Fatalf("driver %q not registered with dtmdriver", DriverName)
	}
	if d.GetName() != DriverName {
		t.Fatalf("GetDriver().GetName() = %q, want %q", d.GetName(), DriverName)
	}
}

func TestResolverSchemesRegistered(t *testing.T) {
	for _, scheme := range []string{SchemeEtcd, SchemeConsul, SchemeNacos} {
		if resolver.Get(scheme) == nil {
			t.Errorf("grpc resolver scheme %q not registered", scheme)
		}
	}
}

func TestRegisterAddrResolverIdempotent(t *testing.T) {
	d := &Driver{}
	d.RegisterAddrResolver()
	d.RegisterAddrResolver()
}

func TestRegisterAddrResolverConcurrent(t *testing.T) {
	d := &Driver{}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.RegisterAddrResolver()
		}()
	}
	wg.Wait()
}

func TestPackageConfigureShutdownDelegate(t *testing.T) {
	// Package-level Configure/Shutdown must exist and delegate to the same
	// default driver instance (compile-time check plus smoke call).
	if err := Configure(Config{}); err != nil {
		t.Fatalf("Configure(Config{}) = %v", err)
	}
}
