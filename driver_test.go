package dtmkitex

import (
	"sync"
	"testing"

	"google.golang.org/grpc/resolver"

	dtmdriver "github.com/dtm-labs/dtmdriver"
)

func TestDriverName(t *testing.T) {
	d := &kitexDriver{}
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
	d := &kitexDriver{}
	d.RegisterAddrResolver()
	d.RegisterAddrResolver()
}

func TestRegisterAddrResolverConcurrent(t *testing.T) {
	d := &kitexDriver{}
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

func TestRegisterServiceEmptyTargetIsNoop(t *testing.T) {
	d := &kitexDriver{}
	if err := d.RegisterService("", "not-an-endpoint"); err != nil {
		t.Fatalf("RegisterService with empty target = %v, want nil", err)
	}
}
