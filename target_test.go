package dtmkitex

import (
	"strings"
	"testing"
)

func TestParseDirectTargetValid(t *testing.T) {
	cases := []struct {
		name string
		uri  string
	}{
		{"ipv4", "127.0.0.1:8080/order.v1.OrderService/Create"},
		{"hostname", "orders.example.com:9000/order.v1.OrderService/Create"},
		{"ipv6", "[::1]:8080/order.v1.OrderService/Create"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bt, err := parseBranchTarget(tc.uri)
			if err != nil {
				t.Fatalf("parseBranchTarget(%q) = %v", tc.uri, err)
			}
			if bt.server != tc.uri[:strings.LastIndex(tc.uri, "/order")] {
				t.Fatalf("server = %q", bt.server)
			}
			if bt.method != "/order.v1.OrderService/Create" {
				t.Fatalf("method = %q", bt.method)
			}
			if bt.discovery != nil {
				t.Fatalf("direct target must not carry discovery config")
			}
		})
	}
}

func TestParseRegistryTargetValid(t *testing.T) {
	const uri = "kitex-etcd://10.0.0.1:2379,10.0.0.2:2379/order-service/order.v1.OrderService/Create?refresh_interval=5s"
	bt, err := parseBranchTarget(uri)
	if err != nil {
		t.Fatalf("parseBranchTarget: %v", err)
	}
	wantServer := "kitex-etcd://10.0.0.1:2379,10.0.0.2:2379/order-service?refresh_interval=5s"
	if bt.server != wantServer {
		t.Fatalf("server = %q, want %q", bt.server, wantServer)
	}
	if bt.method != "/order.v1.OrderService/Create" {
		t.Fatalf("method = %q", bt.method)
	}
	if bt.discovery == nil {
		t.Fatalf("registry target must carry discovery config")
	}
	if bt.discovery.scheme != SchemeEtcd {
		t.Fatalf("scheme = %q", bt.discovery.scheme)
	}
	if got := strings.Join(bt.discovery.endpoints, ","); got != "10.0.0.1:2379,10.0.0.2:2379" {
		t.Fatalf("endpoints = %q", got)
	}
	if bt.discovery.service != "order-service" {
		t.Fatalf("service = %q", bt.discovery.service)
	}
	if bt.discovery.query.Get("refresh_interval") != "5s" {
		t.Fatalf("refresh_interval lost: %q", bt.discovery.query.Encode())
	}
}

func TestParseRegistrationTarget(t *testing.T) {
	const uri = "kitex-etcd://127.0.0.1:2379/dtm-service?prefix=kitex%2Fregistry-etcd"
	rt, err := parseRegistrationTarget(uri)
	if err != nil {
		t.Fatalf("parseRegistrationTarget: %v", err)
	}
	if rt.discovery.service != "dtm-service" {
		t.Fatalf("service = %q", rt.discovery.service)
	}
	if rt.discovery.query.Get("prefix") != "kitex/registry-etcd" {
		t.Fatalf("prefix = %q", rt.discovery.query.Get("prefix"))
	}
}

func TestParseTargetRejectsInvalid(t *testing.T) {
	cases := []struct{ name, uri string }{
		{"unknown scheme", "kitex-zookeeper://127.0.0.1:2181/svc/pkg.Svc/M"},
		{"http scheme", "http://127.0.0.1:8080/svc/pkg.Svc/M"},
		{"userinfo", "kitex-etcd://user:pass@127.0.0.1:2379/svc/pkg.Svc/M"},
		{"fragment", "kitex-etcd://127.0.0.1:2379/svc/pkg.Svc/M#frag"},
		{"no method", "kitex-etcd://127.0.0.1:2379/svc/pkg.Svc"},
		{"empty service", "kitex-etcd://127.0.0.1:2379//pkg.Svc/M"},
		{"empty method", "kitex-etcd://127.0.0.1:2379/svc/pkg.Svc/"},
		{"escaped slash in service", "kitex-etcd://127.0.0.1:2379/a%2Fb/pkg.Svc/M"},
		{"direct query rejected", "127.0.0.1:8080/pkg.Svc/M?x=1"},
		{"empty host", "kitex-etcd://:2379/svc/pkg.Svc/M"},
		{"empty port", "kitex-etcd://127.0.0.1:/svc/pkg.Svc/M"},
		{"bad endpoint port", "kitex-etcd://127.0.0.1:notaport/svc/pkg.Svc/M"},
		{"credential profiles removed", "kitex-etcd://127.0.0.1:2379/svc/pkg.Svc/M?credential_profile=prod"},
		{"malformed credential profile query", "kitex-etcd://127.0.0.1:2379/svc/pkg.Svc/M?credential_profile=prod;x=1"},
		{"malformed credential query", "kitex-etcd://127.0.0.1:2379/svc/pkg.Svc/M?user=dtm;password=secret"},
		{"malformed percent escape", "kitex-etcd://127.0.0.1:2379/svc/pkg.Svc/M?user=%zz"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseBranchTarget(tc.uri); err == nil {
				t.Fatalf("parseBranchTarget(%q) should fail", tc.uri)
			}
		})
	}
}

func TestParseTargetRejectsCrossSchemeQuery(t *testing.T) {
	// etcd-only query on consul scheme must be rejected.
	if _, err := parseBranchTarget("kitex-consul://127.0.0.1:8500/svc/pkg.Svc/M?prefix=x"); err == nil {
		t.Fatal("consul with etcd-only query should fail")
	}
	// consul supports datacenter.
	if _, err := parseBranchTarget("kitex-consul://127.0.0.1:8500/svc/pkg.Svc/M?datacenter=dc1"); err != nil {
		t.Fatalf("consul datacenter rejected: %v", err)
	}
}

func TestParseTargetMultiEndpointRules(t *testing.T) {
	if _, err := parseBranchTarget("kitex-consul://a:8500,b:8500/svc/pkg.Svc/M"); err == nil {
		t.Fatal("consul multi-endpoint should fail in v1")
	}
	if _, err := parseBranchTarget("kitex-nacos://a:8848,b:8848/svc/pkg.Svc/M"); err == nil {
		t.Fatal("nacos multi-endpoint should fail in v1")
	}
	if _, err := parseBranchTarget("kitex-etcd://a:2379,b:2379/svc/pkg.Svc/M"); err != nil {
		t.Fatalf("etcd multi-endpoint must be allowed: %v", err)
	}
}

func TestParseTargetUnknownQueryRejected(t *testing.T) {
	if _, err := parseBranchTarget("kitex-etcd://127.0.0.1:2379/svc/pkg.Svc/M?bogus=1"); err == nil {
		t.Fatal("unknown query key should fail")
	}
}

func TestParseTargetDirectIgnoresSchemesWithoutPort(t *testing.T) {
	// A bare "host/method" without port is not a valid direct endpoint.
	if _, err := parseBranchTarget("host/pkg.Svc/M"); err == nil {
		t.Fatal("missing port should fail")
	}
}

func TestParseTargetErrorNoSecretEcho(t *testing.T) {
	_, err := parseBranchTarget("kitex-etcd://user:secret@127.0.0.1:2379/svc/pkg.Svc/M")
	if err == nil {
		t.Fatal("userinfo must be rejected")
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("error leaks secret: %v", err)
	}
}

func TestCanonicalTarget(t *testing.T) {
	a, err1 := parseBranchTarget("kitex-etcd://127.0.0.1:2379/svc/pkg.Svc/M?refresh_interval=5s&prefix=a")
	b, err2 := parseBranchTarget("kitex-etcd://127.0.0.1:2379/svc/pkg.Svc/M?prefix=a&refresh_interval=5s")
	if err1 != nil || err2 != nil {
		t.Fatalf("parse errors: %v %v", err1, err2)
	}
	if a.discovery.canonicalKey() != b.discovery.canonicalKey() {
		t.Fatalf("canonical keys differ: %q vs %q", a.discovery.canonicalKey(), b.discovery.canonicalKey())
	}
}

func TestDriverParseServerMethod(t *testing.T) {
	d := &kitexDriver{}
	svr, method, err := d.ParseServerMethod("kitex-etcd://10.0.0.1:2379/order-service/order.v1.OrderService/Create?refresh_interval=5s")
	if err != nil {
		t.Fatalf("ParseServerMethod: %v", err)
	}
	want := "kitex-etcd://10.0.0.1:2379/order-service?refresh_interval=5s"
	if svr != want {
		t.Fatalf("server = %q, want %q", svr, want)
	}
	if method != "/order.v1.OrderService/Create" {
		t.Fatalf("method = %q", method)
	}
}
