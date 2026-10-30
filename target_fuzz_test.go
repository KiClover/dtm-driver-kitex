package dtmkitex

import (
	"strings"
	"testing"
)

// FuzzParseTarget asserts parsing never panics and never leaks secrets in
// errors, for both branch and registration targets.
func FuzzParseTarget(f *testing.F) {
	seeds := []string{
		"127.0.0.1:8080/pkg.Svc/M",
		"[::1]:8080/pkg.Svc/M",
		"kitex-etcd://127.0.0.1:2379/svc/pkg.Svc/M?refresh_interval=5s",
		"kitex-etcd://a:2379,b:2379/svc/pkg.Svc/M",
		"kitex-consul://127.0.0.1:8500/svc/pkg.Svc/M?datacenter=dc1",
		"kitex-nacos://127.0.0.1:8848/svc/pkg.Svc/M?group=g&namespace=n",
		"kitex-etcd://127.0.0.1:2379/dtm-service?prefix=kitex%2Fregistry-etcd",
		"kitex-etcd://user:pass@127.0.0.1:2379/svc/pkg.Svc/M",
		"kitex-etcd://127.0.0.1:2379//pkg.Svc/M",
		"",
		"?",
		"://",
		"kitex-etcd://",
		"kitex-etcd://127.0.0.1:2379/svc/pkg.Svc/M?refresh_interval=nope",
		"kitex-etcd://127.0.0.1:2379/a%2Fb/pkg.Svc/M",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, input string) {
		bt, err := parseBranchTarget(input)
		if err == nil {
			if bt.method == "" || !strings.HasPrefix(bt.method, "/") {
				t.Fatalf("parsed method %q is not a full method", bt.method)
			}
			if strings.Contains(bt.server, "?") && !strings.Contains(bt.server, "://") {
				t.Fatalf("direct server %q carries a query", bt.server)
			}
		} else if strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "token=") {
			t.Fatalf("error leaks secret material: %v", err)
		}
		if _, err := parseRegistrationTarget(input); err != nil {
			if strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "token=") {
				t.Fatalf("registration error leaks secret material: %v", err)
			}
		}
	})
}
