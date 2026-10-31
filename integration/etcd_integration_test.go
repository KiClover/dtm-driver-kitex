package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	dtmkitex "github.com/dtm-labs/dtmdriver-kitex"
	order "github.com/dtm-labs/dtmdriver-kitex/integration/kitex_gen/order"
	"github.com/dtm-labs/dtmdriver-kitex/integration/kitex_server"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// etcdEndpoint points at the dedicated test registry container.
const etcdEndpoint = "127.0.0.1:12380"

// TestEtcdDiscoverAndCall closes the Gate C loop against a real etcd:
//  1. a Kitex server registers its instance through the driver
//     (RegisterService is the DTM-server side; the same registry object backs
//     the discovery resolver),
//  2. grpc.Dial("kitex-etcd://...") resolves through our builder to that
//     instance,
//  3. the DTM metadata rides the call and the handler echoes the barrier.
func TestEtcdDiscoverAndCall(t *testing.T) {
	if !etcdReachable(t) {
		t.Skip("etcd test container not reachable on 127.0.0.1:12380")
	}
	srv := kitex_server.Start(t)

	// --- 1. register an instance via the driver (server side). ---
	target := "kitex-etcd://" + etcdEndpoint + "/dtm-gate-c"
	if err := new(dtmkitex.Driver).RegisterService(target, srv.Addr()); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}

	// --- 2. dial through the driver's resolver scheme. ---
	conn, err := grpc.Dial("kitex-etcd://"+etcdEndpoint+"/dtm-gate-c?refresh_interval=500ms",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	// Client conns are never closed here (see direct_integration_test.go).

	// --- 3. invoke with DTM metadata and verify the barrier echo. ---
	ctx := dtmMetadata(withTimeout(t), "gid-etcd", "01", "action")
	resp := &order.CreateResponse{}
	// typed call through the generated client would pin the resolver at Dial
	// time; Invoke on the generic conn exercises the resolver dynamically.
	if err := conn.Invoke(ctx, methodPath, &order.CreateRequest{
		OrderId: "order-etcd",
		Amount:  "1",
		Status:  "prepared",
	}, resp); err != nil {
		t.Fatalf("invoke via etcd discovery: %v", err)
	}

	if resp.GetBarrierGid() != "gid-etcd" {
		t.Fatalf("barrier gid = %q, want gid-etcd", resp.GetBarrierGid())
	}
	if resp.GetOrderId() != "order-etcd" {
		t.Fatalf("order_id = %q, want order-etcd", resp.GetOrderId())
	}

	// The registration key must exist in etcd under the default Kitex prefix.
	if !etcdHasPrefix(t, "dtm-gate-c") {
		t.Errorf("etcd does not carry a registration under service dtm-gate-c")
	}
}

// TestEtcdDynamicMembership proves the resolver picks up instances registered
// after the client conn was built, without redialing.
func TestEtcdDynamicMembership(t *testing.T) {
	if !etcdReachable(t) {
		t.Skip("etcd test container not reachable on 127.0.0.1:12380")
	}
	srv := kitex_server.Start(t)

	target := "kitex-etcd://" + etcdEndpoint + "/dtm-dyn"
	if err := new(dtmkitex.Driver).RegisterService(target, srv.Addr()); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}

	conn, err := grpc.Dial("kitex-etcd://"+etcdEndpoint+"/dtm-dyn?refresh_interval=300ms",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	ctx := dtmMetadata(withTimeout(t), "gid-dyn", "01", "action")
	resp := &order.CreateResponse{}

	// First call may race registration publication; retry within the timeout.
	var lastErr error
	for i := 0; i < 40; i++ {
		lastErr = conn.Invoke(ctx, methodPath, &order.CreateRequest{OrderId: "o", Amount: "1"}, resp)
		if lastErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("invoke never succeeded after registration: %v", lastErr)
	}
	if resp.GetBarrierGid() != "gid-dyn" {
		t.Fatalf("barrier gid = %q, want gid-dyn", resp.GetBarrierGid())
	}
}

// etcdReachable reports whether the dedicated test etcd answers.
func etcdReachable(t *testing.T) bool {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://" + etcdEndpoint},
		DialTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	defer cli.Close()
	_, err = cli.Get(ctx, "probe")
	return err == nil
}

// etcdHasPrefix checks that at least one key under the Kitex registry prefix
// contains service as a substring.
func etcdHasPrefix(t *testing.T, service string) bool {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://" + etcdEndpoint},
		DialTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	defer cli.Close()
	// registry-etcd keys start with the prefix "kitex/registry-etcd/..." and
	// have no leading slash, so the scan must start at the empty key.
	resp, err := cli.Get(ctx, "", clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		t.Fatalf("etcd get: %v", err)
	}
	for _, kv := range resp.Kvs {
		if string(kv.Key) == "" {
			continue
		}
		// registry-etcd keys look like {prefix}/{service}/{addr}; match the
		// service segment anywhere to stay independent of the prefix default.
		if containsSegment(string(kv.Key), service) {
			return true
		}
	}
	return false
}

func containsSegment(key, service string) bool {
	for _, part := range strings.Split(key, "/") {
		if part == service {
			return true
		}
	}
	return false
}
