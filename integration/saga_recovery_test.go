package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/dtm-labs/dtm/client/dtmgrpc"
	dtmkitex "github.com/dtm-labs/dtmdriver-kitex"
	"github.com/dtm-labs/dtmdriver-kitex/integration/kitex_gen/order"
	"github.com/dtm-labs/dtmdriver-kitex/integration/kitex_server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// DTM server gRPC endpoint, matching integration/dtm_server/conf.yaml.
	dtmServerGrpc = "127.0.0.1:36790"
	// etcd-backed DTM server target as registered via the kitex driver.
	dtmServerViaKitex = "kitex-etcd://127.0.0.1:12380/dtm-server/dtmgimp.Dtm"
)

// dtmAvailable probes the DTM server admin API; tests skip when the server
// (started separately via integration/dtm_server) is not running.
func dtmAvailable(t *testing.T) bool {
	t.Helper()
	c, err := net.DialTimeout("tcp", dtmServerGrpc, 2*time.Second)
	if err != nil {
		t.Skipf("dtm server not reachable on %s: %v (start integration/dtm_server first)", dtmServerGrpc, err)
	}
	c.Close()
	return true
}

// dtmDial returns a client conn to the DTM server resolved through the
// Kitex driver etcd chain (kitex-etcd:// -> discovery -> grpc).
func dtmDialViaKitex(t *testing.T) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.Dial(dtmServerViaKitex, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial dtm via kitex-etcd: %v", err)
	}
	return conn
}

// registerBranchService starts a Kitex branch service and registers it in
// etcd under a unique discovery service. A unique name per test is required:
// the DTM server caches its grpc ClientConn per server string, so reusing a
// name would make later tests inherit a connection whose etcd provider has
// been closed by a previous test's Shutdown.
//
// It returns the branch action URL that routes through the very registration
// made here (kitex-etcd://<etcd>/<discovery-service>/<grpc-service>/<method>).
func registerBranchService(t *testing.T) (srv *kitex_server.Server, actionAddr string) {
	t.Helper()
	srv = kitex_server.StartOn(t, "127.0.0.1")
	target := fmt.Sprintf("kitex-etcd://127.0.0.1:12380/order-svc-%d", time.Now().UnixNano())
	if err := new(dtmkitex.Driver).RegisterService(target, srv.Addr()); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}
	actionAddr = target + "/order.OrderService/Create"
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := new(dtmkitex.Driver).Shutdown(ctx); err != nil {
			t.Logf("deregister: %v", err)
		}
	})
	return srv, actionAddr
}

// TestSagaRawRecovery drives a real Saga through the DTM server whose
// branches are invoked back over the Kitex driver path (etcd discovery +
// raw payload + metadata bridge + barrier).
func TestSagaRawRecovery(t *testing.T) {
	if !dtmAvailable(t) {
		return
	}
	_, actionAddr := registerBranchService(t)

	gid := dtmgrpc.MustGenGid(dtmServerGrpc)
	saga := dtmgrpc.NewSagaGrpc(dtmServerGrpc, gid).
		Add(actionAddr, "", &order.CreateRequest{OrderId: "o-saga-1", Amount: "1", Status: "prepared"})
	if err := saga.Submit(); err != nil {
		t.Fatalf("saga submit: %v", err)
	}

	// The DTM server invokes the branch back through the kitex-etcd chain.
	// The saga is submitted asynchronously, so poll the DTM HTTP API for the
	// global transaction to reach a terminal status.
	deadline := time.Now().Add(15 * time.Second)
	var status string
	for time.Now().Before(deadline) {
		status = queryTransStatus(t, gid)
		if status == "succeed" || status == "failed" {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if status != "succeed" {
		t.Fatalf("saga %s final status = %q, want succeed", gid, status)
	}
}

// TestSagaCompensationOnAborted drives a Saga whose action fails with
// codes.Aborted (DTM ErrFailure semantics) and asserts that the global
// transaction rolls back. The compensate URL points at the same Kitex
// service, so the compensation also travels the kitex-etcd chain.
func TestSagaCompensationOnAborted(t *testing.T) {
	if !dtmAvailable(t) {
		return
	}
	_, actionAddr := registerBranchService(t)

	gid := dtmgrpc.MustGenGid(dtmServerGrpc)
	saga := dtmgrpc.NewSagaGrpc(dtmServerGrpc, gid).
		Add(actionAddr, actionAddr,
			&order.CreateRequest{OrderId: "o-saga-fail", Amount: "1", Status: "prepared", FailWith: "aborted"})
	if err := saga.Submit(); err != nil {
		t.Fatalf("saga submit: %v", err)
	}

	// The action fails definitively, so the saga must roll back. With no
	// compensate branch registered (empty compensate URL) DTM marks the
	// global transaction failed after the action error.
	deadline := time.Now().Add(15 * time.Second)
	var status string
	for time.Now().Before(deadline) {
		status = queryTransStatus(t, gid)
		if status == "succeed" || status == "failed" {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if status != "failed" {
		t.Fatalf("saga %s final status = %q, want failed", gid, status)
	}
}

// queryTransStatus asks the DTM server HTTP admin API for the global
// transaction status of gid.
func queryTransStatus(t *testing.T, gid string) string {
	t.Helper()
	c := &http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get("http://127.0.0.1:36789/api/dtmsvr/query?gid=" + gid)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	var out struct {
		Transaction struct {
			Status string `json:"status"`
		} `json:"transaction"`
	}
	if json.Unmarshal(body, &out) != nil {
		return ""
	}
	return out.Transaction.Status
}
