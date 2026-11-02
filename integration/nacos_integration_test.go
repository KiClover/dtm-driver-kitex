package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	dtmkitex "github.com/dtm-labs/dtmdriver-kitex"
	order "github.com/dtm-labs/dtmdriver-kitex/integration/kitex_gen/order"
	"github.com/dtm-labs/dtmdriver-kitex/integration/kitex_server"
)

const (
	nacosEndpoint = "127.0.0.1:8848"
	nacosService  = "dtm-nacos-gate"
	nacosTarget   = "kitex-nacos://" + nacosEndpoint + "/" + nacosService
	nacosConsole  = "http://" + nacosEndpoint + "/nacos"
	nacosHTTPPath = methodPath // "/order.OrderService/Create"
)

// nacosReachable probes the nacos console; integration tests skip when the
// container is not running.
func nacosReachable(t *testing.T) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, nacosConsole, nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode < 500
}

// nacosInstanceList queries the nacos v1 instance API for the service.
type nacosInstanceList struct {
	Hosts []struct {
		Ip      string `json:"ip"`
		Port    int    `json:"port"`
		Healthy bool   `json:"healthy"`
		Enabled bool   `json:"enabled"`
	} `json:"hosts"`
}

func nacosInstances(t *testing.T) nacosInstanceList {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/v1/ns/instance/list?serviceName=%s", nacosConsole, nacosService), nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("instance list: %v", err)
	}
	defer resp.Body.Close()
	var list nacosInstanceList
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("decode instance list: %v", err)
	}
	return list
}

// TestNacosDiscoverAndCall closes the Gate C loop for Nacos: driver
// registration (ephemeral instance, client-side heartbeat), resolver-backed
// grpc.Dial, DTM metadata bridge.
func TestNacosDiscoverAndCall(t *testing.T) {
	if !nacosReachable(t) {
		t.Skip("nacos test container not reachable on 127.0.0.1:8848")
	}
	srv := kitex_server.StartOn(t, "127.0.0.1")
	regPort, err := strconv.Atoi(srv.Port())
	if err != nil {
		t.Fatalf("parse server port: %v", err)
	}
	registerAddr := fmt.Sprintf("127.0.0.1:%d", regPort)
	if err := new(dtmkitex.Driver).RegisterService(nacosTarget, registerAddr); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}

	conn, err := grpc.Dial(nacosTarget+"?refresh_interval=500ms", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	ctx := dtmMetadata(withTimeout(t), "gid-nacos", "01", "action")
	resp := &order.CreateResponse{}
	var lastErr error
	for i := 0; i < 40; i++ {
		lastErr = conn.Invoke(ctx, nacosHTTPPath, &order.CreateRequest{OrderId: "o-nacos", Amount: "1", Status: "prepared"}, resp)
		if lastErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("invoke via nacos discovery: %v", lastErr)
	}
	if resp.GetBarrierGid() != "gid-nacos" {
		t.Fatalf("barrier gid = %q, want gid-nacos", resp.GetBarrierGid())
	}
	if resp.GetOrderId() != "o-nacos" {
		t.Fatalf("order_id = %q, want o-nacos", resp.GetOrderId())
	}

	// The server must carry the registered instance as healthy and enabled.
	// Nacos v2 publishes ip-port client changes asynchronously, so the v1
	// list endpoint can lag behind the registration for a short while: poll
	// instead of checking once.
	found := false
	for i := 0; i < 20 && !found; i++ {
		list := nacosInstances(t)
		for _, h := range list.Hosts {
			if h.Ip == "127.0.0.1" && h.Port == regPort && h.Healthy && h.Enabled {
				found = true
			}
		}
		if !found {
			time.Sleep(500 * time.Millisecond)
		}
	}
	if !found {
		t.Errorf("nacos does not carry healthy instance 127.0.0.1:%d (hosts=%+v)", regPort, nacosInstances(t).Hosts)
	}
}
