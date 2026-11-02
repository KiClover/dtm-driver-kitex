package integration_test

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	dtmkitex "github.com/dtm-labs/dtmdriver-kitex"
	order "github.com/dtm-labs/dtmdriver-kitex/integration/kitex_gen/order"
	"github.com/dtm-labs/dtmdriver-kitex/integration/kitex_server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// grpcDial is a shared helper for registry tests; conns are never closed
// (see direct_integration_test.go).
func grpcDial(target string) (*grpc.ClientConn, error) {
	return grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// consulAPI is the dev agent's HTTP endpoint (see integration/docker-compose.yml).
const consulAPI = "http://127.0.0.1:18500"

// consulRegisterHost is the address both sides of the test reach: the Consul
// agent (container) TCP-checks it, and the grpc-go client on the host dials
// it. Candidate host IPs are verified with a real ephemeral listener probed
// from inside the consul container, because a TUN proxy may answer on the
// published port while refusing arbitrary test ports.
func detectConsulRegisterHost(t *testing.T) string {
	t.Helper()
	candidates := hostIPv4Candidates(t)
	for _, ip := range candidates {
		ln, err := net.Listen("tcp", ip+":0")
		if err != nil {
			continue
		}
		port := fmt.Sprintf("%d", ln.Addr().(*net.TCPAddr).Port)
		// A TUN/proxy interface may pass the container-side probe while
		// refusing the host's own dials on arbitrary ports, so both sides
		// must accept the same listener before the IP qualifies.
		if hostDialOK(ln) && probeConsulContainer(t, ip, port) {
			ln.Close()
			return ip
		}
		ln.Close()
	}
	t.Skipf("no host IPv4 reachable from the consul container; candidates=%v", candidates)
	return ""
}

// hostDialOK dials the listener from the host process itself.
func hostDialOK(ln net.Listener) bool {
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		return false
	}
	c.Close()
	return true
}

func hostIPv4Candidates(t *testing.T) []string {
	t.Helper()
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatalf("interface addrs: %v", err)
	}
	var out []string
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}
		ip4 := ipnet.IP.To4()
		if ip4 == nil || ip4.IsLinkLocalUnicast() {
			continue
		}
		out = append(out, ip4.String())
	}
	return out
}

// probeConsulContainer runs nc inside the consul container against a host
// port and reports whether the connection succeeded. The container's default
// gateway is the host side of the docker network; candidate IPs are dialled
// as-is.
func probeConsulContainer(t *testing.T, ip, port string) bool {
	t.Helper()
	cmd := exec.Command("docker", "exec", consulContainer, "sh", "-c",
		"nc -z -w2 "+ip+" "+port+" >/dev/null 2>&1; echo rc=$?")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("docker exec probe: %v (%s)", err, strings.TrimSpace(string(out)))
		return false
	}
	return strings.TrimSpace(string(out)) == "rc=0"
}

const (
	consulContainer = "dtm-kitex-test-consul"
)

// TestConsulDiscoverAndCall closes the Gate C loop for Consul: driver
// registration, resolver-backed grpc.Dial, DTM metadata bridge.
func TestConsulDiscoverAndCall(t *testing.T) {
	if !consulReachable(t) {
		t.Skip("consul test container not reachable on 127.0.0.1:18500")
	}
	srv := kitex_server.StartOn(t, "0.0.0.0")

	target := "kitex-consul://127.0.0.1:18500/dtm-consul-gate"
	registerAddr := detectConsulRegisterHost(t) + ":" + srv.Port()
	if err := new(dtmkitex.Driver).RegisterService(target, registerAddr); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}
	// The agent-side TCP check needs a few seconds to turn passing; the
	// consul resolver reports "no service found" until then, so wait before
	// dialing instead of relying on resolver error retries.
	deadline := time.Now().Add(10 * time.Second)
	for !consulHasService(t, "dtm-consul-gate") {
		if time.Now().After(deadline) {
			t.Fatalf("registered instance never passed the consul health check")
		}
		time.Sleep(200 * time.Millisecond)
	}

	conn, err := grpcDial("kitex-consul://127.0.0.1:18500/dtm-consul-gate?refresh_interval=500ms")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	ctx := dtmMetadata(withTimeout(t), "gid-consul", "01", "action")
	resp := &order.CreateResponse{}
	var lastErr error
	for i := 0; i < 40; i++ {
		lastErr = conn.Invoke(ctx, methodPath, &order.CreateRequest{OrderId: "o-consul", Amount: "1", Status: "prepared"}, resp)
		if lastErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("invoke via consul discovery: %v", lastErr)
	}
	if resp.GetBarrierGid() != "gid-consul" {
		t.Fatalf("barrier gid = %q, want gid-consul", resp.GetBarrierGid())
	}
	if resp.GetOrderId() != "o-consul" {
		t.Fatalf("order_id = %q, want o-consul", resp.GetOrderId())
	}

	// The agent must carry the service with a passing health check (TTL
	// heartbeat).
	if !consulHasService(t, "dtm-consul-gate") {
		t.Error("consul does not carry service dtm-consul-gate")
	}
}

// consulReachable pings the dev agent leader endpoint.
func consulReachable(t *testing.T) bool {
	t.Helper()
	cli := &http.Client{Timeout: 2 * time.Second}
	resp, err := cli.Get(consulAPI + "/v1/status/leader")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// consulHasService checks the health API for at least one passing instance.
func consulHasService(t *testing.T, service string) bool {
	t.Helper()
	cli := &http.Client{Timeout: 2 * time.Second}
	resp, err := cli.Get(fmt.Sprintf("%s/v1/health/service/%s", consulAPI, service))
	if err != nil {
		t.Fatalf("consul health query: %v", err)
	}
	defer resp.Body.Close()
	var entries []struct {
		Checks []struct {
			Status string `json:"Status"`
		} `json:"Checks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		t.Fatalf("decode consul health: %v", err)
	}
	for _, e := range entries {
		passing := true
		for _, c := range e.Checks {
			if c.Status != "passing" {
				passing = false
			}
		}
		if passing {
			return true
		}
	}
	return false
}
