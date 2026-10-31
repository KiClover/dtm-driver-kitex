// Package kitex_server runs a real Kitex protobuf server for integration
// tests. It is deliberately boring: one unary method, standard gRPC transport,
// and the DTM metadata bridge installed as server middleware.
package kitex_server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/codes"
	npstatus "github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/status"
	"github.com/cloudwego/kitex/server"
	"github.com/dtm-labs/client/dtmgrpc"
	dtmkitex "github.com/dtm-labs/dtmdriver-kitex"
	order "github.com/dtm-labs/dtmdriver-kitex/integration/kitex_gen/order"
	"github.com/dtm-labs/dtmdriver-kitex/integration/kitex_gen/order/orderservice"
)

// Handler implements the generated OrderService interface.
type Handler struct{}

// Create echoes the decoded request and the DTM metadata it observed. The
// fail_with field drives the negative status matrix.
//
// Status errors must be built with Kitex's nphttp2 status package, not
// google.golang.org/grpc/status. Kitex only preserves the code of an error
// implementing its own status.Iface (pkg/remote/trans/nphttp2/codes.go
// convertStatus); a grpc status error falls through to codes.Internal with a
// flattened message, which would break the DTM status contract.
func (h *Handler) Create(ctx context.Context, req *order.CreateRequest) (*order.CreateResponse, error) {
	switch req.GetFailWith() {
	case "aborted":
		return nil, npstatus.Errorf(codes.Aborted, "branch failed")
	case "ongoing":
		return nil, npstatus.Errorf(codes.FailedPrecondition, "ONGOING")
	case "unavailable":
		return nil, npstatus.Errorf(codes.Unavailable, "registry unreachable")
	case "biz-error":
		// A business error that is not a DTM status must not be mistaken for a
		// definite failure by the DTM server.
		return nil, errors.New("inventory not enough")
	}

	barrier, err := dtmgrpc.BarrierFromGrpc(ctx)
	if err != nil {
		return nil, fmt.Errorf("BarrierFromGrpc: %w", err)
	}
	return &order.CreateResponse{
		OrderId:         req.GetOrderId(),
		Status:          req.GetStatus(),
		BarrierGid:      barrier.Gid,
		BarrierBranchId: barrier.BranchID,
		BarrierOp:       barrier.Op,
	}, nil
}

// Server is a started Kitex server bound to a concrete address.
type Server struct {
	svr     server.Server
	addr    string
	runDone chan error
	once    sync.Once
}

// Addr returns the bound host:port.
func (s *Server) Addr() string { return s.addr }

// Port returns the bound port as a decimal string.
func (s *Server) Port() string {
	_, port, _ := net.SplitHostPort(s.addr)
	return port
}

// Stop shuts the server down and waits for Run to return.
//
// On Windows the Kitex gonet transport calls os.Exit(1) as soon as an accept
// fails, and closing the listener makes accept fail, so a graceful in-process
// stop kills the test binary. That is an upstream Kitex defect in the gonet
// transport (see pkg/remote/trans/gonet/trans_server.go: the accept error
// branch ends in os.Exit(1)); production Linux deployments use the netpoll
// transport, which shuts down cleanly. The Windows test process therefore
// leaves its servers running and lets the process exit reclaim them.
func (s *Server) Stop() {
	s.once.Do(func() {
		if runtime.GOOS == "windows" {
			return
		}
		_ = s.svr.Stop()
		select {
		case <-s.runDone:
		case <-time.After(5 * time.Second):
		}
	})
}

// Start launches the Kitex protobuf server on a listener we own, with the DTM
// metadata middleware installed. Binding the listener ourselves means the
// address is known to be taken before Run is called, so no probe connection is
// needed 鈥?a probe connection would race with shutdown inside Kitex's HTTP/2
// transport.
func Start(tb testing.TB) *Server {
	return StartOn(tb, "127.0.0.1")
}

// StartOn is Start with a configurable listen host. Registry integration
// tests listen on 0.0.0.0 when the health check runs inside a container and
// must reach the server from outside the host loopback.
func StartOn(tb testing.TB, host string) *Server {
	tb.Helper()

	ln, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}

	svr := orderservice.NewServer(&Handler{},
		server.WithListener(ln),
		server.WithMiddleware(dtmkitex.ServerMetadataMiddleware),
	)

	runDone := make(chan error, 1)
	go func() { runDone <- svr.Run() }()

	// Give Run a moment to fail if the listener or options are unusable. Run
	// blocks while serving, so a short wait is enough to catch startup errors.
	select {
	case err := <-runDone:
		_ = ln.Close()
		tb.Fatalf("kitex server failed to start: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	s := &Server{svr: svr, addr: ln.Addr().String(), runDone: runDone}
	tb.Cleanup(s.Stop)
	return s
}
