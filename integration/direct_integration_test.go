package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/dtm-labs/dtm/client/dtmgrpc/dtmgimp"
	order "github.com/dtm-labs/dtmdriver-kitex/integration/kitex_gen/order"
	"github.com/dtm-labs/dtmdriver-kitex/integration/kitex_server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// methodPath is the fully qualified path a DTM server uses when it calls a
// branch. Kitex strips the package qualifier and dispatches on the bare
// service name, so this is the exact string both paths must support.
const methodPath = "/order.OrderService/Create"

// Client connections are intentionally never closed in these tests: on an
// abrupt client disconnect the Kitex v0.12.x nphttp2 server races its own
// shutdown and panics with "close of closed channel". The process exit
// reclaims the sockets instead.

// dtmMetadata is the metadata a DTM server attaches to every branch call.
func dtmMetadata(ctx context.Context, gid, branchID, op string) context.Context {
	return metadata.AppendToOutgoingContext(ctx,
		"dtm-gid", gid,
		"dtm-trans_type", "saga",
		"dtm-branch_id", branchID,
		"dtm-op", op,
		"dtm-dtm", "http://dtm:36789",
	)
}

func withTimeout(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestDirectTypedUnary proves the ordinary, typed call path: a grpc-go client
// with generated messages reaches a Kitex protobuf handler and the metadata
// bridge makes the DTM barrier fields readable inside that handler.
func TestDirectTypedUnary(t *testing.T) {
	srv := kitex_server.Start(t)

	conn, err := grpc.Dial(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ctx := dtmMetadata(withTimeout(t), "gid-typed", "02", "action")
	resp := &order.CreateResponse{}
	if err := conn.Invoke(ctx, methodPath, &order.CreateRequest{
		OrderId: "order-1",
		Amount:  "7",
		Status:  "prepared",
	}, resp); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	if resp.GetOrderId() != "order-1" {
		t.Fatalf("order_id = %q, want order-1", resp.GetOrderId())
	}
	if resp.GetStatus() != "prepared" {
		t.Fatalf("status = %q, want prepared", resp.GetStatus())
	}
	if resp.GetBarrierGid() != "gid-typed" {
		t.Fatalf("barrier gid = %q, want gid-typed (metadata bridge did not deliver dtm-gid)", resp.GetBarrierGid())
	}
	if resp.GetBarrierBranchId() != "02" {
		t.Fatalf("barrier branch = %q, want 02", resp.GetBarrierBranchId())
	}
	if resp.GetBarrierOp() != "action" {
		t.Fatalf("barrier op = %q, want action", resp.GetBarrierOp())
	}
}

// TestDirectRawUnary proves the path that actually matters for DTM retries:
// the server hands an already-encoded protobuf payload to conn.Invoke through
// the raw codec ("dtm_raw", content-type application/grpc+dtm_raw). If a Kitex
// server could not decode that, DTM could never resend a persisted branch.
func TestDirectRawUnary(t *testing.T) {
	srv := kitex_server.Start(t)

	conn := dtmgimp.MustGetGrpcConn(srv.Addr(), true)
	reqBytes, err := proto.Marshal(&order.CreateRequest{
		OrderId: "order-raw",
		Amount:  "11",
		Status:  "succeeded",
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	ctx := dtmMetadata(withTimeout(t), "gid-raw", "03", "action")
	var respBytes []byte
	if err := conn.Invoke(ctx, methodPath, reqBytes, &respBytes); err != nil {
		t.Fatalf("raw invoke: %v", err)
	}

	resp := &order.CreateResponse{}
	if err := proto.Unmarshal(respBytes, resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.GetOrderId() != "order-raw" {
		t.Fatalf("order_id = %q, want order-raw", resp.GetOrderId())
	}
	if resp.GetBarrierGid() != "gid-raw" {
		t.Fatalf("barrier gid = %q, want gid-raw", resp.GetBarrierGid())
	}
	if resp.GetBarrierBranchId() != "03" {
		t.Fatalf("barrier branch = %q, want 03", resp.GetBarrierBranchId())
	}
}

// TestDTMStatusMatrix pins the status codes DTM relies on to tell "retry me"
// apart from "this branch definitely failed". It drives the same raw path a
// retried branch uses.
func TestDTMStatusMatrix(t *testing.T) {
	srv := kitex_server.Start(t)

	conn := dtmgimp.MustGetGrpcConn(srv.Addr(), true)
	cases := []struct {
		failWith string
		want     codes.Code
		wantMsg  string
	}{
		{"aborted", codes.Aborted, "branch failed [biz error]"},
		{"ongoing", codes.FailedPrecondition, "ONGOING [biz error]"},
		{"unavailable", codes.Unavailable, "registry unreachable [biz error]"},
		{"biz-error", codes.Internal, "inventory not enough [biz error]"},
	}

	// Kitex wraps every handler error as a biz error: it keeps the code of
	// nphttp2 status errors, maps any other error to codes.Internal, and
	// appends " [biz error]" to the message. codes.Internal is not a code
	// the DTM server treats as a definite branch failure, so a plain
	// business error still lands in the retryable bucket — the contract
	// DTM depends on holds either way.

	for _, tc := range cases {
		tc := tc
		t.Run(tc.failWith, func(t *testing.T) {
			reqBytes, err := proto.Marshal(&order.CreateRequest{OrderId: "order-" + tc.failWith, FailWith: tc.failWith})
			if err != nil {
				t.Fatalf("marshal request: %v", err)
			}
			ctx := dtmMetadata(withTimeout(t), "gid-"+tc.failWith, "01", "action")
			var respBytes []byte
			err = conn.Invoke(ctx, methodPath, reqBytes, &respBytes)
			if err == nil {
				t.Fatalf("invoke succeeded, want %v", tc.want)
			}
			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("error is not a gRPC status: %v", err)
			}
			if st.Code() != tc.want {
				t.Fatalf("code = %v, want %v (message %q)", st.Code(), tc.want, st.Message())
			}
			if st.Message() != tc.wantMsg {
				t.Fatalf("message = %q, want %q", st.Message(), tc.wantMsg)
			}
		})
	}
}
