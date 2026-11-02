package dtmkitex

import (
	"context"
	"errors"
	"strings"
	"testing"

	kitexmetadata "github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/metadata"
	"github.com/dtm-labs/dtm/client/dtmgrpc"
	grpcmetadata "google.golang.org/grpc/metadata"
)

const (
	metadataGID       = "dtm-gid"
	metadataTransType = "dtm-trans_type"
	metadataBranchID  = "dtm-branch_id"
	metadataOp        = "dtm-op"
	metadataDTM       = "dtm-dtm"
	metadataPhase2URL = "dtm-phase2_url"
)

// kitexContext builds a Kitex server-side context carrying md.
func kitexContext(md kitexmetadata.MD) context.Context {
	return kitexmetadata.NewIncomingContext(context.Background(), md)
}

// capturedMetadata runs the middleware and returns what a DTM handler would
// see through the grpc-go metadata API.
func capturedMetadata(t *testing.T, ctx context.Context) (grpcmetadata.MD, bool) {
	t.Helper()
	var (
		got grpcmetadata.MD
		ok  bool
	)
	next := func(ctx context.Context, _, _ interface{}) error {
		got, ok = grpcmetadata.FromIncomingContext(ctx)
		return nil
	}
	if err := ServerMetadataMiddleware(next)(ctx, nil, nil); err != nil {
		t.Fatalf("middleware: %v", err)
	}
	return got, ok
}

func TestServerMetadataMiddlewareDTMHeaders(t *testing.T) {
	ctx := kitexContext(kitexmetadata.MD{
		metadataGID:       {"gid-1"},
		metadataTransType: {"saga"},
		metadataBranchID:  {"01"},
		metadataOp:        {"action"},
		metadataDTM:       {"http://dtm:36789"},
		metadataPhase2URL: {"http://svc/phase2"},
	})
	md, ok := capturedMetadata(t, ctx)
	if !ok {
		t.Fatal("bridged context carries no grpc metadata")
	}
	want := map[string]string{
		metadataGID:       "gid-1",
		metadataTransType: "saga",
		metadataBranchID:  "01",
		metadataOp:        "action",
		metadataDTM:       "http://dtm:36789",
		metadataPhase2URL: "http://svc/phase2",
	}
	for key, value := range want {
		got := md.Get(key)
		if len(got) != 1 || got[0] != value {
			t.Fatalf("md.Get(%q) = %v, want %q", key, got, value)
		}
	}
}

func TestServerMetadataMiddlewareLowercasesKeys(t *testing.T) {
	ctx := kitexContext(kitexmetadata.MD{
		"DTM-GID":       {"gid-1"},
		"X-Custom-Key":  {"v"},
		"DTM-BRANCH_ID": {"02"},
	})
	md, _ := capturedMetadata(t, ctx)
	for _, key := range []string{metadataGID, "x-custom-key", metadataBranchID} {
		if len(md.Get(key)) != 1 {
			t.Fatalf("md.Get(%q) = %v", key, md.Get(key))
		}
	}
	for key := range md {
		if key != strings.ToLower(key) {
			t.Fatalf("bridged key %q is not lower-case", key)
		}
	}
}

func TestServerMetadataMiddlewarePreservesValuesAndUnknownKeys(t *testing.T) {
	binary := string([]byte{0x00, 0x01, 0xff, 'x'})
	ctx := kitexContext(kitexmetadata.MD{
		"x-multi":   {"a", "b", "c"},
		"x-bin-bin": {binary},
		"x-unknown": {"keep-me"},
		metadataGID: {"gid"},
	})
	md, _ := capturedMetadata(t, ctx)
	if got := md.Get("x-multi"); len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Fatalf("x-multi = %v", got)
	}
	if got := md.Get("x-bin-bin"); len(got) != 1 || got[0] != binary {
		t.Fatalf("binary value not preserved: %v", got)
	}
	if got := md.Get("x-unknown"); len(got) != 1 || got[0] != "keep-me" {
		t.Fatalf("unknown metadata dropped: %v", got)
	}
}

func TestServerMetadataMiddlewareDoesNotMutateSource(t *testing.T) {
	src := kitexmetadata.MD{
		"DTM-GID": {"gid"},
		"x-multi": {"a", "b"},
	}
	before := src.Copy()
	capturedMetadata(t, kitexContext(src))
	if src.Len() != before.Len() {
		t.Fatalf("source length changed: %d -> %d", before.Len(), src.Len())
	}
	for key, values := range before {
		got := src[key]
		if len(got) != len(values) {
			t.Fatalf("source %q changed: %v -> %v", key, values, got)
		}
		for i := range got {
			if got[i] != values[i] {
				t.Fatalf("source %q[%d] changed: %q -> %q", key, i, values[i], got[i])
			}
		}
	}
}

func TestServerMetadataMiddlewareWithoutMetadata(t *testing.T) {
	called := false
	next := func(ctx context.Context, _, _ interface{}) error {
		called = true
		if _, ok := grpcmetadata.FromIncomingContext(ctx); ok {
			t.Fatal("no metadata must be injected when Kitex supplies none")
		}
		return nil
	}
	if err := ServerMetadataMiddleware(next)(context.Background(), nil, nil); err != nil {
		t.Fatalf("middleware: %v", err)
	}
	if !called {
		t.Fatal("next was not invoked")
	}
}

func TestServerMetadataMiddlewarePropagatesError(t *testing.T) {
	sentinel := errors.New("handler failed")
	next := func(context.Context, interface{}, interface{}) error { return sentinel }
	err := ServerMetadataMiddleware(next)(kitexContext(kitexmetadata.MD{metadataGID: {"g"}}), nil, nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want sentinel", err)
	}
}

func TestBarrierFromBridgedContext(t *testing.T) {
	ctx := kitexContext(kitexmetadata.MD{
		metadataGID:       {"gid-barrier"},
		metadataTransType: {"saga"},
		metadataBranchID:  {"07"},
		metadataOp:        {"action"},
		metadataDTM:       {"http://dtm:36789"},
	})

	var barrierInfo struct {
		transType, gid, branchID, op string
	}
	next := func(ctx context.Context, _, _ interface{}) error {
		barrier, err := dtmgrpc.BarrierFromGrpc(ctx)
		if err != nil {
			return err
		}
		barrierInfo.transType = barrier.TransType
		barrierInfo.gid = barrier.Gid
		barrierInfo.branchID = barrier.BranchID
		barrierInfo.op = barrier.Op
		return nil
	}
	if err := ServerMetadataMiddleware(next)(ctx, nil, nil); err != nil {
		t.Fatalf("BarrierFromGrpc: %v", err)
	}
	want := struct{ transType, gid, branchID, op string }{"saga", "gid-barrier", "07", "action"}
	if barrierInfo != want {
		t.Fatalf("barrier = %+v, want %+v", barrierInfo, want)
	}
}
