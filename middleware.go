package dtmkitex

import (
	"context"
	"strings"

	"github.com/cloudwego/kitex/pkg/endpoint"
	kitexmetadata "github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/metadata"
	grpcmetadata "google.golang.org/grpc/metadata"
)

// ServerMetadataMiddleware bridges Kitex inbound HTTP/2 metadata into grpc-go
// incoming metadata so DTM's dtmgrpc helpers (BarrierFromGrpc,
// TransBaseFromGrpc) can read the transaction headers.
//
// It is a pure context adapter: it never executes barrier SQL, never rewrites a
// handler result, and never logs metadata values, because those values include
// the global transaction id and branch identifiers.
func ServerMetadataMiddleware(next endpoint.Endpoint) endpoint.Endpoint {
	return func(ctx context.Context, req, resp interface{}) error {
		src, ok := kitexmetadata.FromIncomingContext(ctx)
		if !ok || src.Len() == 0 {
			return next(ctx, req, resp)
		}

		dst := make(grpcmetadata.MD, len(src))
		for key, values := range src {
			// HTTP/2 field names are case-insensitive and grpc-go metadata
			// lookups assume lower-case keys; Kitex preserves the received
			// casing, so normalize here.
			lower := strings.ToLower(key)
			dst[lower] = append(dst[lower], values...)
		}
		return next(grpcmetadata.NewIncomingContext(ctx, dst), req, resp)
	}
}
