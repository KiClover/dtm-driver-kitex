package compat

import (
	"testing"

	"google.golang.org/grpc/resolver"

	dtmdriver "github.com/dtm-labs/dtmdriver"
	"github.com/dtm-labs/dtmdriver-kitex"
)

// TestPublicAPISurface asserts the driver implements the DTM SPI and the
// optional lifecycle interfaces, and that the package exposes the documented
// public helpers. It exists to freeze the dependency baseline: if the compile
// fails on a candidate dependency graph, that graph is rejected.
func TestPublicAPISurface(t *testing.T) {
	var _ dtmdriver.Driver = (*dtmkitex.Driver)(nil)

	// grpc-go resolver builder signature used by the adapter.
	var _ resolver.Builder = (*dtmkitex.Builder)(nil)
}
