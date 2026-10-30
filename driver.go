// Package dtmkitex implements a dtmdriver.Driver that lets DTM call Kitex
// services over standard gRPC transport, discovered through Kitex registry
// layouts (etcd, Consul, Nacos).
package dtmkitex

import (
	"context"

	"google.golang.org/grpc/resolver"

	dtmdriver "github.com/dtm-labs/dtmdriver"
)

// DriverName is the name registered with dtmdriver.
const DriverName = "dtm-driver-kitex"

// Supported registry URI schemes.
const (
	SchemeEtcd   = "kitex-etcd"
	SchemeConsul = "kitex-consul"
	SchemeNacos  = "kitex-nacos"
)

// Driver implements dtmdriver.Driver for Kitex standard gRPC services.
type Driver struct{}

// GetName returns DriverName.
func (d *Driver) GetName() string { return DriverName }

// RegisterAddrResolver is a no-op: the dedicated resolver schemes are
// registered once in package init because grpc-go's resolver registry is
// process-global and not safe for runtime re-registration.
func (d *Driver) RegisterAddrResolver() {}

// RegisterService registers the DTM server endpoint under the given target.
func (d *Driver) RegisterService(target string, endpoint string) error {
	return nil // TODO(Task 7): implement via registry manager.
}

// ParseServerMethod splits a branch URL into the resolver target and the
// fully-qualified gRPC method.
func (d *Driver) ParseServerMethod(uri string) (server string, method string, err error) {
	return "", "", nil // TODO(Task 2): implement strict parsing.
}

// Builder adapts a Kitex discovery resolver into a grpc-go resolver builder.
type Builder struct{}

// Build implements resolver.Builder.
func (b *Builder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	return nil, nil // TODO(Task 6): implement resolver adapter.
}

// Scheme implements resolver.Builder.
func (b *Builder) Scheme() string { return SchemeEtcd }

// Configure applies driver configuration from a JSON string (DTM
// DriverOptions). TODO(Task 3): implement strict decoding and freeze.
func (d *Driver) Configure(raw string) error { return nil }

// Shutdown deregisters all services and closes owned clients. TODO(Task 7).
func (d *Driver) Shutdown(ctx context.Context) error { return nil }

func init() {
	dtmdriver.Register(&Driver{})
}
