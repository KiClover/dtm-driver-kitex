// Package dtmkitex implements a dtmdriver.Driver that lets DTM call Kitex
// services over standard gRPC transport, discovered through Kitex registry
// layouts (etcd, Consul, Nacos).
package dtmkitex

import (
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
// It is retained as a public alias for documentation; the concrete builder is
// resolverBuilder in resolver_builder.go.
type Builder = resolverBuilder

func init() {
	dtmdriver.Register(&Driver{})
}
