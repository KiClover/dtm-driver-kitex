package dtmkitex

import (
	"context"
	"google.golang.org/grpc/resolver"
)

// shutdownDriver is the optional lifecycle interface DTM upstream may assert
// on the concrete driver value.
type shutdownDriver interface {
	Shutdown(ctx context.Context) error
}

// resolverBuilder is the grpc-go builder delegate used for all three schemes.
type resolverBuilder struct {
	scheme string
}

func (b *resolverBuilder) Scheme() string { return b.scheme }

func init() {
	for _, s := range []string{SchemeEtcd, SchemeConsul, SchemeNacos} {
		resolver.Register(&resolverBuilder{scheme: s})
	}
}

// Shutdown deregisters all services and closes owned registry clients.
// Errors from individual backends are collected; the first is returned.
func (d *Driver) Shutdown(ctx context.Context) error {
	var firstErr error
	if err := defaultRegistrations.deregisterAll(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := defaultProviders.closeAll(ctx); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
