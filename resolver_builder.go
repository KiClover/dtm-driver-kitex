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

func (b *resolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	return nil, nil // TODO(Task 6)
}

func (b *resolverBuilder) Scheme() string { return b.scheme }

func init() {
	for _, s := range []string{SchemeEtcd, SchemeConsul, SchemeNacos} {
		resolver.Register(&resolverBuilder{scheme: s})
	}
}

// Shutdown deregisters all services and closes owned registry clients. TODO(Task 7).
func (d *Driver) Shutdown(ctx context.Context) error { return nil }
