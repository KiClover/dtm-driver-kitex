package dtmkitex

import "google.golang.org/grpc/resolver"

type resolverBuilder struct {
	scheme string
}

func (b *resolverBuilder) Scheme() string { return b.scheme }

func init() {
	for _, scheme := range []string{SchemeEtcd, SchemeConsul, SchemeNacos} {
		resolver.Register(&resolverBuilder{scheme: scheme})
	}
}
