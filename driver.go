// Package dtmkitex connects DTM to Kitex standard gRPC services through
// etcd, Consul, or Nacos discovery.
package dtmkitex

import "github.com/dtm-labs/dtmdriver"

const DriverName = "dtm-driver-kitex"

const (
	SchemeEtcd   = "kitex-etcd"
	SchemeConsul = "kitex-consul"
	SchemeNacos  = "kitex-nacos"
)

type kitexDriver struct{}

func (d *kitexDriver) GetName() string { return DriverName }

// grpc-go's resolver registry is process-global; schemes are registered once
// by resolverBuilder's package initializer.
func (d *kitexDriver) RegisterAddrResolver() {}

func (d *kitexDriver) RegisterService(target, endpoint string) error {
	if target == "" {
		return nil
	}

	rt, err := parseRegistrationTarget(target)
	if err != nil {
		return err
	}
	dc := rt.discovery
	return registerService(dc, inlineCredentials(dc), endpoint)
}

func (d *kitexDriver) ParseServerMethod(uri string) (server, method string, err error) {
	bt, err := parseBranchTarget(uri)
	if err != nil {
		return "", "", err
	}
	return bt.server, bt.method, nil
}

func init() {
	dtmdriver.Register(&kitexDriver{})
}
