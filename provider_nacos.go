package dtmkitex

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"

	"github.com/kitex-contrib/registry-nacos/registry"
	"github.com/kitex-contrib/registry-nacos/resolver"
	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/vo"
)

// This file maps the normalized discoveryConfig plus resolved credentials onto
// registry-nacos v0.1.3 objects (nacos-sdk-go v1.1.x).
//
// Design notes:
//   - registry-nacos's NewNacosResolver/NewNacosRegistry return their
//     implementations without an error: construction cannot fail, so provider
//     construction failures only come from client creation.
//   - nacos-sdk-go v1's INamingClient exposes no Close; the SDK's background
//     threads (beat reactor, host reactor update thread, push receiver) live
//     for the process lifetime. closeFn stays nil. The v2 SDK has Close but
//     requires a newer Kitex than this driver pins.
//   - Resolve reports instance loss as an error ("no instance remains"), not
//     as an authoritative empty Result, so the resolver's empty-grace path is
//     never exercised by nacos: the error path (keep last state, ReportError,
//     back off) covers it and empty_grace has no effect on this backend.
//   - UpdateCacheWhenEmpty=true: SelectInstances serves from the SDK's host
//     reactor cache, and with the default false a server-side empty never
//     propagates, leaving stale instances visible after failover.
//   - nacos-sdk-go v1 persists cache and log to the current directory by
//     default; CacheDir/LogDir are pointed at a per-process temp directory to
//     keep the working directory clean.
//   - Ephemeral instances heartbeat from the register-side client, so the
//     address registered is dialed only by resolving clients (no agent-side
//     health dial, unlike Consul).

// nacosOptions maps query parameters onto registry/resolver options.
func nacosOptions(dc *discoveryConfig) (group, cluster string) {
	group = dc.query.Get("group")
	if group == "" {
		group = "DEFAULT_GROUP"
	}
	cluster = dc.query.Get("cluster")
	if cluster == "" {
		cluster = "DEFAULT"
	}
	return group, cluster
}

// newNacosConfig builds the client config and server list for the naming
// client. Endpoints must be host:port; username and password must be paired.
func newNacosConfig(dc *discoveryConfig, creds credentialProfile) (*constant.ClientConfig, []constant.ServerConfig, error) {
	servers := make([]constant.ServerConfig, 0, len(dc.endpoints))
	for _, ep := range dc.endpoints {
		host, portText, err := net.SplitHostPort(ep)
		if err != nil {
			return nil, nil, fmt.Errorf("dtm-driver-kitex: nacos endpoint %q must be host:port", ep)
		}
		port, err := strconv.ParseUint(portText, 10, 64)
		if err != nil || port == 0 {
			return nil, nil, fmt.Errorf("dtm-driver-kitex: nacos endpoint %q has an invalid port", ep)
		}
		servers = append(servers, *constant.NewServerConfig(host, port))
	}

	if (creds.User == "") != (creds.Password == "") {
		return nil, nil, fmt.Errorf("dtm-driver-kitex: nacos username and password must be set together")
	}

	// The per-process temp dir avoids cross-process log contention on Windows.
	dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("dtm-driver-kitex-nacos-%d", os.Getpid()))

	cfg := &constant.ClientConfig{
		TimeoutMs:            10000,
		NotLoadCacheAtStart:  true,
		UpdateCacheWhenEmpty: true,
		Username:             creds.User,
		Password:             creds.Password,
		CacheDir:             dataDir,
		LogDir:               dataDir,
		LogLevel:             "warn",
	}
	cfg.NamespaceId = dc.query.Get("namespace")
	if cfg.NamespaceId == "" {
		cfg.NamespaceId = creds.Namespace
	}
	return cfg, servers, nil
}

// newNacosProvider builds the Nacos discovery resolver and registration
// registry sharing one naming client.
func newNacosProvider(dc *discoveryConfig, creds credentialProfile) (*provider, error) {
	cfg, servers, err := newNacosConfig(dc, creds)
	if err != nil {
		return nil, err
	}
	cli, err := clients.NewNamingClient(vo.NacosClientParam{
		ClientConfig:  cfg,
		ServerConfigs: servers,
	})
	if err != nil {
		return nil, fmt.Errorf("dtm-driver-kitex: create nacos client: %w", err)
	}

	group, cluster := nacosOptions(dc)
	disc := resolver.NewNacosResolver(cli,
		resolver.WithGroup(group),
		resolver.WithCluster(cluster),
	)
	reg := registry.NewNacosRegistry(cli,
		registry.WithGroup(group),
		registry.WithCluster(cluster),
	)
	return &provider{
		name:      "nacos",
		discovery: disc,
		registry:  reg,
		closeFn:   nil, // nacos-sdk-go v1 exposes no Close; see design notes.
	}, nil
}

func init() {
	registerProviderFactory(SchemeNacos, newNacosProvider)
}
