package dtmkitex

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/kitex-contrib/registry-nacos/registry"
	"github.com/kitex-contrib/registry-nacos/resolver"
	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/vo"
)

// This file maps the normalized discoveryConfig plus resolved credentials onto
// registry-nacos v0.1.3 objects (nacos-sdk-go v1.1.x). The SDK exposes no Close,
// so discovery and registration clients live for their cache entry's lifetime.

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

func newNacosConfig(dc *discoveryConfig, creds credentialProfile) (*constant.ClientConfig, []constant.ServerConfig, error) {
	servers := make([]constant.ServerConfig, 0, len(dc.endpoints))
	for _, endpoint := range dc.endpoints {
		host, portText, err := net.SplitHostPort(endpoint)
		if err != nil {
			return nil, nil, fmt.Errorf("dtm-driver-kitex: nacos endpoint %q must be host:port", endpoint)
		}
		port, err := strconv.ParseUint(portText, 10, 64)
		if err != nil || port == 0 {
			return nil, nil, fmt.Errorf("dtm-driver-kitex: nacos endpoint %q has an invalid port", endpoint)
		}
		servers = append(servers, *constant.NewServerConfig(host, port))
	}

	if (creds.User == "") != (creds.Password == "") {
		return nil, nil, fmt.Errorf("dtm-driver-kitex: nacos username and password must be set together")
	}

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
	return cfg, servers, nil
}

func newNacosNamingClient(dc *discoveryConfig, creds credentialProfile) (naming_client.INamingClient, error) {
	cfg, servers, err := newNacosConfig(dc, creds)
	if err != nil {
		return nil, err
	}
	client, err := clients.NewNamingClient(vo.NacosClientParam{
		ClientConfig:  cfg,
		ServerConfigs: servers,
	})
	if err != nil {
		return nil, fmt.Errorf("dtm-driver-kitex: create nacos client: %w", err)
	}
	return client, nil
}

func newNacosDiscovery(dc *discoveryConfig, creds credentialProfile) (discovery.Resolver, error) {
	client, err := newNacosNamingClient(dc, creds)
	if err != nil {
		return nil, err
	}
	group, cluster := nacosOptions(dc)
	return resolver.NewNacosResolver(client,
		resolver.WithGroup(group),
		resolver.WithCluster(cluster),
	), nil
}

func newNacosRegistration(dc *discoveryConfig, creds credentialProfile) (*registrationProvider, error) {
	client, err := newNacosNamingClient(dc, creds)
	if err != nil {
		return nil, err
	}
	group, cluster := nacosOptions(dc)
	return &registrationProvider{registry: registry.NewNacosRegistry(client,
		registry.WithGroup(group),
		registry.WithCluster(cluster),
	)}, nil
}

func init() {
	registerProviderFactories(SchemeNacos, newNacosDiscovery, newNacosRegistration)
}
