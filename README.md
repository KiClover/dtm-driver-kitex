English | [简体中文](README.zh-CN.md)

# dtm-driver-kitex

[DTM](https://github.com/dtm-labs/dtm) driver for Kitex protobuf services that use standard gRPC over HTTP/2. It lets DTM register itself and resolve branch URLs through Kitex-compatible etcd, Consul, or Nacos registries.

Kitex TTHeader, Thrift, and Hertz transports are not supported.

## Install and enable

The package registers itself when imported. Applications normally use a blank import and select it through DTM's standard driver API:

```go
import (
    "github.com/dtm-labs/dtmdriver"
    _ "github.com/dtm-labs/dtmdriver-kitex"
)

if err := dtmdriver.Use("dtm-driver-kitex"); err != nil {
    panic(err)
}
```

A DTM Server build must contain the same blank import. Its configuration stays identical to other DTM microservice drivers:

```yaml
MicroService:
  Driver: 'dtm-driver-kitex'
  Target: 'kitex-etcd://localhost:2379/dtm-server'
  EndPoint: 'localhost:36790'
```

The public package API is intentionally small: `DriverName`, `SchemeEtcd`, `SchemeConsul`, `SchemeNacos`, and `ServerMetadataMiddleware`.

## Supported matrix

| Capability | Support |
| --- | --- |
| Registries | etcd (`registry-etcd v0.2.6`), Consul (`registry-consul v0.2.0`), Nacos (`registry-nacos v0.1.3`) |
| Transport | standard gRPC / HTTP/2 with protobuf |
| Payloads | typed protobuf and DTM raw (`isRaw`) payloads |
| Load balancing | grpc-go `pick_first` and `round_robin` |
| Direct connections | `host:port/<grpc.Service>/<Method>` |
| Weight routing | weight is preserved as `kitex.weight`; standard grpc-go balancers ignore it |

## URI formats

Branch URLs contain `<discovery-service>/<grpc-service>/<method>`:

```text
# direct
127.0.0.1:8080/order.OrderService/Create

# registry-backed
kitex-etcd://127.0.0.1:2379/order-svc/order.OrderService/Create
kitex-consul://127.0.0.1:8500/order-svc/order.OrderService/Create
kitex-nacos://127.0.0.1:8848/order-svc/order.OrderService/Create
```

etcd accepts multiple comma-separated endpoints. Consul and Nacos accept one endpoint.

Common query keys:

| Key | Default | Meaning |
| --- | --- | --- |
| `refresh_interval` | `5s` | discovery polling interval |
| `empty_grace` | `30s` | duration an empty result must persist before addresses are cleared |
| `default_weight` | `10` | registration weight |

Backend-specific keys:

- etcd: `prefix`, `user`, `password`, `dial_timeout`
- Consul: `datacenter`, `token`, `scheme` (`http` or `https`)
- Nacos: `group`, `cluster`, `namespace`, `username`, `password`

Unknown keys, userinfo, fragments, malformed endpoints, and invalid service or method paths are rejected before any provider is created.

### Credentials

Credentials are supplied in the registry URI:

```text
kitex-etcd://127.0.0.1:2379/order-svc?user=dtm&password=secret
kitex-consul://127.0.0.1:8500/order-svc?token=acl-token
kitex-nacos://127.0.0.1:8848/order-svc?username=dtm&password=secret&namespace=prod
```

DTM persists branch URLs, so URI credentials may also be persisted in transaction data. Use a registry-side network boundary or a dedicated least-privilege account, control access to DTM storage and logs, and rotate credentials according to your deployment policy. Raw credential values are excluded from provider cache keys and parser errors; the cache key includes a SHA-256 identity derived from the credentials.

Custom CA files, client certificates, and credential profile files are not part of the minimal API. Configure registry TLS trust at the host/container level when the upstream SDK supports it.

## Required Kitex server middleware

DTM branch helpers read transaction fields from grpc-go incoming metadata. Kitex stores HTTP/2 metadata in its own context, so every Kitex branch server must install the bridge:

```go
import (
    "github.com/cloudwego/kitex/server"
    dtmkitex "github.com/dtm-labs/dtmdriver-kitex"
)

srv := order.NewServer(handler,
    server.WithMiddleware(dtmkitex.ServerMetadataMiddleware),
)
```

The middleware only copies metadata into a grpc-go incoming context. It does not execute barrier SQL, modify results, or log metadata values.

## Runtime behavior

- Resolver schemes are registered once during package initialization. `RegisterAddrResolver` is therefore an intentional no-op.
- Each target uses a single-goroutine resolver state machine with periodic refresh, exponential error backoff, last-good-state retention, an empty-result grace period, and coalesced `ResolveNow` requests.
- Provider clients are shared for the process lifetime. `RegisterService` delegates every call to the selected Kitex registry. There is no exported shutdown API; registry leases, health checks, or ephemeral instances handle process-loss cleanup according to backend behavior.
- Consul registrations use an agent-side TCP health check. Nacos registrations are ephemeral and maintained by the SDK heartbeat.

## Failure semantics

Kitex handlers must create status errors with `github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/status`; plain grpc-go status errors can be flattened to `codes.Internal` by Kitex's transport.

- `OK`: branch succeeded
- `codes.Aborted`: definite failure; DTM rolls back
- `codes.FailedPrecondition`: ongoing; DTM retries
- other errors: infrastructure failure; DTM retries

## Troubleshooting

- `no instance remains for <service>`: the registry has no healthy instance. For Consul, confirm the registered address is reachable from the Consul agent.
- `BarrierFromGrpc` fails: install `ServerMetadataMiddleware` on the branch server.
- DTM ignores `conf.yaml`: start DTM with `-c <path>`.
- A business error becomes `Internal` with a `[biz error]` suffix: return an nphttp2 status error with the intended code.

## Compatibility

- Go 1.22 or newer
- Kitex v0.12.x, grpc-go v1.59.x, dtmdriver v0.0.6, DTM client v1.18.x
- `registry-etcd` remains on v0.2.6 because v0.3.0 requires Go 1.23

## License

MIT
