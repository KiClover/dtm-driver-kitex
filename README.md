# dtm-driver-kitex

[DTM](https://github.com/dtm-labs/dtm) driver that lets a DTM server discover
and invoke branches published by [Kitex](https://github.com/cloudwego/kitex)
gRPC servers. It registers the DTM server itself into a Kitex registry and
resolves DTM branch URLs (`kitex-etcd://...`) into standard gRPC calls, so a
single DTM server can drive Saga/TCC/MSG transactions across Kitex services.

The driver speaks **standard gRPC over HTTP/2** — not Kitex's TTHeader/Thrift
transport. Kitex servers must be generated with protobuf and serve the
standard gRPC protocol (the default for `kitex -type protobuf`).

## Supported matrix

| Capability | Support |
| --- | --- |
| Registries | etcd (`registry-etcd v0.2.6`), Consul (`registry-consul v0.2.0`), Nacos (`registry-nacos v0.1.3`) |
| Transports | standard gRPC (protobuf); **no** TTHeader, **no** Thrift, **no** Hertz |
| Payloads | typed protobuf and raw (`isRaw`) payloads |
| Load balancing | `pick_first` (grpc-go default) and `round_robin` |
| Weight / tag routing | **not supported** — instances are passed with a `kitex.weight` attribute but the grpc-go balancers above ignore it |
| Direct connections | `host:port/<grpc.Service>/<Method>` |

## URI formats

Branch URLs use `<discovery-service>/<grpc-service>/<method>` where
`<grpc-service>/<method>` is the full gRPC method name:

```text
# direct (no registry)
host:port/order.OrderService/Create

# etcd (multiple comma-separated endpoints allowed)
kitex-etcd://127.0.0.1:2379/order-svc/order.OrderService/Create

# consul (exactly one endpoint)
kitex-consul://127.0.0.1:8500/order-svc/order.OrderService/Create

# nacos (exactly one endpoint)
kitex-nacos://127.0.0.1:8848/order-svc/order.OrderService/Create
```

Everything after the discovery service (the query string) configures the
registry backend, not the method. Common query keys for all schemes:

| Key | Default | Meaning |
| --- | --- | --- |
| `refresh_interval` | `5s` | resolver polling interval |
| `empty_grace` | `30s` | how long an authoritative empty result must persist before the resolver reports no addresses |
| `default_weight` | `10` | weight applied to registered instances when unset |
| `credential_profile` | — | name of a profile from the credential file (see below) |

Scheme-specific keys:

- `kitex-etcd`: `prefix`, `user`, `password`, `dial_timeout` (and comma-separated endpoint lists)
- `kitex-consul`: `datacenter`, `token`, `scheme` (`http`/`https`)
- `kitex-nacos`: `group` (default `DEFAULT_GROUP`), `cluster` (default `DEFAULT`), `namespace`, `username`, `password`

Unknown keys are rejected at parse time. Consul registers with the agent-side
TCP health check (5s interval, deregistered after 1m critical); Nacos
instances are ephemeral and kept alive by the registering client's heartbeat.

## Server-side middleware (required)

DTM delivers transaction metadata (`dtm-gid`, `dtm-trans_type`,
`dtm-branch_id`, `dtm-op`, `dtm-dtm`) as gRPC metadata. Kitex stores incoming
metadata under its own context keys, so install the bridge middleware on every
Kitex branch server, or `dtmgrpc.BarrierFromGrpc(ctx)` fails:

```go
import (
    "github.com/cloudwego/kitex/server"
    "github.com/dtm-labs/dtmdriver-kitex"
)

srv := order.NewServer(handler,
    server.WithMiddleware(dtmkitex.ServerMetadataMiddleware),
)
```

The middleware copies DTM metadata keys from Kitex's nphttp2 metadata into
grpc-go's `metadata.NewIncomingContext`. It performs no barrier SQL and never
alters handler results.

## DTM server configuration

```yaml
MicroService:
  Driver: 'dtm-driver-kitex'
  Target: 'kitex-etcd://localhost:2379/dtm-server' # or kitex-consul / kitex-nacos
  EndPoint: 'localhost:36790'
  # DriverOptions: '{}' # optional JSON for the driver's Configure hook
```

`DriverOptions` accepts two keys: `credential_file` (path to the YAML
credential profile file) and `allow_inline_credentials` (default `false`;
when false, inline `user=`/`password=`/`token=` URI credentials are rejected).

## Credential profiles

Secrets belong in a credential file, not in URIs. Reference a profile per
backend scheme via `?credential_profile=<name>`:

```yaml
profiles:
  kitex-etcd:
    prod:
      user: dtm
      password: secret
      tls_ca: /etc/dtm/etcd-ca.pem
      tls_cert: /etc/dtm/etcd-client.pem
      tls_key: /etc/dtm/etcd-client-key.pem
  kitex-consul:
    prod:
      token: <acl-token>
  kitex-nacos:
    prod:
      username: dtm
      password: secret
      namespace: dtm
```

Operational rules:

- **File permissions**: restrict the credential file to the DTM server's OS
  user (on Linux `chmod 600`); it is read once at `Configure` time.
- **TLS files must be mounted on the DTM Server that actually performs
  discovery** — the client side builds the TLS config, so a registry agent or
  sidecar mount is not enough.
- **Rotation**: update the file and restart the DTM server; configuration is
  frozen after first runtime use (`Configure` then fails with
  `ErrAlreadyStarted`).
- **Cluster consistency**: every DTM server in a cluster must use the same
  credential profile set, otherwise instances registered with one profile are
  unreachable from servers using another.
- **Inline credentials default off**: enable `allow_inline_credentials` only
  for throwaway environments.
- Upstream SDK environment variables (etcd/consul/nacos) are not consulted by
  this driver except where the underlying SDK reads them directly; explicit
  configuration in URIs or profiles always wins.

## Explicit shutdown

The driver holds registry clients and background refresh goroutines. DTM
server (with the lifecycle integration from
`dtmsvr.ShutdownDriver`) calls `Shutdown` on signal. Standalone users should
call it before process exit to deregister the DTM endpoint:

```go
err := dtmkitex.Shutdown(ctx) // idempotent; also closes cached registry clients
```

## Failure semantics

Branch handlers communicate outcomes through gRPC status codes, built with
Kitex's `pkg/remote/trans/nphttp2/status` (a plain `grpc/status` error is
flattened to `codes.Internal` by Kitex's transport):

- `OK` — branch succeeded
- `codes.Aborted` — definite failure; DTM rolls the transaction back
- `codes.FailedPrecondition` — "ONGOING"; DTM retries the branch
- any other error — treated as infrastructure failure and retried

## Troubleshooting

- **`no instance remains for <service>`** — the discovery service has no
  healthy instances. With etcd, check for leftover dead endpoints:
  `etcdctl get "kitex/registry-etcd/<service>" --prefix`. With Consul, a
  registered address unreachable *from the consul agent's container/host*
  turns the TCP check critical and hides the instance.
- **`BarrierFromGrpc` fails** — the branch server is missing
  `ServerMetadataMiddleware`.
- **Biz error surfaces as `Internal` with `[biz error]` suffix** — expected
  Kitex behavior for non-status errors; use `npstatus.Errorf` with the code
  you mean.
- **DTM server ignores `conf.yaml`** — `dtm` only reads a config file when
  `-c <path>` is passed.
- **Log hygiene**: credential values, `user`, `password`, `token`,
  `username`, and `credential_profile` never appear in resolver cache keys or
  error output; when filing issues, redact them the same way.

## Version compatibility

- Go ≥ 1.22 (matches DTM)
- Kitex v0.12.x, grpc-go v1.59.x, dtmdriver v0.0.6, dtm client v1.18.x
- `registry-etcd` stays on v0.2.6: v0.3.0 requires Go ≥ 1.23

## License

MIT
