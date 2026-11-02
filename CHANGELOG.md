# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-11-02

### Added

- DTM driver `dtm-driver-kitex` implementing `dtmdriver.Driver`:
  `ParseServerMethod`, `RegisterService`, `RegisterAddrResolver`.
- Branch/registry URI parsing for `kitex-etcd://`, `kitex-consul://`,
  `kitex-nacos://`, plus direct `host:port` targets, with strict validation
  (unknown scheme/query rejection, userinfo/fragment rejection, endpoint
  validation) and a fuzz target.
- grpc-go resolver bridge over Kitex `discovery.Resolver`: single-goroutine
  state machine with periodic refresh (default 5s), error backoff (100ms →
  1m cap), authoritative-empty grace (default 30s), last-good-state
  retention, and deduplicated `UpdateState` reporting.
- Registry providers for etcd (auth, TLS, custom prefix, dial timeout),
  Consul (datacenter, token, http/https), and Nacos (group, cluster,
  namespace, username/password) with a shared provider cache and explicit
  `Shutdown`.
- Credential profile file (per-scheme profiles with user/password/token,
  namespace, TLS CA/cert/key); inline credentials rejected by default;
  sensitive query keys excluded from cache keys and errors.
- `ServerMetadataMiddleware` bridging Kitex nphttp2 metadata
  (`dtm-gid`, `dtm-trans_type`, `dtm-branch_id`, `dtm-op`, `dtm-dtm`,
  `dtm-phase2_url`) into grpc-go incoming metadata so
  `dtmgrpc.BarrierFromGrpc` works on Kitex servers.
- Package-level `Configure(Config)` with frozen-after-runtime-use semantics.
- DTM upstream integration (dtm-labs/dtm fork): driver registration,
  `MicroService.DriverOptions`, driver lifecycle hooks (`Configure` before
  any listener, `Shutdown` on signal), and registration rollback on failure.

### Verified

- Direct (no registry) raw/typed payload calls against a real Kitex gRPC
  server, including the DTM status-code contract.
- etcd/Consul/Nacos discovery and registration with dynamic membership.
- End-to-end DTM Saga through a real DTM server: raw-payload recovery to
  success and aborted-action compensation over the Kitex driver.
