# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- Reduced the public API to `DriverName`, the three registry scheme constants, and `ServerMetadataMiddleware`; the concrete DTM driver and grpc resolver builder are now internal.
- Registry credentials are accepted directly from documented URI query parameters. Provider cache identities use SHA-256 digests rather than raw secrets.
- `RegisterService` now delegates every call to the selected Kitex registry without process-global registration deduplication.
- Integration tests consume the plugin through `dtmdriver.Use("dtm-driver-kitex")` and `dtmdriver.GetDriver()`.

### Removed

- `Config`, `Configure`, `ErrAlreadyStarted`, YAML credential profiles, and configuration freeze behavior.
- Exported shutdown and process-global registration lifecycle tracking.
- Built-in custom CA, client-certificate, and credential-profile TLS configuration.

## [0.1.0] - 2026-11-02

### Added

- DTM driver `dtm-driver-kitex` implementing `dtmdriver.Driver`.
- Strict direct and registry URI parsing for etcd, Consul, and Nacos.
- grpc-go resolver bridge over Kitex `discovery.Resolver`, with periodic refresh, backoff, last-good-state retention, empty-result grace, and deduplicated state updates.
- Registry providers for etcd, Consul, and Nacos.
- `ServerMetadataMiddleware` for DTM barrier metadata on Kitex gRPC servers.
- Direct, registry-backed, raw-payload, dynamic-membership, and Saga integration coverage.
