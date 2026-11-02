# Security Policy

## Reporting a vulnerability

Do **not** open a public GitHub issue for security problems. Email
3533404885@qq.com with:

- affected version / commit
- reproduction steps or a PoC
- impact assessment

You will receive an acknowledgment within 3 business days and a fix timeline
within 7 business days.

## Scope

In scope:

- URI parsing (`target.go`) — injection via branch URLs or registry targets,
  e.g. smuggling userinfo, fragments, or unexpected paths into backend
  addresses
- credential handling (`credentials.go`, `config.go`) — leakage of `user`,
  `password`, `token`, `username`, or `credential_profile` values through
  error strings, cache keys, or logs
- the metadata bridge (`middleware.go`) — header smuggling into barrier
  identity fields

Out of scope: vulnerabilities in etcd/Consul/Nacos, Kitex, grpc-go, or DTM
themselves; report those upstream.

## Handling of secrets

- Credential file contents are loaded once at `Configure` and never logged.
- Sensitive query keys are excluded from resolver cache keys and error output
  (`sensitiveQueryKeys` in `target.go`).
- Inline credentials are rejected unless `allow_inline_credentials` is
  explicitly enabled.
