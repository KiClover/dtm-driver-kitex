# Security Policy

## Reporting a vulnerability

Do not open a public GitHub issue for security problems. Email 3533404885@qq.com with the affected version or commit, reproduction steps or a proof of concept, and an impact assessment.

You will receive an acknowledgment within 3 business days and a fix timeline within 7 business days.

## Scope

In scope:

- URI parsing in `target.go`, including userinfo, fragment, endpoint, path, and query-key validation
- URI credential handling in `credentials.go`, including leakage through errors, logs, or provider cache keys
- metadata bridging in `middleware.go`, including unsafe handling of DTM transaction identity fields

Vulnerabilities in DTM, Kitex, grpc-go, etcd, Consul, or Nacos should also be reported to the relevant upstream project.

## Handling secrets

- etcd `user`/`password`, Consul `token`, and Nacos `username`/`password` are accepted only as documented URI query parameters.
- Sensitive values are removed from canonical target strings and are represented in provider cache identities by a SHA-256 digest.
- Parser and provider errors must not echo the full credential-bearing URI.
- DTM may persist branch URLs. Deployments using URI credentials must protect DTM transaction storage, diagnostic output, and configuration, and should use dedicated least-privilege registry accounts.
- Credential profile files, custom CA files, and client certificates are not supported by the minimal package API.
