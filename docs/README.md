# cache documentation

Requirements (PRDs) and architecture decisions (ADRs) for cache. The
process is described in [ADR-0001](adr/0001-record-architecture-decisions.md)
and [AGENTS.md](../AGENTS.md); new documents start from
[the PRD template](prd/TEMPLATE.md) or [the ADR template](adr/TEMPLATE.md).

## Product requirements

| PRD | Title | Status | Implementation |
|---|---|---|---|
| [0001](prd/0001-cache.md) | cache | Accepted | Complete |

## Architecture decisions

| ADR | Title | Status |
|---|---|---|
| [0001](adr/0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](adr/0002-generic-store-and-cache-api.md) | Generic store contract with composable caches and functional options | Accepted |
| [0003](adr/0003-single-module-and-dependency-policy.md) | One module for all backends, and the dependency policy | Accepted |
| [0004](adr/0004-error-semantics.md) | Error semantics: sentinels, wrapping, joining and fail-open reads | Accepted |
| [0005](adr/0005-concurrency-and-cancellation.md) | Concurrency and cancellation model | Accepted |
| [0006](adr/0006-backend-adapters-and-conformance-suite.md) | Backend adapters with caller-owned clients and a public conformance suite | Accepted |
| [0007](adr/0007-persistence-and-wire-formats.md) | Versioned entry envelope, hashed keys and namespace generations | Accepted |
| [0008](adr/0008-expiry-semantics.md) | Logical expiry in the entry, native TTLs for cleanup, and NoExpiry | Accepted |
| [0009](adr/0009-best-effort-leases-and-evict-only-invalidation.md) | Best-effort fill leases and evict-only, at-least-once invalidation | Accepted |
| [0010](adr/0010-public-api-boundary.md) | Public API boundary | Accepted |
| [0011](adr/0011-supported-go-versions.md) | Supported Go versions | Accepted |
| [0012](adr/0012-testing-strategy.md) | Testing strategy | Accepted |
