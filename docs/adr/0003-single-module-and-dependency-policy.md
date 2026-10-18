# ADR 0003: One module for all backends, and the dependency policy

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0006 |

## Context

Recorded retrospectively for the first public release. The root package and
the eight backend packages (`backend/disk`, `dragonfly`, `hazelcast`,
`memcached`, `memory`, `postgres`, `redis`, `valkey`) form a single Go
module, released with one tag. The module requires these third-party
modules:

| Dependency | Used by | Why |
|---|---|---|
| `go.etcd.io/bbolt` | `backend/disk` | embedded, transactional, ordered key-value file |
| `github.com/redis/go-redis/v9` | `backend/redis`, `backend/dragonfly` | the standard Redis client; Dragonfly speaks the Redis protocol |
| `github.com/valkey-io/valkey-go` | `backend/valkey` | the Valkey project's client |
| `github.com/bradfitz/gomemcache` | `backend/memcached` | the de facto Memcached client, with CAS and custom dialers |
| `github.com/hazelcast/hazelcast-go-client` | `backend/hazelcast` | the official Hazelcast client |
| `github.com/jackc/pgx/v5` | `backend/postgres` | the standard PostgreSQL driver, with pools and LISTEN/NOTIFY |
| `github.com/stretchr/testify` | tests and `cachetest` | assertions; the public conformance suite reports through it |

and three sibling libraries:

| Dependency | Used by | Why |
|---|---|---|
| `github.com/justusschock/go-concurrency` | root (`flight.go`) | typed futures for single-flight fills |
| `github.com/justusschock/go-distributed-coordination` | root (`CoordinationLocker`) | lease backends for fill coordination |
| `github.com/justusschock/go-messaging` | root (`MessagingInvalidationBus`) | transport-neutral invalidation delivery |

Everything else uses the standard library.

## Decision drivers

- D1 Applications should compile and link only the clients they import.
- D2 Releasing and versioning should be simple: one tag, one changelog.
- D3 Third-party code is a maintenance and supply-chain cost; each module
  must earn its place.

## Considered options

1. **One module, backends as sub-packages** (chosen)
   - Pro: one tag versions the whole library; backends cannot drift from the
     root contract.
   - Pro: Go compiles only imported packages, so an application that imports
     `backend/memory` does not link Redis or PostgreSQL clients.
   - Con: every backend client appears in `go.mod` and the module graph that
     `go mod download` fetches.
2. **One module per backend**
   - Pro: minimal module graph per backend.
   - Con: a release becomes a coordinated multi-tag operation, and backends
     can pin an older root contract.
3. **Hand-written protocol clients to avoid dependencies**
   - Pro: no third-party code.
   - Con: re-implementing Redis, PostgreSQL or Hazelcast protocols is far
     riskier than depending on maintained clients.

## Decision

We will keep the root package and all backends in one module released with
one tag. The root package depends only on the standard library and the three
sibling libraries listed above; backend client modules are imported only by
their backend package. A new third-party dependency, or a new import of one
from the root package, requires an ADR.

## Consequences

### Positive
- Simple releases; consumers depend on one version.
- The root package stays small; backends are opt-in at compile time.

### Negative
- `go mod download` and Dependabot track every backend client, even for
  applications that use one backend.
- testify is a non-test dependency of the public `cachetest` package.

### Follow-ups
- Revisit per-backend modules if a client's dependency graph becomes a burden
  for users who do not import it.

## Links

- `go.mod`
