# ADR 0006: Backend adapters with caller-owned clients and a public conformance suite

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (FR-CACHE-06, FR-CACHE-15, FR-CACHE-19), ADR-0002, ADR-0003, ADR-0010 |

## Context

Recorded retrospectively for the first public release. Each backend package
adapts one technology to the root contracts. Their shape is uniform:

- `New` (or `Open` for the bbolt file) returns `*Store[K, V]` implementing
  `cache.Store` and, where the technology allows, `cache.BatchStore`;
- `NewLocker` returns a `cache.Locker` and `NewInvalidationBus` a
  `cache.InvalidationBus` where the technology can provide them safely;
- serialization is configurable through `WithKeyCodec`, `WithValueCodec`,
  `WithLockerKeyCodec` and `WithInvalidationKeyCodec` options;
- remote clients and pools (`redis.UniversalClient`, `valkey.Client`,
  `*memcache.Client`, `*hazelcast.Client`, `*pgxpool.Pool`) are passed in and
  never closed by the library; only the bbolt store owns a resource and has
  `Close`;
- capability gaps are explicit: Hazelcast has no `Locker` because its map API
  cannot atomically verify a token and extend that value's TTL; Memcached
  and Hazelcast have no `BatchStore`.

All backends run the same conformance suites (`RunStoreContract`,
`RunBatchStoreContract`, `RunInvalidationBusContract`), in-process backends
in unit tests and remote backends in integration tests against service
containers.

## Decision drivers

- D1 One contract, verified identically for every backend.
- D2 Connection lifecycle, credentials and pooling belong to the application.
- D3 A backend must not offer a capability it cannot implement safely.
- D4 Third parties must be able to write and verify their own backends.

## Considered options

1. **Uniform adapter packages, caller-owned clients, shared public
   conformance suite** (chosen)
   - Pro: identical guarantees across backends; applications reuse their
     existing clients.
   - Con: the suite is fixed to `string` keys and `int` values.
2. **Backends construct their own clients from configuration**
   - Pro: shorter setup.
   - Con: duplicates every client's configuration surface; prevents sharing
     a pool across caches.
3. **Per-backend ad-hoc tests only**
   - Con: contracts drift; third parties cannot verify against the same
     rules.
4. **Keep the conformance suite internal**
   - Pro: smaller public API.
   - Con: implementers of the public interfaces have no way to check them
     (see ADR-0010).

## Decision

We will keep every backend as an adapter over a caller-owned client with
codec options, expose only the capabilities the technology can implement
safely, and verify every implementation with the public `cachetest`
conformance suites.

## Consequences

### Positive
- New backends have a clear template and a ready-made acceptance test.
- Applications control connection lifecycle and reuse pools.

### Negative
- Callers must close their own clients after the cache is no longer used.
- The conformance suite uses the wall clock because it targets remote
  servers (see ADR-0012).

### Follow-ups
- Consider batch support for PostgreSQL, Memcached and Hazelcast.

## Links

- `backend/`, `cachetest/`
