# ADR 0007: Versioned entry envelope, hashed keys and namespace generations

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (FR-CACHE-18, FR-CACHE-19, FR-CACHE-21, NFR-CACHE-07, NFR-CACHE-08) |

## Context

Recorded retrospectively for the first public release. Serialized backends
share data between processes and, for the disk store, between program
versions. The formats in use are implemented in `internal/wire` and in the
backends:

- **Entry envelope.** Version 2, written today: version byte `2`,
  generation (`uint64`), expiry seconds (signed `int64` Unix time),
  expiry nanoseconds (`uint32`, must be below 10^9), flags byte (bit 0
  `Found`, bit 1 nil value), then the value bytes from the value codec, all
  big-endian. Version 1 (expiry as `int64` Unix nanoseconds) is still
  decoded. Version 2 exists because nanosecond Unix time cannot represent
  expirations far in the future.
- **Physical keys.** Remote backends and the default disk layout store the
  SHA-256 digest of the namespace and of the encoded key (for example
  `go-cache:e:<namespace hex>:<key hex>` in Redis-protocol stores, `gc:e:…` in
  Memcached, `BYTEA` columns in PostgreSQL). The disk store's
  `WithOrderedKeys` layout stores the encoded key verbatim (with a one-byte
  tag) in a separate bucket to allow prefix scans.
- **Namespace generations.** `InvalidateAll` increments a per-namespace
  generation (a counter key, row or bbolt value) instead of deleting entries;
  every envelope records the generation it was written under, and reads
  ignore records from older generations.
- **Invalidation message.** Version byte `1`, kind byte, generation
  (`uint64`), namespace length (`uint32`), namespace bytes, then the encoded
  key for key-scoped events. PostgreSQL carries it base64-encoded in
  `NOTIFY` payloads (limited to 8000 bytes).
- **PostgreSQL schema.** Tables `go_cache_entries`, `go_cache_generations`
  and `go_cache_locks` with expiry indexes, created by the idempotent
  `Migrate`; a never-expiring entry is stored with `expires_at = 'infinity'`.

## Decision drivers

- D1 Processes on different versions may share one backend.
- D2 Namespace invalidation must be O(1) on stores without prefix deletion.
- D3 Cache keys may be sensitive or long; remote key lengths are limited.
- D4 Expirations far in the future (and no expiration) must be representable.

## Considered options

1. **Own versioned binary envelope with hashed keys and generations**
   (chosen)
   - Pro: compact, codec-agnostic, explicit versioning; fixed-length keys
     that do not reveal cache keys.
   - Con: stored data is opaque to other tools; hashed layouts cannot list
     or scan keys.
2. **JSON envelope**
   - Pro: human readable.
   - Con: larger, and wraps already-encoded values awkwardly; no cheaper to
     version.
3. **Native per-backend structures (hash fields, columns per field)**
   - Pro: inspectable with backend tools.
   - Con: a different format per backend; harder to keep semantics identical.
4. **Delete-by-prefix for `InvalidateAll`**
   - Pro: reclaims space immediately.
   - Con: O(n), unavailable or unsafe on several backends (Memcached has no
     key listing).

## Decision

We will keep the versioned envelope and invalidation message in
`internal/wire`, hash namespaces and keys with SHA-256 for remote storage,
implement `InvalidateAll` with namespace generations, and change any of
these formats only reader-first: every process that reads a shared namespace
must understand a new version before any process writes it. If such a staged
rollout is impossible, writers move to a new namespace instead.

## Consequences

### Positive
- Mixed-version fleets can share a backend during upgrades.
- Namespace invalidation is a single counter update everywhere.

### Negative
- Records from older generations stay until they expire, are overwritten,
  or (PostgreSQL) are removed by `DeleteExpired`.
- Hashed and ordered disk layouts are separate; a file must always be opened
  with the same choice.

### Follow-ups
- None.

## Links

- `internal/wire/`, `backend/postgres/store.go`, `backend/disk/store.go`
