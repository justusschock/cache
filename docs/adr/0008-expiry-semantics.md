# ADR 0008: Logical expiry in the entry, native TTLs for cleanup, and NoExpiry

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (FR-CACHE-02, FR-CACHE-03, FR-CACHE-19), ADR-0007 |

## Context

Recorded retrospectively for the first public release. Backends differ in
how they expire data: Redis-protocol servers have millisecond TTLs,
Memcached has second-granularity relative TTLs up to 30 days and 32-bit
absolute times beyond, Hazelcast has per-entry TTLs, PostgreSQL and bbolt
have none, and the memory store sweeps periodically. Layered caches must
also promote entries between layers without extending their lifetime.

The code therefore treats `Entry.ExpiresAt` as the authoritative, logical
expiration: every store hides entries for which `Entry.Expired(now)` is true,
regardless of native TTL support, and native TTLs are only used so that the
backend reclaims space. Memcached rounds TTLs up and uses expiration `0`
beyond its 32-bit range, relying on the envelope; PostgreSQL compares
against the server clock. A zero `ExpiresAt` means "never expires";
callers request it with the explicit sentinel `NoExpiry`
(`math.MinInt64`), while every other non-positive TTL is rejected.

## Decision drivers

- D1 Identical expiry semantics on every backend, whatever its TTL support.
- D2 Promotion between layers must preserve the original expiration.
- D3 "Never expires" must be an explicit opt-in, not an accident of a zero
  or negative duration.

## Considered options

1. **Logical expiry in the entry, native TTL for cleanup only** (chosen)
   - Pro: uniform semantics and exact promotion.
   - Con: expired data may occupy space until native cleanup, the memory
     sweeper or `DeleteExpired` removes it.
2. **Native TTL as the only expiry mechanism**
   - Pro: no expiry check on read.
   - Con: impossible on bbolt and PostgreSQL; Memcached granularity and range
     limits would leak into semantics.
3. **Treat a zero or negative TTL as "no expiry"**
   - Pro: no sentinel needed.
   - Con: a computed TTL that underflows silently becomes immortal.

## Decision

We will store the logical expiration in every entry and enforce it on read
in every store, use native TTLs only for physical cleanup and never attach a
native TTL to a never-expiring entry, represent "never expires" as a zero
`ExpiresAt`, and accept it only through the explicit `NoExpiry` TTL.

## Consequences

### Positive
- Expiry behaves the same on every backend and survives promotion.
- `SetEntry` can write back an entry read with `GetEntry` unchanged.

### Negative
- Expired records may remain physically until cleanup.
- Correctness of expiry depends on reasonably synchronized clocks between
  processes, except on PostgreSQL, which uses the server clock.

### Follow-ups
- None.

## Links

- `store.go` (`Entry`), `cache.go` (`NoExpiry`), `backend/memcached/store.go`
