# ADR 0009: Best-effort fill leases and evict-only, at-least-once invalidation

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (FR-CACHE-11, FR-CACHE-12, FR-CACHE-14, FR-CACHE-15, FR-CACHE-16, FR-CACHE-20), ADR-0005 |

## Context

Recorded retrospectively for the first public release. Two distributed
features cross process boundaries:

- **Fill leases.** `Locker`/`Lease` (and `ObservableLease`) let one process
  at a time fill a key. Backend lockers use random tokens with
  compare-and-act operations (Lua scripts on Redis-protocol servers, row
  predicates on PostgreSQL, add/CAS on Memcached) so that refresh and release
  never affect a successor; Memcached release replaces the token with a
  short tombstone instead of deleting. `CoordinationLocker` adapts any
  go-distributed-coordination backend. Lease keys come from a
  caller-provided `FillKeyFunc` that must be injective over
  `(namespace, key)`.
- **Invalidation.** `InvalidationBus` carries `InvalidationEvent`s (key- or
  namespace-scoped). `LayeredStore` publishes after mutations, and
  `ListenInvalidations` applies events only by evicting the layers above
  the shared layer. `Generation` is transport metadata, not an ordering.
  `MessagingInvalidationBus` maps handler success and failure to
  ack/nack for at-least-once transports.

## Decision drivers

- D1 Cached data is disposable; the source of truth remains authoritative.
- D2 Distributed primitives available in the supported backends do not
  provide fencing or ordered, exactly-once delivery.
- D3 A stale or duplicate signal must never make data less correct.

## Considered options

1. **Leases reduce duplicate fills; invalidation only evicts** (chosen)
   - Pro: implementable on every backend; duplicates and reordering are
     harmless because eviction is idempotent and the next read reloads.
   - Con: no strong coherence: a reader may briefly see stale data until the
     event arrives or the entry expires.
2. **Fenced writes with monotonically increasing tokens**
   - Pro: prevents a fill that lost its lease from writing.
   - Con: requires every store to check fencing tokens; the `Cache` interface
     cannot enforce it for arbitrary implementations.
3. **Invalidation events carry values (update-in-place)**
   - Pro: fewer reloads.
   - Con: out-of-order delivery could install older values; needs ordering
     the transports do not provide.

## Decision

We will treat fill leases as a best-effort mechanism that reduces duplicate
loads, cancel and skip the write on observed loss, and document that leases
are not fencing. Backend lockers will be token-owned and never touch a
successor's lease; a backend that cannot implement that safely (Hazelcast)
offers no locker. Invalidation will be at-least-once and evict-only, and
listeners will never touch the shared layer that is the event's source.

## Consequences

### Positive
- Safe under duplicate, delayed and reordered delivery.
- Works with any coordination or messaging backend.

### Negative
- A write already committed by a store that ignores cancellation cannot be
  undone after lease loss.
- Readers may observe stale values within the delivery delay or TTL.

### Follow-ups
- None.

## Links

- `locker.go`, `coordination.go`, `invalidation.go`, `layered.go`,
  `messaging.go`, `backend/*/locker.go`
