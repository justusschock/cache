# ADR 0005: Concurrency and cancellation model

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (FR-CACHE-09, FR-CACHE-11, NFR-CACHE-01, NFR-CACHE-05) |

## Context

Recorded retrospectively for the first public release. Caches are shared by many goroutines and are
expected to protect the source of truth from stampedes. The code implements:

- every blocking or I/O operation takes `ctx context.Context` first and
  checks it before doing work;
- every exported type is safe for concurrent use (mutex-protected memory
  structures, immutable configuration after construction);
- `PullThroughCache` shares one fill per key through an internal flight
  group built on `concurrency` futures. The fill runs on a context derived
  with `context.WithoutCancel` from the first caller and canceled only when
  the last waiter leaves; each waiter waits with its own context; a
  completed or abandoned flight is removed so later requests start fresh;
- a lease-guarded fill runs a guard goroutine that refreshes the lease every
  half TTL and watches `ObservableLease.Done`; loss or refresh failure
  cancels the loader, the cache write is skipped if loss is observed first,
  and the lease is always released on a context detached from the caller and
  bounded by the lease TTL;
- invalidation listeners (`Subscribe`, `ListenInvalidations`) block until
  their context ends and return its error.

## Decision drivers

- D1 One caller's cancellation must not cancel work other callers wait for.
- D2 Abandoned work must stop and must not leak goroutines.
- D3 A fill must not write after it lost its lease, as far as the `Cache`
  interface allows.
- D4 Cleanup (lease release) must run even when the caller has gone away.

## Considered options

1. **Waiter-counted flights with a detached fill context** (chosen)
   - Pro: satisfies D1 and D2: the fill outlives individual waiters but not
     all of them.
   - Con: more machinery than a plain single-flight.
2. **`golang.org/x/sync/singleflight`**
   - Pro: well known.
   - Con: untyped results; the fill runs on the first caller's context, so
     that caller's cancellation fails every waiter; no hook to cancel a fill
     nobody waits for.
3. **Fill on `context.Background()`**
   - Pro: never canceled by callers.
   - Con: abandoned fills keep loading forever (violates D2).

For leases, options considered were a renewal goroutine at TTL/2 with loss
observation (chosen), renewing only before the write (misses loss during long
loads), and no renewal (requires TTLs longer than any load).

## Decision

We will take contexts first on every blocking operation, keep all exported
types safe for concurrent use, share fills through waiter-counted flights
with detached-but-cancelable fill contexts, guard leased fills with a
half-TTL renewal goroutine that cancels the loader on loss, and release
leases on a detached context bounded by the lease TTL.

## Consequences

### Positive
- No stampedes; canceled callers return promptly; abandoned fills stop.
- Lease loss is detected during long loads, not only at the end.

### Negative
- Loaders and cache implementations must honour context cancellation for
  the lease guarantees to hold; a write already committed cannot be undone
  (see ADR-0009).
- The per-key flight map holds an entry for the duration of each fill.

### Follow-ups
- None.

## Links

- `flight.go`, `pullthrough.go`, ADR-0009
