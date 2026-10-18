# ADR 0004: Error semantics: sentinels, wrapping, joining and fail-open reads

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (FR-CACHE-08, FR-CACHE-22, NFR-CACHE-04) |

## Context

Recorded retrospectively for the first public release. A cache sits between
an application and both a source of truth and one or more stores, so it sees
many partial failures: one layer of several failing, a write succeeding in
the shared tier but not locally, a lease release failing after a successful
fill. The code handles them consistently:

- configuration and contract violations are package-level sentinel errors
  (`ErrEmptyNamespace`, `ErrInvalidTTL`, `ErrNilStore`, `ErrBatchLength`,
  `ErrLockNotAcquired`, `ErrLeaseLost`, `ErrFillLeaseLost`, `ErrLoaderPanic`,
  backend `ErrNilClient`/`ErrNilCodec`, …), returned directly or joined;
- operation failures are wrapped with `%w` and a short operation prefix
  (`cache set: …`, `cache layer 1 get: …`, `cache redis get: …`);
- operations that act on several independent targets (layered writes to
  faster layers, invalidation of every layer, lease loss plus release
  failure) attempt every target and combine failures with `errors.Join`;
- layered reads fail open: a failing layer is reported to an optional
  `Observer` as an `Event` that carries operation, namespace, layer index and
  error but never the key or value, and lookup continues below;
- nil and typed-nil dependencies are rejected at construction.

## Decision drivers

- D1 Callers must be able to branch on causes with `errors.Is`/`errors.As`.
- D2 A cache failure should degrade to a slower read, not an outage, unless
  the caller opts into strictness.
- D3 Errors and events must not leak cache keys or values into logs.
- D4 A library must not log on its own.

## Considered options

1. **Sentinels plus `%w` wrapping, `errors.Join` for multi-target
   operations, fail-open reads with an observer hook** (chosen)
   - Pro: every cause stays inspectable through wrapping and joining.
   - Pro: fail-open layered reads keep serving from lower layers; strictness
     is one option away (`WithStrictLayerReads`, `WithStrictPullThroughReads`).
   - Con: joined errors are harder to read than a single message.
2. **Typed error structs per operation**
   - Pro: structured fields for callers.
   - Con: more API surface; most callers only need `errors.Is`.
3. **Stop at the first failure everywhere**
   - Pro: simple.
   - Con: invalidating only some layers leaves stale data in the others;
     a failed release would mask a successful fill.
4. **Log failures internally**
   - Con: forces a logging dependency and policy on users and risks leaking
     keys.

## Decision

We will expose sentinel errors as part of the API, wrap underlying errors
with `%w`, attempt every target of multi-target operations and join their
errors, keep layered and pull-through reads fail-open by default with strict
options, report skipped failures only through the optional `Observer` (whose
events contain no keys or values), treat nil and typed-nil optional
observers as absent, and never log.

## Consequences

### Positive
- Callers can distinguish contention, lease loss, invalid configuration and
  backend failures.
- A broken fast tier does not take reads down.

### Negative
- Fail-open reads can hide a broken layer unless an `Observer` is installed.
- Sentinel errors are API: renaming or removing one is a breaking change.

### Follow-ups
- None.

## Links

- `cache.go`, `layered.go`, `pullthrough.go`, `locker.go`, `nil.go`
