# ADR 0002: Generic store contract with composable caches and functional options

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0006 |

## Context

Recorded retrospectively for the first public release. The library has to
serve very different storage technologies (an in-process map, a bbolt file,
Redis-protocol servers, Memcached, Hazelcast, PostgreSQL) and several
cross-cutting behaviours (TTL policy, layering, pull-through loading,
invalidation). The code separates these into:

- `Store[K, V]`: a namespaced, typed persistence contract over `Entry[V]`
  (value, `Found` flag for negative entries, logical `ExpiresAt`), plus the
  optional `BatchStore[K, V]` extension discovered by type assertion;
- `Cache[K, V]`: the caller-facing value API, with optional `EntryCache` and
  `BatchCache` extensions;
- `StandardCache` (namespace and TTL policy over a store), `LayeredStore` (a
  `Store` composed of stores) and `PullThroughCache` (a `Cache` wrapping a
  `Cache`);
- constructors that validate their inputs and return `(*T, error)`, with
  functional options (`LayeredOption`, `PullThroughOption[K]`, backend
  `Option[K, V]`) for optional behaviour.

## Decision drivers

- D1 Type safety at the API boundary; no `interface{}` values in user code.
- D2 Backends must be interchangeable and composable without adapters.
- D3 Optional capabilities (batching, entry access, lease observation) must
  not burden implementations that lack them.
- D4 Invalid configuration must fail at construction, not on first use.

## Considered options

1. **Generic `Store`/`Cache` interfaces with optional extension interfaces**
   (chosen)
   - Pro: values are typed end to end; composition is just wrapping.
   - Pro: optional interfaces (`BatchStore`, `EntryCache`, `ObservableLease`)
     let capable implementations opt in, with a defined fallback otherwise.
   - Con: generic interfaces cannot be type-switched cheaply over all
     instantiations; each extension needs its own assertion.
2. **Byte-oriented store interface with serialization in the cache layer**
   - Pro: one store implementation serves every value type.
   - Con: the in-process store would serialize needlessly; negative entries
     and expiry would need an ad-hoc byte encoding at the API boundary.
3. **One large interface with every capability**
   - Pro: no type assertions.
   - Con: every backend must implement batching and entry access, even where
     it adds nothing.

For configuration, options considered were functional options (chosen),
exported config structs, and builder types. Functional options keep
constructors stable as options are added and let each option validate its
argument (`PullThroughOption` returns an error); the memory backend uses a
plain `Options` struct because both of its fields are required together.

## Decision

We will keep `Store[K, V]` and `Cache[K, V]` as small generic interfaces with
optional extension interfaces discovered by type assertion, build behaviour
by composing implementations of those interfaces, validate everything in
constructors that return `(*T, error)`, and express optional behaviour as
functional options.

## Consequences

### Positive
- Any store works under `StandardCache`, `LayeredStore` and
  `PullThroughCache`; any cache works under `PullThroughCache`.
- Adding an option or an optional capability is backwards compatible.

### Negative
- Negative caching silently degrades to "no negative caching" for a wrapped
  `Cache` that is not an `EntryCache`; this is documented.
- Option types such as `Option[K, V]` need both type parameters spelled out
  at call sites.

### Follow-ups
- None.

## Links

- `store.go`, `cache.go`, `layered.go`, `pullthrough.go`
