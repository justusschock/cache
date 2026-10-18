# ADR 0010: Public API boundary

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0006, ADR-0007 |

## Context

Recorded retrospectively for the first public release. Before release, every
exported identifier was reviewed and classified as either part of the
contract users need or an implementation detail. Known consumers use the
root package (`Cache`, `Entry`, `New`, `NewPullThrough`, `StandardCache`) and
`backend/disk` (`Open`, `WithOrderedKeys`, `WithValueCodec`, `GetMany`,
`SetMany`, `DeletePrefix`, `Close`); all of that stays public.

## Decision drivers

- D1 Users and implementers need exactly the contracts, constructors,
  options, errors and conformance suites; nothing else should be frozen by
  semver.
- D2 No identifier used by a known consumer may move or disappear.

## Considered options

1. **Keep the existing surface unchanged**
   - Pro: no churn.
   - Con: the conformance suites are unreachable for third-party
     implementers.
2. **Minimal boundary changes driven by the review** (chosen)
   - Pro: publishes the conformance suites without removing or renaming any
     exported identifier.
   - Con: `cachetest` becomes public API.
3. **Also unexport `memory.(*InvalidationBus).SubscriberCount`**
   - Pro: the repository's own tests no longer need it to wait for a
     subscription, because they use `synctest.Wait` (ADR-0012).
   - Con: it is the only way for users of the in-process bus to observe
     whether a listener is subscribed or has been released; removing it
     breaks any caller for no safety gain.

## Decision

The public API is:

| Package | Public contents |
|---|---|
| `github.com/justusschock/cache` | `Store`, `BatchStore`, `Entry`, `Cache`, `EntryCache`, `BatchCache`, `StandardCache`/`New`, `NoExpiry`, `Codec`/`DefaultCodec`, `LayeredStore`/`NewLayered`/`NewLayeredWithInvalidationBus` with `LayeredOption`s, `Observer`/`Event`, `PullThroughCache`/`NewPullThrough` with `PullThroughOption`s and `Loader`, `Locker`/`Lease`/`ObservableLease`, `CoordinationLocker`/`FillKeyFunc`, `InvalidationBus`/`InvalidationEvent`/`InvalidationKind`/`InvalidationHandler`, `MessagingInvalidationBus`/`SubscriptionFactory`, and all sentinel errors |
| `backend/memory` | `Store` (`New`, `NewWithOptions`, `Options`, `Len`, `Close`, batch methods), `Locker`, `InvalidationBus` (`NewInvalidationBus`, `SubscriberCount`), `ErrInvalidOptions` |
| `backend/disk` | `Store` (`Open`, options, batch methods, `ScanPrefix`, `DeletePrefix`, `Close`) and its errors |
| `backend/redis`, `valkey`, `dragonfly`, `memcached`, `hazelcast`, `postgres` | `Store`/`New`, `Locker`/`NewLocker` and `InvalidationBus`/`NewInvalidationBus` where offered, codec options, errors, and PostgreSQL's `Migrate`/`DeleteExpired` |
| `cachetest` | `RunStoreContract`, `RunBatchStoreContract`, `RunInvalidationBusContract`, `StoreFactory`, `BatchStoreFactory` |

Internal (not part of the API):

- `internal/wire`: the entry envelope and invalidation message codecs. They
  are persistence formats governed by ADR-0007; exposing the encoder would
  freeze its Go API in addition to the byte format.
- Unexported helpers: the flight group, lease guard, nil checks, option
  config structs.

Changes made for the release:

- `internal/cachetest` moved to the public `cachetest` package, because
  `Store`, `BatchStore` and `InvalidationBus` are public interfaces meant for
  third-party implementations (ADR-0006). Its goroutine helper `Go`, used
  only by this repository's tests, was unexported.
- `memory.(*InvalidationBus).SubscriberCount` stays exported (option 3 was
  rejected). It reports the active subscriptions of a namespace, which lets
  users of the in-process bus wait for a listener or check that one was
  released; the repository's tests use it alongside `synctest.Wait`.

Exported option function types with unexported parameter types (for example
`Option[K, V] func(*config[K, V]) error`) are kept: they let callers name and
pass options while keeping the configuration structs private.

## Consequences

### Positive
- Implementers can verify their backends with the official suite.
- The surface frozen by semver is the documented one.

### Negative
- `cachetest` and its testify dependency are now public API.
- `SubscriberCount` remains covered by semver although only the in-process
  bus offers it.

### Follow-ups
- Any future change to this boundary needs a new ADR and a changelog entry.

## Links

- ADR-0006, ADR-0007, ADR-0012, `CHANGELOG.md`
