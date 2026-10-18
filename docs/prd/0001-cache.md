# PRD 0001: cache

| Field | Value |
|---|---|
| Status | Accepted |
| Implementation | Complete |
| Owners | Justus Perillieux |
| Related ADRs | ADR-0002, ADR-0003, ADR-0004, ADR-0005, ADR-0006, ADR-0007, ADR-0008, ADR-0009, ADR-0010, ADR-0011, ADR-0012 |
| Last updated | 2026-10-18 |

Recorded retrospectively for the first public release; describes the library
as implemented.

## 1. Problem and context

Go services repeatedly build the same caching machinery: a typed wrapper over
some key-value store, TTL handling, a local tier in front of a shared tier,
single-flight loading so that a cold key does not stampede the source of
truth, negative caching of authoritative misses, and invalidation between
processes. Hand-rolled versions tend to couple to one backend, mix value and
expiry handling, lose type safety at the store boundary, and get the hard
cases wrong: a canceled caller canceling a fill other callers need, a fill
completing after its distributed lease was lost, or an invalidation that
races a write.

cache provides these pieces as small, typed, composable parts with one
storage contract (`Store`) and interchangeable backends, so that an
application can start with an in-process cache and add a shared tier,
distributed fill leases or cross-process invalidation without changing the
code that reads and writes values.

## 2. Users and use cases

- As a service developer, I want a typed cache with a namespace and default
  TTL over any store so that I can cache values without serializing them by
  hand.
- As a service developer, I want misses to be loaded once per key with
  waiter-scoped cancellation so that a cold key does not overload the
  database and one impatient caller does not abort a load others wait for.
- As a service developer, I want authoritative misses cached for a short time
  so that repeated lookups of absent keys do not reach the database.
- As an operator of several replicas, I want a fast in-process tier in front
  of a shared tier, with invalidations published between replicas, so that
  reads stay local while writes become visible everywhere.
- As an operator of several replicas, I want an optional distributed lease
  per fill so that only one replica loads a cold key at a time.
- As a CLI or daemon developer, I want a persistent on-disk cache with
  entries that never expire, atomic batches and ordered prefix scans so that
  I can keep derived state (for example file hashes) between runs.
- As an implementer of a new backend, I want a public conformance suite so
  that I can verify my store or invalidation bus against the same contract
  the built-in backends meet.

## 3. Goals and non-goals

### Goals

- G1 One generic storage contract (`Store[K, V]`) with optional batch
  operations, implemented by in-process, on-disk and remote backends.
- G2 Composition: namespaced TTL policy (`StandardCache`), layering
  (`LayeredStore`) and pull-through loading (`PullThroughCache`) work over any
  store or cache.
- G3 Correct concurrency and cancellation semantics for fills, leases and
  invalidation listeners.
- G4 Transport-neutral distributed features: fill leases through any
  `Locker`, invalidation through any `InvalidationBus`, with adapters for
  distributed-coordination and messaging.
- G5 Stable, versioned persistence formats for serialized backends.
- G6 A public conformance suite for third-party implementations.

### Non-goals

- NG1 A correctness-critical database, write-through persistence or
  transactions across keys. Cached data is reconstructible and disposable.
- NG2 Strong cache coherence, exactly-once invalidation or total ordering of
  events; these need a consensus system, not a cache.
- NG3 Fenced writes after a distributed lease changes ownership; the `Cache`
  interface cannot carry fencing tokens to arbitrary stores.
- NG4 Connection management, service discovery, credentials or
  infrastructure provisioning; clients and pools are caller-owned.
- NG5 Prescribed metrics, tracing or logging; the library reports through
  return values and an optional `Observer`.

## 4. Functional requirements

| ID | Requirement | Priority (MUST/SHOULD/MAY) |
|---|---|---|
| FR-CACHE-01 | `New` constructs a `StandardCache` from a non-empty namespace, a positive default TTL and a non-nil store; `Get`, `Set`, `SetWithTTL`, `Invalidate` and `InvalidateAll` operate within that namespace, and `Set` stamps entries with the default TTL. | MUST |
| FR-CACHE-02 | `Get` distinguishes a cached zero value from a miss and never returns expired or negative entries as found. | MUST |
| FR-CACHE-03 | `NoExpiry` is the only accepted non-positive TTL; it yields entries with a zero `ExpiresAt` that never expire logically and are written without a backend-native TTL. Every other non-positive TTL is rejected with `ErrInvalidTTL`. | MUST |
| FR-CACHE-04 | `EntryCache` exposes stored entries (`GetEntry`, `SetEntry`); `SetEntry` rejects already expired entries and accepts a zero `ExpiresAt`. | MUST |
| FR-CACHE-05 | `BatchCache`/`BatchStore` provide `GetMany`, `SetMany`, `SetManyWithTTL` and `InvalidateMany` with results aligned to keys, `ErrBatchLength` for mismatched slices, later duplicates winning, and a per-key fallback when the store does not implement `BatchStore`. | MUST |
| FR-CACHE-06 | Every `Store` hides logically expired records (including an expired overwrite of a live entry), stores negative entries, isolates namespaces, makes missing-key invalidation idempotent, implements namespace invalidation and honours canceled contexts; the public `cachetest` suite verifies this. | MUST |
| FR-CACHE-07 | `LayeredStore` reads layers fastest to slowest, promotes a lower-layer hit into every missed faster layer without changing its expiration, writes slowest to fastest (stopping on a slowest-layer failure, joining faster-layer failures), and invalidates every layer, joining errors. | MUST |
| FR-CACHE-08 | Layered reads fail open by default, reporting skipped layer errors to an optional `Observer` without keys or values; `WithStrictLayerReads` makes the first read error terminal. | MUST |
| FR-CACHE-09 | `PullThroughCache` loads misses through a `Loader` with one shared load per key; each caller waits with its own context, the load continues while any waiter remains, is canceled when the last waiter leaves, and a later request starts a fresh load. | MUST |
| FR-CACHE-10 | Authoritative misses are stored as negative entries for the negative TTL when the wrapped cache is an `EntryCache`; loader errors are never cached; loader panics are returned as `ErrLoaderPanic`. | MUST |
| FR-CACHE-11 | With `WithLocker`, a fill holds a renewable lease: contention is polled at the configured interval while re-checking the cache, the lease is refreshed every half TTL, observable loss or refresh failure cancels the loader and prevents the cache write (`ErrFillLeaseLost`), and the lease is released with a non-canceled context. Lock errors fail the load unless `WithLoadOnLockError` is set. | MUST |
| FR-CACHE-12 | `CoordinationLocker` adapts any distributed-coordination `Backend` into a `Locker` with observable leases, mapping contention to `ErrLockNotAcquired` and ownership loss to `ErrLeaseLost`. | MUST |
| FR-CACHE-13 | `DefaultCodec` encodes strings, byte slices, booleans and numeric scalars directly and other values as JSON, and rejects decoding into interface types with `ErrUntypedValue`; custom codecs can be supplied to every serialized backend. | MUST |
| FR-CACHE-14 | A `LayeredStore` with an invalidation bus publishes key-scoped events for `Set` and `Invalidate` and namespace-scoped events for `InvalidateAll`; `ListenInvalidations` applies remote events only to layers above the shared layer, so duplicate and out-of-order delivery is safe. | MUST |
| FR-CACHE-15 | Every `InvalidationBus` delivers published events to subscribers of the namespace across instances, rejects a nil handler with `ErrNilInvalidationHandler`, removes the subscription and returns the context error when its context ends. Implementations exist for memory, Redis, Valkey, Dragonfly, PostgreSQL and Hazelcast; the memory bus also reports a namespace's active subscriptions through `SubscriberCount`. | MUST |
| FR-CACHE-16 | `MessagingInvalidationBus` adapts typed messaging publishers and subscribers, publishing to the subject returned by the subscription factory, acknowledging handled and foreign-namespace deliveries and negatively acknowledging handler failures. | MUST |
| FR-CACHE-17 | The memory backend is bounded by a global LRU capacity, sweeps expired entries periodically, keeps no-expiry entries across sweeps, does not retain metadata for empty namespaces, validates its options and has an idempotent `Close`. | MUST |
| FR-CACHE-18 | The disk backend persists entries in an owner-only bbolt file across reopen, applies each batch in one transaction, and with `WithOrderedKeys` supports `ScanPrefix` and `DeletePrefix` in encoded-key order, returning `ErrOrderedKeysRequired` otherwise. | MUST |
| FR-CACHE-19 | Remote backends (Redis, Valkey, Dragonfly, Memcached, Hazelcast, PostgreSQL) implement `Store` over caller-owned clients, with logical expiry enforced from the envelope and native TTLs only as physical cleanup; Redis, Valkey and Dragonfly implement `BatchStore` with pipelines; PostgreSQL provides `Migrate` and `DeleteExpired` and uses the server clock. | MUST |
| FR-CACHE-20 | Backend lockers (memory, Redis, Valkey, Dragonfly, Memcached, PostgreSQL) issue token-owned leases whose refresh and release never affect a successor's lease and report expiry or loss as `ErrLeaseLost`. | MUST |
| FR-CACHE-21 | Serialized backends store entries in a versioned envelope (version 2 written, version 1 still decoded) and send invalidations in a versioned binary message; malformed input is rejected. | MUST |
| FR-CACHE-22 | Constructors and options reject nil and typed-nil dependencies and invalid configuration with sentinel errors; operation errors wrap the underlying cause with `%w` so that `errors.Is` and `errors.As` work. | MUST |

## 5. Non-functional requirements

| ID | Requirement | Measure |
|---|---|---|
| NFR-CACHE-01 | Every exported type is safe for concurrent use unless documented otherwise. | `go test -race` passes, including the concurrency tests named below. |
| NFR-CACHE-02 | Tests are deterministic; time and goroutine coordination use `testing/synctest`. | `go test -race -count=3 ./...` passes; no wall-clock sleeps outside the conformance suite and integration tests. |
| NFR-CACHE-03 | Supported toolchains: Go 1.25 and the two most recent Go releases. | CI matrix `1.25.x`, `oldstable`, `stable` (ADR-0012). |
| NFR-CACHE-04 | No logging; failures are reported by return values and the optional `Observer`, which never receives keys or values. | Code review; `Event` has no key or value fields. |
| NFR-CACHE-05 | Every blocking or I/O operation takes a `context.Context` first and honours cancellation. | `cachetest` canceled-context cases; pull-through cancellation tests. |
| NFR-CACHE-06 | Applications compile only the backend clients they import. | All backends are sub-packages of one module; no backend is imported by the root package. |
| NFR-CACHE-07 | Shared formats are stable: envelope and invalidation formats change only reader-first, with a new version. | Wire tests for version 1 decode and malformed input (ADR-0007). |
| NFR-CACHE-08 | Persistent state is not world-readable, and remote keys do not expose cache keys. | Disk files are created `0600`; remote backends store SHA-256 digests of namespaces and keys. |

## 6. API sketch

```go
// Storage contract implemented by backends.
type Store[K comparable, V any] interface {
	Get(context.Context, string, K) (Entry[V], bool, error)
	Set(context.Context, string, K, Entry[V]) error
	Invalidate(context.Context, string, K) error
	InvalidateAll(context.Context, string) error
}

// Namespaced TTL policy over a store.
func New[K comparable, V any](namespace string, defaultTTL time.Duration, store Store[K, V]) (*StandardCache[K, V], error)

// Composition.
func NewLayered[K comparable, V any](layers []Store[K, V], options ...LayeredOption) (*LayeredStore[K, V], error)
func NewLayeredWithInvalidationBus[K comparable, V any](layers []Store[K, V], bus InvalidationBus[K], options ...LayeredOption) (*LayeredStore[K, V], error)
func NewPullThrough[K comparable, V any](namespace string, wrapped Cache[K, V], loader Loader[K, V], options ...PullThroughOption[K]) (*PullThroughCache[K, V], error)

// Distributed coordination.
type Locker[K comparable] interface {
	Lock(context.Context, string, K, time.Duration) (Lease, error)
}
type InvalidationBus[K comparable] interface {
	Publish(context.Context, InvalidationEvent[K]) error
	Subscribe(context.Context, string, InvalidationHandler[K]) error
}

// Conformance (package cachetest).
func RunStoreContract(t *testing.T, factory StoreFactory)
func RunBatchStoreContract(t *testing.T, factory BatchStoreFactory)
func RunInvalidationBusContract[K comparable](t *testing.T, newBus func(*testing.T) cache.InvalidationBus[K], event cache.InvalidationEvent[K])
```

## 7. Acceptance criteria

Integration tests (files ending in `_integration_test.go`, build tag
`integration`) run in CI against service containers.

| Criterion | Requirement(s) | Test(s) |
|---|---|---|
| Construction validates namespace, TTL and store; Set uses the default TTL and entries expire exactly at it | FR-CACHE-01 | `TestNewRejectsInvalidConfiguration`, `TestCacheSetUsesDefaultTTLAndGetReturnsValue`, `TestCacheSetWithTTLOverridesDefault`, `TestExistingConstructorsRemainCallable` |
| Zero values, misses, expired and negative entries are distinguished | FR-CACHE-02 | `TestCacheDistinguishesCachedZeroAndMiss`, `TestCacheDoesNotExposeExpiredOrNegativeEntries` |
| NoExpiry is accepted, other non-positive TTLs rejected, no-expiry entries survive | FR-CACHE-03 | `TestNewAcceptsNoExpiryButRejectsOtherNonPositiveTTLs`, `TestNoExpiryEntriesNeverExpire`, `TestSetWithTTLAcceptsNoExpiryOnExpiringCache`, `TestEntryExpiration`, `TestEntryExpirationUsesZeroForNoExpiry`, `TestNoExpiryEntriesHaveNoNativeTTL` |
| Entry access round-trips and rejects expired entries | FR-CACHE-04 | `TestSetEntryAcceptsNoExpiryAndRejectsExpiredEntries` |
| Batch operations align, validate and fall back | FR-CACHE-05 | `TestBatchOperationsUseBatchStore`, `TestBatchOperationsFallBackToStoreLoop`, `TestBatchOperationsValidateArguments`, `TestGetManyRejectsMisalignedBatchStoreResults`, `TestBatchCacheWithMemoryStore`, `TestBatchStoreContract` |
| Store contract holds for every backend | FR-CACHE-06 | `TestStoreContract`, `TestOrderedStoreContract`, `TestStoreIntegrationContract` |
| Layered reads promote; writes and invalidations follow the layer order | FR-CACHE-07 | `TestLayeredGetPromotesLowerHitWithoutChangingExpiration`, `TestLayeredSetWritesSlowestToFastest`, `TestLayeredSetStopsWhenSlowestLayerFails`, `TestLayeredSetJoinsUpperLayerErrors`, `TestLayeredInvalidationAttemptsEveryLayerAndJoinsErrors`, `TestLayeredInvalidateAllAttemptsEveryLayer`, `TestNewLayeredRejectsMissingOrNilLayers` |
| Layered reads fail open with an observer or strictly | FR-CACHE-08 | `TestLayeredReadCanFailOpenOrStrict`, `TestLayeredTreatsTypedNilOptionalObserverAsAbsent` |
| Single flight with waiter-scoped cancellation | FR-CACHE-09 | `TestPullThroughLoadsAndCachesMiss`, `TestPullThroughSuppressesConcurrentLoads`, `TestFlightSharesTypedResultAndLetsWaiterCancel`, `TestFlightCancelsFillOnlyAfterLastWaiterDeparts`, `TestFlightCompletionAndDepartureAlwaysStartLaterLifecycleFresh`, `TestCanceledFlightWaitersDoNotCancelOrDuplicateOwningLoad` |
| Negative caching, uncached loader errors | FR-CACHE-10 | `TestPullThroughCachesAuthoritativeMiss`, `TestPullThroughNegativeEntriesExpireAfterNegativeTTL`, `TestPullThroughDoesNotCacheLoaderErrors`, `TestPullThroughCustomCacheDoesNotRetainLocalNegativeEntries`, `TestFlightPreservesLoaderError`, `TestFlightOwnerErrorRemainsRetryableAfterCanceledWaiters` |
| Lease-guarded fills renew, poll, stop on loss and release | FR-CACHE-11 | `TestPullThroughRenewsAndReleasesDistributedLease`, `TestPullThroughPollsContendedLockUntilAnotherFillLands`, `TestPullThroughContendedLockHonoursContextCancellation`, `TestPullThroughDoesNotPublishFillAfterLeaseLoss`, `TestPullThroughDoesNotPublishFillAfterRenewalFailure`, `TestPullThroughLockerFailurePolicy`, `TestPullThroughRejectsNilSuccessfulLease`, `TestPullThroughPostAcquireHitPropagatesReleaseError`, `TestPullThroughPostAcquireStrictLookupErrorStillReleases` |
| Coordination adapter maps leases and errors | FR-CACHE-12 | `TestCoordinationLockerMapsLeaseOperations`, `TestCoordinationLockerMapsContention`, `TestCoordinationLockerWorksWithMemoryBackend`, `TestCoordinationLockerPreservesKeyErrors`, `TestNewCoordinationLockerRejectsInvalidConfiguration` |
| Default codec encodings | FR-CACHE-13 | `TestDefaultCodecRoundTripsTypedValues`, `TestDefaultCodecUsesDirectRepresentations`, `TestDefaultCodecRoundTripsPointersAndNil`, `TestDefaultCodecRejectsUntypedDecode`, `TestCodecInterfaceSupportsCustomImplementations` |
| Layered invalidation publishes exact scopes and evicts only upper layers | FR-CACHE-14 | `TestLayeredPublishesExactInvalidationScopes`, `TestListenerTreatsDuplicateAndOutOfOrderEventsAsEvictions`, `TestLayeredInvalidationBusEvictsOtherWorkersUpperLayers`, `TestLayeredInvalidationBusPropagatesNamespaceInvalidation` |
| Invalidation buses meet the bus contract | FR-CACHE-15 | `TestMemoryInvalidationBusContract`, `TestInvalidationBusRemovesSubscriberWhenContextEnds`, `TestSubscribeRejectsNilHandler`, `TestInvalidationBusIntegration`, `TestInvalidationBusFiltersMismatchedNamespacePayloads` |
| Messaging adapter acknowledges correctly | FR-CACHE-16 | `TestMessagingBusPublishesTypedEnvelope`, `TestMessagingBusPublishesToFactorySubject`, `TestMessagingBusAcknowledgesHandledEvent`, `TestMessagingBusNacksHandlerError`, `TestMessagingBusAcknowledgesMismatchedNamespaceWithoutHandling`, `TestMessagingBusRejectsNilHandler`, `TestMessagingBusRoundTripsThroughMemoryBroker` |
| Memory backend bounds and sweeps | FR-CACHE-17 | `TestStoreEvictsLeastRecentlyUsedEntryAtCapacity`, `TestSetManyRespectsCapacity`, `TestStoreSweepsExpiredEntriesWithoutReads`, `TestStoreKeepsNoExpiryEntriesAcrossSweeps`, `TestCapacityBoundsNamespaceMetadata`, `TestInvalidateAllDoesNotRetainEmptyNamespace`, `TestNewWithOptionsRejectsInvalidConfiguration`, `TestStoreCloseIsIdempotent` |
| Disk backend persists and scans | FR-CACHE-18 | `TestStorePersistsAcrossReopen`, `TestStoreFileIsOwnerOnly`, `TestStoreExpiresEntriesButKeepsNoExpiryEntries`, `TestStoreNoExpiryPersistsAcrossReopen`, `TestOrderedKeysUseSeparateLayout`, `TestScanPrefixVisitsLiveEntriesInKeyOrder`, `TestScanPrefixStopsWhenCallbackReturnsFalse`, `TestScanPrefixSkipsEntriesBeforeInvalidateAll`, `TestDeletePrefixRemovesMatchingRecords`, `TestPrefixOperationsRequireOrderedKeys`, `TestPrefixOperationsValidateArguments`, `TestScanPrefixWithCustomKeyCodec` |
| Remote backends enforce expiry correctly | FR-CACHE-19 | `TestStoreIntegrationContract`, `TestExpirationRoundsSubsecondTTLUp`, `TestExpirationUsesAbsoluteTimeBeyondThirtyDays`, `TestExpirationReliesOnEnvelopePastMemcachedUnixRange`, `TestStorePreservesSubsecondExpiry`, `TestStorePreservesExpiryPast2038`, `TestSetRechecksExpiryWithServerClockAfterLockWait`, `TestDeleteExpiredRemovesHighCardinalityLockRows` |
| Lockers never affect a successor | FR-CACHE-20 | `TestLockerAcquireRefreshAndRelease`, `TestLockerExpiredLeaseCanBeReplaced`, `TestLockerReleaseRejectsNaturalExpiry`, `TestLockerAllowsOnlyOneConcurrentHolder`, `TestExpiredLocksAreRemovedDuringChurn`, `TestLeaseReleaseCannotDeleteSuccessorAfterTombstoneCAS`, `TestLockerRejectsExpiryOutsideMemcachedUnixRange`, `TestLeaseRefreshRejectsExpiryOutsideMemcachedUnixRange`, `TestLockerIntegration` |
| Wire formats round-trip and stay compatible | FR-CACHE-21 | `TestEntryRoundTripsExpiryOutsideUnixNanoRange`, `TestDecodeAcceptsVersionOneEntry`, `TestDecodeRejectsMalformedVersionTwoNanoseconds`, `TestInvalidationRoundTrip`, `TestNamespaceInvalidationRoundTrip`, `TestInvalidationRejectsMalformedPayload` |
| Invalid and nil dependencies are rejected; errors wrap | FR-CACHE-22 | `TestConstructorsRejectTypedNilDependencies`, `TestNewPullThroughRejectsInvalidConfiguration`, `TestNewMessagingInvalidationBusRejectsNilDependencies`, `TestOpenRejectsEmptyPathAndNilCodecs`, `TestNewRejectsNilClientAndCodecs`, `TestCachePropagatesStoreErrors`, `TestBatchOperationsWrapStoreErrors` |
| Concurrent use is race-free | NFR-CACHE-01 | `TestStoreIsSafeForConcurrentAccess`, `TestLockerAllowsOnlyOneConcurrentHolder`, `TestPullThroughSuppressesConcurrentLoads` |

## 8. Source references

None.

## 9. Open questions

- Q1 Should the conformance suites accept a clock or synctest hook so that
  in-process implementations can run the logical-expiration case on virtual
  time?
- Q2 Should `BatchStore` be implemented for PostgreSQL, Memcached and
  Hazelcast, which currently use the per-key fallback?
