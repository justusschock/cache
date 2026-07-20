# go-cache

`go-cache` provides typed, composable caches for Go: bounded in-process storage, layered promotion, pull-through loading, negative entries, renewable fill leases, and transport-neutral invalidation.

The repository is private and requires Go 1.22 or newer. Cache data is assumed to be reconstructible and disposable; correctness-critical state belongs in a durable system of record.

## Installation

Configure private-module access and authenticate GitHub through your normal Git credential flow:

```bash
go env -w GOPRIVATE='github.com/justusschock/*'
go env -w GONOSUMDB='github.com/justusschock/*'
go get github.com/justusschock/go-cache@v0.2.0
```

The module depends on `go-concurrency`, `go-messaging`, and `go-distributed-coordination` at the matching `v0.2.0` compatibility line. All backend packages share this module and release, so one repository tag versions the complete library. Remote-client dependencies appear in the module graph, but Go compiles only packages imported by an application.

## Basic use

```go
package main

import (
	"context"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/backend/memory"
)

type User struct {
	ID   string
	Name string
}

func main() {
	store := memory.New[string, User]()
	defer store.Close()

	users, err := cache.New("users", 5*time.Minute, store)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	if err := users.Set(ctx, "user-42", User{ID: "user-42", Name: "Ada"}); err != nil {
		panic(err)
	}
	user, found, err := users.Get(ctx, "user-42")
	_, _, _ = user, found, err
}
```

`memory.New` is globally bounded to 10,000 live entries and sweeps expired entries once per minute. Use `memory.NewWithOptions` to choose both limits explicitly:

```go
store, err := memory.NewWithOptions[string, User](memory.Options{
	MaxEntries:     50_000,
	CleanupInterval: 15 * time.Second,
})
if err != nil {
	return err
}
defer store.Close()
```

The store uses a global least-recently-used order across namespaces. Reads refresh recency, expiration and invalidation remove entries from the order, and `Close` is idempotent. Every memory store must be closed: construction starts an expiration-sweeper goroutine, and omitting `Close` leaves that goroutine running for the lifetime of the process.

## Layered caches

Stores are ordered from fastest to slowest. A lower-layer hit is promoted into every missed upper layer without changing its expiration. Writes start at the slowest layer, then proceed toward the fastest layer.

```go
fast := memory.New[string, User]()
defer fast.Close()
shared := memory.New[string, User]()
defer shared.Close()

layers, err := cache.NewLayered([]cache.Store[string, User]{fast, shared})
if err != nil {
	return err
}
users, err := cache.New("users", 5*time.Minute, layers)
```

Reads fail open by default: a failed upper layer is reported through an optional `Observer`, and lookup continues below it. `cache.WithStrictLayerReads()` makes the first layer error terminal. A slowest-layer write failure stops the write; errors from upper-layer writes are joined.

## Pull-through loading

`NewPullThrough` shares one typed future per key. Each caller waits with its own context, so canceling a waiter does not cancel a fill needed by other callers. When the last waiter departs, the fill context is canceled and the flight is removed; a later request starts a fresh fill lifecycle even if the abandoned loader has not returned yet. Loader errors are never cached.

```go
store := memory.New[string, User]()
defer store.Close()
users, err := cache.New("users", 5*time.Minute, store)
if err != nil {
	return err
}

loaded, err := cache.NewPullThrough(
	"users",
	users,
	func(ctx context.Context, id string) (User, bool, error) {
		return database.FindUser(ctx, id)
	},
	cache.WithNegativeTTL[string](30*time.Second),
)
if err != nil {
	return err
}
```

Authoritative misses are stored as `Entry.Found == false` only when the wrapped cache implements `EntryCache`; `StandardCache` does. A custom implementation of only `Cache` remains supported, but misses are loaded again rather than retained in hidden process-local state.

For cross-process fill coordination, adapt any `go-distributed-coordination` backend:

```go
coordinationBackend := coordinationmemory.NewBackend(nil)
locker, err := cache.NewCoordinationLocker(
	coordinationBackend,
	"worker-7",
	func(namespace, key string) (string, error) {
		encoded, err := json.Marshal([2]string{namespace, key})
		return string(encoded), err
	},
)
if err != nil {
	return err
}

loaded, err := cache.NewPullThrough(
	"users",
	users,
	loadUser,
	cache.WithLocker[string](locker, 10*time.Second),
)
```

`FillKeyFunc` is part of the coordination schema. Its output must be deterministic and injective over the complete `(namespace, key)` tuple. Delimiter concatenation is unsafe when either input may contain the delimiter because different tuples can map to the same lease. Keep the derivation stable across deployments and align its key semantics with the cache key codec.

Observable lease loss or renewal failure cancels the loader, waits for it to return, and releases the lease with a non-canceled context. Loss detected before storage prevents the cache write from starting. If ownership changes while `Cache.Set` is already running, the write receives a canceled context and `Get` returns `ErrFillLeaseLost`, but the `Cache` interface cannot fence an implementation that has already committed or ignores cancellation. Loaders and cache implementations used with leases must honor context cancellation. These leases reduce duplicate fills; they are not a substitute for fenced writes in a correctness-critical data store.

## Distributed invalidation

`LayeredStore` can publish typed invalidations through any `InvalidationBus`. `Set` and `Invalidate` publish a key-scoped event; `InvalidateAll` publishes a namespace-scoped event. Listeners only evict upper layers, making duplicate and out-of-order delivery safe. `InvalidationEvent.Generation` is transport metadata, not a globally ordered revision.

`MessagingInvalidationBus` adapts typed `go-messaging` publishers and subscribers:

```go
broker := messagingmemory.NewBroker[cache.InvalidationEvent[string]](10 * time.Millisecond)
defer broker.Close()

bus, err := cache.NewMessagingInvalidationBus[string](
	broker,
	broker,
	func(namespace string) messaging.Subscription {
		return messaging.Subscription{
			Subject:     "cache.invalidations." + namespace,
			Consumer:    "worker-7",
			Semantics:   messaging.AtLeastOnce,
			MaxInFlight: 32,
		}
	},
)
if err != nil {
	return err
}

layers, err := cache.NewLayeredWithInvalidationBus(
	[]cache.Store[string, User]{local, shared},
	bus,
)
if err != nil {
	return err
}

listenCtx, stop := context.WithCancel(ctx)
defer stop()
go func() {
	_ = layers.ListenInvalidations(listenCtx, "users", 1)
}()
```

`SubscriptionFactory` is also the source of the publish subject, so it must return a stable subject for each namespace. `Subscribe` follows the messaging lifetime contract and normally blocks until its context ends. Successfully handled and namespace-mismatched deliveries are acknowledged; handler failures are negatively acknowledged for at-least-once retry.

## Store and adapter contracts

`Store[K,V]` persists `Entry[V]` values under a string namespace. A returned `present` flag distinguishes an absent record from a stored negative entry. Stores must hide logically expired records, isolate namespaces, make missing-key invalidation idempotent, and honor canceled contexts.

`StandardCache` owns namespace and TTL policy. It exposes ordinary value operations through `Cache` and entry-aware operations through `EntryCache`. `SetEntry` rejects entries that are already expired.

The automatic `Codec[T]` directly encodes strings, byte slices, booleans, numeric scalars, and JSON-compatible composites. Interface values require a custom codec because their concrete decode type is ambiguous.

The namespace, key type and codec, value type and codec, and persistent envelope format together define a shared cache schema. Reusing a namespace with incompatible codecs or types can alias keys or turn old values into decode failures. Keep that schema stable across every process sharing a backend; use a new namespace or an explicit invalidation/migration when making an incompatible change.

Key identity depends on the backend. The memory backend uses Go `==` for `K`; serialized backends use the bytes produced by the key codec. In particular, two pointers to equal values are distinct memory keys but produce the same default JSON key. Prefer scalar or value keys when caches may be layered or swapped, or supply a custom codec whose byte identity matches the application's key semantics.

Values are not deep-copied by the root API. The memory backend stores and returns reference-bearing values such as pointers, maps, and slices by reference, so callers must treat cached values as immutable, clone them at the application boundary, or provide their own synchronization. Serialized backends normally return newly decoded values, but that is a codec property rather than a `Cache` guarantee.

## Optional backend packages

Import only the backend needed by an application. Every backend package is included in the root `v0.2.0` module release.

| Package | Store | Locker | Invalidation bus |
| --- | --- | --- | --- |
| `backend/disk` (bbolt) | yes | no | no |
| `backend/postgres` | yes | yes | yes |
| `backend/redis` | yes | yes | yes |
| `backend/valkey` | yes | yes | yes |
| `backend/dragonfly` | yes | yes | yes |
| `backend/memcached` | yes | yes | no |
| `backend/hazelcast` | yes | no | yes |

For example, import `github.com/justusschock/go-cache/backend/redis` after installing the root module.

Remote clients and connection pools remain caller-owned; stores, lockers, and invalidation buses never close them. The bbolt store owns its file handle and must be closed. PostgreSQL requires an explicit idempotent `Migrate` call and uses the server clock for entry and lease expiration decisions. Its `DeleteExpired` operation physically removes expired entry and lock rows and returns their combined count.

Persistent entry envelopes write version two expiration as signed seconds plus nanoseconds and continue to decode version-one nanosecond timestamps. Wire changes require a reader-first rollout: every process that can read a shared namespace must understand a new version before any process writes it. If a dual-reader/write-old staging release is unavailable, cut the new writer fleet over to a new namespace instead of mixing incompatible readers and writers.

Memcached rounds positive TTLs up to the nearest second and uses relative expiration for durations up to 30 days. Entry expirations after its signed 32-bit Unix range use server expiration `0`; the entry envelope still enforces logical expiry, while physical removal is left to Memcached eviction. Leases cannot safely use that fallback, so `Lock` and `Refresh` reject a TTL whose required absolute expiration exceeds the signed 32-bit Unix range. Lease release atomically replaces the owner token with a one-second tombstone and lets that tombstone expire rather than issuing a later delete that could remove a successor. Hazelcast intentionally has no `Locker`: its map API cannot atomically verify a lease token and extend that same value's TTL, so a two-command refresh could extend a successor's lease.

## Errors and observability

Stable sentinel errors support `errors.Is`. Important cases include invalid namespace/TTL/store configuration, invalid memory options, missing layered-cache dependencies, loader panic, lock contention, lost fill leases, and invalid messaging/coordination adapter dependencies.

Layered fail-open errors are available through `Observer` without exposing cache keys or values. A nil or typed-nil optional observer is treated as absent. Every invalidation bus rejects a nil subscriber callback with `ErrNilInvalidationHandler`. The library deliberately does not prescribe metrics, tracing, or logging implementations.

## Non-goals

This module does not provide:

- a correctness-critical database, write-through persistence, or transactions;
- strong cache coherence, exactly-once invalidation, or total event ordering;
- fenced database writes after a distributed lease changes ownership;
- connection management, service discovery, credentials, or infrastructure provisioning;
- product-specific keys, resources, or codecs.

## Development

The root and all backend packages form one Go module. Run the same repository-wide verification as CI with workspace discovery disabled:

```bash
GOWORK=off go mod verify
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
pre-commit run --all-files
```

GitHub Actions runs the complete unit, race, vet, formatting, assertion-policy, and module-verification gates on Go 1.22.x, 1.23.x, 1.24.x, 1.25.x, and 1.26.x. Integration CI retains race-enabled checks against Redis, Dragonfly, Valkey, Memcached, Hazelcast, and PostgreSQL. Tag builds additionally use `GOWORK=off` and verify the single released module. The repository publishes one annotated release tag, `v0.2.0`.

CI private-module access uses the repository secret `GO_MODULES_TOKEN` before `setup-go` or any Go dependency command. The workflow fails explicitly when the secret is absent, passes it only through a masked environment variable, and never prints it. The runner sets `GOPRIVATE` and `GONOSUMDB` for `github.com/justusschock/*` and installs an ephemeral authenticated Git URL rewrite; it does not change developer credential configuration.

Local implementation plans under `docs/` and local workspaces are intentionally ignored by Git.

## License

Apache License 2.0. See [LICENSE](LICENSE).
