# go-cache

`go-cache` provides typed, composable caches for Go: bounded in-process storage, layered promotion, pull-through loading, negative entries, renewable fill leases, and transport-neutral invalidation.

The repository is private and requires Go 1.26 or newer. Cache data is assumed to be reconstructible and disposable; correctness-critical state belongs in a durable system of record.

## Installation

Configure private-module access and authenticate GitHub through your normal Git credential flow:

```bash
go env -w GOPRIVATE='github.com/justusschock/*'
go env -w GONOSUMDB='github.com/justusschock/*'
go get github.com/justusschock/go-cache@v0.1.0
```

The root module depends on the tagged `go-concurrency`, `go-messaging`, and `go-distributed-coordination` modules. Database and remote-cache clients are isolated in separately versioned nested modules, so they do not enter the root dependency graph.

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

The store uses a global least-recently-used order across namespaces. Reads refresh recency, expiration and invalidation remove entries from the order, and `Close` is idempotent.

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

`NewPullThrough` shares one typed future per key. Each caller waits with its own context, so canceling a waiter does not cancel a fill needed by other callers. Loader errors are never cached.

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
	func(namespace, key string) (string, error) { return namespace + "/" + key, nil },
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

Key identity depends on the backend. The memory backend uses Go `==` for `K`; serialized backends use the bytes produced by the key codec. In particular, two pointers to equal values are distinct memory keys but produce the same default JSON key. Prefer scalar or value keys when caches may be layered or swapped, or supply a custom codec whose byte identity matches the application's key semantics.

## Optional backend modules

Install only the backend needed by an application. Every nested module requires its matching `backend/<name>/v0.1.0` repository tag.

| Module | Store | Locker | Invalidation bus |
| --- | --- | --- | --- |
| `backend/disk` (bbolt) | yes | no | no |
| `backend/postgres` | yes | yes | yes |
| `backend/redis` | yes | yes | yes |
| `backend/valkey` | yes | yes | yes |
| `backend/dragonfly` | yes | yes | yes |
| `backend/memcached` | yes | yes | no |
| `backend/hazelcast` | yes | no | yes |

For example:

```bash
go get github.com/justusschock/go-cache/backend/redis@v0.1.0
```

Remote clients and connection pools remain caller-owned; stores, lockers, and invalidation buses never close them. The bbolt store owns its file handle and must be closed. PostgreSQL requires an explicit idempotent `Migrate` call and uses the server clock for entry and lease expiration decisions. Persistent entry envelopes store expiration as signed seconds plus nanoseconds and continue to decode version-one nanosecond timestamps.

Memcached rounds positive TTLs up to the nearest second and uses relative expiration for durations up to 30 days. Expirations after its signed 32-bit Unix range use server expiration `0`; the entry envelope still enforces logical expiry, while physical removal is left to Memcached eviction. Hazelcast intentionally has no `Locker`: its map API cannot atomically verify a lease token and extend that same value's TTL, so a two-command refresh could extend a successor's lease.

## Errors and observability

Stable sentinel errors support `errors.Is`. Important cases include invalid namespace/TTL/store configuration, invalid memory options, missing layered-cache dependencies, loader panic, lock contention, lost fill leases, and invalid messaging/coordination adapter dependencies.

Layered fail-open errors are available through `Observer` without exposing cache keys or values. The library deliberately does not prescribe metrics, tracing, or logging implementations.

## Non-goals

This module does not provide:

- a correctness-critical database, write-through persistence, or transactions;
- strong cache coherence, exactly-once invalidation, or total event ordering;
- fenced database writes after a distributed lease changes ownership;
- connection management, service discovery, credentials, or infrastructure provisioning;
- product-specific keys, resources, or codecs.

## Development

The root and heavyweight backends are separate Go modules. Before the first release tag exists, create an ignored workspace for local cross-module testing:

```bash
go work init .
go work use backend/disk backend/postgres backend/redis backend/valkey
go work use backend/dragonfly backend/memcached backend/hazelcast
go work edit -go=1.26
go work edit -replace=github.com/justusschock/go-cache@v0.1.0=.
go work edit -replace=github.com/justusschock/go-cache/backend/redis@v0.1.0=./backend/redis
```

Run the same verification matrix as CI:

```bash
for module in . backend/disk backend/postgres backend/redis backend/valkey backend/dragonfly backend/memcached backend/hazelcast; do
  (cd "$module" && go test -race ./... && go vet ./...) || exit 1
done
pre-commit run --all-files
```

Integration CI runs the same race-enabled checks against Redis, Dragonfly, Valkey, Memcached, Hazelcast, and PostgreSQL. Tag builds additionally disable workspaces with `GOWORK=off` and verify the tagged module against its released dependencies. Publish dependency tags before dependents: root first, then Redis, then the other backend modules.

Local implementation plans under `docs/` and local workspaces are intentionally ignored by Git.

## License

Apache License 2.0. See [LICENSE](LICENSE).
