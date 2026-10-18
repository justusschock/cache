# cache

cache provides typed, composable caches for Go: one generic storage
contract with in-process, on-disk and remote backends, layered stores with
promotion, pull-through loading with single flight and negative caching,
renewable distributed fill leases, and transport-neutral invalidation between
processes. It is for Go services and tools that want caching without
hand-rolling stampede protection, expiry handling and cross-process
invalidation for each storage technology. Cached data is assumed to be
reconstructible and disposable; correctness-critical state belongs in a
durable system of record.

## Features

- Typed `Cache[K, V]` and `Store[K, V]` contracts with namespaces, default
  and per-write TTLs, negative entries and explicit never-expiring entries
  (`NoExpiry`).
- Batch reads, writes and invalidations (`BatchCache`, `BatchStore`), one
  round trip or transaction where the backend supports it.
- `LayeredStore`: fastest-to-slowest layers with promotion that preserves
  expiration, fail-open or strict reads, and an `Observer` hook.
- `PullThroughCache`: per-key single flight with waiter-scoped cancellation,
  negative caching, loader-panic recovery, and optional distributed fill
  leases.
- Distributed invalidation through any `InvalidationBus`, with adapters for
  messaging and distributed-coordination.
- Backends: bounded in-process LRU, bbolt file with ordered prefix scans,
  Redis, Valkey, Dragonfly, Memcached, Hazelcast and PostgreSQL.
- `cachetest`: public conformance suites for your own stores and
  invalidation buses.

## Installation

```sh
go get github.com/justusschock/cache@latest
```

Requires Go 1.25 or newer. The root package and every backend are one
module; an application compiles only the backend packages it imports.

## Quick start

This is the package `Example` in [example_test.go](example_test.go):

```go
package main

import (
	"context"
	"fmt"
	"time"

	cache "github.com/justusschock/cache"
	"github.com/justusschock/cache/backend/memory"
)

type User struct {
	ID   string
	Name string
}

func main() {
	store := memory.New[string, User]()
	defer func() { _ = store.Close() }()

	users, err := cache.New("users", 5*time.Minute, store)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	if err := users.Set(ctx, "user-42", User{ID: "user-42", Name: "Ada"}); err != nil {
		panic(err)
	}
	user, found, err := users.Get(ctx, "user-42")
	if err != nil {
		panic(err)
	}
	fmt.Println(found, user.Name) // true Ada
}
```

`memory.New` is bounded to 10,000 live entries and sweeps expired entries
once per minute; `memory.NewWithOptions` sets both limits. The store uses one
least-recently-used order across namespaces. Every memory store must be
closed, because construction starts the sweeper goroutine.

```go
store, err := memory.NewWithOptions[string, User](memory.Options{
	MaxEntries:      50_000,
	CleanupInterval: 15 * time.Second,
})
```

### Entries without expiration

TTLs must be positive, with one explicit opt-in: `cache.NoExpiry`. Passed to
`New`, it makes `Set` and `SetMany` write entries that never expire; passed
to `SetWithTTL` or `SetManyWithTTL`, it does the same for one write. Any other
zero or negative TTL is rejected with `ErrInvalidTTL`.

```go
hashes, err := cache.New("file-hashes", cache.NoExpiry, store)
err = hashes.Set(ctx, "src/main.go", digest)

// Or keep a default TTL and opt out per write:
err = sessions.SetWithTTL(ctx, "pinned", session, cache.NoExpiry)
```

A never-expiring entry has a zero `Entry.ExpiresAt` (`Entry.NeverExpires`).
Backends keep it until it is invalidated, replaced or evicted by the
backend's own capacity policy: memory never sweeps it, Redis, Valkey,
Dragonfly and Hazelcast write it without a native TTL, Memcached uses
expiration `0`, and PostgreSQL stores `expires_at = 'infinity'`.

### Batch operations

`StandardCache` implements `BatchCache`: `GetMany`, `SetMany`,
`SetManyWithTTL` and `InvalidateMany`. Results are aligned with the key
slice, value slices must have the same length (`ErrBatchLength` otherwise),
and a later duplicate key wins.

```go
digests, found, err := hashes.GetMany(ctx, []string{"src/a.go", "src/b.go"})
err = hashes.SetMany(ctx, changedPaths, changedDigests)
```

When the store implements `BatchStore`, each call is one store call;
otherwise the cache falls back to one call per key and stops at the first
error. The disk store applies a batch in one bbolt transaction (atomic, one
commit); the memory store under one lock; Redis, Valkey and Dragonfly in one
pipeline, which is not a transaction, so a failed call can leave part of a
batch applied.

### Layered caches

Stores are ordered from fastest to slowest. A lower-layer hit is promoted
into every faster layer that missed, without changing its expiration. Writes
go to the slowest layer first.

```go
layers, err := cache.NewLayered([]cache.Store[string, User]{local, shared})
users, err := cache.New("users", 5*time.Minute, layers)
```

Reads fail open by default: a failing layer is reported to the optional
`Observer` (`cache.WithLayerObserver`) and lookup continues below it;
`cache.WithStrictLayerReads()` makes the first layer error terminal. A
slowest-layer write failure stops the write; faster-layer failures are joined.

### Pull-through loading

`NewPullThrough` loads misses with one shared load per key. Each caller waits
with its own context, so canceling one waiter does not cancel a load other
callers need; when the last waiter leaves, the load is canceled and a later
request starts afresh. Loader errors are never cached.

```go
loaded, err := cache.NewPullThrough(
	"users",
	users,
	func(ctx context.Context, id string) (User, bool, error) {
		return database.FindUser(ctx, id)
	},
	cache.WithNegativeTTL[string](30*time.Second),
)
user, found, err := loaded.Get(ctx, "user-42")
```

Authoritative misses (`found == false`) are stored as negative entries only
when the wrapped cache implements `EntryCache`, as `StandardCache` does.

For cross-process fill coordination, pass a `Locker`: a backend locker, or
any distributed-coordination backend through `NewCoordinationLocker`.

```go
locker, err := cache.NewCoordinationLocker(
	coordinationBackend,
	"worker-7",
	func(namespace, key string) (string, error) {
		encoded, err := json.Marshal([2]string{namespace, key})
		return string(encoded), err
	},
)
loaded, err := cache.NewPullThrough("users", users, loadUser,
	cache.WithLocker[string](locker, 10*time.Second))
```

The `FillKeyFunc` must be deterministic and injective over the whole
`(namespace, key)` tuple; concatenating with a delimiter is unsafe when either
part may contain it. Leases are refreshed every half TTL. Observed lease loss
or a failed refresh cancels the loader, waits for it, skips the cache write
if it has not started, and returns `ErrFillLeaseLost`; the lease is released
on a non-canceled context. Loaders and caches used with leases must honour
cancellation. Leases reduce duplicate fills; they do not fence writes.

### Distributed invalidation

A `LayeredStore` built with `NewLayeredWithInvalidationBus` publishes a
key-scoped event for `Set` and `Invalidate` and a namespace-scoped event for
`InvalidateAll`. `ListenInvalidations` applies remote events by evicting only
the layers above the shared layer, so duplicate and out-of-order delivery is
safe. `InvalidationEvent.Generation` is transport metadata, not an ordering.

```go
bus, err := cache.NewMessagingInvalidationBus[string](publisher, subscriber,
	func(namespace string) messaging.Subscription {
		return messaging.Subscription{
			Subject:     "cache.invalidations." + namespace,
			Consumer:    "worker-7",
			Semantics:   messaging.AtLeastOnce,
			MaxInFlight: 32,
		}
	})
layers, err := cache.NewLayeredWithInvalidationBus(
	[]cache.Store[string, User]{local, shared}, bus)

go func() { _ = layers.ListenInvalidations(listenCtx, "users", 1) }()
```

The subscription factory is also the source of the publish subject, so it
must return a stable subject per namespace. Handled deliveries and deliveries
for another namespace are acknowledged; handler failures are negatively
acknowledged for at-least-once redelivery.

### Disk prefix scans

The bbolt store hashes keys by default. `disk.WithOrderedKeys` stores the key
codec's bytes verbatim, which keeps entries in encoded-key order and enables
`ScanPrefix` and `DeletePrefix`:

```go
store, err := disk.Open[string, string](
	filepath.Join(stateDir, "hashes.db"),
	disk.WithOrderedKeys[string, string](),
)
defer store.Close()

err = store.ScanPrefix(ctx, "file-hashes", []byte("src/"),
	func(path string, entry cache.Entry[string]) bool {
		fmt.Println(path, entry.Value)
		return true // false stops the scan
	})
removed, err := store.DeletePrefix(ctx, "file-hashes", []byte("vendor/"))
```

The prefix is matched against encoded key bytes: strings and `[]byte` encode
to themselves, integers to decimal text (lexicographic order), composite keys
to JSON; use `disk.WithKeyCodec` for another ordering. `ScanPrefix` runs in
one read transaction and its callback must not write to the same store.
Without ordered keys both return `disk.ErrOrderedKeysRequired`. Ordered and
hashed layouts are separate; always reopen a file with the same choice.

## Packages

| Package | Purpose |
| --- | --- |
| [`github.com/justusschock/cache`](https://pkg.go.dev/github.com/justusschock/cache) | Contracts (`Store`, `Cache`, `Locker`, `InvalidationBus`), `StandardCache`, `LayeredStore`, `PullThroughCache`, codecs and adapters |
| `backend/memory` | Bounded in-process LRU store, locker and invalidation bus |
| `backend/disk` | Persistent bbolt store with atomic batches and prefix scans |
| `backend/redis` | Redis store, locker and Pub/Sub invalidation bus |
| `backend/valkey` | Valkey store, locker and Pub/Sub invalidation bus |
| `backend/dragonfly` | Dragonfly store, locker and invalidation bus (Redis protocol) |
| `backend/memcached` | Memcached store and locker |
| `backend/hazelcast` | Hazelcast map store and topic invalidation bus |
| `backend/postgres` | PostgreSQL store, row-lease locker and LISTEN/NOTIFY invalidation bus |
| `cachetest` | Conformance suites for `Store`, `BatchStore` and `InvalidationBus` implementations |

Packages under `internal/` are not part of the API.

| Backend | Store | BatchStore | Locker | Invalidation bus |
| --- | --- | --- | --- | --- |
| `backend/memory` | yes | yes (one lock) | yes | yes (in-process) |
| `backend/disk` | yes | yes (one transaction) | no | no |
| `backend/redis` | yes | yes (pipelined) | yes | yes |
| `backend/valkey` | yes | yes (pipelined) | yes | yes |
| `backend/dragonfly` | yes | yes (pipelined) | yes | yes |
| `backend/memcached` | yes | no | yes | no |
| `backend/hazelcast` | yes | no | no | yes |
| `backend/postgres` | yes | no | yes | yes |

To test your own implementation:

```go
func TestStoreContract(t *testing.T) {
	cachetest.RunStoreContract(t, func(t *testing.T) cache.Store[string, int] {
		return mybackend.New[string, int](client)
	})
}
```

## Design and guarantees

**Concurrency.** Every exported type is safe for concurrent use. Operations
that block or perform I/O take a `context.Context` first and honour
cancellation. Remote clients and pools are caller-owned: stores, lockers and
buses never close them. The memory and disk stores own resources and must be
closed; `Close` is idempotent.

**Expiry.** `Entry.ExpiresAt` is the logical expiration and is enforced on
read by every store; native TTLs are used only to reclaim space. PostgreSQL
uses the server clock and offers `Migrate` (idempotent schema creation) and
`DeleteExpired` (physical cleanup of entries and locks). Memcached rounds
TTLs up to whole seconds; leases whose expiry exceeds its signed 32-bit Unix
range are rejected. Hazelcast has no `Locker`, because its map API cannot
atomically verify a lease token and extend that value's TTL.

**Errors.** Sentinel errors (for example `ErrInvalidTTL`, `ErrNilStore`,
`ErrBatchLength`, `ErrLockNotAcquired`, `ErrLeaseLost`, `ErrFillLeaseLost`,
`ErrLoaderPanic`) support `errors.Is`; underlying errors are wrapped with
`%w`, and operations on several layers join their errors. Nil and typed-nil
dependencies are rejected at construction. The library never logs; layered
fail-open errors reach the optional `Observer`, whose events contain no keys
or values.

**Keys, values and schema.** The memory backend compares keys with Go `==`;
serialized backends compare the bytes produced by the key codec, so two
pointers to equal values are distinct memory keys but the same default JSON
key. Values are not copied: the memory backend returns reference-bearing
values by reference, so treat cached values as immutable. The namespace, key
and value types and codecs, and the entry envelope together form a shared
schema; change it with a new namespace or an explicit migration. The
`DefaultCodec` encodes strings, byte slices, booleans and numbers directly
and other values as JSON; interface types need a custom codec.

**Wire formats.** Serialized backends write a versioned entry envelope
(version 2; version 1 is still read) and a versioned invalidation message.
Format changes are rolled out reader-first: every process that reads a
shared namespace must understand a new version before any process writes it.

**Non-goals.** cache is not a correctness-critical database and offers no
write-through persistence or transactions across keys, no strong coherence or
exactly-once invalidation, no fenced writes after lease loss, and no
connection management or provisioning.

**Stability.** The module is at `v0.x`: minor releases may break the API.
Every break is listed in [CHANGELOG.md](CHANGELOG.md) and backed by an ADR.

## Documentation

- [docs/README.md](docs/README.md): requirements (PRD) and architecture
  decision records.
- [pkg.go.dev](https://pkg.go.dev/github.com/justusschock/cache): API
  reference and examples.

## Contributing

Contributions are welcome through pull requests; nothing is pushed to `main`
directly. Read [AGENTS.md](AGENTS.md) first: it describes the docs-first
process, the API and testing rules, and the checks to run before opening a
pull request.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
