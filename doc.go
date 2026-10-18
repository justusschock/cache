// Package cache provides typed, composable caches.
//
// A [StandardCache] applies a namespace and a default TTL to a [Store], the
// storage contract implemented by the backend packages under backend/ (an
// in-process LRU, a bbolt file, Redis, Valkey, Dragonfly, Memcached,
// Hazelcast and PostgreSQL). Stores compose: [LayeredStore] stacks stores
// from fastest to slowest with read promotion and optional distributed
// invalidation through an [InvalidationBus], and [PullThroughCache] loads
// misses with per-key single flight, negative caching and optional
// distributed fill leases from a [Locker].
//
// Entries are typed ([Entry]) and carry a logical expiration; a zero
// expiration, requested with [NoExpiry], never expires. Persistent backends
// serialize keys and values with a [Codec]; [DefaultCodec] covers strings,
// byte slices, scalars and JSON-compatible values.
//
// Cached data is assumed to be reconstructible and disposable. Fill leases
// reduce duplicate work but do not fence writes, and invalidation delivery
// is at-least-once without ordering guarantees.
//
// All exported types are safe for concurrent use unless their documentation
// says otherwise. Operations that block or perform I/O take a
// [context.Context] and honour its cancellation.
package cache
