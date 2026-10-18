// Package cachetest provides conformance suites for implementations of the
// go-cache store and invalidation-bus interfaces.
//
// Every backend in this module passes these suites, and third-party
// implementations of [cache.Store], [cache.BatchStore] and
// [cache.InvalidationBus] should run them from their own tests:
//
//	func TestStoreContract(t *testing.T) {
//		cachetest.RunStoreContract(t, func(t *testing.T) cache.Store[string, int] {
//			store := mybackend.New[string, int](client)
//			t.Cleanup(func() { _ = store.Close() })
//			return store
//		})
//	}
//
// The suites exercise stores with string keys and int values. Each subtest
// prefixes the namespaces it uses with the subtest name and a timestamp, so a
// factory may return stores that share one remote server or database.
//
// The suites use the wall clock because they target implementations whose
// expiry or delivery may be enforced by a remote server: the
// logical-expiration case sleeps for a few milliseconds, and the
// invalidation-bus contract polls for delivery for up to a few seconds.
// [RunStoreContract] and [RunBatchStoreContract] call [testing.T.Run] and
// therefore must not be called inside a [testing/synctest] bubble.
// [RunInvalidationBusContract] may run inside a bubble when the bus under
// test is entirely in-process, which turns its waits into virtual time.
package cachetest
