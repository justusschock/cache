# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the major version is `0`, a minor release may break the API.

## [Unreleased]

## [0.1.0] - 2026-10-18

First public release.

### Added

- Typed `Cache[K, V]` and `Store[K, V]` contracts with namespaces, default
  and per-write TTLs, negative entries and never-expiring entries
  (`NoExpiry`); `New` builds a `StandardCache` over any store.
- Batch reads, writes and invalidations through `BatchCache` and
  `BatchStore`.
- `LayeredStore` (`NewLayered`): fastest-to-slowest layers with
  expiry-preserving promotion, fail-open or strict reads, and an `Observer`
  hook.
- `PullThroughCache` (`NewPullThrough`): per-key single flight with
  waiter-scoped cancellation, negative caching, loader-panic recovery and
  optional renewable fill leases through a `Locker`.
- Distributed invalidation over any `InvalidationBus`
  (`NewLayeredWithInvalidationBus`, `ListenInvalidations`), with adapters
  for messaging (`NewMessagingInvalidationBus`) and distributed-coordination
  (`NewCoordinationLocker`).
- In-process and on-disk backends: `backend/memory` (bounded LRU) and
  `backend/disk` (bbolt, atomic batches, ordered prefix scans).
- Remote backends: `backend/redis`, `backend/valkey`, `backend/dragonfly`,
  `backend/memcached`, `backend/hazelcast` and `backend/postgres`.
- Package `cachetest`: the `RunStoreContract`, `RunBatchStoreContract` and
  `RunInvalidationBusContract` conformance suites.
- Requirements and architecture decision records under `docs/`.

Requires Go 1.25 or newer.

[Unreleased]: https://github.com/justusschock/cache/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/justusschock/cache/releases/tag/v0.1.0
