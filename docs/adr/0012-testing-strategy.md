# ADR 0012: Testing strategy

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (NFR-CACHE-01, NFR-CACHE-02), ADR-0006, ADR-0011 |

## Context

Recorded retrospectively for the first public release. Much of the library's
value lies in timing and concurrency behaviour: TTL expiry, sweeps, lease
renewal at half TTL, lock polling, waiter cancellation and invalidation
delivery. Tests that used real sleeps and wall-clock deadlines were slow and
could flake under load. The test suite now consists of:

- unit tests per package, written with testify `assert`/`require` (CI
  rejects `t.Fatal`/`t.Error` style assertions to keep one style);
- `testing/synctest` for every unit test that depends on time or goroutine
  coordination, using virtual time and `synctest.Wait` instead of sleeps,
  polling loops and observed-context hooks;
- the public `cachetest` conformance suites, run against every backend;
- integration tests (build tag `integration`) against Redis, Dragonfly,
  Valkey, Memcached, Hazelcast and PostgreSQL service containers in CI;
- hermetic fakes where a protocol interaction must be forced (the Memcached
  release-race server is served over `net.Pipe` through the client's
  `DialContext`);
- runnable examples kept in sync with the README;
- requirement IDs from PRD-0001 cited in test comments.

Every unit test passes with `go test -race -count=3 ./...`.

## Decision drivers

- D1 Deterministic tests: no dependence on scheduler timing or machine load.
- D2 Fast feedback: simulated hours and years must run in milliseconds.
- D3 Every backend meets the same contract.
- D4 Unit tests must run without network access or external services.

## Considered options

1. **synctest for time and concurrency, wall clock only where a remote
   server owns time** (chosen)
   - Pro: exact assertions (an entry expires exactly at its TTL; a 35 ms
     load under a 20 ms lease is refreshed exactly three times).
   - Con: code inside a bubble must not block on real I/O, so tests against
     servers cannot use it.
2. **An injectable `Clock` interface in the library**
   - Pro: works on older Go versions.
   - Con: adds API surface that exists only for tests and cannot control
     timers inside dependencies.
3. **Real sleeps with generous margins**
   - Con: slow and still flaky.

## Decision

We will use `testing/synctest` for every unit test involving time or goroutine
coordination and keep no `Clock` abstraction in the API. The following stay
on the wall clock deliberately:

- the `cachetest` store suites, because they call `t.Run` (forbidden in a
  bubble) and target servers that enforce expiry themselves; their
  logical-expiration case sleeps 20 ms;
- integration tests, because they perform network I/O against real servers;
- disk tests outside the TTL cases, which only perform file I/O.

`RunInvalidationBusContract` runs inside a bubble for the in-process bus.

## Consequences

### Positive
- The unit suite is deterministic and fast under the race detector.
- New time-dependent behaviour has an established testing pattern.

### Negative
- Requires Go 1.25 (ADR-0011).
- Contributors must know synctest's rules (no `t.Run`, no real blocking I/O
  inside a bubble, close goroutine-owning resources before the bubble ends).

### Follow-ups
- Consider a synctest-friendly mode for the conformance suites
  (PRD-0001 Q1).

## Links

- `cachetest/`, `.github/workflows/ci.yml`, ADR-0006
