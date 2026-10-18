# AGENTS.md

Rules for anyone, human or agent, changing go-cache. They are binding. If a
change has to break one, say so in the pull request and, where it is a design
decision, record it in an ADR.

go-cache is a library of typed, composable caches for Go: a generic storage
contract with in-process, on-disk and remote backends, layering, pull-through
loading with single flight and fill leases, and distributed invalidation.
Its requirements and design decisions are recorded in
[docs/](docs/README.md).

## Docs first

No behaviour is added or changed before the documents that govern it say so.

- **PRDs** (`docs/prd/NNNN-title.md`, from [the template](docs/prd/TEMPLATE.md))
  state *what* the library does: problem, use cases, goals and non-goals,
  functional requirements `FR-CACHE-NN`, non-functional requirements
  `NFR-CACHE-NN`, API sketch, acceptance criteria mapped to named tests, open
  questions. This library's area code is `CACHE`.
- **ADRs** (`docs/adr/NNNN-title.md`, MADR style, from
  [the template](docs/adr/TEMPLATE.md)) state *how and why*: context, decision
  drivers, considered options with pros and cons, decision, consequences.
- **Lifecycle.** New PRDs and ADRs start as `Proposed` and are reviewed in a
  pull request; merged, they become `Accepted`. Implementation follows
  acceptance — for small changes in the same pull request, as separate
  commits, documents first.
- A decision made while implementing gets a new ADR **in the same pull
  request as the code**. Changing public API, adding a dependency, changing a
  wire or persistence format, or changing error, concurrency or cancellation
  semantics always needs one.
- **Accepted ADRs are never edited**, except for their status line. To change
  a decision, add a new ADR with `Supersedes: ADR-NNNN` and set the old one to
  `Superseded by ADR-MMMM`.
- PRDs are kept current: a pull request that changes behaviour updates the
  requirements it touches. Requirement IDs are never reused or renumbered.
- [docs/README.md](docs/README.md) indexes every PRD and ADR with its status;
  update it whenever a document is added or changes status.
- **Traceability.** Tests cite the requirement they verify in a comment
  (`// FR-CACHE-03`). Every functional requirement has at least one test.

## Public API

- The public API is every exported identifier outside `internal/`. It is
  documented, covered by tests and examples, and changed only through an ADR.
- Implementation details live under `internal/` or stay unexported. Do not
  export something only to test it; test from inside the package instead.
- Conformance suites for implementers of the library's interfaces are public;
  test helpers used only by this repository are internal.
- Versioning follows semver. While the major version is `0`, a minor release
  may break the API; every break is listed in `CHANGELOG.md` and backed by an
  ADR.

## Layout

- `.` (package `cache`): the `Store`/`BatchStore` contract, `Entry`,
  `StandardCache`, `LayeredStore`, `PullThroughCache`, `Codec`, the
  `Locker`/`Lease` and `InvalidationBus` contracts, and the
  go-distributed-coordination and go-messaging adapters.
- `backend/memory/`: bounded in-process LRU store, locker and invalidation
  bus; the reference implementation.
- `backend/disk/`: persistent bbolt store with atomic batches and ordered
  prefix scans.
- `backend/redis/`, `backend/valkey/`, `backend/dragonfly/`: Redis-protocol
  stores, lockers and Pub/Sub invalidation buses.
- `backend/memcached/`: store and locker.
- `backend/hazelcast/`: store and topic invalidation bus (no locker, see
  ADR-0009).
- `backend/postgres/`: store with `Migrate`/`DeleteExpired`, row-lease
  locker and LISTEN/NOTIFY invalidation bus.
- `cachetest/`: conformance suites every `Store`, `BatchStore` and
  `InvalidationBus` implementation must pass.
- `internal/wire/`: the versioned entry envelope and invalidation message
  formats (ADR-0007).

## Engineering rules

1. **Small, explicit contracts.** Every exported type and function documents
   its behaviour, errors, concurrency safety and cancellation.
2. **Dependencies.** Standard library first. The root package imports only the
   standard library and the sibling libraries go-concurrency,
   go-distributed-coordination and go-messaging; backend client modules are
   imported only by their backend package (ADR-0003). A new dependency needs
   an ADR.
3. **Errors.** Sentinel errors and typed errors are part of the API; wrap
   with `%w` and test with `errors.Is`/`errors.As`.
4. **Context.** Operations that block or do I/O take `ctx context.Context`
   first and honour cancellation.
5. **No logging in the library.** Report through return values or observer
   hooks; never write to stdout/stderr or a global logger.
6. **Concurrency.** Every exported type states whether it is safe for
   concurrent use; tests run with the race detector.

## Testing

- Unit tests for every package, asserting with testify `assert`/`require`
  (CI rejects `t.Fatal`/`t.Error`); examples (`example_test.go`) for every
  public entry point, kept in sync with the README.
- **`testing/synctest`** for anything involving time or concurrency
  (timeouts, TTLs, retries, tickers, leases, goroutine coordination). No real
  `time.Sleep` waits in tests; the only exceptions are the `cachetest`
  suites and integration tests, which target servers that own their clocks
  (ADR-0012).
- Implementations of the library's interfaces pass the public conformance
  suite in `cachetest`.
- Tests are deterministic and pass with `go test -race -count=3 ./...`.

## Workflow

- Branch from `main`, open a pull request, merge only with green CI. Nothing
  is pushed to `main` directly.
- Commit subjects use conventional-commit prefixes (`feat:`, `fix:`,
  `refactor:`, `docs:`, `test:`, `ci:`, `chore:`; `!` for breaking changes)
  and say what changed; the body says why.
- Before opening a pull request:

  ```sh
  gofmt -l .                     # prints nothing
  go vet ./...
  go test -race -count=1 ./...
  golangci-lint run ./...
  go mod tidy && git diff --exit-code go.mod go.sum
  ```

  The repository is a single module. Integration tests need the servers
  from the CI `integration` job and run with
  `go test -race -tags=integration ./backend/...` (addresses from
  `REDIS_ADDR`, `DRAGONFLY_ADDR`, `VALKEY_ADDR`, `MEMCACHED_ADDR`,
  `HAZELCAST_ADDR`, `POSTGRES_URL`).
- CI workflows pin every action to a full commit SHA, use only `push` and
  `pull_request` triggers (never `pull_request_target`) and read-only
  permissions. Dependabot keeps Go modules and actions current.
- Releases are tags `vX.Y.Z` on `main`, with the changelog section as
  release notes.
