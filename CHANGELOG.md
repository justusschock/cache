# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/). While the major
version is 0, minor releases may contain breaking changes; each one is listed
here.

## [Unreleased]

Preparation for the first public release.

### Added

- Public `cachetest` package with the `RunStoreContract`,
  `RunBatchStoreContract` and `RunInvalidationBusContract` conformance suites
  for third-party `Store`, `BatchStore` and `InvalidationBus`
  implementations (previously `internal/cachetest`).
- Package documentation, doc comments for every exported identifier, and
  runnable examples for the quick start, pull-through loading and layered
  stores.
- Requirements (PRD-0001) and architecture decision records in `docs/`,
  `AGENTS.md`, `NOTICE`, and this changelog.
- CI on Go 1.25.x, oldstable and stable with the race detector,
  golangci-lint and the existing integration job; Dependabot for Go modules
  and GitHub Actions.

### Changed

- Time- and concurrency-dependent tests run on virtual time with
  `testing/synctest`; the Memcached lease-release test no longer needs
  network access.
- CI no longer needs repository secrets or module-access configuration.

### Removed

- **Breaking:** `memory.(*InvalidationBus).SubscriberCount` is no longer
  exported. It existed only for tests.

### Requirements

- Go 1.25 or newer (unchanged; required by `testing/synctest`).

