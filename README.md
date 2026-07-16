# go-cache

Typed, composable, layered caches with pluggable stores and distributed invalidation for Go.

> **Status:** Existing experimental implementation; API hardening and platform integration are planned.

Module path: `github.com/justusschock/go-cache`

## Scope

Typed cache and store contracts, memory and remote backends, layered promotion, single-flight fills, TTLs, and reconstructible-state invalidation.

This library is private and intended for reuse by future Go projects.

## Target platform dependencies

Required imports:

- [`go-concurrency`](https://github.com/justusschock/go-concurrency)
- [`go-messaging`](https://github.com/justusschock/go-messaging)
- [`go-distributed-coordination`](https://github.com/justusschock/go-distributed-coordination)

Optional adapter imports:

None currently planned.

Optional integrations must remain one-way: concrete adapters import their neutral contracts, never the reverse.

## Design constraints

- Keep public contracts independent of product-specific resource types.
- Keep the dependency graph acyclic; imports point toward lower-level mechanisms.
- Put generic lifecycle transfer in `go-lifecycle/handoff` and finite resource-right transfer in `go-resources/handoff`.
- Use `go-cache` only for reconstructible, disposable state, never correctness-critical state.

## Development

Install and run the repository hooks:

```bash
pre-commit install
pre-commit run --all-files
```

Once the module implementation exists, run its Go checks with:

```bash
go test ./...
```

Local implementation plans live under `docs/` and are intentionally excluded from Git.

## License

Apache License 2.0. See [LICENSE](LICENSE).
