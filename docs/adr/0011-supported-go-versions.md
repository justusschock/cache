# ADR 0011: Supported Go versions

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (NFR-CACHE-03), ADR-0012 |

## Context

Recorded retrospectively for the first public release. The module's `go`
directive is `1.25.0`. The tests depend on `testing/synctest` with its stable
API (`synctest.Test`, `synctest.Wait`), which first shipped in Go 1.25, and
the code uses `reflect.TypeFor` and range-over-int, available since Go 1.22.
The Go project supports the two most recent major releases; at the time of
writing those are Go 1.26 and Go 1.27.

## Decision drivers

- D1 Tests must be able to use `testing/synctest` (ADR-0012).
- D2 Users on supported Go releases must be able to use the library.
- D3 The supported range must be verified in CI.

## Considered options

1. **Floor at Go 1.25, test 1.25 plus the two supported releases** (chosen)
   - Pro: synctest is available; users on the oldest still-common toolchain
     are not cut off.
   - Con: one more CI matrix entry than the upstream support window.
2. **Track only the two upstream-supported releases**
   - Pro: smallest matrix; newest language features.
   - Con: raises the floor every six months without a technical need.
3. **Lower floor (for example Go 1.22) with synctest tests behind build
   tags**
   - Pro: wider compatibility.
   - Con: two test paths for every time-dependent test; the experimental
     pre-1.25 synctest API differs.

## Decision

We will require Go 1.25 or newer (`go 1.25.0` in `go.mod`) and test in CI on
Go `1.25.x`, `oldstable` and `stable`. The floor is raised only by a new ADR,
typically when a needed language or library feature requires it.

## Consequences

### Positive
- synctest is available everywhere the library is tested.
- The supported range is explicit and verified.

### Negative
- Users on Go 1.24 or older cannot use the library.

### Follow-ups
- Revisit the floor when Go 1.25 is far outside the upstream support window
  and a newer feature is needed.

## Links

- `go.mod`, `.github/workflows/ci.yml`
