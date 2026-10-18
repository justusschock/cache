# ADR 0001: Record architecture decisions

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 |

## Context

Recorded retrospectively for the first public release. cache has grown a
number of decisions that are visible in the code (a generic storage
contract, one module for all backends, best-effort leases, versioned wire
formats) but were never written down. Contributors who were not present
cannot tell which properties are deliberate and must be preserved, and which
are incidental. A public project also needs a place where changes to the API,
dependencies, formats and semantics are proposed and reviewed before they are
implemented.

## Decision drivers

- D1 Contributors must be able to find why the library behaves as it does.
- D2 Changes to public API, dependencies, formats and concurrency semantics
  must be reviewed before implementation.
- D3 The record must be cheap to maintain and live next to the code.

## Considered options

1. **MADR-style ADRs and PRDs in `docs/`, docs first**
   - Pro: versioned with the code and reviewed in the same pull requests.
   - Pro: MADR is a widely known, lightweight format with explicit options
     and consequences.
   - Con: requires discipline to write the document before the code.
2. **Design notes in the README and package documentation only**
   - Pro: no extra files.
   - Con: rationale and rejected alternatives do not fit into user-facing
     documentation; history of decisions is lost when text is edited.
3. **Issues or wiki pages outside the repository**
   - Pro: easy discussion.
   - Con: not versioned with the code; unavailable offline and to tools that
     only see the repository.

## Decision

We will record requirements as PRDs in `docs/prd/` and decisions as
MADR-style ADRs in `docs/adr/`, both from the templates in those
directories, and follow a docs-first process:

- New documents start as `Proposed` and become `Accepted` when their pull
  request merges; implementation follows acceptance.
- Accepted ADRs are immutable except for their status line. A changed
  decision gets a new ADR with `Supersedes: ADR-NNNN`, and the old one is
  marked `Superseded by ADR-MMMM`.
- PRDs are kept current; requirement IDs (`FR-CACHE-NN`, `NFR-CACHE-NN`) are
  never reused or renumbered, and tests cite the IDs they verify.
- `docs/README.md` indexes every PRD and ADR with its status.

The decisions already embodied in the code are recorded retrospectively as
ADR-0002 to ADR-0012.

## Consequences

### Positive
- Rationale and rejected alternatives are preserved next to the code.
- Reviews of API and format changes have a fixed place and shape.

### Negative
- Small changes that touch semantics need a document as well as code.

### Follow-ups
- Keep `docs/README.md` in sync whenever a document is added or changes
  status.

## Links

- [AGENTS.md](../../AGENTS.md)
- [MADR](https://adr.github.io/madr/)
