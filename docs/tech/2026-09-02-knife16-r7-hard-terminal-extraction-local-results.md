# Knife16 R7 hard-terminal extraction local results

Date: 2026-09-02.

Status: **SEGMENTS A-D LOCALLY ACCEPTED AS A BEHAVIOR-PRESERVING,
NECESSARY-ONLY EXTRACTION; NO PRODUCTION TERMINAL BEHAVIOR OR PHYSICAL R7
ACCEPTANCE**.

## Outcome

The four segments frozen by
`2026-09-02-knife16-r7-hard-terminal-extraction-architecture-amendment.md`
are locally closed:

- the oversized Knife16 provenance block moved to one private child;
- one code-aware expected RED locked the future production child route and
  symbol ownership;
- the complete hard-terminal Implementation moved behavior-preservingly from
  `continuation.rs` into one deep private production child; and
- the obsolete `continuation.rs` grandfather entry was removed only after the
  implementation, regression, provenance, governance, and review evidence was
  green.

This document is the authoritative dated evidence owner for that A-D
implementation promotion. The architecture amendment remains the design and
scope contract; implementation evidence is not retroactively inserted into it.

## Scope and non-goals

The accepted change is a source-locality and Authority-local extraction only.
It preserves the existing terminal state, ordering, values, errors, `cfg`, and
caller behavior.

It does not prove or enable:

- production hard-terminal behavior;
- a production `OwnerPhysicalSessionRoot<T>`;
- controller/output or whole-root terminal exact zero;
- physical B topology, authenticated hint, runtime restart credit, attach,
  arbiter, or no-oracle composition;
- physical R7a-d or Task 4 completion;
- a public TUN route change; or
- VPS, macOS TUN, WAN, throughput, M2, or other external acceptance.

## Segment A: Knife16 provenance characterization Move

The existing continuation provenance test block moved behavior-preservingly
from the oversized integration-test parent into one private child:

```text
tests/knife16_owned_leg_provenance.rs
  3362 -> 2988 lines

tests/knife16_owned_leg_provenance/continuation.rs
  initially 383 lines
```

The migrated logical block was exactly `376` lines. Its recorded SHA-256 is:

```text
11917c7ea87d6fc5a9fcd16bee8270a534f28fc9ba596ca4e3de1f750691aef0
```

Because the child is declared from the integration-test crate root, the route
uses the explicit test-side path exception:

```rust
#[path = "knife16_owned_leg_provenance/continuation.rs"]
mod continuation;
```

The Module remains private. Test identity and assertions were preserved; this
Move changed neither production source nor runtime behavior.

Accepted Segment-A evidence:

- moved focused characterization: `1/1`;
- complete Knife16 provenance at that boundary: `158/158`; and
- the Knife16 parent exact debt was promoted from `3362` to `2988` only after
  the Move and checks were green.

## Segment B: production-child route and symbol-ownership RED

The private provenance child was extended with one code-aware tracer using the
existing trivia/literal-aware scanner. At its final A-D boundary the child is
`787` lines, below the `800`-line test-leaf limit.

The tracer freezes one direct production child named `hard_terminal` with all
of these properties:

- private;
- unconditional;
- external rather than inline;
- default-path routed;
- no source `include!` or inline-wrapper bypass;
- moved Implementation declarations and bodies present in the child;
- stable Interface/storage/mixed-inventory anchors retained in the parent; and
- moved Implementation declarations and bodies absent from the parent.

Before Segment C, the focused tracer produced the one expected RED: the count
of the required production `hard_terminal` Module was found as `0`. Work stopped
at that expected boundary; no production repair preceded authorization of the
pure Move.

## Segment C: production pure Move

The complete existing hard-terminal Implementation moved from the production
parent into one private, unconditional, default-path child:

```text
src/owned_upstream/quinn_leg_composition/owner/continuation.rs
  1698 -> 971 lines

src/owned_upstream/quinn_leg_composition/owner/continuation/hard_terminal.rs
  779 lines
```

The moved closure contains:

- both terminal state forms and their inventory implementations;
- terminal correlation and quarantine cause;
- the terminal transaction guard;
- root terminal observation, begin, and failpoint method bodies;
- the transaction implementation and private settlement helpers; and
- `Drop`.

The parent retains the stable Interface and mixed Authority anchors:

- `QuinnOwnerControllerOutputRoot<T>` storage and construction;
- `controller_failed` and mixed output inventory;
- terminal observation vocabulary;
- terminal begin-error vocabulary; and
- terminal callback outcome vocabulary.

Every moved item retained its existing `cfg(test)` or `cfg(not(test))` behavior
and its old effective scope or a narrower one. No caller, test-only physical
adapter, owner root, or other production Module changed for this Move. It added
no re-export, pass-through, callback, parameter seam, source include, macro, or
production behavior.

The Segment-B tracer changed from the exact expected RED to `1/1` GREEN through
this Move.

## Segment D: shrink-ratchet promotion

After the production Move and accepted verification, the Integrator removed the
obsolete `continuation.rs 1698` grandfather entry. The production parent is now
under the normal `1500`-line ceiling, and the new child is also under that
ceiling. No replacement grandfather entry was added.

The code-growth governance source is `699` lines at this boundary.

## Final accepted evidence

Only the exact accepted evidence is recorded here; no unrecorded command line or
result is inferred.

- hard-terminal production-child tracer: `1/1`;
- continuation tests: `16/16`;
- terminal tests: `3/3`;
- owner physical-recovery tests: `2/2`;
- owner tests: `9/9`;
- physical-attach tests: `9/9`;
- Knife16 provenance: `159/159`;
- repository provenance: `13/13`;
- standalone code-growth governance: `6/6`;
- Cargo all-features code-growth governance: `6/6`;
- format check: green;
- diff check: green;
- `cargo check --lib`: green; and
- diagnostics contained only the existing vendored smoltcp `22` warnings.

The independent final review reported:

```text
P0/P1/P2 = 0/0/0
Verdict = Approve
Scoped structural quality = 10/10
```

The review found the state/inventory/correlation/cause/guard/root-method/
transaction/`Drop` closure complete, stable parent Interface and storage intact,
test/non-test configurations symmetric, effective visibility equivalent or
narrower, callers unchanged, and no Interface or behavior bypass.

## Exact accepted line inventory

```text
tests/knife16_owned_leg_provenance.rs                                  2988
tests/knife16_owned_leg_provenance/continuation.rs                      787
src/owned_upstream/quinn_leg_composition/owner/continuation.rs          971
src/owned_upstream/quinn_leg_composition/owner/continuation/hard_terminal.rs
                                                                          779
tests/code_growth_governance.rs                                         699
```

The Segment-A child was `383` lines immediately after its pure Move and reached
`787` only when the independent Segment-B route/symbol tracer was added. These
figures describe distinct accepted boundaries and must not be conflated.

## Acceptance boundary

This result closes only hard-terminal extraction A-D. The production child still
preserves the pre-existing behavior: the complete terminal transaction remains
test-gated and production retains its `Awaiting` placeholder. Therefore this
result is necessary-only architecture maintenance, not production terminal
reachability or terminal settlement evidence.

The next architecture decision is owned by
`2026-09-02-knife16-r7-post-extraction-owner-root-cutover-amendment.md`. That
document must place the production CurrentOnly root Authority before any
root-minted terminal capability consumer.

## Source links

- `docs/current/STATUS.md`
- `docs/tech/2026-09-02-knife16-r7-hard-terminal-extraction-architecture-amendment.md`
- `docs/tech/2026-09-02-knife16-r7-post-extraction-owner-root-cutover-amendment.md`
- `docs/tech/2026-09-01-knife16-r7-physical-root-move-and-architecture-convergence-local-results.md`
- `src/owned_upstream/quinn_leg_composition/owner/continuation.rs`
- `src/owned_upstream/quinn_leg_composition/owner/continuation/hard_terminal.rs`
- `tests/knife16_owned_leg_provenance.rs`
- `tests/knife16_owned_leg_provenance/continuation.rs`
- `tests/code_growth_governance.rs`
