# Code-Growth and AI-Navigability Governance Local Results

Date: 2026-09-01

## Decision

Future Rust additions and modifications are now governed before the existing
oversized files are refactored. This stage changes repository policy and CI
only; it does not change data-plane behavior or claim progress on physical R7,
public TUN, WAN, throughput, or M2.

The executable authority is `tests/code_growth_governance.rs`. The stable
design contract is in the `Code growth and AI navigability gate` section of
`AGENTS.md`.

## Frozen contract

- New production Rust files are limited to `1,500` lines.
- New test leaves are limited to `800` lines.
- New test `mod.rs` indexes are limited to `300` lines.
- The `28` pre-existing oversized production files and `22` pre-existing
  oversized test files have exact, shrink-only line baselines. New debt paths
  are forbidden.
- The existing `48` code-aware `clippy::too_many_arguments` suppressions across
  `8` production files have an exact, shrink-only baseline. New suppressions
  are forbidden.
- Every current-repository `.rs` file is classified, including explicit or
  default positive-`cfg(test)` Module targets. Vendored, generated,
  tool-worktree, and immutable archive trees are not current source.
- Every `#[path]` target must resolve to a governed `.rs` file, source
  `include!` is forbidden, and broad `clippy::all` or `allow(warnings)` bypasses
  are rejected through direct, `expect`, and `cfg_attr` forms.
- A feature Writer may report a shrink but may not change the debt inventory.
  The stage Integrator lowers or removes the exact entry in the same promoted
  change. Raising a value or adding a path is not an accepted repair.

Line count is only the executable structural trigger. The accompanying review
contract requires Module Depth, Authority Locality, one Writer per overlapping
Module, symbol-first bounded reading, coherent transaction locality, and a
behavior-preserving extraction before behavior change. A shallow pass-through
split, `pub(super)`/callback/parameter sprawl, or hot-path indirection does not
become acceptable merely because a file gets shorter.

## TDD and cutover evidence

The initial RED runs were performed with each inventory absent:

- production scanning reported all `28` existing files above `1,500` lines;
- allowance scanning reported the existing `8` files and `48` occurrences;
- test scanning first reported `21` path-classified test files above their
  applicable `300`- or `800`-line limit; and
- an intentionally unsorted first inventory failed its sorted/duplicate-free
  invariant before the baseline could be accepted.

The first provisional production snapshot raced an already active R7 Writer in
the shared checkout. The gate correctly rejected three changed files instead
of silently normalizing them. The Integrator waited for the in-flight coherent
slice, observed the R7 test extraction settle into a `382`-line parent and a
`106`-line private leaf, and froze the final pre-cutover production debt:

- `owner_session_controller.rs`: `3,620` lines;
- `quinn_leg_composition/owner.rs`: `2,754` lines; and
- `quinn_leg_composition/owner/continuation.rs`: `1,698` lines.

These are cutover debt, not desired sizes. From this accepted snapshot onward,
none of the exact values may be raised.

The first independent review then found three P2 coverage gaps: an external
cfg-test Module could receive the production limit, `#[path]`/`include!` could
escape the fixed roots, and test/broad lint suppressions were not scanned. A
shared code-aware scanner was extracted green-to-green from the existing
repository provenance test (`6/6`). Focused RED fixtures locked all three
cases, and the real repository RED discovered the pre-existing
`src/quic_path_probe.rs` as a `1,235`-line cfg-test leaf. It became the exact
22nd test debt entry before final promotion; this corrected pre-acceptance
classification is not permission to add future debt.

Follow-up review extended those same classes to nested `cfg_attr(path)`, test
context inherited through inline Modules and inner attributes, renamed
`include!` imports, and Clippy paths containing whitespace/comments. Each
received a focused RED before the code-aware scanner was extended.

## Green evidence

```text
rustc -D warnings --edition=2024 --test tests/code_growth_governance.rs -o <temp>/code-growth-governance
<temp>/code-growth-governance
  6 passed

cargo test --all-features --test code_growth_governance
  6 passed

cargo test --all-features --test repository_test_module_provenance
  6 passed
```

The Cargo runs emitted the already known `22` vendored smoltcp warnings. They
did not produce a mini_vpn governance failure and were not changed in this
stage. The strict standalone command passes; applying repository-wide
`RUSTFLAGS=-D warnings` to Cargo is rejected by those same vendored warnings and
is not claimed as new green evidence.

Targeted rustfmt and whitespace/diff checks pass for every governance file.
The final governance test, shared scanner, and existing provenance consumer are
`707`, `740`, and `435` lines respectively, all below the applicable `800`-line
test-leaf limit. The CI workflow also passes a standalone YAML parse.
At the initial cutover, repository-wide `cargo fmt --all -- --check` was blocked
only by an import-ordering difference in the concurrently owned R7
`quinn_leg_composition/owner/continuation.rs`; this governance stage did not
edit that business Module. R7 later closed that formatting difference, and the
post-cutover ratchet advancement below passes repository-wide format and diff
checks.

## Review and limits

The first independent read-only review reported no P0/P1 and three P2 coverage
gaps. Follow-up review found the deeper variants named above. After their
focused RED/GREEN repairs, the final read-only closure review returned
**Approve** with unresolved P0/P1/P2 `0/0/0` and confirmed the existing
repository provenance target remained intact.

The scanner is deliberately a bounded structural lexer, not a full Rust
compiler. Unsupported `#[path]` syntax fails closed. The line ratchet still
cannot prove good Module boundaries or Authority ownership by itself; those
remain mandatory plan/review gates in `AGENTS.md`.

Initial-cutover SHA-256 provenance, before later shrink-only advancement:

```text
3dff79540beb2c330a4224ff8da5f1beab84783cfb9c4559e407dbdee72c90bd  tests/code_growth_governance.rs
f50655ca89b4221ae98a7871c258aa31c5ff117d6afb0431ed49162abc72adab  tests/support/rust_source_scan.rs
89388971e504e22b4ef459aa38f330c214f0fcbd40b7995ed39b4b1ebcce0380  tests/repository_test_module_provenance.rs
c3949ffc42d3a747a06b98bb6d57bb1fd4ed492dcf99e6791ce90f8bc5e22e26  AGENTS.md
7f6cc64936f02a365e0986e74a1daa31e3722e969df46302aab2ff65c6ddfa13  .github/workflows/ci.yml
```

## First post-cutover ratchet advancement

The first governed R7 Move Implementation reached a reviewed green boundary
without adding a debt path. The exact executable gate first failed closed on
the observed shrink and required the stage Integrator to lower only these
entries:

- `quinn_leg_composition/owner.rs`: `2,754` to `2,062` lines; and
- `r7_physical_attach_permit.rs`: `1,552` to `1,426`, then to `1,407`
  lines after exact test-fixture cleanup.

The new private production child `owner/physical_root.rs` is `702` lines and
the new private test cleanup child is `195` lines, both within their normal
limits. The feature Writer did not edit the governance inventory.

Post-move evidence is standalone/Cargo code-growth `6/6`, repository test
layout `6/6`, physical-attach `9/9`, legacy-B `2/2`, terminal transaction
`5/5`, Knife16 provenance `158/158`, `cargo check --lib`, repository-wide
rustfmt, and diff checks. Cargo continues to emit only the known `22` vendored
smoltcp warnings.

Independent review initially returned one P1 because two otherwise-green tests
discarded actor join results and dropped retained physical authority instead of
using the exact fixture cleanup path. The repair settled both scalar-equal and
exact arbiters, settled the pressure-retained `AttachPending` stage, and made
terminal/stamp/join observation mandatory. The same Reviewer then returned
unresolved P0/P1/P2 `0/0/0` and **Approve**.

Executable-governance SHA-256 at this first ratchet advancement:

```text
dc9852c95add13aae1196c14132cbe8a7408b59362132d9e89ed9d0c9639e25c  tests/code_growth_governance.rs
```

## Second post-cutover ratchet advancement

The next R7 capacity stage moved three existing owner-scan tests into the
private `two_leg_harness/tests/r7_capacity/progress_scan.rs` child without a
behavior change. The parent `r7_capacity.rs` shrank from its exact `984`-line
baseline to `942` lines; the new child is `50` lines. The focused owner-scan
tests pass `3/3`, format and targeted diff checks pass, and the Integrator
lowered only that existing test-debt entry.

Standalone and Cargo code-growth both pass `6/6` after the ratchet update. The
general repository provenance gate does not yet recursively freeze this nested
capacity Move; that known gap belongs to a separate net-zero or shrinking
provenance slice and is not claimed as evidence for the Move here.

Executable-governance SHA-256 at this second ratchet advancement:

```text
680cb78243536e5892ab75ac25c618c9ffe6dcf43b13f6130d52918212b398a2  tests/code_growth_governance.rs
```

## Third post-cutover ratchet advancement

Symbol-first review before the first capacity-width behavior tracer found two
remaining groups of scan-only assertions inside mixed oversized-parent tests.
A supplemental behavior-preserving Move reconstructed the same witnesses and
assertions as two private tests in `r7_capacity/progress_scan.rs`; it did not
move the surrounding wire, timer, storage, or formula assertions and added no
helper or visibility seam.

The parent `r7_capacity.rs` shrank from `942` to `936` lines and the private
child grew from `50` to `82`, still below the normal test-leaf limit. The
focused capacity set passes `19/19`, format/diff checks pass, and standalone and
Cargo code-growth pass `6/6` after the Integrator lowered only the parent entry.
Independent read-only review returned unresolved P0/P1/P2 `0/0/0` and
**Approve**. The previously recorded nested-provenance limitation remains open
and was not mixed into this Move.

Executable-governance SHA-256 at this third ratchet advancement:

```text
fcb3ff331e3a2f1c80d6b5c8cf9e1a929eaf341a8de5af633f5fdeee38c962a3  tests/code_growth_governance.rs
```

## Fourth post-cutover ratchet advancement

The first static R7 owner-scan width tracer changed the manifest derivation
from a scalar `2*F` assumption to typed flow/tombstone capacity `2*(F+Z)`.
The follow-up static attempt bound then checked `S+R` before multiplying by the
typed owner-scan width. These are capacity-math results only; they do not claim
a production restart-credit reserve/return/commit transaction.

Behavior-preserving interface convergence removed four private-child-only
shallow getters and made `R7OwnerScanCapacity::derive` consume the typed episode
witness directly. The production `two_leg_harness.rs` therefore shrank from
`3,433` through `3,431` to `3,423` lines; `r7_capacity.rs` remains `936` and
the private `progress_scan.rs` child is `148` lines. The Integrator lowered
only the production debt entry.

Evidence is the exact S+R tracer `1/1`, real-manifest tracked-flow addition
overflow `1/1`, full R7 capacity `22/22`, owner-progress regression `1/1`,
standalone/Cargo code-growth `6/6`, format/diff/check green, and independent
review with unresolved P0/P1/P2 `0/0/0`, **Approve**. Nested provenance and
runtime credit/root-ledger work remain separate open slices.

Executable-governance SHA-256 at this fourth ratchet advancement:

```text
10e4656acea2e5ca2eeba957c7f8a0c8ef2d808fa069a2705ba330f372587d81  tests/code_growth_governance.rs
```

No commit, push, external host run, or completion notification is claimed.
