# Knife16 R7 hard-terminal extraction architecture amendment

Date: 2026-09-02.

Status: **ARCHITECTURE-ONLY AMENDMENT INDEPENDENTLY ACCEPTED; SEGMENT A
PROVENANCE CHARACTERIZATION MOVE IS NEXT; NO PRODUCTION RUST MOVE OR BEHAVIOR
CHANGE IS ACCEPTED**.

## Scope and supersession

This is a narrow amendment to the **Behavior-preserving extraction gate** and
exact implementation sequence item 4 in
`2026-09-01-knife16-r7-physical-root-move-and-architecture-convergence-local-results.md`.
It does not rewrite that result's accepted evidence, physical-root design,
capacity ledger, or later R7 sequence.

The old literal extraction wording required the complete terminal state,
caller-facing types, guard, and implementation to move into a private child
while also forbidding every visibility adjustment. A symbol-first Rust privacy
audit falsified that combination: a parent cannot name a child-private storage
type, and an owner sibling cannot use a child-private transaction returned by a
method. Re-export, pass-through, callback, or parameter workarounds would have
made the architecture shallower and are still forbidden.

This amendment supersedes only that impossible literal boundary. It separates
the stable terminal Interface that callers and root storage must know from the
deep terminal Implementation that they must not own.

Physical R7a-d, Task 4, public TUN, WAN, throughput, and M2 remain incomplete.
The B-only recovery adapter remains `#[cfg(test)]`.

## Decision

`owner::continuation` remains the stable controller/output Authority seam. It
keeps the terminal names and storage/inventory anchors that its parent and
physical-root sibling must know:

- `QuinnOwnerControllerOutputRoot<T>` and its `terminal` storage field;
- construction, `controller_failed`, and mixed `output_inventory` logic;
- `QuinnOwnerControllerOutputTerminalObservation`;
- `QuinnOwnerControllerOutputTerminalBeginError`; and
- `QuinnOwnerControllerOutputTerminalCallbackDrive`.

The new private, unconditional, default-path child
`owner::continuation::hard_terminal` owns the complete private terminal
Implementation:

- both existing `cfg(test)` and `cfg(not(test))` terminal state-machine forms
  and their inventory implementations;
- terminal correlation and quarantine-cause vocabulary;
- the terminal transaction guard;
- root terminal observation, begin, and test failpoint method bodies; and
- the transaction implementation, private settlement helpers, and `Drop`.

The child Module declaration is unconditional so the existing non-test
`Awaiting` storage implementation remains production-compiled through the
default path. Every moved item retains its exact current `cfg(test)` or
`cfg(not(test))` attribute. This Move does not ungate terminal behavior.

The parent uses a private import for the child-owned state type. It does not
re-export the child or add a forwarding method. The child directly implements
the parent-owned root and directly accesses its ancestor-private fields, which
is permitted by Rust privacy and preserves one `&mut root` transaction.

## Effective visibility contract

Visibility is judged by the set of Modules that can name or call an item, not
by whether the source spelling changes from `pub(super)` to `pub(in ...)`.

Let:

- `O` be `crate::owned_upstream::quinn_leg_composition::owner` and descendants;
- `C` be `O::continuation` and descendants; and
- `H` be `C::hard_terminal` and descendants.

No moved item may become visible outside its old effective scope.

| Symbol group | Old location/scope | New location/scope | Proof |
|---|---|---|---|
| test and non-test `QuinnOwnerControllerOutputTerminalState` | parent private / `C` | child `pub(super)` / `C` | The spelling changes only because the defining Module moves; the effective Module set is identical. |
| both state `inventory` implementations | parent private / `C` | child `pub(super)` / `C` | Parent mixed inventory can call the method; no owner sibling gains access beyond the old `C` set. |
| `QuinnOwnerTerminalCorrelation` and `QuinnOwnerTerminalQuarantineCause` | parent private / `C` | child `pub(super)` / `C` | They are payload types of the `pub(super)` state variants; matching scope preserves the old set and avoids a `private_interfaces` violation. |
| Observation, BeginError, and CallbackDrive | parent `pub(super)` / `O` | unchanged in parent / `O` | These are stable sibling-facing Interface anchors. |
| terminal transaction guard type | parent `pub(super)` / explicitly nameable in `O` | child `pub(in crate::owned_upstream::quinn_leg_composition::owner)` / inferred value and methods usable in `O`, explicit path capped at `C` by the private child | Existing callers infer the return type and retain exactly their old operations; explicit type-path reachability strictly shrinks. |
| `terminal_observation`, `begin_terminal_transaction`, and the test failpoint method | parent `pub(super)` / `O` | child implementation with `pub(in crate::owned_upstream::quinn_leg_composition::owner)` / `O` | Method bodies move; signatures and effective caller set do not. |
| guard entry/callback methods used by owner/physical root | parent `pub(super)` / `O` | child `pub(in crate::owned_upstream::quinn_leg_composition::owner)` / `O` | The explicit path compensates for one extra lexical Module level without widening the Interface. |
| guard-only settlement helpers and `Drop` body | parent private / `C` | child private / `H` | Callable scope shrinks; automatic drop semantics are unchanged. |
| root field, constructor, `controller_failed`, and mixed `output_inventory` | parent private or existing `pub(super)` / unchanged | unchanged in parent | These anchors combine terminal state with controller/output Authority and must not be split merely for line count. |

Allowed visibility changes are limited to the scope-preserving or narrowing
child spellings and operation scopes shown above. In particular, the state and
its private payload vocabulary share scope `C`; making the payloads narrower
would fail `private_interfaces`, while making them wider would expand the old
scope. Any crate-wide/public widening, re-export, additional callable method,
or change outside `O` is Interface growth and stops the Move.

## Deletion tests

The Module boundary must satisfy both directions:

1. Deleting `hard_terminal.rs` removes the state machine, transaction type,
   root terminal method bodies, settlement helpers, and `Drop`. The parent has
   no duplicate implementation or forwarding fallback, so its private state
   import and all terminal call sites fail to compile.
2. Deleting the parent Interface anchors removes the root storage type and the
   Observation/BeginError/CallbackDrive vocabulary. The child and the owner /
   physical-root callers then fail to compile; the child is not an alternate
   Interface root.

These are architectural deletion tests, not instructions to delete files in a
gate run. Source provenance locks the same facts without destructive actions.

## Falsifiable failure tree

### F1 — parent cannot name child state

Prediction: moving the state enum as child-private makes the parent root field,
constructor, `controller_failed`, and mixed inventory fail privacy checking.

Closure: child `pub(super)` restores exactly scope `C` for the state, its
inventory method, and its correlation/quarantine payload types; no re-export or
parent wrapper is introduced.

### F2 — owner sibling cannot use the returned guard

Prediction: a child-private guard or child-relative `pub(super)` methods make
the physical-root transaction call chain fail privacy checking.

Closure: the guard and its caller-facing methods use explicit owner-scoped
visibility, preserving the existing inferred-value call chain throughout `O`.
Because the child Module stays private, explicit guard path nameability shrinks
to `C`; no current caller names it. Private helpers remain in `H`.

### F3 — stable outcome vocabulary migrates into Implementation

Prediction: moving Observation, BeginError, or CallbackDrive would require a
re-export, changed call path, or edits in `owner.rs` / `physical_root.rs`.

Closure: those declarations remain in the parent unchanged. Provenance proves
that the child consumes them but does not redeclare or export them.

### F4 — line-count extraction splits Authority

Prediction: moving root storage, `output_inventory`, controller/output fields,
or adding pass-through methods makes terminal and mixed inventory independently
owned.

Closure: only implementation blocks and their private state vocabulary move.
The one root and one exclusive guard borrow remain unchanged.

### F5 — an unconditional route accidentally ungates behavior

Prediction: removing item-level `cfg(test)` would make recovery-B terminal
behavior production reachable.

Closure: the Module route is unconditional, but every moved state variant,
guard, root method, implementation, and `Drop` retains the exact current item
attributes. The non-test placeholder remains only `Awaiting`.

### F6 — provenance passes through text or route ambiguity

Prediction: comments, literals, inline Modules, `#[path]`, multiple child
declarations, or source `include!` could make a text-only test report a false
route.

Closure: the route tracer uses code-aware Module metadata and AST declarations,
requires exactly one named private/unconditional/default-path child, and locks
declaration/implementation ownership rather than whole-file strings or total
test counts.

## Frozen A-E sequence

Each segment has one Writer for its overlapping Module. A later segment cannot
begin until the preceding segment is at its reviewed green boundary, except
that segment B deliberately produces the one focused RED that segment C closes.

### A. Knife16 provenance characterization Move

Goal: shrink the grandfathered Knife16 provenance parent before adding a new
structural assertion.

Allowed writes:

- `tests/knife16_owned_leg_provenance.rs`: add only one private default-path
  `mod continuation;` declaration and remove the exact moved test body.
- new `tests/knife16_owned_leg_provenance/continuation.rs`: compact Module docs,
  `use super::*;`, and the unchanged test
  `owner_continuation_authority_is_private_and_binds_one_controller_output_root`.

The current test block is lines 822-1197 inclusive (`376` lines). The expected
pre-rustfmt direction is parent `3362 -> about 2988` and child `about 383`, well
below the `800`-line test-leaf limit. Exact post-rustfmt counts own the gate.

The test's fully qualified route intentionally changes from the crate root to
`continuation::owner_continuation_authority_is_private_and_binds_one_controller_output_root`.
Its function symbol, body, assertions, and focused substring remain unchanged;
no wrapper preserves the old route.

Acceptance:

- characterize the original symbol and focused result before moving;
- run the moved exact test and the complete Knife16 provenance target;
- run format, tracked/untracked diff checks, and standalone code-growth;
- accept only the expected shrink-only Knife parent ratchet RED; and
- after the Integrator records the exact smaller count, rerun standalone and
  Cargo all-features governance and obtain read-only review.

Stop if the Move changes an assertion, requires a public/re-exported helper,
changes any production file, yields a test failure other than the predicted
fully qualified route change, or requires the feature Writer to edit the
governance inventory.

### B. Code-aware hard-terminal route RED

Goal: add one falsifiable provenance tracer before production movement.

Allowed write:

- `tests/knife16_owned_leg_provenance/continuation.rs` only.

The child may load the already governed scanner through the exact
repository-closed test-only route
`#[path = "../support/rust_source_scan.rs"] mod rust_source_scan;`. The test
reuses that scanner and the existing parent `syn` helpers; neither scanner nor
oversized parent is modified, and the child must remain below `800` lines. It
must prove:

- exactly one external Module named `hard_terminal` is declared directly by
  production `continuation.rs`;
- that declaration is private, unconditional, non-inline, and uses the default
  `continuation/hard_terminal.rs` path;
- no source `include!` or inline wrapper supplies the implementation;
- child declarations/impls contain the two state forms and inventory methods,
  correlation/cause, guard, root terminal methods, guard implementation, and
  `Drop`;
- parent declarations retain the root storage/mixed-inventory anchors and the
  three stable outcome/error enums;
- parent AST contains no moved state/correlation/cause/guard declaration and
  no moved terminal or guard implementation body; and
- the old and new effective scopes in the table above are exact.

The focused test is expected to RED only because the production child route and
symbols do not yet exist. If it compiles incorrectly, passes without proving
the route, or fails for a different reason, stop before production editing.
Do not freeze the total Knife16 test count or a whole-source byte string.

### C. Production pure Move GREEN

Goal: satisfy the segment-B tracer by moving implementation without changing
behavior.

Allowed writes:

- `src/owned_upstream/quinn_leg_composition/owner/continuation.rs`;
- new
  `src/owned_upstream/quinn_leg_composition/owner/continuation/hard_terminal.rs`.

Allowed source movement from the current `continuation.rs` is exactly:

- the two terminal state declarations and inventory implementations, currently
  lines 111-219;
- correlation and quarantine cause, currently lines 221-237;
- the terminal guard declaration, currently lines 305-308;
- root `terminal_observation`, `begin_terminal_transaction`, and
  `arm_terminal_bind_postcommit_panic_for_test`, currently lines 375-479; and
- the terminal transaction implementation and `Drop`, currently lines
  886-1362.

The parent adds only the private unconditional `mod hard_terminal;`, a private
state import, and mechanical import routing. Observation, BeginError,
CallbackDrive, root storage, constructor, `controller_failed`, and
`output_inventory` remain byte-for-byte or rustfmt-equivalent in the parent.

The moved blocks total about `716` current lines. Expected post-rustfmt direction
is parent `1698 -> about 985` and child `about 730-780`; exact counts own the
gate. The child documents that it owns terminal state/transaction
Implementation, does not own the stable caller Interface or whole physical
settlement, and is tested by the continuation unit tests plus Knife16
provenance.

No `cfg`, match arm, ordering, state, error, value, or `Drop` semantic may
change. No edits are allowed in `owner.rs`, `physical_root.rs`, continuation
behavior tests, the scanner, repository provenance, documentation, or
governance inventory during this Move.

Acceptance:

- the exact segment-B provenance tracer becomes GREEN;
- all continuation unit tests, terminal transaction tests, directly affected
  physical recovery/terminal tests, complete Knife16 provenance, and repository
  provenance pass;
- standalone and Cargo all-features code-growth, format, diff, and
  `cargo check --lib` pass; and
- an independent read-only review finds no effective visibility expansion,
  duplicate implementation, Interface/pass-through growth, or behavior change.

Any non-provenance repair/regression RED stops writing. A privacy error that
cannot be closed by the exact scope-preserving-or-narrowing entries in the
visibility table also stops; it does not authorize another re-export, method,
callback, parameter, or caller edit.

### D. Shrink-ratchet promotion

The feature Writer reports exact line counts and stops at the coherent C
boundary. Only the Integrator may edit `tests/code_growth_governance.rs` to:

- lower the Knife16 provenance ratchet after A; and
- remove the `continuation.rs 1698` production debt entry once C leaves the
  file at or below the normal `1500`-line limit.

The new production child must remain at or below `1500`; it may not enter a
grandfather inventory. After each inventory-only promotion, standalone and
Cargo all-features governance must pass. Any requested increase, new debt path,
allowance, `#[path]` escape, or governance-contract change stops the sequence.

### E. Later terminal behavior RED/GREEN

This amendment authorizes no segment-E Rust files or symbols. A later behavior
slice must independently freeze its failure tree, one tracer, exact files and
symbols, capacity/ownership effects, and repair-test stop rule after the pure
Move is reviewed green.

It may not treat extraction as proof of production reachability, terminal
exact-zero, physical-root settlement, or R7 completion. It must preserve one
Authority-local settlement model across success, rejection, pressure, fatal,
cancellation, and shutdown.

## Non-goals

- No terminal state/order/error/value behavior change in A-D.
- No productionization of the current test-only recovery-B terminal path.
- No true physical-root construction, runtime restart-credit transaction,
  physical B probe/hint, arbiter wiring, or no-oracle harness in this sequence.
- No public TUN switch, UDP, R8/R9, owner/server, second ingress, external run,
  commit, or push.
- No governance relaxation or new lint allowance.

## Global stop rule

Stop immediately if a segment needs a wider write set, changes a non-goal,
requires effective visibility beyond `O`, duplicates Interface or Authority,
changes item `cfg`, or produces any unexpected repair/regression failure. Report
the evidence and a minimum revised plan; do not begin another repair without
the required user confirmation.

## Independent review

The same Reviewer who approved the prior impossibility stop reviewed this
amendment for Rust privacy, Interface depth, Authority Locality, deletion tests,
A-E ordering, source provenance, ratchets, and stop rules. The closure verdict
is **Approve**, P0/P1/P2 `0/0/0`.

The review specifically confirmed the `private_interfaces` payload scope, the
guard's preserved caller-operation scope plus narrowed explicit path, the
unconditional route with unchanged item-level `cfg`, both deletion tests, and
the exact A-E stop boundaries. This accepts the amendment and authorizes only
segment A. It is not implementation evidence for segment C or any R7
completion claim.
