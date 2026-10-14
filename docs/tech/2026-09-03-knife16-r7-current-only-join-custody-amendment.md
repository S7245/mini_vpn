# Knife16 R7 CurrentOnly join-custody amendment

Date: 2026-09-03.

Status: **ARCHITECTURE-ONLY INDEPENDENTLY ACCEPTED; THE CURRENTONLY RUST
IMPLEMENTATION IS NOT ACCEPTED, THE NEXT RUST REPAIR IS NOT AUTHORIZED, CASE C
REMAINS OPEN, AND THE PUBLIC ROUTE REMAINS LEGACY**.

## Scope and narrow supersession

This amendment preserves the accepted architecture and historical evidence in
`2026-09-02-knife16-r7-post-extraction-owner-root-cutover-amendment.md`. It
narrowly supersedes only that document's future assumption that structural
current-A join custody would be complete once a `JoinHandle` was stored in the
private CurrentOnly root.

The first constructor tracer has now exercised that design and exposed a
smaller, falsifiable lifecycle gap: the Running actor can leave its root slot
before the spawned join handle is published back into root state. Unwind in
that interval can lose exact custody. An abort-on-`Drop` implementation or
ordinary Tokio handle detachment does not prove ownership settlement.

This document freezes the correction. It does not rewrite the 2026-09-02
architecture record, promote the current Rust, repair the separate actor test
fixture, or authorize a later code slice.

## Grounded implementation and review stop

The separately scoped first CurrentOnly constructor tracer reached this local
writer evidence:

- the focused test
  `settled_initial_seed_enters_current_only_root_with_exact_shutdown_authority`
  failed first with the expected `E0433` missing production root seam;
- the minimum implementation then passed focused `1/1` and ten serial repeats,
  each `1/1`;
- the directly related owner-initial accepted-root tests passed `1/1` and
  `1/1`, continuation passed `16/16`, and join-FIFO inventory passed `2/2`;
- `owner.rs` remained exactly `2062` lines;
- the new production child `owner/physical_session_root.rs` is `756` lines;
  and
- the private `current_only.rs` test leaf is `402` lines.

These results are writer evidence, not an accepted implementation result. The
same independent Reviewer returned P0/P1/P2 `0/1/0`, Request Changes:

> Removing the Running actor from its slot before publishing the `JoinHandle`
> creates an unwind interval with no durable join custodian.

The review also rejected abort-on-`Drop` and normal task detachment as custody
or cleanup evidence. The implementation therefore remains stopped and must not
be used as production reachability, exact-zero, or root-lifecycle evidence.

### Independent amendment review

The first review of this amendment found one P1: its flat custody description
required the unique handle and the results obtained only after consuming that
handle to coexist. The phase-tagged ownership repair removed that contradiction.
The same independent Reviewer then reported no findings, P0/P1/P2 `0/0/0`,
Approve; the prior P1 is fully closed. Clean Architecture and System Design are
both `10/10`, with no remaining scoped gap.

This verdict accepts only the architecture contract. It does not accept the
current Rust prototype, authorize its A/B repair, close ultimate root
destruction in case C, or change the public route.

## Goal

Freeze one Authority-local model that:

1. keeps a living CurrentOnly root visibly nonzero through observer
   cancellation and join-admission unwind;
2. prevents retry, restoration to Running, or exact-zero publication after an
   uncertain admission disposition;
3. identifies the longer-lived custodian required before ultimate root value
   destruction can be safe; and
4. preserves exactly one lifecycle owner throughout every permitted transfer.

## Non-goals

- No Rust, test, governance, archive, or previously accepted architecture edit
  in this amendment slice.
- No claim that the current constructor Implementation is accepted.
- No production caller, initial-A Authority absorption, terminal preservation,
  no-ACK finalizer, physical B, runtime credit, arbiter, or public TUN wiring.
- No whole-root exact-zero or ultimate root-destruction acceptance.
- No repair of the independent ordered-delivery actor fixture failure.
- No macOS TUN, VPS, WAN, throughput, M2, commit, or push.

## Three distinct lifecycle cases

The following cases must not share one vague "cancellation" claim.

### A. Observer future cancellation while the root remains alive

The join task has been admitted and its exact custody is already stored in the
root before the observer reaches its first await. Dropping the observer releases
only observation. The same root must expose the same non-copyable custody for a
later observation to re-enter and complete.

Required disposition:

```text
root owns Joining custody
  -> observer awaits
  -> observer is dropped
  -> root still owns the identical Joining custody
  -> a later observer resumes that custody
```

This is recoverable and must not mint another join or restore Running.

### B. Join-admission unwind while the root remains alive

Before actor extraction, spawn, or any admission work that may unwind, the root
must install a typed, sticky, independently inventoried quarantine/custody
state. A non-escaping admission transaction may temporarily hold the exact
actor, but the root is never represented by an empty slot. If admission
unwinds, the installed root state remains permanently nonzero and uncertain.

Required disposition:

```text
Running actor
  -> root installs typed admission quarantine/custody
  -> exact actor enters non-escaping admission work
  -> unwind before JoinHandle publication
  -> root remains sticky nonzero and cannot retry or report zero
```

This is fail-closed evidence, not successful cleanup. It must not reconstruct
the initial seed, restore Running, detach work as success, or normalize the
unknown task disposition.

### C. Ultimate destruction of the root value

The current private root has no production caller and no longer-lived
custodian. Its four-file local state machine can close A and B while the root
remains alive, but it cannot structurally prove what owns an admitted actor or
join task after the root value itself is destroyed.

This limitation is not a relaxation. Production exposure, caller cutover, and
promotion of the root as the unique production shutdown Authority are frozen
until an enclosing upstream session owner, session supervisor, or nursery
accepts the one consuming transfer described below. A unit test that merely
drops the root cannot substitute for that Authority.

## JoinCustody Authority

`JoinCustody<Phase>` is a future private, non-`Clone`, non-`Copy`, phase-tagged
owning capability. Every non-Running phase is represented by exactly one such
capability, whose payload contains only the ownership that exists in that
phase:

- `AdmissionPhase` owns the exact actor/session correlation, the actor awaiting
  admission, one root-minted sticky quarantine lease, and independently visible
  nonzero custody/inventory;
- `JoiningPhase` is produced by consuming the admission capability and owns the
  unique non-`Clone` `JoinHandle` together with the same correlation,
  quarantine lease, and nonzero inventory; no join result exists yet; and
- `JoinedPhase` is produced by consuming and awaiting that unique handle. It
  owns the exact nested outer task result and, when the outer result succeeded,
  its exact inner actor join result, plus the correlation, quarantine lease,
  and remaining inventory. It no longer owns or claims the consumed handle.

At every instant exactly one location owns the capability:

```text
CurrentOnly root XOR enclosing parent custodian
```

No second lifecycle owner, join Interface, token factory, or copied identity
may recreate it. Each phase change consumes the entire previous capability and
produces exactly one next capability; a phase payload is never flattened into
simultaneously owned actor, handle, and results.

The following are forbidden as custody or settlement:

- detaching or forgetting a join handle;
- aborting a task on `Drop` and calling the task settled;
- ignoring either the outer or inner join result;
- synthesizing zero inventory or a delivered/application ACK;
- reconstructing an initial seed after CurrentOnly commit;
- restoring Running after an uncertain admission disposition; or
- adding a second caller-facing join path.

## Living-root A/B repair contract

The next Rust repair is not authorized by this amendment. If separately
authorized, it may close only A and B inside the existing private CurrentOnly
Module.

### State and ordering

The root must always contain one explicit state. There is no empty/sentinel
state that can be mistaken for zero:

```text
Running { exact actor }
  -> AdmissionQuarantined { custody: JoinCustody<AdmissionPhase> }
  -> Joining { custody: JoinCustody<JoiningPhase> }
  -> Joined { custody: JoinCustody<JoinedPhase> }

Admission unwind
  -> AdmissionQuarantined remains sticky, nonzero, and non-retryable
```

Before spawn or admission begins, the complete admission-phase custody must be
published into root state. Only its consuming transition may move the exact
actor through spawn and replace it with the unique handle in joining-phase
custody. That next capability must be back in root custody before any await or
observer cancellation point; an `Option` or empty-state transition is
forbidden.

The repair must delete the destructive abort-on-`Drop` cleanup claim. A root
that still lives must retain and expose custody; a root whose destruction is
possible remains blocked on case C rather than acquiring a detach or abort
bypass.

### Deterministic acceptance tracers

Only these two new behavior tracers belong to the first repair:

1. **Observer cancellation:** block an admitted join, drop the observer future,
   prove the root retains the same custody and nonzero inventory, re-enter with
   a second observer, and consume both outer and inner results exactly once.
2. **Admission unwind:** install quarantine, trigger a deterministic pre-spawn
   failpoint panic, catch the unwind while retaining the root, and prove the
   root stays permanently nonzero, cannot retry admission, and cannot report
   exact zero.

Neither tracer may destroy the whole root and call that case C closure. Neither
may use sleep, abort, detach, ignored results, synthetic delivery, or a
test-only scalar as Authority evidence.

### Proposed future write scope

If authorized, the living-root repair remains limited to:

- `src/owned_upstream/quinn_leg_composition/owner/physical_session_root.rs`,
  only the private actor/join state, custody, inventory, failpoint, and
  structural-shutdown symbols; and
- `src/owned_upstream/quinn_leg_composition/owner/physical_session_root/tests/current_only.rs`,
  only the two deterministic tracers and their private fixture glue.

The existing private route in `owner.rs` and one-line test index require no
behavior change. `owner.rs` must remain at or below `2062`, the production child
at or below `1500`, the test index at or below `300`, and the test leaf at or
below `800`. No governance inventory edit, visible Interface, re-export,
`pub(super)` diffusion, callback, parameter, dynamic dispatch, global lock,
per-record allocation, macro, or `include!` bypass is permitted.

## Case C: enclosing-custodian prerequisite

Before any production caller can receive or expose `OwnerPhysicalSessionRoot`,
an independent architecture slice must identify the enclosing upstream session
owner, session supervisor, or nursery that outlives the root value. That parent
must accept `JoinCustody` through one consuming move when root ownership ends.

The transfer contract is:

1. root and parent never hold two copies or split one phase payload;
2. the transfer consumes the single `JoinCustody<Phase>` in its current phase:
   admission carries the exact actor, joining carries the unique handle, and
   joined carries the exact nested outer/inner outcome; every phase also carries
   its correlation, quarantine lease, and nonzero inventory;
3. parent settlement consumes the handle once when joining, then accounts for
   the exact outer result and its inner actor result when present; it never
   fabricates an inner result after outer failure or retains a consumed handle;
4. failure to prove the transfer leaves production exposure and caller cutover
   disabled; and
5. the transfer is part of the single lifecycle Authority, not a second join
   Interface.

Only that longer-lived ownership path can make ultimate root destruction
auditable. The current local root cannot close it by adding a `Drop`
implementation, spawning detached cleanup, aborting the task, or leaking a
handle.

## Ownership and capacity inventory

The existing constructor transaction and static capacity math remain
unchanged. This amendment adds no queue width, restart credit, timer, or
per-record allocation.

During join admission, the audit must distinguish at least:

- current actor custody;
- actor-contained FIFO messages and independently summed/stored bytes;
- ordered-delivery wait ownership;
- join-admission quarantine lease;
- admitted `JoinHandle` ownership while in `JoiningPhase`;
- the exact nested outer/inner join outcome after entering `JoinedPhase`;
- retained handoff, controller, executor, and output-root Authority; and
- endpoint/session Authority.

Actor messages/bytes may reach zero while join or retained root Authority is
still nonzero. A/B acceptance therefore proves exact custody or sticky nonzero
quarantine, never whole-root zero. The accepted static witness remains
`N = 2 * (F + Z)`, `Q = 32`, `A = S + R`, and
`max_scan_turns = ceil(N / Q) * A`, all with checked arithmetic. Runtime restart
credit remains explicitly `NotInstalled`.

## Independent actor fixture debt

After the direct CurrentOnly regressions passed, the older actor exact test

```text
real_loopback_actor_marks_ordered_delivery_only_after_peer_admission_and_mints_join_only_after_clean_await
```

failed with:

```text
ResidualOwnership { messages: 1, bytes: 0 }
```

This is a separate test-fixture debt. It represents a pending undelivered
ordered wait and is consistent with the fail-closed join contract; it is not
the Reviewer's join-admission P1 and must not be folded into the A/B repair.

A later independently scoped, behavior-preserving slice may move that exact
fixture into `join_fifo_inventory.rs`, let the Integrator ratchet any shrink,
and then settle the real `LocalCancelled` terminal plus FIFO ownership through
the existing exact cleanup path. This amendment makes no such edit or
acceptance claim.

## Failure tree and falsifiers

### Observer cancellation

Prediction: after the first observer is dropped, the identical admitted
custody remains in the living root and a later observer completes it.

Falsifier: a second join is minted, Running is restored, a handle detaches, or
inventory decreases merely because observation was cancelled.

### Admission unwind

Prediction: a pre-spawn failpoint after quarantine publication leaves a sticky
nonzero root that rejects retry and exact-zero publication.

Falsifier: unwind leaves an empty slot, reports clean zero, restores Running,
or permits another spawn.

### Ultimate root destruction

Prediction: no local four-file proof can close this branch without a
longer-lived unique custodian.

Falsifier: a design claims root `Drop`, task abort, or detached execution is
exact custody without showing a consuming parent transfer and both join
results.

### Duplicate lifecycle ownership

Prediction: at most one of root or enclosing parent owns `JoinCustody`.

Falsifier: two handles, two join entry points, copied tokens, or independent
cleanup paths can act on the same actor.

### False zero

Prediction: admission quarantine, joining, retained results, or a pending
ordered wait always contributes visible nonzero inventory.

Falsifier: actor message/byte zero alone makes the root report whole-root zero.

## Gates and stop rule

This amendment is independently accepted. Its acceptance does not authorize a
Rust repair; the later A/B slice remains separately gated.

For a later A/B repair, the required order is:

1. focused RED for observer cancellation, then minimum GREEN;
2. focused RED for admission unwind, then minimum GREEN;
3. serial repeats and directly affected constructor, owner-initial,
   continuation, actor join/FIFO, and structural-shutdown regressions;
4. Knife16 and repository provenance;
5. format, tracked/untracked diff, and library check;
6. standalone and Cargo all-features code-growth governance; and
7. the same independent Reviewer over Authority Locality, no-empty-state,
   inventory, cancellation, unwind, and scope.

Any unexpected compile/test/format/check/governance failure, required write
scope expansion, visible Interface growth, second lifecycle owner, or attempt
to treat A/B as C stops the Writer. The independent actor fixture debt also
stops its own future repair on the first new red.

Case C requires its own accepted architecture and deterministic ownership
tracer before production exposure or caller cutover. No green A/B result can
waive that gate.

## R7 status

The constructor tracer remains a necessary-only, unaccepted local prototype.
It does not close production terminal preservation, root-owned no-ACK
settlement, authenticated B topology, runtime credit, physical R7a-d, Task 4,
or whole-root exact zero. The public TUN route remains
`LegacyOwnedUpstream`. macOS TUN, VPS, WAN, throughput, and M2 remain forbidden.

## Source-of-truth links

- `docs/current/STATUS.md`
- `docs/tech/2026-09-02-knife16-r7-post-extraction-owner-root-cutover-amendment.md`
- `docs/tech/2026-09-02-knife16-r7-hard-terminal-extraction-local-results.md`
- `docs/tech/2026-09-01-knife16-r7-physical-root-move-and-architecture-convergence-local-results.md`
- `src/owned_upstream/quinn_leg_composition/owner.rs`
- `src/owned_upstream/quinn_leg_composition/owner/physical_session_root.rs`
- `src/owned_upstream/quinn_leg_composition/owner/physical_session_root/tests/current_only.rs`
- `src/owned_upstream/quinn_leg_actor/tests/mod.rs`
