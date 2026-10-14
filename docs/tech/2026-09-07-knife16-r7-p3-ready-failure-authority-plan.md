# P3 ready-failure Authority convergence

Status: DESIGN, STAGE A AND FIRST PUBLISHER BRANCH ACCEPTED; later tracers open.
Date: 2026-09-07. Full P3, Case C and R7 remain incomplete.

## Scope and evidence

The accepted [scheduler amendment](2026-09-04-knife16-r7-target-open-ready-scheduler-amendment.md)
§7 makes ready corruption and revision/epoch exhaustion root-fatal. The latest
[local result](2026-09-07-knife16-r7-p3-owner-progress-local-results.md)
accepts claim/finish root-fatal mapping and remaining-Full registration, not
publisher-fatal propagation. Current root/scheduler/dispatch leaf are 825/598/528
lines, with root47 and both governance6/6 accepted. HEAD is `532d766`; no push.

Read-only audit found `ReadySlot.fault` only reaches the root when that slot is
claimed. A completed B capacity publication can record exhaustion while the
next root visit still sends A. This is a second fault-propagation seam, so the
structural convergence gate requires one failure Authority and admission order
before another behavior patch. Ordinary source unknown remains session-local.

The active full-P3 goal permits causal repairs inside this frozen scope. It
does not permit P4 table/adoption, Target work in this slice, public cutover,
external execution, commit/push, new visibility, or governance inventory edits.

## One existing Module, one first-failure record

Deepen the existing private `server_root::scheduler` Module. Its `ReadyShared`
owns one private `ReadyFailureGate` containing one `AtomicU8`: zero is Open;
each nonzero assigned code represents one exact `ReadyFault`. Use explicit
safe encode/decode matches; an unknown nonzero code fails closed as Corrupt.
No unsafe conversion, allocation, new lock, task, callback or error list.

`fail(fault)` performs one strong `compare_exchange(0, code, AcqRel, Acquire)`.
Its successful RMW is the root-failure commit point. Losers observe the same
first recorded cause; no overwrite, clear, retry or counter saturation exists.
First means first successful failure publication, not wall-clock discovery.

`check()` loads Acquire. `admit()` performs one strong
`compare_exchange(0, 0, AcqRel, Acquire)`. This unchanged-value operation is a
real RMW in the same atomic modification order, not a load-then-act check.
If failure commits first, admission fails. If admission linearizes first, that
one already-admitted unit may finish its actual send/rearm disposition. Later
admissions fail. There is no promise to undo an already enqueued command.

The existing non-Clone `ReadyClaim`, Running cell phases and root `in_flight`
already own this unit. Do not add an admitted flag/counter or a second claim.
Within the existing slot guard, validate the selected source/phase first;
admit is the last fallible step before infallible Running/claim publication.
No source/receiver/Waker callback runs under that guard. Root stores the claim
before calling the existing complete source transaction.

Delete competing root-failure records: `OwnerServerRoot.terminal` and
`ReadySlot.fault`. Keep `CommandResident.fault` only for session-local source
failure. Expose only the root-scoped `ReadyIndex::check_root` and `fail_root`
operations; gate/codes/admission remain private. Do not export raw atomics or
introduce a generic fault bus, provider, re-export or test getter.

## Detection, publication, completion and park

1. Capacity publication records source/process revision failure or slot poison
   in the gate before Notify or any user Waker. Preserve surviving metadata;
   release the per-slot guard before signalling. No fault is only per-slot.
2. Missing/stale non-owning wakes, already session-quarantined cells and an
   empty capacity mask remain harmless notifications, not newly fatal events.
3. Root select/claim/finish/park/quarantine errors and impossible root dispatch invariants
   all use the same failure gate. Claim admission checks it even if publication
   races an earlier root check. Root entry checks it before source-unwind
   recovery, so a published root fault cannot become resident-only quarantine.
   `ReadyIndex::quarantine` becomes fallible: reject missing/poisoned metadata
   and commit that fault to the gate before any metadata mutation. Never recover
   a poisoned guard using `into_inner`. Check the gate after successfully acquiring
   the slot guard, before the first mutation: a same-slot publisher may have
   committed fatal while quarantine waited for that guard. The healthy path then
   infallibly quarantines only that resident's leases. Root validates the resident
   and performs this metadata transaction before adding its source-local fault
   or clearing `in_flight`; on error it retains the same claim and survivors.
   This is proven source-unknown settlement only when scheduler metadata is
   healthy, not a scheduler poison bypass or a second failure Authority.
4. `finish` must not reject merely because another slot has closed the gate:
   already-admitted work records its actual result and finishes intact metadata.
   Its own metadata failure still records the first root fault and retains the
   unfinished claim/actual command disposition. No synthetic rollback or zero.
5. Park retains the actual cached registration and checked epoch protocol.
   Include gate observation in its final empty/epoch check. A concurrent fatal
   event may yield bounded Rescan or Quarantined; it cannot admit another unit.
   A fatal event ordered after the final park check wakes the registered root.
   A panicking user Waker does not roll back the committed first fault; re-entry
   is fail-closed. No liveness guarantee follows a caller that never re-enters.
6. Root-fatal closes ordinary admission; this slice invents no finalizer. All
   unresolved commands, claims, capacity registrations and actual mailbox
   receipts retain their existing owners. Arbitrary Drop is not settlement.

## Alternatives and costs

- Keep per-slot errors plus a root flag: rejected because it retains competing
  fault ledgers and a check/admit race unless separately ordered.
- Hold one global mutex through sends: rejected because user callbacks can
  re-enter publication; it also adds global lock contention to source work.
- Two atomics or an admitted-bit state machine: unnecessary duplicate state;
  the existing claim already owns the unit. The one-word RMW supplies ordering.

The chosen design adds one inline atomic and one RMW per selected scheduler
unit, not per encoded record, plus a failure RMW on cold exceptional paths.
It adds no heap object, scan, DATA routing, dynamic dispatch or timer. Actual
struct padding/layout and aggregate cold-peak bytes must be measured at P3's
still-open physical-capacity gate; this document makes no TOTALBYTE or
throughput claim. Tradeoff: a common atomic cache line; concurrency/performance
measurement remains required before claiming external service capacity.

## A — pure characterization Move, separately green

Move the existing three real dispatch tests and their private fixture together
under the scheduler Authority so counter-boundary tests need no production
injection Interface. Allowed paths, relative to
`src/owned_upstream/quinn_leg_composition/owner/server_root/`:

- `tests/ready_scheduler.rs`: delete only `mod ready_dispatch;` (90 to 89).
- `scheduler.rs`: append only `#[cfg(test)] mod tests;` (598 to about 601).
- `tests/ready_scheduler/ready_dispatch.rs`: move the entire 528-line file,
  byte-for-byte, to `scheduler/tests/ready_dispatch.rs`; no fixture duplication.
- New `scheduler/tests/mod.rs`: private route plus imports only, below 300.
  Descendant access to existing private ancestors requires no visibility change.

Old FQ prefix:
`owned_upstream::quinn_leg_composition::owner::server_root::tests::ready_scheduler::ready_dispatch::`.
New FQ prefix:
`owned_upstream::quinn_leg_composition::owner::server_root::scheduler::tests::ready_dispatch::`.
The old leaf has no Rust caller beyond its route and no fixed provenance read.
Record identical file hash and exactly three tests before/after. The full root
filter becomes `owned_upstream::quinn_leg_composition::owner::server_root::`
(47 tests); the narrower old `server_root::tests::` filter would omit the move.
Run focused3, full47, provenance, formatting, scope/whitespace, check and both
governance gates. Review the coherent Move before behavior; no inventory change.

## B — one real publisher RED, then minimum convergence GREEN

After A is accepted, only root `server_root.rs`, `scheduler.rs`, moved
`scheduler/tests/ready_dispatch.rs` and a new private
`scheduler/tests/ready_dispatch/publisher_fatal.rs` may change. The moved leaf
adds its private child declaration and changes only the old `root.terminal`
observation to the converged failure operation. Its existing assertions and
fixture behavior remain fixed. Production files remain below 1500, test leaf
below 800, index below 300; all other Rust and inventory are frozen.

First exact tracer:
`publisher_revision_exhaustion_closes_admission_before_another_resident_send`.

Prefill B's mailbox with its second real OPEN, then use B's existing
`SessionCommands::drive_one_unit` before root construction to get the actual
Full return. Build real residents in [B,A] order; the constructor already
accepts that exact blocked command. The first root visit registers B's actual
capacity waiting; its persistent cursor next selects still-unsent A. In the owning private
test, set only B's live Control source revision to `u64::MAX`, then unlock.
Do not mint or change a lease/key/phase/mask/command. Dequeue the real prefill:
its installed capacity Waker executes the actual publisher and exhausts.
The next root poll must be Quarantined, never A MailboxOwned. Preserve B's
exact Full box/content and A's real Held command, both inbox observations and
the prefill receipt. Repeated polls stay stopped. Reduce the actual receipt.

Predicted RED is that next poll sends A, not a fixture/private-route/compiler
failure. Ranked alternatives: wrong cursor/registration (preconditions fail),
not a real capacity wake (no metadata exhaustion), or incorrect expected fault
scope (contradicted by accepted §7). Diagnose a feedback-loop error before any
production repair; do not weaken actual-custody assertions to get GREEN.

Minimum GREEN implements the single gate, deletes both competing root records,
orders publication/admission and keeps existing source transactions unchanged.
Atomic ordering is reviewed source proof until independently exercised below.

## Later vertical tracers, not first-test coverage claims

One at a time: process-revision exhaustion; actual slot poison; poisoned-metadata
source-unwind quarantine (retain claim, close admission); immutable first
cause; fatal during an already-admitted send (that send finishes its actual
result, then no new unit); Notify/Waker unwind after fault commit and root
re-entry; source-unknown isolation without root corruption; park/fatal ordering.
Use real mailbox receipts and callbacks where applicable, no sleep, copied
lease, synthetic notification rescue or fake clean teardown. Private revision
boundary injection tests arithmetic exhaustion without waiting 2^64 wakes.

Each coherent behavior runs exact plus justified repeats, full root and direct
mailbox/TCP/client regressions, Knife16 and repository provenance, fmt/diff/
all-features check, standalone `rustc -D warnings` and Cargo all-feature
governance, scope/hash audit, then the same independent Reviewer. Unexpected
red pauses for causal analysis; autonomous repair stays within this contract.
New Authority/Interface, third ownership seam, callback-under-guard, unknown
command discard, post-admission unrelated side effect or size escape stops the
slice for architecture review. No Target/Driver/fairness/lifecycle completeness
is inferred from this narrow fault-admission closure.

## Review ledger

Initial same Reviewer found P0/P1/P2 0/1/0, Request Changes: old quarantine
recovered a poisoned guard and cleared root `in_flight`, bypassing §7. The
single design repair above includes quarantine in the same gate, rejects
poison before mutation and orders local quarantine after metadata success.
CAS ordering, Stage A private Move and corrected tracer had no extra blocker.

Same Reviewer Heisenberg re-review: P0/P1/P2 0/0/0, Approve; scoped Module
Depth/Clean Architecture/System Design/DDIA 9/9/9/9. The quarantine P1 is closed
at design level; no new findings. Facts may be transcribed and Stage A executed
under the full-P3 goal. A must close green and be reviewed before B.
Design approval is not Rust acceptance. Exact evidence remains in the local
result; compact status is promoted only at a reviewed coherent boundary.

Stage A closure: same Reviewer P0/P1/P2 0/0/0, Approve for the pure Move.
Identical 528-line leaf hash, old route absent/new route unique, index15 and
scheduler601/parent89 verified; root825/other Rust/governance unchanged.
Before/after focused3 and root47, provenance162+13, fmt/diff/check and both
governance6/6 passed. Frozen Stage B may proceed under the existing goal.

First B review found 0/1/0: the implementation checked gate before taking the
slot lock, leaving a same-slot publish window. The minimum repair moves that
check below successful lock acquisition. Same Reviewer re-review0/0/0 Approve,
Module/Clean/System/DDIA9/9/9/9, permits factual acceptance transcription.
This does not expand the first tracer's runtime coverage to the quarantine race.
