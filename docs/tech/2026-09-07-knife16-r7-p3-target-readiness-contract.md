# P3 — Target readiness and bounded notification contract

Date: 2026-09-07.

Status: **CONDITIONAL PROTOCOL AND DOCUMENT CLOSURE ACCEPTED; NO RUST WRITE SCOPE**.

## 1. Scope and decision

Deepen the existing static TargetIo Interface, not a new provider. Retain
per-Flow demanded readiness and add one bounded shared-notification operation
on that same Interface. Both operate under the actual Executor/Controller/
output Authority selected by the
[source-service decision](2026-09-07-knife16-r7-p3-owner-source-service-decision.md).
No Target/socket/Driver getter, callback Interface, dynamic future, task,
worker or reverse server_root dependency is introduced.

This refines that decision's section6: a named socket poll alone is insufficient
for MemoryTarget shared buffer pressure and Target terminal-capacity pressure.
The separate
[Owner source-credit contract](2026-09-07-knife16-r7-p3-owner-credit-rearm-contract.md)
stays unchanged. Source byte/segment/FIFO permits and Target buffer/tombstone
capacity are distinct resources and must not share a fictitious release event.

The [scheduler amendment](2026-09-04-knife16-r7-target-open-ready-scheduler-amendment.md)
still owns four classes, five Session sources, F Target positions, O<=F opens,
two send cells and Z terminal capacity. The notification operation is one
finite OwnerContinuation subcategory, not a sixth Session source or new ready
lease. Concrete categories/K, Layout/cold peak, caller binding and Rust symbol
scope must be frozen together before behavior work. This is a protocol choice,
not implementation approval or full P3/Case C/R7/Task4 acceptance.

## 2. Source facts and rejected shortcuts

Current [TargetIo](../../src/owned_upstream/target.rs) has a synchronous
readiness snapshot at523; only MemoryTarget and test delegates implement it.
The snapshot is diagnostic, with no Context or retained registration.
MemoryTarget's definition at1429 must not become a default poll implementation:

- A queued WouldBlock directive makes its writable snapshot false, but only
  an actual write at1063 consumes that finite directive. Waiting for an
  external edge there deadlocks the deterministic trace.
- write acceptance is constrained by both per-Flow and shared buffered bytes.
  A different Flow's read/drain/reset/cancel/expiry can unblock a writer.
- joinable omits the shared tombstone limit. Actual join at1483 can still
  return PendingTerminalCapacity; retirement at1506 may unblock another Flow.
- ordinary graceful terminal does not automatically discard written bytes;
  cancel and expiry differ. Metadata cannot stand in for actual release.

The independent source audit additionally found that harness take_written
at two_leg_harness2631 is outside the Owner turn and relies on explicit
schedule_owner_followups. Owner drain_pending_target_joins at5202 scans all
Flows into a Vec; separate immediate/expiry retirements do not uniformly pass
through it. Neither is evidence of bounded release-driven readiness. The
future bound route must replace that reachable scan with actual ready selection,
not retain it behind the new poll and claim the whole operation is bounded.
Directive-count decreases have no independent shared-capacity waiter and do
not justify another notification domain.

Pinned Tokio1.52.0 source provides independent read/write Waker slots:
`net/tcp/stream.rs:557,869`, `runtime/io/scheduled_io.rs:305`. It registers then
rechecks readiness under its waiter exclusion; dropping a borrowing poll does
not own or remove those slots. Notification takes Wakers under exclusion and
invokes them after releasing it (`scheduled_io.rs:238`). `try_read/try_write`
use the public try-I/O path; WouldBlock clears the observed readiness tick,
not a newer event (`registration.rs:183`, `scheduled_io.rs:359`).

However `registration.rs:144` checks Tokio cooperative budget before reactor
registration. A plain Pending may instead have queued a deferred Waker;
`task/coop/mod.rs:350,396` and `scheduler/defer.rs:15` show that separate path.
It can grow a runtime Vec of distinct Wakers. Neither an actual socket waiter
nor zero per-poll allocation can be inferred from plain poll_read_ready alone.

Rejected alternatives:

1. Snapshot-to-Pending: loses real wakeups and misclassifies directives/Z.
2. Wake every Flow on shared release: O(F) callback work on a read/cleanup
   call, including blocked unrelated Flows; a hidden notify-all does not fix it.
3. Relay shared release through one arbitrary Flow's Waker: closure, credit
   blocking or disabled interest can stop the notification chain. Its progress
   must not depend on that chosen Flow being serviced.

The selected operation keeps shared capacity, registrations and incremental
notification together in the Target Adapter. Deleting this deepened operation
would spread Adapter-specific release knowledge into Owner callers. MemoryTarget
and TCP remain two concrete Adapters at the existing inward Seam.

## 3. Proposed static operation shape

These are protocol signatures, subject to the later coupled symbol freeze:

```rust
enum TargetReadyInterest { Read, Write, Join }
enum TargetReadyOutcome { Attempt, Failed(TargetFailure) }
// On the existing TargetIo trait:
fn poll_readiness(&mut self, flow: SessionFlowId,
    interest: TargetReadyInterest, cx: &mut Context<'_>)
    -> Poll<Result<TargetReadyOutcome, TargetIoError>>;
fn poll_ready_notifications(&mut self, cx: &mut Context<'_>)
    -> Poll<Result<(), TargetIoError>>;
```

No default Pending or snapshot fallback. Test delegates forward both methods
to their actual MemoryTarget. Read/write/terminal operations and cold inventory
remain on TargetIo; split-phase begin/poll/finish/open_inventory is not replaced.
The current synchronous open route is still deleted only at its full cutover.

Named polling arms/rechecks only one demanded interest. Pending retains the
registration in the Adapter/reactor, not in a temporary future. Read and Write
must not overwrite each other; Join is separate logical settlement readiness.
Attempt allows one later bounded operation, not guaranteed bytes or capacity.
An unused writable socket never schedules Read. Operational socket failure
returns Failed for the existing owning failure transaction, not an invariant
Err and not an endless Attempt/WouldBlock loop. Err retains existing business
ownership and reports an Adapter invariant; validate before changing registers.
Unwind is not modeled rollback and follows the entered-turn quarantine.

The shared operation first installs/replaces its sole retained continuation
Waker, then performs at most one candidate notification quantum. Ready(Ok)
means that quantum was consumed; the Owner keeps this subcategory runnable for
the next budgeted check. Pending means idle after registration/recheck. It is
never an instruction to poll all Targets. Its Waker is armed before any named
poll can establish a shared-capacity wait; the real owning turn enforces that
ordering, not Main or an external caller. Cold Controller pairing starts no
Target operation or publication.

## 4. TCP direct-poll restriction

Each Read/Write readiness quantum invokes exactly one public poll_read_ready
or poll_write_ready. Wrap only that one primitive in a stack-pinned concrete
`tokio::task::coop::unconstrained(poll_fn(...))`, polling it once and dropping
the temporary borrow. `task/coop/unconstrained.rs:16` restores the enclosing
budget on return/unwind. This introduces no stored/dynamic future or task.
It bypasses only the hidden cooperative pre-registration branch, not the
Owner's explicit class/source/byte budgets. Do not wrap a loop, whole Target
operation, root future, DNS state machine or application callback this way.

The proof obligation is one finite readiness primitive per admitted unit and
a bounded enclosing root poll that actually yields. If those cannot be proved,
stop rather than enabling unconstrained execution. The concrete Adapter must
also audit any other readiness entry for cooperative deferral separately.
There is a distinct build-variant prerequisite: Tokio's optional unstable
Linux taskdump trace_leaf can also return Pending and defer the Waker before
registration; unconstrained does not bypass that branch. Current Cargo.toml
requests full/test-util, not taskdump. The future exact resolved-feature/build
gate must exclude active taskdump capture from this proof; enabling it requires
separate registered-defer/boundedness treatment, not a claim of socket waiting.
Relevant source is Tokio lib.rs569 and runtime/task/trace/mod.rs292, with the
complete cfg predicate at macros/cfg.rs509. No build flags change in this stage.

Use only that generation's controlled non-owning publisher Wakers; no arbitrary
caller Waker is passed into the socket. Their clone/drop cannot run business
code or allocate; publication obeys the already accepted first-fault ordering.
No second reader/writer task may replace the reactor's latest directional
registration. Successful read/write keeps useful work behind the fair cursor;
WouldBlock consumes that attempt and requires another budgeted readiness arm.
EOF/terminal follows the real terminal transaction, not repeated reads.

## 5. MemoryTarget demanded readiness

Polling is side-effect-free with respect to bytes, directives and terminal
ownership. For Read, queued bytes or an undelivered EOF allows Attempt; an
empty live readable queue retains its Waker. feed_read/finish_read commit
their actual change before notifying Read. A poll never consumes EOF or data.

For Write, a finite queued WouldBlock or Zero directive allows one Attempt:
the subsequent write consumes exactly that directive, preserving old test
semantics. A queued AcceptAtMost still requires positive real buffer capacity.
After an actual directive is consumed, remaining finite work may rearm; empty
buffer pressure alone does not. No synthetic directive is inserted as a wake.

Join attempts require the real retained terminal, zero owned buffers/directives
and an available terminal position (or the already-joined idempotent result).
PendingOwnedState and PendingTerminalCapacity retain the exact Flow; they are
not clean zero. Each actual drain/terminal/retirement mutation triggers the
appropriate local or shared recheck. Existing reset/cancel/expiry semantics
are preserved, including retention by ordinary graceful terminal.

The old snapshot can remain diagnostic for existing tests, explicitly distinct
from attempt eligibility. Do not rewrite existing fixtures to report writable
or joinable merely to make the new polling trace pass.

## 6. Bounded shared notification

Each Adapter retains shared-release request/pass state, one continuation Waker,
and waiting Read/Write/Join metadata within its existing F-position envelope.
Shared buffer decreases request a Write recheck; actual terminal retirement
requests Join recheck. Per-Flow-only changes target that Flow. A release before
registration is observed by arm's state recheck; after registration it dirties
the retained request before waking the shared continuation. No failed probe,
unchanged/Absent retirement or speculative rollback requests a new pass.

Preallocated summaries and a persistent circular cursor select one registered
candidate per shared quantum, without iterating the Flow BTreeMap. Wake at most
the three bounded interest registrations of that candidate. Commit cursor and
notification disposition first, then invoke Wakers outside all Target/ready
guards. A shared release does at most constant notification work and never
walks all waiters. A full pass still costs O(F) total budgeted work.

New releases during a pass set dirty/request; they never reset the cursor.
After at most F candidates and one end check, restart only for a newer request.
New waiters behind the cursor have checked the later resource state; a later
release requests the next pass. Repeated notifications coalesce on the same
Flow lease. A candidate's external readiness/credit/closure cannot stall the
pass: the common continuation drives it independently. Named rechecks may
return Pending again if another Flow consumed capacity; no reservation is
promised by wake or Attempt.

Mutation, notification and error paths must be closed together. A normal
recoverable release finalizes its request before returning even on Err; no
post-Ok-only notification. Unwind retains the same exact owned disposition
and sticky nonzero quarantine. A Waker failure cannot discard later candidates
or normalize the active pass to idle. No extra fault authority is introduced.

## 7. Owner routing, generations and park

The existing F-position Target cell has separate demanded/hinted operation
bits, not three owning leases. A registered named Waker carries immutable
session/slot/scheduling/Flow generation and operation correlation. Publish the
named bit before ancestors/root epoch, retaining dirty changes during service.
It never releases source credit or performs I/O. Credit-blocked Read can retain
a hint but cannot reserve/read or sustain pressure-only retries. A poll hint
and a later source attempt are separately charged units.

The shared continuation Waker publishes only its actual OwnerContinuation
subcategory. Clearing must observe its active pass/request as well as named
Target bits. Register-before-recheck, checked revision, acquire clear/recheck
and arm/scan/epoch root park remain mandatory at both levels; no second socket
poll is hidden in a zero-budget recheck. A dropped borrowing observer owns
neither registrations, source cells, notification pass nor Controller result.

Terminal/retirement disables inappropriate interests before releasing their
registrations. Late callbacks cannot reopen DATA, reactivate a stale occupant
or consume a new generation's bits. Old immutable publisher backing and in-flight
callbacks must be included in cold coexistence/retirement capacity; mutating an
old Waker's key to reuse its allocation is forbidden. This is a remaining
physical-Lifecycle/Layout freeze obligation, not a present boundedness proof.

Shutdown closes production first. Named Read/Write hints no longer admit live
DATA; terminal/tombstone notification work migrates with legal Settlement
work. Root destruction, whole-root no-ACK zero and P4–P7 are not closed here.

## 8. Capacity and deterministic acceptance

No additional source/command/socket/lease domain is minted by notifications.
Actual storage is not free: F slots' interest state/Waker registrations,
summary words, pass/cursor state, the shared Waker and old-generation backing
must be checked and charged in physical Layout/cold peak. Z remains terminal
only; waiting Join stays in its existing F position, never a Z ready slot.

One notification pass is at most F+1 subcategory visits. A release during an
old pass needs at most 2*(F+1) such visits. Subsequent named probe and actual
operation have their own fair queues and units; the earlier five-source bound
alone proves neither their end-to-end bound nor throughput. Concrete K_cont,
operation counts, byte caps and checked products are still a coupled freeze.
No guessed K/default, saturating capacity or unmeasured performance claim.

Future vertical tracers must use actual bound Controller/Executor/output turns,
MemoryTarget and genuine queued data/permits, with no manual ready insertion
after the triggering event. Port-specific loopback tests additionally prove
the pinned TCP registration behavior; they are not whole binding evidence:

- registered empty Read -> drop borrowing observation -> real feed -> reentry;
- independent Read/Write; idle writable cannot spin Read; spurious wake then
  WouldBlock -> registered quiet Pending;
- queued one-shot WouldBlock consumed once, then bounded progress or true wait;
- B frees global buffer while A waits; common pass wakes A without a full scan;
- A waits for Z while B's real tombstone retires; Absent gives no fake release;
- release before/during/after clear, release during pass and new waiter behind
  cursor; another blocked Flow or retired generation cannot stall notification;
- EOF once, graceful retained bytes, cancel versus expiry, shutdown late wake;
- entered mutation/publish unwind, first-fault retention, stale Waker/reuse and
  observer cancellation; no false zero;
- real TCP directional registration after one bounded primitive even with
  exhausted Tokio cooperative budget; no task/dynamic/per-poll allocation.

Before Rust: independently accept this protocol; freeze actual shared/Flow
selector/guard/cold-pair Interface, all TargetIo delegates, complete Moves for
oversized parents and exact allowed files/symbols. Then one real vertical tracer
and minimal GREEN at a time, scoped regressions/provenance/governance, format/
diff/check and independent review. No commit/push or external TUN/VPS/WAN/M2.

## 9. Evidence status

Read-only source checks and the draft are recorded under
`/tmp/mini-vpn-p3-target-readiness-ptZukx/`; immutable entry matches the prior
accepted seven documents and all336 Rust files. No Rust/test/Cargo or governance
change has occurred. `primitive-audit.json` fingerprints six pinned Tokio
sources; it is source evidence, not a compile or runtime test.

Same Heisenberg's first independent review reported P0/P1/P2=0/0/1,
Request Changes solely for the drain_pending_target_joins source anchor.
That factual anchor is corrected5086→5202; protocol behavior is unchanged.
No protocol P0/P1 or additional necessary optimization was found. Scoped
Module/Clean/System/DDIA8/9/7/8; the remaining implementation proofs in
sections7–8 are required for10/10. Same-Reviewer final protocol/six-document
closure returned0/0/0 Approve, independently checking336 unchanged Rust,
HEAD/index, six fingerprints and21 local links. The separate §4 taskdump
prerequisite review also returned0/0/0 Approve against docs-review-v2.json.
Only factual pending→accepted transcription was permitted without another
review. This acceptance grants no Rust scope or full-P3 completion.
