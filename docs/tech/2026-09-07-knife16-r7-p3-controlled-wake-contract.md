# P3 — controlled wake and generation retirement contract

Date: 2026-09-07.

Status: **CONDITIONAL PROTOCOL ACCEPTED; NO RUST WRITE SCOPE**.

## 1. Accepted conditional decision

The [binding preflight](2026-09-07-knife16-r7-p3-binding-preflight.md) remains
accepted source evidence. Its raw-drop Notify candidate is not the solution.
Keep standard Arc-backed `Wake`: clone/drop remain reference operations with
no business notification. Reclaim only after actual Owner-side unregister
completion or a controlled consuming wake has released its real reference.
Exclude hidden cooperative defer from the finite bound ports. Separate
Owner-local metadata publication from unlocked outer notification.

This is conditional on the complete finite holder/caller proof below, not a
claim that arbitrary Wakers eventually invoke wake instead of drop. In
particular no unknown normal Drop may be declared fatal to save this design.
No weak ticket export, immutable-key rewrite, raw vtable, helper task, timer
polling or trusted-runtime caller is introduced. The selected Controller /
Executor / output ownership and current four/five scheduler sources remain.

This refines the [Target readiness contract](2026-09-07-knife16-r7-p3-target-readiness-contract.md)
and [Owner-local credit contract](2026-09-07-knife16-r7-p3-owner-credit-rearm-contract.md).
The accepted notification-unwind classification in section5 is an explicit
addition to the existing [ready first-fault contract](2026-09-07-knife16-r7-p3-ready-failure-authority-plan.md),
not an error branch already implemented by current Rust.
Full P3, Case C/R7/Task4 and later P4–P7 remain open. No commit/push/external.

## 2. Finite holder proof and a real missing edge

The source audit used locally pinned Tokio1.52.0 and standard-library code;
Mencius independently traced the four classes below. These are current-source
facts plus explicit future obligations, not runtime acceptance of Retiring.

| Registration | Ordinary Drop / normal consuming release | Required closure |
|---|---|---|
| Exclusive TCP direct Read/Write slot | Owner replacement/unregister clears the slot; reactor moves its Waker to a lock-external WakeList and normally consumes it with wake | Only direct named slots, no async readiness list/extra reader; post-unregister check and consuming-release notification |
| Actual Driver mpsc receivers | Owner replaces registration; send and last Sender Drop consume the cached Waker | Owner-only producers; full Driver registration destruction, not close alone; exclude cooperative defer |
| Paired mailbox capacity observer | Owner resets/destroys cached registration; broadcast normally consumes the one Waker | Keep exclusive observer and registration-before-lease destruction; no second observer or external normal releaser |
| MemoryTarget demanded Read/Write/Join plus common | Registration storage does not exist yet | Future Adapter must implement bounded owned slots, controlled notification and exact unregister; no present proof is claimed |

Tokio mpsc `chan::recv` polls `trace_leaf` and `coop::poll_proceed` before
installing `rx_waker`. Budget exhaustion calls `context::defer`, which clones
the Waker into `Defer`'s Vec. Current-thread `CoreGuard` owns that Context;
normal block_on completion can drop the cached Vec without consuming wake.
If this is the last outside reference after retirement's failed uniqueness
check, no retirement edge follows. This is a source-permitted conditional
counterexample, not a runtime reproduction in the not-yet-built Owner binding.

Do not reuse the preflight's WakeList unwind case as a normal-path example:
when all controlled callbacks return normally, its remaining range is empty.
Fault-path closure is separate and mandatory in section5.

## 3. Only finite primitives exclude hidden defer

Extend the already accepted Target primitive restriction to the actual Owner
Driver and selected inbox receive polls. Poll one stack-pinned concrete
`unconstrained(poll_fn(...))` once, then destroy that borrowing wrapper. It
covers only the selected finite receive primitive, never a root poll, drain
loop, arbitrary future, DNS iteration or whole effect/Controller turn.

Control's existing RESET-then-Control selection is at most two receiver polls;
Ordered is one. An inbox unit polls only its selected lane/prefetch. Pull and
send stay separate charged units. Existing try-only producer/receipt-before-
publication requirements remain. Client/public mixed receive is unchanged.
Root budgets and yields must remain independently enforced; unconstrained
is not permission to exceed B_session=1 or silently retry Pending.

The existing taskdump/trace build prerequisite remains mandatory: unconstrained
does not bypass `trace_leaf`. The resolved feature/build graph must exclude
active hidden defer/capture or a separately reviewed bounded registered path
must replace this choice. A version/feature change invalidates the proof.
There is no fallback to a noop Waker or an unregistered quiet Pending.

## 4. Publication belongs to its actual call context

Driver DATA, CLOSE, RESET and sink-completion sends invoke receiver Wakers
while `PortState::admission_gate` is still held. `try_send` removes awaited
producer paths, not this synchronous receiver callback. Relevant bodies are
[tcp.rs](../../src/owned_upstream/tcp.rs) `commit_normalized`, `commit_close`,
`send_ordered_close`, `send_reset` and completion commit; Tokio Chan.send
pushes, then calls AtomicWaker.wake synchronously.

**Owner-local publisher:** its controlled Driver/mailbox Waker updates only
non-owning correlated ready/dirty metadata and the existing first-fault gate.
It calls no Notify, user Waker, Target I/O or reducer. Metadata exclusion must
never call back into a source/Target primitive; preserve one-way lock order.
The same actual Owner turn finalizes publication to outer summaries/epoch
after the source call and all its admission/primitive guards have returned,
before either Ok, normal recoverable Err or Pending can escape. No new source
poll, mailbox send or callback wrapper is hidden in finalization.

That deferral is legal only because the actual paired senders, Driver, source
Flow and normal releasers are inside the owning turn. No normal out-of-turn
producer/last-sender destruction is permitted by this binding. A discovered
one falsifies the classification; it cannot be patched with a missed-wake
scan or by calling it fatal. Existing generic TCP progress remains unchanged.

**External Target publisher:** a normal reactor wake may occur while root is
parked. It publishes exact named/retirement metadata, ancestors and epoch,
then signals outside its own metadata and dependency guards. It performs no
business operation or synchronous root work. MemoryTarget's future owning
mutations/shared pass must defer notification through their actual Owner turn;
an external feed Adapter must release its actual guards before calling the
controlled publisher. An arbitrary blocking/reentrant caller Waker is not
certified as harmless; the contract removes internal callback-under-lock paths,
not every possible caller-created deadlock.

The final binding must prove local-to-process correlation and one shared
first-fault Authority without inward imports of server_root, generic callback
ports or a per-session shadow error that permits another root admission.
Current private type placement does not already supply that wiring. It remains
a coupled cold/Interface freeze obligation, not a getter/visibility grant.

## 5. Normal wake, notification fault and inert Drop

For a controlled external consuming `Wake::wake(self: Arc<Self>)`:

1. Copy the immutable old key and safely retain only metadata-root association.
2. Release the real ticket Arc before any candidate publication. Do not keep
   a hidden temporary ticket Arc. No ticket field is accessed afterwards.
3. Under lifecycle exclusion, publish only if the old key still owns the slot;
   live interests and Retiring candidate bits have different meanings.
4. Unlock before signalling. The ordinary completion is the retirement edge;
   it cannot be published before the reference is released.

`wake_by_ref` releases no reference and supplies no retirement-complete claim.
Its lifetime must terminate through the actual finite holder table: Owner
replacement/unregister or a later consuming wake. No arbitrary off-turn holder
is added merely to make a test convenient. Late old-generation callbacks never
reuse a new occupant's ready bits. Metadata corruption still follows the
existing root-fatal model, not a poison recovery shortcut.

**Conditional fault containment:** guard the shared outer notifier's existing
first-fault Authority across its unlocked `Notify::notify_waiters()` call.
Freeze **at most one registered waiter on this shared Root Notify**: Root owns
one cached `OwnedNotified`; reset destroys the old registration before arming
its replacement. No second observer, overlapping cache or registration export
is allowed. The current `OwnerServerRoot::from_commands`/`poll_ready_unit` path in
[server_root.rs](../../src/owned_upstream/quinn_leg_composition/owner/server_root.rs)
supplies the source witness, not permission to widen this cardinality later.

The two WakeLists have different stack positions. Notify first removes its
sole registered Waker; the inner WakeList advances past that Waker before
invoking its consuming wake. If user wake unwinds, **inner Notify cleanup runs
before the outer notifier guard**, but no unconsumed Waker remains in that
inner list. The guard then commits typed notification-unwind to the same
first-fault gate **before the calling reactor's outer WakeList cleanup** can
drop its remaining same-root controlled Wakers. Zero waiters invokes no user
wake. A second inner waiter would invalidate this ordering proof and is
forbidden, not covered by the guard. This lemma does not cover future local
aggregators, Target callback lists or extra observers; each needs its own
cardinality and same-root coverage proof before binding.

The guard may perform only bounded atomic failure/dirty publication; it must
not lock, notify again, allocate, drop an arbitrary panic payload or pretend
successful wake. Prior first cause wins. No separate fault list, swallowed
error or retry is added.

This is fault containment, not a promise that arbitrary user panic never
propagates. The live retirement proof ends on this genuine notifier failure:
ordinary root admission is closed and all still-referenced positions remain
owned/nonzero. Dependent Drop of other *same-root controlled* Wakers is inert
reference release. It cannot restore live reuse or synthetic zero. Do not
generalize that conclusion to unrelated Wakers in a shared runtime list.
No future poll/liveness is promised after a broken caller never re-enters;
normal source I/O failure remains session-local and cannot use this escape.

This narrowly replaces the raw-drop proposal's impossible transfer of a
normal wake proof into arbitrary destructor context: no callback runs on
ticket Drop at all. Review must explicitly accept the fault classification
and prove the single-waiter inner Notify lemma plus the relevant outer
reactor list's same-root controlled coverage, or reject this candidate before
Rust. Swallowing arbitrary panic with catch_unwind is not a substitute for
ownership, cardinality or list coverage.

## 6. Exact retirement, not reference-count polling

A retiring position still consumes its original F position; it is not a Z
tombstone or a new queue. Disable publication/registration for the old
generation, complete actual Target/Driver unregister/destruction, and eliminate
all Owner temporary Wakers before the initial retirement check.

Root retains each actual ticket's owner Arc; no Weak of the ticket is ever
exported (Weak metadata-root association is distinct). Use a real uniqueness
operation, not `strong_count()==1`. `try_unwrap` failure returns the same Arc;
`get_mut` may prove uniqueness but may not rewrite the key. When all tickets
are truly unique, destroy their old backing synchronously before publishing
the slot vacant and minting a new checked generation. No new old-generation
clone is allowed after that proof. Unknown destruction remains nonzero.

Failure retains the same Retiring position, then waits for its actual next
consuming-release edge; no time bound is promised while a real reference is
held. Initial checking covers the case where all references were already gone.
Candidate release during check/clear must leave dirty state or defeat the
clear revision check. Checked counters, acquire recheck and root arm/scan/epoch
park remain necessary. A failed check cannot erase a newer release edge.

Count allocations separately from leases and clones. Three named Target and
two Driver ticket objects per Flow imply a candidate upper bound5*F, not five
owning Flow leases. Shared/common and paired mailbox tickets are separately
session-scoped. Actual object choice, registrations, Layout, cold coexistence
and checked products remain unproved; no literal5*F grants extra allocations.
The static-open timer/DNS/attempt callbacks and session/root destruction have
separate unclosed holder proofs; this contract does not close all P3 publishers
or P6/P7 by analogy. Whole-P3 acceptance must include them explicitly.

## 7. Required falsifiers and implementation stop

Before any Rust grant, complete and independently review the actual
cold/guard/K/Layout/symbol freeze. Then deterministic
tracers must exercise real bound Controller/Executor/output operations:

- Exhaust Tokio coop before the actual Driver poll; Pending has the intended
  receiver registration and no defer-held ticket; drop observation/re-enter.
- During actual DATA/RESET/completion publication, attempt the old lock-reentry
  scenario; external notification occurs only after admission lock release.
- F=1 retirement waits for a real in-flight controlled wake; it consumes the
  reference, wakes root, reclaims and only then admits the next generation.
- Unregister with no in-flight reference reclaims without waiting for a wake;
  a retained real reference keeps pressure, without repeated polling.
- Candidate release before/during/after clear and old wake after slot reuse;
  no missed edge, stale activation, key rewrite or extra backing.
- Shared Root Notify retains at most one registration across cache reset;
  no second observer/export can add a waiter. Actual user-wake unwind exhausts
  the inner Notify list before guard failure publication, which precedes the
  remaining outer same-root Waker cleanup. Source-error isolation and prior-
  first-cause behavior survive; extra/local/Target lists are not inferred safe.
- MemoryTarget named/common registration, cancellation, buffer/Z release and
  actual unregister mirror the same contract, not fake manual ready insertion.

Missing normal release, deferred clone, mixed-root callback list, callback
under a source guard, post-Ok-only finalization, shadow fault Authority, new
provider/task, or failure to bound real backing falsifies the candidate.
Stop its behavior work for architecture review rather than adding another
local workaround. Existing source, output, capacity and growth gates remain.

## 8. Evidence / review status

`/tmp/mini-vpn-p3-controlled-wake-86Ev3O/entry.json` verifies the prior accepted
seven-file packet and captures1151 repository files/338 Rust plus HEAD/index.
`primitives.json` fingerprints12 local primitive sources; it is not a runtime
test. The source-audit lookup initially used an absent session/progress.rs;
rg --files corrected it to existing session.rs plus session/tests/progress.rs,
without a file or behavior repair. No Rust/Cargo or external test was run.

Initial independent review: P0/P1/P2=0/1/0, Request Changes. The sole P1 was
the missing single-waiter premise and distinction between inner Notify and
outer reactor cleanup. The section5/7 doc-only repair freezes both. Same
Reviewer re-review: P0/P1/P2=0/0/0, Approve; scoped Module/Clean/System/DDIA
8/9/7/8. Heisenberg verified audit-repair-anchor.json, unchanged six companion
documents/338 Rust/governance/HEAD/index, and permitted factual seven-document
promotion without another review. The corrected actual symbol anchor is part
of that reviewed packet; the first Request Changes remains recorded above.
audit-final.json records final scope/fingerprints and document checks.
Only conditional protocol/document closure is accepted. Coupled binding is
NOT FROZEN; actual holders/cold/guard/K/Layout and runtime evidence remain open.
No Rust/governance/commit/push change, implementation grant, runtime boundedness
or full-P3 completion is claimed.
