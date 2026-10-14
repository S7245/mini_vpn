# P3 — construction order and local feedback custody

Date: 2026-09-07.

Status: **DIRECTION/DOCUMENT EVIDENCE ACCEPTED; COUPLED FREEZE OPEN; NO RUST GRANT**.

## 1. Scope and progress

The [controlled-wake contract](2026-09-07-knife16-r7-p3-controlled-wake-contract.md)
is accepted conditionally. This next stage resolves the construction order and
feedback placement needed by the actual binding; it does not claim the full
cold/guard/K/Layout/symbol freeze is complete. It preserves the selected
Controller/Executor/output Module, four process classes, five Session sources,
two send cells, F/O/Z separation and one process first-fault Authority.

The full-P3 goal permits autonomous causal design/repair in scope. Rust,
governance, P4–P7, public routing, external gates and commit/push remain closed
in this document stage. P3/Case C/R7/Task4 remain incomplete.
This is source evidence and an accepted direction, not a reproduced deadlock or
new green test. No further file-size-only Move is requested.

## 2. Actual dependency order

Today [Controller new_checked](../../src/owned_upstream/owner_session_controller.rs)
at1457 consumes Executor, clock and the Copy replay/session plan. Its capacity
failure returns those same inputs plus the exact cause. The factory was
already minted by Executor construction; a Controller rejection cannot return
a new pristine bootstrap or mint again.

By contrast [ReadyIndex::new](../../src/owned_upstream/quinn_leg_composition/owner/server_root/scheduler.rs)
at419 creates ReadyShared, its sole ReadyFailureGate, summaries and Notify.
The current [outer Root constructor](../../src/owned_upstream/quinn_leg_composition/owner/server_root.rs)
at582 invokes it after preparing the prototype's commands. Neither the inner
Controller nor Executor already receives this same metadata Authority.
Passing a naked Waker and a separately created gate does not prove shared
identity or close poison/exhaustion before another session is admitted.

**Select metadata preparation before checked Controller construction.** The
complete ready metadata Module must have an inward dependency location and
prepare the actual process-domain storage first. A cold, non-Clone session
reservation from that same domain supplies correlation, the shared gate and
the checked service limits. It is metadata authority, not a physical session,
Target socket, actual command or proof of P4/P5 adoption. Do not add a second
fault gate, an inward server_root import, a generic callback bus or an
independently callable post-publication installation operation.

The new reservation's name and private type placement below are illustrative;
the eventual complete Module move, access operations and final Root ownership
must still pass the coupled Interface/deletion test. Moving only a gate into
a shallow helper while leaving its identity/order obligations in callers is
not the selected deepening. Existing root claim/admission first-fault ordering
and the controlled-wake single Root Notify registration must survive.

## 3. Concrete constructor direction and exact returns

Retain the existing replay/session plan, rather than adding hidden defaults
to every bootstrap plan. The fourth input carries new, independently checked
positive M_ctl/M_data and 0<O<=F together with the same-domain reservation:

```rust
new_checked(executor: OwnerTargetExecutor<T>,
            clock: SessionMonotonicClock,
            plan: OwnerSessionCapacityPlan,
            ready: OwnerReadySessionReservation)
    -> Result<Self, OwnerSessionControllerBuildFailure<T>>;
```

This is a construction direction, not a callable signature or Rust grant.
The old [plan](../../src/owned_upstream/owner_session_capacity.rs) has only
replay_storage/session; it does not suddenly contain the new service bounds.
Reservation preparation must check its limits against the same actual
Executor/plan and enclosing process domain before any publication. Exact
representation and bidirectional identity tests remain in the joint freeze.

Every modeled preparation failure returns the original clock, Executor, plan
**and reservation**, plus its precise cause. Preserve the old
Capacity(OwnerSessionCapacityError) mapping; do not squash mailbox, layout,
storage or pair failures into a generic string. Destroy only fresh empty
unpublished preparation. No Driver/Target poll, external Waker registration or
source publication occurs during preparation. All fallible preparation
precedes infallible inward sender/cell installation and matching Controller
inbox installation; publish the Controller last.

Returning authority does not undo a real already-committed process fault.
If the domain faulted, the returned reservation remains attached to that
same failed domain, not a newly Open gate. On successful construction, the
whole pair follows Controller ownership. Later output-root allocation failure
still returns the whole Controller plus OwnerOutputDrainConfigError, as
[continuation.rs new](../../src/owned_upstream/quinn_leg_composition/owner/continuation.rs)
at192 already does. No extractor downgrades it to an unbound Executor.

### Preserve real existing caller states

Cold means an exclusive construction interval, not an empty business session.
The accepted domain must preserve existing valid quiescent live/recovery
inputs; an invalid new binding is rejected before mutation. No clearing,
replay reconstruction or silent Active-only/pristine-only restriction:

- [compose_registered_owner_b](../../src/owned_upstream/quinn_leg_actor/tests/r7_physical_attach_permit.rs)
  at61/77 consumes an Executor from registered_fixture at119, whose checked
  fixture uses (2,1,true). [Actor fixture](../../src/owned_upstream/quinn_leg_actor/tests/mod.rs)
  at3740 creates that Executor;3749–3757 opens one real Target flow on the same
  mutable Executor before returning it for checked Controller construction.
- [checked_root_with_pending_recovery](../../src/owned_upstream/quinn_leg_composition/owner/continuation/tests/mod.rs)
  installs replacement and enqueues acceptance at215–225, then new_checked at229.
  Those retained facts are not an empty fixture or a legacy capacity bypass.

The full caller inventory in the [binding preflight](2026-09-07-knife16-r7-p3-binding-preflight.md)
still applies. Each caller needs explicit real limits/domain preparation and
unchanged provenance/error assertions; none gets a guessed compatibility plan.

## 4. RESET feedback must not wait on its own input exclusion

Current [apply_target_failure](../../src/owned_upstream/owner_target.rs) at4878
stages RESET, immediately mixed-dequeues it, and reduces the exact completion.
The accepted binding deletes this mixed intake: RESET goes through the same
Driver lane, Control send cell and Controller mailbox as other local commands.

An incorrect straightforward conversion would retain the failing work in
pending_continuation until that queued RESET is reduced. But
begin_inbound_admission at310 requires a vacant output page, and
begin_service_turn at347 grants continuation only after a drained page that
still needs continuation. Controller also excludes ordinary actor input while
pending_continuation is present (at3259). There is currently no legal
mailbox-completion-under-continuation entry. The resulting proposed wait graph
would be:

```text
continuation waits for its RESET completion
  -> RESET waits for ControlInbox reducer admission
  -> admission waits for continuation/output exclusion to finish
  -> continuation still waits for RESET
```

This is a counterexample to that proposed conversion, not current runtime
behavior: today's direct synchronous intake does not take that asynchronous
route. Neither an extra ready bit nor a larger budget breaks this dependency.

**Select Flow-local completion custody for external feedback waits, not an
outstanding effect continuation waiting on mailbox input.** Deepen the existing
TargetFlowOwner/PendingTargetCompletion owning phase. Transfer the exact failed
current work into its bounded Flow completion position before settling that
effect step. Keep the original effects/output/observations under their existing
owners; no duplicate RESET command, new session queue or raw work escape.
The command travels once through Driver/cell/mailbox; retained correlation is
not a second command. A failed Flow cannot issue another Target operation while
that completion remains unresolved. Ordinary effect continuation still owns
actual remaining effect work, not the wait for this future mailbox event.

Output admission/drain remains unchanged as an exclusion: no special
allow-while-continuing flag or arbitrary reducer callback may bypass it.
Once actual effect/output work settles, normal ControlInbox admission can
consume prior queued commands and then the exact RESET. Its observed reducer
transaction settles the retained failure phase and any generated effects.
The concrete phase table must prove no remaining effect depends on that same
queued RESET before the ordinary gate reopens. If it cannot, this candidate
is rejected, not implemented with a FIFO scan, priority side channel or drop.

Existing PendingTargetCompletion at1895 contains Open/Failure/HalfClose and
retains exact rejected commands. It is a useful owning location, not an
already implemented pre-RESET transaction. Full phase/error representation
must still cover duplicate failure, Full/Closed, unknown send, earlier peer
terminal, Z rejection, cancellation and shutdown. Original work/permits may be
released only in the real observed release-only settlement. A Flow ID, empty
metadata or accepted notification is not that settlement. Later split-phase
OPEN waits must obey the same no-self-exclusion rule, without an extra queue.

## 5. Guard, quantum and physical proof still required

The existing output root remains the whole owning Seam. Inbox reduction uses
its admission guard; source/notification-only work needs its existing service
authority deepened without requiring an unrelated output page to be vacant.
Entered unknown must remain nonzero and ordinary re-entry excluded. No new
parallel failure/continuation ledger. Release-only observations and local outer
notification finalization cover both normal Ok and recoverable Err before
publication, as the accepted credit/wake contracts require.

K cannot be frozen by counting method names. Actual continuation categories
must include bounded effect service, Target named probes/attempts, common
notification, credit sweep, retained completion, join and publisher retirement.
Both current full-map selectors (retry_one_pending_target_completion at4975,
drain_pending_target_joins at5202) must leave the bound route. A returned effect
page may itself perform several Target calls; its max_effect_steps and actual
byte bounds must be charged, not called a single constant-time I/O operation.
No numerical K, end-to-end bound or TOTALBYTE is published in this decision.

Logical capacity witnesses do not yet prove physical retained backing:

- [Output drain](../../src/owned_upstream/owner_output_drain.rs) at1051/1055
  separately reserves pending-output and encoded-length scratch containers.
- Root at607–630 prepares residents, masks, Wakers, identities and metadata
  while the original input Vec remains owned until658: these coexist cold.
- [Mailbox](../../src/owned_upstream/session.rs) at52–54 creates two mpsc
  channels and progress Arc; the observer at94 adds a pinned registration Box.
  M_ctl/M_data and pointer size alone are not their backing envelope.

The concrete completion phase, retained buffer capacity, summaries,
generation tickets and scratch coexistence determine checked Layout. Existing
logical capacity remains accepted; there is no claim it already bounds RSS,
allocator metadata or recovery from allocator abort.

## 6. Next decisive work and evidence

With these construction/feedback directions independently accepted, freeze
the same-domain reservation and complete Flow completion phase with all
caller migrations, existing guard operations and physical storage. Derive
categories/K from those actual operations, with exact per-visit byte/loop
bounds. Only that coupled review may open a symbol-first Rust Move/TDD scope.
Do not replace full P3 with a constructor-only or simulated ready-bit success.

Required owning tracers include: rejection returning the same reservation and
Executor; preserved live/recovery input; shared domain failure preventing a
later ordinary admission; and an actual Target failure whose RESET traverses
a pressured Control lane behind earlier work, eventually settles through the
same output root without duplicate I/O or continuation deadlock. Include all
failure/terminal races named above; no manual post-event readiness injection.

Mencius supplied independent source/constructor audit and challenged the
feedback cycle; Main checked the actual source bodies. Heisenberg is the
independent Reviewer, not a co-designer. Final review: P0/P1/P2=0/0/0, Approve;
scoped Module/Clean/System/DDIA8/9/7/8. Heisenberg verified seven fingerprints,
42 links and338 unchanged Rust/governance/HEAD/index against audit-packet.json.
Only direction/document evidence is accepted; full phase/guard, feedback
admission, K/Layout and Rust scope remain open. Factual seven-document status/
review-evidence promotion was allowed without another review; audit-final.json
records that promotion. The immutable
entry at /tmp/mini-vpn-p3-binding-construction-EzJiOD/entry.json verifies the
previous accepted packet and1152 repository files/338 Rust/HEAD/index.
No Rust/Cargo/test/fmt/governance/external run occurred. An initial read-only
lookup for resumable/reducer.rs was corrected with rg --files to session.rs;
it was not a repair-test failure. No commit/push or full-P3 promotion.
