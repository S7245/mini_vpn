# P3 — actual Owner binding preflight

Date: 2026-09-07.

Status: **SOURCE AUDIT ACCEPTED; COUPLED BINDING NOT FROZEN; NO RUST GRANT**.

## 1. Boundary and result

The [Target Adapter Move](2026-09-07-knife16-r7-p3-target-adapter-move-plan.md)
remains accepted. This stage reads the actual next call/ownership closure;
it changes no Rust and reruns no Rust/Cargo gate. Previous green evidence is
historical, not new binding evidence. Full P3/Case C/R7/Task4 remain incomplete.

The accepted [source placement](2026-09-07-knife16-r7-p3-owner-source-service-decision.md),
[local credit](2026-09-07-knife16-r7-p3-owner-credit-rearm-contract.md) and
[Target readiness](2026-09-07-knife16-r7-p3-target-readiness-contract.md)
contracts remain controlling. The full-P3 goal permits continued in-scope
design and causal repairs, not a new unchecked constructor or a bypass.
No P4–P7, public routing, external run, commit/push or governance change.

The joint freeze is not ready for a Rust allowed-files grant. Most importantly,
bounded live Flow count does not bound retained old-generation publisher
allocations. The next design must close reclamation and pressure, not just
compare a generation number when a callback wakes. This is a newly grounded
counterexample to a proposed shortcut, not a reproduced runtime regression.

## 2. Actual cold construction, not an empty-fixture assumption

In [owner_session_controller.rs](../../src/owned_upstream/owner_session_controller.rs),
`new_checked` at1457 consumes the actual Executor, clock and
`OwnerSessionCapacityPlan`; failure returns those same three inputs plus error.
Its current witness is derived before the checked Controller is published.
The Executor already minted its source factory at construction; rollback
must not pretend to restore a pre-mint bootstrap.

The [plan](../../src/owned_upstream/owner_session_capacity.rs) stores paired
replay/session configuration. The [witness](../../src/owned_upstream/owner_session_controller/capacity.rs)
checks factory, Target, progress, output/effect and replay/receive geometry.
Neither currently supplies independent positive mailbox M_ctl/M_data,
paired inbox identity, two send cells, local generation backing or cold peak.
Source-channel slots and the outer scalar ReadyProcessPlan cannot stand in
for those missing physical inputs.

The observed checked-constructor callers are all tests, not an installed
production session lifecycle. Their bounded source inventory is retained in
`/tmp/mini-vpn-p3-target-adapters-yCzYFb/next-cold-caller-audit.md`:

- Controller capacity provenance/rejection tests;
- continuation `checked_root` and `checked_root_with_pending_recovery`;
- physical attach's registered owner-B fixture;
- physical recovery's checked `legacy_active_b_terminal_fixture`;
- initial-acceptance settlement fixtures;
- CurrentOnly's settled-seed fixture.

`checked_root_with_pending_recovery` has already processed leg loss,
replacement installation and acceptance enqueue before construction. It is
not pristine. A new constructor must explicitly preserve that real state or
review its caller migration; blanket empty-state rejection is not a drop-in
binding. Existing failure-return and replay/factory provenance tracers stay
mandatory. Legacy test Controller is a distinct type, not a migration escape.

The selected cold transaction remains: validate inputs/exclusivity; prepare
one fresh pair, sole observer and all modeled fallible storage; infallibly
install sender/cells inward and matching inbox in Controller; publish last.
No Driver poll, Target I/O or Waker publication may occur during preparation.
Std/channel/Arc allocator abort is not modeled recoverable OOM evidence.

## 3. Two intake sites, one real command route

[read_source.rs](../../src/owned_upstream/owner_target/read_source.rs)
`try_read_target_observed` reserves, reads Target, enqueues DATA/EOF, then
mixed-dequeues and reduces. In [owner_target.rs](../../src/owned_upstream/owner_target.rs),
`apply_target_failure` at4878 similarly enqueues RESET, mixed-dequeues it and
submits failure completion. Read, write and half-close failures share that
second closure. Cutting only the read method leaves a second live intake.

The existing [server_root.rs](../../src/owned_upstream/quinn_leg_composition/owner/server_root.rs)
`SessionCommands` at127 owns ingress, exclusive capacity observation, two
cells, cursor and transfer quarantine. `from_open_completion` at287 requires
a real request/completion; it is not an empty cold-pair constructor. Its
`CommandResident` still owns these commands in the outer prototype.

The complete Implementation and narrow value dependencies must move inward;
the actual bound resident cannot retain another pair. Prototype tests using
private fields need an explicit migration closure, not field export or a
second production implementation. Inward code cannot import server_root's
private SendLane, scheduler or callback types. No visibility choice is frozen
by this audit, and no new provider/getter is authorized.

The actual lane ports are [driver_receive.rs](../../src/owned_upstream/tcp/driver_receive.rs)
`poll_owner_control` at115 and `poll_owner_ordered_source` at142. One result
must enter the preflighted real cell before notification/yield; pull and send
remain separate units. The mixed Client path is unchanged. A stored failure
intent must survive delayed RESET intake and completion pressure without a
second Target operation.

## 4. Existing guard coverage is not automatic

The [output root](../../src/owned_upstream/quinn_leg_composition/owner/continuation.rs)
owns the Controller, output page, first failure and continuation authority.
`begin_inbound_admission` at310 requires vacant output and exact drain zero;
its entered guard handles observed actor admission and sticky unwind.
`begin_service_turn` at347 handles output/continuation phases. No current
method admits a selected mailbox receipt as the new guarded local operation.

The existing recovery-service state/guard is not evidence that all new local
work is already covered. Any reuse or deepening must preserve one settlement
authority and old exact inventory/callers; adding another failure wrapper is
not approved. Common Target notification must not be stalled indefinitely
behind an unrelated output page merely because ordinary input is excluded.

The future release-only interval must surround every normally recoverable
reducer/effect/continuation/cleanup exit: sample actual aggregate permits at
entry, finalize/rearm on both Ok and recoverable Err, and retain the existing
sticky entered disposition on unwind. Mixed source acquisition is excluded.
The current synchronous read-plus-reduce method is not that separation.
No normal external release may be relabeled fatal to preserve the protocol.

Two existing hot-route hazards also remain: pending completion selection at
`retry_one_pending_target_completion` searches the Flow map; join draining at
`drain_pending_target_joins` at5202 collects a Vec and iterates pending Flows.
The bound route must replace their selection through one persistent bounded
index, not hide them inside a nominal one-unit call. Current bounded effect
steps do not prove every inner operation constant-time.

## 5. Cross-generation backing: source-proved missing lifecycle

The current [scheduler](../../src/owned_upstream/quinn_leg_composition/owner/server_root/scheduler.rs)
`ReadyKey` at143 contains slot/slot-generation/scheduling-generation, currently
initialized to1. It has no Flow-generation reuse lifecycle. `CapacityWake`
at386 holds an immutable key and Weak metadata, not business authority.
Healthy key mismatch is ignored under slot exclusion; poison handling commits
the root fault before key comparison. Do not claim all stale callbacks are
unconditionally effect-free, including corrupt shared metadata.

`ReadyIndex::capacity_waker` allocates a new Arc per invocation; its current
closed constructor calls it once per resident, R<=N. Waker clone only adds a
reference to that generation's allocation; it is not another allocation.
There is no reusable cross-generation publisher pool in this code.

Tokio1.52.0 Notify/ScheduledIo can move Wakers into a lock-external WakeList
before invocation. Removing a registration cannot retract a reference already
in that list. The local registry sources audited were `sync/notify.rs`,
`runtime/io/scheduled_io.rs`, `runtime/io/registration.rs`; standard-library
Arc/Waker sources were inspected locally, with no external request.
The existing persistent mailbox waiter and registration-before-lease Drop
prove registration ownership, not completion of every in-flight callback.

Counterexample: with F=1, retain one old publisher reference per closed Flow
and allocate another publisher for each next Flow. Live Flows stay <=1;
independent backing grows with generations. By contrast, many clones of one
generation still share one allocation. An Arc strong-count snapshot is not
a concurrent reclamation operation. Mutating an old immutable Waker key or
charging these references to Z is forbidden by the accepted contract.

[MemoryTarget](../../src/owned_upstream/target.rs) uses BTreeMaps/VecDeques,
not a stable generation arena. Terminalization clears contents but may retain
buffer backing. [join](../../src/owned_upstream/target/memory_io.rs) removes a
live entry only after owned state and Z permit it; joined tombstones have a
separate lifetime. A higher FlowId can use live capacity while old tombstones
remain. That is not proof that old publisher allocations were reclaimed.

The required actual sequence is: prevent new old-generation registrations;
unregister existing ones; account for in-flight references; prove backing
reclamation; only then permit its bounded position to be reused. If references
remain, the same position needs a retained pressure disposition and an actual
wake edge for later reclamation, not a full scan, timer spin or silent leak.

## 6. Coupled freeze exit criteria

The first reclamation candidate has now been source-audited, not approved.
Keeping Retiring inside F, retaining an owner Arc, never exporting ticket Weak
and using real `try_unwrap`/`get_mut` uniqueness can conditionally bound one
ticket backing per occupied slot. Failure must retain the same Arc; success
must synchronously destroy the old backing before any new ticket is made.
This is not a total byte bound; actual operation-specific tickets and cold
coexistence still need enumeration. An immutable key is never rewritten.

Its proposed last-reference notification has a concrete unresolved hazard:
Tokio's ScheduledIo `poll_readiness`/`clear_wakers` drop old registrations under
the waiter Mutex; WakeList's DropGuard can drop remaining Wakers during unwind.
Connecting raw drop directly to current `signal -> notify_waiters` would add
callback/reentry under that dependency lock and a possible second panic.
Dropping under the Owner's own publisher guard can also self-lock. Merely
publishing an atomic bit avoids callback reentry but cannot wake an already
parked root. Existing controlled clone/drop did not execute business callbacks;
that earlier proof cannot be carried over to this changed operation.

A correct candidate must also cover consuming `Waker::wake` (which does not
subsequently call vtable.drop), initial retirement when no future drop remains,
temporary references, release-before-candidate ordering, dirty/clear races and
late old-generation candidates. Standard Wake plus an Owner unregister check
does not notify on every ordinary final Drop; no proof classifies all those
missing edges as fatal. Retaining F safely is not recovery liveness.

These are obligations, not selected signatures, guessed K or a Rust grant:

1. Choose exact checked-constructor inputs and all caller migrations, including
   pending recovery, exact failure payloads and bidirectional pair identity.
2. Name the complete SessionCommands move and prototype test migration,
   then both actual Owner intake cutovers. Freeze scope, visibility and
   growth-preserving movement separately from behavior.
3. Name actual output-root operation/guard and every release-only entry/exit;
   prove common notification can progress without illegal reducer admission.
4. Close immutable publisher reclamation, registration and in-flight reference
   bounds, pressure, wake/clear/park races and shutdown generation exclusion.
   The same reasoning must cover demanded Target and actual Driver callbacks,
   not only the outer capacity observer.
5. Enumerate concrete continuation subcategories and each unit's operation/
   byte limit. Derive K and checked per-Flow delay from that enumeration;
   five-source fairness alone is insufficient. Bound existing inner work.
6. Charge actual retained/allocated backing and cold coexistence, including
   mailbox/prefetch, generations, registrations, summaries and buffer capacity;
   logical byte length, Arc clone count and ready-lease count are distinct.
7. Freeze deterministic actual-Controller tracers and independent review before
   any binding behavior. Source-only conclusions do not replace runtime RED.

The next design work is a real, non-reentrant and panic-contained wake mechanism
safe in those destructor contexts, coupled back to items1/3/5/6. The candidate's
storage argument alone cannot authorize it. This audit does not prove that no
solution exists, nor choose an unsafe RawWaker, timer polling, helper task,
trusted-runtime caller or P6 expansion as the answer. Such a choice needs its
own feasibility/authority and independent design review before the joint freeze.
No further preparatory Move is requested by this preflight result.

## 7. Evidence and review

`/tmp/mini-vpn-p3-binding-czzlml/entry.json` verifies the prior accepted final
packet and captures1150 repository files/338 Rust, HEAD/index before edits.
Mencius separately audited actual publisher/registration/MemoryTarget lifetime
and confirmed no existing cross-generation pool, then audited the candidate's
conditional capacity argument and missing safe post-release notification.
Main independently inspected the relevant scheduled_io, WakeList and Arc
source bodies. This is source evidence, not design approval or a runtime test.
Main also audited cold construction, command intake and output guard boundaries.

Same independent Heisenberg's final source-audit/seven-document review returned
P0/P1/P2=0/0/0, Approve. Scoped Module/Clean/System/DDIA scores8/9/7/8;
the missing proof is actual lifecycle/liveness, not another preparatory Move.
Reviewer verified seven fingerprints,44 links,338 unchanged Rust and unchanged
governance/HEAD/index. No independent runtime test or Cargo rerun was claimed.
Only factual pending-to-source-audit-accepted/evidence transcription was allowed
without another review. `audit-candidate.json` binds the reviewed packet;
`audit-final.json` binds the factual promotion. Coupled binding remains NOT
FROZEN, no Rust scope is granted and full P3 remains incomplete.

No Rust/Cargo/fmt/test, governance, external, commit or push action occurred
in this stage. The prior accepted code remains unchanged.
