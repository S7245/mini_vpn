# Quinn client-root combination repair: architecture and execution

Date: 2026-09-04.

Status: **Independently accepted architecture; implementation not yet started.**

The user authorized fixing the public-path blocking, wrong-routing, and panic
risks, followed by these two client-root combination defects. That authorization
covers implementation of the reviewed repair below; design acceptance is not an
implementation result. Public-path repairs precede root
behavior work. No additional owner/server, public-TUN cutover, physical B,
production switch-arbiter wiring, WAN, throughput, or M2 work is authorized here.

## Contract sources and exact scope

The requirements remain [Rules.md](../../Rules.md), the
[R6d client-root contract](2026-08-25-knife16-r6d-recovery-capacity-production-root-architecture-spec.md),
the [R7 architecture](2026-08-26-knife16-r7-no-oracle-switch-architecture-and-plan.md),
and [current status](../current/STATUS.md). This sidecar proposes only:

1. a complete, bounded live-driver scan before root park; and
2. separation of current reducer replay occupancy from one retained,
   same-generation local ACK-stall decision.

R6d's single composition owner, category fairness, bounded work, producer-owned
progress cursors, lifecycle CAS, exact retry ownership, and structured shutdown
remain frozen. R7's actor-captured arrival, exact-deadline ACK priority, source
correlation, and sole future switch arbiter remain frozen. The correction is
necessary-only. It makes no throughput or full R7 reachability claim.

The root currently records local due observations; it does not consume them
through physical B or `ClientSwitchArbiter`. Coalescing a later replay interval
under an unresolved stall decision cannot be presented as completing that work.

## Code-grounded failure tree

Line anchors below identify the reviewed pre-repair tree, not permanent API.

| Priority | Branch and evidence | Falsifiable prediction |
|---|---|---|
| 1 | `drive_one_root_driver`, `quinn_client_root.rs:3939`, checks one driver; `run_root:5202` parks after seven idle categories | With two drivers and one enqueue only on the unvisited driver, real peer DATA waits for unrelated progress; the single-driver control passes |
| 2 | Live scan cursors are resampled on progress while category emptiness says nothing about remaining drivers | An enqueue before cursor resampling is still stranded unless the subsequent scan covers the whole registry |
| 3 | The category ring spans cooperative slices and the driver registry uses append and `swap_remove` | Budgets `1`, `7`, `64`, append, and removal expose incomplete or repeated coverage if the proof resets at yield or survives topology mutation |
| 4 | `observe_inbound:1788` retains an old episode after a late ACK clears actual replay; `observe_replay_became_outstanding:1723` rejects a later nonempty transition | Real DATA, late full-replay ACK, then new DATA yields `DuplicateAggregateOutstandingEpisode` and root failure |
| 5 | `observe_reducer` clears policy whenever its aggregate replay fact is false | A flow-terminal reducer turn can erase a retained due decision despite the late-ACK protection |

These are one root's control and observation composition defects. The repair
must converge those decisions in that owner; independent category, timer, and
occupancy flags with caller-managed reconciliation are not sufficient.

## One live-scan Authority

The private root scheduler owns the category cursor, driver coverage, and the
actor/factory cursors captured before that coverage starts. There is one live
scan at a time. It contains no DATA payload, new channel, task, or per-flow ready
entry. The driver registry remains owned by the same root.

A scan begins after real progress or a completed wait. It captures producer
cursors before inspecting work. An empty driver visit is **scan progress**, not
work progress: it advances bounded coverage without resampling producer cursors
or restarting the scan. A work-budget yield preserves the complete scan value.

One Driver-category turn inspects at most one driver. For a stable registry of
`F` drivers, a round-robin traversal must account for every driver exactly once
before driver emptiness is established. The starting index may be any valid
fairness position. Empty registries are complete immediately. Coverage counters
are checked/bounded by the checked registry capacity; modulo/index arithmetic
must never operate on an empty registry.

Global driver admission can instead be blocked by a retained effect batch or
exact deferred input. That is a separate coverage outcome, not evidence that
individual queues are empty. It permits park only while the exact blocking
dependency is retained and its existing actor/factory/timer progress source is
included in the wait. When that dependency clears, the responsible category
reports real progress and starts a fresh scan. A local queue-empty result cannot
claim this global-blocked outcome.

Every append/removal invalidates driver coverage in the same synchronous root
turn. All such sites, including terminal retirement and `swap_remove`, must
enter this rule. Retaining coverage across a changed registry is forbidden.
No additional monotonically incremented topology counter is required: the one
root can restart the bounded scan directly at the mutation seam. If a counter
is chosen, overflow must fail closed rather than wrap.

Park is allowed only when:

- every category has completed an idle/blocked visit since the last real work;
- driver coverage is complete, or its exact global blocking dependency is
  retained and wakeable;
- the lifecycle and shutdown gate still allow the existing wait transaction;
- the wait uses this scan's pre-check actor/factory cursors; and
- applicable absolute timer deadlines are included unchanged.

An edge after cursor capture therefore causes either an observable queue item
during this complete scan or an immediately ready cursor wait. Capturing a new
cursor after the final empty check is forbidden. An edge before capture is
covered by the subsequent complete scan. Select order is not a substitute for
this proof. Shutdown retains its separately accepted drain ordering and
pre-scan-cursor rules; the live repair may not weaken them.

For a stable, wholly idle registry, with `C = 7` current categories:

```text
category visits before park <= C * max(F, 1)
synchronous visits per cooperative slice <= work_budget <= 64
cooperative slices <= ceil(C * max(F, 1) / work_budget)
scan storage = O(1); extra per-driver storage = 0
```

The multiplication is a checked validation/test bound, not an allocation or a
per-packet calculation. Real work/topology mutation restarts the proof; this
bound does not falsely promise completion while the system changes forever.
Each synchronous slice remains bounded, categories remain fair, and shutdown
is reobserved before each admitted turn. Quiescence must end in a real wait,
not repeated zero-delay timer wake or `yield_now` polling.

## Replay facts and one pending stall decision

Actual replay ownership belongs to `SessionSupervisor`. Root must not introduce
another replay registry or a persistent `replay_nonempty` authority. It consumes
the existing supervisor/controller observations and discards their scalar replay
facts after each synchronous policy transition. `session_became_nonempty` means
an actual reducer empty-to-nonempty transition; it does not prove that a
historical stall decision was settled. A retained source is evidence of an
unsettled observed policy episode, not a continuously synchronized inventory of
actual replay.

For each current session/generation, the policy owner retains at most one
`ClientLocalAckStallSource`, one source episode, one local timer slot, and one
pending/observed due decision. The source episode represents the current
unsettled observed policy decision. It is not a promise that the reducer's
replay has remained continuously nonempty, including before its deadline.

The conceptual policy states are:

| State | Source | Source episode / timer | Due decision |
|---|---:|---:|---|
| Unestablished or session retired | 0 | 0 / 0 | None |
| Clear, same correlation retained | 1 | 0 / 0 | None |
| Tracking | 1 | 1 / 1 | Deadline still subject to the ACK arrival rule |
| EligiblePending | 1 | 1 / 1 | Old deadline protected; timer category has not yet observed it |
| EligibleObserved | 1 | 1 / 1 | Same protected deadline already observed; no repeated timer wake |
| Quarantined | Exact retained prior owners | Exact retained prior slots | Sticky invalid/uncertain transition evidence |

These are semantic states, not a requirement to add parallel enums and flags.
The implementation should represent invalid combinations as unconstructible
states where practical. Source identity and the source's opaque episode seal
remain unchanged while an eligible decision is retained.

Root time may establish `now >= D`, where `D` is the existing absolute deadline.
A positive committed application ACK establishes its own deadline eligibility
using only its immutable actor arrival `a`. `a > D` also proves the old deadline
is already eligible, even when the timer category has not run. That branch must
enter/preserve `EligiblePending`, not clear the episode or start a new timer.

An ACK with `a <= D` still wins after a delayed timer observation: with replay
remaining, refresh through the existing source API to `a + stall_after`; with
no replay remaining, clear the source episode and timer together. This is the
existing before/equal boundary, not an extension based on root service time.
The refresh/clear invalidates the old due observation and seal exactly as the
accepted source contract requires.

For `a > D`, commit and count the genuine ACK, but do not refresh or clear the
old policy episode. If that ACK empties replay and later real DATA creates a
new replay interval, the new interval is serviced normally and coalesces under
the one existing unresolved stall decision. It creates no second source,
episode, timer, trigger, or history queue. Subsequent late empty/nonempty cycles
have the same bounded behavior. This does not defer a fresh deadline: an older
eligible decision is already pending and remains the earliest unresolved work.

The coalescing admission check is narrow: the old source and timer must match,
and either an already retained eligibility fact or the current admitted root
turn's `now >= D` must prove the old decision eligible. A fresh nonempty fact
while an existing old episode is still noneligible remains
`DuplicateAggregateOutstandingEpisode`; no unconditional duplicate-success
branch is allowed. The root need not know whether replay was empty between two
observations to preserve the already eligible decision.

Ordinary flow terminal can clear noneligible Tracking when the reducer has no
remaining replay. It cannot discard EligiblePending/EligibleObserved, even if
all current replay is gone. Session terminal and root shutdown instead close
the session-level decision through explicit audited retirement. No future
generation can inherit a source with an old generation identity.

## Existing observation seam and transaction order

The original late-ACK/new-DATA defect is fully expressible through existing
`RootPositiveAckObservation`, `RootReducerObservationView`, and root monotonic
time. No controller, supervisor, composition result-envelope, or wire change is
required. Positive ACK timing remains stricter than a decoded ACK or transport
delivery. The root may never invent a positive ACK from a snapshot or an effect.

An existing reducer view with `session_has_outstanding_replay = false` clears
only a noneligible episode. If `now >= D` or eligibility is already retained,
that same view preserves the old decision. The flow/session terminal distinction
in this document applies to the existing root-observed event seams; it is not a
claim that every currently unreported inbound event has gained a new observation.

The current inbound `Applied` path exposes an optional positive-ACK fact and
does not expose every non-ACK replay-release event. An inbound RESET can
therefore leave the root's older observed policy episode present. That existing
observation limitation is outside this repair: there is no new occupancy mirror
to make it a new invariant, no widening to complete R7 observation wiring, and
no claim that this stage closes all possible replay-drain/restart sequences.
In particular, a previously unobserved pre-deadline drain followed by new DATA
does not become an authorized duplicate-success case. If a required tracer for
the two scoped defects demonstrates that this missing observation prevents the
repair, stop for a separate reviewed scope decision rather than silently expand.

Do not call full `SessionSupervisor::snapshot()` per turn to compensate: it sums
retained uplink extents. Independent sums of messages/bytes and physical/source
inventory belong at bounded audit/test/shutdown boundaries, not on each DATA
record. Tests may compare actual replay with the opaque facts emitted at the
scoped producer events without adding another production replay authority.

Each policy transition validates correlation, source/timer correspondence,
time arithmetic, and every affected history counter before its commit. It then
updates source, timer, and phase as one synchronous, non-awaiting transaction.
No callback/publication/ACK may interrupt a half-applied policy state. The
producer's reducer turn has already committed; policy validation failure cannot
pretend to roll that reducer back or discard its accompanying effects.

On such failure, preserve the exact old policy ownership and the fixed-size
failed observation in one sticky quarantine slot, retain/admit the accompanying
effects through the existing exact-failure path, and stop live service. There
can be only one failed observation because no further live turn is admitted.
Shutdown records its explicit disposition. If correspondence or ownership cannot
be established, the quarantine remains nonzero and no clean-zero witness is
minted; error strings alone cannot normalize unknown ownership to zero.

## Bidirectional invariants and capacity

For every valid live state, audit both directions:

1. A source episode exists **iff** its one physical timer slot exists. Both
   name the same absolute deadline; the source matches root session/generation.
2. EligiblePending/EligibleObserved exist **iff** one protected source episode
   and timer are retained. Observed implies eligible, and the timer's no-wake
   marker exists **iff** that exact due decision is observed.
3. Clear has no source episode/timer. A completed applicable nonempty-to-empty
   observation clears only a noneligible episode. Eligible may coexist with
   either reducer occupancy. Neither Tracking nor Eligible is an independent
   replay-ownership assertion; absence of an observation cannot be inferred from
   the presence of a source.
4. A replay-became-nonempty fact implies that its same existing reducer result
   is nonempty. No independently supplied booleans or test-minted facts authorize
   production transitions. At the scoped event seams, tests independently
   confirm that the opaque facts reflect actual replay ownership. The physical
   bidirectional invariant is source/episode/timer/eligibility correspondence,
   not the invalid equation `source exists iff actual replay is nonempty`.
5. Session retirement/shutdown accounts for every physically present source,
   episode, timer, eligible/observed decision, and quarantine fact before zero.
   Historical disposition counters are not live owners and cannot substitute
   for examining the actual stored slots.

Let `F` be checked max flows, `Z` checked terminal tombstones, `Q` command slots,
`E` checked pending-effect slots, and `W` the bounded work budget. Existing
timer capacity remains exactly:

```text
timers <= 1 current-A recovery + 1 standby-B wake + 1 local stall + Z
local sources <= 1; local episodes <= 1; local timer slots <= 1
eligible decisions <= 1; observed due decisions <= eligible decisions
failed committed-observation slots <= 1
checked root slot envelope = Q + W + E + F + (3 + Z) + 1
```

The last slot is the named fixed-size quarantine fact, preaccounted before root
construction, not an unbounded error log. An implementation that stores the same
fact inside an already counted exclusive state must prove that equivalence in
review and avoid double-counting. It cannot omit the physical fault-state bound.
The eligibility and observed markers refer to the existing local timer owner,
not additional independently issued trigger capabilities. No new DATA buffer,
queue, actor, endpoint, timer task, per-record allocation, or global lock exists.

Live/shutdown inventories must distinguish protected eligibility from timer
observation, and record actual retired quantities. Checked history increment
overflow rejects before policy commit. Allocation/time/capacity failures cannot
leave a source without its timer or silently replace an old owner.

## Branch and settlement matrix

| Input/branch | Required behavior | Exact ownership / progress |
|---|---|---|
| First actual replay interval | Create Tracking after checked preflight | One source episode and one timer, matching deadline |
| Additional replay while nonempty | Keep existing policy baseline | No extra episode, timer, or allocation |
| Existing reducer observation reports no replay before eligibility | Consume that existing fact and clear Tracking | Source correlation retained; episode/timer both absent |
| Positive ACK before/equal deadline, replay remains | Refresh to actor arrival plus configured interval | Rotate source seal and timer together; old due becomes stale |
| Positive ACK before/equal deadline, replay empty | Clear old policy episode | No residual timer or observed due marker |
| Positive ACK after deadline, timer not yet observed | Preserve exact old deadline as EligiblePending | Genuine ACK counted; one timer remains wakeable until observed |
| Positive ACK after deadline, timer already observed | Preserve EligibleObserved | No new timer wake and no deletion of due evidence |
| New DATA after late full-replay ACK | Service DATA and coalesce under protected decision | One source/episode/timer; no duplicate-episode root failure |
| New nonempty fact with an existing noneligible episode | Keep duplicate-episode invariant failure | No unconditional reset or success; retain exact failure state |
| Repeated late replay drain/restart | Same coalescing rule | Constant storage; no episode queue |
| Ordinary flow terminal with protected due | Preserve protected decision; consume actual replay fact | No accidental session-level retirement |
| Session terminal | Explicitly retire the complete local policy state | Exact terminal disposition; no later live source reuse |
| Source/control/actor/effect pressure | Retain exact existing input/effect/permit | No policy refresh from failed admission; wait on real dependency |
| Rejected or duplicate inbound without reducer progress | Snapshot-inert policy | No ACK freshness, source creation, or new deadline |
| Wrong source, missing timer, contradictory fact, checked overflow | Sticky quarantine and root failure | Retain old owners plus one failed fact; no further live mutation |
| Hard transport failure | Keep exact first cause and enter existing terminal/shutdown path | No new attach/arbiter bypass; audit local policy retirement |
| Caller/observer cancellation during live work or wait | Preserve root ownership and scan cursors | No consumed DATA, selected command, or timer due to cancelled observation |
| Shutdown CAS wins before selected turn | Reject exact selected input through existing cutoff | Zero post-cutoff live turns; no policy mutation |
| Shutdown after an admitted synchronous turn | Finish that exact turn, then retire local policy before awaits | Source/timer/due audit and actor/factory/driver cleanup all retained |
| Interrupted/uncertain settlement | Sticky nonzero quarantine | Never report ordinary Joined with fabricated zero |

The accepted source/arbiter interface remains the only future trigger path:
`take_due_trigger` followed by `ClientSwitchArbiter::on_local_ack_stall`.
This repair issues no physical trigger or ATTACH. A later wiring stage must
consume/restore the exact source-bound decision, deduplicate all trigger kinds,
and settle the old generation before installing a new source. That future
requirement cannot be satisfied here by deleting a due marker.

## Allowed files, Writer ownership, and Move-first sequence

One root Writer owns overlapping root scheduling and policy work. No Rust is
started before this draft's independent review accepts the contract and exact
root-only implementation scope. Existing dirty changes remain user-owned.

| Area | Allowed axis |
|---|---|
| `src/owned_upstream/quinn_client_root.rs` | Root storage/outcome/inventory anchors, `run_root`, live scan, `drive_one_root_driver`, observed-turn adapters, local policy seam; preserve external API |
| New `quinn_client_root/run_loop.rs` | Deep private implementation of the complete existing root loop before changing its live scan; keep lifecycle/cleanup order exact |
| New `quinn_client_root/client_progress.rs` and private tests | Whole policy transition/validation/retirement model; parent retains types that its storage and stable callers must name |
| `quinn_client_root/tests/root_client_observations.rs` | Move existing characterization into the owning private test seam before new behavior; no new inline Rust tests |
| New `quinn_leg_actor/tests/root_combinations.rs` | Real peer/root tracers, using existing private fixture helpers |
| `quinn_leg_actor/tests/mod.rs` | Move one existing complete root OPEN/shutdown characterization into the new leaf before adding its module declaration; index shrinks |
| `tests/code_growth_governance.rs`, relevant provenance leaves | Integrator-only exact shrink ratchet and reviewed intentional route/test-name mapping |

`reverse_switch.rs`, `supervisor.rs`, `client_controller.rs`,
`quinn_leg_composition.rs`, public TUN selection, owner/server, fake-IP, TUIC
transport, and external acceptance scripts are outside this root stage. If the
frozen APIs cannot implement the reviewed policy without a change there, stop
and obtain an independently reviewed scope amendment before that change.

The parent is currently 6,468 lines. New production children must each remain
at most 1,500 lines; test leaves at most 800; new test indexes at most 300. No
grandfathered path/count increase or new lint allowance is permitted. Production
implementation moves and behavior changes are separate green boundaries.
Storage/outcome vocabulary may stay at the parent so private-child extraction
does not manufacture wrappers, reexports, wider caller scope, or callbacks.
Each child documents ownership, non-ownership, invariant, entry, and tests.

Execution is vertical:

1. Accept this narrow design independently; finish the preceding public-path
   repair gate. Freeze allowed symbols/call sites and current focused evidence.
2. Move the existing real root OPEN/shutdown test into the new test leaf; freeze
   its intentional qualified-name mapping and rerun it unchanged.
3. Move the complete existing root-loop implementation behind its private seam,
   with no scheduling behavior change; rerun characterization and lifecycle
   provenance. Integrator lowers shrink debt only at an accepted green boundary.
4. Add one true sparse-two-flow RED, then implement the complete live-scan
   Authority. Add the budget/topology/pressure boundary tracers one at a time.
5. Move the policy state machine and existing observation tests unchanged;
   rerun them. Preserve the existing observed-result interfaces exactly.
6. Add one late full-replay ACK then new DATA RED through the actual root and
   peer; implement the reviewed coalescing transaction. Add the remaining
   deadline/terminal/failure/cleanup cases incrementally.
7. Independently review the coherent implementation and exact ownership result;
   Integrator updates dated evidence, current status/navigation, and learnings.

## Required evidence and stop rules

The real fixture is `direct_production_root_fixture_with_plan` under
`quinn_leg_actor/tests`; scheduler policy supports `work_budget = 1`. Use the
real root handle's OPEN and flow enqueue, real peer receive, fixed actor queues,
and the existing actor publication hooks. Tests may add bounded private
one-shot Driver/pre-park barriers, but no producer-bypassing production API.

For the sparse-flow tracer, place one enqueue on the unvisited flow at the
exact scan seam; provide no unrelated actor/factory event to rescue it. Assert
the actual peer receives the expected flow/offset/bytes, then verify quiescent
scan counters stabilize at a real wait. Repeat budgets `1`, `7`, `64`; cover
registry append/removal and exact retained-pressure release.

For ACK/replay, send real first DATA, observe it at the peer, cross the deadline
using the same endpoint session clock, send a genuine full-replay application
ACK, prove root consumed it, then enqueue second DATA. Assert peer delivery,
Running phase, and one preserved due decision. Cover timer observation both
before and after the late ACK, repeated drain/restart, and an ordinary flow
terminal while due remains protected. A single-flow version separates this
failure from the scheduler failure.

Before/equal/after nanosecond boundaries use deterministic value-policy tracers
in addition to endpoint coverage. Real actor tests use captured/bracketed arrival
and the shared session clock, never an invented observation passed to root.
Positive ACK must release real reducer bytes; QUIC ACK/DELIVERED is insufficient.

Every endpoint tracer settles root, peer, factory, driver, timer, replay,
terminal, listener, and all join outcomes. No ignored join result, Drop-only
cleanup, or test-minted zero is accepted. Include exact source-without-timer and
timer-without-source invalidity, history overflow before commit, cancellation,
and lifecycle-cutoff tracers. Run focused tests, applicable library/provenance
and growth gates, format/diff checks, then independent review. Existing unrelated
lint failures are reported separately and do not justify unrelated repairs.

Stop before promotion if a scoped tracer needs an unavailable observer path,
per-turn full replay
inventory is introduced, a second local source/timer/trigger can exist, a late
ACK or ordinary flow terminal deletes protected due, a cursor is recaptured
after the empty check, or a failure/cancellation branch loses exact ownership.
If a repair test fails, analyze and present a concrete modification plan, then
wait for the user's confirmation before another repair attempt, as AGENTS
requires. Do not use new patches to bypass a failed architecture or test gate.

Review acceptance of this document means the scoped design is accepted, not
that Rust is green or R7 is physically complete. An implementation result must
own exact test evidence and limits before any compact status promotion.

Independent read-only review closed P0/P1/P2 at `0/0/0`, Approve. The reviewed
draft SHA-256 was `bbb106caea305614c414753aa08ecf14ffed72a1c6d5d2124a9c1878246a6725`.
The review checked live-scan/cursor ownership, source/timer correspondence,
eligible-before-observe protection, bounded quarantine, and Move/provenance
seams. It ran no Cargo tests and makes no implementation or external claim.
