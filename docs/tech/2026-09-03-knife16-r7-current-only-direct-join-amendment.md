# Knife16 R7 CurrentOnly direct-join amendment

Date: 2026-09-03.

Status: **ARCHITECTURE-ONLY INDEPENDENTLY ACCEPTED**. The separately authorized
necessary-only living-root A/B/error implementation is independently accepted
by its
[dated local result](2026-09-03-knife16-r7-current-only-direct-join-local-results.md).
Neither verdict closes Case C, completes R7 or Task 4, or permits production
exposure or an external acceptance claim.

## Scope and narrow supersession

This sidecar preserves the history and accepted A/B/C separation in the
[CurrentOnly join-custody amendment](2026-09-03-knife16-r7-current-only-join-custody-amendment.md).
It narrowly supersedes only that amendment's future clauses that require:

- an extra CurrentOnly-root `runtime.spawn`;
- a root-level `JoinHandle`/outer-result phase;
- a pre-spawn actor-transfer quarantine and rollback transaction; or
- a future parent that receives `JoinCustody<JoiningPhase>` rather than the
  whole actor-owning root custody.

The constructor commit contract remains owned by the
[post-extraction owner-root cutover amendment](2026-09-02-knife16-r7-post-extraction-owner-root-cutover-amendment.md).
This sidecar does not rewrite either accepted document or erase their review
evidence. Further Rust work remains separately gated.

## Historical causal RED and accepted implementation

The superseded pre-implementation prototype stopped at this exact causal RED:

- `owner/physical_session_root.rs`: `891` lines;
- `owner/physical_session_root/tests/current_only.rs`: `590` lines;
- the corrected B failpoint fires after the exact actor has been moved to a
  stack local and root phase has become `AdmissionUnknown`, but before the
  root-local task handle is published;
- the B assertion then correctly fails because the same concrete actor is not
  present in root state; and
- Tokio `1.52.0` binds the actor-bearing future, invokes a configurable spawn
  hook that may unwind, and only then returns the `JoinHandle`.

That RED remains the causal disproof of the extra-spawn seam. It is historical
failure evidence, not the current implementation state.

The separately authorized direct-join TDD then removed that seam and passed its
focused, repeated, regression, provenance, governance, and implementation-review
gates. Exact current lines are `owner.rs 2062`,
`physical_session_root.rs 722`, and `current_only.rs 691`. The dated local
result owns the exact evidence; this amendment continues to own the design.

The replacement is grounded in existing actor behavior:

- `StartedQuinnLegActor::join(&mut self)` already exists without an Interface
  change;
- it has exactly one await, Tokio `JoinSet::join_next().await`;
- Tokio `1.52.0` documents that await as cancellation-safe; and
- after that await is Ready, actor join has no second await or yield before it
  returns its complete result.

## Goal

Freeze the smallest Authority-local replacement in which:

1. CurrentOnly always physically owns the same `StartedQuinnLegActor`;
2. an observer borrows the root to drive the actor's existing join rather than
   moving the actor into a second task;
3. observer cancellation and pre-await unwind preserve real, nonzero actor
   ownership in the living root;
4. the complete join result becomes root state before any observation escapes;
   and
5. ultimate root destruction remains blocked on a real longer-lived parent.

## Non-goals

- No further Rust, test, governance, existing architecture, archive, commit,
  push, or external change is authorized by this implementation promotion.
- No new actor getter, callback, join entry point, visibility, dynamic dispatch,
  global lock, channel, nursery, or receiver.
- No terminal preservation, no-ACK finalizer, authenticated B, runtime restart
  credit, arbiter, no-oracle harness, or public TUN wiring.
- No whole-root exact-zero, Case C closure, VPS, WAN, throughput, or M2 claim.

## Root-held actor Authority

`OwnerPhysicalSessionRoot<T>::CurrentOnly` must own one stable, private,
non-optional actor custody. An equivalent Rust layout may be chosen during a
separately authorized implementation, but its semantic shape is:

```text
OwnerPhysicalCurrentActorCustody {
    actor: StartedQuinnLegActor,
    phase:
        Running
      | Joining { admitted_inventory }
      | Joined { admitted_inventory, exact_joined }
      | Failed {
            admitted_inventory,
            exact_error: QuinnLegActorError,
        }
}
```

The whole aggregate is the non-`Clone`, non-`Copy` owning capability because it
contains the exact actor. A copied inventory value is only an audit snapshot;
it is never actor, correlation, task, or cleanup Authority.

### Running

The root owns the exact running actor. Its actor-contained FIFO and nursery
remain inside that actor. Handoff, controller/output, Target executor,
capacity, and restart-credit state remain in the surrounding CurrentOnly root.

### Joining

After all fallible shutdown/FIFO/correlation checks pass, the root monotonically
publishes `Joining` while the actor is still physically in the same custody.
`Joining` retains the admitted exact inventory. The actor itself owns its
existing `JoinSet`; there is no root-local task or handle.

An observer future owns only a temporary mutable borrow of the root. It never
owns the actor, nursery, FIFO, joined result, or cleanup disposition.

### Joined

On success, `StartedQuinnLegActor::join` has already changed the actor's internal
phase to Joined and moved physical incarnation, terminal, failure, stamp, and
zero message/byte evidence into one `QuinnLegActorJoined`. The root may retain:

- the now-inert joined actor shell; and
- the exact `QuinnLegActorJoined` witness in root phase.

Those are not two actor Authorities. The shell no longer owns the physical
incarnation or live controller parts; the joined witness owns the result that
was moved out exactly once.

### Failed

A modeled `Err(QuinnLegActorError)` is moved exactly once into `Failed` with the
same actor and admitted inventory. The retained exact error is never cloned or
moved out. Repeated calls may only borrow it and map that borrow to the existing
stable root-level `ActorJoinQuarantined` observation/category. The root must not
call actor join again, restore Running, discard the error, or translate it into
clean zero.

An unknown unwind for which no modeled actor error exists remains in sticky,
nonzero `Joining`. It permits only same-actor roll-forward; it cannot fabricate
a category-only `Failed`, synthesize an error, or claim an exact panic outcome.

## A/B ordering and re-entry

### B. Pre-await monotonic-shutdown unwind

The new B ordering is:

```text
Running actor in root
  -> complete every fallible preflight
  -> publish Joining with actor still in root
  -> deterministic test-only failpoint
  -> synchronously request cancellation
  -> directly borrow-await actor.join(&mut self)
```

The failpoint is after `Joining` publication but before `request_cancel` and
before the join await begins. If it unwinds, the root still contains the same
concrete actor and exact nonzero inventory. It must not return to Running or
publish Joined, Failed, or zero without a real join result.

Re-entry is allowed only as roll-forward of this same actor and the same
Joining disposition. It is not a retry, a new admission, a new spawn, a seed
reconstruction, or a new lifecycle Authority. Re-entry resumes the monotonic
shutdown by calling the existing idempotent cancellation path and the same
actor join.

Because the actor never leaves the root, the old `AdmissionUnknown`, actor
rollback guard, pre-spawn transfer, and `Quarantined(actor)` handoff are removed
rather than repaired.

### A. Observer cancellation while join is Pending

Once direct actor join reaches its only await:

```text
root owns Joining + exact actor + actor-owned JoinSet
  -> observer temporarily borrows root
  -> JoinSet::join_next is Pending
  -> observer is dropped
  -> mutable borrow ends; no JoinSet task is removed
  -> root still owns Joining + the identical actor
  -> a later observer resumes the same actor join
```

`StartedQuinnLegActor::request_cancel` is repeat-safe: the first call changes
the actor from Running to Closing, and later calls observe non-Running and do
nothing. Safe Rust's mutable borrow also prevents concurrent calls on the same
actor.

## Result Authority and the root-side settle edge

Tokio cancellation cannot interleave arbitrary code inside one future poll.
When actor join's unique await becomes Ready, the actor completes its remaining
synchronous tail and returns:

```text
Result<QuinnLegActorJoined, QuinnLegActorError>
```

At the CurrentOnly seam, the first root-side state action must synchronously
store that entire value:

```text
Ok(joined)  -> Joined { exact_joined: joined, ... }
Err(error)  -> Failed { exact_error: error, ... }
```

The `Err` arm moves its `QuinnLegActorError` into `Failed` exactly once. Later
observations borrow that retained value and expose only the stable root-level
`ActorJoinQuarantined` category; they never clone or move out the exact error.

No root-side await, callback, logging, allocation, extra validation, observer
return, or fallible operation may occur between actor join returning and this
state write. Only after the value is root state may the method derive a
repeatable, non-owning observation.

`Joined` and `Failed` are terminal for this structural join entry. Neither may
invoke actor join again, restore Running, discard the retained value, or map
actor message/byte zero to whole-root zero.

This sidecar deliberately does not claim that the synchronous Ready tail inside
actor join is panic-infallible. It may format or allocate while mapping internal
failure. Without a modeled error, unknown unwind leaves sticky, nonzero
`Joining` with the actor still physically in the root and permits only
same-actor roll-forward. It does not mint a synthetic exact outcome or a
category-only `Failed`. A later implementation or review must not silently
strengthen that claim.

## Constructor and runtime disposition

The accepted constructor transaction is unchanged:

1. `QuinnOwnerInitialPhysicalRootSeed::into_parts()` is reversible
   decomposition, not commit.
2. Every modeled precommit rejection exact-returns the same actor, handoff, and
   controller through `from_parts()`.
3. Existing clock, runtime, FIFO, capacity, inventory, acceptance, and
   destination checks keep their accepted order.
4. `QuinnOwnerControllerOutputRoot::new(controller)` remains the final fallible
   operation and its success remains fused with infallible CurrentOnly
   publication.

In particular, `tokio::runtime::Handle::try_current()` and its
`RuntimeUnavailable` exact-return branch remain mandatory. Removing an extra
root-local spawn does not authorize deleting or reordering that validation.

The accepted direct-join implementation deleted the `runtime: Handle` that had
been stored only for the extra spawn. A cloneable Tokio handle is not unique
lifecycle Authority and was not retained through a meaningless read merely to
avoid a warning.

This does not reduce retained executor inventory. That inventory belongs to the
already validated handoff/Target executor retained through the controller/output
root. If a future design claims the stored Tokio handle is independently owned
Authority, it must first show a real inventory entry and a production caller
that needs it; this sidecar provides neither.

## Ownership and capacity inventory

Direct join adds no queue, channel, task, timer, allocation, or runtime credit.
The live and terminal inventory must continue to distinguish:

- the exact current actor and its actor-owned `JoinSet`;
- actor-contained FIFO messages, independently summed/stored bytes, and any
  ordered-delivery wait;
- CurrentOnly phase: Running, Joining, Joined, or Failed;
- exact retained joined witness or, in `Failed`, the exact retained modeled
  `QuinnLegActorError` that observations only borrow;
- handoff and attached physical identity;
- controller/output root and Target executor;
- endpoint and session Authority where the enclosing owner retains them; and
- restart credit explicitly `NotInstalled`.

There is no extra root-level JoinHandle, admission lease, outer task result, or
TaskLost inventory category after the old spawn layer is deleted. Actor join
already maps its internal nursery outcome into the canonical actor result.

Actor messages and bytes may be zero while the actor, Joining disposition,
joined/error result, handoff, controller/output, executor, endpoint, session,
or `NotInstalled` state remains nonzero. Unknown unwind remains sticky nonzero
in `Joining` and may only roll forward the same actor. No scalar snapshot can
substitute for the exact actor or result, and no category-only `Failed` may
stand in for a missing modeled error.

The accepted static capacity equations remain unchanged and necessary-only:

```text
N = 2 * (F + Z)
Q = 32
turns_per_attempt = ceil(N / Q)
A = S + R
max_scan_turns = turns_per_attempt * A
```

All additions and multiplications remain checked. Direct join consumes no
runtime restart credit.

## Case C remains open

Dropping an unjoined `StartedQuinnLegActor` calls cancellation and
`JoinSet::abort_all`. Dropping a Tokio `JoinSet` also aborts its tasks. Neither
operation awaits the actor nursery, retains the exact result, settles terminal
and FIFO ownership, or proves endpoint/session exact zero.

Therefore Case C remains a production-exposure hard gate. Before CurrentOnly
can gain a production caller or replace the legacy route, a symbol-first
architecture slice must identify a real longer-lived parent. That parent must
accept, through one consuming move, either:

- the complete `OwnerPhysicalSessionRoot<T>`; or
- its single private actor-owning custody in the current phase.

The parent is not named here because no production parent has yet been audited
and accepted. It must not receive a copied token, a synthetic handle, or a
second join Interface. A whole-root Drop test cannot close Case C.

## Rejected alternatives

### Runtime-no-hook constraint

Correctness may not depend on an undocumented assumption that the ambient Tokio
runtime has no spawn hook. `Handle` exposes no stable inspection proof for that
property, and the current Tokio path invokes the hook before returning the
handle. Encoding the assumption would hide a fragile external precondition
instead of owning lifecycle state.

### Pre-spawn receiver or root nursery

A receiver-first adapter could spawn a task before transferring the actor, but
it would add a channel, allocation, task, handle, outer result, and further
failure phases. No accepted production parent currently needs that seam. It
would be a hypothetical adapter and a second lifecycle layer, not a minimal
repair.

If a future audited Case C parent genuinely requires a nursery, that need must
be demonstrated in a separate architecture slice with exact capacity and
settlement evidence.

## Module depth and deletion test

The extra spawn layer is shallow Implementation: deleting it removes a task,
handle, callback window, outer error, and transfer state without changing the
caller-facing root entry. No caller needs to learn how actor join is driven.

Direct borrowed join keeps construction, actor custody, shutdown admission,
observation cancellation, result retention, and error disposition inside the
same private `physical_session_root` deep Module. Deleting that Module still
removes the entire CurrentOnly production foothold; deleting the extra spawn
does not require a re-export, pass-through, callback, or parameter elsewhere.

## Falsifiable failure tree

### Observer cancellation

Prediction: Pending observation can be dropped and later resumed against the
same actor and actor-owned nursery.

Falsifier: actor leaves the root, a second join/spawn is created, the nursery
task is removed on cancellation, or inventory changes merely because the
observer disappeared.

### Pre-await unwind

Prediction: the failpoint observes root phase Joining with the exact actor still
inside the root; unwind leaves that same state available only for roll-forward.

Falsifier: an empty/sentinel state appears, Running is restored, seed is rebuilt,
or only a copied inventory claims the actor still exists.

### Ready result retention

Prediction: the complete actor result is stored in Joined or Failed before any
root-side observation is returned.

Falsifier: raw result is returned or dropped first, another await/hook occurs,
Joined lacks its witness, Failed lacks its exact modeled error, or join can be
called again. It is also a falsifier if repeated observation clones or moves out
the exact error, or if unknown unwind fabricates a category-only Failed value.

### Constructor/runtime drift

Prediction: runtime validation and exact seed-return behavior are unchanged even
though the stored spawn Handle disappears.

Falsifier: `RuntimeUnavailable` is removed/reordered, a rejected seed changes,
or Target executor inventory is decremented with the Tokio field.

### Ultimate root destruction

Prediction: no A/B result is promoted to Case C without a real consuming parent
transfer and exact settlement.

Falsifier: actor Drop, abort, detach, leak, or a whole-root Drop test is called
clean shutdown.

## Implemented Rust scope and remaining boundary

The separately authorized implementation remained limited to:

- `src/owned_upstream/quinn_leg_composition/owner/physical_session_root.rs`:
  private actor custody/phase, shutdown admission, direct join, result/error
  retention, inventory, runtime-field removal, and test failpoint symbols; and
- `src/owned_upstream/quinn_leg_composition/owner/physical_session_root/tests/current_only.rs`:
  direct A/B/error tracers and their existing private fixture glue.

It did not change `quinn_leg_actor.rs`, the tests index, governance inventory,
provenance scanners, or caller Modules. The existing private route in
`owner.rs` remained `2062` lines; its closure-only adjustment was rustfmt-stable
Module ordering. Any future need for another Rust file, actor Interface change,
getter, visibility expansion, second join path, callback, or parent seam still
requires a new review.

The exact accepted sizes are:

- production child: `891 -> 722` lines; and
- CurrentOnly test leaf: `590 -> 691` lines.

The normal production/test limits remain `1500/800`; no grandfather entry may
be added.

## Executed RED/GREEN and retained gates

After independent sidecar acceptance and separate explicit Rust authorization,
the implementation executed this sequence:

1. Add one characterization that requires zero root-local `runtime.spawn`,
   `JoinHandle`, `AdmissionUnknown`, actor extraction, and root-level outer
   result in this transaction. The pre-implementation prototype REDed on those
   facts.
2. Preserve the corrected causal B RED as historical disproof of the old seam;
   replace its executable contract with Joining-before-cancel, same concrete
   actor retained, and same-actor roll-forward after the failpoint.
3. Implement the minimum actor-resident state and direct borrowed join. Do not
   change actor Interface or constructor validation.
4. Run B to GREEN, then A observer cancellation/re-entry and the existing
   constructor tracer independently.
5. Add or run one deterministic real inner-error tracer proving the exact error
   is moved into `Failed` once, remains stored there, is only borrowed by
   repeated `ActorJoinQuarantined` observations, and prevents later actor join.
   If this needs an actor Interface or third file, stop rather than widening
   scope.
6. Repeat A/B/error deterministically, then run the frozen owner-initial,
   continuation, actor join/FIFO, and structural-shutdown regressions.
7. Run Knife16 and repository provenance, format, tracked/untracked diff,
   library check, standalone/Cargo code-growth governance, and the same
   independent Reviewer.
8. Perform a bounded source audit proving the physical root has no extra spawn,
   handle, actor slot `Option`, actor-moving `mem::replace`, abort/detach bypass,
   or second join entry; also reconfirm actor join's unique await and synchronous
   Ready tail.

All eight stages are green at the accepted boundary. Exact counts and review
evidence live only in the dated local result. The same stop rule remains for
future work: the first unexpected compile/runtime/format/check/governance
failure, need for a wider Interface or third behavior file, result storage
after observation, actor extraction, constructor drift, warning suppression,
or attempted Case C claim stops the Writer.

## Independent architecture and implementation reviews

The first review found one P1: the draft allowed a category-only replacement
for the exact modeled `QuinnLegActorError`. The repair requires that error to be
moved into `Failed` exactly once and only borrowed by repeatable
`ActorJoinQuarantined` observations; unknown unwind instead remains sticky,
nonzero `Joining` with same-actor roll-forward.

The same independent Reviewer then reported no findings, P0/P1/P2 `0/0/0`,
Approve, with the prior P1 closed and Clean Architecture/System Design both
`10/10`. That verdict accepted only the sidecar architecture.

After deterministic implementation, the same Reviewer separately reported no
findings, P0/P1/P2 `0/0/0`, Approve, and scoped Clean Architecture, System
Design, and Reliability/lifecycle `10/10`. That later verdict accepts only the
CurrentOnly constructor and living-root A/B/error slice. Case C remains a
production-exposure hard gate that neither verdict nor score can waive.

## R7 status

R7 and Task 4 remain incomplete and necessary-only. The public TUN route remains
`LegacyOwnedUpstream`. Production terminal preservation, root-owned no-ACK
settlement, authenticated B topology, runtime credit, physical arbiter,
no-oracle byte harness, six-way whole-root exact settlement, macOS TUN, VPS,
WAN, throughput, and M2 remain open or forbidden at this gate.

## Source-of-truth links

- [Current status](../current/STATUS.md)
- [Direct-join local results](2026-09-03-knife16-r7-current-only-direct-join-local-results.md)
- [CurrentOnly join-custody amendment](2026-09-03-knife16-r7-current-only-join-custody-amendment.md)
- [Post-extraction owner-root cutover amendment](2026-09-02-knife16-r7-post-extraction-owner-root-cutover-amendment.md)
- [Hard-terminal extraction local results](2026-09-02-knife16-r7-hard-terminal-extraction-local-results.md)
- [Current physical-session root](../../src/owned_upstream/quinn_leg_composition/owner/physical_session_root.rs)
- [CurrentOnly focused tests](../../src/owned_upstream/quinn_leg_composition/owner/physical_session_root/tests/current_only.rs)
- [Quinn actor implementation](../../src/owned_upstream/quinn_leg_actor.rs)
- [Locked dependency versions](../../Cargo.lock)
