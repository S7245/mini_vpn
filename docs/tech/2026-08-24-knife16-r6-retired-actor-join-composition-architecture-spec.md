# Knife16 R6 Retired-Actor Join Composition Architecture Specification

Date: 2026-08-24

Status: **DESIGN IMPLEMENTED AND LOCAL TRACER ACCEPTED; NECESSARY-ONLY; R6
REMAINDER, R7-R9, TUN, VPS, WAN, THROUGHPUT, TASK 4, AND M2 NOT ACCEPTED**

Local result:
`docs/tech/2026-08-24-knife16-r6-retired-actor-join-composition-local-results.md`.

## Stage goal

Introduce one crate-private, production-shared composition module that owns a
`ClientSessionController` together with the fixed Quinn actor incarnation
whose exact `LegIo` and sole FIFO the controller installed. After A is
successfully replaced by active B, the module must:

1. keep A's sole retired slot occupied while A still owns any actor task,
   connection, Endpoint-map entry, driver reference, socket, message, or byte;
2. synchronously prove before cancellation that the retained actor owner is
   the exact incarnation pre-bound to controller-retired A;
3. cooperatively cancel and really join only that A actor;
4. consume the exact joined incarnation and terminal once to clear only A;
5. emit no `LegLost`, `ResumeGraceStarted`, or other reducer event; and
6. leave current B's generation, route, recovery state, queue, actor, and
   production-shared outbound ordered exchange unchanged, including while A's
   close tail is still pending.

The user-visible fault this removes is permanent retired capacity after the
first successful A-to-B replacement. Today the old A slot is never cleared,
so every later replacement is rejected as `RetiredCapacityExceeded` even
after A has physically terminated.

## Necessary-only classification

This transaction is a necessary lifecycle and bounded-capacity precondition.
It is not sufficient for complete R6, Task 4, production reachability, a
sub-second blackout result, or the M2 one-second continuity acceptance.

The real TUN entry still selects the legacy upstream. R6 actual-close,
silent-failure, composition deadline, and recovery-capacity closure remain
open after this stage. R7 reverse-only no-oracle switching, authenticated
hints, rejection, and trigger-race arbitration remain separate; R8-R9, UDP,
the enhanced Target owner, WAN, and M2 also remain open.

## Existing split seam

The current tree has two independently accepted halves:

- `ClientSessionController` can atomically install B and move A into one
  bounded retired logical slot, but that slot stores no physical actor
  incarnation and has no clear transaction.
- `StartedQuinnLegActor::join` can mint one exact
  `QuinnLegActorJoined` only after structural await and resource release, but
  the actor owns its own `LegIo` and FIFO and has no composition owner binding
  them to the controller.

A controller-only method accepting an arbitrary Quinn joined value would be a
false seam: a caller could mix a memory-controller A with an unrelated real
actor. A terminal-only post-join comparison is also insufficient. If active B
is selected by mistake, calling join first already cancels B even if the later
terminal comparison correctly refuses to clear retired A.

## Module and dependency direction

```text
resumable reducer / ClientSessionController / LegIo policy
                         ^
                         | physical-incarnation identity and exact terminal
                         |
        client Quinn session composition module
             /                         \
 fixed Quinn actor adapter       controller owner
             ^
             |
       Quinn / Tokio / UDP
```

The controller must not depend on Quinn, Tokio, `JoinSet`, or an actor future.
A transport-independent, factory-minted physical-incarnation identity may
travel inside `LegIo` and its controller-owned active/retired transport. The
outer composition module is the only adapter allowed to pair that identity
with `StartedQuinnLegActor` and `QuinnLegActorJoined`.

The composition interface must be deep: callers may bootstrap an already
authenticated initial actor, install an already authenticated registered
replacement, enqueue/flush the current controller path, start one retained
retired-A join, and non-blockingly poll its joined/clear commit. They may not
extract raw `LegIo`, FIFO, encoded actor transport, actor incarnation,
terminal, controller mutable reference, nursery, or cleanup-task handle.

Final current shutdown has its own monotonic composition state:
`Running -> ShuttingDown -> Joined`. `ShuttingDown` is published before the
first await that asks current B to enter actor `Closing`. Cancelling that
caller cannot reopen admission: session-frame/control enqueue, flush,
recovery drive, standby registration, registered replacement, and retired
join mutation all reject or make no progress outside `Running`. Only the same
current-join transaction may resume `ShuttingDown` to `Joined`.

This slice does not add the typed actor-raw-inbound to controller bridge.
After `LegIo` moves into the controller, the old actor-local classified receive
port is intentionally not a production claim. Closing that bidirectional
composition seam remains a later R6 gate; the real TUN entry remains legacy.

## Physical-incarnation binding

The fixed actor factory allocates one opaque, non-caller-mintable incarnation
identity before it creates `LegIo`. The same identity is retained in two
non-interchangeable ownership halves:

- the runtime half retained by `StartedQuinnLegActor` and moved into
  `QuinnLegActorJoined`; and
- the policy half carried inside the exact `LegIo` consumed by
  `ClientSessionController`.

The identity must not be serialized, accepted from configuration, derived
from `LegId`, generation, route, address, or certificate, or made a public raw
integer. Exact connection seal and endpoint terminal matching remain a second
independent discriminator; incarnation equality never substitutes for the
terminal proof.

When B installation commits, the composition transaction moves the exact
current A runtime half into its one retired runtime slot in the same
infallible post-commit section in which the controller has already moved A's
policy half into its retired slot. B becomes current in both places. No
fallible or awaiting operation may occur between those two publications.

## Retired transaction state machine

```text
NoRetired
   |
   | successful A -> B controller commit plus infallible actor-slot move
   v
RetiredRunning(A)
   |
   | pre-cancel controller-policy identity == runtime A identity
   | move A into the composition's sole bounded cleanup task
   v
RetiredJoining(A)
   |
   | non-blocking poll; B controller/actor remain driveable
   | real structural/resource join completes
   v
RetiredJoined(A, joined witness retained)
   |
   | synchronous recheck of identity + exact terminal discard
   v
NoRetired
```

The composition, not a caller waiter, owns `RetiredJoining`. Dropping or
cancelling a caller-side drive loses only an observation; the exact task and A
remain retained. While it is pending, retired controller capacity stays
occupied and current B enqueue/flush continues without borrowing across A's
await. A semantic actor join error returns the exact actor to `RetiredRunning`
for retry. Cleanup-task panic/abort loses no capacity: it enters a sticky
fail-closed task-loss state and can never clear the controller slot.
`RetiredJoined` retains the unique joined witness if controller validation
rejects, so a mismatch cannot lose evidence or free capacity. There is no
await after the joined witness is persisted and before the synchronous
controller clear attempt.

The pre-cancel check covers both sides of the composition invariant:

```text
controller.retired physical identity == retained retired actor identity
controller.current physical identity == retained current actor identity
```

Failure returns without cancelling either actor and without touching either
controller slot.

## Exact controller clear

The controller clear operation may inspect and remove only its `retired`
field. It must not call `bind_current_terminal`,
`discard_terminating_current_recovery`, `PendingLegLoss::into_event`, or any
supervisor reducer method.

The synchronous transaction is:

1. retain the supplied joined identity and terminal on every rejection;
2. take the retired slot only inside the controller mutation;
3. compare its stored physical identity;
4. call `RetiredClientLeg::discard_terminal` for exact seal, endpoint, and
   closed-state validation;
5. restore the complete slot and joined inputs on any mismatch; or
6. drop the inert retired `LegIo` and publish one typed cleared fact.

The current slot, controller phase, pending replacement application ACK,
supervisor snapshot, and recovery ownership are outside this mutation.

## Capacity and task accounting

This stage adds no data queue, replay buffer, payload copy, transport task, or
unbounded notification. Per composed session it owns exactly:

```text
runtime actor slots <= 1 current + 1 retired
joined witnesses    <= 1, only while retired clear is pending
controller retired  <= 1 (unchanged)
extra payload bytes == 0
extra cleanup tasks <= 1, only while retired physical join is pending
```

The fixed actor's existing one root plus four children and checked message/
byte bounds remain unchanged. The cleanup task owns the already-counted whole
retired actor and introduces no payload queue, byte permit, or actor child.
Its handle is private, retained across caller-side polling cancellation, and
never multiplies for repeated start calls. Retired capacity becomes reusable
only after the exact structural/resource join and synchronous controller
clear. The composition's synchronous `Drop` is explicitly an unclean
fail-closed fallback: it requests task abort but cannot await Tokio
cancellation. Therefore only the explicit begin/poll retired join followed by
the explicit current join may claim structural cleanup; a future production
composition root must drive those paths and must not claim clean shutdown from
`Drop`.

No throughput claim follows from this O(1) ownership closure. It fixes a
necessary capacity leak; it does not establish the continuous progress loop
or byte rate required for 30, 100, or 170 Mbit/s.

## First tracer bullet

The one first RED is:

```text
exact_clean_join_of_retired_a_clears_only_a_while_active_b_keeps_exchanging
```

It uses the real localhost Quinn factory and shared listener, not a generic
future, memory actor, test-minted incarnation, terminal, or joined witness.
The production composition module installs actor A's own exact `LegIo`/FIFO
as the initial controller transport, then installs a genuinely registered B
using B's own actor transport.

The test must:

1. reach active B with A occupying both exact retired slots;
2. snapshot supervisor state, B generation, B route, and B actor identity;
3. independently reject a wrong current actor and wrong retired actor before
   either can be cancelled, even when the wrong actor reuses A's `LegId`;
4. block A inside structural join, prove capacity remains occupied, and
   complete a real ordered B leg-control exchange with a positive typed
   delivery receipt while that close tail is pending;
5. queue a second exact ordered B sentinel and retain its typed receipt before
   A clears, then complete A's join and retired-only synchronous clear;
6. prove the retired route is absent while every saved B/controller fact is
   unchanged and no `LegLost`/`ResumeGraceStarted` effect exists;
7. flush and decode the exact pre-clear B sentinel after A clears, including
   its positive typed ordered-delivery completion;
8. cancel one caller-side current-join await, prove the now-closing B refuses
   session/control admission, flush, recovery drive, standby registration,
   registered replacement, and retired mutation, then resume and
   cooperatively join both current-B peers with zero ownership before listener
   shutdown; and
9. prove the joined B continues to reject queue admission.

The expected initial RED is a compile failure: the production composition
module, dual incarnation binding, actor/controller ownership split, and
retired clear interface do not exist.

## Ranked failure discriminators

1. **No correct composition seam:** actor and controller each still own a
   separate `LegIo`/FIFO; the test cannot construct a bound session without
   test minting. Stop and deepen the module before behavior work.
2. **Wrong actor chosen before cancellation:** pre-cancel identity mismatch
   must return with A and B both running. Any B cancellation is a P0 isolation
   failure.
3. **Premature retired reuse:** a later replacement succeeds before A's joined
   witness and controller clear. This is a capacity/lifecycle failure.
4. **False logical loss:** supervisor snapshot or effects change during A
   clear. This proves the clear path reused current `LegLost` authority.
5. **Active-B disturbance:** B route/generation changes or its next ordered
   exchange fails after A clear. This is a slot-selection or shared-listener
   isolation failure.
6. **Polling cancellation consumes ownership:** stopping a caller-side poll
   loses the task, actor, or unique witness. The owner transaction is not
   structurally retained.

## Stop rules

Stop instead of forcing GREEN if the implementation requires:

- a test-only joined/incarnation/terminal constructor;
- binding by raw `u64`, `LegId`, generation, route, address, or certificate;
- exposing raw `LegIo`, FIFO, encoded transport, controller mutation, or
  nursery to an unrestricted sibling;
- cancelling any actor before both current and retired pre-bindings match;
- clearing retired on terminal, channel close, timeout, abort, Drop, or
  `is_finished` without successful actor join;
- applying `LegLost` or any reducer event for already-retired A;
- releasing retired capacity before actor join/resource cleanup;
- an await after consuming joined but before persisting or clearing it; or
- a TUN, VPS, WAN, throughput, Task-4, or M2 claim from this local slice.

## Local acceptance

- the single real-loopback RED becomes GREEN through production-shared code;
- exact wrong/stale actor facts are ownership-preserving and mutate nothing;
- caller-side polling can stop and resume while the same retained cleanup task
  continues, without blocking B progress;
- retired capacity remains occupied until exact joined clear;
- B completes real typed ordered leg-control delivery both while A join is
  blocked and after A clears;
- cancelled and completed current shutdown both retain the monotonic
  composition barrier and reject every later admission/mutation path;
- actor, controller, standby, supervisor, provenance, concurrency, real-
  smoltcp, all-target, release, strict Clippy, rustdoc, vendored Quinn/proto,
  fmt, diff, secret, and generated-artifact gates pass; and
- independent lifecycle, composition, and documentation review reports no
  unresolved P0/P1.

## Design score

The current uncomposed seam is **5/10** for this specific lifecycle problem:
both halves are individually deep, but no interface preserves locality across
their ownership transaction. The target after this stage is **8/10**: one
production-shared composition module owns the atomic mapping and exact clear,
while full R6 actual-close/silent-failure/deadline/capacity work remains open.
It cannot score 9/10 or 10/10 before those later gates and production
reachability exist.
