# Knife16 R6c Late-Record Closure Architecture Specification

Status: **ACCEPTED LOCALLY — necessary-only local TDD/coverage closure; no
production runtime behavior change; no TUN, WAN, throughput, or M2
acceptance**

Date: 2026-08-25

Result:
`docs/tech/2026-08-25-knife16-r6c-late-record-closure-local-results.md`.

## Stage goal

Close the bounded late-record matrix around one already committed A-to-B
replacement. A record which was validly carried by its originating physical
actor may finish late, but it must not:

- let the client-side `RetiredClientLeg` construct another session event for
  retired A;
- refresh current B's probe state;
- satisfy B's first positive application-ACK recovery barrier;
- release B replay or Target ownership;
- mutate the active session/Target snapshot; or
- leak actor message, byte, delivery-ACK, queue, receipt, or task ownership.

The covered record classes are DATA, application ACK, CLOSE, RESET,
ATTACH_ACCEPTED, ATTACH_GENERATION_STATUS, LEG_PROBE_ACK, and actor-local
DELIVERED. The proof must cross the production-shared fixed Quinn actor,
`LegIo`, `QuinnClientSessionComposition`, controller, reducer, and existing
owner/Target seam. A decoder-only or directly constructed reducer test is not
sufficient.

This stage is a necessary-only lifecycle result. It does not make the real TUN
entry select the two-leg transport, provide the production probe responder or
session nursery, close the R6d `<=500ms` component budget, implement R7-R9,
cover UDP two-leg switching, or justify a WAN/throughput/M2 run.

## Non-goals and trust boundary

- R6c does not make arbitrary authenticated peer behavior benign. A malformed
  frame, wrong route/lane, unknown or duplicate DELIVERED ordinal, or byte
  injected on another QUIC connection remains a protocol failure of the actor
  on which it arrived.
- A valid late A DELIVERED is meaningful only on A's own actor-private
  DELIVERED stream and pending-ordinal map. It cannot migrate onto B's QUIC
  stream. Treating an unknown ordinal on B as harmless would hide corruption.
- R6c does not accept OPEN/OPEN_RESULT as useful retired traffic and does not
  broaden the retired leg into an event-producing authority.
- R6c does not claim that a frame carrying B's exact physical seal and B's
  authenticated generation is an A frame based on payload intuition or local
  dequeue time. After the owner has authenticated/committed B, it may
  legitimately re-emit current cumulative ACK state under B's generation even
  if those bytes originally reached Target through A. Such a record may have
  queued behind ATTACH_ACCEPTED before the client publishes B and remains
  current-B traffic after publication. The protocol provenance boundary is the
  connection seal, generation, session, nonce/sequence where present, and
  actor-owned route—not a missing local receive timestamp.
- No queue, timeout, task, payload copy, or capacity increase is authorized by
  this stage.

## Grounded end-to-end routes

### Retired A inbound session/control record

```text
A peer DATA stream
  -> fixed Quinn A data reader
  -> one bounded EncodedDelivery owned by A actor
  -> QuinnClientSessionComposition::try_receive_retired
  -> exact retired-runtime/controller-incarnation check
  -> StartedQuinnLegActor::try_transact_controller_inbound
  -> LegIo::receive_classified_parts (route/decode/lane/exact A seal)
  -> ClientSessionController::receive_retired_from_quinn
  -> RetiredClientLeg::discard_late_inbound
  -> inert classification/rejection; never SessionSupervisor::apply
  -> if the physical header was ordered, actor A publishes DELIVERED only
     after classification; ordinary Active session records never reserve it
  -> exact A actor permits/bytes and any ordered ACK reservation release
```

Once `begin_retired_join` changes A from `Running` to `Joining`,
`try_receive_retired` must return `NotRunning` before dequeue. Cancellation and
join then own and release the queued record; they must not fabricate DELIVERED.

There are two deliberately different retired-A states. A clean normal
replacement can leave A's actor `Running`, so the matrix above yields typed
inert classifications. A post-loss/silent-loss A already has an actor
first-failure before it becomes terminal-bound retired ownership. Its fixed
children are therefore cancelling and any record queued across that boundary
is drained without semantic classification or fabricated DELIVERED; polling
may report the already-owned actor failure. Both results are correct only if B,
session, replay, and Target remain unchanged and all A ownership reaches zero.

### Asymmetric owner-B/client-A commit window

Owner and client publication are separate authenticated transactions. The
owner may already have committed B while the client still temporarily regards
A as current. A well-formed uplink record then follows a third, deliberately
different route:

```text
client-current A actor
  -> real Quinn A DATA stream
  -> owner-side physical A actor and exact A seal
  -> old owner AttachedLeg A constructs a peer-frame event
  -> OwnerTargetExecutor / SessionSupervisor already current at B
  -> exact SessionError::LegGenerationMismatch { current: B, actual: A }
  -> no Target effect, pending work, replay release, or owner snapshot change
```

This is reducer rejection, not the client-side retired inert sink. It proves
that a late physical A uplink cannot mutate the live Target during asymmetric
publication without conflating the two policies.

### Cross-incarnation record on current B

```text
B peer DATA stream carrying old-A generation/correlation
  -> fixed Quinn B data reader
  -> current-B actor and B route/seal classification
  -> QuinnClientSessionComposition::try_receive_current
  -> ClientSessionController::receive_current_from_quinn
  -> attach record: rejected before reducer
     leg control: exact current-probe correlation check, otherwise rejected
     session record: exact B seal binds event, reducer rejects old generation
  -> no accepted reducer effect, recovery observation, replay release, or
     Target mutation
```

The physical actor may acknowledge that it classified a well-formed record;
that DELIVERED fact is deliberately separate from resumable application ACK.

### Actor-local DELIVERED

```text
originating peer delivery writer
  -> originating actor's dedicated DELIVERED stream
  -> exact actor-private pending ordinal lookup
  -> exact route/lane match
  -> mark only that actor's enqueue receipt delivered
  -> release that actor's pending message/byte ownership
```

No controller, reducer, recovery barrier, replay receipt, or Target owner is
reachable from the DELIVERED reader. A late publication for an already
causally reconciled receipt is idempotent at the shared completion token; an
unknown/duplicate wire ordinal is not the same case and fails its actor.

## Exact policy matrix

| Arrival provenance | Record | Required local result | Forbidden effect |
| --- | --- | --- | --- |
| retired A actor, exact physical A seal, any decodable generation | DATA | inert retired discard | downlink write or Target/session mutation |
| retired A actor, exact physical A seal, any decodable generation | application ACK | inert retired discard | B recovery, replay release, ACK advance |
| retired A actor, exact physical A seal, any decodable generation | CLOSE | inert retired discard | close active B/Target flow |
| retired A actor, exact physical A seal, any decodable generation | RESET | inert retired discard | reset active B/Target flow |
| retired A actor | ATTACH_ACCEPTED | classified rejection before reducer | reinstall/resurrect A |
| retired A actor | ATTACH_GENERATION_STATUS | classified rejection before reducer | catch-up or generation change |
| retired A actor | LEG_PROBE_ACK | classified rejection before current liveness | refresh B or cancel B deadline |
| owner already at B, client/physical uplink still A | DATA/ACK/CLOSE/RESET | exact `LegGenerationMismatch { current: B, actual: A }` | Target/session mutation or owner pending work |
| current B actor with A generation/correlation | DATA/ACK/CLOSE/RESET | exact `FrameGenerationMismatch { current: B, actual: A }` after B physical classification | accepted session effect or recovery |
| current B actor with A generation/correlation | ATTACH_ACCEPTED/status | rejection before reducer | attach/catch-up publication |
| current B actor with A correlation | LEG_PROBE_ACK | rejection by full current correlation | B liveness refresh |
| current B actor with exact B generation after authenticated owner attach | application ACK or other session record | ordinary current policy; ACK may legitimately summarize Target state first accepted through A | classifying it as A merely because it queued before local publish |
| originating actor, known route/lane/ordinal | late DELIVERED | complete only pointer-identical physical receipt | application ACK/replay/Target change |
| any actor, unknown/duplicate or wrong-route DELIVERED | DELIVERED | actor-local protocol failure and bounded cleanup | completion of another actor's receipt |
| retired A after join starts | any queued raw record | no dequeue; cancellation/join releases ownership | fabricated DELIVERED or B disturbance |
| terminal-bound post-loss A whose actor first-failure already won | any queued raw record | actor-local drain/failure; no semantic classification or DELIVERED | second loss, reducer event, or B disturbance |

For each benign retired/rejected row, the session snapshot, Target snapshot,
current B routes, current B actor ownership, pending replacement application
ACK, and replay extents are unchanged. The originating actor's raw inbound
message/bytes and reserved DELIVERED ownership reach zero after classification.

## Ranked falsifiable hypotheses

1. A retired application ACK can still reach the reducer and satisfy the
   replacement barrier. Prediction: replay extents or
   `ReplacementRecovered` changes after `try_receive_retired`.
2. Attach acceptance/status and probe ACK use only numeric correlation and can
   cross the A/B incarnation boundary. Prediction: A is republished, B probe
   state becomes idle/ACKed, or generation changes. Initial inspection already
   rejects the narrow standby/current-probe nonce-collision variant because
   registered B uses A's generation, installed B uses the next generation, and
   current probe sequence authority is not reset on replacement.
3. Retired CLOSE or RESET constructs an event and disturbs the active Target.
   Prediction: Target flow state or active session effects change.
4. Physical DELIVERED is accidentally treated as application ACK. Prediction:
   a held/nonexistent B application ACK is bypassed after only the actor receipt
   completes.
5. An A DELIVERED ordinal can complete B ownership. Prediction: equal numeric
   ordinals on two actors change B's receipt/snapshot. The current actor-private
   pending maps predict isolation; the real test must prove it.
6. Rejection consumes or leaks the wrong ownership. Prediction: actor raw
   message/bytes/ACK reservation do not return to zero, or B ownership changes.
7. Retired join races one last dequeue. Prediction: a record is classified or
   acknowledged after `Joining`, or A cleanup changes B/Target state.

Each RED must name one prediction and use the public production-shared seam.
If a test is already green, it is evidence for the existing contract, not a
claimed repair; move to the next unproven hypothesis rather than manufacturing
a production change.

## Capacity and performance classification

R6c adds no data-path capacity. At most one already charged actor inbound
record crosses one synchronous composition transaction; the actor's existing
message, inbound-byte, composition-message, and delivery-ACK reservations
remain the bounds. The policy adds no payload clone and no additional loop,
timer, channel, task, or retry queue.

The product's eventual `100 Mbit/s` target is `12,500,000 bytes/s`; R6c neither
changes nor claims a loop/batch/flush capacity capable of that rate. The old
legacy TUN/smoltcp egress path remains active in production. Therefore this
stage is necessary for lifecycle correctness and explicitly insufficient for
the next Mbps gate, `<1s` recovery, Task 4, or M2.

## Deterministic feedback loop and TDD sequence

1. Add one real-Quinn post-install tracer for a retired A application ACK while
   B replay and its application-ACK barrier are live. It must fail if A can
   release replay, forge `ReplacementRecovered`, mutate Target, or leak actor
   ownership.
2. Extend one record at a time through retired A: DATA, CLOSE, RESET, then
   ATTACH_ACCEPTED, ATTACH_GENERATION_STATUS, and LEG_PROBE_ACK. Keep the same
   B/Target sentinels and verify exact per-record classification plus zero A
   raw ownership. Repeat the four data-plane records with both A and B wire
   generations: the exact retired physical seal remains the stronger inert
   sink and neither generation may reach the reducer.
3. Send the corresponding old-generation/correlation records over current B
   and prove reducer/liveness rejection without snapshot or replay change.
4. Hold a real actor DELIVERED publication across B commit/recovery. Prove it
   completes only its pointer-identical receipt and cannot substitute for the
   positive Target-backed B application ACK. Prove equal ordinals remain
   actor-private.
5. Race the last retired record with `begin_retired_join`: Running may perform
   one complete transaction; Joining performs none. Both branches release all
   A ownership and preserve B/Target.
6. Queue one genuinely ordered representative record at the post-loss boundary
   and prove terminal-bound A's already-won first failure drains it without a
   second loss or DELIVERED. This gate is category-independent because the
   actor returns the retained first failure before dequeue or controller
   classification; the full category policy remains covered before that gate.
7. Run focused controller/leg/LegIo/composition/actor tests, exact 100-repeat
   stress for the new real-Quinn tracer and join race, all-target default,
   provenance, concurrency, real-smoltcp, release, strict Clippy, rustdoc,
   vendored Quinn/proto, formatting, diff, and secret gates.
8. Perform a concentrated correctness/performance/lifecycle/backpressure/
   regression review before updating result, handoff, TODO, and learning files.

The production active actor queue intentionally cannot mint a post-acceptance
ATTACH_ACCEPTED or ATTACH_GENERATION_STATUS. Those two defensive rows therefore
use the production-shared `EncodedDelivery -> LegIo -> controller` seam as an
auxiliary matrix; adding a raw production injection API merely for a test is
out of scope. The primary data-plane, probe, DELIVERED, join, first-failure,
replay, and Target proofs all cross real Quinn actors.

Only one new failing tracer is introduced at a time. Production code changes
must be the smallest ownership or classification repair which makes that
specific invariant green.

The existing production contracts already satisfy this matrix. The accepted
stage therefore changes only deterministic tests and test helpers; it does not
change production Rust behavior.

## Acceptance invariants

1. The client-side `RetiredClientLeg` has no API capable of constructing
   `SessionEvent`; this does not conflate the separately tested owner-B/client-A
   asymmetric window, where the old owner A event is constructed and then
   rejected by the B-current reducer.
2. The four allowed retired data-plane records are inert and exactly typed for
   the exact retired physical actor regardless of their claimed generation;
   attach responses/status and probe ACK are rejected before active policy.
3. An old physical A event at an owner already on B fails with exact
   `LegGenerationMismatch`; an A-generation record on current physical B fails
   with exact `FrameGenerationMismatch`. Neither can produce an accepted
   reducer effect, recovery observation, replay release, or Target mutation.
4. Current probe ACK matching includes exact B connection seal, session,
   generation, epoch nonce, sequence, and switch epoch.
5. Physical DELIVERED completes only an actor-private route/lane/ordinal
   receipt and never advances resumable application ACK.
6. Every classified benign record releases the originating actor's raw
   message, bytes, composition permit, and delivery-ACK reservation exactly.
7. Malformed/cross-connection bytes fail only the actor that observed them;
   they are not mislabeled as a benign retired record.
8. Once retired join begins, no late record is dequeued and no physical
   DELIVERED is fabricated for dropped ownership.
9. A cleanup or actor-local failure cannot clear/replace current B, alter its
   routes/ownership, or mutate the active Target.
10. All new state and probes remain bounded, non-secret, and panic-free on the
    production hot path.

## Stop rules

- Stop if any late A input can enter the active reducer or current-liveness
  policy.
- Stop if physical DELIVERED releases replay or satisfies recovery.
- Stop if a benign rejection leaks ownership or if join acknowledges data it
  discarded.
- Stop if making arbitrary unknown/duplicate DELIVERED benign would be needed;
  that would weaken protocol corruption detection.
- Stop and report the exact missing seam if the active Target/replay barrier
  cannot be observed through the production-shared composition. Extract that
  seam before adding speculative policy.
- Even after all local gates pass, report R6c as necessary-only and proceed to
  R6d. Do not run macOS TUN, VPS, WAN, throughput, or M2.
