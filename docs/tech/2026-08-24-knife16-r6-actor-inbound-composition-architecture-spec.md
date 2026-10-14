# Knife16 R6 Actor-Inbound Composition Architecture Specification

Date: 2026-08-24

Status: **IMPLEMENTED AND LOCALLY ACCEPTED; NECESSARY-ONLY; ACTUAL-CLOSE,
SILENT-FAILURE, R7-R9, TUN, VPS, WAN, THROUGHPUT, TASK 4, AND M2 NOT
ACCEPTED**

## Stage goal

Close the production-shared receive seam between one fixed Quinn actor and the
exact `ClientSessionController` transport installed from that actor. One
bounded raw DATA record read by Quinn must be able to cross, without exposing
raw bytes or policy authority, through:

```text
Quinn RecvStream
  -> fixed actor InboundRecord (Im + Ib + Cm + optional Am/Ab)
  -> QuinnClientSessionComposition exact-incarnation transaction
  -> ClientSessionController current/retired policy slot
  -> LegIo route/decode/lane provenance
  -> ClientActiveLeg apply or RetiredClientLeg inert discard
  -> optional physical DELIVERED commit
```

The transaction must preserve the existing pre-decode recovery barrier,
physical ordered-delivery meaning, application-ACK separation, exact actor
identity, bounded ownership, and cooperative shutdown.

## Non-goals and necessary-only classification

This stage does not bind an actor failure or terminal to the controller, emit
`LegLost`, start recovery, select a standby actor, create a production session
nursery, or add a TUN entry. Those require the later actual-close and
silent-failure transactions. It also does not implement R7 reverse-only
switching, UDP two-leg behavior, WAN qualification, throughput acceptance, or
M2.

The result is therefore a necessary bidirectional composition seam only. It
cannot by itself make the one-second continuity test pass.

## Historical pre-fix break and accepted repair

`QuinnClientSessionComposition::bootstrap_initial` consumes the fixed actor's
sole `LegIo` and FIFO into `ClientSessionController`. The actor correctly
retains only its Quinn sender, inbound receiver, counters, cancellation, and
nursery. Before this slice, `StartedQuinnLegActor::recv_classified` dequeued an
`InboundRecord` and then required the actor-local `self.io`. After composition
that field is intentionally empty, so the old receive path terminated as
`AlreadyJoined` after removing a record from the bounded queue.

The controller's existing `receive_current` and `receive_retired` paths are
otherwise the correct policy path, but they accepted only an
`EncodedDelivery` produced by the memory adapter and took a caller-supplied
feature set. The accepted repair adds one gate-owned adapter and keeps the old
feature-taking actor receive entry only for pre-composition bootstrap/tests;
the composed controller receive entry cannot accept caller feature authority.

## Deep interface and dependency direction

The composition exposes synchronous, one-event operations. The complete
drive-result surface is:

```text
try_receive_current()
  -> Idle | PolicyBlocked | Applied{effects, observation} | Rejected
   | ReducerRejected | SessionShuttingDown | MissingCurrent
   | CompositionInvariant | ActorFailed

try_receive_retired()
  -> Idle | Retired(kind) | Rejected | SessionShuttingDown | MissingRetired
   | NotRunning | CompositionInvariant | ActorFailed
```

They expose no `InboundRecord`, `Bytes`, `EncodedDelivery`, `BoundInbound`,
permit, receiver, `LegIo`, FIFO, actor sender, or nursery. The fixed actor
accepts the unforgeable composition gate and runs a synchronous callback with
one adapter-minted encoded delivery. Raw ownership never escapes the actor /
composition transaction.

The controller stores the authenticated installed `FeatureSet` at initial
bootstrap. Registered replacement already requires identical session
semantics, including negotiated features, so current and retired decode use
that local fact. No production **composed-controller** receive call accepts a
feature set supplied by the outer root or incoming record. The older actor-
local `recv_classified(FeatureSet)` remains production-compiled only for the
pre-composition bootstrap seam and is not reachable through
`QuinnClientSessionComposition`.

The controller reducer/policy core remains independent of Quinn, Tokio tasks,
and actor lifecycle. Its narrow adapter surface depends only on the opaque
`QuinnLegCompositionGate`, not on Quinn values or tasks. The actor remains
independent of reducer policy. The composition is the only module allowed to
pair their exact physical-incarnation halves.

## Synchronous transaction and cancellation

The public owner loop never awaits while holding a dequeued record. It first
checks:

1. composition phase is `Running`;
2. current controller policy is not `Recovering` or `TerminatingCurrent`;
3. the selected controller slot and actor share the exact private physical
   incarnation; and
4. the actor has no previously published fatal cause.

Only then may it try to consume at most one record with `try_recv`. Empty is a
bounded idle result. After dequeue, the actor first-failure linearization lock
covers delivery construction, controller route/decode/lane/bind/apply, and
the physical DELIVERED decision. There is no `await` in that interval.

If the controller defensively returns `Blocked`, the actor restores the exact
non-cloneable delivery into one private pending slot. That slot is retried
before the mpsc receiver and retains the original queued marker, Ib/Cm permits,
payload backing, and optional DELIVERED reservation. Push-back into mpsc is
forbidden because it could reorder records. Caller cancellation can therefore
lose only an observation; it cannot strand a record between dequeue and
commit.

The expected normal barrier path does not dequeue at all. The pending slot is
only a defensive ownership closure for a future predicate drift or an exact
blocked result returned by the controller.

## DELIVERED versus application ACK

The fixed actor's `DELIVERED` record remains a physical ordered-delivery fact:
the exact route and lane passed, bytes decoded under locally installed
features, and `LegIo` minted this connection's provenance. It is committed
exactly once after that classification succeeds. It may therefore precede a
higher-level `Rejected` or `ReducerRejected` disposition, as in the existing
actor-local behavior.

It is not a resumable protocol `Record::Ack`, does not advance a byte offset,
does not release replay, and cannot emit `ReplacementRecovered`. Application
recovery still requires the controller/supervisor's exact positive ACK-range
receipt after sink/Target acceptance.

`PolicyBlocked`, wrong physical actor, route/lane/decode error, actor fatal,
and pre-classification cancellation never commit DELIVERED. A `LegIoError`
becomes the actor's exact first fatal and requests cooperative cancellation,
but this stage does not convert that fatal into a controller terminal event.

## Current and retired routing

Current receive validates only the controller-current policy half against the
current actor runtime half. It must remain driveable while retired A is in the
composition-owned `Joining` task, where no borrowable retired runtime identity
exists.

Retired receive validates only a still-running retired actor against the
controller-retired policy slot. It may decode and inertly discard only the
existing allowed late DATA/ACK/CLOSE/RESET kinds. It never applies a reducer
event. Once retired join has started, no further retired dequeue is admitted;
the actor's cooperative cancel/join path drains or drops retained records and
proves zero ownership before controller clear.

## Capacity and progress math

This bridge adds:

```text
new tasks                 = 0
new channels/queues       = 0
new message permits       = 0
new payload allocation    = 0
new payload byte copies   = 0
private pending slots     <= 1 per actor, holding already-counted ownership
```

The fixed checked envelope remains:

```text
messages <= Qm + Om + Im + Cm + Am + 2
bytes    <= Qb + Ob + Ib + Ab + 18 + W + 18
```

For the existing composition fixture this remains exactly `22 messages` and
`852,076 bytes` per leg. A pending record merely moves an already-counted
message out of the mpsc receiver; it retains its queued marker and permits.

The data reader advances the actor's existing readiness epoch only after a
complete record is published into the bounded inbound receiver. Publication
uses a monotonic maximum, so producers that reserve epochs in one order but
publish in the opposite order cannot roll the watch value backward or strand
a waiter. A future
single-owner root can therefore run bounded receive/flush/recovery work, then
await an existing readiness/cancellation signal without a polling loop. This
stage adds no pump task and makes no Mbps sufficiency claim.

## TDD tracer bullets

The positive bridge tracer is intentionally part of the complete real A-to-B
composition lifecycle test:

```text
exact_clean_join_of_retired_a_clears_only_a_while_active_b_keeps_exchanging
```

It reuses the real localhost A-to-B registered replacement fixture. Before
replacement, the client opens one local flow. After B is installed but while
its controller remains `Recovering`, owner B sends a valid `OpenResult` over
the real Quinn actor. The tracer must prove:

1. actor B has one complete bounded raw record with Ib/Cm held;
2. current receive returns `PolicyBlocked` without dequeue, LegIo counter
   change, supervisor change, or DELIVERED;
3. completing bounded recovery admission does not consume or duplicate it;
4. the next current receive classifies and applies that exact record once,
   producing the expected local-open resolution;
5. the following receive is idle and Ib/Cm return to zero; and
6. both peers still cooperatively join with zero ownership.

That exact test is run in isolation for the 100-repeat gate. Focused follow-ups
add one real ordered leg-control record to prove physical
DELIVERED timing and application-ACK separation, wrong-current and
wrong-retired pre-dequeue rejection, retained late-A discard, decode/fatal
race, readiness waiter cancellation, final-shutdown rejection, and repeated
zero-ownership cleanup.

The independent negative tracer
`real_quinn_composition_wrong_route_is_first_fatal_never_acknowledges_and_joins_zero`
forces a controller-owned `LegIo` wrong route after real actor dequeue. It
proves that the composition publishes the exact first fatal before any
physical DELIVERED commit and joins with zero ownership. The readiness
regression is
`readiness_publication_cannot_roll_back_or_lose_a_waiter_when_producers_finish_out_of_order`.

## Failure discriminators

1. `AlreadyJoined` after a real record: the bridge still depends on actor-local
   `LegIo`.
2. LegIo counters move while recovery is blocked: the barrier occurs after
   dequeue/decode.
3. DELIVERED becomes positive while blocked or on decode failure: physical
   acceptance is premature.
4. A raw type or permit reaches an unrestricted sibling/test extractor: the
   adapter provenance seam is forgeable.
5. A second record is consumed while the first is blocked: pending ordering or
   bounded backpressure is broken.
6. Active B stops receiving while retired A joins: receive used the whole
   layout predicate instead of current-only identity.
7. Actor join reports residual ownership after cancellation: pending or ACK
   reservation was not part of actor cleanup.

## Stop rules

Stop rather than force GREEN if the implementation requires a pump task, a
second channel, mpsc push-back, a payload clone/copy, caller-provided
production features, a raw delivery/permit extractor, an await after dequeue,
capacity/timeout growth, DELIVERED-as-application-ACK, actor-fatal-to-`LegLost`
conversion, test-only memory ingress for the acceptance tracer, or a TUN/VPS/
WAN/throughput/M2 claim.

## Local acceptance gate

- focused actor, controller, `LegIo`, composition, and provenance tests;
- exact first tracer plus concentrated barrier/ACK/identity/fatal/cancel
  regressions and a 100-repeat real-loopback run;
- all targets/all features, ignored concurrency sweeps, real-smoltcp repeat,
  release/check/strict Clippy/rustdoc/rustfmt/diff;
- vendored Quinn/proto gates if the bridge changes their reachable contracts;
- changed-secret and generated-artifact checks; and
- independent correctness, lifecycle/backpressure, and scope reviews with no
  unresolved P0/P1 before any external run.

## Accepted result

The bridge and its focused/broad local gates pass. The exact result, review
repairs, test counts, and remaining boundary are recorded in
`docs/tech/2026-08-24-knife16-r6-actor-inbound-composition-local-results.md`.
No TUN, VPS, WAN, throughput, or M2 run was performed.
