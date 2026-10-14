# Knife16 R6b Silent-Probe And Failure-Arbitration Architecture

Date: 2026-08-25

Status: **LOCALLY ACCEPTED AS NECESSARY-ONLY R6b; TASK 4 AND M2 NOT ACCEPTED**

Accepted local result:
`docs/tech/2026-08-25-knife16-r6b-silent-probe-arbitration-local-results.md`.

## Stage goal

Close the local silent-failure transaction that R6a deliberately left open:

```text
exact registered B
  -> exact B LEG_PROBE is physically submitted
  -> matching B LEG_PROBE_ACK mints bounded fresh-B authority

current A LEG_PROBE is physically submitted
  -> matching A ACK cancels the exact timer
  OR exact outstanding A timer expires while A remains physically open
  -> expiry and a spontaneous transport close share one actor first-failure winner
  -> one exact terminal -> one existing LegLost -> one switch epoch
  -> only fresh exact B may begin the existing post-loss attach/replay transaction
```

The controller/composition must infer failure only from authenticated wire and
timer capabilities it owns. The deterministic blackout injector is test
infrastructure only; it cannot call a switch, construct a terminal, or create a
`SessionEvent`.

## Non-goals

- reverse-only application-ACK stall detection or authenticated switch hints
  (R7);
- the complete R6c late-record matrix for DATA, ACK, CLOSE, RESET, attach
  acceptance, and generation status;
- the checked end-to-end `<=500ms` recovery-budget sum and production session
  nursery/root (R6d);
- the production owner-side `LEG_PROBE` responder and the production root that
  continuously schedules both current and standby liveness;
- UDP two-leg switching, real TUN reachability, WAN, VPS, throughput, macOS
  TUN, or M2 acceptance;
- timeout, MTU, QUIC, D16, Endpoint, Cubic, GSO, workload, or SLI tuning.

This is a necessary-only lifecycle stage. Passing it cannot by itself prove
that the one-second uninterrupted M2 test will pass.

## Grounded reachability gap

The production-shared codec already classifies and authenticates fixed-size
`LEG_PROBE` and `LEG_PROBE_ACK` records with exact session, generation,
leg-epoch nonce, and monotonic sequence. `LegIo` preserves the exact local leg
seal, the Quinn actor owns the one ordered leg-control FIFO, and
`LegControlEnqueued` exposes positive writer completion. R6a already owns the
sole real-terminal -> controller `LegLost` -> post-loss B attach/replay/first
application-ACK transaction.

The missing seam is ownership, not wire format:

- `ClientActiveLeg::accept_inbound` currently rejects every leg-control record,
  so a valid current-A probe ACK cannot reach liveness policy;
- no controller state owns one outstanding probe, deadline token, switch epoch,
  or stale-token rejection;
- registered B currently proves authentication and queue liveness only; it
  does not prove recent bidirectional reachability;
- `QuinnClientSessionComposition` does not yet own registered B and therefore
  cannot enforce that silent recovery consumes the exact fresh capability;
- requesting actor cancellation without first publishing a semantic failure
  yields `LocalCancelled`, which the running recovery path correctly rejects;
- a bare `SimTime` or a test callback could otherwise become a false fault
  oracle.

Therefore the first implementation step is a branch-by-abstraction deepening:
one controller-owned liveness state plus one composition-owned registered-B
slot. A true silent-blackout tracer is allowed only after that seam exists.

As implemented locally on 2026-08-25, the composition now consumes the whole
unregistered B actor before registration authority or the first registration
byte can escape. It owns registration, exact B probe freshness, signed ATTACH,
the authenticated accepted response, install, failure classification, and
split-phase cleanup. The remaining reachability gap is the production session
root/responder and its real entry-point composition, not the local actor seam.

## Ranked falsifiable hypotheses

1. A correctly encoded current probe ACK is decoded as leg control and then
   rejected by the active-leg session-only gate. Prediction: wire/control
   counters advance but liveness remains unchanged.
2. Arming at queue admission rather than positive writer completion can kill a
   healthy leg while its bounded FIFO is blocked. Prediction: an undelivered
   receipt would expire without any probe on the transport.
3. Comparing only generation and sequence lets a wrong seal, session, attach
   epoch, or prior switch epoch refresh the current leg. Prediction: a stale
   replay changes the freshness/deadline snapshot.
4. Publishing timeout directly to the reducer races the actor's real close and
   creates two logical losses or strands one physical live A. Prediction: the
   race produces more than one `ResumeGraceStarted`, terminal, switch epoch,
   nonce, or attach.
5. Treating `STANDBY_ACCEPTED`, endpoint-open, or queue-live as B health admits
   a never-probed or stale B. Prediction: silent A loss can enter attach without
   a matching recent B probe ACK.

Each focused RED/GREEN case below maps to one of these predictions.

## Required deep ownership model

### Exact probe key

Every current or standby probe owns one private key containing:

- the exact process-local physical actor/leg seal;
- `SessionId`;
- `LegGeneration`;
- `LegEpochNonce` (`AttachNonce` for current A, `StandbyNonce` for registered
  B);
- `ProbeSequence`;
- the controller's current switch epoch.

No numeric `LegId`, fault interval, dropped-packet count, scheduler clock, or
bare protocol record is sufficient authority.

### Current-A typestate

At most one A probe exists:

```text
Idle
  -> Queued { exact key, non-clone LegControlEnqueued }
  -> Armed { exact key, deadline, opaque non-clone timer token }
  -> Acked/Idle
  OR ExpiryCommitted { exact key, switch epoch }
  OR TransportTerminalWon { switch epoch }
```

`Queued` becomes `Armed` only after the receipt proves the whole ordered record
was accepted by the real Quinn writer. Queue full, ordered-delivery pending,
would-block, or retryable send pressure returns/retains exact ownership and
never marks the leg dead.

Only a `BoundInbound::LegControl` from the same seal whose complete tuple is a
matching `LEG_PROBE_ACK` may consume `Armed`. Wrong and late ACKs are classified
and discarded without entering `SessionModel` and without changing liveness,
application ACK, replay, or Target state.

The timer scheduler receives only the opaque token. Expiry additionally checks
`now >= deadline` and that the same token is still current. A token is neither
`Clone` nor reconstructable from public fields. Replaced, ACKed, retired,
terminal, prior-generation, and prior-switch-epoch tokens are inert.

### Registered-B typestate

The composition owns at most one exact B actor from before registration's
first byte, plus every unregistered/registered typestate derived from it.
Registration acceptance creates only `Registered/Unprobed`:

```text
Registered/Unprobed
  -> ProbeQueued
  -> ProbeArmed
  -> Fresh { exact ACK witness, fresh_until }
  -> Stale/Unprobed
```

Only a matching exact B probe ACK mints `Fresh`. Freshness is a private,
non-clone capability tied to the registered standby and its physical actor,
not a boolean supplied beside it. Starting the silent post-loss attach consumes
that capability and checks `now <= fresh_until`. The ordinary R6a actual-close
compatibility path remains available separately and must not be presented as
silent-failure evidence.

### One failure winner

Probe expiry never constructs a terminal or calls `SessionEvent::LegLost`.
The composition submits the exact expiry authority to the current actor. Under
the actor's existing `ActorFirstFailure` mutex, exactly one of these wins:

- spontaneous peer close/reset/fatal I/O;
- matching outstanding probe deadline expiry;
- explicit session shutdown.

If expiry wins, the actor records a dedicated semantic
`ProbeDeadlineExpired`, then requests cooperative cancellation. The existing
root reporter publishes the sole exact terminal after fixed-child and byte
ownership reach zero; the existing composition/controller path consumes the
sole `PendingClientLegLoss`. If a real close already won, expiry is suppressed
and returned/inert. If expiry already won, later transport symptoms and the
late ACK cannot mint another cause.

The probe winner is a *logical* hard failure while physical A may still be
open. The actor publishes its exact semantic terminal and the controller
applies the sole `LegLost` before B may derive ATTACH. A therefore enters B
installation as the exact terminal-bound post-loss retirement source, even if
its physical close tail is deliberately blocked. Physical A join remains a
separate split-phase cleanup and must not sit on the B attach/first-positive-
application-ACK continuity clock.

An exact authenticated `ATTACH_ACCEPTED` is also causal evidence that the peer
received the corresponding request. Installation may therefore reconcile the
pointer-identical retained request receipt before the redundant local
`DELIVERED` publication arrives. Wrong nonce/leg/generation responses cannot
mint that receipt; a late matching `DELIVERED` remains idempotent after commit.

## Scheduling contract

The deterministic scheduler already orders `TransportReceive` before
`TimerExpiry` at one `SimTime`. Freeze that rule here: a matching ACK delivered
at the exact deadline cancels the token before expiry is considered. An ACK
processed after expiry committed is late and inert.

No background timer task owns the controller, actor, or registered B. The
session root will later schedule and return opaque tokens; local tests drive the
same synchronous interface using `SimTime`.

## Capacity and performance gate

The retained 100-Mbit/s baseline is:

| Quantity | Exact capacity |
|---|---:|
| 100 Mbit/s | 12,500,000 B/s |
| 500ms per direction | 6,250,000 B |
| 5ms quantum | 62,500 B |
| quanta per 500ms | 100 |
| max-DATA encoded replay per direction | 6,253,552 B / about 96 frames |
| original + replay per direction | 12,507,104 B |
| one physical copy allowance per direction | 25,014,208 B |

R6b adds at most one current probe, one standby probe, one timer token per leg,
one registered-B slot, O(1) counters, and at most one composition-owned
structured join task for a failed/retired leg. It adds no payload queue or
payload copy and does not change the figures above.

For R6d, the newer and stricter contract is authoritative: normal ACK age,
hard detection, B attach/install, replay, Target acceptance, and the first
positive B application ACK must fit inside one checked total `<=500ms` budget.
The older expression `blackout_budget + max_normal_application_ACK_age` is not
permission to add an unbudgeted ACK-age tail. If product requirements later
select that larger horizon, capacity must increase by
`rate * ACK_age / 8`; the current 6.25-MB/100-quantum value cannot be silently
reused.

The end-to-end code path intended by this stage is:

```text
real Quinn A writer -> exact LEG_PROBE bytes -> silent peer
  -> opaque delivered receipt -> controller timer token -> actor first-failure
  -> root exact terminal -> controller LegLost -> existing R6a recovery

real Quinn B writer -> exact LEG_PROBE bytes -> peer exact LEG_PROBE_ACK
  -> exact-seal inbound -> fresh registered-B capability
  -> existing signed ATTACH / recovery / first Target-backed application ACK
```

The real TUN entry still selects `LegacyOwnedUpstream`; no production session
root continuously drives this path. This stage is therefore necessary but not
sufficient for `<1s`, 100/170 Mbit/s, Task 4, or M2.

## TDD sequence

1. Controller/standby RED: a real encoded matching A probe ACK is currently
   rejected and registered B can attach without freshness. GREEN introduces
   exact non-clone queued/armed/ACKed/stale typestate; an undelivered receipt
   cannot arm.
2. Actor arbitration RED: semantic expiry is unavailable and bare cancellation
   reports `LocalCancelled`. GREEN makes expiry and actual close share
   `ActorFirstFailure`, publishes one exact fatal terminal, and keeps duplicate
   expiry/late ACK inert.
3. Real Quinn tracer: obtain a fresh B ACK over B; submit A's real probe; make
   server A consume but intentionally not acknowledge that probe while keeping
   the connection open. At `deadline - 1ns` the snapshot is unchanged. At the
   deadline, expiry wins, produces exactly one `ResumeGraceStarted`, then the
   exact composition-owned fresh B enters ATTACH. With B's local request
   `DELIVERED` deliberately withheld, the exact authenticated response commits;
   9 replay bytes reach the existing Target, `ReplacementRecovered 0->9`
   releases exact replay ownership, and only then may retired A clear at zero
   physical ownership.
4. Matching-ACK-first case: A ACK cancels the token; deadline and duplicate ACK
   are inert and A remains current.
5. Exact rejection matrix: wrong seal/session/generation/epoch/sequence/switch
   epoch, stale B freshness, registration-only B, undelivered probes, queue
   pressure, duplicate timer, late ACK, and actor-close-first all preserve
   snapshot and exact owners as specified.
6. Race matrix: close-first and expiry-first each create exactly one actor
   cause, exact terminal, `LegLost`, switch epoch, attach nonce, and B install.
   Shutdown-first remains cleanup-only.
7. Run focused actor/controller/standby/protocol/session tests, 100-repeat
   deterministic races, all-target default, provenance, concurrency,
   real-smoltcp, release, strict Clippy, rustdoc, vendored Quinn/proto,
   formatting, diff, and secret gates. Then perform a concentrated code review.

R6c late-record closure remains mandatory after R6b and before R6d. It must not
be omitted merely because older HANDOFF/TODO wording jumped directly to R6d.

## Acceptance invariants

1. One leg owns at most one outstanding probe and one live deadline token.
2. Timer time starts only after positive ordered writer completion.
3. Only the exact matching ACK refreshes liveness; every correlation mismatch
   is snapshot-inert and never enters the reducer.
4. Registration, endpoint-open, queue-live, inbound data, QUIC delivery, and
   application ACK are not substitutes for a fresh B probe ACK.
5. Pressure cannot become a hard failure.
6. Actual close, probe expiry, and shutdown share one actor first-failure
   linearization point.
7. Exactly one existing terminal and `PendingClientLegLoss` path produces at
   most one `ResumeGraceStarted` for the generation.
8. A live-but-logically-lost actor remains structurally owned until exact
   cooperative join; its cleanup cannot disturb current B.
9. A silent-loss attach is unreachable without consuming the exact fresh B
   capability. Stale health returns all B owners.
10. The first recovery success remains a positive Target-backed application
    ACK intersecting installed replay; probe ACK never releases replay.
11. Every retry, rejection, close, cancellation, and shutdown path leaves one
    owner for each actor, queue, receipt, probe, token, terminal, retired slot,
    replay extent, and Target flow.
12. All probe/timer/standby state is fixed-capacity and contains no secret in
    `Debug` output.
13. Install rejection exposes exactly one root action: retry bounded pressure,
    fail the session, clean a terminal B, or stop on an invariant. Deterministic
    recovery-plan/integrity failures cannot be retried as pressure.
14. A permanent B cleanup error cannot prevent current/retired actors from
    receiving their own cancellation and structural join attempts; the first B
    error remains sticky and is returned only after independent cleanup drives.

## Stop rules

- Stop if test code reads the blackout oracle and calls a raw switch/attach or
  constructs a loss event.
- Stop if a timer can be reconstructed from time/correlation fields or held by
  a detached task that also owns session state.
- Stop if registration acceptance or transport liveness is treated as B
  freshness.
- Stop if queue pressure or an undelivered probe can mark a leg dead.
- Stop if timeout and real close can produce two losses, terminals, epochs,
  nonces, or attaches.
- Stop if timeout bypasses the actor reporter/controller reducer, or if simple
  cancellation is mislabeled as recoverable failure.
- Stop if probe ACK is treated as application ACK or releases replay/Target
  ownership.
- Stop if any queue, timer, task, standby, or retired owner is unbounded.
- Stop and report necessary-only status if the complete R6d budget cannot fit
  in `<=500ms` or the real TUN path remains legacy; do not substitute a WAN or
  M2 run for the missing code-level reachability gate.
