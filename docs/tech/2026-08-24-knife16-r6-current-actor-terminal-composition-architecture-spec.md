# Knife16 R6 Current-Actor Terminal Composition Architecture

Date: 2026-08-24

Status: **NECESSARY-ONLY LOCAL PASS; TASK 4 AND M2 NOT ACCEPTED**

Accepted result:
`docs/tech/2026-08-24-knife16-r6-current-actor-terminal-composition-local-results.md`.

## Stage goal

Close one necessary R6a slice: a spontaneous fatal/peer-close fact from the
exact production-shared Quinn current actor must cross the composition boundary
into the controller's matching current slot and cause exactly one existing
`LegLost` reducer transition.

This stage separates logical failure from physical cleanup. The exact terminal
fact may become available only after the actor's fixed children have stopped
and all actor message/byte/DELIVERED ownership is zero, but it must not wait for the
connection, Endpoint driver, or UDP socket close tail. The actor remains owned
until its existing structural join later proves zero resource ownership.

## Non-goals

- silent-blackout detection and probe/PTO expiry;
- long-lived ownership or freshness tracking for standby B;
- post-loss B attach, replay, or first positive B application ACK;
- the complete checked `500ms` recovery-budget sum or a `<1s` continuity
  claim;
- production session-nursery/root reachability;
- TUN, VPS, WAN, throughput, M2, R7-R9, or UDP acceptance.

This result is necessary-only. A passing actual-close tracer cannot by itself
solve the one-second M2 failure.

## Pre-implementation ranked failure hypotheses

1. The exact terminal is stranded in `QuinnLegActorJoined`, so a real peer
   close leaves the controller Active until full physical cleanup or shutdown.
2. Reusing `join_current_for_shutdown` would irreversibly enter
   `ShuttingDown`, close the recovery admission surface, and put Quinn's
   physical close tail on the logical-loss critical path.
3. Once `bind_current_terminal` moves controller current into
   `TerminatingCurrent`, the normal current-layout predicate necessarily
   fails; a phase-specific physical-current predicate is required.
4. `PendingLegLoss::into_event` destroyed the only terminal fact before this
   slice. A later post-loss replacement would then have put A in the retired
   slot while retired physical clear asked for an impossible second fact.
5. A generic close callback could misclassify explicit local session shutdown
   as a recoverable hard failure and synthesize a false `LegLost`.
6. A fixed child may publish the actor's first fatal before the terminal
   handoff becomes ready while controller-only `open_local`, recovery,
   standby, or replacement paths still commit under the stale `Unbound`
   composition state.
7. Once a terminal has consumed its one logical-loss capability, treating a
   supervisor/effect/invariant failure as healthy `Applied` can turn the next
   drive into a false success.

## Required ownership design

The fixed actor root owns a single non-cloneable terminal handoff channel.
After fixed-child join, inbound drain, pending-delivery release, and the
existing zero-counter barrier, it reports and publishes the exact terminal
into that channel before physical
connection/Endpoint cleanup. The started actor remains the sole receiver and
the sole structural-join owner.

The started actor exposes one composition-gated terminal transaction and one
payload-free synchronous policy transaction. The latter holds the exact
first-failure mutex across controller-only policy commits and the Unbound
shutdown publication; it does not expose or move actor I/O, FIFO, payload, or
task ownership.

```text
root reporter (after actor counters are zero)
  -> exact terminal handoff (one slot, non-Clone)
  -> composition validates exact runtime/current-controller identity
  -> controller bind_current_terminal
  -> controller discard_terminating_current_recovery
  -> exactly one ResumeGraceStarted / LegLost

in parallel after the handoff:
  actor root -> connection drain -> Endpoint/driver/socket drain -> joined
```

If the handoff has not arrived, the drive first asks a fatal current actor to
enter cooperative cancellation so its private inbound receiver can return to
the root, then remains pending and mutates no controller state. If the
controller bind rejects, the same terminal is restored to the actor. Once bind
succeeds, ownership is durably in the controller. A retryable recovery-cleanup
rejection remains `CurrentTerminalBound` and may retry only discard. A
rejection after the one loss capability was consumed becomes a sticky
`ConsumedFailed` result; it must neither retry the loss nor appear as healthy
`Applied`. Neither class may roll the terminal back to the actor. Bind success
marks actor-side terminal ownership consumed; structural join may then produce
a joined witness with no terminal payload but with exact runtime identity and
zero-resource proof.

The controller must retain the consumed terminal as terminal-bound retired
cleanup authority. Applying the one logical `LegLost` consumes only the exact
committed-leg event authority, not this terminal. A future post-loss B install
may move that authority into the retired slot. Its final clear will pair:

- the controller-retained terminal-bound retired authority; and
- the actor's exact joined runtime/zero-resource witness.

The existing normal A-to-B replacement remains distinct: its retired A has not
already bound a terminal, so its joined witness still supplies the terminal at
clear time. No terminal may be cloned, re-minted, or synthesized.

## Composition state

The current lifecycle is monotonic:

```text
Running
  -> CurrentTerminalBound
  -> Applied | ConsumedFailed
  -> (future post-loss replacement; outside this stage)

Running -> ShuttingDown -> Joined
```

`CurrentTerminalBound` exists only to make a failed/retryable controller
discard explicit. `Applied` means the current is logically lost; it rejects
replay of the actor terminal and a
second `LegLost`. `ConsumedFailed` closes admission, repeats the same error,
and permits structural cleanup without ever reporting healthy completion.
`LocalCancelled` is valid only on the separate shutdown path; observing it on
the Running recovery path fails closed without reducer mutation.

Shutdown and fatal are linearized by the actor's exact first-failure mutex. If
Unbound shutdown publishes `ShuttingDown` under that lock first, any later
terminal is cleanup-only and cannot emit `LegLost`. If the actor's non-local
first fatal publishes first, shutdown cannot reclassify it; it leaves the
composition Running, initiates cooperative cancellation, and returns the
exact actor failure so terminal drive can complete the one loss transaction.
After `Applied` or `ConsumedFailed`, final physical cleanup may publish
`ShuttingDown` directly; `CurrentTerminalBound` must finish its discard first.

## Deadline classification and capacity

Actual close is event driven and adds no semantic timeout. The bounded
`fatal observed -> logical LegLost applied` ordering is one diagnostic
component only. The later complete hard-path design must still prove its
separately budgeted detection, attach, and first-application-ACK component sum
is at most `500ms`; this slice neither allocates that sum nor claims `<1s`.

Each actor adds exactly one terminal handoff slot. It adds no payload copy,
payload/message permit, actor child, background task, or hot-path queue. The
controller retains at most one O(1) terminal-bound cleanup authority for its
terminating current. Existing current and retired physical cleanup owners do
not multiply beyond their already bounded one-slot lifecycles.

## Invariants

1. One authenticated endpoint mints at most one `ExactLegTerminal`.
2. After actor first-failure publication wins and until terminal handoff,
   controller snapshot, recovery ownership, and queues are unchanged.
3. A terminal from an unrelated physical actor is returned untouched and
   neither actor is cancelled by the composition mismatch check.
4. When an exact non-local current first failure wins the shared mutex while
   composition is Running, its terminal produces exactly one
   `ResumeGraceStarted` effect and exactly one observed leg loss.
5. Re-driving after `Applied` is snapshot-inert.
6. Existing retired/standby/unrelated actor ownership is not consumed or
   mutated by current loss.
7. Logical loss never claims physical join, zero bytes, Endpoint release, or
   retired capacity release.
8. Physical DELIVERED and resumable application ACK semantics remain
   unchanged.
9. Every error path retains exactly one owner for every still-existing actor,
   controller transport, queue, recovery tail/reservation, terminal, and
   physical-cleanup value. If reporter mint fails, no terminal is synthesized;
   actor and cleanup ownership remain exact while the handoff stays closed.
10. `actor counters zero -> report -> terminal publication -> physical close
    tail` is ordered; publication and the close tail are independently
    observable in deterministic tests.
11. Once actor first-failure publication wins, controller-only current policy
    operations and Unbound shutdown execute no closure and mutate no session
    state. If one of those synchronous commits wins, fatal cannot publish in
    its middle.
12. `Applied` is published only after exact generation/reason/effect
    validation. Consumed post-commit failures are sticky across re-drive and
    physical join.

## TDD sequence and acceptance

1. Real localhost Quinn peer close: before the new drive the controller is
   unchanged; the early exact handoff becomes available before an intentionally
   blocked physical join tail; one drive applies one `LegLost`; a second drive
   is inert.
2. Exact failure classification: the effect is
   `ResumeGraceStarted { generation }`, the terminal reason equals the exact
   mapping of the observed first-failure `Arc`, and explicit local shutdown
   does not enter the loss path.
3. Wrong-incarnation at the composition boundary returns the exact terminal
   without controller mutation. A real poisoned reporter transition closes
   the terminal handoff before the composition callback; repeated drive keeps
   controller snapshot, routes, and `LegIo` counters unchanged.
4. Recovering/current-with-retired variants: current loss does not clear the
   existing retired slot or mutate an unrelated live actor.
5. Shutdown-first and fatal-first races obey the shared-mutex winner; a
   deterministic gate inside a live policy closure proves that the
   first-failure mutex spans the whole commit. Bind rejection restores the
   actor token. Recovery-cleanup rejection persists as controller-bound retry,
   while post-consumption rejection persists as the same sticky failure.
6. Same logical `LegId` with a different physical actor cannot bind; after an
   intentionally blocked physical close tail, final join still proves zero
   ownership.
7. Existing actor, controller, provenance, concurrency, smoltcp, Clippy,
   rustdoc, formatting, diff, and secret gates remain green.

The tracer bounds and witnesses the ordering from actor fatal observation to
successful logical `LegLost` application. This slice records no accepted
latency number; any later timing sample is diagnostic only, not the full
recovery horizon and not a standalone `<1s` acceptance value.

## Stop rules

- Stop if logical loss requires awaiting full actor/Endpoint/socket join.
- Stop if the design needs a cloned, re-minted, or synthetic terminal fact.
- Stop if `LegLost` can be emitted from queue-full, would-block, retryable send
  pressure, caller cancellation, or generic timeout.
- Stop if post-loss retired cleanup would require a second terminal fact.
- Stop if the local seam cannot preserve the exact controller and actor
  ownership on rejection; extract a deeper abstraction instead of adding an
  ad-hoc callback.
