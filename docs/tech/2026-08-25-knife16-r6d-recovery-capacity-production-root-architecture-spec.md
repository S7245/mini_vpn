# Knife16 R6d checked recovery capacity and client-root architecture

Date: 2026-08-25

Status: locally implemented and accepted as a necessary-only R6d stage; see
`docs/tech/2026-08-25-knife16-r6d-recovery-capacity-production-root-local-results.md`.
No production TUN, WAN, throughput, Task-4, or M2 acceptance claim.

## Stage goal

R6d closes two local prerequisites that remain after R6a-c:

1. one checked recovery policy must prove that every complete recovery path
   fits inside the single retained-coverage horizon; and
2. one production-shared client session root must own the composition and its
   structured cleanup instead of requiring a test to call composition methods
   manually.

The first item is a pure capacity contract. The second is a lifecycle and
reachability contract. Both are necessary before R7-R9, but neither is a claim
that the public TUN path, a real two-ingress owner, UDP, WAN throughput, or M2
is ready.

## Non-goals and frozen behavior

- Do not select the resumable path from `client_tun::run_event_loop`; the
  public production entry remains `LegacyOwnedUpstream`.
- Do not implement `mini_vpn-upstreamd`, a real Target owner, or two ingress
  paths. Those remain Task 6 work.
- Do not implement the R7 reverse-only authenticated hint state machine. R6d
  only reserves and validates its complete time budget.
- Do not change D16, local egress, Endpoint pacing, MTU/PLPMTUD, QUIC windows,
  Cubic, GSO, pool policy, UDP behavior, or Knife15/M2 thresholds.
- Do not add an unbounded task, queue, retry, timer, replay range, or flow map.

## Failure tree

### Capacity and time

- A probe, attach, and replay timeout can each be locally valid while their
  sum exceeds the retained horizon.
- Adding normal application-ACK age twice can allocate too much memory; omitting
  it can release retained bytes too early.
- `Duration`, rate-by-time, full-duplex, storage, channel, task, or platform
  narrowing arithmetic can overflow after a task has already started.
- Recomputing a relative timeout at each stage can move the absolute recovery
  deadline forward.
- A transport record and its timer can become ready at the exact same instant;
  timer-first arbitration would reject an ACK that R6b defines as valid.

### Startup and runtime ownership

- A can start while later B, budget, factory, mailbox, or root publication
  fails, leaving an actor or Endpoint without a structural join owner.
- Readiness can be published before the root owns every capability required
  for bounded service and cleanup.
- A caller can cancel a startup or shutdown observation future after ownership
  moved but before a clean witness is returned.
- Data/replay pressure can starve liveness, terminal, or shutdown work.
- A stale timer can mutate a newer generation, or one session failure can
  cancel a shared listener or another session.

### Shutdown

- The first cleanup error can short-circuit the remaining joins.
- Dropping or aborting the root can be mistaken for a clean join.
- A cancelled join waiter can start a second cleanup owner or reopen admission.
- A root can report joined while actor messages, bytes, ACK receipts, replay,
  flow drivers, timers, mailbox entries, or capacity reservations remain.

## Checked recovery budget

`ReplayHorizon` remains the one representation of retained coverage:

```text
retained_horizon = blackout_budget + max_normal_application_ACK_age
```

`RecoveryBudgetSpec` receives that checked horizon plus durations measured
after the normal ACK-age allowance. It derives both complete paths:

```text
hard_last_ack_to_signal = normal_ack_age + hard_liveness_after_normal_ack
hard_total = hard_last_ack_to_signal
           + attach_install
           + replay_to_first_application_ack

reverse_last_ack_to_stall = normal_ack_age + reverse_stall_after_normal_ack
reverse_total = reverse_last_ack_to_stall
              + authenticated_hint
              + attach_install
              + replay_to_first_application_ack

hard_total <= retained_horizon
reverse_total <= retained_horizon
```

Every addition is checked. Overflow and either inequality violation reject the
configuration before any actor, queue, or task is created. Zero-valued
components are permitted for deterministic immediate transitions; the retained
horizon itself remains non-zero through `ReplayHorizon`.

`ReplayCapacitySpec::from_recovery_budget` consumes the derived budget's exact
`ReplayHorizon`. It does not reconstruct `500ms + ACK age`, so the ACK allowance
cannot be counted twice. The legacy constructor remains for existing callers
and tests. `ReplayStoragePlan` retains that same checked `ReplayHorizon` as a
read-only witness. The production root must compare it with the derived
`RecoveryBudget` before the one-shot TCP factory is minted; matching byte
counts without a matching horizon are not sufficient admission evidence.

### Capacity math

For `100,000,000 bit/s` and a total `500ms` horizon:

```text
ceil(100,000,000 * 500,000,000 / 8,000,000,000)
  = 6,250,000 bytes per direction

100,000,000 * 5ms / 8 = 62,500 bytes per quantum
6,250,000 / 62,500 = exactly 100 quanta per direction
```

Using a one-flow `62,500B` coalescing geometry therefore requires exactly 100
logical retained records per direction. A generic `65,536B` geometry yields a
different record count and must not be used to claim the 5ms-quantum result.
Aggregate bytes continue to derive from aggregate directional rate, never
`per_flow * flow_count`.

## One absolute deadline and tie-break

The future client root derives one absolute deadline from the recovery start
and the accepted retained horizon. Every stage receives the remaining budget;
no stage may call `now + component_timeout` to extend the original deadline.
Generation and switch-epoch capabilities bind every timer.

At one timestamp the deterministic order is:

1. transport receive and exact application-ACK classification;
2. control/terminal publication;
3. timer expiry.

This preserves the R6b rule: an exact-deadline ACK is accepted, while the same
ACK at deadline plus one nanosecond is stale. A raw `tokio::select!` race is not
an acceptable implementation of this order.

The actor queue and deadline commit must also close their internal Empty/send
window. A complete reader reserves an actor-private inbound-publication slot
before it can race the first-failure transaction; every success or suppression
publishes that reservation. A due-deadline transaction first consumes a ready
current record and defers while any such publication is in flight. Only an
exact Empty plus zero in-flight publications may commit expiry under the same
first-failure gate. A readiness epoch alone is not sufficient because producer
publications may complete out of order.

Production receive time must come from the same monotonic time domain as the
opaque deadline. If a record carries no arrival timestamp, delayed root
service cannot distinguish an ACK queued before the deadline from one queued
after it. In that state the atomic queue seam is a necessary race repair, but
full absolute-deadline correctness remains open and the root must not claim the
deadline gate accepted.

## Production-shared client root and nursery

The ownership tree is:

```text
Started client root (non-Clone join owner)
├── bounded, cloneable command handle
├── sole QuinnClientSessionComposition
│   ├── current Quinn leg actor root + four fixed children
│   ├── optional standby Quinn leg actor root + four fixed children
│   └── optional retired/standby structural join tasks
├── sole ResumableTcpPortFactory and bounded flow-driver registry
├── checked recovery budget and epoch-bound deadlines
└── one sticky failure plus one structural shutdown/join witness
```

The root, not an `Arc<Mutex<QuinnClientSessionComposition>>`, owns the mutable
composition. Cloneable adapters carry only bounded command senders and
read-only snapshots. State is monotonic:

```text
Starting -> Running -> ShuttingDown -> Joined
                             \------> FailedJoined
```

`Ready` may be minted only after A composition, checked budget, bounded
mailbox, sole TCP factory, timer authority, and every cleanup owner are
installed. B registration is required only by the later two-leg-ready policy;
R6d must not fabricate a real server-side B when Task 6 does not exist.

Startup is a synchronous prepare-and-spawn transaction. It preallocates every
bounded registry/mailbox/timer slot and obtains the runtime before the factory
mint. Factory mint is the final fallible ownership mutation; after it succeeds,
the whole prepared aggregate moves directly into the spawned future with no
intervening `await` or fallible operation. The non-Clone `Started` value keeps
the only `JoinHandle`; readiness and join waiters borrow that owner, so caller
cancellation cannot detach the root.

All drivers minted by the sole TCP factory share one opaque, identity-bound
progress source. Successful source/control/reset enqueue, completion
publication or release, downlink/terminal dequeue, byte/segment-permit release,
partial-reservation rollback, and local endpoint drop publish an edge. The
root captures this cursor before its final bounded driver scan and parks on it
without busy polling. Cursor wait cancellation consumes no driver input and a
cursor from another factory is rejected.

Shutdown first seals admission and starts or adopts every cleanup owner before
its first await. It records the first sticky failure but still joins current,
standby, and retired actors. Cancelling an observer loses only observation;
the root task retains cleanup. `Drop`/abort is fallback cleanup and can never
mint `Joined`.

Because there is no real owner daemon or production initial-ATTACH responder,
R6d's root acceptance is deliberately local: the production-shared root API
must own real loopback Quinn actors and clean them without tests manually
driving the composition. Public TUN selection remains closed until Task 6 and
R7-R9 supply the missing peer and acceptance path.

### Flow-reachability implementation gate

The empty-flow root lifecycle tracer is only the first ownership slice. It
must not be widened into an `Open` command until the following production
seams are present and individually locked by tests:

1. every current-leg enqueue rejection returns the exact reducer-produced
   `Frame`, including actor loss, missing current, shutdown, and retryable
   queue pressure;
2. every TCP sink or half-close delivery rejection returns the exact offer,
   segment backing, or completion capability for bounded retry;
3. root-start quiescence includes the controller-owned current FIFO: active
   phase, zero queued frames/bytes, and no ordered-delivery waiter;
4. root-start quiescence also requires reducer flow, replay, receive, sink-
   offer, and terminal-tombstone ownership to be zero. A snapshot containing
   a tombstone cannot be adopted because it does not expose the private
   `TerminalGrace` capability needed to build the root's timer;
5. the root preallocates two recovery/liveness timer slots plus one typed
   terminal-grace slot per configured terminal tombstone, using checked
   arithmetic;
6. one monotonic clock stamps a terminal token when it is first emitted, and
   retrying queue/effect work cannot move that absolute deadline forward;
7. shutdown seals command and factory admission, adopts or creates one typed
   local-abandon input per live flow, drives terminal effects and timers, and
   only then removes drivers and joins actors; and
8. one bounded scheduler round services actor receive/terminal, command,
   round-robin driver input, pending effects, outbound flush, and due timers.
   Continuous readiness in one category may not skip the remaining categories.

The timer capacity is therefore:

```text
root_timer_capacity = 2 recovery/liveness slots
                    + max_terminal_tombstones
```

The production root plan must also carry an explicit, non-zero checked
terminal-grace duration. The deterministic harness's `32ns` constant and the
recovery retained horizon are different policies; neither may be silently
reused as the production value. Until that configuration contract exists,
typed timer storage can be extracted and tested, but production terminal
expiry cannot be called complete.

`Vec::capacity()` or `VecDeque::capacity()` is never a logical admission
contract: allocators may reserve more than requested. Every admission compares
against the checked plan value. A capacity miss retains the rejected
ownership in an explicit aborted-work state and makes clean join impossible;
it must not drop the value and later report zero.

The open transaction itself is ordered:

```text
reserve registry/reply/effect capacity
  -> reducer LocalOpen commit
  -> factory pair install under the same bounded root
  -> ordered OPEN queue admission
  -> endpoint reply publication
```

If the reply receiver is cancelled after install, the root consumes the
returned endpoint with `abandon_before_install` and must drive exactly one
`OPEN -> RESET` terminal path. An endpoint may never escape before the OPEN
has positive queue admission, because its immediately ready DATA would
otherwise be able to overtake OPEN.

The existing `open_local -> factory.open_flow -> enqueue_current_frame` calls
are not that transaction: `SessionModel` has already inserted the flow and
advanced its IDs before either post-reducer operation can fail. R6d therefore
requires a `PreparedLocalOpen` branch-by-abstraction seam, analogous to the
existing prepared client-replacement transaction. It must validate and
materialize the target, next flow/authority IDs, capacity, and exact OPEN frame
without reducer mutation; a root-owned callback then creates the TCP pair. Only
after that callback succeeds may the reducer commit and move the pair, OPEN
frame, reply authority, and reserved registry slot into one `PendingOpen`.

Ordinary FIFO pressure returns the exact frame and retains the whole
`PendingOpen` for fair retry. Actor loss, shutdown, invalid-frame rejection, or
another permanent postcommit enqueue failure cannot be described as a clean
Open error unless the actor supplies a typed one-frame queue reservation that
makes enqueue infallible after reducer commit. Without that reservation, the
root must keep the aborted endpoint/driver/frame/reply ownership, make the
failure sticky, drive typed local abandonment where still possible, and finish
only as `FailedJoined`. Clearing the registry is not compensation.

## End-to-end hot-path inventory

The eventual client path that the root must continuously progress is:

1. Quinn `data_reader_task` / `delivery_reader_task` place bounded actor facts;
2. `QuinnClientSessionComposition::try_receive_current` or the standby/retired
   equivalent classifies exact physical provenance;
3. `ClientSessionController::receive_current_from_quinn` and
   `SessionSupervisor` reduce one event;
4. sink effects reach `ResumableTcpDownlink` and `spawn_resumable_action_bridge`;
5. `client_tun::handle_owned_tun_action` retains the exact `SinkDelivery`;
6. `flush_resumable_sink` calls smoltcp `send_slice` and only positive
   acceptance completes the typed receipt;
7. the driver returns that application fact through
   `QuinnClientSessionComposition::apply_driver_input`;
8. `drive_recovery_one` and `try_flush_current` enqueue replay/control through
   the current Quinn actor;
9. `iface.poll`, `flush_tx_and_release_downlink_permits`, and `TunIo::flush_tx`
   continue local egress.

R6d capacity validation is necessary for this path but does not itself make
the path continuously reachable. The client root/nursery is the reachability
step; R7-R9 and Task 6 remain required for a sufficient end-to-end product
path.

## Old-path audit

- `client_tun::run_event_loop` still wraps TUIC/Reality/Failover in
  `LegacyOwnedUpstream`.
- Legacy relay readers, D16, smoltcp/TUN egress, Endpoint pacing, and all UDP
  paths remain active and unchanged.
- `FakeResumableUpstream` remains a harness adapter and is not production
  reachability evidence.
- `QuinnClientSessionComposition::Drop` remains fallback cleanup, not a clean
  join witness.
- The existing per-call current probe timeout and per-standby liveness limits
  must not be treated as an aggregate recovery proof.

Therefore R6d is a necessary-only local architecture stage. It cannot be
described as sufficient for `30 Mbit/s`, `100+ Mbit/s`, WAN continuity, or M2.

## Failure discriminators

- `RecoveryBudgetError::PathDurationOverflow` identifies arithmetic/config
  failure before spawn.
- `RecoveryBudgetError::PathExceedsHorizon` names hard versus reverse and
  carries exact total/horizon values.
- Replay-plan horizon equality distinguishes double-counted/missing ACK age.
- Per-stage elapsed and one absolute deadline distinguish detection, attach,
  replay, Target/local acceptance, and first application-ACK delay.
- Actor snapshots distinguish remote-read service, actor queue pressure,
  controller/replay pressure, local sink acceptance, and physical close tail.
- Root phase, first sticky error, task/join counts, and final owned message/
  byte/ACK/replay/flow/mailbox/reservation totals distinguish clean join from
  fallback abort.

## TDD order

1. Exact `500ms` hard and reverse sums pass; `+1ns` on either path fails with
   the exact path and values.
2. Checked-add overflow fails before construction.
3. The budget-to-replay bridge preserves exactly one horizon; 100 Mbit/s and
   5ms geometry yields `6,250,000B` and 100 records per direction.
4. A root ready/shutdown tracer owns a real loopback Quinn actor and returns
   one clean zero-ownership join witness without direct composition calls.
5. Startup failure/caller cancellation at each ownership handoff remains
   unready and still joins all adopted actors.
6. Exact-deadline transport receive wins over expiry; `+1ns` expires.
7. Filled data/replay work cannot starve terminal/deadline/shutdown service.
8. Cancelling a shutdown waiter and retrying observes the same sealed root and
   the same structural join; no second owner is minted.
9. Child fatal/panic is sticky, all other cleanup still runs, and no failed
   tree can mint a clean witness.

Each tracer is added red/green one at a time. Critical lifecycle and deadline
tracers repeat 100 times before the broad suite.

## Stop rules

- Stop before root implementation if no non-polling/no-busy-loop wake Seam can
  drive actor inbound, terminal, timer, and command work with the required
  receive-before-expiry order. Extract that Seam first.
- Stop before publishing readiness if any required capacity or lifecycle
  reservation is still created after spawn.
- Stop before calling R6d locally closed if tests still instantiate or drive
  `QuinnClientSessionComposition` directly instead of going through the root.
- Stop before any TUN/VPS/WAN/M2 run while the public path remains legacy or
  the real owner/server is absent.

## Architecture assessment

Pre-R6d score: **8/10**. The existing Module had deep protocol/ownership
Implementation and useful actor/composition Interface, but the session-level
Depth ends before aggregate time and lifecycle ownership.

R6d local result: **8.5/10** for the necessary-only seam. One
production-shared Module now owns the `RecoveryBudget`, composition, factory,
bounded scheduler, exact Open transaction, semantic/transport cleanup, and
zero-ownership witness behind a narrow command handle. The lifecycle cutoff
is linearized with each bounded synchronous work turn, and progress cursors
park without polling. This has high Leverage because timing, replay capacity,
TUN flow creation, recovery, and shutdown all depend on it, and high Locality
because transport details remain inside the root rather than spreading into
`client_tun`.

Task 4 can reach **9/10** only after R7-R9 pass through the same root. The route
to **10/10** still requires UDP dual-leg delivery, the real single owner and
two ingresses, real-socket capacity/security qualification, independent-path
WAN tests, and long macOS/M2 acceptance.
