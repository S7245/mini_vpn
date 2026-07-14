# Knife14h10d16 Endpoint Pre-Accounting Pacing Service Architecture Spec

Date: 2026-07-13

Status: **PROPOSED; code-level GO for local TDD only after explicit review and
confirmation. No implementation or VPS run is authorized by this document.**

Source baseline: `a54fb171ad57dc48902a79f9d31bd08d2ac41802` plus the
existing uncommitted Knife14 worktree and the pinned local
`quinn-proto 0.11.16` cap64 patch. Gate A and Gate B remain accepted. Task 12
step 4 remains stopped before P8.

## Decision

Replace the rejected stored-token cap candidate with one optional,
endpoint-owned, pre-accounting UDP datagram service inside Quinn. The service
is shared by every QUIC connection created by one `proto::Endpoint`, reserves
wire bytes before packet construction and Quinn send accounting, and keeps
accepted-but-socket-blocked bytes outstanding until the Quinn driver reports a
real socket outcome.

The first candidate is a byte token bucket plus bounded two-connection DRR
service:

```text
aggregate sustained rate       = 30,720,000 wire bytes/s
aggregate burst                = 61,440 wire bytes
control reserve within burst   = 10,240 wire bytes
bulk connection quantum        = 20,480 wire bytes
```

At the frozen initial MTU `1280`, those values are respectively `24`, `48`,
`8`, and `16` MTU-equivalents per millisecond/service unit. The contract is
byte-based so PLPMTUD or a safe1200 diagnostic does not silently change the
aggregate wire rate.

This is a new deep Module, not another cap value, socket cooldown, GSO switch,
driver poll constant, or product parameter adjustment.

## Evidence And Deterministic RED

The authorized cap64 VPS discriminator proved reachability but rejected
sufficiency. The data connection carried `99.9998%` of QUIC TX bytes and one
snapshot proved `81920B = 64*1280` effective capacity, yet client egress still
reached `267 packets/1ms` and `1337/10ms`, with `29` TUN TX drops and
`50,621,275B` formal QUIC loss.

A focused fake-time acceptance test was then added at the exact Pacer seam:

```text
RTT       = 200us
cwnd      = 40,000B
MTU       = 1280B
stored cap= 64 datagrams
driver    = at most 20 datagrams per poll, one poll every 50us
window    = 1ms
```

The red assertion expected at most one stored bucket and observed exactly
`259` issued datagrams in 1ms. The default test now preserves that result as a
mechanism characterization:

```text
configured_cap_refills_multiple_buckets_inside_one_millisecond
```

This selects the sub-ms refill hypothesis. It rejects policy propagation,
pool aggregation, GSO-only, migration, extreme-cwnd, and socket-sender
reachability as the missing mechanism.

## Goals

1. Bound aggregate connection-generated endpoint UDP service over real time,
   not only stored tokens at one instant.
2. Preserve enough sustained capacity for an exact local `32 MiB` forward
   transfer strictly above `170 Mbit/s` with clean EOF.
3. Share the bound across both frozen TUIC pool connections.
4. Let one busy data connection borrow idle endpoint capacity while bounding
   unfairness once a second connection becomes backlogged.
5. Preserve bounded service for handshake, ACK, loss, close, keepalive, path,
   and MTU-control traffic.
6. Compose the endpoint deadline with Quinn's existing per-path congestion and
   Pacer deadline without busy wake or post-accounting delay.
7. Refund or settle reservations exactly on short build, cancellation,
   migration, connection drain, socket `WouldBlock`, socket success, and fatal
   socket failure.
8. Preserve default Quinn behavior byte-for-byte when the policy is absent.

## Non-Goals And Frozen Values

- Do not change D16 ownership, the 24-packet TUN feedback window, actor
  cadence, EOF ordering, or self-wake.
- Do not change MTU/PLPMTUD, pool size, QUIC stream/connection/send windows,
  the `64 KiB` application chunk, Cubic, GSO default, or driver
  `MAX_TRANSMIT_DATAGRAMS`.
- Do not revive the fixed `48 then 2ms` `AsyncUdpSocket` cooldown sender.
- Do not retry Pacer cap constants or describe `PacerCap64` as a product
  candidate.
- Do not add a Linux-only `SO_MAX_PACING_RATE`/`sch_fq` dependency.
- Do not redesign TUIC framing, TCP relay ownership, UDP relay mode, fake-IP
  DNS, REALITY, smoltcp, or TUN.
- Do not run VPS, macOS TUN, old-path cleanup, or P8 from this architecture
  stage.

## Capacity And Window Math

The measured failed local transfer used `34,479,447B` of QUIC UDP traffic for
`33,554,432B` of application data:

```text
wire/application ratio = 1.027567595
170 Mbit/s application = 21,250,000 application bytes/s
required wire rate     = 21,835,811.399 bytes/s
candidate wire rate    = 30,720,000 bytes/s
candidate app capacity = 29,895,843.487 bytes/s = 239.167 Mbit/s
headroom over 170M     = 40.69%
```

Let `F(t)` be cumulative actual UDP datagram bytes finalized from endpoint
reservations, after short-build refunds, and let `S(t)` be cumulative bytes
accepted by the UDP socket. Live reservations and finalized-but-unsent bytes
remain charged against the same bucket. The service must prove both envelopes
for every `t2 >= t1`:

```text
F(t2) - F(t1) <= 61,440 + 30,720,000 * (t2 - t1)
S(t2) - S(t1) <= 61,440 + 30,720,000 * (t2 - t1)
```

Therefore:

| Window | Maximum finalized/socket-accepted bytes | 1280B equivalents |
| ---: | ---: | ---: |
| instantaneous stored burst | `61,440B` | `48` |
| `1ms` | `92,160B` | `72` |
| `10ms` | `368,640B` | `288` |

The one-millisecond byte bound is `27.0%` of the failed cap64 peak
`341,760B`; the ten-millisecond bound is `21.5%` of the failed
`1,711,360B` peak. The sustained candidate is still above the measured wire
demand for `170 Mbit/s`.

This is a sufficient code-level capacity path for the exact single-flow local
gate. It remains necessary-only for P8 and final product acceptance.

## Module, Interface, Implementations, And Adapters

### Deep Module

`EndpointPacingService` owns:

- the aggregate token bucket and last refill instant;
- outstanding bytes already built/accounted by Quinn but not yet accepted by
  the UDP socket;
- a bounded control-waiter queue;
- a bounded bulk DRR queue, per-connection deficit, and current turn;
- one waiter/waker record per live connection/path generation;
- cumulative service, wait, refund, cancel, fairness, and socket-outcome
  diagnostics.

Its Interface exposes only policy-level operations:

```text
poll_reserve(connection_key, class, path_ready_at, planned_bytes, waker, now)
  -> Granted(DatagramReservation) | Blocked(deadline, reason)

DatagramReservation::settle_built(actual_datagram_bytes)
note_socket_sent(batch_bytes, now)
note_socket_abandoned(batch_bytes, now)
cancel_waiter(connection_key)
detach_connection(connection_key)
```

`connection_key` is endpoint-local `ConnectionHandle + path_generation`.
There is never more than one waiter record for one key.

### Implementations

- `QuinnDefault`: no endpoint service allocation, lock, waker, reservation,
  callback, diagnostic, or changed deadline.
- `EndpointWindowV1`: the fixed policy in this spec.

`PacerCap64` remains a rejected diagnostic Implementation pending cleanup. It
is mutually exclusive with `EndpointWindowV1`, as is the rejected bounded
socket Adapter.

### Adapters

- The mini_vpn Adapter selects the endpoint policy once while building
  `EndpointConfig` and logs one non-secret startup fingerprint.
- A thin pinned `quinn 0.11.11` driver Adapter supplies the current connection
  waker and reports socket success/abandonment. It contains no rate, burst,
  fairness, or token policy.

The policy and invariants remain inside `quinn-proto`; mini_vpn, TUIC, D16,
smoltcp, and TUN do not learn coordinator internals.

The seam has two real consumers immediately: both connections in the frozen
TUIC pool. It is therefore not a speculative one-adapter abstraction.

## Exact Construction Reachability

`EndpointConfig`, not `TransportConfig`, owns the optional static service
configuration. `proto::Endpoint::new` creates fresh runtime service state for
that endpoint. Cloning an `EndpointConfig` must not make two independently
constructed endpoints share one budget.

`Endpoint::add_connection` already owns the endpoint-local
`ConnectionHandle`; it passes that handle and the shared service into
`Connection::new`. Path generation already increments on migration and joins
the service key.

The service reservation point is the “start one more UDP datagram” branch in
`Connection::poll_transmit`, before `PacketBuilder` construction and before
`PacketBuilder::finish_and_track` records `SentPacket.time_sent`, in-flight
bytes, loss timers, or `Pacer::on_transmit`.

One service reservation covers one UDP datagram. GSO reserves each segment
separately before it joins a `Transmit`; a returned GSO `Transmit` carries the
sum as one pending socket-outcome batch inside the connection. Short datagrams
refund unused reservation bytes.

The pinned `quinn` driver calls the outcome callback only after:

- successful `AsyncUdpSocket::try_send`: `note_socket_sent`;
- fatal send error or connection-driver teardown: `note_socket_abandoned`;
- `WouldBlock`: no callback; the outstanding batch remains charged while the
  existing buffered `Transmit` waits.

Holding both live reservations and blocked bytes inside the bucket is required
for the temporal theorem.
Before removing outstanding bytes, the service refills at the socket-outcome
instant while enforcing:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= burst_bytes
```

Thus elapsed time during `WouldBlock` cannot accumulate a second full burst
behind the buffered transmit. A post-block socket success can release at most
the already outstanding burst; later service must be earned after that
instant.

## Traffic Classes And Control Reserve

Every connection-generated UDP datagram enters the aggregate byte envelope.
This includes pure ACK datagrams; their existing zero-sized *per-path Pacer*
accounting remains unchanged, but the endpoint service charges their real UDP
bytes as `Control`.

`Control` includes:

- Initial and Handshake spaces;
- ACK-only and ACK/control-only data packets;
- CONNECTION_CLOSE;
- loss probes and keepalive PING;
- PATH_CHALLENGE/PATH_RESPONSE;
- MTU probes and protocol flow-control-only packets.

Any packet eligible to carry STREAM or application DATAGRAM payload is
`Bulk`; protocol frames piggybacked with application payload do not receive a
second reservation.

The `10,240B` control reserve is part of, never additional to, the `61,440B`
aggregate burst. Bulk cannot consume the final reserve while a control waiter
exists. If no control waiter exists, bulk may borrow it. A later control waiter
has priority for newly refilled bytes and waits at most one maximum-datagram
refill interval from the endpoint service, plus any later per-path Quinn
deadline. At `1280B` that service-only interval is about `41.7us`.

Control traffic cannot escape the aggregate envelope or starve bulk forever:
after it consumes the reserve quantum, additional control service participates
in the normal turn policy.

Endpoint stateless responses have no connection lifecycle or timer. The first
product implementation may drop them when no endpoint token is immediately
available, matching Quinn's existing best-effort response behavior. Formal
client acceptance requires the new stateless-response counter to remain zero;
otherwise the run is not an all-endpoint attribution proof.

## Idle Borrowing And Fairness

Idle connections have no queue entry, deficit, or reserved rate. One saturated
data connection therefore borrows the full endpoint rate and burst while the
auxiliary connection is idle.

When two bulk connections are continuously backlogged, byte DRR rotates after
`20,480B` of granted bulk service. After both are registered, excluding
bounded control service, their cumulative grant difference must remain no
greater than:

```text
connection_quantum + one maximum datagram
```

At `1280B`, one turn consumes `0.6667ms` at the configured rate. Cancellation
or an idle transition removes the waiter and immediately lends its unused turn
to the next eligible connection.

No background task, channel, payload queue, or per-packet heap allocation is
allowed. Waiter metadata is O(live endpoint connections), bounded by Quinn's
existing connection set.

## Deadline And Wake Composition

Quinn's congestion check and per-path Pacer remain authoritative. The next
datagram is built only when all three conditions hold:

```text
congestion permits
per-path Pacer permits
endpoint service grants an exact reservation
```

The endpoint service receives the path's earliest eligible instant. A blocked
connection installs one pacing timer at the later of the path and endpoint
deadlines. It must not self-wake merely because endpoint service denied it.

The driver supplies its current waker so a turn handoff, waiter cancellation,
socket outcome, or connection detach can wake a newly eligible peer earlier
than a conservative timer. Wakers are replaced, deduplicated with
`will_wake`, removed on detach, and invoked outside the service mutex.

No endpoint mutex may be held while constructing/encrypting a packet, calling
the socket, invoking a waker, or awaiting anything.

## Cancellation, Migration, And Close

- A reservation is RAII. Dropping it before a datagram is finalized refunds
  all planned bytes and turn deficit once.
- Settling a short datagram moves only actual bytes into the pending socket
  batch and refunds the difference.
- `poll_transmit` finding no sendable datagram cancels that connection's
  waiter without affecting another connection.
- Migration cancels the old `(handle, generation)` waiter before installing
  the new generation. The shared aggregate bucket is not reset or refilled.
- A connection drain detaches its waiter/waker and abandons any unsent socket
  batch exactly once.
- Socket `WouldBlock` neither refunds nor creates tokens.
- Fatal socket error abandons unsent bytes and wakes a peer only after the
  accounting transition is complete.
- Endpoint drop requires zero waiter, reservation, and outstanding leaks.

## End-To-End Hot Paths

### Forward path selected by the failure

```text
TunIo RX
  -> run_event_loop_sinked
  -> iface.poll / smoltcp TcpSocket recv
  -> process_dirty_relay
  -> process_listener_activity
  -> handle_local_payload (try_reserve before recv)
  -> RelayCommand::Data
  -> run_relay_writer
  -> TuicNativeTcpWriter / quinn::SendStream::poll_write
  -> quinn ConnectionDriver::drive_transmit
  -> quinn-proto Connection::poll_transmit
  -> per-path congestion + Pacer deadline
  -> EndpointPacingService reservation
  -> PacketBuilder::finish_and_track
  -> AsyncUdpSocket::try_send
  -> EndpointPacingService socket outcome
```

Continuous progress comes from smoltcp readiness/dirty scheduling, the bounded
relay channel writer, Quinn stream-write wakes, the connection driver, the
combined pacing timer, and cross-connection service wakers.

### Reverse D16 path preserved as a regression consumer

```text
quinn::RecvStream
  -> TuicNativeOrderedReader / poll_native_tcp_chunk
  -> ByteQueueReadReservation
  -> per-flow leased byte queue
  -> RelayEvent::DataReady
  -> service_local_egress_until
  -> process_dirty_relay(EgressActor)
  -> flush_downlink / TcpSocket::send_slice
  -> iface.poll
  -> flush_tx_and_release_downlink_permits
  -> TunIo::flush_tx
```

This path remains the only D16 smoltcp downlink admission owner. The endpoint
service changes QUIC UDP egress scheduling only; it does not reopen D16.

## Old-Path Audit

Still active and unchanged:

- Quinn congestion controller, RTT estimator, loss detection, per-path Pacer
  refill/debt, ACK generation, and connection timers;
- connection-driver `20` datagram work bound and immediate self-wake only when
  protocol work—not endpoint denial—remains;
- GSO enabled with Quinn's existing segment maximum;
- kernel UDP buffering and real `WouldBlock` handling;
- frozen TUIC pool=2, Cubic, MTU/PLPMTUD, QUIC windows, chunk, D16 actor and
  TUN feedback behavior;
- default Quinn policy when endpoint service is absent.

Must be inactive with `EndpointWindowV1`:

- `PacerCap64`;
- bounded `AsyncUdpSocket` cooldown;
- GSO-disabled product profile;
- driver poll-count or frozen-parameter tuning.

The rejected code may remain default-off for evidence until Task 12 cleanup,
but no acceptance launcher may compose it with this candidate.

## Safety, Liveness, And Boundedness

Safety:

- no connection datagram is built/accounted without endpoint reservation;
- aggregate finalized and socket-accepted accounting obey both time-window
  envelopes;
- `tokens + live reservations + outstanding <= burst` after every transition;
- a reservation is settled/refunded exactly once;
- `WouldBlock` cannot accumulate a hidden second burst;
- default policy has no changed behavior;
- pure ACK local Pacer accounting, loss tracking, congestion, crypto, packet
  number, GSO, and close semantics remain Quinn-owned.

Liveness:

- control reserve and priority keep protocol traffic bounded but live;
- one idle connection cannot retain service capacity;
- two busy connections rotate within one quantum;
- denied connections park on a timer/waker and do not busy-spin;
- cancellation and detach wake the next eligible connection;
- a dead connection cannot hold the endpoint turn or outstanding bytes.

Boundedness:

- one mutex-protected service state per configured endpoint;
- one fixed waiter record per live connection, no per-packet allocation;
- at most one outstanding socket batch per connection because Quinn already
  buffers at most one `Transmit` per driver;
- no new payload queue, task, channel, or kernel buffer.

## Observability Contract

Expose one endpoint snapshot with:

- configured rate, burst, control reserve, and quantum;
- available tokens, live reservation bytes, and outstanding socket bytes;
- granted/refunded/sent/abandoned bytes and datagrams;
- endpoint-delay events and maximum delay;
- current control/bulk waiter counts and high water;
- per-connection granted bulk/control bytes, turn count, wait count, and
  maximum service gap;
- fairness lead high water;
- socket `WouldBlock` outstanding high water;
- reservation, cancellation, migration, detach, and stale-waker counts;
- stateless response sent/dropped counts.

Acceptance logs identify endpoint policy and stable pool connection IDs but
never payload, UUID, password, certificate, key, or environment contents.

## Failure Discriminators

- RED replay does not produce more than 64 datagrams: the selected mechanism
  diagnosis is wrong; stop.
- Default equivalence differs: reject the patch.
- Token/window property test fails: service algorithm bug; no real local run.
- Actual built bytes exceed reservation or double-refund: accounting bug; no
  real local run.
- `WouldBlock` permits tokens plus outstanding above burst: physical burst
  theorem fails; no real local run.
- One busy connection cannot borrow full rate: idle-borrowing bug/capacity
  risk.
- Two busy connections exceed the fairness lead: scheduling bug/P8 risk.
- ACK/handshake/loss/close/path tests hang: control/liveness regression.
- Exact local `32 MiB` is clean but `<=170 Mbit/s`: implementation is not a
  sufficient capacity path. Stop; do not tune constants.
- Local capacity passes but endpoint delay is absent or old sender/cap is
  active: attribution failure; no VPS request.
- Later VPS pcap meets endpoint grant bounds but TUN/loss still fails: pacing
  is not the sufficient product root; stop pacing work.

## TDD Gate Summary

Implementation must proceed one vertical slice at a time:

1. preserve the 259-datagram sub-ms Pacer characterization;
2. default endpoint/config equivalence;
3. pure single-connection token/window reservation and refund;
4. socket-outstanding/`WouldBlock` theorem;
5. endpoint construction and stable connection keys;
6. two-connection idle borrowing and DRR fairness;
7. control classification/reserve and protocol liveness;
8. deadline/waker composition with no busy wake;
9. migration/cancel/detach cleanup;
10. mini_vpn branch-by-abstraction and startup attribution;
11. exact GSO-enabled local `32 MiB >170 Mbit/s` plus clean EOF;
12. vendored upstream suites, full mini_vpn library/harness, explicit
    64/256/1024 concurrency, UDP sweep, check/fmt/shell/diff-check, and final
    code review.

No VPS request follows automatically. A new architecture-specific VPS plan
and explicit user authorization are still required after all local gates pass.

## Stop Rules

- Do not implement until this spec and its implementation plan are explicitly
  confirmed.
- Do not run a real local throughput measurement before default, window,
  outstanding, fairness, control, no-busy-wake, and cleanup tests pass.
- If any unexpected repair test fails, analyze the violated invariant, propose
  a modification plan, and wait for confirmation before further repair edits.
- Do not change fixed service constants after a failed capacity test.
- Do not run VPS, P8, macOS TUN, old-path cleanup, or commit from this spec.

## Architecture And Refactoring Review

Clean-architecture score: **9/10**. Policy remains in the Quinn-proto inner
Module, while mini_vpn and the pinned Quinn driver are thin Adapters. The last
point requires implemented default-equivalence and lifecycle tests.

System-design score: **9/10**. Requirements, capacity, window theorem,
fairness, control, wake, failure, and observability contracts are explicit.
The remaining point requires measured local capacity and scheduler-cost data.

Refactoring score: **8/10** before implementation. The safe path to 10/10 is
named and incremental: Introduce Parameter Object
(`EndpointPacingServiceConfig`), Extract Module (`EndpointPacingService`),
Branch by Abstraction (`QuinnDefault`/`EndpointWindowV1`), and Parallel Change
for the additive Quinn driver callback before retiring cap64. No big-bang
rewrite is authorized.

Deletion test: removing the optional endpoint config and its two thin Adapters
must restore exact upstream Quinn behavior without touching TUIC, D16,
smoltcp, TUN, DNS, or UDP relay code.
