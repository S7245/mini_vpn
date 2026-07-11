# Knife14h10d16 Byte-Owned TCP/TUN Egress Architecture Spec

Date: 2026-07-09
Status: approved for implementation

Amended: 2026-07-10 after the credit-rearm Gate A. This amendment corrects
Linux TUN counter direction, closes the global-drop/per-flow-Recovery scope
gap, and adds a required TUN RX starvation falsifier. It preserves the D16
ownership, actor, EOF, Gate A, and Gate B decisions.

Amended: 2026-07-11 after the alternate-Exit capacity run. This amendment
preserves the data-plane architecture, makes queue closure cause explicit, and
defines Gate A as timed capacity plus fixed-byte clean EOF under one AND gate.

## Decision

Keep the H10d15 capability that mattered: a continuously serviced, independent
TUIC/QUIC read pump feeding a single-owner smoltcp/TUN egress loop. Replace the
current split ownership with one byte ledger that begins before the Quinn read,
remove payload staging that is bounded only by message count, make D3 actor mode
the only smoltcp downlink admission owner, and separate read/admission pause from
local drain service.

This is a boundary-closing architecture change, not another read-floor, chunk,
self-wake, MTU, VPS, or QUIC-window tuning stage.

## Evidence Behind The Decision

- The same `.27 -> .33 -> .77` topology supports mature sing-box client
  receiver throughput around `173-185 Mbit/s`.
- H10d15 reached `186 Mbit/s` sender and `185 Mbit/s` receiver with
  `engine=native_permit_pump` and `native_read_floor=true`.
- H10d15 reduced active data read gaps from multi-second scale to sub-second
  scale, proving that independent service-sized read progress is necessary.
- H10d15 was not clean acceptance: it ended with `tx_dropped_delta=229`,
  `global_rx_paused=true`, `close_egress_class=terminal_closed_no_send`, and
  `close_egress_bytes=327272`.
- Code review found three contract breaks:
  - `TuicNativeOrderedPumpReader` reads into a message-count channel before the
    D6 byte-permit queue, and its source ignores `_max_len`;
  - D3 actor mode still admits bytes through legacy `process_dirty_relay`
    call sites outside `service_local_egress_until`;
  - hard pause returns before ACK/TUN RX, `iface.poll`, and `flush_tx`, so the
    actor stops the drain work needed to recover.

## Goals

### Primary throughput goal

- Preserve sing-box-class single-flow reverse throughput on the current
  topology.
- Target a three-run median receiver throughput of at least `170 Mbit/s`.
- Require every parity run to exceed `150 Mbit/s`.
- If a same-window sing-box control is itself below `170 Mbit/s`, require the
  mini_vpn median to reach at least `90%` of that control while still exceeding
  `150 Mbit/s` per run.

### Correctness and stability goals

- No TUN `tx_dropped` increase during the accepted window or close tail.
- No `terminal_closed_no_send`, terminal pending reap, or nonzero close-egress
  bytes for the data flow.
- Preserve ordered, exact-once TCP byte delivery and existing half-close rules.
- Keep queues and reservations bounded per flow and process-wide.
- Preserve UDP, fake-IP DNS, TUN lifecycle, and high-concurrency behavior.

## Non-Goals

- Do not tune VPS sysctl, iperf3, MTU/PLPMTUD, stale pool behavior, or broad
  QUIC windows.
- Do not revive unordered reassembly as the product path.
- Do not make H4 ordered-chunk or the current H10d15 implementation the default
  before the new gates pass.
- Do not share `quinn::RecvStream`, smoltcp `Interface`, `SocketSet`, or TUN
  ownership through `Arc<Mutex<_>>`.
- Do not rewrite the UDP data plane or replace smoltcp with a Linux-only
  transparent-proxy mechanism.

## Capacity Math

- `170 Mbit/s = 21.25 MB/s`.
- At `5ms`, the data plane must advance about `106.25 KB`.
- At `10ms`, it must advance about `212.5 KB`.
- The existing actor target of `128 KiB / 5ms` has a theoretical service rate
  of about `209.7 Mbit/s`, leaving roughly `23%` headroom over the goal.
- Four `128 KiB` service quanta form a `512 KiB` heavy-flow reservoir, enough
  for about `24.7ms` of data at `170 Mbit/s`.

The initial product budget is therefore:

```text
per active TCP flow reservoir cap = 512 KiB
process-wide TCP reservoir cap    = 64 MiB
actor service target              = 128 KiB per service window
actor max cycles                  = 8 per bounded invocation
```

The service target is cumulative, not one smoltcp admission burst. The local
TUN feedback boundary adds a second, MTU-derived sliding limit:

```text
maximum unacknowledged payload packets per D16 flow = 24
available admission = 24 * (TUN MTU - IPv4/TCP minimum headers)
                      - current smoltcp send_queue bytes
```

At MTU 1200 this is at most `27,840B` per admission edge; at MTU 1500 it is at
most `35,040B`. Eight actor cycles still cover the `128 KiB` service target.
The 24-packet bound reserves room in the modeled 64-packet TUN RX ring for up
to two ACK/window-update feedback packets per admitted payload packet. The
local actor therefore consumes up to 48 feedback packets before treating a
further ready packet as device backlog; the remaining 16 ring slots are
pre-existing-backlog headroom. Running keeps its `512 KiB` read reservoir and
Recovery keeps its `128 KiB` read quantum; neither value is permission to
inject that many bytes into smoltcp in one edge.

Reservations are demand-allocated. Idle flows hold zero payload reservation,
so the per-flow cap does not imply `512 KiB * all listener slots`. The shared
cap prevents many simultaneously saturated flows from multiplying memory
without bound. Global fairness is required before concurrency acceptance, but
the first single-flow gate may use one shared budget with one active data flow.

## Target Data Flow

```text
quinn::RecvStream owner task
  -> reserve bytes from per-flow + global budget
  -> ordered Quinn read limited to reservation length
  -> commit bytes into per-flow leased byte queue
  -> coalesced DataReady(handle, epoch) wake on global_rx
  -> single TCP egress actor drains queue into SocketCtx pending
  -> smoltcp TcpSocket::send_slice
  -> iface.poll
  -> TunIo::flush_tx
  -> release byte lease on observed TUN/local egress commit
```

`global_rx` carries readiness, close, and error events. It is not a second
payload reservoir. A duplicate `DataReady` is harmless; payload ownership stays
inside the per-flow queue and remains covered by the byte ledger.

## Rust Task And Ownership Model

### TUIC read pump

- One Tokio task owns each `quinn::RecvStream`.
- The task must acquire a `ReadReservation` before polling Quinn.
- The read operation may produce at most the reservation length.
- Unused reservation bytes are refunded immediately.
- Cancellation, EOF, error, and task drop release uncommitted reservation by
  RAII.
- No nested ordered-pump task or message-count payload channel is allowed.

### TUIC write pump

- Keep the existing independent writer task and bounded uplink command channel.
- Writer shutdown must not abort a useful reverse read before lifecycle rules
  permit it.

### Local egress actor

- The main event-loop task remains the only owner of smoltcp `Interface`,
  `SocketSet`, and `TunIo`.
- In D3 mode, only the actor may call `send_slice` for downlink data.
- Timer, TUN RX, relay wake, and close events may schedule actor work, but may
  not perform an unmeasured legacy admission first.
- Work is bounded and fair: one busy flow cannot consume an unbounded loop.

## Byte Ownership Invariant

For each flow, every remote byte is in exactly one state:

```text
reserved_for_read
  -> queue_queued
  -> queue_leased_to_actor
  -> local_pending
  -> smoltcp_inflight
  -> committed_to_tun_or_explicitly_dropped
```

At all times:

```text
reserved + queued + leased + pending + smoltcp_inflight <= per_flow_cap
sum(all flow owned bytes) <= global_cap
```

Permit release is allowed only for:

- observed successful local egress commit;
- explicit terminal drop with a recorded reason and byte count;
- unused read reservation;
- cancellation before bytes are produced.

Dispatcher pop, actor wake, and smoltcp admission alone do not release the
permit.

## Backpressure State Machine

```text
Running
  read=yes, admit=yes, drain=yes

DrainOnly
  read=no, admit=no, drain=yes

Recovery
  read=one bounded quantum, admit=one bounded quantum, drain=yes
```

Transitions:

- `Running -> DrainOnly` on hard local pressure, active drop debt, TUN drop, or
  terminal no-send state.
- `DrainOnly -> Recovery` only after pressure is below the low watermark,
  drop debt is clear, and a drain cycle made progress.
- `Recovery -> Running` after four clean progress cycles.
- `Recovery -> DrainOnly` immediately on renewed pressure or drop evidence.

The key rule is that `DrainOnly` never disables ACK/TUN RX, `iface.poll`,
`flush_tx`, permit release, or close-tail drain. Drop feedback is an emergency
circuit breaker, not the primary throughput pacer.

### Post-Gate-A device-pressure amendment

Linux `tun0` direction is part of the contract:

- `tx_dropped` is the kernel-to-userspace TUN transmit-ring drop counter. In
  this product path it means local ACK/control/uplink packets were not read by
  mini_vpn in time.
- `VirtualTunDevice::flush_tx` writes remote/downlink packets in the opposite
  userspace-to-kernel direction. A zero `flush_tx` failure count does not make
  `tx_dropped` a TUN-write failure.
- Therefore TUN-drop prevention must be tested at the TUN RX service boundary,
  not implemented by assuming OS write backpressure in `flush_tx`.

Drop recovery has one ownership scope:

- a TUN drop episode and its debt are device-global;
- measured aggregate pressure reduction across feedback samples may pay that
  global debt exactly once, up to the observed drain, without minting actor
  admission credit;
- the same one-shot clean-drain evidence is offered to every active D16 flow
  held by that global episode; each flow still applies its own terminal and
  low-watermark guards before entering Recovery;
- ordinary actor-cycle `drain_progress` means strictly positive completed
  drain bytes. A successful zero-byte poll/flush cycle remains observable but
  does not advance Recovery.

Before another Gate A, a deterministic production-seam harness must model a
bounded kernel-to-userspace TUN RX ring and reproduce or falsify the observed
starvation shape. The required behavior is:

1. sustained D16 downlink admission causes local ACK/control packets to enter
   the modeled TUN RX ring;
2. reader service, actor admission, `iface.poll`, and close use the production
   scheduling seam;
3. the current implementation must demonstrate the drop/backlog failure before
   a preventive mechanism is selected;
4. if reproduced, use device-wide, self-resetting backlog evidence at the TUN
   RX boundary to stop D16 reads/admission while TUN RX/poll/flush continues;
5. recovery requires two independent clean observations separated by one
   admission-free control epoch, not a timer or arbitrary debt forgiveness;
6. a flow that still has unacknowledged smoltcp `send_queue` bytes remains in
   DrainOnly until its own queue reaches zero, without blocking clean flows.

Task 11A implementation clarification, accepted from the production-seam RED:

- the device backlog guard is authoritative for phase transitions, published
  read credit, and the initial credit/phase of a newly installed relay;
- local actor service consumes the full 48-packet feedback allowance for one
  24-payload-packet window. Only a further ready packet after that allowance
  proves backlog and upgrades the same call to the existing bounded pressure
  budget (maximum 256 packets), so normal ACK batches do not trip the circuit
  breaker while real backlog is consumed before new admission;
- a first clean `WouldBlock` after backlog only arms recovery. The device guard
  stays active through one `ControlOnly` poll/flush epoch with zero admission;
  only a later independent clean `WouldBlock` releases the device guard;
- forcing a flow to DrainOnly records a per-flow ACK-completion barrier when
  its smoltcp `send_queue` is nonzero. Device recovery is readiness-only: each
  flow clears its own barrier and enters Recovery only after its own
  `send_queue` reaches zero, so one slow flow cannot impose global head-of-line
  blocking;
- read-reservoir capacity and actor-admission quantum are distinct. `Running`
  may keep a `512 KiB` owned read opportunity and Recovery a `128 KiB` read
  opportunity, while both phases use the cumulative 24-packet sliding
  admission window described above.

This falsifier does not authorize a raw-splice path, deletion of D16 phases,
MTU/PLPMTUD changes, broad QUIC-window changes, chunk-size changes, or
self-wake tuning.

## Read Service Policy

- `paused` or `DrainOnly` means no Quinn read is armed.
- In `Running`, a nonzero read opportunity may request at least one actor
  quantum (`128 KiB`) but cannot exceed available per-flow/global reservation.
- In `Recovery`, request at most one actor quantum.
- A read already pending must select on both Quinn readiness and feedback state
  changes. Entering `DrainOnly` cancels that pending read and refunds the
  reservation before any new read is armed.
- The final implementation must not preserve the H10d15 behavior where any
  nonzero credit silently bypasses a smaller requested limit without a tracked
  reservation.

## EOF And Close Contract

- Clean remote EOF closes the per-flow queue for new pushes.
- `DataReady` wakes the actor to drain all queued and leased bytes.
- EOF becomes visible to the local TCP lifecycle only when the queue is closed
  and empty and all reservations are accounted for.
- Data and EOF must not race through different global channel producers.
- Local FIN does not cancel useful reverse data while the local socket can
  still receive it.
- A terminal local socket may explicitly drop remaining owned bytes, but must
  report one terminal reason and release the exact byte count once.
- Rearm increments the epoch and detaches the old queue so late wakes cannot
  attach old data to a new flow.

## Observability Contract

Per-flow or aggregate diagnostics must expose:

- `read_reserved_bytes`, `read_reservation_high`;
- `queue_queued_bytes`, `queue_leased_bytes`, `queue_high`;
- `actor_admitted_bytes`, `actor_bypass_admitted_bytes`;
- `egress_phase`, phase transition counts, and recovery resets;
- `drain_only_cycles`, `drain_only_drain_bytes`;
- `permit_released_bytes`, `permit_terminal_drop_bytes`;
- `remote_eof_queue_bytes`, `remote_eof_inflight_bytes`;
- maximum remote read, actor no-progress, and local drain gaps.

Acceptance requires `actor_bypass_admitted_bytes=0` in D3 mode and exact byte
conservation at close.

## Failure Discriminators

- Reservation or ownership invariant fails locally: implementation bug; do not
  run VPS acceptance.
- Local gates pass, but queue stays full and actor drain is low: local
  smoltcp/TUN actor is the limiter.
- Local gates pass, actor drains, but remote read gaps return: Quinn read wake
  or reservation rearm is the limiter.
- Throughput is high but TUN drops remain: local commit/release semantics or
  proactive kernel-to-userspace TUN RX service/backpressure is insufficient.
- TUN drops are zero but close bytes remain: EOF/local lifecycle ordering is
  insufficient.
- One clean high run followed by large variance: architecture capacity exists,
  but scheduling/fairness stability is not accepted.

### 2026-07-10 ACK-capacity Gate A observation

The repaired 64 MiB production seam passed 50 capacity-qualified repeats at
about `224 Mbit/s`, then the single authorized VPS Gate A reached only
`19.2/17.9 Mbit/s` sender/receiver. The VPS run kept TUN drops, actor bypass,
local pressure/backlog, send/flush failures, and QUIC loss/congestion/blocking
at zero. The actor admitted all bytes it received, but the ordered data reader
showed repeated gaps up to `3548ms` despite active polling.

This selects the existing “actor drains, but remote read gaps return” branch.
It does not invalidate the byte-owned queue, actor exclusivity, DrainOnly, or
24-packet admission contracts. The next local seam must provide same-stream
evidence: connection-global STREAM frame progress alone cannot prove that
contiguous bytes were deliverable on the data stream. Gate B remains frozen
until a future Gate A passes.

### 2026-07-11 terminal-close and Gate A evidence amendment

The alternate-Exit run proved `183 Mbit/s` receiver capacity with zero TUN
drop, actor bypass, send/flush error, and QUIC loss/blocking evidence. At the
20-second boundary, however, the iperf data socket moved directly from
`Established` to terminal `Closed` before the TUIC data stream observed remote
EOF. The exact remaining ownership was the bounded `524288B` D16 reservoir plus
`27840B` in the smoltcp send queue. Those bytes cannot be delivered after the
peer reset and must not be hidden by shrinking the reservoir or changing actor
cadence.

The architecture remains unchanged. Queue closure is now an explicit state:

```text
Open | RemoteEof | Terminal(direction, reason)
```

The first terminal cause is authoritative and survives queue cleanup, reader
shutdown, relay supervision, and final reporting. `RemoteEof` remains distinct
and may become visible to the local TCP side only after owned queue, pending,
and inflight bytes drain.

Gate A remains one AND gate, but it now contains two traffic-shaped evidence
windows from the same binary, safe1200 profile, Exit service, and tunnel:

1. **A-capacity:** the established 20-second reverse-first P1 proves sustained
   receiver capacity and cadence. A timed peer abort is not a clean-EOF proof;
   if it occurs, it must be classified exactly as
   `local_to_remote/local_socket_terminal`, release ownership once, remain
   bounded by the configured D16 and smoltcp reservoirs, and produce no other
   terminal reason.
2. **A-clean:** after the capacity flow is quiet, one fixed-byte reverse
   `iperf3 -n 64M -P 1 -R` flow proves natural completion. It must report
   `clean_queue_lifecycle`, exact receiver completion, and zero queue,
   reserved, leased, pending, inflight, terminal-drop, and close-egress bytes.

This supersedes the impossible requirement that one abort-capable timed socket
simultaneously prove graceful EOF. It does not weaken byte ownership or clean
EOF: each property is now tested by a generator that can actually establish it.

### 2026-07-11 composite Gate A pool-lifecycle observation

The composite run used a capable temporary Exit window: mature control reached
`157.650 Mbit/s` receiver and direct reverse reached `212.607 Mbit/s`. Pool-2
mini_vpn stopped receiving sustained TUIC stream data before the D16 actor,
while every local ownership, pressure, actor, TUN-drop, and QUIC-loss/blocking
surface stayed clean. Pool 1 improved the same path to `115 Mbit/s`, still below
Gate A.

This does not amend the byte-owned egress architecture. It exposes an adjacent
transport-pool lifecycle policy: a healthy-looking idle auxiliary slot is
forcibly reconnected after `10s`, while the primary connection is exempt. The
next architecture-safe correction is to use observable connection health,
close state, or bounded open failure rather than elapsed idle time as the
reconnect authority. Pool 1 is not an acceptable substitute for pool health or
concurrency. Gate B remains frozen.

## Stage Mapping

The project remains nominally at stage 8 because H10d15 proved capacity but
failed the clean Gate A contract. Before another VPS run, stages 3 through 7
must be reopened and closed against this spec:

1. Update architecture spec and capacity contract.
2. Add pre-read reservation and exact byte ownership tests.
3. Replace hidden TUIC payload staging with the byte-owned per-flow queue.
4. Make D3 actor the exclusive downlink admission owner.
5. Add Running/DrainOnly/Recovery feedback semantics.
6. Make EOF and close drain through the same owned queue.
7. Pass deterministic integrated harness and full local regression gates.
8. Run one composite VPS Gate A: timed capacity plus fixed-byte clean EOF.
9. Run parity/stability Gate B against the `170 Mbit/s` target.
10. Run TCP concurrency, UDP/live-streaming, fake-IP DNS, and TUN lifecycle
    regressions.
11. Remove or retain old H4/D2/D3/D6/D15 paths only according to evidence.

## Acceptance

### Local architecture gate

- No read occurs without a live reservation.
- Produced bytes never exceed the reservation.
- Ownership sums never exceed per-flow or global caps.
- D3 actor admission equals total D3 `send_slice` admission.
- DrainOnly produces zero read/admission bytes and positive drain progress when
  local work exists.
- A device-global drop episode can consume measured aggregate drain exactly
  once and cannot strand eligible flows in DrainOnly after pressure is clean.
- A zero-byte drain cycle does not count as Recovery progress.
- The bounded TUN RX starvation production seam passes before another VPS run.
- A first clean TUN RX probe cannot reopen admission before an admission-free
  control epoch and a later independent clean probe.
- Per-flow ACK-completion barriers isolate a slow flow rather than requiring a
  device-global all-flows-zero condition.
- D16 Running and Recovery never exceed 24 modeled payload packets outstanding
  after deducting the current smoltcp send queue.
- A ready local TCP packet can schedule the same bounded actor without waiting
  for the 5ms timer; no TUN-RX call site may admit outside that actor.
- The 64 MiB production seam exceeds `170 Mbit/s` while the circuit breaker
  remains exceptional rather than firing once per admission window.
- EOF is observed after payload drain, with zero permit leak or double release.
- Full lib tests, check, fmt, diff-check, and suite self-test pass.

### VPS Gate A

- A-capacity uses one focused 20-second reverse-first P1 and exceeds
  `150 Mbit/s` receiver.
- A-capacity has `tx_dropped_delta=0`, actor bypass `0`, send/flush error `0`,
  active remote-read/local-egress gaps below `1s`, and QUIC loss, congestion,
  and blocking remain non-root.
- A-capacity may end with exactly one classified `local_socket_terminal` data
  flow. Its released D16 ownership and smoltcp egress are exact, one-shot, and
  bounded; `other_terminal_events=0`.
- After the capacity flow is quiet, A-clean uses one fixed `64 MiB` reverse P1
  on the same binary/profile/tunnel and completes by remote EOF.
- A-clean has `clean_queue_lifecycle`, `tx_dropped_delta=0`,
  `close_egress_bytes=0`, terminal pending/drop `0`, and queue queued/leased/
  reserved bytes `0`.
- Gate A passes only when both A-capacity and A-clean pass.

### VPS Gate B

- One same-window sing-box control and three mini_vpn focused runs.
- Every mini_vpn receiver result exceeds `150 Mbit/s`.
- Median target is at least `170 Mbit/s`.
- If the control is below `170 Mbit/s`, mini_vpn median is at least `90%` of
  the control while every run still exceeds `150 Mbit/s`.
- All timed runs have zero TUN drops, no unclassified terminal cause, and exact
  bounded terminal accounting when their time boundary aborts a data socket.
- One fixed-byte A-clean repeat on the same accepted build has zero close tail
  and `clean_queue_lifecycle` before product regression begins.

### Product regression gate

- Sustained 60-second TCP reverse run remains above `150 Mbit/s` with clean
  tail.
- Multi-flow TCP keeps bounded per-flow/global ownership and no loop stall.
- Existing UDP/live-streaming, fake-IP DNS, and TUN lifecycle suites pass.

## Stop Rules

- Do not run VPS Gate A until the local ownership, actor-exclusivity,
  DrainOnly, EOF, global-drop recovery, and bounded TUN RX starvation tests all
  pass.
- Do not tune reservoir size or actor quantum after a failed VPS run until the
  failure discriminator identifies capacity or cadence as the active limiter.
- Do not call one `>170 Mbit/s` run stable acceptance.
- Do not promote the feature gate to default before Gate B and product
  regression pass.

## 2026-07-11 Transport-Service Observation

Evidence-based auxiliary pool health is now implemented at `6209910`, but its
clean generation-1, no-reconnect pool-2 A/B reached only `108 Mbit/s` against a
same-window `195.033 Mbit/s` mature control. This does not change any D16
architecture invariant. It rejects destructive idle reconnect as the active
capacity root and moves the open diagnosis boundary upstream to TUIC stream
service/frontier progress. Gate A remains frozen until that boundary has a
deterministic discriminator and a reviewed sufficient path.

## 2026-07-12 Stream Frontier And Armed-Read Amendment

The direct D16 reader remains ordered and transport-owned. A service-sized
application `AsyncRead` buffer is not part of the architecture: clean VPS
evidence stopped after `18356B`, while restoring Quinn-owned ordered chunks
recovered useful burst capacity without adding payload staging. The loopback
gate now locks down both `>=170 Mbit/s` capacity and native-chunk-sized handoff.

Application-owned unordered reassembly is also rejected for D16. A local
tracer bullet showed that one missing offset can consume the complete `512 KiB`
per-flow byte ledger before retransmission arrives. Increasing that reservoir
would trade away the high-concurrency bound; dropping already consumed
unordered chunks would violate TCP correctness.

An acquired read reservation is an already-owned opportunity. A quantitative
credit change within `Running` updates the next reservation but must not cancel
or resize the pending Quinn read. Only `DrainOnly`/hard pause, channel close,
reader stop, or transport completion may end that pending operation. The
reservation remains included in per-flow and global ownership throughout.

The external acceptance stop rule is unchanged. A fresh mature-client control
above `150 Mbit/s` with zero socket drops is required before one scoped D16 run;
composite Gate A remains forbidden until that scoped run also exceeds
`150 Mbit/s` with clean local invariants.
