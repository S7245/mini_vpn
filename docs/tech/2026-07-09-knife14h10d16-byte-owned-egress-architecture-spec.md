# Knife14h10d16 Byte-Owned TCP/TUN Egress Architecture Spec

Date: 2026-07-09
Status: approved for implementation

Amended: 2026-07-10 after the credit-rearm Gate A. This amendment corrects
Linux TUN counter direction, closes the global-drop/per-flow-Recovery scope
gap, and adds a required TUN RX starvation falsifier. It preserves the D16
ownership, actor, EOF, Gate A, and Gate B decisions.

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
to two ACK/window-update feedback packets per admitted payload packet plus the
ordinary 16-packet drain budget. Running keeps its `512 KiB` read reservoir and
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
- reaching the ordinary reader budget with another packet ready upgrades that
  same call to the existing bounded pressure budget (maximum 256 packets), so
  ACK backlog is consumed before the actor flushes the already-admitted tail;
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
8. Run one clean `>150 Mbit/s` VPS Gate A.
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
- EOF is observed after payload drain, with zero permit leak or double release.
- Full lib tests, check, fmt, diff-check, and suite self-test pass.

### VPS Gate A

- One focused 20-second reverse-first P1 receiver result exceeds
  `150 Mbit/s`.
- `tx_dropped_delta=0`.
- `close_egress_bytes=0`, terminal pending reap `0`, and no
  `terminal_closed_no_send` for the data flow.
- Active remote read and local egress gaps remain below `1s`.
- QUIC loss, congestion, and blocking remain non-root.

### VPS Gate B

- One same-window sing-box control and three mini_vpn focused runs.
- Every mini_vpn receiver result exceeds `150 Mbit/s`.
- Median target is at least `170 Mbit/s`.
- If the control is below `170 Mbit/s`, mini_vpn median is at least `90%` of
  the control while every run still exceeds `150 Mbit/s`.
- All runs have zero TUN drops and clean close accounting.

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
