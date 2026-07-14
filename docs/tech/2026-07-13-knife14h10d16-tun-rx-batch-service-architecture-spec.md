# Knife14h10d16 TUN RX Batch Relay Service Architecture Spec

Date: 2026-07-13
Status: **IMPLEMENTED; local gates PASS, VPS acceptance pending**

Source evidence:

- `docs/tech/2026-07-13-knife14h10d16-endpoint-pacing-service-vps-results.md`
- `docs/tech/2026-07-09-knife14h10d16-byte-owned-egress-architecture-spec.md`
- `docs/tech/2026-07-13-knife14h10d16-endpoint-pacing-service-architecture-spec.md`

## Stage Goal

Remove the per-packet relay-service amplification on the local TUN ingress hot
path while preserving exact packet ingestion, transparent TCP/UDP/DNS
classification, D16 byte ownership, lifecycle, and every frozen product
parameter.

The candidate must make one bounded relay-service pass per non-empty TUN
drain batch instead of one pass per packet. It is intended to be sufficient
for a fresh forward P1 with:

- receiver throughput strictly above `170 Mbit/s`;
- zero TUN RX/TX drops;
- aggregate QUIC lost bytes no greater than `16 MiB`;
- clean endpoint conservation and relay lifecycle.

This is not another pacing candidate. EndpointWindowV1 remains unchanged and
default-off; QuinnDefault remains the product default outside the explicit
acceptance profile.

## Frozen Inputs And Non-Goals

Do not change:

- H10d16 byte-owned queue capacities, global budget, actor quantum, phases, or
  lifecycle;
- TUN MTU `1200` or txqueuelen `500` in the Gate profile;
- TUIC pool `2`, QUIC windows, the `64 KiB` application chunk, Cubic, GSO
  enabled, Quinn UDP sender, driver work bound, or D3 self-wake;
- EndpointWindowV1 rate, burst, control reserve, quantum, reservation rules,
  fairness, or timers;
- TCP socket buffers, downlink thresholds, TUN drain constants, qdisc, or any
  environment value;
- fake-IP DNS, UDP relay, REALITY/failover, server configuration, or protocol
  semantics.

Do not reopen bounded sender, PacerCap64/cap values, GSO-only, queue-length
tuning, drain-budget tuning, or broad parameter sweeps.

## Observed Failure And Rejected Branches

The endpoint VPS P1 reached `194 Mbit/s` receiver, kept aggregate QUIC loss at
`11,922,924B`, and ended with `live=0`, `outstanding=0`, and no socket blocking
or stateless drop. It nevertheless accumulated `10` local TUN TX drops.

Rejected as the next root:

- external path capacity: all direct/Exit baselines were `262-332 Mbit/s`;
- endpoint service capacity: local was `240.466 Mbit/s`, VPS was
  `194 Mbit/s`, and the fixed service was active;
- endpoint conservation or socket leak: final accounting closed exactly;
- QUIC flow-control blocking: all blocked deltas were zero;
- process CPU saturation: main-loop active time peaked at `25.6%`;
- remote/downlink lifecycle: forward data had no downlink payload pressure,
  pending reaping, or close-egress tail;
- another pacing constant: the failure is on Linux TUN TX before QUIC egress.

The remaining code-level branch is service amplification and scheduling
jitter between the kernel TUN qdisc and the already-bounded D16 uplink queue.

## Current End-To-End Hot Path

For each local TCP packet:

```text
Linux tun0 qdisc
  -> VirtualTunDevice::wait_for_rx / try_recv_rx (one packet)
  -> process_ready_tun_rx_packet
       -> classify_inbound / inspect_inbound_tcp
       -> Interface::poll
       -> flush_tx_and_release_downlink_permits
       -> process_dirty_relay
            -> allocate dirty handle snapshot
            -> process_listener_activity
            -> pump_established_uplink
            -> RelayCommand::Data
  -> drain_ready_tun_rx repeats the entire sequence for the batch
  -> service_local_egress_until polls and services dirty handles again
  -> run_relay_d16 writer
  -> Quinn stream
  -> EndpointPacingService reservation/settle
  -> UDP socket
```

The accepted local actor window already owns a batch boundary:

```text
48 TUN packets/cycle * up to 8 cycles/window
```

Pressure recovery may extend a drain attempt to at most `256` packets. The
VPS run recorded `452,225` packets in `6,565` drain attempts and `6,561` actor
cycles, but the inner call graph performed `process_dirty_relay` once per
packet. That is at least `452,225` relay passes from the drain path, plus the
outer actor passes, rather than O(drain attempts/cycles).

## Proposed Architecture

Split the current combined operation into two explicit services:

1. `ingest_ready_tun_rx_packet` owns exactly one ready packet:
   classification, UDP/DNS handling, TCP SYN inspection, `Interface::poll`,
   TUN TX flush, dirty marking, and packet-kind accounting. It performs no
   relay traversal.
2. `service_dirty_after_tun_batch` owns the relay boundary. After a non-empty
   bounded drain batch it calls `process_dirty_relay` exactly once using the
   same `ControlOnly` downlink admission and existing read-hard-pause inputs.

`process_ready_tun_rx_packet` remains the one-packet Adapter for the default,
non-H10 path. The H10d16 `wait_for_rx` branch passes its already-ready first
packet plus every immediately ready follow-up packet through the existing
bounded `drain_ready_tun_rx` service. This reachability refinement was required
after the first full-path tracer proved that leaving the direct branch on the
one-packet Adapter could still perform one relay traversal per packet when the
harness did not accumulate a kernel-sized backlog.

`drain_ready_tun_rx` uses the ingest-only operation inside its existing
bounded loop, then invokes one dirty service if and only if it ingested at
least one TCP packet. DNS/UDP-only batches do not traverse TCP listeners.
The enclosing local-egress actor may retain its subsequent phase/admission
pass; the new formal bound is therefore at most two relay passes per actor
cycle, independent of packet count.

No payload queue, task, channel, buffer, timer, wake source, or constant is
added. Packet order and packet-by-packet smoltcp polling remain unchanged in
the first implementation. A future multi-packet device buffer is out of
scope unless this smallest extraction fails its deterministic discriminator.

The bounded-drain probe may prefetch one additional packet into the device's
existing single RX slot. `wait_for_rx` now returns immediately when that slot
is already populated instead of overwriting it with a second read. This closes
the exactly-once boundary without adding storage.

## Invariants

Packet and protocol correctness:

- every successfully read TUN packet is classified and consumed exactly once;
- TCP packet order is unchanged;
- clean SYN registration happens before that packet enters smoltcp;
- DNS and UDP packets continue to bypass smoltcp and never trigger a TCP relay
  pass by themselves;
- every TCP ingest still runs `Interface::poll` and flushes generated control
  packets before the batch service boundary.

Scheduling and boundedness:

- a non-empty TCP drain batch performs exactly one inner dirty-service pass;
- the number of dirty-service passes is O(batch count), not O(packet count);
- drain limits and backlog-guard transitions are unchanged;
- no new queue or unbounded work is introduced;
- one busy flow may fill the existing smoltcp receive buffer and D16 command
  channel only through their existing nonblocking/backpressure contracts.

D16 and endpoint safety:

- D16 byte/reservation ownership, actor-only admission, phase transitions,
  EOF cleanup, and global/per-flow caps are unchanged;
- EndpointWindowV1 conservation remains:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= burst_bytes
```

- the default Quinn path is behaviorally unchanged.

## Capacity And Latency Math

At the observed `194 Mbit/s`, `1200`-MTU TCP traffic is about
`20,000-21,000` packets/s. The current drain path adds one active-handle
snapshot and relay traversal per packet. With the fixed `48`-packet actor
cycle, batch service reduces the dominant traversal rate by up to `48x`; a
pressure-extended `256`-packet drain reduces it by up to `256x`.

The D16 uplink pump already extracts all contiguous bytes currently available
from the `1 MiB` smoltcp receive buffer and can reserve up to `64` relay
commands per pass. Servicing once per `48` packets yields roughly `55 KiB` of
TCP payload at MTU `1200`, below the frozen `64 KiB` application chunk. Thus
one batch pass is sufficient to keep the existing writer fed without changing
chunk or channel capacity.

The path target is `>170 Mbit/s = >21.25 MB/s`. A `48`-packet batch contains
about `55 KiB`; fewer than `400` such batches/s sustain the target. The VPS
run already completed `6,565` drain attempts in about 20 seconds, over `320`
attempts/s, with many pressure-extended batches and `194 Mbit/s` delivered.
The change removes redundant work; it does not reduce a capacity-bearing
service rate.

## Necessary Versus Sufficient

Removing per-packet relay traversal is necessary to make the local ingestion
service proportional to bounded batches and to eliminate avoidable scheduling
jitter. The candidate is intended to be sufficient for the next formal VPS
gate because:

- endpoint wire capacity already passed;
- QUIC loss already passed its formal ceiling;
- only TUN TX drops failed;
- the modified seam is directly before the failed TUN qdisc boundary and
  preserves downstream backpressure.

VPS acceptance is still required. Deterministic tests cannot prove Linux
qdisc behavior or Tokio/kernel scheduling.

## Local Implementation Evidence

The focused RED consumed eight ready TCP packets and observed eight relay
entries. The minimal drain extraction turned the same trace GREEN at one
entry. TCP-only, UDP-only, and mixed TCP/UDP batches now prove that a non-empty
TCP batch receives exactly one inner relay pass while UDP-only work receives
none.

The first full-path tracer exposed that the direct ready branch still bypassed
the drain seam: it delivered exact bytes with zero modeled ring drops but
reported zero batch counters. Routing only H10d16 ready events through the
existing 48-packet bounded service closed that gap while retaining the
default one-packet Adapter.

The final real-Quinn forward gate delivered exactly `33,554,432B` at
`319.455 Mbit/s`, with zero pattern errors, clean EOF, zero modeled drops, and
a `29/500` ring high water. It ingested `28,934` TCP packets in `4,093`
batches, performed exactly `4,093` batch relay passes, and avoided `24,841`
per-packet passes. Final D16 owned, pending, inflight, terminal-drop, and late
payload bytes were all zero.

## Failure Discriminators

- RED tracer observes one relay pass per ingested packet: the diagnosed
  amplification is reachable.
- GREEN tracer still observes more than one inner relay pass per batch: the
  extraction is incomplete; no capacity run.
- Batch service changes packet order, DNS/UDP bypass, SYN handling, D16 bytes,
  EOF, or endpoint accounting: regression; stop and repair the violated
  invariant.
- A modeled `500`-packet forward TUN ring still drops while the old path drops:
  candidate is insufficient locally; do not run VPS.
- Exact local forward path is `<=170 Mbit/s`: architecture capacity failure;
  do not tune constants.
- Local gates pass but VPS still has any TUN drop: the batch-service
  architecture is insufficient; stop this branch and do not tune batching or
  drain constants.
- VPS clears TUN drops but QUIC loss exceeds `16 MiB` or endpoint conservation
  fails: QUIC/endpoint regression; diagnose from the existing counters.

## Observability

Add aggregate counters, exposed in the existing TUN RX diagnostic line:

- ingested packets;
- non-empty TCP batches;
- dirty-service passes after batches;
- packets-per-batch high water;
- relay passes avoided, computed as `tcp_packets - batch_service_passes`.

Acceptance requires nonzero avoided passes, exact equality between non-empty
TCP batches and batch dirty-service passes, and no counter reset inside the
formal window.

## Stop Rules

- No frozen parameter or endpoint constant may change.
- Expected RED may enter the minimal extraction directly.
- Unexpected repair/regression failure must first be tied to a violated
  invariant and a concrete repair plan; the user's continuous-repair
  authorization permits that safe in-scope repair without another prompt.
- Do not run macOS TUN.
- Do not rerun EndpointWindowV1 acceptance until focused RED/GREEN, exact local
  capacity, full gates, and code review pass.
