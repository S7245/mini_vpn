# Knife14h10d16 TUN Ingress Service Architecture Spec

Date: 2026-07-13
Status: **LOCAL GATE PASS; eligible for one frozen VPS P1**

## Stage Goal

Eliminate the kernel-facing TUN read-cadence gap and the remaining per-packet
smoltcp poll/flush amplification while preserving transparent TCP, UDP/DNS
bypass, H10d16 ownership, EndpointWindowV1, and every frozen product
parameter.

This stage is intended to be sufficient for one new zero-TUN-drop,
`>170 Mbit/s` forward P1. The Mbps result still requires VPS acceptance.

## Accepted Evidence

EndpointWindowV1 alone reached `194 Mbit/s` receiver with `10` TUN TX drops.
The follow-up batch relay service reached the same `194 Mbit/s`, kept QUIC
loss at `11,717,996B`, and proved exact batch reachability:

```text
847,092 TCP packets
3,712 batches == 3,712 dirty-relay passes
843,380 avoided dirty-relay passes
38 TUN TX drops
loop active 20.0-62.6%, poll 14.1-44.0%, relay 0.8-2.8%
```

The relay share fell below `3%`, but TUN drops increased and the poll section
remained dominant. The current `VirtualTunDevice` also reads the real TUN fd
only when the single relay actor selects or probes it. Each TCP packet still
invokes `Interface::poll` and `flush_tx_and_release_downlink_permits` before
the next packet is read.

## Ranked Design Tree

### H1 — Selected: no independent TUN read owner plus per-packet poll/flush

Prediction: a dedicated reader can keep the kernel queue drained during actor
work, and one smoltcp poll/flush per staged TCP batch can reduce the dominant
on-loop section without changing packet or byte service limits.

Discriminator: local and VPS metrics must show exact pump FIFO delivery,
`tcp_batch_iface_polls == tcp_batches`, nonzero avoided poll/flush calls,
pump FIFO below capacity, and zero TUN drops.

### H2 — Rejected: dirty-relay traversal is still the primary bottleneck

Relay service was coalesced exactly and now consumes at most `2.8%` of the
loop, yet the drop gate worsened. Do not add another relay-only optimization.

### H3 — Rejected: endpoint pacing capacity or QUIC flow control

Receiver throughput passed, endpoint conservation was exact, aggregate QUIC
loss stayed below `16 MiB`, and every QUIC blocked counter remained zero.
Pacing constants and QUIC windows remain frozen.

### H4 — Rejected: TUN queue/MTU/drain-budget tuning

The stage must work with MTU `1200`, kernel queue estimate `500`, the existing
48-packet normal bound and derived 240-packet pressure extension. Increasing
any capacity would hide rather than replace the missing ownership seam.

## Capacity Gate

At MTU `1200`, a conservative full-size TCP payload is about `1160B`.

```text
170 Mbit/s / 8 / 1160B = 18,319 packets/s
239.167 Mbit/s / 8 / 1160B = 25,772 packets/s
observed failed P1 ingress = 847,092 / 20s = 42,355 packets/s
```

The actor retains the existing maximum pressure batch of `240` packets. At
only `200` batch services/s it has a nominal `48,000 packets/s` capacity;
the failed run already executed roughly `300+` local-egress service cycles/s
while remaining below CPU saturation. Coalescing poll/flush should increase,
not reduce, this service capacity.

The userspace FIFO capacity is not a tuned value. It mirrors the already
frozen `tun_tx_queue_len` estimate (`500` packets in the accepted profile):

```text
bounded pump FIFO          <= 500 * rx_buffer_capacity_for_mtu(1200)
                           < 0.7 MiB plus queue metadata
live reader ownership     <= one additional packet
raw prefetch ownership    <= one additional packet
smoltcp staged ownership  <= existing 240-packet pressure batch
kernel + userspace jitter envelope = 1,000 packets
1,000 / 42,355 packets/s = 23.6ms observed-rate envelope
```

VPS eligibility additionally requires the FIFO not to reach capacity. A full
wait means the architecture lacks headroom even if the kernel happens not to
report a drop.

## Old Hot Path Inventory

The accepted H10d16 forward path is:

```text
Linux TCP -> tun0 kernel queue
  -> VirtualTunDevice::wait_for_rx / try_recv_rx
  -> drain_ready_tun_rx
  -> prepare_ready_tun_rx_packet for every packet
       -> inspect/classify
       -> Interface::poll
       -> flush_tx_and_release_downlink_permits
  -> process_dirty_relay once per TCP batch
  -> service_local_egress_until / TUIC stream write
  -> Quinn EndpointPacingService
  -> UDP socket -> sing-box -> target
```

The batch repair removed only repeated `process_dirty_relay`. The real TUN fd
read, `Interface::poll`, and flush remain serialized per packet on the actor.

## New Architecture

### 1. H10d16-Only Reader Pump

For H10d16, split `tun::AsyncDevice` with `tokio::io::split`:

- a spawned `TunIngressService` owns the read half;
- `VirtualTunDevice` retains the write half;
- the pump reads exactly one TUN packet at a time into a bounded FIFO;
- FIFO capacity equals `runtime_config.tun_tx_queue_len`;
- send waits rather than drops when the FIFO is full;
- read error and EOF are explicit terminal events;
- dropping the device aborts the pump and closes the FIFO; deterministic
  natural-close/error tests join the task and prove terminal accounting.

The default non-H10 path retains the current unsplit `AsyncDevice` adapter.

### 2. Separate Raw-Ready And smoltcp-Staged Queues

`TunIo` gains a narrow staging operation. A packet is first owned by the raw
ready slot for DNS/UDP classification and SYN inspection. TCP packets are
moved exactly once into a private smoltcp-ready FIFO. `Device::receive` drains
that FIFO in order. DNS/UDP packets remain bypass-owned and never enter
smoltcp.

Invariants:

```text
raw slot + smoltcp staged FIFO + pump FIFO own each received packet exactly once
no packet exists in more than one owner
TCP order is FIFO-preserved
DNS/UDP bypass packets are consumed exactly once
the pump FIFO is capacity-bounded; the raw slot owns at most one packet; the
smoltcp staged FIFO is bounded by the existing 48/240 packet drain limits
```

### 3. One Batch Poll/Flush/Relay Service

`drain_ready_tun_rx` keeps its existing normal and pressure packet bounds. It
classifies each ready packet and prepares SYN listeners before staging TCP.
After collection of a nonempty TCP batch it performs:

1. one `Interface::poll`, which repeatedly calls `Device::receive` until the
   staged TCP FIFO is empty;
2. one `flush_tx_and_release_downlink_permits`;
3. existing backlog-guard transition handling;
4. one `process_dirty_relay` with unchanged `ControlOnly` admission.

The default `process_ready_tun_rx_packet` remains a one-packet composition and
therefore stays byte/behavior equivalent outside H10d16.

## Ordering And Lifecycle

- TCP packets are staged in TUN read order and consumed by smoltcp in that
  order.
- SYN inspection and listener creation happen before the staged SYN can be
  consumed.
- DNS and raw UDP preserve their existing bypass handlers. Cross-protocol
  global ordering is not a transport guarantee; per-flow order and exact
  ownership are mandatory.
- A pump read error is surfaced once and closes the ingress service. A
  terminal TUN wait error exits `run_event_loop`, so a closed FIFO cannot
  become a busy wake loop.
- Natural EOF/error and consumer close terminate the reader task. Dropping the
  device aborts any still-pending read and drops all remaining owned buffers.
- TUN writes and all smoltcp/socket state remain actor-owned; the pump never
  touches `Interface`, `SocketSet`, relays, fake IP, metrics state, or the
  writer.

## Observability

Periodic aggregate diagnostics add:

```text
pump_packets / pump_bytes
pump_queue_high_water / pump_queue_capacity
pump_full_waits
pump_read_errors / pump_closed
tcp_batch_iface_polls
tcp_batch_flushes
avoided_iface_polls
tcp_batch_packets_high_water
```

No per-packet logging is allowed. Required live equalities are:

```text
tcp_batch_iface_polls == tcp_batches
tcp_batch_flushes == tcp_batches
avoided_iface_polls = tcp_packets - tcp_batch_iface_polls
pump_packets = classified packets + current raw-prefetch/FIFO ownership
```

## TDD And Reachability Gate

1. RED the current product seam with eight ready TCP packets: relay passes are
   already one, but poll/flush calls remain eight. The desired assertion is
   one poll and one flush.
2. RED/GREEN a FIFO ownership test covering prefetched raw slot, staged TCP
   queue, ordering, mixed TCP/UDP/DNS, and no overwrite.
3. RED/GREEN pump tests for bounded capacity, full-wait accounting, read
   error/EOF, receiver close, and task cancellation.
4. Keep default non-H10 one-packet behavior exact.
5. Run the full real-Quinn `32 MiB` forward tracer with the frozen 500-packet
   modeled kernel ring and 500-packet pump FIFO. Require:
   - receiver strictly above `170 Mbit/s`;
   - exact `33,554,432B`, pattern, and clean EOF;
   - zero modeled kernel and userspace drops;
   - pump FIFO high water below `500`, `pump_full_waits=0`;
   - poll/flush batch equality and nonzero avoided work;
   - endpoint/D16 conservation and zero terminal ownership.
6. Run full root, harness, concurrency, UDP, vendored Quinn, check, fmt,
   runner, and diff gates.

## VPS Acceptance And Stop Rule

After local gates and code review pass, run one isolated target-only,
forward-only P1 with the exact prior frozen profile. PASS requires:

- receiver `>170 Mbit/s`, `20/20` nonzero, no tail collapse;
- TUN RX/TX drops `0/0`;
- aggregate QUIC loss `<=16 MiB`;
- pump high water below capacity and `full_waits=0`;
- exact batch poll/flush/relay attribution;
- endpoint conservation, clean pool/lifecycle, and cleanup.

If the exact local tracer is `<=170 Mbit/s`, the pump FIFO reaches capacity,
or VPS reports any TUN drop, stop this architecture. Do not change FIFO,
drain, queue, MTU, pacing, pool, QUIC, chunk, Cubic, GSO, driver, or self-wake
constants.

## Frozen Non-Goals

- no PacerCap64, bounded UDP sender, GSO-only, or pacing constant retry;
- no D16, MTU, kernel queue, pool, window, chunk, Cubic, GSO, driver-work, or
  self-wake tuning;
- no full data-plane rewrite, protocol change, or QUIC API change;
- no macOS TUN execution on the current machine;
- Gate A and Gate B remain accepted.
