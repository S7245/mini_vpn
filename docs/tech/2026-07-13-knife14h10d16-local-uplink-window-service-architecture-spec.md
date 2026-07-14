# Knife14h10d16 Local Uplink Window Service Architecture Spec

Date: 2026-07-13
Status: **APPROVED FOR LOCAL TDD IMPLEMENTATION**

Source evidence:

- `docs/tech/2026-07-13-knife14h10d16-endpoint-pacing-service-vps-results.md`
- `docs/tech/2026-07-13-knife14h10d16-tun-rx-batch-service-vps-results.md`
- `docs/tech/2026-07-13-knife14h10d16-tun-ingress-service-vps-results.md`
- `docs/tech/2026-07-13-knife14h10d16-endpoint-pacing-service-architecture-spec.md`

## Stage Goal

Bound the local TCP sender credit that can be released toward `tun0` in one
receive-window epoch, while preserving the configured `1 MiB` smoltcp receive
storage, H10d16 byte ownership, EndpointWindowV1, and all frozen transport and
queue parameters.

The candidate is intended to be sufficient for one new forward P1 with:

- receiver throughput strictly above `170 Mbit/s`;
- zero TUN RX/TX drops;
- TUN ingress-pump high-water below `500/500` and zero full waits;
- aggregate QUIC lost bytes no greater than `16 MiB`;
- exact endpoint conservation and clean relay lifecycle.

This stage does not change endpoint pacing. It closes the previously unowned
credit boundary between the local Linux TCP sender and the kernel TUN qdisc.

## Frozen Inputs And Non-Goals

Do not change:

- H10d16 queues, global budget, actor quantum, phases, ownership, or lifecycle;
- TUN MTU `1200`, txqueuelen `500`, or the mirrored ingress FIFO `500`;
- the configured `1 MiB` smoltcp TCP RX/TX storage buffers;
- TUIC pool `2`, QUIC windows, `64 KiB` application chunk, Cubic, GSO enabled,
  Quinn UDP sender, driver work bound `20`, or D3 self-wake;
- EndpointWindowV1 `30,720,000 wire B/s`, `61,440B` burst, `10,240B` control
  reserve, `20,480B` quantum, DRR, reservation, settlement, or timer rules;
- the existing `48` normal / `240` pressure TUN batch limits;
- DNS/UDP bypass, fake-IP, failover, server configuration, or protocol targets.

Do not reopen bounded sender, PacerCap64, GSO-only, queue enlargement, drain
budget tuning, TCP-buffer tuning, or parameter sweeps. The new local window is
derived from the already accepted endpoint theorem, not selected by a sweep.

## Failure Evidence And Ranked Design Tree

Commit `20a0f8c` completed a frozen P1 at `194 Mbit/s` receiver, but failed
with `419` TUN TX drops, pump high-water `500/500`, and `347` full waits. The
full waits were all present in the first metrics window and did not increase
afterward. The main loop remained only `4.8-17.8%` active.

The decisive socket trace repeatedly alternated between:

```text
recv_queue ~= 278 KiB -> 0
recv_queue = 1,048,576B -> 0
```

`extract_socket_payload` dequeues the largest contiguous slice, while
`pump_established_uplink` is bounded by `64` messages rather than bytes. A
single relay pass can therefore consume the complete `1 MiB` receive buffer.
smoltcp then advertises the newly free buffer as TCP window credit. The local
Linux sender can turn that credit into a sub-scheduler-quantum TUN burst even
though the downstream endpoint wire service is correctly bounded.

### H1 — Selected: storage capacity is incorrectly also sender credit

Prediction: preserving `1 MiB` storage while independently limiting both the
advertised window and receive acceptability edge prevents the `1 MiB -> 0`
credit release. The P1 ingress FIFO remains below capacity without changing
the FIFO, qdisc, or drain service.

### H2 — Rejected: another actor scheduling optimization

Batch relay reduced relay time below `3%`; the reader pump and batched
poll/flush kept the loop mostly parked. More actor priority or another batch
shape cannot formally limit bytes already authorized by the TCP window.

### H3 — Rejected: cap only `extract_socket_payload`

Limiting one dequeue does not cap the receive buffer's already-free space.
With `1 MiB` storage, an ACK can still advertise far more than the intended
credit. A dequeue-only cap is necessary neither for the initial window nor
sufficient for subsequent window updates.

### H4 — Rejected: enlarge the qdisc/FIFO or alter pacing

The ingress architecture already filled both existing `500`-packet layers.
Increasing capacity hides the unowned credit. Endpoint pacing conservation,
capacity, and QUIC-loss gates already passed and are downstream of the drop.

## Proposed Architecture

### 1. Separate smoltcp Storage From Advertised Credit

Vendor the pinned `smoltcp 0.10.0` source and add one optional TCP socket
receive-window limit. Default sockets retain byte-for-byte upstream behavior.
When a limit is installed before listen/connect:

```text
max_receive_extent = min(rx_buffer.capacity, configured_window_limit)
admissible_window = max_receive_extent - rx_buffer.len
advertised_window = admissible_window >> negotiated_window_scale
receive_window_end = application_consumed_seq + max_receive_extent
```

The same limit must govern SYN window advertisement, later ACK/window updates,
and segment acceptability. Capping only the emitted header without capping the
acceptance edge would violate TCP ownership and is forbidden.

Reset/relisten preserves the configured limit. Runtime changes on an active
connection are outside this stage; mini_vpn installs the value before listen.

### 2. Derive The H10d16 Limit From EndpointWindowV1

The fixed local credit uses the numeric value of the accepted endpoint
service's formal ten-millisecond wire bound:

```text
local_uplink_window = burst + rate * 10ms
                    = 61,440 + 30,720,000 / 100
                    = 368,640B
```

H10d16 listener sockets use `368,640B`; their storage remains `1,048,576B`.
Default/non-H10 sockets remain unlimited beyond their physical buffer, exactly
as upstream smoltcp behaves today. There is no environment override.

The endpoint theorem counts QUIC wire bytes, while TCP flow-control credit
counts TCP payload bytes. They are not claimed to be unit-equivalent. The
local safety discriminator below converts the TCP credit to MTU-sized packets:
`ceil(368,640 / 1,160) = 318 < 500`. Endpoint pacing continues to own the
separate wire-byte theorem.

### 3. Preserve Existing Backpressure And Ownership

The actor continues to reserve a relay-channel slot before dequeuing smoltcp
bytes. A full relay channel leaves bytes in smoltcp. The writer, Quinn stream,
EndpointPacingService reservations, socket outstanding-byte settlement, and
the core endpoint invariant remain unchanged:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= burst_bytes
```

No new queue, task, timer, wake source, or payload copy is introduced.

## End-To-End Hot Path After The Change

```text
Linux TCP sender
  <- smoltcp ACK + advertised window <= 368,640B
  -> tun0 qdisc (500, MTU 1200)
  -> H10d16 TUN reader pump (500)
  -> bounded 48/240 packet staging
  -> one Interface::poll / flush / dirty relay per TCP batch
  -> smoltcp receive acceptance edge <= next_seq + 368,640B
  -> nonblocking RelayCommand reservation + payload dequeue
  -> relay writer, 64 KiB application chunks
  -> Quinn stream / frozen QUIC windows
  -> EndpointPacingService reserve / settle
  -> UDP socket / sing-box / target
```

All previous paths remain active. This is a local admission repair, not a
replacement for D16 queues, the TUN ingress service, or endpoint pacing.

## Invariants

For each limited socket:

```text
0 < advertised_or_admissible_window <= 368,640B
recv_queue <= 368,640B for an in-order conforming peer
physical_rx_capacity = 1,048,576B
```

More exactly, when `recv_queue = q`:

```text
advertised_free = 368,640 - q
acceptance_end = application_consumed_seq + 368,640
```

The implementation must also preserve:

- TCP sequence/ACK/window-scale correctness across SYN, data, zero-window, and
  relisten/reset;
- exactly-once TUN packet ownership and TCP ordering;
- relay-channel permit-before-dequeue backpressure;
- DNS/UDP bypass and non-H10 behavior;
- no hot-path panic or unchecked new `unwrap`/`expect`.

## Capacity And Burst Math

At MTU `1200`, a full IPv4/TCP payload is about `1160B`:

```text
ceil(368,640 / 1160) = 318 full-payload packets
318 < frozen tun0 txqueuelen 500
```

This leaves roughly `182` qdisc slots for ACK/control and scheduling overlap
before the userspace FIFO is considered. The bound is a bulk-P1 discriminator,
not a universal small-packet packet-rate theorem.

The local capacity floor using only the existing `5ms` maintenance cadence is:

```text
368,640B / 5ms * 8 = 589.824 Mbit/s
```

This is above both the `>170 Mbit/s` gate and EndpointWindowV1's approximately
`239.167 Mbit/s` application capacity. Event-driven TUN service is faster than
that floor. The change is therefore plausibly sufficient without reducing the
accepted wire capacity.

## Necessary Versus Sufficient

An independent advertised/acceptance window is necessary because every
downstream service starts after the local TCP sender has already received
credit. The fixed `368,640B` candidate is intended to be sufficient for the
next P1 because it is below the observed full-payload qdisc envelope while
retaining more than twice the target service capacity at the slow timer edge.

VPS acceptance is still required for Linux TCP/TUN/Tokio scheduling. Local
tests prove protocol bounds and reachability, not the real qdisc result.

## TDD And Failure Discriminators

Expected RED tests:

1. `1 MiB` storage with a `368,640B` limit must advertise the limited SYN and
   established windows while retaining `1 MiB` physical capacity.
2. A segment beyond the limited acceptance edge must not enter the receive
   buffer; a segment at the edge must be accepted.
3. Dequeue/window-update and reset/relisten must preserve the limit.
4. Default sockets must retain upstream full-buffer behavior.
5. The mini_vpn H10 config must build listener sockets with storage
   `1,048,576B` and limit `368,640B`; non-H10 remains unlimited.

Required local full-path gate:

- exact `32 MiB`, zero pattern/lifecycle/ownership errors;
- throughput strictly above `170 Mbit/s`;
- ring and pump high-water below `500`, pump full waits zero;
- observed receive-queue high-water no greater than `368,640B`.

Unexpected repair/regression failures require causal analysis before another
code change. If the exact local `32 MiB` gate is `<=170 Mbit/s`, this
architecture fails; do not tune the limit, buffers, batch, qdisc, or endpoint
constants.

One frozen VPS P1 is allowed only after focused tests, root gates, explicit
vendored-smoltcp tests, and code review pass. It must stop on `<=170 Mbit/s`,
any TUN drop, pump capacity/full-wait, excessive QUIC loss, conservation leak,
or lifecycle regression.
