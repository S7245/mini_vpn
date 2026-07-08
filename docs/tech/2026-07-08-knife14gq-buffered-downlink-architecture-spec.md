# 2026-07-08 Knife14gq Buffered Downlink Architecture Spec

## B0 Evidence Freeze

Old-path baseline:

- Knife14gp commit `bd0d264` completed focused safe1200 reverse-first P1 with
  healthy direct baselines but reached only `15.0/13.5 Mbit/s`.
- The same `.27/.33/.77` topology reached `173/173 Mbit/s` with a mature
  sing-box client.
- Knife14gp surfaces were clean for TUN drops, send errors, close-tail
  accounting, QUIC loss, and QUIC flow-control blocking.
- The remaining failure signal was local read-service collapse:
  `remote_read_service_len_min=1200`,
  `remote_batch_limit_bytes_min=1200`, and repeated stale pending windows with
  `conn_rx_stream_frames_since_pending=0`.

Rejected branches for this stage:

- VPS retune, iperf3, MTU/PLPMTUD, stale pool, broad QUIC windows, and more
  self-wake timer work.
- Another pressure-credit constant patch without a new failure attribution
  surface.

## B1 Architecture

The repository already has a per-flow downlink buffer:
`SocketCtx.downlink_pending`. The failure is not that mini_vpn has no buffer;
the failure is that remote TUIC read credit is still tightly coupled to
smoltcp's instantaneous `send_queue`, `headroom`, and projected pressure.

Knife14gq promotes the existing `downlink_pending` into the explicit buffered
downlink layer:

```text
TUIC stream reader
  -> RelayEvent::Data
  -> SocketCtx.downlink_pending as per-flow bounded downlink buffer
  -> flush_downlink local egress writer
  -> smoltcp/TUN
```

The new controller is feature flagged. The old path remains available for A/B.

```text
MINI_VPN_BUFFERED_DOWNLINK=1
```

## Watermarks

Per-flow watermarks:

- `low`: resume remote reads after buffered bytes drain below this level.
- `high`: soft pressure; keep reads bounded but do not collapse to MTU-sized
  service merely because smoltcp has transient headroom pressure.
- `hard`: pause remote reads for that flow.

Global watermarks:

- global pending is the sum of all per-flow `downlink_pending` bytes.
- global high/hard protect high-concurrency memory use.
- per-flow hard or global hard is the only normal reason to pause the TUIC
  reader in buffered mode, aside from terminal no-recv and explicit hard
  drop/error feedback.

## Invariants

- Feature flag off keeps the old read-credit path.
- Fully accepted egress progress must not be converted into immediate read
  collapse.
- Buffered mode must still stop remote reads on:
  - terminal no-recv local TCP state;
  - per-flow hard watermark;
  - global hard watermark;
  - active TUN/drop hard feedback.
- Close-tail accounting remains unchanged: buffered bytes must either drain or
  be reported through existing terminal pending accounting.
- No secret material is written to docs, logs, or tests.

## Required Diagnostics

Buffered mode must expose enough counters to explain a failed B7 run:

- mode enabled/disabled;
- per-flow pending current/high-water;
- global pending current/high-water;
- read credit selected by the buffered controller;
- pause/resume reason;
- hard watermark hits;
- bytes accepted by the local egress writer.

If B7 fails below `30 Mbit/s` and these diagnostics cannot explain whether the
reader, buffer, writer, local TCP/TUN, or remote stream is the bottleneck, the
stage fails and stops.

## B7 Acceptance

Focused gate:

- safe1200 reverse-first P1, 30 seconds, single stream;
- receiver `>30 Mbit/s`;
- direct baseline healthy;
- TUN drops `0`;
- send errors `0`;
- close-tail clean or explicitly accounted;
- QUIC loss/blocking not the primary cause;
- buffered diagnostics show the TUIC reader is not being shrunk to `1200B`
  solely by transient smoltcp headroom pressure.

If B7 does not exceed `30 Mbit/s`, do not continue to B8/B9. Stop, record the
failure attribution, and reassess whether this architecture direction is valid.
