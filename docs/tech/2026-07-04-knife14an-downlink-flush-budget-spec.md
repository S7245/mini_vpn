# Knife14an spec - bounded downlink flush budget

## Grounding

- Current branch head before this stage: `4da93e2`.
- Knife14al same-window evidence pinned the current limiter to client-local
  downlink/TUN egress pressure:
  `/tmp/mini_vpn/mvpn_knife14al_samewindow_defaultqlen2_usclient_suite_20260704_133130.tar.gz`.
- Clean reverse-first tunnel P1 was 13.7/12.4 Mbit/s with
  `local_tun_egress_drop+local_downlink_backpressure`, while direct paths and
  sing-box service state were healthy.
- The suite runs with 1MiB smoltcp TCP tx buffers. Current `flush_downlink`
  gives the whole `downlink_pending` slice to `TcpSocket::send_slice`; smoltcp
  can therefore accept a large burst in one loop pass and `device.flush_tx()`
  can push hundreds of packets into the local TUN/qdisc at once.
- With MTU 1200 and default TUN qlen 500, a 1MiB flush can exceed the local
  queue in a single burst. That matches the observed `tun_tx_dropped_delta`
  branch better than another sing-box or iperf branch.

## Problem

Downlink backpressure currently reacts after per-handle pending reaches the
high watermark. It does not bound the local egress burst created by one
`flush_downlink -> iface.poll -> device.flush_tx` cycle. Large socket tx buffers
improved forward throughput, but they also allow reverse/downlink bursts that
can overrun the local TUN qdisc.

## Grill / Design Tree

- Branch A: Increase TUN `txqueuelen`.
  - Rejected as the product fix. Knife14ak showed it is only an A/B knob and
    can move the bottleneck without fixing pacing.
- Branch B: Lower downlink backpressure high/low watermarks.
  - Useful later, but it still permits one large `send_slice`/`flush_tx` burst
    when pending is below the high watermark.
- Branch C: Bound bytes accepted into smoltcp per downlink flush pass.
  - Chosen. It directly limits local egress burst size while preserving pending
    bytes for later flushes.
- Branch D: Sleep in the hot path between TUN writes.
  - Rejected. Sleeping inside the main loop risks hurting unrelated flows and
    is harder to test deterministically.
- Branch E: Redesign global_rx per-handle scheduling.
  - Likely needed later, but larger than this stage. Start with the smaller
    burst bound first.

## Goal

Introduce a configurable per-flush budget for TCP downlink delivery:

- default: 256 KiB per `flush_downlink` call;
- env override: `MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES`;
- valid range: 4 KiB through 16 MiB;
- `flush_downlink` sends at most this many pending bytes into smoltcp in one
  call and leaves the rest in `downlink_pending` for later dirty/timer passes;
- diagnostic startup logs and the US-client suite report show the active value.

The default is intentionally below the packet count implied by 1MiB/MTU1200
bursts, while still high enough that a 5ms timer can theoretically sustain
hundreds of Mbit/s if the local path can absorb it.

## Non-Goals

- Do not change TUIC framing, TCP pool selection, QUIC congestion control, or
  sing-box configuration.
- Do not change downlink backpressure high/low defaults in this stage.
- Do not drop pending bytes to satisfy the budget.
- Do not add sleeps or wall-clock pacing inside the main loop.
- Do not claim final throughput acceptance until a VPS run verifies it.

## Invariants

- `downlink_pending` preserves any bytes not accepted this pass.
- `TcpDownlinkDiag` still counts accepted bytes and pending high-water.
- Dirty handles remain dirty while pending bytes exist, so later timer/relay
  passes continue flushing.
- Setting `MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES=1048576` approximates the previous
  1MiB socket-buffer burst behavior for A/B testing.

## Acceptance

- Red tests cover the flush budget helper and env parser before implementation.
- `cargo test --lib client_tun` passes.
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh` passes.
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test` passes.
- `git diff --check` passes.
- Next VPS run should compare default 256KiB budget against the previous
  behavior using the same-window reverse-first suite.
