# Knife14am spec - inherited QUIC congestion attribution

## Grounding

- Current branch head before this stage: `261efa0`.
- Relevant same-window bundle:
  `/tmp/mini_vpn/mvpn_knife14al_samewindow_defaultqlen2_usclient_suite_20260704_133130.tar.gz`.
- Reverse-first P1 was the clean reverse signal: tunnel reverse collapsed to
  13.7 Mbit/s sender and 12.4 Mbit/s receiver while direct client-target and
  exit-target reverse paths stayed around the high-200 Mbit/s range.
- Reverse-first attribution was useful:
  `local_tun_egress_drop+local_downlink_backpressure`.
- The standard forward P1 then reached 204/191 Mbit/s but created 761 local
  write-pressure events and large QUIC loss/congestion deltas.
- The standard reverse P1 after that forward burst ran at 11.5/10.7 Mbit/s and
  reported `attribution: no_pressure_signal` because the probe only considered
  per-window QUIC deltas. Its first QUIC sample was already damaged:
  `cwnd=5808`, `lost_bytes=500976100`, `congestion_events=93571`.

## Problem

`scripts/knife14b-lowrtt-probe.sh` correctly labels new QUIC loss/congestion
inside an iperf window, but it misses inherited connection state from a prior
probe. This can make a post-forward reverse probe look pressure-free even when
the TUIC connection starts the window with an already-collapsed cwnd and large
historical loss/congestion counters.

## Grill / Design Tree

- Branch A: Treat every non-zero historical loss as congestion.
  - Rejected. Long-lived connections can carry small historical counters that
    are not the current bottleneck.
- Branch B: Label inherited congestion when the first sample has both low cwnd
  and large historical loss or congestion counters.
  - Chosen. It matches Knife14al and stays conservative.
- Branch C: Reset the tunnel or TUIC connection before every probe.
  - Useful for future acceptance design, but not a parser fix and too invasive
    for this stage.
- Branch D: Tune downlink watermarks first.
  - Rejected for this stage. The attribution gap should be fixed before product
    pacing changes so the next VPS run is easier to read.

## Goal

Teach the low-RTT attribution summary to emit `inherited_quic_congestion` when a
probe window starts with an already-bad QUIC connection state:

- first observed cwnd is low;
- first observed `lost_bytes` or `congestion_events` is already large;
- no new per-window loss/congestion delta is required.

Also print enough absolute-start fields in the `quic:` line to explain the
label.

## Non-Goals

- Do not change Rust data-plane behavior.
- Do not change TUIC congestion control, flow-control windows, socket buffer
  defaults, TUN queue length, or downlink backpressure watermarks.
- Do not run a full VPS suite for this parser-only stage.
- Do not change iperf3 or sing-box configuration.

## Invariants

- Existing attribution labels remain additive and conservative.
- `reverse_sender_backpressured` must not be emitted when inherited QUIC
  congestion already explains the low reverse sender/receiver rates.
- Existing `--self-test` coverage keeps passing.
- The parent suite summary continues to surface the relevant attribution lines.

## Acceptance

- A new embedded self-test sample reproduces the Knife14al post-forward reverse
  shape and fails before the implementation.
- `bash -n scripts/knife14b-lowrtt-probe.sh` passes.
- `bash scripts/knife14b-lowrtt-probe.sh --self-test` passes on macOS.
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh` passes.
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test` passes.
- `git diff --check` passes.
