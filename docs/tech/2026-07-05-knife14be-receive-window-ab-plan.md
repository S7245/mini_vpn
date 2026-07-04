# Knife14be plan - receive-window A/B

Date: 2026-07-05

## Design tree

1. Keep patching close-drain.
   Rejected for this slice. Knife14bd made terminal pending visible and showed
   the useful receiver path is already constrained earlier by tx-queue pressure.
   More close-drain changes would not explain the burst/idle pattern before
   local close.

2. Increase TUN queue length or tune egress pacing.
   Rejected. The clean reverse-first window had `tun_tx_dropped_delta=0`,
   `tun_flush_failures=0`, and `tun_flush_deferred=0`.

3. Revisit TUIC pool, sing-box, or iperf3.
   Rejected. Stale pool is closed, service preflight is healthy, and the clean
   window has no QUIC congestion/loss signal.

4. Run a tx-buffer receive-window A/B.
   Selected. It directly tests whether the 1MiB local TCP tx buffer is the
   current window cap. Existing code already supports the override and adaptive
   backpressure should scale automatically.

5. Instrument local TCP/TUN drain cadence.
   Deferred until the A/B result. If 4MiB does not improve useful throughput,
   this becomes the next root.

## Tasks

1. Commit this spec/plan.
2. Sync `.27` to the branch head.
3. Run suite self-test on `.27`.
4. Preflight `.33` sing-box and `.77` iperf3 service health.
5. Run the clean reverse-first A/B from `.27` with:
   - `SUITE_TAG=knife14be_tx4m_auto`;
   - `MINI_VPN_TCP_RX_BUFFER_BYTES=1048576`;
   - `MINI_VPN_TCP_TX_BUFFER_BYTES=4194304`;
   - `MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=`;
   - `MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=`;
   - `RUN_REVERSE_FIRST_P1=1`;
   - existing service preflight checks enabled.
6. Pull the bundle locally and parse the clean reverse-first report first.
7. Record results and learning/error memory.
8. Commit and push result docs.

## Test shape

This stage is intentionally a VPS acceptance test rather than a Rust behavior
patch. The TDD-style red/green condition is external and observable:

- RED baseline: Knife14bd clean reverse-first receiver `26.2 Mbit/s` with
  `tx=1MiB` and auto high `1MiB`.
- GREEN candidate: Knife14be clean reverse-first receiver materially improves
  with `tx=4MiB` and auto high `4MiB`, while clean loss/drop/error signals stay
  quiet.

## Risk checks

- A 4MiB tx buffer increases per-flow memory. This run is `P1` only and does
  not establish a high-concurrency product default.
- If the run shows TUN drops or QUIC congestion, do not accept a throughput
  increase as proof of a good fix.
- If the run times out or sudo/TTY fails, discard the bundle and rerun cleanly
  with a true TTY from the beginning.
