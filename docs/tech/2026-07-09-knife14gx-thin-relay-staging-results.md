# Knife14gx Thin Relay Staging Results

Date: 2026-07-09

## Scope

Knife14gx tested the first architecture-slice attempt after Knife14gw:

- parse fresh/stale TUIC stream pending causes in the suite summary;
- add a feature-gated thin TCP relay staging path with
  `MINI_VPN_THIN_TCP_RELAY=1`;
- keep the default legacy relay path unchanged;
- run only the VPS first threshold gate and stop.

Non-goals: no VPS tuning, no iperf3 changes, no MTU/PLPMTUD work, no stale-pool
work, and no broad QUIC window changes.

## Commits

- `f0944e8` - `test(knife14): parse fresh stale TUIC pending causes`
- `1bda549` - `feat(knife14): add thin TCP relay staging gate`

## Local Gates

- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test client_tun::tests::thin_relay_reader_stages_payloads_without_global_rx_admission`
- `cargo test client_tun::tests::relay_ -- --nocapture`
- `cargo test --lib` (`433 passed`)

## VPS Gate

Remote worktree:

- Client `.27`: `/tmp/mini_vpn_knife14_thin.EAVaDz`
- HEAD: `1bda5497`
- Bundle: `/tmp/mini_vpn_knife14_thin_gate/mvpn_knife14thin_g1_usclient_suite_20260709_112459.tar.gz`
- Local copy: `/tmp/mini_vpn_knife14_thin_gate_local/extract/`

Run shape:

- `MINI_VPN_THIN_TCP_RELAY=1`
- `RUN_REVERSE_FIRST_P1=1`
- `STOP_AFTER_REVERSE_FIRST_P1=1`
- `BUILD_RELEASE=0`
- `TARGET=43.130.32.77`
- `SERVER_EVIDENCE_CHECK=1`

Preflight:

- `.33` sing-box active.
- `.77` iperf3 active.
- Client to target direct reverse baseline: `274 Mbit/s` receiver.
- Exit to target reverse baseline: `299 Mbit/s` receiver.
- Startup log confirmed `thin TCP relay staging: enabled capacity=64 messages`.
- Runtime log confirmed `tcp-relay-engine ... engine=thin_staging`.

## Result

The VPS first threshold failed.

- iperf reverse P1: `17.6 Mbit/s` sender, `15.2 Mbit/s` receiver.
- The run remained bursty with many `0.00 bits/sec` one-second buckets.
- It did not exceed the `30 Mbit/s` first threshold, so no further stages were run.

Key parsed signals:

- `global_rx_pressure: events=0 max_wait_ms=0.000`
- `tun_tx_dropped_delta=0`
- QUIC loss/congestion/blocking deltas: `0`
- `downlink_flush accepted_bytes=57696520`
- `send_queue_max=557386`
- `may_recv_false=4649`
- `headroom_limited=4609`
- `hard_edge_guard_limited=4609`
- `relay_remote_timing data_max_read_gap_ms=3417`
- `tuic_tcp_stream data_read_gap_max_ms=4038`
- `tuic_stream_pending data_pending_gap_max_ms=4006`
- pending causes:
  - `connection_fresh_stream_frames_pending=7`
  - `connection_stale_stream_frames_pending=10`
  - `connection_rx_no_stream_frames=3`
- attribution: `local_pressure_credit`

## Conclusion

Thin relay staging did take effect, but it did not restore data-moving cadence
above `30 Mbit/s`. The failed threshold is not explained by VPS service health,
direct path capacity, TUN drops, QUIC loss/congestion/blocking, or `global_rx`
channel pressure.

The active evidence is still the ordered TUIC stream service / local admission
contract: remote stream frames arrive, but useful reads still have multi-second
gaps while local pressure-credit/headroom guards repeatedly constrain egress.
Therefore this architecture slice is insufficient for `100+ Mbit/s` and should
not be extended blindly as-is.

Next design should target the direct coupling between ordered TUIC stream
polling and local pressure-credit/headroom feedback, with a code-level gate that
requires sustained remote-read cadence plus local admission progress before the
next VPS run.
