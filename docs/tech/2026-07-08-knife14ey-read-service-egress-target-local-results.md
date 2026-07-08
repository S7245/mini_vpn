# Knife14ey read-service and egress-target results

Date: 2026-07-08

## Goal

Finish the next local downlink-control stage after Knife14ex without widening
ACK/window drains or tuning server/path knobs.

Knife14ex showed two remaining local symptoms:

- The active TUIC stream could stay pending while connection-level stream frames
  advanced (`pending_cause=connection_stream_frames_pending`).
- Once data moved again, local egress pressure returned and `send_queue` parked
  near the old credit edge (`send_queue_max=557386`,
  `may_recv_false=13443`, `headroom_deferred_bytes=16754655`).

## Code changes

- Split remote relay reads into a dedicated `run_relay_reader` task.
  The reader owns the `remote_reader.read(...).await` future, so writer signals,
  relay supervisor diagnostics, ACK hint ticks, and ordinary read-credit updates
  no longer rebuild the active remote read future.
- Kept bounded behavior:
  - read-credit pause still stops new reads before a read starts;
  - an already in-flight read may complete with at most its previously granted
    credit;
  - relay data still flows through the bounded global channel;
  - close/idle paths stop both reader and writer tasks with bounded timeouts.
- Moved ACK drain hint timing to the relay supervisor, so hint ticks can request
  TUN RX service without cancelling the remote read future.
- Added `tx_queue_egress_target_threshold`, halfway between clean flush and the
  old credit-spend edge. Ordinary payload drain credit and projected pressure
  debt now target this edge instead of the old credit edge.
- Kept close-drain terminal tail behavior separate: terminal close-drain can
  still use the existing credit guard path so FIN ordering remains protected.
- Clamped ACK cadence boost at/above the new egress target back to the tiny
  ACK/window floor, preventing gap hints from turning into multi-MTU payload
  staging at the local high-water edge.

## Local gates

Passed:

- `cargo test --lib relay_read_credit --quiet`
- `cargo test --lib relay_ --quiet`
- `cargo test --lib tun_rx_drain --quiet`
- `cargo test --lib downlink_ --quiet`
- `cargo test --lib --quiet` (`384 passed`)
- `cargo test --features harness --quiet` (`386 lib passed`, `10 harness passed`, `4 ignored`)
- `cargo clippy --all-targets --features harness --quiet -- -D warnings`
- `rustfmt --edition 2024 --check src/client_tun.rs src/tuic.rs`
- `cargo build --release --quiet`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`

## Remote gates

After SSH recovered, the remote focused gate passed on `.27`:

- `cargo test --lib relay_ --quiet`
- `cargo test --lib downlink_ --quiet`
- `cargo test --lib tun_rx_drain --quiet`
- `cargo test --lib relay_read_credit --quiet`
- `cargo test --lib tuic_tcp_stream_pending --quiet`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo build --release --quiet`
- `cargo clippy --all-targets --features harness --quiet -- -D warnings`

VPS service preflight was healthy:

- `.33` `sing-box`: `active`
- `.77` `iperf3`: `active`

## VPS acceptance result

Suite tag: `knife14ey_readsvc_target_p1_30`

Bundles:

- Remote: `/tmp/conn/mvpn_knife14ey_readsvc_target_p1_30_usclient_suite_20260708_013132.tar.gz`
- Local: `/tmp/mini_vpn/knife14ey_readsvc_target_p1_30/mvpn_knife14ey_readsvc_target_p1_30_usclient_suite_20260708_013132.tar.gz`

Direct and exit-to-target baselines were healthy:

- Client to target forward: sender `328 Mbit/s`, receiver `279 Mbit/s`
- Client to target reverse: sender `304 Mbit/s`, receiver `277 Mbit/s`
- Exit to target forward: sender `311 Mbit/s`, receiver `258 Mbit/s`
- Exit to target reverse: sender `314 Mbit/s`, receiver `281 Mbit/s`

The safe1200 policy was active: `dg_max=Some(1166)` and
`plpmtud(sent=0,lost=0,black_holes=0)`.

Acceptance failed:

- reverse-first P1 sender: `25.5 Mbit/s`
- reverse-first P1 receiver: `24.2 Mbit/s`
- target: stable clean reverse-first receiver `100+ Mbit/s`

Clean surfaces stayed clean:

- QUIC loss/congestion/blocking stayed zero.
- `tun_tx_dropped_delta=0`.
- `tun_flush_tx_failures=0`, `send_slice_zero=0`,
  `send_slice_errors=0`.
- `terminal_pending_reap=0`.
- `pending_at_close=0`.
- `global_rx_pressure_events=0`; relay global RX queue stayed bounded
  (`global_rx_queue_used_max=39/1024`).

Useful improvement:

- During the active transfer, ordinary egress stayed below the new target for
  much longer: at `72.3MB` delivered, `send_queue_max=447679`,
  `may_recv_false=0`, and `headroom_deferred_bytes=72259`.
- This rejects "the read-service split and egress target are no-ops"; they did
  improve the active payload loop.

Remaining failure:

- Throughput still averaged only `24.2 Mbit/s`, with bursty 100 Mbit/s-class
  intervals followed by zero intervals.
- After the iperf close tail, local TCP entered a `may_recv=false` shape while
  pending and send queue pressure returned:
  `may_recv_false=11961`, `headroom_deferred_bytes=14717970`,
  `pending_total=30813`, `send_queue_max=557386`,
  `tun_flush_deferred=4`.
- The close-drain path reintroduced the old credit edge:
  `tcp-close-drain-flush-credit ... pending=91438 send_queue=496761
  clean_high=449999 credit_high=557386 pause_high=572726`.
- The data relay still reported stream-frame pending with read gaps:
  `pending_cause=connection_stream_frames_pending`,
  `max_read_gap_ms=4542`, `max_pending_gap_ms=3404`.

## Next branch

Do not continue tuning ACK cadence, MTU/PLPMTUD, stale pool, iperf3,
sing-box, or the ordinary egress target constants for this evidence shape.

The next branch should make terminal/CloseWait drain target-aware:

- Treat `CloseWait` or `may_recv=false` with pending downlink as a terminal
  pressure mode.
- Keep QUIC receive serviced only for ACK/window drain and a tiny bounded tail
  while local egress is not progressing.
- Do not let `close_drain_terminal_pending_flush_limit` expand from the new
  target edge to the old credit edge unless measured local egress progress has
  created room.
- Add deterministic tests that a close-tail flush cannot move a flow from
  `send_queue=496761` to `557386` while pending is large and `may_recv=false`.
- Add diagnostics for close-drain target limiting and CloseWait pending
  pressure.

## Current progress

Implementation progress: 98%.

The active-transfer controller improved, but throughput acceptance is still
blocked by the terminal/CloseWait close-drain path. The next coherent stage is
Knife14ez close-tail target-aware drain.
