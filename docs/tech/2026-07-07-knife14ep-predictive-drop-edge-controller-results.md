# Knife14ep Predictive Drop-edge Controller Results

Date: 2026-07-07

## Stage Goal

Make the Knife14 local downlink controller predictive at the TUN egress drop
edge. Knife14eo reduced the passive headroom-debt spiral, but projected remote
payload credit still allowed local `send_queue + pending` pressure to cross the
drop edge before feedback became strong enough.

## Code Result

- Added a smaller ACK/window drain floor below the older pressure floor.
- Relay read credit now shrinks inside the local pressure region:
  - below the flush edge, it is capped by remaining local pressure headroom;
  - at or above the flush edge, it keeps only tiny non-paused ACK/window drain
    credit while bounded staging has room.
- The per-flow `DownlinkCreditController` can shrink read-credit ceiling and
  flush budget below the old `16 * MTU` pressure floor under repeated
  headroom/debt feedback.
- Hard relay read pause remains reserved for full bounded staging or explicit
  TUN/drop feedback pause.

## Local Gates

- `cargo test --lib relay_read_credit --quiet`
- `cargo test --lib downlink_credit_controller --quiet`
- `cargo test --lib projected_payload_pressure --quiet`
- `cargo test --lib downlink_pressure_credit --quiet`
- `cargo test --lib deferred_ack_drain --quiet`
- `cargo test --lib relay_remote_ready_burst --quiet`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib --quiet` (`370` passed)
- `cargo test --features harness --quiet` (`372` lib/harness unit tests and
  `10` integration tests passed, `4` ignored)
- `cargo clippy --all-targets --features harness --quiet -- -D warnings`
- `cargo build --release --quiet`
- `git diff --check`

## Remote Focused Gates

On `.27` after rsync with `.env`, `.git`, `target`, and tar bundles excluded:

- `cargo test --lib relay_read_credit --quiet`
- `cargo test --lib downlink_credit_controller --quiet`
- `cargo test --lib downlink_pressure_credit --quiet`
- `cargo test --lib projected_payload_pressure --quiet`
- `cargo build --release --quiet`

All passed.

## VPS Reverse-first P1 Acceptance Attempt

Run time: 2026-07-07T09:38Z UTC

- Local bundle:
  `/tmp/mini_vpn/knife14ep_predictive_drop_edge_p1_30/mvpn_knife14ep_predictive_drop_edge_p1_30_usclient_suite_20260707_173838.tar.gz`
- Remote bundle:
  `/tmp/conn/mvpn_knife14ep_predictive_drop_edge_p1_30_usclient_suite_20260707_173838.tar.gz`
- Mode: `.27 -> .33 -> .77`, `MINI_VPN_TUIC_MTU_MODE=safe1200`,
  reverse-first P1, duration `30s`, close-tail settle `12s`,
  `STOP_AFTER_REVERSE_FIRST_P1=1`.

Preflight and direct baselines were healthy:

- `.33` sing-box active and time synchronized.
- `.77` iperf3 active and time synchronized.
- `.27` suite environment ready.
- Direct `.27 -> .77` reverse baseline: receiver about `297 Mbit/s`.
- Exit `.33 -> .77` reverse baseline: receiver about `287 Mbit/s`.

Tunnel result did not pass acceptance:

- P1 sender/receiver: `30.3/28.7 Mbit/s`, below the `100+ Mbit/s` target.
- Per-second iperf profile remained bursty, with repeated zero-throughput
  intervals.
- `tun_tx_dropped_delta=0`, fixing the Knife14eo drop-edge failure.
- QUIC remained clean: loss, congestion, tx blocked, rx blocked, and PLPMTUD
  black-hole counters were all zero in the attribution summary.
- Close/lifecycle remained clean:
  `terminal_pending_reap=0`, `pending_at_close=0`, `egress_at_close=0`, and
  `terminal_late_remote_payload=0`.
- Remaining local pressure signals:
  `send_queue_max=557240`, `may_recv_false=8335`,
  `headroom_deferred_bytes=20226670`, `pressure_credit_blocked_bytes=874739`,
  `tun_flush_deferred=87`, `pending_max=85438`.
- The new sub-floor credit path was active:
  `read_credit_limit_bytes_min` reached about `1111` bytes on the data stream.
- Data stream cadence still stalled:
  `data_read_gap_max_ms=3470`, `data_pending_gap_max_ms=3469`.

## Interpretation

Knife14ep fixed the immediate local TUN drop edge, but it over-throttled the
receive side. A fixed one-packet ACK/window floor avoids egress drops, yet it
does not deliver enough continuous remote read cadence to keep the reverse TCP
window healthy. The result is a clean but too-narrow local controller: no TUN
drops, no QUIC blocking, no close/reap problem, but still bursty low-average
throughput.

This run keeps the main root local:

- Server path is still healthy via direct baselines.
- QUIC MTU/PLPMTUD is still rejected for this branch because safe1200 was
  active and QUIC loss/congestion/blocking remained zero.
- Lifecycle/terminal pending is still clean.
- The remaining branch is controller cadence: read credit and flush budget must
  grow from observed clean egress progress faster than a static one-packet floor
  allows, while still shrinking rapidly near high water.

## Next Modification Plan

The next Knife14 controller stage should not shrink credit further. It should
keep the predictive drop-edge cap, but make the ACK/window drain floor adaptive:

1. Start at the tiny floor only when local pressure is at the flush edge and no
   recent egress progress is observed.
2. Grow the non-paused drain floor to a bounded multi-MTU batch when
   `tun_tx_dropped_delta=0`, `pending` is below the staging high watermark, and
   `send_queue` is draining.
3. Avoid repeated `paused=true` updates while QUIC remains clean; prefer a
   bounded non-paused drain credit unless staging is full or TUN feedback is
   paused.
4. Add focused TDD for "no-drop egress progress grows the sub-floor drain
   cadence" and "near high water still clamps before local pending expands".
5. Rerun the same scoped safe1200 reverse-first P1 after local and `.27`
   focused gates pass.

## Current Position

Knife14ep is a coherent 97% follow-up: design/spec, TDD, code, local gates,
remote focused gates, one scoped VPS run, and bundle parsing are complete. VPS
acceptance is still not met.
