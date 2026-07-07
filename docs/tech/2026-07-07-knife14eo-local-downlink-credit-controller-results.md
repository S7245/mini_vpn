# Knife14eo Local Downlink Credit Controller Results

Date: 2026-07-07

## Stage Goal

Move the remaining Knife14 downlink work from passive headroom accounting to a
per-flow local credit controller. The controller couples relay read credit,
flush budget, bounded staging, and observed smoltcp egress progress.

## Code Result

- Added `DownlinkCreditController` to each `SocketCtx`.
- Headroom deferral now feeds hard local feedback:
  - repeated deferral without egress progress shrinks relay read credit;
  - the next flush budget is compressed to the pressure floor;
  - bounded staging prevents continued pending growth.
- Observed egress progress grows read credit additively and reopens the local
  flush budget, while remaining bounded by staging headroom.
- Relay read hard pause is still reserved for receive-window high water or TUN
  feedback pause, preserving the Knife14dz lesson that local pressure debt must
  not starve QUIC stream receive progress.

## Local Gates

- `cargo test --lib downlink_credit_controller --quiet`
- `cargo test --lib relay_read_credit --quiet`
- `cargo test --lib downlink_pressure_credit --quiet`
- `cargo test --lib deferred_ack_drain --quiet`
- `cargo test --lib relay_remote_ready_burst --quiet`
- `cargo test --lib --quiet` (`369` passed)
- `cargo test --features harness --quiet` (`371` lib/harness unit tests and
  `10` integration tests passed, `4` ignored)
- `cargo clippy --all-targets --features harness --quiet -- -D warnings`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo build --release --quiet`
- `git diff --check`

## Remote Focused Gates

On `.27` after rsync with `.env`, `.git`, and `target` excluded, and after
touching edited source files to avoid stale cargo artifacts:

- `cargo test --lib downlink_credit_controller --quiet`
- `cargo test --lib relay_read_credit --quiet`
- `cargo test --lib downlink_pressure_credit --quiet`
- `cargo build --release --quiet`

All passed.

## VPS Reverse-first P1 Acceptance Attempt

Run time: 2026-07-07T09:16Z UTC

- Local bundle:
  `/tmp/mini_vpn/knife14eo_credit_controller_p1_30/mvpn_knife14eo_credit_controller_p1_30_usclient_suite_20260707_171633.tar.gz`
- Remote bundle:
  `/tmp/conn/mvpn_knife14eo_credit_controller_p1_30_usclient_suite_20260707_171633.tar.gz`
- Mode: `.27 -> .33 -> .77`, `MINI_VPN_TUIC_MTU_MODE=safe1200`,
  reverse-first P1, duration `30s`, close-tail settle `12s`,
  `STOP_AFTER_REVERSE_FIRST_P1=1`.

Preflight was healthy:

- `.33` sing-box active and time synchronized.
- `.77` iperf3 active and time synchronized.
- `.27` release binary and suite script present.
- Direct `.27 -> .77` reverse baseline: receiver about `278 Mbit/s`.
- Exit `.33 -> .77` reverse baseline: receiver about `291 Mbit/s`.

Tunnel result did not pass acceptance:

- P1 sender/receiver: `32.3/30.8 Mbit/s`, still far below the `100+ Mbit/s`
  target.
- `tun_tx_dropped_delta=117`, violating the clean-window requirement.
- QUIC remained clean: `lost_bytes_delta=0`, `congestion_events_delta=0`,
  `tx_blocked=0`, `rx_blocked=0`, and `plpmtud(sent=0,lost=0,black_holes=0)`.
- Close/lifecycle remained clean:
  `terminal_pending_reap=0`, `pending_at_close=0`, `egress_at_close=0`,
  `terminal_late_remote_payload=0`.
- Controller pressure improved versus Knife14en but still over-shot:
  `may_recv_false=2129` versus `14700`, `headroom_deferred_bytes=50881438`
  versus `4018847472`, `pending_high=576836`, `pending_total=97919`,
  `send_queue_max=553848`, `pressure_credit_blocked_bytes=1002263`.
- The data stream still showed read stalls:
  `data_read_gap_max_ms=4655`, `data_pending_gap_max_ms=4654`.

## Interpretation

The 95% controller changed the failure mode in the intended direction, but it
is still too permissive around the kernel egress edge:

- The previous huge passive headroom-debt spiral was largely reduced.
- QUIC transport and server path remain exonerated in this window.
- The remaining failure is a local overrun: projected payload debt lets
  `send_queue + pending` climb past the egress pause edge before the controller
  clamps hard enough, causing a TUN TX drop and bursty throughput.

The next code change should not tune QUIC, PLPMTUD, sing-box, iperf3, or the
stale TCP pool. It should make the local controller drop-edge predictive:

- cap projected payload credit before `tx_queue_pause_high`, not after;
- let drop/pressure debt immediately shrink the per-flow ceiling and flush
  budget below the current floor for one control epoch;
- preserve bounded ACK/window drain so QUIC receive progress is not fully
  starved;
- add a focused test for "projected payload over pause edge does not accept
  another local pending burst".

## Current Position

This takes Knife14eo to the requested 97% stop point: code, deterministic
coverage, local gates, remote focused gates, one scoped VPS reverse-first P1
run, and bundle parsing are complete. The acceptance target is not met.

## Next Acceptance

After the predictive drop-edge controller change, rerun one `.27 -> .33 -> .77`
reverse-first P1 suite before declaring the branch complete. The acceptance
target remains:

- receiver `100+ Mbit/s`;
- `may_recv_false` and `headroom_deferred_bytes` significantly lower than
  Knife14en;
- `pending_at_close=0`;
- `terminal_pending_reap=0`;
- `tun_tx_dropped_delta=0`;
- QUIC loss/congestion/blocking remain zero.
