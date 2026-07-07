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

## Current Position

This takes Knife14eo to the requested 95% stop point: code, deterministic
coverage, local gates, and remote focused gates are green. The remaining 5% is
the expensive VPS reverse-first acceptance run and bundle parsing.

## Next Acceptance

Run one `.27 -> .33 -> .77` reverse-first P1 suite before declaring the branch
complete. The acceptance target remains:

- receiver `100+ Mbit/s`;
- `may_recv_false` and `headroom_deferred_bytes` significantly lower than
  Knife14en;
- `pending_at_close=0`;
- `terminal_pending_reap=0`;
- `tun_tx_dropped_delta=0`;
- QUIC loss/congestion/blocking remain zero.
