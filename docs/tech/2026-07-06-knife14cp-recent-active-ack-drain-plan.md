# Knife14cp Recent-Active ACK Drain Plan

Date: 2026-07-06

## Stage Plan

1. Ground Knife14co results and isolate the gap between payload-triggered drain
   and post-burst idle.
2. Add RED tests for a recent-active timer ACK drain budget and diagnostic
   source accounting.
3. Add a small recent-active window constant and helper.
4. Track a global recent-active deadline in the event loop when remote payload
   work is accepted or pending.
5. On timer ticks, run pressure maintenance drain first; if pressure is not
   active but the recent-active deadline is live, run a small active-flow drain.
6. Keep the helper and runtime off when the recent-active window expires.
7. Run focused local tests, broad gates, then a scoped VPS reverse-first P1.
8. Record results and update learning/error memory.

## Patch Shape

- Add `TUN_RX_ACTIVE_FLOW_TIMER_DRAIN_MS`.
- Add `TUN_RX_DRAIN_SOURCE_TIMER_ACTIVE_FLOW`.
- Add `tun_rx_drain_budget_for_recent_active_flow(...)`.
- Add `timer_active_flow_attempts` to `TunRxDrainDiag`.
- Add `active_flow_tun_rx_drain_until` in the main loop and refresh it after
  remote payload work.
- Timer path chooses:
  1. pressure maintenance budget/source when dirty pressure is eligible;
  2. recent-active active-flow budget/source when the deadline is still live;
  3. zero otherwise.

## Local Gate Checklist

- `cargo test recent_active --lib`
- `cargo test tun_rx_drain --lib`
- `cargo test downlink_backpressure --lib`
- `cargo test pressure_credit --lib`
- `cargo test drop_credit --lib`
- `cargo test close --lib`
- `cargo test reap --lib`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib`
- `cargo test`
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `git diff --check`

## VPS Checklist

- Sync scoped source/docs and touch `src/client_tun.rs` on `.27`.
- Source `.env` in the same true TTY shell before running the suite.
- Use reverse-first P1 stop mode.
- Treat `.77:22` target SSH evidence tail pressure as test noise unless it
  appears inside the P1 attribution window.
- Parse throughput, zero intervals, TUN drops, timer active-flow attempts,
  send queue max, pending/close/reap, and QUIC health.
