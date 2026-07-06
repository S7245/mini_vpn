# Knife14cn Debt-Coupled Receive Gate Plan

Date: 2026-07-06

## Stage Plan

1. Ground Knife14cm valid VPS evidence and identify the receive/backpressure
   handoff in `src/client_tun.rs`.
2. Add a focused RED test for active credit debt pausing remote receive at the
   tx-queue credit edge.
3. Implement a small debt-aware wrapper around the existing downlink
   backpressure helper. Keep the original helper as the no-debt behavior.
4. Reorder the event-loop edge slightly: decide whether ordinary backpressure
   would pause, install pressure debt if needed, then compute the final
   receive pause with the active debt state included.
5. Run focused debt/backpressure/close tests, then broader local gates.
6. Sync the code to `.27`, force source freshness before build, run the
   reverse-first P1 VPS suite, fetch the bundle, and compare against Knife14cm.
7. Record results and learning/error updates before the next code patch.

## Patch Shape

- Add `DownlinkEgressCreditDebt::has_active_debt()`.
- Add `next_downlink_backpressure_with_credit_debt(...)`.
- The helper returns paused when active debt exists and pressure reaches the
  existing credit-debt pressure threshold.
- The helper otherwise delegates to the existing high/low and tx-queue
  hysteresis logic.
- Runtime logs keep the existing `pressure_credit_edge` versus
  `pressure_pause_edge` distinction by computing the no-debt pause decision
  before installing debt.

## Local Gate Checklist

- `cargo test active_credit_debt --lib`
- `cargo test downlink_backpressure --lib`
- `cargo test pressure_credit --lib`
- `cargo test drop_credit --lib`
- `cargo test pending_downlink_close_deferral --lib`
- `cargo test close --lib`
- `cargo test reap --lib`
- `cargo test tun_rx_drain --lib`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib`
- `cargo test`
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `git diff --check`

## VPS Checklist

- On `.27`, copy the scoped source patch and run `touch src/client_tun.rs`
  before build/test so cargo cannot reuse stale artifacts.
- Preflight `.33` sing-box and `.77` iperf3 health.
- Check `.33` current log window for `fail auth`; if present, inspect service
  state, time sync, and config alignment before blaming the code patch.
- Run the same reverse-first P1 acceptance shape as Knife14cm.
- Parse the returned bundle for throughput, `pressure_credit_debt_paid_bytes`,
  `drop_credit_debt_paid_bytes`, TUN drops, `send_queue_max`, pending/close,
  terminal reap, and QUIC health.
