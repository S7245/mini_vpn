# Knife14cq Recent-Active Timer Opt-In Plan

Date: 2026-07-06

## Stage Plan

1. Record Knife14cp as a failed default-path experiment.
2. Add a default-disabled, bounded runtime knob:
   `MINI_VPN_TUN_RX_ACTIVE_FLOW_TIMER_MS`.
3. Gate recent-active timer budget on that knob so default timer ticks do not
   run below-pressure active-flow drain.
4. Keep payload-triggered active-flow drain and pressure maintenance drain
   unchanged.
5. Add TDD coverage for default-disabled config, parser bounds, and explicit
   A/B enablement.
6. Run focused local regressions and broad gates.
7. If local gates pass, sync the patch to `.27` and run a scoped VPS check only
   if the default-path restoration needs real-path confirmation before the next
   no-pressure branch.

## Local Gate Checklist

- `cargo test recent_active --lib`
- `cargo test tun_rx_active_flow --lib`
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

## Expected Result

Default diagnostics should show `timer_active_flow_attempts=0` in future runs
unless the operator explicitly enables `MINI_VPN_TUN_RX_ACTIVE_FLOW_TIMER_MS`.
This restores the cleaner Knife14co baseline and keeps the next investigation
focused on no-pressure burst/idle stream read or wake cadence.
