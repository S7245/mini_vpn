# Knife14ch Adaptive Pressure Credit Debt Plan

Date: 2026-07-06

## Design Tree

1. Keep full pressure-edge debt.
   - Rejected: Knife14cf already showed full-span pressure debt reduces drops
     but keeps reverse-first P1 in the `10-20 Mbit/s` class.
2. Remove pressure-edge debt and keep only drop debt.
   - Rejected as a direct revert: Knife14ce proved post-drop feedback arrives
     too late for the hot burst.
3. Make pressure-edge debt adaptive while keeping full drop debt.
   - Strong: pressure is an early warning, so it should reduce stale credit
     gradually. Actual TUN drop remains a hard-loss signal and keeps the full
     debt span.
4. Increase receive/read-ahead to hide stalls.
   - Rejected by Knife14cg: bounded global receive decoupling increased
     pending and TUN drops without improving throughput.

## Implementation Plan

1. Add a derived pressure-debt sizing helper.
   - Return `0` below the credit edge.
   - Return at least `tx_queue_credit_guard_bytes(cfg)` at the edge.
   - Add actual bytes above `tx_queue_credit_spend_threshold(cfg)`.
   - Cap at `downlink_egress_credit_span(cfg)`.
2. Change `DownlinkEgressCreditDebt::note_egress_pressure` to install the sized
   pressure debt instead of the full span.
3. Keep `note_tun_drop` unchanged so observed drops still install full debt.
4. Update focused TDD tests for guard-sized pressure debt and overshoot capping.
5. Run local gates.
6. Commit/push the code/spec/plan before VPS.
7. Sync `.27`, run the same clean reverse-first P1 acceptance, parse the
   bundle, and record results/learnings/errors in a second coherent commit.

## Test Plan

Focused:

- `cargo test pressure_credit --lib`
- `cargo test credit_debt --lib`
- `cargo test drop_credit --lib`
- `cargo test downlink_egress_clock --lib`

Regression:

- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib`
- `cargo test`
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `git diff --check`

## VPS Plan

- Use `.27` true TTY if sudo is needed.
- Source `.27` `.env` in the same shell without printing secrets.
- Run `RUN_REVERSE_FIRST_P1=1 STOP_AFTER_REVERSE_FIRST_P1=1`.
- Keep server evidence enabled and check `.33` current-window TUIC auth.
- Compare against Knife14cd, Knife14ce, Knife14cf, and Knife14cg:
  - throughput class;
  - TUN drops;
  - pressure/drop debt installed and paid;
  - final pending/egress close backlog.

## Stop Conditions

- If current-window `.33` shows TUIC `fail auth`, inspect auth/time/config
  before attributing the run to mini_vpn.
- If adaptive pressure debt remains in the low-throughput class, stop pressure
  debt variants and escalate to local egress architecture review.
- If throughput is high but drops/backlog remain, preserve the evidence and
  make the next patch target drain cleanliness rather than pool or receive
  windows.
