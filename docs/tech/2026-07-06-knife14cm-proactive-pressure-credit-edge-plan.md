# Knife14cm Proactive Pressure Credit Edge Plan

Date: 2026-07-06

## Stage Plan

1. Ground Knife14cl evidence and inspect pressure-debt hot paths.
2. Add focused tests for proactive pressure-debt installation at the credit
   edge before pause.
3. Refactor the runtime pressure debt gate so a credit-edge observation with
   dirty downlink can install bounded pressure debt once per pressure episode.
4. Keep debt payment tied to observed send-queue drain; do not release extra
   credit while debt remains.
5. Run local focused tests, broader Rust tests, harness, clippy, and diff check.
6. Sync the scoped patch to `.27`, run the reverse-first P1 VPS acceptance, and
   parse the bundle.
7. Record results in docs plus `.learnings/LEARNINGS.md` and
   `.learnings/ERRORS.md` as needed.

## TDD Targets

- `downlink_pressure_credit_debt_installs_before_pause_at_credit_edge`
  should fail before code changes because the current runtime gate requires a
  pause edge.
- Existing tests around debt sizing, stale-credit blocking, and debt repayment
  should remain unchanged unless they reveal a real invariant mismatch.

## Patch Shape

- Add a small helper that decides whether pressure debt should be installed
  from dirty-downlink pressure observations, not only pause transitions.
- Use the existing `downlink_egress_pressure_debt_bytes` sizing function so the
  patch changes timing, not threshold math.
- Avoid repeated install churn by checking the active debt/source before adding
  another generation.
- Preserve diagnostic lines, but make their `reason` distinguish proactive
  credit-edge installation from pause-edge installation.

## Local Gate Checklist

- `cargo test pressure_credit --lib`
- `cargo test drop_credit --lib`
- `cargo test tun_rx_drain --lib`
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

- Preflight `.33` sing-box and `.77` iperf3 services.
- Check current `.33` log window for `fail auth`; if it appears, inspect
  service state, time sync, and no-secret config alignment before blaming code.
- Run the reverse-first P1 suite with the same `.27/.33/.77` shape.
- Parse the generated bundle and compare against Knife14cl:
  throughput, pressure debt installation/payment, TUN drops, pending/close/reap
  accounting, QUIC path health, and direct baselines.
