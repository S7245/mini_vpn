# Knife14co Active-Flow ACK Drain Plan

Date: 2026-07-06

## Stage Plan

1. Ground Knife14cn results and inspect TUN RX drain budget wiring in
   `src/client_tun.rs`.
2. Add one focused RED test for active-flow ACK drain below the credit edge.
3. Add a small bounded active-flow budget helper.
4. Route remote-payload pre/post drain through the helper when the explicit
   operator override is zero and local egress is still below the pressure edge.
5. Keep maintenance/timer drain pressure-gated so the patch is not background
   polling.
6. Run focused and broad local gates.
7. Sync the scoped code/docs to `.27`, touch `src/client_tun.rs`, source `.env`,
   run the reverse-first VPS suite, fetch/parse the bundle, and compare with
   Knife14cn.
8. Record results and learning/error updates before the next patch.

## Patch Shape

- Add `TUN_RX_ACTIVE_FLOW_DRAIN_MAX_PACKETS`.
- Add `active_flow_tun_rx_drain_budget(cfg, tun_mtu)`.
- Use a fraction of the pressure ACK budget for active-flow drain, with an
  independent cap.
- Keep `tun_rx_drain_budget_after_remote_payload(false, ...) == 0`.
- Keep explicit `MINI_VPN_TUN_RX_DRAIN_BUDGET` behavior unchanged.
- Keep `tun_rx_drain_budget_for_dirty_pressure(...)` unchanged below the credit
  edge.

## Local Gate Checklist

- `cargo test active_flow --lib`
- `cargo test tun_rx_drain --lib`
- `cargo test downlink_backpressure --lib`
- `cargo test pressure_credit --lib`
- `cargo test drop_credit --lib`
- `cargo test pending_downlink_close_deferral --lib`
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

- Confirm local worktree is clean except the current stage before sync.
- Sync only scoped source/docs to `.27` and run `touch src/client_tun.rs`.
- On `.27`, use a login shell and source `.env` in the same command before
  running the suite.
- Preflight `.33` sing-box and `.77` iperf3 health.
- Inspect `.33` current logs for `fail auth`; if present, check service state,
  time sync, and config alignment before changing mini_vpn.
- Parse throughput, TUN drops, debt paid/blocked, `send_queue_max`,
  pending/close/reap, TUN RX drain sources, and QUIC health from the bundle.
