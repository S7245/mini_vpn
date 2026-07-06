# Knife14cg Bounded Global RX Receive Window Plan

Date: 2026-07-06

## Design Tree

1. More pressure/drop debt is needed.
   - Rejected for this stage: Knife14cf already proved debt engaged and reduced
     drops, but throughput remained burst/stall with active send-capable close
     backlog.
2. The receive window should simply be larger.
   - Weak if done as a raw threshold change. The key design change is separating
     the remote receive gate from local egress pressure; the bounded high/low
     values are derived from the existing backpressure config.
3. Local egress and remote receive need separate gates.
   - Strong: smoltcp tx queue pressure should stop extra local flushing, not
     necessarily stop relay/TUIC stream reads while app-owned pending has
     bounded room.
4. TUN feedback pause should stay as a receive stop.
   - Weak as a hard gate: it can preserve the same coupling Knife14cf exposed.
     It should still install debt and remain visible, but global receive should
     be paused only by bounded pending receive-window exhaustion.

## Implementation Plan

1. Add bounded receive-window helpers.
   - Derive high/low from `DownlinkBackpressureConfig`.
   - High is larger than the local egress high watermark but bounded by the TCP
     buffer maximum.
   - Low is the local egress high watermark, giving enough hysteresis to drain
     a burst before relay receive resumes.
2. Add a separate `global_rx_receive_paused` state.
   - Update it from raw pending stats each loop.
   - Do not use tx-queue-only pressure or TUN feedback pause as direct reasons
     to disable `global_rx.recv()`.
3. Keep local egress observability intact.
   - Continue computing `downlink_rx_paused` and TUN feedback pause for logs,
     debt installation, and flush gating.
   - Log a new `tcp-global-rx-backpressure` line on receive-window transitions,
     with receive high/low, pending stats, local egress paused, and feedback
     paused.
4. Update parsers.
   - Summaries should report `global_rx_receive: pause_edges=...`.
   - Attribution should distinguish `local_global_rx_receive_window` from
     `local_downlink_backpressure`.
5. Run local gates, sync to `.27`, run scoped VPS acceptance, parse the bundle,
   and record learning.

## Test Plan

RED first:

- Add tests for the receive-window pause function and global receive gate.
- Add a diagnostic-format test.
- Add parser self-test lines for `tcp-global-rx-backpressure`.

GREEN:

- Implement receive-window helpers and main-loop state split.
- Update parser summaries and grep/tail patterns.

Regression:

- `cargo test global_rx_receive --lib`
- `cargo test downlink_backpressure --lib`
- `bash -n scripts/knife14b-lowrtt-probe.sh scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib`
- `cargo test`
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `git diff --check`

## VPS Plan

- Preflight `.27`, `.33`, `.77`.
- Source `.27` `.env` in the same true TTY shell.
- Run only clean reverse-first P1:
  `RUN_REVERSE_FIRST_P1=1`, `STOP_AFTER_REVERSE_FIRST_P1=1`,
  `PARALLEL_SET=1`, `DURATION=30`, `TARGET=43.130.32.77`.
- Keep server evidence and direct baselines enabled.

## Stop Conditions

- If `global_rx_receive` stays inactive while throughput remains low, the
  receive-decoupling hypothesis is rejected.
- If `global_rx_receive` engages and throughput remains low with unchanged
  TUIC stream gaps, stop for architecture confirmation rather than tuning more
  local thresholds.
- If `.33` shows current-window TUIC `fail auth`, inspect auth/time/config
  before attributing the result to mini_vpn.

## Post-Run Plan Update

The VPS run hit the second stop condition. `global_rx_receive` engaged, but
reverse-first P1 stayed low and pending/TUN-drop pressure increased. Keep the
code and parser support as an opt-in A/B only, record the failed evidence, and
move the next stage away from receive-window growth toward local egress
drain/cadence.
