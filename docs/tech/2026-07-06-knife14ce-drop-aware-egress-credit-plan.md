# Knife14ce Drop-Aware Egress Credit Plan

Date: 2026-07-06

## Design Tree

1. TUN drops are unrelated to mini_vpn local credit.
   - Rejected for this stage: Knife14cd had clean direct baselines, clean QUIC
     counters, no send errors, but drops appeared exactly with local tx-queue
     pressure and credit hard-edge backlog.
2. Static thresholds are too high.
   - Weak: lowering thresholds alone may trade drops for throughput and would
     not explain why drop-induced queue decreases are considered safe credit.
3. Current egress clock mistakes drop-cleared queue space for successful drain.
   - Strong: credit is granted from `last_send_queue - send_queue` with no
     distinction between real delivery and TUN drop feedback.
4. Close/reap is losing bytes.
   - Not the first patch: Knife14cd showed `terminal_pending_reap=0` and
     send-capable close backlog, so lifecycle accounting is visible rather than
     hidden.

## Implementation Plan

1. Add a small `DownlinkEgressDropDebt` state machine.
   - Positive drop feedback increments a generation and installs bounded debt
     equal to the normal drain-credit span.
   - Per-flow clocks notice a new generation, clear stale drain credit, and
     stop spending credit while debt remains.
2. Thread the drop-debt state into downlink flush planning.
   - `bounded_downlink_flush_limit_for_window_with_clock` pays debt from
     observed queue decreases before granting credit.
   - Clean headroom remains available even when debt is active.
3. Wire TUN feedback to drop debt.
   - On `TunEgressFeedbackReason::DropDelta`, install debt in the main loop's
     shared egress credit state.
4. Make it observable.
   - Add aggregate/close diagnostics for:
     - drop-credit debt installed bytes/events;
     - drop-credit debt paid bytes;
     - drain-credit bytes blocked while debt was active.
   - Update both Knife14 parsers and self-tests.
5. Verify locally, then run one scoped VPS acceptance.

## Test Plan

RED first:

- Add focused tests around the pure credit planner showing current code wrongly
  spends or grants credit immediately after a drop.
- Add a diagnostic formatting/parser assertion so new counters cannot disappear
  silently.

GREEN:

- Implement the drop-debt clock.
- Update parser summaries.

Regression:

- `cargo test drop_credit --lib`
- `cargo test downlink_egress_clock --lib`
- `cargo test tcp_downlink_flush_aggregate_formats_progress_signal --lib`
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

Use the same scoped shape as Knife14cd:

- `.27` client, `.33` sing-box/TUIC, `.77` iperf3 target
- default `MINI_VPN_TUIC_TCP_POOL=2`
- `RUN_REVERSE_FIRST_P1=1`
- `STOP_AFTER_REVERSE_FIRST_P1=1`
- `PARALLEL_SET=1`
- `DURATION=30`
- direct `.27 -> .77` and `.33 -> .77` baselines required
- server evidence enabled

Record the bundle and parse:

- throughput shape
- TUN drops and feedback counters
- downlink flush counters including drop-credit debt
- close pending/egress classes
- sing-box current-window auth evidence

## Stop Conditions

- If local tests show the proposed debt model blocks clean headroom, revise the
  algorithm before VPS.
- If VPS throughput regresses below high-throughput class while TUN drops fall,
  treat that as an algorithm balance failure, not acceptance.
- If TUN drops remain high and new debt counters show activation, re-evaluate
  whether smoltcp/TUN flush cadence needs an architecture-level change.
