# Knife14cf Proactive Egress Credit Gate Plan

Date: 2026-07-06

## Design Tree

1. Post-drop debt is sufficient, but Knife14ce sampled too slowly by chance.
   - Rejected for this stage: the first positive drop feedback happened after
     the useful hot flush path had already stopped, and final flush debt fields
     stayed zero. This is a timing design problem, not just sample variance.
2. Static thresholds are too high.
   - Weak: lowering values alone risks trading throughput for fewer drops and
     does not distinguish clean drain from pressure-cleared queue space.
3. Local egress pressure must become an early credit-denial signal.
   - Strong: the failing window hit pending/tx-queue credit edges before
     drop feedback. Clearing stale credit at the pressure edge can prevent the
     next refill burst from immediately spending above clean headroom.
4. Feedback pause is a separate lifecycle bug.
   - Plausible: final `pause_edges=11 resume_edges=0` means the feedback gate
     can remain paused after useful downlink work is gone. The update path
     should use raw current pressure for resume, while retained recent pressure
     remains only for drop attribution.

## Implementation Plan

1. Generalize the credit-debt state.
   - Track drop-triggered and pressure-triggered debt buckets under one
     generation.
   - Keep total debt bounded to the normal credit span.
   - Record which source last advanced the generation so stale per-flow credit
     can be attributed.
2. Add a proactive pressure edge gate.
   - On a downlink backpressure rising edge, install pressure credit debt only
     if raw local egress pressure is at the credit edge:
     `max_tx_queue >= tx_queue_credit_spend_threshold`, or pending is high while
     `max_tx_queue >= tx_queue_flush_threshold`.
   - Do not install debt for pure app-pending pressure with a low tx queue.
3. Tighten debt repayment semantics.
   - Observed queue drain pays debt first.
   - If any debt was paid in this observation, grant no fresh extra credit from
     the same observation even if the observed drain was larger than the debt.
   - A later clean drain observation can grant credit again.
4. Fix feedback resume call-site behavior.
   - Use raw current pressure for `TunEgressFeedbackState::update`.
   - Keep `effective_downlink_pressure_at` only for ordinary downlink
     backpressure hold.
5. Make the new path observable.
   - Add `pressure_credit_*` counters next to the existing `drop_credit_*`
     counters in aggregate and close diagnostics.
   - Update both Knife14 parsers and self-tests.

## Test Plan

RED first:

- Add pure credit-clock tests for pressure debt installation and same-observation
  overpay behavior.
- Add a feedback test proving raw-low resume can happen while the separate
  egress hold is still active.
- Add parser/format assertions for `pressure_credit_*` fields.

GREEN:

- Implement generalized credit debt and pressure-edge installation.
- Update diagnostic formatters and parsers.
- Switch feedback update to raw pressure.

Regression:

- `cargo test pressure_credit --lib`
- `cargo test drop_credit --lib`
- `cargo test downlink_egress_clock --lib`
- `cargo test tun_egress_feedback --lib`
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

Use the same shape as Knife14ce:

- `.27` client, `.33` exit, `.77` target
- source `.27` `.env` in the true TTY shell
- default pool=2
- `RUN_REVERSE_FIRST_P1=1`
- `STOP_AFTER_REVERSE_FIRST_P1=1`
- `PARALLEL_SET=1`
- `DURATION=30`
- direct `.27 -> .77` and `.33 -> .77` baselines required
- server evidence enabled

## Stop Conditions

- If proactive pressure credit debt engages and throughput remains low with
  drops unchanged, stop and re-evaluate the downlink architecture.
- If pressure debt reduces drops only by collapsing throughput, treat it as an
  algorithm failure, not acceptance.
- If current-window sing-box `fail auth` appears, inspect auth/time/config
  before attributing the result to mini_vpn egress behavior.
