# Knife14bu Durable Egress Pressure Plan

## Plan

1. Add one focused Rust test.
   - Given raw pressure reaches high at `t0`.
   - When the next raw snapshot is zero before the hold deadline.
   - Then the effective pressure still keeps the downlink gate paused.
   - After the deadline, zero raw pressure resumes through the existing low
     watermark rule.

2. Implement the smallest core state.
   - Extend the existing TUN egress feedback state with a bounded local egress
     pressure hold.
   - Keep recent-pressure drop attribution independent.
   - Use a short hold interval rather than a 1s sysfs-sample-scale pause.

3. Wire the gate.
   - Main loop downlink backpressure decisions use effective pressure.
   - Feedback sampling can also see effective pressure when deciding whether a
     paused feedback state should resume.
   - Raw pressure observations continue to refresh drop attribution and the
     hold.

4. Run local gates.
   - Focused new test.
   - `downlink_backpressure` tests.
   - `tun_egress_feedback` tests.
   - Full `cargo test --lib`.
   - `git diff --check`.

5. Review risk.
   - Confirm no env/script behavior changed.
   - Confirm explicit high/low behavior remains unchanged.
   - Confirm the hold is bounded and cannot keep global rx paused forever.

6. Update learning memory and commit/push.

7. Run one scoped VPS reverse-first P1 if local gates pass.
   - Reuse the Knife14bt acceptance shape.
   - Stop after reverse-first P1.
   - Parse the bundle before deciding the next patch.

## Risk Review

- Risk: the hold over-pauses remote reads and further reduces throughput.
  - Mitigation: the interval is short and bounded; if VPS regresses, revert or
    tune the hold model with measured evidence instead of adding more pauses.

- Risk: effective pressure logs can look like smoltcp queue pressure even after
  poll/flush.
  - Mitigation: the result doc must compare raw close/flush counters and note
    that effective pressure is a gate input, not necessarily app-owned pending.

- Risk: drop feedback and hold state interact badly.
  - Mitigation: keep the old TUN feedback tests and add only one new behavior
    test in this stage.
