# Knife14am plan - inherited QUIC congestion attribution

> Spec:
> `docs/tech/2026-07-04-knife14am-inherited-quic-attribution-spec.md`.

1. Add the TDD red sample.
   - Reuse `scripts/knife14b-lowrtt-probe.sh --self-test`.
   - Embed a reverse iperf sample at about 11.5/10.7 Mbit/s.
   - Embed QUIC stats whose deltas are zero but whose first sample has
     `cwnd=5808`, `lost_bytes=500976100`, and `congestion_events=93571`.
   - Assert `inherited_quic_congestion` and no `no_pressure_signal`.

2. Implement the parser fix.
   - Track first cwnd, first lost bytes, and first congestion events per
     `conn:id`.
   - Use conservative built-in thresholds:
     first cwnd <= 65536 bytes and either first lost bytes >= 1048576 or first
     congestion events >= 100.
   - Add `inherited_quic_congestion` as an additive label.
   - Include absolute-start fields in the `quic:` summary.

3. Keep parent-suite visibility.
   - Verify the existing summary grep still surfaces the `quic:` and
     `attribution:` lines.
   - Only change the parent suite if the new fields are hidden.

4. Verify locally.
   - `bash -n` for modified scripts.
   - `bash scripts/knife14b-lowrtt-probe.sh --self-test`.
   - `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`.
   - `git diff --check`.

5. Stage review and learning.
   - Review thresholds for false positives.
   - Confirm no secrets or raw noisy logs were added.
   - Record the stage outcome in `.learnings/LEARNINGS.md`; record failures in
     `.learnings/ERRORS.md` only if they change future behavior.

6. Commit and push the coherent task.
