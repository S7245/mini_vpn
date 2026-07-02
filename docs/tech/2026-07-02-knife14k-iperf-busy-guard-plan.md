# Knife14k plan - guard iperf busy sweeps

> Spec: `docs/tech/2026-07-02-knife14k-iperf-busy-guard-spec.md`.

## Task Tree

1. Probe guard
   - Add `IPERF_BUSY_RETRIES` and `IPERF_BUSY_WAIT_SECS`.
   - Capture iperf output per attempt while preserving the report transcript.
   - Retry only when iperf says the server is busy.

2. Suite wiring
   - Document the new knobs in the suite usage.
   - Pass the configured values into the low-RTT probe.

3. Verification
   - Run `bash -n` on both scripts.
   - Exercise the busy guard with a temporary fake `iperf3`.
   - Re-check git diff.

4. Stage review and commit
   - Review that non-busy failures are not masked.
   - Commit as one knife14k test-harness stage.
