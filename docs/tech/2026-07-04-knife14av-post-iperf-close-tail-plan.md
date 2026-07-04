# Knife14av plan - post-iperf close-tail reporting window

Date: 2026-07-04

## Stage Goal

Close the remaining acceptance-report timing gap exposed by Knife14au, then
rerun the scoped VPS suite so `pending/close/reap` evidence is summarized in
the same probe where it occurred.

## Design Tree

1. Change mini_vpn close-drain behavior.
   Rejected for this slice. Reverse throughput is already high in the latest
   scoped acceptance, and the new evidence points to reporting order rather
   than a new Rust lifecycle bug.

2. Keep relying on final post-run metric tails.
   Rejected. Final tails are useful for manual inspection but do not update the
   per-probe attribution labels that drive the Knife14 branch decisions.

3. Add a bounded post-iperf settle window before per-probe summaries.
   Selected. This preserves the raw start-line window, includes close-tail logs
   that arrive just after `iperf3`, and keeps the behavior configurable for
   quick local runs.

## Tasks

1. Add `POST_IPERF_METRICS_SETTLE_SECS` to the probe usage and report header.
2. Validate it as a non-negative integer.
3. Wait for that window after each `iperf3` attempt completes and before final
   TUN drop sampling, metric extraction, and attribution summary.
4. Add a shell self-test that appends a `tcp-handle-close` line after the
   simulated iperf window and verifies `pending_at_close`/terminal pending are
   included after settling.
5. Update project learning/error memory with the post-iperf timing lesson.
6. Run local gates, review diff, commit, and push.
7. Update `.27` to the pushed commit and rerun a scoped acceptance suite.

## Regression Checks

- Default added suite cost is bounded: `2s * number_of_probe_commands`.
- `POST_IPERF_METRICS_SETTLE_SECS=0` remains available for immediate summaries.
- Existing attribution parsing and older log compatibility are unchanged.
- This change must not alter the tunnel process, iperf command shape, or VPS
  service configuration.
