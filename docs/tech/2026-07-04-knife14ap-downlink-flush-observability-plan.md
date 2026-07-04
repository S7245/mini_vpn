# Knife14ap plan - observe bounded downlink flush progress

Spec:
`docs/tech/2026-07-04-knife14ap-downlink-flush-observability-spec.md`.

1. Record failed Knife14ao acceptance.
   - Add an `.learnings/ERRORS.md` entry that 512/128 KiB defaults were active
     but not sufficient.
2. Add RED tests.
   - Rust: `TcpDownlinkDiag` tracks flush attempts, no-capacity attempts,
     budget-limited calls, and max accepted bytes.
   - Rust: aggregate formatting includes the new `tcp-downlink-flush` fields.
   - Shell: low-RTT probe self-test expects a `downlink_flush:` summary.
3. Implement Rust diagnostics.
   - Extend `TcpDownlinkDiag`.
   - Update `flush_downlink` without changing delivery behavior.
   - Add an aggregate helper and print it on metrics ticks.
4. Implement script parsing.
   - Include `tcp-downlink-flush` in `METRIC_RE`.
   - Summarize max/current fields into `downlink_flush:`.
   - Include the new line in the parent suite's probe summary grep.
5. Verify locally.
   - Run the local acceptance gates from the spec.
6. Stage review and learning.
   - Review the diff for behavior drift and hot-path overhead.
   - Record the stage result in `.learnings/LEARNINGS.md`.
   - Commit and push the coherent observability task.
