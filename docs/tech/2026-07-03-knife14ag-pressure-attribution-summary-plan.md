# Knife14ag plan - pressure attribution summaries

> Spec:
> `docs/tech/2026-07-03-knife14ag-pressure-attribution-summary-spec.md`.

1. Add focused shell parser self-test coverage.
   - Cover receiver bitrate extraction.
   - Cover local write pressure, downlink pressure, stale reconnect, and QUIC
     loss/congestion parsing.
   - Assert the attribution label includes the expected branches.

2. Implement per-iperf summary output in the low-RTT probe.
   - Keep raw metric blocks untouched.
   - Append a compact `Attribution Summary` after each metric block.
   - Keep all logic local to `scripts/knife14b-lowrtt-probe.sh`.

3. Teach the parent US-client suite summary to surface attribution lines.

4. Verify locally.
   - `bash -n` for both modified scripts.
   - `bash scripts/knife14b-lowrtt-probe.sh --self-test`.
   - Optional parser smoke against the extracted `knife14af2` bundle.

5. Stage review.
   - Check quoting, temporary-file cleanup, macOS/BSD awk compatibility, and
     failure behavior.
   - Confirm the script still exits on real iperf failures as before.

6. Commit the coherent task and update `.learnings/LEARNINGS.md`.
