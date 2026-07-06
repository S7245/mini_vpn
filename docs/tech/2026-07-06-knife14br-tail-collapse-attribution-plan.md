# Knife14br plan - tail-collapse attribution

Date: 2026-07-06

## Stage Goal

Close the reporting gap exposed by Knife14bq: high average throughput must not
hide an unhealthy final tail. This stage is parser/report-only unless the new
evidence forces a separate confirmed behavior plan.

## Tasks

1. Add a red shell self-test to `scripts/knife14b-lowrtt-probe.sh`.
   - Fixture: reverse TCP iperf with about `150 Mbit/s` total, high first
     intervals, and a final six-second `~16 Mbit/s` tail.
   - Fixture logs: local downlink backpressure, TUN egress drop, TUN feedback,
     terminal-late accounting, no QUIC loss/congestion.
   - Expected initial result: self-test fails because no interval profile or
     throughput shape is emitted.

2. Implement a small iperf interval parser.
   - Ignore aggregate `sender`/`receiver` summary rows.
   - Parse per-interval `bits/sec`, `Kbits/sec`, `Mbits/sec`, and `Gbits/sec`.
   - Emit sample count, average interval rate, prefix average, tail sample
     count, tail average, tail minimum, and `tail_collapse=0|1`.

3. Add throughput-shape classification to the attribution summary.
   - `tail_collapse_local_pressure` when reverse TCP tail collapse and local
     TUN/downlink pressure are both present.
   - `tail_collapse` when the tail collapses without local pressure evidence.
   - `no_data`, `stable_high`, and fallback shapes for clearer future bundle
     triage.

4. Surface new summary lines in the parent US-client suite.
   - Add `iperf_interval_profile` and `throughput_shape` to the report grep.

5. Verify locally.
   - Run script syntax checks and self-tests.
   - Run a parser smoke against the extracted Knife14bq bundle if available.
   - Review for parser-only scope, macOS/BSD awk compatibility, and secret
     hygiene.

6. Update learning memory, commit, and push.

## Regression Checks

- Existing pressure, close-tail, inherited-QUIC, stream-gap, and reverse-sender
  self-tests still pass.
- No new dependency on Linux-only tools is introduced in `--self-test`.
- Parent suite still works when old probe reports lack the new lines.
- The change must not alter tunnel startup, iperf command shape, VPS service
  config, or data-plane runtime behavior.
