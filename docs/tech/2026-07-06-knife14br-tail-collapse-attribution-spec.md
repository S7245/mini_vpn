# Knife14br spec - tail-collapse attribution

Date: 2026-07-06

## Grounding

Knife14bq repeated `d5d8542` with default server evidence and same-window
`.33 <-> .77` path checks:

- direct `.27 <-> .77` receiver baselines: `281-284 Mbit/s`;
- exit `.33 <-> .77` receiver baselines: `282-283 Mbit/s`;
- reverse-first P1 over mini_vpn: `150/149 Mbit/s`;
- first 24 seconds mostly stayed above `126 Mbit/s`;
- final 6 seconds collapsed to about `15.7-16.8 Mbit/s`;
- `.77` iperf3 sender matched the tunnel total at `537 MBytes / 150 Mbit/s`;
- `.33` showed current TUIC inbound/direct outbound and no current TUIC
  `fail auth`;
- mini_vpn reported `tun_tx_dropped_delta=14804`,
  `downlink_backpressure=693/693`, `tun_flush_deferred=693`,
  `terminal_pending_reap=0`, `pending_at_close=0`, and
  `terminal_late_remote_payload=1474528B`;
- QUIC loss/congestion and blocked-window deltas stayed `0/0`.

## Problem

The current per-probe attribution summary reports aggregate sender/receiver
throughput and aggregate pressure counters. It does not make the per-second
iperf tail visible as structured data.

That gap matters because Knife14bq has a high average but still fails stable
throughput acceptance. Without a machine-readable tail profile, a later bundle
can be misread as accepted merely because `iperf_receiver_mbps` is high, while
the last seconds still collapse with local TUN/downlink pressure.

## Goal

Make `scripts/knife14b-lowrtt-probe.sh` classify TCP reverse probe throughput
shape without manual log reading:

- `no_data`: near-zero sender and receiver totals;
- `tail_collapse`: high earlier per-second rates followed by a low tail;
- `tail_collapse_local_pressure`: tail collapse with local TUN/drop/downlink
  pressure evidence in the same probe window;
- `stable_high`: high aggregate and high tail rates;
- lower-confidence fallback shapes for low or unclassified runs.

The classification must be printed inside every `Attribution Summary` and
surfaced by the parent US-client suite summary grep.

## Non-Goals

- Do not change Rust data-plane behavior in this slice.
- Do not tune the downlink egress pacer, TUN queue length, TCP pool, iperf3,
  sing-box, or QUIC congestion control.
- Do not infer exact per-log-line wall-clock alignment from logs that do not
  include stable timestamps. This stage correlates per-second iperf tail shape
  with pressure counters from the same bounded probe window.
- Do not store secrets, TUIC credentials, sudo passwords, private keys, or raw
  noisy logs in docs or learning memory.

## Design Tree

1. Patch data-plane backpressure immediately.
   Rejected for this slice. Knife14bq is partial success but not final
   acceptance; AGENTS.md requires analysis and a confirmed plan before behavior
   changes after a failed/partial repair.

2. Use only aggregate `iperf_receiver_mbps`.
   Rejected. `149 Mbit/s` average hid the final six-second collapse.

3. Add a parser-only iperf interval profile and throughput-shape label.
   Selected. It is low risk, testable locally, and makes the next behavior
   patch easier to judge.

4. Add timestamped mini_vpn metric emission before classification.
   Deferred. It may be useful later, but the immediate gap is that existing
   bundle data already contains per-second iperf intervals that the parser is
   ignoring.

## Invariants

- Existing `Attribution Summary` lines and labels remain backward-compatible.
- Missing per-second iperf intervals must not break the summary; the shape
  becomes `unknown` or falls back to aggregate classification.
- Tail-collapse labels must not fire for UDP probes.
- The accepted close accounting invariant remains unchanged:
  `Closed && !can_send` late payload stays outside app-owned pending and is
  counted explicitly.

## Acceptance

Local:

- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`

Evidence smoke:

- Run the parser against the extracted Knife14bq bundle and verify its
  reverse-first P1 summary includes:
  - `iperf_interval_profile`;
  - `throughput_shape: shape=tail_collapse_local_pressure`;
  - `attribution` including `iperf_tail_collapse` and
    `tail_collapse_local_pressure`.

VPS:

- After this evidence patch is pushed, the next scoped `.27` run must include
  the throughput-shape lines in the parent suite report, so the next behavior
  patch can be judged without reopening the raw bundle manually.
