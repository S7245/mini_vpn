# Knife14ag spec - pressure attribution summaries

## Grounding

- Latest accepted stale-slot fix: `7c683b0`.
- Latest recorded follow-up commit: `afb18f5`.
- Input bundle:
  `/tmp/mini_vpn/mvpn_knife14af2_usclient_suite_20260703_214744.tar.gz`.
- The suite completed successfully and showed the desired stale-slot signal:
  `tuic-tcp-pool-reconnect conn=1 reason=stale_tcp_pool_slot`.
- Direct baselines were healthy in the same bundle:
  - Client `.27 -> .77`: 280 Mbit/s receiver.
  - Target `.77 -> .27`: 278 Mbit/s receiver.
  - Exit `.33 -> .77`: 274 Mbit/s receiver.
  - Target `.77 -> .33`: 286 Mbit/s receiver.
- Tunnel throughput varied sharply:
  - reverse-first P1: 30.9 Mbit/s receiver.
  - standalone forward P1: 2.93 Mbit/s receiver.
  - standalone reverse P1: 26.7 Mbit/s receiver.
  - full forward P1: 191 Mbit/s receiver.
  - full reverse P1: 23.1 Mbit/s receiver.
- The full forward run emitted hundreds of `tcp-local-write-pressure` lines and
  one TCP pool connection showed large QUIC loss/congestion with a small cwnd.
- Reverse runs emitted `tcp-downlink-backpressure` without QUIC flow-control
  blocked frames.

## Problem

The low-RTT probe currently copies raw metrics into the report. That is enough
for manual analysis, but the next bottleneck is variance across probe segments,
not a single obvious failure. Without a per-iperf attribution summary, each VPS
bundle still requires manual line-by-line correlation between throughput,
selected TUIC pool connection, local write pressure, downlink pending, and QUIC
loss/congestion.

## Goal

Add a structured, per-iperf-run attribution summary to
`scripts/knife14b-lowrtt-probe.sh` so the next acceptance bundle can quickly
distinguish:

- local write pressure on forward/uplink-heavy runs;
- local downlink pending/backpressure on reverse-heavy runs;
- QUIC loss/congestion and cwnd collapse on a selected pool connection;
- peer flow-control blocked frames;
- stale pool reconnect events that are expected rather than failures.

## Non-Goals

- Do not change TUIC framing, relay lifecycle, TCP pool selection, or stale-slot
  reconnect behavior.
- Do not change congestion-control defaults, flow-control windows, socket buffer
  defaults, MTU, or downlink backpressure watermarks.
- Do not run another VPS suite before the local parser behavior is tested.
- Do not store secrets or raw noisy logs in learning memory.

## Design

- Keep the existing raw metrics section unchanged.
- After each iperf command, append an `Attribution Summary` subsection derived
  from:
  - the iperf command output captured for that run;
  - mini_vpn log lines since that command started.
- Summarize at least:
  - `iperf_receiver_mbps`;
  - TCP pool opens and stale reconnect count;
  - local write pressure event count and max wait;
  - global_rx pressure event count and max wait;
  - downlink backpressure pause/resume count and max pending bytes;
  - worst QUIC connection by lost bytes/congestion events, with min cwnd and
    max blocked-frame counters.
- Emit a conservative attribution label:
  - `quic_flow_control`;
  - `quic_loss_congestion`;
  - `local_write_pressure`;
  - `local_downlink_backpressure`;
  - `global_rx_backpressure`;
  - `no_pressure_signal`;
  - or a `+`-joined combination when several signals are active.
- Add a local `--self-test` mode that tests parser output from small embedded
  log and iperf samples without requiring Linux, `iperf3`, `curl`, or `timeout`.
- Include attribution lines in the parent suite's probe summary grep.

## Acceptance

- `bash -n scripts/knife14b-lowrtt-probe.sh` passes.
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh` passes.
- `bash scripts/knife14b-lowrtt-probe.sh --self-test` passes on macOS.
- A sample extracted from the latest acceptance bundle can be summarized without
  changing raw metrics.
- Next VPS bundle should contain `Attribution Summary` sections for each TCP
  forward/reverse probe, making pressure/loss attribution visible without
  reopening stale-slot diagnosis.
