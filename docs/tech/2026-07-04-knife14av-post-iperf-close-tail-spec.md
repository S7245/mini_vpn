# Knife14av spec - post-iperf close-tail reporting window

Date: 2026-07-04

## Grounding

Knife14au added close pending taxonomy and then ran scoped US-client
acceptance. The run showed reverse TCP throughput was no longer in the
`10-20 Mbit/s` band:

- reverse-first P1: `179/179 Mbit/s`;
- standard reverse P1: `185/185 Mbit/s`;
- full reverse: `183/181 Mbit/s`.

The taxonomy worked for probe-window close events: reverse-first P1 reported
`terminal_closed_no_send` pending bytes, and standard reverse P1 reported
`active_no_send` pending bytes.

However, full reverse had a raw `tcp-handle-close` line with
`close_pending_class=terminal_closed_no_send` and non-zero pending bytes after
the per-probe attribution summary was already written. The final post-run
metric tail showed the line, but the full reverse `pending_at_close` summary
reported zero.

## Problem

`scripts/knife14b-lowrtt-probe.sh` currently summarizes immediately after
`iperf3` exits:

1. run `iperf3`;
2. sample TUN dropped counters;
3. append matching mini_vpn metrics since the probe start line;
4. append attribution summary.

TCP close and reap logging may arrive shortly after the application command
returns. When that happens, the raw log contains the close-tail evidence, but
the per-probe summary and attribution omit it. That means the acceptance report
can still hide the exact `pending/close/reap` signal Knife14au was created to
surface.

## Goal

Make each iperf probe wait a short, bounded post-iperf settle window before
sampling final TUN counters and before writing mini_vpn metrics and attribution
summary.

The report must record the settle duration so future runs can interpret the
window consistently.

## Non-Goals

- Do not change Rust data-plane behavior.
- Do not change close-drain timing, receive-window behavior, downlink pacing,
  TUN queue length, TUIC pool behavior, iperf3 settings, or sing-box settings.
- Do not infer that terminal close pending is useful data loss by itself. This
  stage only makes the evidence consistently visible in the per-probe report.
- Do not store secrets, TUIC credentials, sudo passwords, private keys, or
  credential-bearing raw logs in docs or learning memory.

## Invariants

- `POST_IPERF_METRICS_SETTLE_SECS=0` must preserve the previous immediate
  summary behavior for fast local debugging.
- The default settle window must be short enough not to materially inflate the
  suite, while long enough to catch close-tail logs that arrive just after
  `iperf3` exits.
- TUN drop deltas must be sampled after the settle window so tail flush/drop
  effects are attributed to the same probe.
- The late close-tail parser path must be covered by the shell self-test.

## Acceptance

Local:

- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`

VPS:

- Run a scoped `.27` suite after pushing this script fix.
- The report header must include `post_iperf_metrics_settle_secs`.
- If raw logs contain a `tcp-handle-close pending>0` immediately after a probe,
  the matching per-probe attribution summary must include `pending_at_close`
  and the proper pending class.
