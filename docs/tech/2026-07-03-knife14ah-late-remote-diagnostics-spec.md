# Knife14ah spec - late remote diagnostics after local finish

## Grounding

- Latest branch head: `eb73646`.
- Latest VPS bundle:
  `/tmp/mini_vpn/mvpn_knife14ag_usclient_suite_20260703_225103.tar.gz`.
- Direct preflights in that bundle were healthy:
  - Client `.27 -> .77`: 291 Mbit/s receiver.
  - Target `.77 -> .27`: 273 Mbit/s receiver.
  - Exit `.33 -> .77`: 297 Mbit/s receiver.
  - Target `.77 -> .33`: 282 Mbit/s receiver.
- Tunnel reverse-first P1 exited 0 but delivered 0 receiver bytes.
- The per-iperf attribution summary reported `no_pressure_signal`: no local
  write pressure, no global_rx pressure, no downlink backpressure, no QUIC
  loss/congestion delta, and no blocked-frame delta.
- The decisive late signal appeared after the iperf window: the relay had
  `remote_to_global_rx_bytes=0` during iperf, then after local `Finish` it read
  43,772 bytes and later closed by `half_closed_idle_timeout`.

## Problem

The current diagnostics show that reverse-first failed without local pressure or
QUIC loss, but the report does not automatically label the late remote bytes
that arrive after local `Finish`. This leaves the next operator doing manual log
correlation across iperf output, relay live lines, write-half-close lines, and
final close lines.

## Goal

Make late remote bytes after local `Finish` visible and machine-summarized:

- relay live/close diagnostics should include enough counters to tell whether
  remote bytes arrived before or after local `Finish`;
- low-RTT attribution summaries should emit a `late_remote_after_local_finish`
  label when remote bytes only show up after local `Finish`;
- low-RTT post-run metrics should include half-close and close lines so a
  reverse-only probe can preserve this signal in its report.

## Non-Goals

- Do not change TUIC framing, TCP relay behavior, half-close timeouts, pool
  selection, QUIC congestion control, socket buffer sizes, or backpressure
  thresholds.
- Do not reopen the accepted stale TCP pool slot diagnosis.
- Do not require a full VPS suite for this stage before local tests and review.
- Do not store TUIC secrets or `.evn` values in docs, logs, learnings, or final
  summaries.

## Design

- Extend `RelayTaskDiag` with counters for:
  - remote bytes observed after local `Finish`;
  - remote reads observed after local `Finish`.
- Update `tcp-relay-live` and `tcp-relay-close` diagnostic lines with these
  counters under `MINI_VPN_TCP_DIAG=1`.
- Teach `scripts/knife14b-lowrtt-probe.sh` to parse:
  - `tcp-relay-write-half-closed ... reason=local_finish`;
  - `tcp-relay-live` / `tcp-relay-close` post-finish remote counters;
  - a concise `relay_late_remote` summary line.
- Add `late_remote_after_local_finish` to attribution labels when post-finish
  remote bytes are non-zero and no stronger pressure/loss label explains the
  run.
- Include `tcp-relay-write-half-closed`, `tcp-relay-close`, and
  `tcp-handle-close` in `METRIC_RE` so post-run report tails preserve the
  relevant lifecycle evidence.

## Acceptance

- `bash -n scripts/knife14b-lowrtt-probe.sh` passes.
- `bash scripts/knife14b-lowrtt-probe.sh --self-test` passes and covers the new
  late-remote attribution label.
- Focused Rust tests for relay diagnostics pass.
- `git diff --check` passes.
- Stage review finds no behavioral relay changes and no secret exposure.
