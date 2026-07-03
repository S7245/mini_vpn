# Knife14ai spec - reverse sender backpressure attribution

## Grounding

- Latest branch head before this stage: `c2fa2e8`.
- Latest VPS bundle:
  `/tmp/mini_vpn/mvpn_knife14ah_usclient_suite_20260703_231523.tar.gz`.
- Direct preflights in that bundle were healthy:
  - Client `.27 -> .77`: 257 Mbit/s receiver.
  - Target `.77 -> .27`: 264 Mbit/s receiver.
  - Exit `.33 -> .77`: 283 Mbit/s receiver.
  - Target `.77 -> .33`: 269 Mbit/s receiver.
- Tunnel reverse-first P1 delivered only 88.2 KBytes / 24.1 Kbit/s receiver.
- Client-side diagnostics did not show local write pressure, global_rx pressure,
  downlink backpressure, QUIC loss/congestion, QUIC blocked-frame deltas, or
  stale pool reconnects.
- Client relay accepted 186,936 remote bytes into smoltcp with `pending=0`.
- `.77` iperf3 journal showed the reverse sender itself only sent 3.00 MBytes
  in the first second, then 29 seconds of 0 bytes.

## Prior Similar Failures

Similar reverse-low-throughput shapes appeared before, but the active branch has
changed:

- Knife14ag showed reverse-first silence with `no_pressure_signal` and late
  remote bytes after local `Finish`.
- Earlier reverse/downlink failures involved stale TCP pool slots,
  `tcp-downlink-backpressure`, or pending/reap behavior.
- Knife14ah is narrower: the target sender is backpressured through the
  TUIC/sing-box path while the client local loop does not show pressure.

## Problem

`scripts/knife14b-lowrtt-probe.sh` currently summarizes receiver throughput and
client-side pressure/loss signals. When reverse receiver throughput is low and
client-side pressure is absent, the report falls back to `no_pressure_signal`
unless late remote bytes are visible. It does not distinguish the knife14ah
case where the iperf sender is also low, which points upstream to
sing-box/TUIC/server-side stream pressure rather than local downlink queues.

## Goal

Make target-side sender backpressure visible in the low-RTT attribution summary:

- parse and print `iperf_sender_mbps` alongside `iperf_receiver_mbps`;
- identify reverse probes from the iperf output;
- emit `reverse_sender_backpressured` when both sender and receiver throughput
  are low in a reverse TCP run and no stronger client-side pressure/loss signal
  explains the run;
- cover the behavior with `--self-test`.

## Non-Goals

- Do not change Rust data-plane behavior.
- Do not tune TUIC congestion control, TCP pool defaults, socket buffers, or
  downlink backpressure watermarks.
- Do not change sing-box configuration or VPS services.
- Do not continue stale TCP pool slot diagnosis.

## Design

- Add a generic iperf summary parser that can extract sender and receiver Mbps
  from TCP iperf output.
- Keep the existing `iperf_receiver_mbps` output for compatibility and add
  `iperf_sender_mbps`.
- Treat `Reverse mode` in the iperf output as the public signal for reverse TCP
  probes.
- Use a conservative low-throughput threshold for this label. The label is an
  attribution hint, not acceptance policy.
- Add the label only when stronger local/client-side labels are absent:
  local write pressure, global_rx pressure, downlink backpressure, QUIC
  loss/congestion, QUIC flow-control, and stale reconnects remain more direct
  local explanations.

## Acceptance

- `bash -n scripts/knife14b-lowrtt-probe.sh` passes.
- `bash scripts/knife14b-lowrtt-probe.sh --self-test` passes and covers
  `iperf_sender_mbps` plus `reverse_sender_backpressured`.
- `git diff --check` passes.
- Stage review confirms the change is parser-only and does not expose secrets.
