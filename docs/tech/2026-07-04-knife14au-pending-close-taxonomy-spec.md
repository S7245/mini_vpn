# Knife14au spec - pending-at-close taxonomy

Date: 2026-07-04

## Grounding

Knife14at showed that the reverse-first P1 window can reach high throughput
while terminal pending reap remains zero:

- reverse-first P1 sender/receiver: `185/183 Mbit/s`;
- `terminal_pending_reap: events=0 bytes=0`;
- no clean-window QUIC loss/congestion, local write pressure, send-slice error,
  or TUN flush failure;
- runtime TUN egress drops were observable but not sufficient to explain the
  previous low reverse result.

However, raw close logs in the same high-throughput run still contained
`pending>0` with `tcp_state=Established active=true can_send=false`. The current
summary only reports terminal pending, so non-terminal close tails can disappear
from the acceptance report.

## Problem

`terminal_pending_reap_bytes=0` proves only that no close matched the existing
`Closed && inactive && !can_send` terminal-reap definition. It does not prove
that close-time pending was absent. For Knife14as/14au, this leaves a reporting
gap:

- terminal pending is counted;
- active send-capable pending is implicitly non-terminal;
- active no-send pending is visible only in raw logs;
- no summary distinguishes expected close cleanup from a close happening while
  downlink bytes are still blocked by local socket send capacity.

## Goal

Make every `tcp-handle-close` pending tail classifiable and summarizable without
changing TCP data-plane behavior.

The close diagnostic must expose:

- `close_pending_class`;
- `close_pending_bytes`;
- the existing `terminal_pending_reap_bytes`;
- the existing socket snapshot fields.

The probe summary must report:

- all pending-at-close events, bytes, and max bytes;
- terminal pending events and bytes;
- active no-send pending events and bytes;
- send-capable pending events and bytes;
- inactive no-send and unknown pending events and bytes.

## Non-Goals

- Do not change close-drain timing, reap predicates, downlink pacing, TUN queue
  length, TUIC pool behavior, QUIC configuration, iperf3, or sing-box.
- Do not treat active no-send pending as terminal data loss by itself. It is an
  explicit lifecycle signal to correlate with throughput, backpressure, and
  local close timing.
- Do not store secrets, TUIC credentials, sudo passwords, private keys, or raw
  credential-bearing logs in docs or learning memory.

## Taxonomy

- `none`: no close-time pending bytes.
- `terminal_closed_no_send`: `pending>0`, `tcp_state=Closed`, `active=false`,
  and `can_send=false`; this preserves the Knife14as terminal definition.
- `active_no_send`: `pending>0`, `active=true`, and `can_send=false`; this is
  not terminal, but it means the close happened while the local TCP socket could
  not currently accept downlink bytes.
- `active_send_capable`: `pending>0`, `active=true`, and `can_send=true`.
- `inactive_send_capable`: `pending>0`, `active=false`, and `can_send=true`.
- `inactive_no_send`: `pending>0`, `active=false`, `can_send=false`, and the
  state is not the terminal `Closed` case.
- `unknown`: pending exists but the parser lacks enough snapshot fields to
  classify it.

## Acceptance

Local:

- Rust tests cover the close pending taxonomy and keep terminal byte accounting
  compatible.
- Low-RTT probe self-test covers explicit terminal pending plus active no-send
  pending.
- `cargo test --lib client_tun`
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`

VPS:

- Run a scoped reverse-first P1 acceptance after local gates pass.
- The report must include both `terminal_pending_reap` and
  `pending_at_close`.
- If reverse throughput falls back into the `10-20 Mbit/s` band, the report must
  show whether close-time pending was terminal, active no-send, send-capable,
  inactive no-send, or unknown.
