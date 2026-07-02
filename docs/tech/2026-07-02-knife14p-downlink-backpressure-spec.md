# Knife14p spec - bound downlink backlog with relay backpressure

Date: 2026-07-02

## Grounding

The `4f34f1d` live run at
`/tmp/mvpn_knife14o_usclient_suite_20260702_135529.tar.gz` completed, and the
VPS preflight was clean:

- `.33` route/ping passed.
- `.77:5201` direct iperf passed.
- the suite status was `COMPLETED`.
- `dead_slot_reap` dropped to one event and no `state=Closing pending>0` reap
  remained.

Knife14o therefore fixed the narrow deferred-close sweep race, but exposed a
larger downlink scheduling problem:

- `tcp-global-rx-pressure` occurred 1612 times;
- several relay closes had hundreds of MB in `downlink_pending`;
- examples include `pending=465461533`, `pending=443527919`,
  `pending=295343570`, and `pending=205485767`;
- the same flows accepted only about 6-11 MB into smoltcp before close;
- P1/P2/P4/P8 forward collapsed to roughly `2.44`, `2.17`, `1.68`, and
  `1.68 Mbits/sec` receiver.

The main loop currently keeps consuming `global_rx` data and appending it to
`SocketCtx.downlink_pending` even when smoltcp cannot accept more bytes. That
turns the relay channel into an unbounded userspace buffer and prevents natural
TCP/QUIC backpressure from reaching the upstream reader.

## Problem

`downlink_pending` must be a short retry buffer for bytes that did not fit in
the current smoltcp TX window. It must not grow into hundreds of MB. When it
does, the loop spends its time accepting more remote payload and flushing a
window that cannot advance fast enough, while TUN-side ACK/window-update
processing loses priority.

## Design

Add explicit downlink backpressure in the event loop:

- compute downlink pending stats across dirty socket contexts before each
  `tokio::select!` so the hot path does not scan every listener;
- when the largest pending backlog reaches a high watermark, pause the
  `global_rx.recv()` branch;
- while paused, keep timer/TUN ingress/dirty flushing active so smoltcp can
  consume ACKs and drain pending bytes;
- resume `global_rx` only after the largest pending backlog drops below a low
  watermark;
- use hysteresis so the branch does not flap on every packet;
- expose the watermarks via env vars so different VPS/RTT deployments can tune
  without code changes.

This preserves data instead of dropping payloads. Backpressure is applied by
stopping consumption from the relay mpsc channel; relay tasks then block on
`send`, stop reading from the upstream stream, and the upstream transport sees
real flow-control pressure.

## Defaults

The default high watermark is `32 * TCP_SOCKET_BUFFER_SIZE` (about 2 MiB). The
default low watermark is `8 * TCP_SOCKET_BUFFER_SIZE` (about 512 KiB).

Env overrides:

- `MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES`
- `MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES`

Invalid values fall back to defaults. A low value greater than or equal to the
high watermark also falls back to the default low watermark.

## Non-Goals

- No data drop policy.
- No per-flow relay channels in this stage.
- No QUIC congestion-control change.
- No change to TUIC TCP pool selection.

## Acceptance

- Unit tests cover high/low hysteresis.
- Unit tests cover env parsing defaults and invalid values.
- Existing relay/deferred-close tests still pass.
- Full `cargo test` and clippy pass.
- Next live run should show sharply lower `downlink_pending_high` and
  `tcp-global-rx-pressure`, with no return of `state=Closing pending>0` reaps.
