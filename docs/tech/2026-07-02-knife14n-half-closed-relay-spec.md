# Knife14n spec - bound half-closed relay lifetime

Date: 2026-07-02

## Grounding

The controlled pool=1 run at
`/tmp/mvpn_knife14m_pool1_usclient_suite_20260702_120622.tar.gz` showed that
pool=4 was not the only cause of the low-throughput behavior:

- VPS preflight passed: `.33` was reachable and `.77:5201` direct iperf worked.
- The suite launched with `MINI_VPN_TUIC_TCP_POOL=1`.
- Standalone P1 forward completed but only reached `2.30 Mbits/sec` receiver.
- Standalone P1 reverse completed with only `23.0 Kbits/sec` receiver.
- The full sweep was skipped because one relay stayed active past the 20s quiet window.

The accept log shows the remaining relay pattern:

- `tcp-relay-write-half-closed ... reason=local_finish`
- later `tcp-handle-close ... reason=uplink_channel_closed`

That means the relay writer handled the local finish, then the main loop later saw
the closed uplink channel as a terminal abnormal close while the relay read half
could still be alive.

## Problem

`Finish` intentionally shuts down only the upstream write half so reverse/downlink
bytes can still be read. But after the writer task exits, the main loop still
holds an `uplink_tx` sender whose receiver is gone. Any later dirty processing can
treat that closed channel as `uplink_channel_closed` and abort the socket.

At the same time, if the remote side never sends more data or EOF after local
finish, the parent relay task waits for the normal 90s idle timeout, which is far
longer than the suite quiet window.

## Design

Make the half-closed relay state explicit and bounded:

- Once the main loop has sent `Finish`, a later closed uplink channel is expected,
  not an abnormal close. Clear `uplink_tx` and keep the relay readable.
- Allow remote payloads to be accepted while `local_fin_sent` is true even if
  `uplink_tx` is already gone.
- Preserve active `CloseWait` slots after local finish until the relay close event
  arrives.
- Add a short half-closed idle timeout in the parent relay after
  `WriteHalfClosed(local_finish)`. Remote read progress keeps extending this
  shorter window; no progress closes the relay before the suite quiet window.

## Non-Goals

- No TUIC TCP pool change.
- No congestion-control change.
- No suite behavior change.
- No broad smoltcp lifecycle rewrite.

## Acceptance

- A relay test proves `Finish` with no remote progress closes on the short
  half-closed idle timeout, not the 90s full idle timeout.
- A predicate test proves active `CloseWait` after local finish is not reaped just
  because `uplink_tx` is gone.
- A helper test proves remote payloads are still accepted in the post-Finish
  read-only relay state.
- Existing relay close, write timeout, local finish, and reap tests still pass.
