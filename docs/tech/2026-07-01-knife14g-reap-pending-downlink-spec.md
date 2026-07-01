# Knife14g spec - do not reap pending downlink

> Date: 2026-07-01 | Branch: `codex/knife14d-downlink-reap-open`
> Input bundle: `/tmp/mvpn_knife14f_usclient_suite_20260701_153420.tar.gz`.
> Companion plan: `docs/tech/2026-07-01-knife14g-reap-pending-downlink-plan.md`.

## Grounding

Knife14f changed the failure mode:

- Full P1/P2/P4/P8 no longer hit the harness timeout; every iperf command exits `0`.
- The relay write timeout fires and closes stuck local-to-remote writers.
- Throughput still collapses at higher parallelism: forward P4 receiver is about `2.23 Mbit/s`, forward P8
  receiver is about `2.15 Mbit/s`.
- Final gauges still show `TCP relay 活跃=9/累计=42`.

Client diagnostics now show the next byte-preservation bug:

- `dead_slot_reap` and `uplink_channel_closed` rearm slots with large `downlink_pending`, for example
  `pending=6199714`, `pending=4048438`, and several `pending=1-2MB` cases.
- `tcp-loop-flush-tx failures=0`, so the main loop is not failing to flush. The tail is being discarded by
  rearm while still pending.

## Problem

`should_reap_slot` treats `TcpState::CloseWait` as immediately reapable. In a half-closed TCP flow, the local
application can stop sending while it is still allowed to receive response bytes. If `SocketCtx.downlink_pending`
is non-empty and the smoltcp socket is still active, reaping aborts the socket and clears the buffered downlink
tail. This defeats knife14c/14e's "never drop accepted downlink bytes" invariant.

## Design

Keep the existing hard reap for truly inactive sockets. For active sockets, do not reap a slot just because it
is in `CloseWait` while `downlink_pending` is non-empty. Let the dirty-set flush path keep driving
`flush_downlink`; once pending drains, the existing CloseWait reap can reclaim the slot.

## Non-Goals

- No TUIC connection pool.
- No congestion-control change.
- No change to relay write timeout.
- No attempt to deliver pending bytes after the socket is no longer active.

## Acceptance

- A focused unit test proves active `CloseWait + downlink_pending` is not reaped.
- The same test proves `CloseWait` reaps again once pending is empty.
- Existing dead-slot and relay lifecycle tests still pass.
- Full library tests and clippy pass.
