# Knife14ae TCP Pool Stale Reconnect Spec

## Grounding

`ebd3567` was tested from `.27` with
`/tmp/mini_vpn/mvpn_knife14ae3_usclient_suite_20260703_212412.tar.gz`.

The new Exit-to-Target preflight succeeded:

- Client `.27 -> .77`: 283 Mbit/s receiver
- Target `.77 -> .27`: 280 Mbit/s receiver
- Exit `.33 -> .77`: 281 Mbit/s receiver
- Target `.77 -> .33`: 275 Mbit/s receiver

After restarting sing-box on `.33`, TUIC startup recovered. Tunnel P1 was mostly
healthy:

- reverse-first P1: 187 Mbit/s receiver
- standalone forward P1: 155 Mbit/s receiver
- standalone reverse P1: 155 Mbit/s receiver
- full forward P1: 187 Mbit/s receiver

The remaining failure was the final full reverse P1. It opened TCP over pool
connections `conn=0` and `conn=1`, but `conn=1` was already `closed=TimedOut`.
The iperf reverse run transferred 0 bytes and timed out.

## Goal

Avoid giving a new TCP relay a stale TUIC TCP pool connection that is likely to
close immediately after `open_bi`.

## Non-Goals

- Do not change smoltcp relay lifecycle.
- Do not change downlink backpressure thresholds.
- Do not change TUIC authentication or UDP behavior.
- Do not remove the TCP connection pool.

## Design

Track the last TCP use time and active relay count per TUIC pool slot. When
`open_tcp` selects a non-primary pool slot, if the slot has no active/opening
relay and has not carried TCP for a conservative stale window, close and
reconnect that slot before opening the new TUIC Connect stream.

The primary connection keeps its existing UDP/health behavior; the stale
reconnect rule is skipped for pool index `0`.

An active/opening relay owns a lightweight slot lease until the returned stream
is dropped. This prevents one long or concurrent TCP flow from being cut by a
later `open_tcp` that happens to choose the same pool slot. The lease uses an
atomic `0 -> 1` reservation; only the caller that exclusively reserved an idle
slot may perform stale reconnect.

## Acceptance

- Unit test covers stale threshold boundaries.
- `cargo check` passes.
- In the next VPS run, the final full reverse P1 should not fail because a
  selected TCP pool connection is already `closed=TimedOut`.
- The report should show `tuic-tcp-pool-reconnect ... reason=stale_tcp_pool_slot`
  when a stale pool slot is refreshed.
