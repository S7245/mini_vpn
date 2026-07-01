# Knife14h spec - concurrent relay pump

> Date: 2026-07-01 | Branch: `codex/knife14d-downlink-reap-open`
> Input bundle: `/tmp/mvpn_knife14g_usclient_suite_20260701_155854.tar.gz`.
> Companion plan: `docs/tech/2026-07-01-knife14h-concurrent-relay-pump-plan.md`.

## Grounding

Knife14g shows the VPS services are healthy before the tunnel starts:

- Exit VPS `43.153.32.33:8443` is reachable.
- Target VPS `43.130.32.77:5201` passes direct iperf3.
- mini_vpn completes the TUIC handshake.

The remaining failure is in the TCP relay pump:

- Full forward P2 and P4 still hit the harness timeout (`exit=124`), with receiver summary at `0.00 bits/sec`.
- The client log repeatedly reports `local_to_remote` `remote_write_timeout` after successful uplink writes.
- Reverse tests improve, but later `dead_slot_reap state=Relaying pending=...` appears only after the smoltcp socket
  is already inactive; those bytes are not safely deliverable by preserving the slot longer.

## Problem

`run_relay` uses one task for both directions. When the `local_msg` branch receives a payload, it awaits
`stream.write_all(&payload)` inside that branch. During that await the same task cannot poll the remote-to-local
read side.

On a full-duplex TUIC stream this can deadlock under flow control:

- the peer may need us to read a small control/response payload before it keeps reading our upload;
- while we are stuck in `write_all`, that remote payload is not read;
- the peer stops granting more send credit, and the local write eventually hits `remote_write_timeout`.

## Design

Split the relay stream into independent read and write halves:

- a local-to-remote writer task owns the write half and the existing uplink channel;
- the parent relay task owns the read half and keeps forwarding remote bytes to the main loop;
- writer progress and terminal events are reported back to the parent so the existing lifecycle log and
  `Closed` event remain single-source.

Keep the knife14f per-payload write timeout, but make it local to the writer half. A stuck write no longer blocks
remote reads.

## Non-Goals

- No TUIC connection pool.
- No congestion-control change.
- No change to the preflight scripts.
- No attempt to deliver pending downlink after smoltcp has entered `Closed`/`TimeWait`.

## Acceptance

- A focused relay test proves remote reads still reach the main loop while a local write is pending.
- Existing write failure, write timeout, idle timeout, and close-event tests still pass.
- Full library tests and clippy pass.
- Next US-client bundle should reduce `remote_write_timeout` during forward P2/P4 and avoid timeout-killed iperf.
