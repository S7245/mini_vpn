# Knife14f spec - bound relay upstream writes

> Date: 2026-07-01 | Branch: `codex/knife14d-downlink-reap-open`
> Input bundle: `/tmp/mvpn_knife14e_usclient_suite_20260701_142546.tar.gz`.
> Companion plan: `docs/tech/2026-07-01-knife14f-relay-write-timeout-plan.md`.

## Grounding

Knife14e improved the lifecycle surface:

- VPS service preflight passed before the tunnel route was installed.
- Standalone P1 no longer collapsed to kilobits: forward receiver was `19.4 Mbit/s`, reverse receiver was
  `20.1 Mbit/s`.
- Full P1 reached `50.6 Mbit/s` receiver.
- The full sweep no longer hangs the harness forever: P2/P4/P8 end with bounded `exit=124`.

The remaining failure is different from knife14e:

- Full P2 forward keeps sending for about 50s, then the iperf command is killed by `timeout`.
- Client diagnostics show `TCP relay 活跃=2/累计=9` for minutes while the main loop is parked:
  `loop-active=0.0%`, `poll=0.0%`, `relay=0.0%`.
- The relays that do close now report `tcp-relay-close` and `tcp-handle-close`, proving the close event path works.
- The two remaining active relays do not report close at all, which means their relay tasks are still inside an
  awaited operation and never return to the `select!` idle branch.

## Problem

`run_relay` awaits `stream.write_all(&payload)` inside the `local_msg` select branch. If the TUIC send stream
or underlying QUIC connection stops making write progress, that await can park the relay task indefinitely.
While parked there:

- the 90s relay idle timeout cannot fire because the outer `select!` is not running;
- no `Closed` event is sent to the main loop;
- the socket context can remain counted as active `Relaying`, polluting later probes.

## Design

Wrap each local-to-remote `write_all` in a relay-local timeout. On timeout, terminate the relay with:

- `direction = "local_to_remote"`
- `reason = "remote_write_timeout"`

The existing knife14e close event path will then rearm the socket in the main loop. The timeout is per payload
accepted from smoltcp, not a total flow duration limit.

## Non-Goals

- No TUIC connection pool.
- No congestion-control change.
- No change to `open_tcp` timeout.
- No rewrite of smoltcp backpressure.

## Acceptance

- A focused test proves a permanently pending upstream write emits `Closed(remote_write_timeout)`.
- Existing relay close tests still pass.
- Full library tests and clippy pass.
- Next US-client bundle should no longer leave `TCP relay 活跃>0` indefinitely after timeout-killed P2.
