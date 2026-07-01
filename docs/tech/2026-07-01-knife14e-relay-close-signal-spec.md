# Knife14e spec — relay close signal and bounded probes

> Date: 2026-07-01 | Branch: `codex/knife14d-downlink-reap-open`
> Input bundle: `/private/tmp/mvpn_knife14d_usclient_suite_20260701_132036.tar.gz`.
> Companion plan: `docs/tech/2026-07-01-knife14e-relay-close-signal-plan.md`.

## Grounding

The 2026-07-01 13:20 US-client run proves the new suite preflight works:

- Exit VPS `43.153.32.33:8443` is reachable, and mini_vpn completes the TUIC handshake.
- Target VPS `43.130.32.77:5201` passes direct iperf before the TUN route is installed.
- The full sweep starts from a quiet tunnel: `TCP relay 活跃=0`, `fake-IP 活跃=0`.

The tunnel still fails:

- P1 forward is only about `2 Mbit/s` receiver.
- P1 reverse is nearly stalled at `4.52 Kbit/s` receiver.
- Full P2 forward ends with `iperf3: error - unable to receive results`.
- After P2, the client log keeps reporting `TCP relay 活跃=2/累计=9` while the main loop is idle:
  `loop-active=0.0%`, `poll=0.0%`, `tcp-loop-flush-tx failures=0`.
- The relay task logs `remote_write_failed` and `handshake_failed`, but a non-EOF relay exit does not send a
  close/rearm event back to the main loop.

## Problem

`run_relay` only notifies the main loop on remote EOF by sending an empty payload. Other terminal conditions
such as `remote_write_failed`, `remote_read_failed`, `local_channel_closed`, and `idle_timeout` only log and
shutdown the stream. The main loop can therefore keep a socket context in `Relaying` until smoltcp itself
becomes inactive or `CloseWait`. In this run, that left stale active relays visible for minutes and allowed
later probes to run against polluted relay state.

The probe script has a separate harness issue: `iperf3` commands are not bounded by an external timeout, so
a stuck P4 can leave the suite report cut off after the command line.

## Design

Replace the TCP relay return channel payload with an explicit event, and carry the socket `conn_epoch` on
every event:

- `Data { epoch, bytes }` for remote bytes.
- `Closed { epoch, direction, reason }` for relay-task termination.

The relay task must send one `Closed` event after every terminal reason, preserving FIFO order after any
previous `Data` events sent by that same task. The main loop must ignore stale events whose epoch does not
match the current socket context. It re-arms the handle on a matching `Closed` unless the slot has already
returned to `Listening`. If the matching slot still has `downlink_pending`, the close is deferred until those
bytes are flushed so the close signal does not silently discard an already accepted remote tail.

The US-client suite keeps its default VPS service preflight before the tunnel starts: exit VPS reachability
and target VPS direct iperf are checked before the target route is moved into the TUN. The low-RTT probe also
wraps each `iperf3` command in `timeout`, with an env override, so future bundles end with a bounded failure
instead of a hanging command.

## Non-Goals

- No TUIC connection pool in this stage.
- No congestion-control change.
- No rewrite of smoltcp buffering or downlink pending semantics.
- No change to TUIC open timeout.

## Acceptance

- Focused unit tests prove relay idle/terminal close emits a close event.
- Focused unit test proves a stale close event cannot rearm a newer epoch on the same handle.
- Focused unit test proves a matching close with `downlink_pending` is deferred until pending bytes drain.
- Existing async-open/reap tests still pass.
- `bash -n` passes for both suite scripts.
- Next US-client run either completes the full probe or fails with a bounded timeout and a final log snapshot.
