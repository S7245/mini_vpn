# Knife14ay plan - local TCP Closed transition diagnostics

Date: 2026-07-04

## Stage Goal

Explain the pre-terminal edge for clean reverse-first failures by making local
TCP state transitions visible.

## Design Tree

1. Tune tx-queue backpressure again.
   Rejected. Knife14ax showed the tx-queue pressure fields stayed zero in clean
   reverse-first.

2. Preserve terminal pending longer.
   Rejected. The observed socket is `Closed && !can_send`; smoltcp cannot
   deliver those bytes.

3. Add behavior-neutral lifecycle transition diagnostics.
   Selected. The missing evidence is how the socket reached `Closed`, not how
   to drain after `Closed`.

4. Change local FIN/defer behavior immediately.
   Deferred. Knife14v already added a remote-progress defer. Knife14ay first
   needs to prove whether the current failure still follows that path or a
   different local TCP close edge.

## Tasks

1. Add a red Rust test for a lifecycle transition diagnostic carrying previous
   state and terminal context.
2. Implement per-handle lifecycle observations and reset them on rearm.
3. Observe transition points in remote payload handling, dirty relay processing,
   relay close handling, and dead-slot reap.
4. Extend low-RTT probe parsing and self-test with `tcp_lifecycle` summary.
5. Run local regression gates.
6. Record stage learning, commit, push.
7. Run one scoped reverse-first VPS acceptance and parse the bundle before any
   behavior change.

## Regression Checks

- Diagnostic only: no close/reap/backpressure behavior changes.
- `tcp-lifecycle-transition` lines do not emit every tick.
- Parser remains compatible with older logs that have no lifecycle lines.
- `last_tcp_lifecycle` is cleared on rearm so new flow generations do not
  inherit stale state.
