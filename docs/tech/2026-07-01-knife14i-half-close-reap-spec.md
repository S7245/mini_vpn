# Knife14i spec - preserve live half-closed relay

> Date: 2026-07-01 | Branch: `codex/knife14d-downlink-reap-open`
> Input bundle: `/tmp/mvpn_knife14h_usclient_suite_20260701_182235.tar.gz`.
> Companion plan: `docs/tech/2026-07-01-knife14i-half-close-reap-plan.md`.

## Grounding

Knife14h confirms the `.33` and `.77` VPS preflight checks are healthy and the forward sweep no longer times out.
The dominant remaining failure is reverse TCP:

- Reverse P1/P2 exit `0` but deliver only tens of KB.
- Reverse P4/P8 hit the harness timeout with active relays stuck.
- The client log contains `dead_slot_reap state=Relaying` after reverse runs, including several `pending=0`
  and several non-zero pending cases.

Knife14g preserved `CloseWait + downlink_pending`, but it left a second half-close branch exposed:

- in reverse mode the local app can finish its send half after a small control/request payload;
- the remote side may still be expected to send data later on the same relay;
- during that gap `downlink_pending` is legitimately empty.

## Problem

`should_reap_slot` still treats active `TcpState::CloseWait` with empty `downlink_pending` as reapable. That is too
strong when the slot still has a live relay channel (`uplink_tx.is_some()`).

For a reverse flow, this can abort the smoltcp socket and drop the relay before the remote-to-local side has had a
chance to deliver data.

## Design

Keep these existing safety properties:

- idle `Listening` slots are never reaped;
- truly inactive sockets still reap, because pending bytes can no longer be delivered;
- stuck inline `OpeningRemote` with no `uplink_tx` still reaps.

Change only active `CloseWait`:

- if the relay channel is still installed, keep the slot alive even when pending is empty;
- once the relay channel is gone and there is no pending downlink, `CloseWait` is reapable again.

The live relay task remains bounded by the existing relay idle timeout and close-event path, so this does not create
an unbounded permanent slot leak.

## Non-Goals

- No TUIC connection pool.
- No congestion-control change.
- No relay idle-timeout tuning.
- No change to the US-client test scripts.

## Acceptance

- A focused predicate test proves active `CloseWait + uplink_tx` is not reaped even with empty pending.
- The same area still proves `CloseWait` without a live relay is reapable.
- Existing reap, relay close, and async-open tests still pass.
- Full library tests and clippy pass.
