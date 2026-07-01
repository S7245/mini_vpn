# Knife14j spec - propagate local finish without closing relay

> Date: 2026-07-01 | Branch: `codex/knife14d-downlink-reap-open`
> Input bundle: `/tmp/mvpn_knife14h_usclient_suite_20260701_185153.tar.gz`.
> Companion plan: `docs/tech/2026-07-01-knife14j-local-finish-relay-plan.md`.

## Grounding

Knife14i removes the premature `dead_slot_reap` from the latest smoke run:

- `.33` TUIC and `.77:5201` preflight both pass before the tunnel test starts.
- Forward P1 exits `0`.
- Reverse P1 exits `0` and improves versus knife14h, but still delivers low throughput.
- The full sweep is skipped because the quiet wait sees one active relay for 20 seconds:
  `TCP relay 活跃=1/累计=4`.

The accept log has no `dead_slot_reap` lines. The remaining active relay is therefore not being killed too early;
it is staying alive too long after the local side has already finished sending.

## Problem

After knife14i, `CloseWait + live relay` is intentionally preserved so reverse/downlink bytes are not lost. But the
relay writer never receives an explicit "local send side is finished" signal.

Dropping `uplink_tx` is not a valid fix: the current relay treats channel close as terminal, which also shuts down
remote reads and would reintroduce reverse data loss. Keeping `uplink_tx` forever leaves the remote write half open
until relay idle timeout, which is longer than the suite quiet window.

## Design

Split relay uplink messages into explicit commands:

- `Data(Vec<u8>)`: existing local-to-remote payload.
- `Finish`: local TCP receive side reached `CloseWait`, so the relay should `shutdown()` only the remote write half.

The writer half reports `WriteHalfClosed` to the parent relay task. This is non-terminal:

- parent relay keeps reading remote-to-local bytes;
- writer signal polling is disabled after the write half finishes, avoiding a ready-loop on a closed signal channel;
- true writer failures and dropped uplink channels remain terminal.

The socket context records whether `Finish` has already been sent, so the main loop sends it at most once per flow.
The flag resets on rearm.

## Non-Goals

- No relay idle-timeout tuning.
- No TUIC connection-pool change.
- No change to downlink buffering or fake-IP ownership.
- No change to the US-client test script behavior.

## Acceptance

- A focused relay test proves `Finish` calls remote write shutdown while remote reads still reach the main loop.
- Existing relay idle, write failure, write timeout, and concurrent read/write tests still pass.
- Reap tests still preserve `CloseWait + live relay`.
- Full library tests, clippy, and `git diff --check` pass.
