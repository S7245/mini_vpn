# Knife14ca Headroom-Bounded Remote Accept Spec

Date: 2026-07-06

## Grounding

Knife14bz rejected static midpoint egress flush tuning. The configured
`tx_queue_flush_high=720896B` was active, but remote-read bursts still pushed
the smoltcp send queue to `917489-983022B`, produced
`tun_tx_dropped_delta=340`, and regressed reverse-first P1 to
`20.7/19.5 Mbit/s`.

That evidence means the next fix must move earlier than the post-send immediate
flush decision. The code should bound how many bytes enter the smoltcp tx queue
in one pass based on the remaining clean local egress headroom.

## Stage Goal

Prevent a remote payload or pending downlink flush from overshooting the local
TUN egress clean ceiling by limiting each `send_slice` attempt to the remaining
tx_queue headroom.

## Non-goals

- Do not add another ad hoc threshold or tune VPS/script values.
- Do not change TUIC, sing-box, iperf3, stale pool, TUN queue length, or egress
  pacer env defaults.
- Do not weaken app-owned pending safety, terminal pending accounting,
  close-drain/reap accounting, or stale relay epoch checks.
- Do not drop remote payload bytes that cannot be accepted immediately.

## Invariants

- Remote payload bytes that cannot fit within the current clean egress headroom
  remain in `SocketCtx.downlink_pending`.
- `send_slice` must not be called with more bytes than:
  - configured per-flush budget;
  - pending backlog length;
  - current smoltcp send capacity; and
  - remaining clean egress headroom.
- If the local send queue is already at or above the clean egress ceiling,
  `flush_downlink` returns `0`, leaves pending intact, and lets dirty backpressure
  plus TUN egress drain make progress.
- New accounting must expose headroom-limited flushes so VPS parsing can
  distinguish intentional bounded accept from hidden pending loss.

## Acceptance

Local:

- RED focused test fails before implementation and passes after.
- Existing pending, close/reap, backpressure, pacer, TUN feedback, and harness
  tests stay green.
- `cargo test --lib`, script syntax/self-tests, harness tests, release build,
  clippy with harness, and `git diff --check` pass.

VPS:

- Re-run the same `.27 -> .33 -> .77` reverse-first P1 suite.
- Require throughput to beat Knife14by receiver baseline (`29.0 Mbit/s`) or
  produce a clearly better stop/go shape with no new hidden-loss signal.
- Require bounded or zero TUN egress drops, clean QUIC loss/congestion/blocking,
  and clean pending/close/reap accounting.
