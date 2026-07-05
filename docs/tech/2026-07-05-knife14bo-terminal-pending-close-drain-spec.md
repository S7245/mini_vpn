# Knife14bo Terminal Pending Close-Drain Spec

Date: 2026-07-05

## Context

Knife14bn A/B evidence kept `3d06bea` as the current base. It showed that the
recent TUN-drop pressure latch is active, but the better leg still closed with
large terminal pending:

- tunnel reverse P1: `19.3/18.2 Mbit/s`;
- `tcp-downlink-backpressure`: `6/6`;
- TUN feedback: `pause/resume=1/1`, drops `1272`;
- `terminal_pending_reap`: `809715B`;
- QUIC loss/congestion: `0/0`.

The remaining branch is local TCP downlink lifecycle. The unsafe ambiguity is
that terminal pending currently proves bytes were dropped at rearm, but it does
not prove whether they were:

1. already queued before the local socket became terminal and simply failed to
   drain; or
2. accepted from the relay after the local socket was already `Closed` and
   unable to send.

## Stage Goal

Knife14bo must make terminal pending causal:

- preserve read-only remote payload after local FIN while the local socket is
  still send-capable or active;
- reject and account remote payload that arrives after the local socket is
  terminal no-send;
- keep the existing bounded reap rule for true `Closed && !active && !can_send`
  sockets;
- make close logs separate terminal pending backlog from terminal-late remote
  bytes.

## Non-Goals

- No egress pacer tuning.
- No TUN queue-length or TUN feedback retuning.
- No stale pool, sing-box, iperf3, or QUIC congestion changes.
- No attempt to resurrect a smoltcp socket once it is terminal no-send.

## Invariants

- A socket in `CloseWait` after local FIN can still receive reverse/downlink
  bytes until the bounded local-finish defer logic decides to finish.
- `Closed && !active && !can_send` pending remains undeliverable and reapable.
- Terminal no-send remote payload must not grow `downlink_pending`.
- Dropped terminal-late payload must be visible in per-flow close diagnostics.
- Deferred close-drain remains bounded by the existing grace rules.

## Acceptance

Local acceptance:

- focused tests prove remote payload is allowed after local FIN when the socket
  is still active/send-capable;
- focused tests prove remote payload is rejected and counted when the socket is
  terminal no-send;
- focused accounting tests prove close logs distinguish terminal backlog from
  terminal-late payload;
- focused `cargo test --lib` filters pass.

VPS acceptance after commit:

- run one scoped reverse-first P1 suite from `.27` against `.33`/`.77`;
- parse the bundle and report tunnel reverse throughput, direct baselines,
  TUN/downlink pressure, TUN drops, terminal pending, terminal-late payload,
  close pending classes, and QUIC loss/congestion;
- Knife14 remains open unless reverse-first P1 clearly exits the `10-20 Mbit/s`
  band with no hidden pending/close/reap loss point.
