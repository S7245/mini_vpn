# Knife14az spec - local TCP socket policy

Date: 2026-07-04

## Grounding

Knife14ay showed that clean reverse-first low throughput is no longer explained
by stale TUIC pool slots, iperf3, sing-box, egress pacing, clean-window QUIC
loss/congestion, TUN syscall failure, TUN qdisc drops, or close-drain accounting.

The decisive signal was a local smoltcp TCP transition from `Established` to
`Closed` inside `dirty_relay` while app-owned pending was still `0`. The later
`terminal_pending_reap` bytes appeared after that local close edge and were not
deliverable through the already closed socket.

The remaining clean-window limiter is local downlink drain: smoltcp accepted
about `30.8 MiB` over about `2945` flush attempts, `send_queue` reached about
`522 KiB`, and tx-queue backpressure repeatedly paused and resumed remote reads.

## Goal

Apply and verify an explicit local virtual-link TCP socket policy for mini_vpn
downlink sockets so the local smoltcp endpoint does not add avoidable
Nagle/delayed-ACK latency to high-throughput reverse traffic.

This stage must prove the policy is applied consistently when a listener socket
is first built and when it is rearmed after a completed connection.

## Non-goals

- Do not change TUIC TCP pool behavior.
- Do not tune sing-box, iperf3, VPS service config, or server congestion
  control.
- Do not change egress pacing, TUN queue length, pool size, or existing
  backpressure watermarks.
- Do not reinterpret terminal pending as pre-close deliverable data.
- Do not add secrets, host passwords, UUIDs, keys, or one-off noisy logs to docs
  or learning memory.

## Invariants

- Socket buffer sizes remain unchanged.
- Backpressure accounting and terminal pending accounting remain observable.
- Listener creation and rearm must use the same local TCP policy.
- A rearmed socket must return to listening state and keep existing fake-IP
  release semantics.
- Local tests must fail if a future edit accidentally restores smoltcp defaults
  for the local virtual-link sockets.

## Acceptance

Local acceptance:

- A focused listener-construction test proves local TCP sockets disable Nagle and
  delayed ACK.
- A focused rearm test proves rearmed sockets restore the same policy before
  returning to listening state.
- `cargo test --lib client_tun` passes.
- Existing Knife14 parser/suite self-tests still pass.
- `git diff --check` is clean.

VPS acceptance:

- Run one scoped `.27` reverse-first P1 acceptance on the patched commit.
- Direct `.27 -> .77` baseline remains healthy enough to trust the run.
- Clean reverse-first P1 escapes the prior `10-20 Mbit/s` band, or the bundle
  gives a new explicit limiter.
- Parse and record `tcp_lifecycle`, downlink backpressure, terminal pending,
  QUIC loss/congestion, TUN drops, and tx-queue high-water signals.

