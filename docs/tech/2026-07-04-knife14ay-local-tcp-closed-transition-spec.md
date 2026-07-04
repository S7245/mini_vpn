# Knife14ay spec - local TCP Closed transition diagnostics

Date: 2026-07-04

## Grounding

Knife14ax tested commit `58f847d` and rejected tx-queue-aware downlink
backpressure as the clean reverse-first root. The new tx-queue metrics were
visible but stayed at zero, while clean reverse-first P1 failed with
`iperf3: unable to receive results`.

The critical close tail was:

```text
remote_to_global_rx_bytes=221884
pending=41208
reason=dead_slot_reap
tcp_state=Closed active=false can_send=false can_recv=false
```

That means terminal pending is still post-close accounting. The missing evidence
is the transition into `Closed`: whether it follows a local FIN/RST, an upstream
relay event, a dirty-pass socket update, or a dead-slot sweep that only sees the
terminal state after the fact.

## Goal

Make local TCP lifecycle transitions observable before the next behavior change.

## Non-Goals

- Do not change close/reap predicates.
- Do not change downlink backpressure or egress pacing behavior.
- Do not change TUIC pool, iperf3, sing-box, TUN queue length, or QUIC config.
- Do not store secrets or credential-bearing logs.

## Invariants

- Lifecycle diagnostics are per-handle and reset on rearm.
- A lifecycle line is emitted only when TCP state or send/receive capability
  changes, not on every metrics tick.
- The diagnostic includes enough context to classify a terminal close edge:
  previous source/state, current state, send/recv capability, pending bytes,
  remote-to-local bytes, local FIN fields, and whether the edge is a terminal
  pending candidate.
- Existing terminal pending and pending-at-close parsing remains compatible.

## Acceptance

Local:

- focused Rust test proves the lifecycle transition line reports previous and
  current socket state plus pending/remote/local-FIN context;
- low-RTT probe self-test parses `tcp_lifecycle` summary fields;
- existing `client_tun` lifecycle/downlink tests still pass;
- script syntax checks, `cargo test`, harness tests, clippy, and `git diff
  --check` pass.

VPS:

- scoped `.27` reverse-first P1 run from the pushed commit;
- report includes raw `tcp-lifecycle-transition` lines and `tcp_lifecycle`
  attribution summary;
- if reverse still fails, the report should identify the source immediately
  before `Closed` instead of hiding behind terminal pending alone.
