# Knife14l spec - opt-in TUIC TCP connection pool

Date: 2026-07-02

## Grounding

`56d2c85` (`knife14k`) cleaned up the acceptance harness noise:

- The suite preflight passed for both VPSes.
- The iperf sweep no longer hit `server is busy`; every sweep used `iperf_attempt: 1/7`.
- Every forward and reverse `P=1/2/4/8` iperf command exited `0`.
- Full forward `P1` reached `545 MBytes / 152 Mbits/sec` receiver, proving the relay can exceed 100M on this path.
- Forward `P2/P4/P8` collapsed to roughly `2.5 Mbits/sec` receiver aggregate, with several `remote_write_timeout` lines during `P8`.
- Reverse remained usable but non-linear: `P1 14.1M`, `P2 19.0M`, `P4 24.7M`, `P8 11.6M`.

Earlier knives removed closer blockers first: listener rearm, pending downlink preservation, half-close preservation,
concurrent relay pumping, local FIN propagation, and iperf busy retries. That leaves a narrower question: whether multiple
TCP streams sharing one TUIC/QUIC connection are hitting per-connection flow-control, congestion, or scheduling limits.

## Grill Design Tree

```text
What should knife14l do?
+-- Keep debugging relay close/reap?
|   `-- Not first. P1 forward can now hit 152M, so the relay is not globally capped.
+-- Change the default transport topology?
|   `-- No. The evidence supports an experiment, not a production default change.
+-- Add only more logging?
|   `-- Useful, but does not answer whether #3 connection sharing is the ceiling.
`-- Add an opt-in TUIC TCP pool
    `-- Yes: preserve default=1, expose MINI_VPN_TUIC_TCP_POOL, round-robin TCP opens, keep UDP on primary.
```

## Scope

Add a small, explicit TCP-only connection pool to `TuicUpstream`.

- Default pool size remains `1`.
- `MINI_VPN_TUIC_TCP_POOL=N` enables the experiment.
- TCP `open_tcp` round-robins across the pool.
- UDP datagram/uni-stream relay stays on the primary connection.
- Health probe and failover blackhole detection continue to observe the primary connection.
- Each pooled connection reconnects independently when its own `close_reason` is set.

## Non-Goals

- Do not make pooling the default.
- Do not change UDP behavior.
- Do not rewrite relay backpressure or close semantics in this knife.
- Do not add a second endpoint per pooled connection unless the single endpoint proves insufficient.

## Acceptance

Offline:

- Unit tests cover default/clamped pool parsing and TCP round-robin selection.
- `cargo test --lib tuic` passes.
- Formatting/checks pass.

Live:

- Re-run the US-client suite twice:
  - control: `MINI_VPN_TUIC_TCP_POOL=1`
  - experiment: `MINI_VPN_TUIC_TCP_POOL=4`
- Compare forward `P2/P4/P8`, reverse `P2/P4/P8`, and `remote_write_timeout` counts.
- Pool is promising only if multi-flow forward improves materially without regressing `P1` or increasing resets/timeouts.
