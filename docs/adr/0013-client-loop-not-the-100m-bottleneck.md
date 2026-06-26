# 刀12 verdict: the client central loop is NOT the 100M bottleneck (#4 refuted); the WAN path is the wall

The "multicore → 100M" brief (刀12) rested on one load-bearing hypothesis: that the **single-core
smoltcp `poll` segment** of the central `run_event_loop` task (deferred bottleneck **#4**) is what caps
client throughput near 100 Mbps, and that **event-loop sharding** (route a) is therefore the next lever.
刀12 was scoped **quantify-only** (先量化、别凭猜改) to test that hypothesis before any refactor, via a
new env-gated `LoopProfiler` (poll/relay/loop-active wall-fractions, `MINI_VPN_PROFILE_LOOP=1`).

**Real-egress measurement (2026-06-26/27, 深圳 macOS client → 47.251.188.205 sing-box, TUIC native+cubic)
REFUTES #4.** The decision: **the central loop's `poll` segment is not the ceiling; the trans-Pacific WAN
path is. Do NOT shard the event loop in 刀13.**

## Measurement

`MINI_VPN_PROFILE_LOOP=1 MINI_VPN_METRICS_SECS=5`, iperf3 `-c 47.77.215.177` over the client, 1/4/8
parallel flows. Two readouts: the in-process `🔬` line (loop-active/poll/relay/park) and the `📊` line
(`TCP relay 累计` = relays actually created — the ground-truth "did traffic enter the tunnel" signal).

| condition | aggregate | client `🔬` | reading |
|---|---|---|---|
| direct / no traffic in tunnel (`TCP relay 累计=0`) | 22–26M (raw 深圳↔US, up to 63509 retr/60s) | `loop-active≈0.1% poll≈0.1% park≈99.9% iters≈1007` (timer-only) | loop idle — traffic bypassed it |
| **tunneled, P=4 setup window** | scaled to **~46M** | `loop-active=72% poll=3.8% relay=66% iters=16523` | busy, but the cost is **relay**, not poll; transient (connection setup) |
| tunneled, P=8 window | ~44M | `loop-active=10.4% poll=0.7% relay=9.3%` | poll negligible |

**`poll` ≤ 3.8% in every sample, ~0.1% typical** — across direct AND tunneled, at every load this path
could deliver. The central loop is 99.9% parked whenever traffic is direct; even when tunneled, `poll`
(smoltcp's single-threaded socket iteration — the supposed #4 cost) is trivial.

The path itself is the wall: a single trans-Pacific TCP flow is RTT-limited to ~22M; parallel flows
aggregate to ~46M then plateau, with heavy loss. The machines' 100M **port** speed is not 100M of
**achievable end-to-end throughput**; **100M is physically unreachable on this 深圳↔US path**, so the
client's true CPU ceiling is not observable here.

## Verdict

1. **#4 (single-core smoltcp `poll` = 100M ceiling): REFUTED.** The instrument disproved the brief's
   premise, exactly as 刀3.5's instrumentation disproved the "5.3M datagram ceiling" (a link-cap
   artifact; see ADR-0005). Quantify-first earned its keep again.
2. **The binding constraint at every achievable load is the WAN path**, not the client. The central loop
   is idle/poll-negligible throughout.
3. **Bottleneck-model correction:** the only on-loop cost that ever appeared was the **relay** segment
   during connection **setup** (relay=66% in one 4-flow setup window) — i.e. inline stream-open +
   uplink-drain scheduling, **not** `poll`. If the central loop ever bottlenecks, it is relay-scheduling
   under connection churn, not smoltcp poll.
4. **#3 (single QUIC connection): unresolved on this path.** Aggregate scaled to ~46M with parallel flows
   (so the connection is **not** a 7M-style hard cap), then plateaued; this path is too lossy/high-RTT to
   separate "QUIC connection congestion control" from "WAN capacity."

## Consequences

- **CANCEL route (a) event-loop sharding for 刀13.** Sharding a 99%-parked, poll-negligible loop yields
  nothing. This is exactly the very-large refactor that quantify-only existed to prevent — avoided.
- **#3 connection pool (route c)** is the only remaining *client-side* throughput lever, but it is
  **unproven and likely WAN-limited** on realistic high-RTT egress. Defer it until a **fat, low-RTT,
  genuinely ≥100M end-to-end path** (e.g. a same-region / LAN egress, not 深圳↔US) is available to
  measure single-conn-vs-pool cleanly. Without that path, a pool may move no needle.
- **New candidate axis for "大并发" (Rules ③):** the relay spike at connection setup suggests the real
  client-side concurrency cost — if any — is **connection-churn relay scheduling / inline stream open**,
  not bulk-throughput poll. A future knife could probe thousands of concurrent connection *opens* (not
  bulk bytes) and, if the loop's relay segment saturates, move inline `open` off the central task. This
  is distinct from the throughput question and was not measured here.
- The `LoopProfiler` instrument stays (env-gated, default `NoopSink` zero-cost) as the standing tool to
  re-test #4/#3 on any future path.

## Honest gaps (尽力而为如实记录)

- A clean **sustained tunneled-at-path-max** `🔬` reading was **not** captured: two 60s `-P 4` attempts
  had the iperf traffic **bypass the tunnel** (`TCP relay 累计=0` throughout — client restart changed the
  utun number, leaving a stale `route ... -interface utunX` that fell back to the default route). The
  tunneled evidence is from shorter, setup-contaminated windows. This does **not** weaken the #4
  refutation (poll is negligible in every tunneled window observed), but the loop's *steady-state*
  occupancy at the ~46M path-max is **inferred, not cleanly measured**.
- The `📊 TCP relay 累计` counter (刀11) was the load-bearing signal that caught the bypass — without it
  the idle `🔬` would have been misread as "loop idle under tunneled load."
- Cosmetic: the first `🔬` line after startup shows a degenerate `wall≈0ms / loop-active≈99%` (tokio
  `interval` fires its first tick immediately). Harmless; ignore the first line. A one-line guard
  (skip report when `wall` is degenerate) is a trivial optional follow-up.

## Status

刀12 quantify-only **complete**: instrument delivered + reviewed + real-egress measurement → **#4
refuted, sharding cancelled, #3 deferred pending a fat low-RTT path.** No multicore refactor was
performed, by design and on evidence.
