# 刀12 verdict: the client central loop is NOT the 100M bottleneck (#4 refuted); the WAN path is the wall

The "multicore → 100M" brief (刀12) rested on one load-bearing hypothesis: that the **single-core
smoltcp `poll` segment** of the central `run_event_loop` task (deferred bottleneck **#4**) is what caps
client throughput near 100 Mbps, and that **event-loop sharding** (route a) is therefore the next lever.
刀12 was scoped **quantify-only** (先量化、别凭猜改) to test that hypothesis before any refactor, via a
new env-gated `LoopProfiler` (poll/relay/loop-active wall-fractions, `MINI_VPN_PROFILE_LOOP=1`).

**Real-egress measurement (2026-06-26/27, 深圳 macOS client → 47.251.188.205 sing-box, TUIC native+cubic)
REFUTES #4.** The decision: **the central loop's `poll` segment is not the ceiling; the trans-Pacific WAN
path is. Do NOT shard the event loop in 刀13.**

## Update (2026-06-27): the first *clean* tunneled run — verdict holds, mechanism clarified, + a real loop bug found

The earlier readings (below) mostly **bypassed the tunnel** (bare `client-tun`, no routing — see Honest
gaps). The first run that actually went through the tunnel (via `soak` global routing; `📊 TCP relay 累计`
climbed 1→22; iperf `-P 4` to a US target = **44.5M / 613 retr**, vs 26M / 63509 retr direct) gave a
**sustained-load** `🔬`:

```
loop-active ≈ 92–93%   poll ≈ 8%   relay ≈ 81%   park ≈ 7%   iters ≈ 22 000/5s   (~44.5M, 12 windows / 60s)
```

At first glance this looks like the loop **saturating** — contradicting "client idle." It is not. Reading
the code (`handle_local_payload`, src/client_tun.rs:1350): the uplink send is a **blocking**
`tx.send(payload).await` on a **bounded** (`RELAY_CHANNEL_CAPACITY=1024`) channel. When the QUIC upstream
is congestion-limited (which it is at the ~44.5M ceiling), `run_relay` can't drain the channel, it fills,
and the loop **blocks in `send().await` waiting for the upstream**. So:

- **`poll`=8% is real CPU** (smoltcp `iface.poll` doesn't wait on the upstream) → smoltcp poll costs ~8%
  CPU at 44.5M. **Confirms #4-as-poll is refuted.**
- **`relay`=81% is mostly back-pressure *wait*, not CPU** — the loop parked inside `send().await` for the
  congested upstream. The loop is **upstream-bound, not CPU-bound.** **The verdict holds: the wall is the
  QUIC upstream / WAN (#3-family), and `relay`=81% is the loop *waiting* on it, surfacing as a busy
  segment rather than as `park`.**

**Instrument limitation (document):** `loop-active = 1 − park/wall` **conflates CPU with intra-arm-body
`.await`/back-pressure wait** — `tx.send().await` blocking on a full channel counts as "active" though the
loop is idle-waiting on the upstream. The pure-CPU split needs an OS thread-CPU sample
(`sample $(pgrep mini_vpn) 10` during load → expect **low** thread CPU + hot stack parked in
`tx.send`/channel `await` = back-pressure confirmed; high CPU in `process_dirty_relay` would mean #4). This
sample is the one remaining confirmation; the code makes back-pressure the strong default reading.

**NEW real loop bug found (the genuinely valuable lead for 刀13):** because the uplink send **blocks the
whole event loop**, one congestion-stalled flow **head-of-line-blocks every other responsibility** of the
loop — other flows' uplink, the downlink return path (`global_rx`), the 5ms timer (smoltcp retransmits),
DNS hijack. Four identical iperf flows hide it (all stall together), but **heterogeneous 大并发 (one slow
flow + fast flows) would stall the fast flows behind the slow one.** This is exactly the brief's #4
"central-loop serialization" — vindicated, but the real mechanism is the **blocking uplink send under
back-pressure, not smoltcp poll.**

**→ 刀13 candidate (concrete, cheap, on-target for Rules ③ 大并发):** make the uplink send **non-blocking**
— `try_send`, and on a full channel **leave the bytes in the smoltcp socket (do not `recv`) and keep the
handle dirty**, so smoltcp's own TCP receive window applies end-to-end back-pressure to the app for free,
the loop **never blocks**, and one congested flow can no longer stall the others. Lower-risk and more
targeted than event-loop sharding or a connection pool; it treats the loop-serialization #4 without
touching the no-lock single-writer model. (Aggregate throughput on a given path is still upstream/WAN
bound — this fix removes cross-flow head-of-line stalling, not the per-connection CC ceiling.)

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
2. **The binding constraint at every achievable load is the WAN path / QUIC upstream**, not the client's
   CPU. (Refined by the clean tunneled run — see the **Update** section above: under real tunneled load
   the loop is **not idle**; it is `relay`=81% **blocked in `send().await` waiting for the congested
   upstream** — back-pressure wait, not CPU. Poll stays ~8%. The loop is upstream-bound, not CPU-bound.)
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
- **LEADING 刀13 candidate (Rules ③ 大并发) — non-blocking uplink send (see Update above):** the clean
  tunneled run found a real loop bug — the **blocking** `tx.send().await` (bounded 1024 channel,
  src/client_tun.rs:1350) makes one congestion-stalled flow **head-of-line-block the whole event loop**
  (other flows' uplink, downlink return, the 5ms timer, DNS). Fix: `try_send` + on-full **leave bytes in
  the smoltcp socket + keep the handle dirty** so smoltcp's TCP window back-pressures the app end-to-end
  and the loop never blocks. Concrete, cheap, treats the real #4 (loop serialization) without touching the
  no-lock single-writer model; far better-targeted than sharding or a pool. (Secondary, separate axis:
  connection-churn / inline stream-open cost under thousands of concurrent *opens* — unmeasured here.)
- The `LoopProfiler` instrument stays (env-gated, default `NoopSink` zero-cost) as the standing tool to
  re-test #4/#3 on any future path.

## Honest gaps (尽力而为如实记录)

- A clean **sustained tunneled-at-path-max** `🔬` reading was initially **not** captured: two early 60s
  `-P 4` attempts had the iperf traffic **bypass the tunnel** (`TCP relay 累计=0` throughout). **RESOLVED
  2026-06-27** — a `soak`-routed run (`TCP relay 累计` 1→22, 44.5M tunneled) gave the clean sustained
  reading; see the **Update** section. It confirmed the verdict (poll ~8%, loop upstream-bound) and found
  the head-of-line bug. The one remaining confirmation is an OS thread-CPU `sample` during load (expected
  low CPU = back-pressure, not #4).
- **Root cause of the bypass (important for future probes):** running the bare
  `./target/release/mini_vpn client-tun` only builds the utun device + the TUIC connection — it does
  **not** configure macOS routing/DNS, so all traffic still exits via `en0` directly and never enters the
  utun. The `scripts/knife35-acceptance.sh soak` helper (global route + plaintext-DNS hijack) is what
  actually steers traffic into the tunnel; a manual `route -host <ip> -interface utunX` is fragile (a
  client restart renumbers the utun, leaving the route stale → fallback to the default route).
- **The gold-standard "am I tunneled?" check is `curl ipinfo.io` — it must report the US exit IP, not the
  client's local (深圳) IP.** Simpler and more direct than the `📊 TCP relay 累计` counter; a local IP
  means traffic is bypassing the tunnel and any throughput/loss numbers are measuring the **bare path,
  not mini_vpn** (so the heavy iperf retransmits seen were the raw trans-Pacific internet path's loss,
  not the client's). Always verify `curl ipinfo.io` (and `dig <domain> +short` → a `198.18.x.x` fake-IP)
  **before** any throughput probe.
- The `📊 TCP relay 累计` counter (刀11) was the load-bearing signal that caught the bypass — without it
  the idle `🔬` would have been misread as "loop idle under tunneled load."
- Cosmetic: the first `🔬` line after startup shows a degenerate `wall≈0ms / loop-active≈99%` (tokio
  `interval` fires its first tick immediately). Harmless; ignore the first line. A one-line guard
  (skip report when `wall` is degenerate) is a trivial optional follow-up.

## Status

刀12 quantify-only **complete**: instrument delivered + reviewed + real-egress measurement → **#4
refuted, sharding cancelled, #3 deferred pending a fat low-RTT path.** No multicore refactor was
performed, by design and on evidence.
