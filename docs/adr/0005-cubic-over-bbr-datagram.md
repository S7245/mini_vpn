# Default to Cubic (not BBR) for the QUIC datagram data plane on quinn 0.10

The TUIC UDP data plane defaults to **Cubic** congestion control, not BBR, set via
`DEFAULT_TUIC_CC="cubic"` (overridable per-run with `MINI_VPN_TUIC_CC=bbr`). The default UDP relay
mode stays **native** (QUIC datagram), with the per-packet uni-stream path kept only as the
oversized-packet fallback (Stage/刀3) and an opt-in `udp_relay_mode=quic` — **not** the high-rate path.

This reverses the assumption we entered 刀3.5 with (BBR, because BBR usually wins on high-RTT/lossy
paths). The decision rests on **worst-case behaviour / lower variance**, not on BBR being uniformly
worse. Real-egress measurement (深圳 client → 47.251.188.205 sing-box → 43.110.37.170 iperf3, both
links 80 Mbps, 2026-06-17), 40 Mbps offered, downlink:

| run | Cubic native (datagram) | BBR native (datagram) |
|---|---|---|
| machine A | **39.8 M / 0.25%** (cwnd 12 000, RTT 172 ms, stable) | 30.1 M / **24%** (cwnd 245 K→252 K, RTT 178→**252 ms**, bufferbloat) |
| machine B | 37.0 M / 7.6% | 37.7 M / 5.8% |

On machine B the two are comparable (BBR slightly better); on machine A BBR had a **severe over-drive
episode** — `cwnd` ballooned to ~245 K with RTT inflating to 252 ms and 24% loss. The pattern: quinn
0.10's BBR keeps probing bandwidth and grows `cwnd` without the loss-driven backoff Cubic applies, and
because QUIC **datagrams are not retransmitted and not application-flow-controlled**, an over-drive
turns directly into loss + RTT inflation rather than throughput. Cubic's conservative window has the
**better worst case** (7.6% vs 24%) and lower variance. For a consistency-sensitive streaming VPN that
makes Cubic the safer default. This is **not** a general claim that Cubic beats BBR — only that, for
unreliable datagrams on quinn 0.10 over this path, Cubic's tail is tighter. A separate all-uni-stream
(`quic`) run collapsed regardless of CC (machine A: 7 M / 71%, cwnd 4.5 MB; machine B: 0.95 M / 39%),
confirming the stream path is not the throughput answer.

## Context correction (why this ADR exists at all)

刀3's acceptance reported a "~5.3 Mbps hard ceiling on the native QUIC datagram path (both directions,
independent of offered rate), while QUIC streams hit 50 Mbps." 刀3.5 was scoped to break that ceiling
by routing high-rate flows over streams. **The ceiling was a measurement artifact**: the test VPS link
(47.x) was capped at **5 Mbps** and the iperf3 target (43.x) at 10 Mbps. After both were raised to
80 Mbps, native datagram delivers ~40 Mbps down / ~37 Mbps up. There was never a datagram transport
ceiling to break. The lesson (HANDOFF「先量化、别凭猜改」) held: the quinn-level instrumentation
(`cwnd`/`RTT`/`lost`/`send_buffer_space`) added in 刀3.5 is what exposed both the artifact and the
Cubic-vs-BBR truth.

## Considered Options

- **Default BBR** (our pre-measurement plan) — rejected on **worst-case**: comparable to Cubic on one
  run but a 24%-loss / cwnd-245K / RTT-252ms over-drive episode on another. Too variable for a default.
- **Default Cubic (chosen)** — tighter tail (worst observed 7.6% vs BBR's 24%), lower variance, and it
  matches quinn's own default controller, so it is also the low-surprise choice.
- **Default to quic relay mode (all UDP over uni-streams)** — rejected: 7 M / 71% at 40 M; per-packet
  uni-stream (~4000 streams/s) + congestion collapse. Kept only as an opt-in (see Consequences).

## Consequences

- `DEFAULT_TUIC_CC="cubic"`; `MINI_VPN_TUIC_CC=bbr` remains for experiments / specific links. The
  switch is per-process and cheap to flip if a future quinn fixes BBR or a different path favours it.
- `udp_relay_mode=quic` (all-stream) stays implemented and tested but **non-default**. It is retained
  because it has a plausible **anti-censorship** use (networks that drop QUIC datagrams but pass the
  connection's stream traffic), not because it helps throughput — it does not.
- The oversized-packet → per-packet uni-stream **fallback inside native mode** (刀3) is unaffected and
  remains correct: it is a low-frequency tail, not a high-rate primary.
- Rules.md ② is met on datagram: typical (≤5 M), 1080p60 (~8–12 M) and 4K (~25 M) downlink all fit
  under the ~40 M clean datagram throughput, preserving datagram's low latency (no stream HOL/setup).
- 刀3's findings doc is annotated with the artifact correction so the "5.3 M ceiling" is not cited as
  a real limit by future work.
