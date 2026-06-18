# Default to Cubic (not BBR) for the QUIC datagram data plane on quinn 0.10

The TUIC UDP data plane defaults to **Cubic** congestion control, not BBR, set via
`DEFAULT_TUIC_CC="cubic"` (overridable per-run with `MINI_VPN_TUIC_CC=bbr`). The default UDP relay
mode stays **native** (QUIC datagram), with the per-packet uni-stream path kept only as the
oversized-packet fallback (Stage/刀3) and an opt-in `udp_relay_mode=quic` — **not** the high-rate path.

This reverses the assumption we entered 刀3.5 with (BBR, because BBR usually wins on high-RTT/lossy
paths). Real-egress measurement (深圳 client → 47.251.188.205 sing-box → 43.110.37.170 iperf3, both
links 80 Mbps, 2026-06-17) contradicted it for **unreliable datagrams on quinn 0.10**:

| 40 Mbps offered, downlink | delivered / loss | quinn `cwnd` / `RTT` |
|---|---|---|
| Cubic, native (datagram)  | **39.8 M / 0.25%** | 12 000 / 172 ms (stable) |
| BBR, native (datagram)    | 30.1 M / **24%**   | 245 K→252 K / 178→**252 ms** (bufferbloat) |
| BBR, quic (all uni-stream)| **7.0 M / 71%**    | **4.5 MB** / 259 ms (collapse) |

quinn 0.10's BBR keeps probing bandwidth and grows `cwnd` without the loss-driven backoff Cubic
applies; because QUIC **datagrams are not retransmitted and not application-flow-controlled**, that
over-driving turns directly into packet loss and RTT inflation rather than throughput. Cubic's
conservative window delivers the line rate cleanly. This is specific to unreliable datagrams + this
quinn version; it is **not** a general claim that Cubic beats BBR.

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

- **Default BBR** (our pre-measurement plan) — rejected: 24% loss at 40 M on datagram, 71% on the
  all-stream path, with `cwnd`/RTT blow-up. Actively worse for this data plane.
- **Default Cubic (chosen)** — clean line-rate delivery on datagram; matches quinn's own default
  controller, so it is also the low-surprise choice.
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
