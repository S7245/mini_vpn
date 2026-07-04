# Knife14bg TUN RX Drain Cadence Plan

Date: 2026-07-05

## Hypothesis

Clean reverse-first low throughput is caused by insufficient local TUN ingress
drain fairness under remote downlink pressure. In practical terms, TCP ACKs and
window updates from the local kernel can sit behind a stream of ready remote
downlink events, so smoltcp sees a full tx queue, pauses remote reads, and ends
with terminal pending at close.

## TDD Plan

1. Add a small test seam for nonblocking TUN RX reads.
   - Behavior: when no packet is ready, the call returns `Ok(false)` and does
     not await.
   - Behavior: when a packet is ready, it fills the existing single-packet
     `rx_buffer` and returns `Ok(true)`.

2. Add a bounded-drain helper test using the harness device.
   - Behavior: a drain budget of `N` processes at most `N` packets.
   - Behavior: `WouldBlock` / no ready packet stops the helper without error.
   - Behavior: processed TCP packets run through the same poll/flush path that
     the awaited `wait_for_rx` branch uses.

3. Add diagnostics tests if a new line is emitted.
   - Proposed line:
     `tcp-tun-rx-drain packets={} budget={} would_block={} errors={} source={}`
   - Parser self-test must include the new line if VPS attribution will use it.

## Implementation Plan

1. Extend `TunIo`.
   - Add `try_recv_rx(&mut self) -> std::io::Result<bool>`.
   - Production `VirtualTunDevice`: use the already-nonblocking underlying TUN
     fd through `self.device.get_mut().read(...)`.
   - Harness `LoopbackTunDevice`: pop one inbound packet if present.
   - Test-only DNS recorder can return `Ok(false)`.

2. Deduplicate TUN RX processing.
   - Extract the existing awaited `device.wait_for_rx()` TCP/DNS/UDP
     classification body into a helper that assumes `rx_buffer` is already
     populated.
   - Keep DNS and UDP behavior byte-for-byte equivalent where possible.

3. Add bounded opportunistic drain.
   - After remote `RelayEvent::Data` handling, if `accepted_bytes > 0` or the
     handle still has pending/tx pressure, call `drain_ready_tun_rx` with a small
     hard budget.
   - The helper must not await TUN read readiness; it only consumes packets
     already available.
   - Keep the existing `wait_for_rx` branch as the canonical blocking ingress
     path.

4. Add observability.
   - Track drain attempts, packets drained, budget stops, would-block stops, and
     errors.
   - Emit a diagnostic summary under `MINI_VPN_TCP_DIAG=1`.
   - Update `scripts/knife14b-lowrtt-probe.sh` only if the metric is used in
     attribution.

5. Run local gates.
   - Use targeted tests first.
   - Do not run root `cargo fmt`; keep formatting scoped to edited hunks.

6. VPS acceptance.
   - Sync `.27` using the local bundle path if GitHub SSH fetch is unavailable.
   - Use a true TTY for sudo.
   - Run reverse-first P1 first, then parse the bundle before running a broader
     suite.

## Risk Review

- Risk: nonblocking drain could starve remote downlink if budget is too high.
  Mitigation: low hard cap and diagnostics.
- Risk: duplicated packet classification could diverge from the awaited path.
  Mitigation: extract shared helper rather than copying logic.
- Risk: reading directly from the underlying nonblocking TUN fd might behave
  differently across platforms.
  Mitigation: treat `WouldBlock` as no packet; keep awaited path unchanged.
- Risk: this improves ACK cadence but exposes another downstream bottleneck.
  Mitigation: acceptance requires both throughput and lifecycle metrics, not
  only one counter.

## Stop Conditions

Stop and re-evaluate architecture if the next VPS run shows:

- no material clean reverse-first throughput improvement;
- `downlink_backpressure` and terminal pending unchanged;
- drain diagnostics prove many ACK/window packets are processed promptly;
- QUIC remains clean and TUN flush errors remain zero.

That combination would weaken the fairness hypothesis and require a broader
local TCP/TUN architecture review.
