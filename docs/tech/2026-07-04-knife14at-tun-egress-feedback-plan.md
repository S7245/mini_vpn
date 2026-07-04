# Knife14at plan - runtime TUN egress feedback

Spec:
`docs/tech/2026-07-04-knife14at-tun-egress-feedback-spec.md`

## Tasks

1. Runtime TUN name
   - Store the OS interface name in `VirtualTunDevice` when available.
   - Add a default `TunIo::interface_name()` method so tests and non-product
     devices remain no-op.

2. Diagnostic sampler
   - Add a small `TunEgressDropSampler` that reads Linux sysfs counters through
     an injectable read function.
   - Return explicit sample states for unavailable stats, first sample,
     monotonic delta, and non-monotonic counter reset.
   - Format a `tcp-tun-egress` line with drop delta and current downlink stats.

3. Main-loop wiring
   - Instantiate the sampler once after the device exists.
   - On the TCP diagnostic metrics tick, sample the counter and log it next to
     `tcp-downlink-flush`.
   - Keep behavior unchanged: no new pauses, pacing, queue changes, or drops.

4. Probe/report support
   - Extend `scripts/knife14b-lowrtt-probe.sh` to parse
     `tcp-tun-egress`.
   - Print `runtime_tun_egress: samples=... drop_events=... max_delta=...`.
   - Include `tcp-tun-egress` in the suite report grep.

5. Verification and review
   - Run focused Rust tests and shell self-tests.
   - Review the diff for accidental product behavior changes.
   - Commit and push if local gates pass.
   - Run one scoped VPS reverse-first acceptance only after the commit is on
     `.27`.
