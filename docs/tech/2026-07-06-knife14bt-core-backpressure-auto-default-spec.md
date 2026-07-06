# Knife14bt Core Backpressure Auto Default Spec

## Grounding

Knife14bs proved that the suite-side legacy-env normalization works, but the
binary auto default it exposed is not safe on the current `.27 -> .33 -> .77`
topology:

- mini_vpn started with `high=1048576B low=262144B` because the TCP tx buffer
  was `1048576`.
- reverse-first P1 fell to `20.2/18.8 Mbit/s` with repeated zero-throughput
  intervals.
- direct `.27 <-> .77` and `.33 <-> .77` baselines stayed healthy.
- QUIC loss, congestion, and blocking deltas stayed zero.
- TUN drops decreased versus the old 512 KiB run, but throughput also
  collapsed; stream read gaps reached multi-second scale.

Therefore `TCP tx buffer size == safe downlink high watermark` is a rejected
default rule. The project core should keep the conservative product default
unless an operator explicitly chooses a larger high/low pair for an A/B run.

## Design Tree

1. Keep tx-buffer-scaled auto defaults and tune scripts.
   - Rejected. Knife14bs already normalized scripts to auto and the core auto
     value caused a low-average failure.

2. Increase receive window or TUN queue length again.
   - Rejected. The failure appeared after increasing the effective high
     watermark; no new evidence says more buffering is safer.

3. Revert the core auto default to the conservative 512 KiB / 128 KiB pair.
   - Accepted for this stage. It removes the proven low-average branch while
     preserving explicit high/low A/B controls.

4. Solve tail collapse with durable TUN egress pressure in the same patch.
   - Deferred. It is a separate lifecycle/pressure-gate behavior change and
     should get its own focused TDD slice after the unsafe auto default is
     removed.

## Stage Goal

Knife14bt must make empty/unset downlink backpressure env resolve to the
conservative default, independent of a larger TCP tx buffer.

## Non-Goals

- Do not change scripts as the primary fix.
- Do not change TUIC auth, sing-box, iperf3, stale pool handling, TUN qlen, or
  egress pacer behavior.
- Do not remove explicit `MINI_VPN_DOWNLINK_BACKPRESSURE_*` A/B support.
- Do not claim final Knife14 acceptance from this patch alone; the old
  512 KiB branch still needs tail-collapse handling.

## Invariants

- Missing, empty, invalid, or zero high/low env values fall back to the core
  default.
- Explicit valid high/low env values are honored.
- A bad explicit low value is repaired below high.
- Startup logs remain the source of truth for the effective high/low pair.

## Acceptance

Local:

- A focused Rust test proves `parse_downlink_backpressure_config_for_tx_buffer`
  keeps `512 KiB / 128 KiB` when `tx_bytes=1048576`.
- Existing explicit high/low parser tests still pass.
- `cargo test --lib client_tun::tests::parse_downlink_backpressure_config_keeps_safe_defaults_with_large_tcp_tx_buffer`
- A focused `client_tun` test subset passes.
- `git diff --check`.

VPS:

- The next scoped reverse-first P1 run should show suite config high/low as
  `<auto>` and mini_vpn startup high/low as `524288B / 131072B`.
- Expected behavior is recovery from the Knife14bs `low_average` 10-20 Mbit/s
  branch. Tail collapse and TUN egress pressure remain active acceptance
  signals for the next patch.
