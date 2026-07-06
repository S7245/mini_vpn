# Knife14ck ACK-Sized Pressure Drain Budget Plan

Date: 2026-07-06

## Design Tree

1. Increase `MINI_VPN_TUN_RX_DRAIN_BUDGET`.
   - Rejected. That is an operator override and would hide the product default
     algorithm bug.
2. Drain until TUN RX would-block without a cap.
   - Rejected. It risks reintroducing the broad opportunistic drain behavior
     that Knife14bi rejected.
3. Derive packet budget from ACK-sized packets and cap it.
   - Strong. It keeps the pressure gate and credit-guard derivation, but fixes
     the unit mismatch exposed by Knife14cj.

## Implementation Plan

1. Add a named estimated pressure-drain packet size for ACK/window traffic.
2. Raise the adaptive pressure-drain hard cap to the smallest value that can
   cover the default guard without becoming unbounded.
3. Change `pressure_tun_rx_drain_budget` to use
   `min(tun_mtu, ack_packet_estimate)` as the packet-size denominator.
4. Update focused tests:
   - small guard still yields one packet;
   - default guard uses ACK-sized estimate and is capped;
   - tiny MTU input remains bounded.
5. Run local gates and commit.
6. Sync `.27`, run the same reverse-first P1 suite with explicit Exit/Target
   SSH env, parse bundle, and record results.

## Test Plan

- `cargo test tun_rx_drain --lib`
- `cargo test tun_rx_pressure --lib`
- `cargo test pre_payload --lib`
- `cargo test maintenance --lib`
- `cargo test pressure_credit --lib`
- `cargo test drop_credit --lib`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib`
- `cargo test`
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `git diff --check`

## Stop Conditions

- If local tests show the new budget triggers below the pressure edge, fix the
  gating before VPS.
- If VPS shows `would_block=0` again with high drops, increase budget via a
  bounded elapsed-work algorithm rather than an env default.
- If VPS shows `would_block>0` but drops persist, leave ACK-budget work and
  inspect TUN egress flush scheduling.
