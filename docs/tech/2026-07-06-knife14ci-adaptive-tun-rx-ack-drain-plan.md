# Knife14ci Adaptive TUN RX ACK Drain Plan

Date: 2026-07-06

## Design Tree

1. Add more pressure credit debt.
   - Rejected for this stage. Knife14ch showed the failing repeat did not cross
     the pressure-pause edge, so edge-only debt stayed at `0`. More debt may
     make counters cleaner, but it can also slow remote reads without improving
     local ACK/window cadence.
2. Enable fixed TUN RX drain by default.
   - Rejected. Knife14bi showed broad opportunistic drain regressed
     reverse-first behavior. The product default must remain safe below real
     pressure.
3. Add pressure-triggered adaptive TUN RX drain.
   - Strong. It targets the exact Knife14ch signal: send queue near the egress
     credit guard, no global receive pressure, no TUN drops, and long TUIC
     stream read gaps. It processes ready local TCP ACK/window updates only
     when downlink work and egress pressure coexist.
4. Increase receive windows or pool size.
   - Rejected by current evidence and project constraints. Knife14cg made
     receive decoupling worse, and stale pool/pool sizing branches are closed
     unless new evidence reopens them.

## Implementation Plan

1. Add a bounded `TUN_RX_PRESSURE_DRAIN_MAX_PACKETS` constant.
2. Add pure helpers:
   - `pressure_tun_rx_drain_budget(cfg, tun_mtu)`;
   - `tun_rx_drain_budget_after_remote_payload(has_work, send_queue, explicit,
     cfg, tun_mtu)`.
3. Update the remote payload branch:
   - keep dirty-handle logic unchanged;
   - compute whether downlink work exists from accepted bytes or pending bytes;
   - sample current `TcpSocket::send_queue()`;
   - call `drain_ready_tun_rx` only when the resolved budget is non-zero.
4. Update startup logging to show the adaptive pressure budget when explicit
   TUN RX drain is `0`.
5. Add focused tests for explicit override, no-work/no-pressure suppression,
   pressure-edge enablement, and guard/MTU/cap derivation.
6. Run local gates.
7. Commit/push the code/spec/plan as one coherent stage before VPS.
8. Sync `.27`, build release, run a clean reverse-first P1 suite with required
   exit-to-target baseline, parse the bundle, and record results/learnings.

## Local Test Plan

Focused:

- `cargo test tun_rx_drain --lib`
- `cargo test downlink_egress_clock --lib`
- `cargo test pressure_credit --lib`

Regression:

- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib`
- `cargo test`
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `git diff --check`

## VPS Plan

- Use `.27` true TTY if sudo is needed.
- Source `.27` `.env` in the same shell without printing secrets.
- Ensure `MINI_VPN_BOUNDED_GLOBAL_RX_RECEIVE_WINDOW` is unset.
- Force `EXIT_TO_TARGET_IPERF_CHECK=1` and `EXIT_TO_TARGET_IPERF_REQUIRED=1`.
- Run:
  - `RUN_REVERSE_FIRST_P1=1`
  - `STOP_AFTER_REVERSE_FIRST_P1=1`
  - `PARALLEL_SET=1`
  - `DURATION=30`
  - `TARGET=43.130.32.77`
  - `IPERF_PORT=5201`
  - server evidence enabled for `.33` and `.77`

## Evaluation

- Success: reverse-first P1 materially leaves the `10-20 Mbit/s` class, TUN RX
  drain shows bounded TCP ACK/window packets, and no close/reap/pending loss
  appears.
- Partial: TUN RX drain engages and removes read gaps but throughput remains
  low; next target is local TUN egress pacing/scheduling with the new evidence.
- Failure: no TUN RX drain engagement and no pressure signal; inspect whether
  the credit edge is too late or the stall happens before smoltcp egress.
