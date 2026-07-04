# Knife14ak spec - TUN tx queue A/B attribution

## Grounding

- Latest remote-tested head before this stage: `fd3de4a`.
- Stale TUIC TCP pool slot diagnosis is closed. The accepted signal was
  `tuic-tcp-pool-reconnect conn=1 reason=stale_tcp_pool_slot` with no later
  stale-slot timeout.
- Knife14aj kept `Closed && !can_send` pending immediately reapable and added
  per-probe TUN drop deltas.
- The latest scoped VPS bundle is:
  `/tmp/mini_vpn/mvpn_knife14aj_tun_drop_env_usclient_suite_20260704_112101.tar.gz`.
- Direct baselines in that bundle stayed healthy, around 277-283 Mbit/s
  receiver.
- Tunnel probes showed throughput variance plus local TUN egress drops:
  - reverse-first P1: 182 Mbit/s receiver,
    `tun_tx_dropped_delta=3209`, attribution
    `local_tun_egress_drop+local_downlink_backpressure`;
  - standard forward P1: 164 Mbit/s receiver,
    `tun_tx_dropped_delta=17`, attribution
    `quic_loss_congestion+local_write_pressure+local_tun_egress_drop`;
  - standard reverse P1: 123 Mbit/s receiver,
    `tun_tx_dropped_delta=16661`, attribution
    `local_tun_egress_drop+local_downlink_backpressure`.
- The final link snapshot in that bundle showed Linux TUN egress queueing with
  `qdisc fq_codel` and `qlen 500`.
- mini_vpn's TUN fd writes reported success. That means the current observable
  loss is after userspace writes into the kernel device/qdisc path, not a
  `flush_tx` syscall failure.
- `.27` has a valid `/home/ubuntu/mini_vpn/.evn`; the requested
  `/home/ubuntu/mini_vpn/.env` file is absent. Future runs must source `.evn`
  quietly because it prints exported values.

## Problem

The current report can see TUN TX drops, but it cannot separate these branches:

1. Linux TUN `txqueuelen=500` is too shallow for bursty downlink flushes.
2. mini_vpn's downlink backpressure high/low watermarks allow too much burst
   before pausing remote reads.
3. `VirtualTunDevice::flush_tx` drains all queued packets in one call and may
   overfeed the qdisc even when the userspace backlog is bounded.
4. QUIC path loss/congestion still contributes on some forward probes.
5. The terminal closed-pending reap is only a symptom after the local socket has
   already stopped accepting bytes.

Changing product backpressure or adding pacing before this A/B would be a weak
guess. The smallest next step is to make the suite able to run the same probe
with a known TUN queue length and record the actual value in the report.

## Goal

Add an opt-in harness control for Linux TUN `txqueuelen` so the next VPS run can
compare default `qlen 500` with a larger queue under the same mini_vpn binary,
same TUIC config, and same probe shape.

This stage should answer:

- Does increasing the TUN tx queue materially reduce `tun_tx_dropped_delta`?
- Does it improve reverse P1 throughput and reduce downlink pressure?
- If it helps, is the next product fix likely pacing/backpressure rather than
  lifecycle/reap logic?
- If it does not help, should the next branch move to downlink watermarks,
  paced TUN flush, or QUIC/path attribution?

## Non-Goals

- Do not continue stale TUIC TCP pool slot diagnosis.
- Do not change Rust data-plane behavior in this slice.
- Do not tune default queue lengths silently for normal acceptance runs.
- Do not make persistent host network changes outside the lifetime of the TUN
  device created by the suite.
- Do not hide low throughput by declaring a larger qdisc queue to be the final
  product fix.
- Do not store TUIC secrets, private keys, or raw `.evn` output in docs,
  learning memory, or final summaries.

## Design Tree

1. Increase TUN `txqueuelen` in the suite only.
   Selected for this slice. It is reversible, observable, and tests whether the
   kernel queue is the immediate drop point.

2. Lower mini_vpn downlink high/low watermarks now.
   Deferred. This may be correct, but it changes product behavior before we know
   whether the kernel queue is merely too shallow for normal bursts.

3. Add paced/budgeted `flush_tx` now.
   Deferred. It is a likely product direction if qlen A/B helps, but it needs a
   harness model or careful wakeup design to avoid throughput regressions.

4. Relax closed-pending reap.
   Rejected. `TcpState=Closed && can_send=false` cannot drain in smoltcp and was
   already covered in Knife14aj.

5. Treat sing-box or iperf3 as the primary cause.
   Rejected for this stage. Direct client-target and exit-target preflights were
   healthy during the same acceptance run.

## Invariants

- The default suite behavior is unchanged when `TUN_TX_QUEUE_LEN` is unset.
- A configured `TUN_TX_QUEUE_LEN` must be a positive integer.
- The report records both the requested and actual TUN tx queue length.
- The target route still points to the discovered TUN interface, and the exit
  route must not recurse into TUN.
- Existing MTU alignment remains unchanged.
- Probe summaries continue to include TUN drop deltas and pressure attribution.
- Any suite failure caused by an invalid knob fails before expensive probes.

## Acceptance

Local acceptance:

- `bash -n scripts/knife14b-usclient-tunnel-suite.sh` passes.
- `git diff --check` passes.
- Review confirms the knob is opt-in, bounded, Linux-only with the suite, and
  does not leak secrets.

VPS acceptance:

- On `.27`, run the scoped reverse-first suite once with
  `TUN_TX_QUEUE_LEN=5000`, using the existing `.evn` through a quiet source
  wrapper.
- The report includes:
  - `tun_tx_queue_len_requested`;
  - `tun_tx_queue_len_actual`;
  - `ip link show <tun>` after the queue setup;
  - per-probe `tun_tx_dropped_delta`.
- Compare against the previous default-qlen bundle above.
- If qlen=5000 sharply lowers TUN drops and improves reverse P1 throughput, the
  next product branch should design bounded pacing/backpressure rather than
  relying on OS queue length as the final fix.
- If qlen=5000 does not lower drops or improve throughput, the next branch
  should focus on downlink watermarks, flush pacing, or QUIC/path attribution
  based on the pressure counters.
