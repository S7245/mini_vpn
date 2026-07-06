# Knife14cb Egress-Progress-Clocked Accept Results

Date: 2026-07-06

## Code Under Test

- Commit: `5bf60d9` (`fix(knife14cb): clock downlink accept by egress progress`)
- Branch: `codex/knife14d-downlink-reap-open`
- Client VPS: `.27` (`43.172.75.27`)
- Exit VPS: `.33` (`43.153.32.33`)
- Target VPS: `.77` (`43.130.32.77`)
- Bundle:
  `/tmp/mini_vpn/knife14cb_egress_clock_20260706_1552/mvpn_knife14cb_egress_clock_usclient_suite_20260706_235136.tar.gz`

## Run Shape

- Suite tag: `knife14cb_egress_clock`
- Probe: clean reverse-first P1 only
- MTU: `1200`
- Parallel: `1`
- Duration: `30s`
- TCP diag: enabled
- Server evidence: enabled
- Direct reverse baseline: required
- Exit-to-target baseline: required

## Preflight

- `.27` was fast-forwarded to `5bf60d9`; working tree clean.
- Direct `.27 -> .77` baseline was healthy:
  - forward receiver: `278 Mbit/s`
  - reverse receiver: `279 Mbit/s`
- Exit `.33 -> .77` baseline was healthy:
  - forward receiver: `280 Mbit/s`
  - reverse receiver: `288 Mbit/s`
- `.33` and `.77` clocks were NTP synchronized.
- `.33` sing-box/TUIC accepted the run. The current-window evidence did not
  show TUIC `fail auth`; observed sing-box entries were normal inbound TUIC
  connections and the expected iperf stream close/cancel after the probe.

## Positive Signals

- Reverse-first P1 climbed to `152/152 Mbit/s`.
- This clearly escapes the rejected `10-20 Mbit/s` band and exceeds the
  Knife14by reference of `29.0 Mbit/s`.
- The low-RTT probe classified the run as:
  `throughput_shape=stable_high`.
- No QUIC loss, congestion, or flow-control blocking was observed:
  `max_lost_bytes_delta=0`, `max_congestion_events_delta=0`,
  `max_tx_blocked_data_delta=0`, and `max_rx_blocked_data_delta=0`.
- `send_slice_zero=0`, `send_slice_errors=0`, and
  `tun_flush_failures=0`.
- Final close/reap accounting was clean:
  - `final_pending_at_close: events=0 bytes=0`
  - `final_terminal_pending_reap: events=0 bytes=0`
  - main reverse flow closed with `pending=0`,
    `close_pending_class=none`, and `terminal_pending_reap_bytes=0`.
- The new algorithm was active and observable:
  - final `drain_credit_granted_bytes=274946761`
  - final `drain_credit_planned_bytes=86054519`
  - final `drain_credit_used_bytes=86054519`

## Remaining Negative Signals

- TUN egress drop was still present:
  - probe `tun_tx_dropped_delta=712`
  - final `drop_delta_total=712`
  - `final_tun_egress_feedback: pause_edges=1 resume_edges=1`
- The first drop occurred while local pressure touched
  `max_pressure_bytes=917504`, the current hard tx_queue pause threshold.
- The iperf series still had a two-second zero-throughput gap:
  - `23.00-24.00 sec: 0.00 bits/sec`
  - `24.00-25.00 sec: 0.00 bits/sec`
- The main reverse flow reported terminal late payload:
  - `terminal_late_remote_payload_bytes=3128992`
  - `terminal_late_remote_payload_events=200`
- There was one global receive queue pressure event:
  `global_rx_wait_max_us=73635`, `global_rx_queue_used_max=1024`.

## Interpretation

Knife14cb validates the core hypothesis that fixed queue-occupancy acceptance
was too conservative: clocking accept by observed local egress progress restored
reverse throughput from the `10-20 Mbit/s` band to `152 Mbit/s`, while keeping
pending-at-close and terminal-reap accounting clean.

This is not yet a full Knife14 acceptance. The remaining bottleneck is no
longer hidden pending/reap loss; it is a local egress burst/feedback problem
near the hard pause threshold. The next patch should keep the progress-clocked
accept model but prevent credit spending from repeatedly driving smoltcp/TUN
egress to the hard-pause edge.

## Next Bias

- Do not revert to static threshold tuning.
- Do not reopen stale pool, iperf3, sing-box, QUIC, or egress pacer branches
  unless new evidence appears.
- Add TDD around credit-spend smoothing or drop-aware credit debt so progress
  credit cannot spend all the way to the hard pause threshold after a TUN drop.
- Preserve final lifecycle summaries; they are now the discriminator that
  proved pending/reap is clean while TUN egress is still not fully clean.
