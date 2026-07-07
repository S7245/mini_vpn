# Knife14dq TUIC Stream Read Gap Results

Date: 2026-07-07

## Stage Goal

Finish the Knife14as close/downlink lifecycle attribution after the clean
reverse-first result regressed. The immediate goal was to decide whether the
remaining throughput gap was still hidden in local close-drain, receive-window,
terminal pending, or dead-slot reap accounting, or whether a new branch had
enough evidence to take over.

## Runs

### knife14do: clean d85 baseline before sing-box restart

- Bundle:
  `/tmp/mini_vpn/knife14do_d85_clean_baseline_p1_30_20260707_101108/mvpn_knife14do_d85_clean_baseline_p1_30_usclient_suite_20260707_101108.tar.gz`
- Binary hash:
  `040e2ad8760eee58c10d62386502a23d2c5a1471d5e6fcf8e8d57931346b617e`
- Direct baselines were healthy for `.27 <-> .77` and `.33 <-> .77`.
- Reverse-first P1 failed as near no-data: `384 Kbit/s` sender,
  `8.28 Kbit/s` receiver.
- Local loss surfaces were clean: no local/global RX pressure, no pending at
  close, no terminal pending reap, no TUN drops, and no QUIC
  loss/congestion/blocking.

### knife14dp: clean d85 after sing-box restart

- Bundle:
  `/tmp/mini_vpn/knife14dp_d85_after_singbox_restart_p1_30/mvpn_knife14dp_d85_after_singbox_restart_p1_30_usclient_suite_20260707_101638.tar.gz`
- `.33` sing-box was restarted at `2026-07-07 10:15:35 CST`; service returned
  active and NTP stayed synchronized.
- Direct baselines stayed healthy:
  `.27 -> .77` about `277 Mbit/s` receiver,
  `.27 <- .77` about `278 Mbit/s` receiver,
  `.33 -> .77` about `260 Mbit/s` receiver, and
  `.33 <- .77` about `281 Mbit/s` receiver.
- Reverse-first P1 improved from no-data but remained low:
  `20.8/19.9 Mbit/s`.
- d85 still exposed local egress pressure during/after the run:
  runtime `tun_tx_dropped_delta` reached `7312`, feedback stayed paused, and
  the data stream closed with `max_read_gap_ms=4251`.

### knife14dq: current dirty code after sing-box restart, forced pool=2

- Bundle:
  `/tmp/mini_vpn/knife14dq_current_after_singbox_restart_pool2_p1_30/mvpn_knife14dq_current_after_singbox_restart_pool2_p1_30_usclient_suite_20260707_101838.tar.gz`
- Binary hash:
  `0476678f6d9c66791fb1b2696abc6710f5c1d5b2719d25e9f710982d594a03ba`
- Direct baselines stayed healthy:
  `.27 -> .77` about `280 Mbit/s` receiver,
  `.27 <- .77` about `279 Mbit/s` receiver,
  `.33 -> .77` about `283 Mbit/s` receiver, and
  `.33 <- .77` about `294 Mbit/s` receiver.
- Reverse-first P1 remained low average: `30.9/29.9 Mbit/s`.
- Local close/lifecycle surfaces were clean:
  `pending_at_close=0`, `egress_at_close=0`,
  `terminal_pending_reap=0`, `terminal_late_remote_payload=0`,
  `send_slice_zero=0`, `send_slice_errors=0`,
  `tun_flush_failures=0`, `tun_flush_deferred=0`, and
  `tun_tx_dropped_delta=0`.
- QUIC loss/congestion/blocking stayed clean:
  `max_lost_bytes_delta=0`, `max_congestion_events_delta=0`,
  `max_tx_blocked_*_delta=0`, `max_rx_blocked_*_delta=0`.
- The remaining signal was stream cadence:
  `tuic_stream_pending data_pending_gap_max_ms=3413`,
  data stream close `max_read_gap_ms=3414`, and interval throughput repeatedly
  dropped to zero despite no local pressure signal.

## Sing-box/Auth/Time Findings

- Current-window `.33` logs showed no TUIC `fail auth`.
- `.27`, `.33`, and `.77` clocks were synchronized in the same window.
- Restarting `.33` sing-box did not recover stable high throughput.
- Direct `.33 <-> .77` iperf stayed healthy after restart, so the target path
  and iperf3 service are not the current bottleneck.

## Conclusion

The original Knife14as close/downlink lifecycle question is now mostly closed:
current code makes pending/close/reap accounting explicit and clean in the
failing reverse-first P1 window. The remaining throughput gap is not hidden in
terminal pending, close egress, dead-slot reap, local/global RX pressure, TUN
drops, or QUIC loss/congestion.

The next branch should target TUIC TCP stream read cadence between sing-box and
the client relay. One concrete local candidate is the current
`drain_ready_remote_reads` implementation: it manually polls the remote reader
with a no-op waker while trying to drain already-ready bytes. If that poll
reaches `Pending`, it can overwrite the stream's real task waker and force the
next data burst to wait for a timer or unrelated event. This must be proven with
a focused deterministic test before the next patch.

## Next Patch Plan

1. Add a deterministic test that proves a ready-drain helper must not replace a
   registered real waker with a no-op waker when the underlying stream returns
   `Pending`.
2. Replace the no-op-waker ready drain with a safe drain pattern, or remove the
   speculative ready-drain path if the safe pattern is not clearly better.
3. Keep the change scoped to relay/TUIC stream read cadence; do not retune
   egress thresholds, pool size, sing-box, or iperf3.
4. Re-run local tests first, then one clean reverse-first VPS run only after
   the deterministic test explains the cadence failure.
