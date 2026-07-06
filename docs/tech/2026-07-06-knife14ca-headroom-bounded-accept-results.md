# Knife14ca Headroom-Bounded Accept Results

Date: 2026-07-06

## Commits And Artifacts

- Code commit: `82003b8` (`fix(knife14ca): bound downlink accept by egress headroom`)
- VPS bundle:
  `/tmp/mini_vpn/knife14ca_headroom_accept_20260706/mvpn_knife14ca_headroom_accept_usclient_suite_20260706_224238.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14ca_headroom_accept_20260706_local/`

## Run Shape

- Client: `.27` (`43.172.75.27`), repo at `82003b88`, clean worktree.
- Exit: `.33` (`43.153.32.33`), sing-box active and accepting TUIC traffic.
- Target: `.77` (`43.130.32.77`), iperf3 active on TCP `:5201`.
- Suite mode: `RUN_REVERSE_FIRST_P1=1`, `STOP_AFTER_REVERSE_FIRST_P1=1`,
  `SERVER_EVIDENCE_CHECK=1`, `EXIT_TO_TARGET_IPERF_CHECK=1`,
  `DIRECT_IPERF_REVERSE_CHECK=1`, `DIRECT_IPERF_REVERSE_REQUIRED=1`,
  `PARALLEL_SET=1`, `DURATION=30`, `MTU=1200`, `MINI_VPN_TCP_DIAG=1`.
- Startup config confirmed `tx_queue_flush_high=720896B`,
  `tx_queue_pause_high=917504B`, and `tx_queue_resume_high=524288B`.

## Baselines

- `.27 -> .77` direct forward: `337/295 Mbit/s` sender/receiver.
- `.27 <- .77` direct reverse: `303/277 Mbit/s` sender/receiver.
- `.33 -> .77` direct forward: `307/278 Mbit/s` sender/receiver.
- `.33 <- .77` direct reverse: `322/295 Mbit/s` sender/receiver.
- `.33` and `.77` reported NTP synchronized.
- `.33` current-window evidence showed TUIC inbound and direct outbound lines
  for `.77:5201`. No current-window TUIC `fail auth` signal appeared.

## Result

Reverse-first P1 regressed and failed the Knife14 throughput goal:

- iperf sender: `16.1 Mbit/s`
- iperf receiver: `14.8 Mbit/s`
- shape: `low_average`
- parser attribution during the clean P1 summary: `local_downlink_backpressure`

The interval profile remained burst/idle and worse than Knife14bz. Strong
one-second bursts still appeared (`61.8`, `88.1`, `79.7 Mbit/s`), but many
adjacent seconds were `0.00 bits/sec`.

## Key Signals

- The new algorithm was active: `send_queue_max=720896`, exactly the configured
  `tx_queue_flush_high`, and lower than the overshoot seen in Knife14bz.
- Clean P1 summary:
  `headroom_limited=1094`, `headroom_deferred_bytes=203578857`,
  `tun_flush_deferred=0`, `send_slice_zero=0`, `send_slice_errors=0`,
  `tun_flush_failures=0`.
- Final close evidence after the parser summary showed the limiter becoming a
  receive-window stall:
  `may_recv_false=11897`, `budget_limited_calls=12291`,
  `headroom_limited_calls=12973`, and
  `headroom_deferred_bytes=3317103134`.
- Final close had useful pending data, but the socket was still send-capable:
  `pending=566509`, `close_pending_class=active_send_capable`,
  `terminal_pending_reap_bytes=0`, `tcp_state=CloseWait`, `can_send=true`,
  `may_recv=false`, `send_queue=720896`.
- Final suite logs showed post-summary TUN egress drops despite the cap:
  `drop_delta_total=2691`, `max_delta=1349`, `max_pressure=720896`.
- QUIC remained clean in the observed window: no client-side loss,
  congestion, stream/data blocking, or inherited low-cwnd signal.
- TUIC stream 4 closed with `rx_bytes=60461840`, `reads=3661`,
  `max_read_gap_ms=4731`, and `max_pending_gap_ms=4730`.

## Interpretation

Knife14ca proved the pre-`send_slice` headroom cap works mechanically, but it
disproved it as the throughput fix. The cap prevented queue overshoot above
`720896B`, yet throughput dropped from Knife14bz's `19.5 Mbit/s` receiver rate
to `14.8 Mbit/s`. The limiter moved the failure from "overshoot and later TUN
drop" into "sticky capped egress queue plus receive-window starvation."

This is not a case for more static threshold tuning. The evidence says a fixed
queue-occupancy ceiling is too blunt: it can withhold remote bytes long enough
to create long read gaps and pending backlog, while still not preventing late
TUN/qdisc drops when the capped queue is flushed under pressure.

The remaining root is still mini_vpn local TCP downlink lifecycle and egress
cadence, not stale pool, iperf3, sing-box auth, server time sync, QUIC loss, or
TUN syscall errors. The next algorithm needs to couple remote acceptance to
actual local egress progress, not only to the current smoltcp queue length.

There is also a parsing/acceptance lesson: the low-RTT attribution summary was
generated before the final close/drop lines, so it reported `pending_at_close=0`
and `tun_drops=0` while the suite-level final logs later showed
`pending=566509` and `drop_delta_total=2691`. Future acceptance must parse the
whole bundle, including final lifecycle snapshots, before declaring pending,
close, reap, or TUN-drop accounting clean.

## Next Candidate Plan

Stop before another behavior edit and do an architecture checkpoint for the
downlink accept cadence:

1. Add or fix parser coverage so final post-summary `tcp-handle-close`,
   `tcp-tun-egress`, and `tcp-tun-egress-feedback` lines are included in the
   stage summary.
2. Add deterministic tests for a capped queue that later drains, proving
   pending bytes resume only after actual egress progress and do not accumulate
   indefinitely behind `may_recv=false`.
3. Replace the static pre-send occupancy cap with an egress-progress-clocked
   accept rule: remote reads should advance according to observed local drain
   progress and bounded per-loop quantum, not just remaining headroom below a
   fixed queue threshold.
4. Keep close/reap pending accounting strict, including send-capable
   `CloseWait` pending data and post-summary terminal/drop events.
5. Re-run the same reverse-first P1 suite only after local tests and parser
   self-tests pass.

Current overall Knife14 progress estimate: about `78%`. The percentage moved
down because Knife14ca rejected the first algorithmic cap, but it also exposed
a sharper next target: the fix must be egress-progress-clocked rather than
threshold-clocked.
