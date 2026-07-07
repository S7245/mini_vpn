# Knife14eb Gap-Hint Pressure Drain Results

Date: 2026-07-07

## Stage Goal

Escalate relay read-gap ACK/window drain from the small active-flow budget to
the pressure-sized budget, without reopening TUN drops, QUIC blocking, or
hidden close/reap loss.

## VPS Run

- Bundle:
  `/tmp/mini_vpn/mvpn_knife14eb_gap_hint_pressure_drain_p1_30_usclient_suite_20260707_134432.tar.gz`
- Extracted local copy:
  `/tmp/mini_vpn/knife14eb_gap_hint_pressure_drain_p1_30/`
- Direct baselines were healthy:
  `.27 -> .77` receiver about `283 Mbit/s`;
  `.27 <- .77` receiver about `280 Mbit/s`.
- Tunnel reverse-first P1 improved but stayed below target:
  `38.3/37.4 Mbit/s`, `throughput_shape=low_average`.

## Signals

- Gap-hint ACK drain became materially stronger:
  `relay_gap_hint_attempts=120`, with gap hint logs using budget `256`.
- The local egress pressure surfaces stayed clean:
  `tun_tx_dropped_delta=0`, runtime drop events `0`,
  `headroom_limited=0`, `pressure_credit_debt_bytes=0`, and
  `send_queue_max=448888`.
- QUIC loss/congestion/blocking stayed clean:
  all parsed rx/tx blocked, lost-bytes, and congestion deltas were zero.
- Pending/reap accounting stayed clean:
  `terminal_pending_reap=0`, `pending_at_close=0`, and no send-slice or TUN
  flush failures.
- The remaining failure was still cadence:
  data stream `data_read_gap_max_ms=3593`,
  `data_pending_gap_max_ms=3437`, and repeated zero-throughput iperf
  intervals.
- The ACK drain loop still hit its cap in live traffic:
  `tun_rx_drain budget_exhausted=180` while `would_block=22736`.
- `.33` evidence in the same window showed TUIC inbound/direct outbound
  entries for `.27 -> .77`; no current TUIC `fail auth` was present. NTP was
  synchronized and sing-box was active.

## Conclusion

Knife14eb proves the ACK/window starvation branch is real: strengthening
relay-gap drains improved throughput from `26.3` to `37.4 Mbit/s` while keeping
drop, QUIC, close, and pending surfaces clean. It is still insufficient because
each gap hint performs only one bounded drain pass. If that pass exhausts its
budget, remaining queued local ACK/window packets can sit until another event,
leaving second-scale remote read gaps.

## Next Rule

Do not simply raise the fixed packet budget. Add a bounded follow-up drain that
arms only when a relay-gap drain exhausts its current budget. The follow-up
should repeat until TUN RX reaches would-block or local egress headroom/tun
feedback says to stop.
