# Knife14ec Gap Follow-Up Results

Date: 2026-07-07

## Stage Goal

After Knife14eb showed stronger relay-gap ACK drain helped but remained below
target, add a bounded follow-up drain that only arms when a relay-gap drain
itself exhausts its budget. The goal was to drain residual ACK/window backlog
without raising a static packet budget.

## Code Result

- Added `relay_gap_hint_followup_attempts` to TUN RX drain diagnostics.
- Added `RelayGapAckDrainState` and tests for coalescing follow-up arms.
- Added `tun_rx_drain_budget_for_relay_gap_followup`, using the same
  pressure-sized budget and egress-headroom taper as relay-gap hints.
- Local gates passed:
  `cargo test relay_gap --lib`,
  `cargo test tun_rx_drain --lib`,
  `cargo test relay_ --lib`,
  `cargo test --lib`,
  `git diff --check`,
  script syntax checks, and `cargo build --release`.
- `.27` focused tests and release build passed after sourcing the remote cargo
  environment explicitly.

## VPS Run

- Bundle:
  `/tmp/mini_vpn/mvpn_knife14ec_gap_followup_p1_30_usclient_suite_20260707_135859.tar.gz`
- Extracted local copy:
  `/tmp/mini_vpn/knife14ec_gap_followup_p1_30/`
- Direct baselines were healthy:
  `.27 -> .77` receiver about `298 Mbit/s`;
  `.27 <- .77` receiver about `298 Mbit/s`.
- Tunnel reverse-first P1 failed:
  `23.2/21.5 Mbit/s`, `throughput_shape=low_average local_pressure=1`.

## Signals

- The new relay-gap follow-up did not trigger:
  `relay_gap_hint_followup_attempts=0`.
- Therefore the previous assumption was wrong: relay-gap drains were not the
  drains that reached the cap.
- TUN RX drain still hit caps elsewhere:
  `budget_exhausted=210`, with `remote_payload_deferred_attempts=812`.
- Local pressure reappeared without TUN drops:
  `send_queue_max=724376`, `headroom_limited=706`,
  `headroom_deferred_bytes=112324991`, and
  `pressure_credit_debt_bytes=196608`.
- Close/reap loss remained visible rather than hidden:
  `terminal_pending_reap=0`, `pending_at_close=0`,
  `terminal_late_remote_payload=5632`, and `egress_at_close=2816`.
- QUIC and TUN drop surfaces stayed clean:
  no QUIC loss/congestion/blocking and `tun_tx_dropped_delta=0`.
- Stream cadence remained the throughput limiter:
  data stream `data_read_gap_max_ms=4267` and
  `data_pending_gap_max_ms=4267`.

## Conclusion

Knife14ec is a useful negative result. The follow-up algorithm was locally
correct but attached to the wrong trigger. In the VPS run, relay-gap drains did
not exhaust, so the follow-up path never ran. The exhausted work is in the
remote-payload/deferred ACK drain path, whose healthy default still uses the
small active-flow budget.

## Next Rule

Keep the relay-gap follow-up diagnostic, but move the adaptive escalation to
the deferred ACK drain path: start with the active-flow budget after a remote
payload, then if that pass exhausts its budget, re-arm the next deferred pass
in pressure mode. Pressure-mode deferred drains must still stop on would-block,
TUN feedback pause, or the local egress headroom curve.
