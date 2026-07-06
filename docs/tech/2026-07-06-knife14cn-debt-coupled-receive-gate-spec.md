# Knife14cn Debt-Coupled Receive Gate Spec

Date: 2026-07-06

## Goal

Repair the Knife14cm failure where pressure credit debt was installed at the
tx-queue credit edge but remote receive continued, growing pending bytes until
close-time deferral exposed the backlog.

The target is not a new threshold. The target is to make active local egress
debt participate in the receive-window decision so the remote reader stops
adding bytes while the local send queue is already credit-edge constrained.

## Evidence

Knife14cm valid VPS run:

- `tcp-egress-credit-debt reason=pressure_credit_edge installed_bytes=24576`
- debt installed at `pending=9074 max_tx_queue=892928`
- subsequent remote payloads showed `accepted_bytes=0` while pending grew:
  `24562`, `58354`, `91505`, and eventually `541802`
- `pressure_credit_debt_bytes=42090`
- `pressure_credit_debt_paid_bytes=0`
- `tun_tx_dropped_delta=4056`
- `tcp-deferred-close-pending ... pending=541802`
- reverse-first P1 stayed low at `20.4/19.1 Mbit/s`

Clean discriminators:

- direct `.27/.33 -> .77` baselines were healthy
- current `.33` checks showed no TUIC `fail auth`
- QUIC loss/congestion/blocking stayed zero
- `send_slice_zero=0`, `send_slice_errors=0`, `tun_flush_failures=0`
- close/reap accounting was visible rather than hidden

## Non-Goals

- Do not tune stale pool, iperf3, sing-box, QUIC congestion, or the egress
  pacer from this evidence.
- Do not widen static backpressure thresholds or receive windows.
- Do not move behavior into scripts; scripts only validate the core fix.
- Do not make active debt a permanent receive deadlock.

## Invariants

- With no active drop/pressure credit debt, existing downlink backpressure
  hysteresis stays unchanged.
- When active credit debt exists and local pressure is at the credit-debt edge,
  remote receive pauses before more pending can accumulate.
- Recovery still uses the existing low/resume threshold. Active debt alone must
  not keep receive paused after local pressure has drained to the ordinary
  recovery point.
- Debt payment remains tied to observed send-queue drain. No credit is minted
  from elapsed time or pending size alone.

## Acceptance

Local:

- Add a RED/GREEN test proving active credit debt pauses receive at the credit
  edge even though the ordinary hard pause edge has not been reached.
- Prove the same helper resumes once pressure reaches the existing recovery
  threshold.
- Existing backpressure, pressure-credit, drop-credit, close, and reap tests
  continue to pass.

VPS:

- Reverse-first P1 must not fall back to no-data.
- Desired improvement: `pressure_credit_debt_paid_bytes > 0`, lower
  `send_queue_max`, lower final pending, or a materially cleaner TUN-drop
  profile versus Knife14cm.
- `pending_at_close`, `egress_at_close`, `terminal_pending_reap`, and
  `terminal_late_remote_payload` remain visible and explainable.
- `.33` must still show no current TUIC `fail auth` before blaming mini_vpn
  throughput behavior.
