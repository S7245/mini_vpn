# Knife14cm Proactive Pressure Credit Edge Spec

Date: 2026-07-06

## Goal

Repair the TCP reverse/downlink pressure-control path after Knife14cl removed
the hidden local-close rearm. The target is to keep useful pending downlink
bytes moving without letting the local smoltcp/TUN egress path hit the hard
tx-queue edge first and only then installing credit debt.

## Evidence

Knife14cl improved reverse-first P1 from the former 10-20 Mbit/s band to
`32.9/31.9 Mbit/s`, and close accounting no longer hid a loss point:

- `tcp-deferred-close-pending ... direction=local_to_remote ... pending=528364`
- `pending_at_close=0`
- `egress_at_close=0`
- `terminal_pending_reap=0`
- `terminal_late_remote_payload=0`

The remaining failure is local egress pressure recovery:

- `pending_total=528364`
- `send_queue_max=892928`
- `tun_tx_dropped_delta=4604`
- `pause_edges=2 resume_edges=0`
- `drop_credit_debt_paid_bytes=0`
- `pressure_credit_debt_paid_bytes=0`

Existing tests model pressure debt at the credit edge, but the runtime
installation path is still tied to a backpressure pause edge. That means the
hot path can spend credit until it reaches the hard edge, then discover the
debt too late.

## Non-Goals

- Do not tune stale pool, iperf3, sing-box, QUIC congestion, or egress pacer
  settings from this evidence.
- Do not widen static tx-queue thresholds or receive windows as the fix.
- Do not add script-only logic; scripts may validate, but the behavior belongs
  in the core data-plane.

## Design Tree

1. **Close lifecycle still hides bytes**
   - Rejected for this stage: Knife14cl has explicit pending deferral and clean
     parser close accounting.
2. **ACK/TUN-RX drain cannot reach ready ACKs**
   - Rejected for this stage: Knife14ck/cl show ACK drain mostly reaches
     `would_block`, so simply increasing the budget is not justified.
3. **Pressure debt is installed too late**
   - Supported: pressure tests name the credit edge, while the runtime install
     gate requires `!was_paused && is_paused`.
   - Supported: Knife14cl shows local pressure reached the hard edge and drop
     feedback paused without recovery.
4. **Debt is installed but cannot be repaid**
   - Still tracked: repayments must remain tied to observed send-queue drain.
     This stage must not mint credit from time or from pending size alone.

## Invariants

- Clean headroom up to `tx_queue_flush_threshold` remains usable.
- Extra credit above clean headroom is only available after observed local
  egress drain.
- Newly installed drop or pressure debt clears stale credit immediately.
- Pressure debt may be installed at the credit edge without waiting for a pause
  edge, but it must be bounded by the existing credit span.
- Repeated pressure observations must not grow debt unbounded while the same
  pressure generation is already active.

## Acceptance

Local:

- A focused RED test proves credit-edge pressure can install debt even when
  downlink backpressure has not yet paused.
- Existing drop/pressure credit tests continue to prove stale credit is blocked
  and debt payment precedes new credit.
- `cargo test pressure_credit --lib`
- `cargo test drop_credit --lib`
- Relevant downlink/close/reap local regressions pass.

VPS:

- Reverse-first P1 should not fall back into the 10-20 Mbit/s band.
- `pressure_credit_debt_bytes` should be non-zero when the credit edge is hit.
- `pressure_credit_debt_paid_bytes` or lower `send_queue_max` should show
  recovery/proactive control, not just late drop feedback.
- `pending_at_close`, `egress_at_close`, `terminal_pending_reap`, and
  `terminal_late_remote_payload` remain zero or explained.
