# Knife14au plan - pending-at-close taxonomy

Date: 2026-07-04

## Stage Goal

Close the reporting gap between terminal pending and broader close-time
pending, then use a scoped VPS run to decide whether reverse-first low
throughput still correlates with downlink lifecycle state.

## Design Tree

1. Change close-drain behavior immediately.
   Rejected for this slice. Knife14at did not reproduce the low reverse window,
   and behavior changes without stronger classification risk tuning the wrong
   branch.

2. Keep relying on raw `tcp-handle-close` inspection.
   Rejected. Acceptance reports need machine-readable counters so repeated VPS
   runs do not hide active no-send close tails.

3. Add behavior-neutral close pending taxonomy.
   Selected. It preserves the terminal accounting already accepted in Knife14as
   and adds enough classes to distinguish terminal loss, active no-send, and
   still-send-capable cleanup.

## Tasks

1. Add `ClosePendingClass` and close pending accounting in `src/client_tun.rs`.
2. Log `close_pending_class` and `close_pending_bytes` on every
   `tcp-handle-close` line.
3. Extend `scripts/knife14b-lowrtt-probe.sh` to summarize all pending-at-close
   classes while keeping the existing `terminal_pending_reap` line.
4. Add/extend Rust and shell self-tests for terminal and active no-send close
   pending.
5. Add `pending_at_close` to the suite report grep.
6. Run local gates.
7. Commit/push this behavior-neutral observability task.
8. Run scoped VPS acceptance from `.27`, sourcing its existing `.env` without
   printing secrets and using a real TTY for any sudo prompt.

## Regression Checks

- No data-plane behavior should change: reap predicates, pacer budgets,
  backpressure thresholds, TUIC pool handling, and iperf/sing-box settings stay
  untouched.
- Old logs without `close_pending_class` must still parse terminal pending from
  `pending>0 tcp_state=Closed active=false can_send=false`.
- New logs with `active=true can_send=false pending>0` must appear in
  `pending_at_close` and `pending_close_active_no_send` attribution.
