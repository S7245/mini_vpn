# Knife14bv Close Egress Drain Spec

## Grounding

Knife14bu rejected a simple 25ms durable-pressure hold as the throughput fix.
The scoped `.27` VPS run on commit `fd3f2ed` still failed reverse-first P1 at
`17.3/15.3 Mbit/s`.

The important new evidence:

- direct `.27 <-> .77` and `.33 <-> .77` baselines were healthy;
- `.33` TUIC/sing-box startup and server evidence were clean for the probe;
- clean-window QUIC loss, congestion, and blocking deltas stayed zero;
- `tun_tx_dropped_delta=0`;
- `terminal_pending_reap=0`, `terminal_late_remote_payload=0`, and
  `pending_at_close=0`;
- `downlink_backpressure` churn increased to `63/62`;
- the data handle closed with `tcp_state=CloseWait`, `may_recv=false`,
  `can_send=true`, `may_send=true`, `pending=0`, and `send_queue=524288`.

The missing accounting is that current close classification only reports
app-owned `downlink_pending`; it does not classify smoltcp queued egress bytes.
Therefore the close line can say `close_pending_class=none` while the local TCP
send queue is still at the high watermark.

## Design Tree

1. Lengthen the 25ms pressure hold.
   - Rejected. Knife14bu removed TUN drops but worsened throughput and increased
     pause/resume churn. More hold is another timing guess.

2. Return to sing-box, iperf3, stale pool, TUN qlen, or QUIC tuning.
   - Rejected. Current evidence keeps those branches clean for this failure.

3. Add close-egress observability first.
   - Accepted. Before changing lifecycle behavior, logs must distinguish app
     pending from smoltcp egress queue at close and raw pressure from held
     effective pressure.

4. Add a bounded close/egress drain rule.
   - Accepted only after TDD. If a relay close arrives while the local socket is
     still active/send-capable and `send_queue >= high`, rearm should be
     deferred briefly so queued egress can drain through normal TUN flush/poll.

## Stage Goal

Make close-time egress pressure explicit and, if deterministic tests prove the
current lifecycle can rearm while useful smoltcp egress is queued, defer that
rearm until the tx queue drains below the low watermark or a short bounded grace
expires.

## Non-Goals

- Do not change TUIC auth, sing-box config, iperf3, stale pool, TUN qlen, or
  QUIC congestion settings.
- Do not keep tuning the pressure-hold duration as the primary fix.
- Do not make `scripts/` the product surface; script changes are allowed only if
  parser/acceptance visibility needs them.
- Do not retain unbounded relay slots or unbounded local close waits.

## Invariants

- App-owned pending bytes remain accounted separately from smoltcp egress queue
  bytes.
- Terminal no-send close with app pending still reports terminal pending reap.
- Empty app pending plus non-empty send queue must be visible in close logs.
- Deferred close drain must be bounded and must not keep unrelated sessions open.
- A socket that is not active/send-capable must still rearm promptly.
- Release builds should be warning-clean before VPS acceptance.

## Acceptance

Local observability:

- Release build no longer warns about the test-only pressure wrapper.
- A focused test proves close accounting reports queued egress even when
  app-owned pending is zero.
- A focused test proves the backpressure log can report both raw and effective
  pressure when the hold is active.

Local behavior:

- A focused test first fails, then proves relay close is deferred for
  `CloseWait + send_queue >= high + can_send + pending=0`.
- A focused test proves the deferred close finishes once tx queue drains to/below
  low.
- A focused test proves the deferred close remains bounded after its grace.
- Existing close/reap/backpressure/TUN feedback tests pass.
- Full `cargo test --lib`, release build, and `git diff --check` pass.

VPS:

- Scoped reverse-first P1 starts with `high=524288B low=131072B`.
- Required safety: no terminal pending reap regression, no terminal late payload
  regression, no send-slice errors, no TUN flush failures, no clean-window QUIC
  loss/congestion/blocking.
- Desired signal: fewer stop/go zero-throughput intervals and receiver result
  leaves the 10-20 Mbit/s band.

