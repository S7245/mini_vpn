# Knife14ar plan - close-safe egress pacing

Spec:
`docs/tech/2026-07-04-knife14ar-close-safe-egress-pacing-spec.md`

## Design Tree

Rejected branches:

1. Keep tuning `65536B/tick`: rejected by Knife14aq acceptance. It reduced TUN
   drops but worsened throughput and exposed pending-at-close.
2. Remove the pacer entirely: rejected because `tun_flush_deferred` is now a
   useful A/B and observability knob.
3. Make `0` mean "old behavior": rejected because small or zero budgets should
   remain explicit pacing stress knobs. Under Knife14ar, even a zero budget is
   overridden by non-empty pending backlog so the knob cannot strand data at a
   close boundary.
4. Change reap logic to never reap inactive pending: rejected because that can
   leak listener slots and fake-IP refs indefinitely.
5. Tune server or iperf3: rejected by healthy direct baselines and clean QUIC
   evidence.

Chosen branch:

- Make the default immediate egress budget large enough to preserve old
  immediate-flush behavior in normal operation.
- Keep smaller budgets available only when explicitly set by env/suite.
- Change the pacer decision so non-empty `downlink_pending` always forces an
  immediate poll/flush attempt. Budget deferral is allowed only after
  `flush_downlink` leaves no app-owned backlog.
- Extend close diagnostics with `tun_flush_deferred`.

## TDD Slices

1. RED/GREEN: default and missing/invalid env no longer resolve to `65536`.
2. RED/GREEN: pacer allows immediate flush when `pending_bytes > 0`, even if
   the accepted byte count exceeds the current budget.
3. RED/GREEN: pacer still defers when budget is exhausted and no pending
   backlog remains.
4. Update shell suite self-test so its default matches the Rust default.
5. Run focused and broad local verification.
6. Stage code review for lifecycle correctness and hot-path cost.
7. Record learning, commit, push, then run one `.27` clean reverse-first
   acceptance suite.
