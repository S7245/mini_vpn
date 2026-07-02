# Knife14r: deferred close must be bounded by drain progress

## Grounding

- Project goal comes from `AGENTS.md`: mini_vpn is the VPN data-plane core, so
  lifecycle correctness and stable high-pressure TCP behavior are higher
  priority than cosmetic cleanup.
- Test bundle:
  `/tmp/mvpn_knife14q_usclient_suite_20260702_160853.tar.gz`.
- Knife14q confirms the previous uplink fix worked: `remote_write_timeout` no
  longer appears, and reverse iperf completes instead of timing out.
- The same run still shows `dead_slot_reap` closing slots with non-zero
  `downlink_pending`, including `state=Closing pending=215676` and
  `state=Closing pending=8653`.
- Downlink backpressure now engages at the configured high watermark and resumes
  near the low watermark, so this stage should not remove or bypass that
  bounded queue behavior.

## Problem

Knife14o added a fixed 5 second grace after a relay close when
`downlink_pending` still exists. That prevents immediate tail loss, but it still
treats a slowly draining queue as stuck once the original close timestamp ages
out.

That is the wrong invariant for a VPN data plane. The question is not "has it
been 5 seconds since close?", it is "has the pending downlink stopped making
progress for the bounded grace window?"

## Goal

When a relay close has been deferred because there are pending downlink bytes,
keep the slot alive while the pending buffer is still decreasing. Reap only when
the deferred close has made no observable drain progress for the grace window.

## Non-Goals

- Do not change TUIC stream write semantics from knife14q.
- Do not retune the high/low downlink backpressure watermarks.
- Do not claim the forward P2/P4/P8 throughput collapse is fixed until the next
  VPS suite proves it.
- Do not keep closed slots forever: no-progress deferred pending must remain
  bounded.

## Invariants

- A relay close with pending downlink bytes stores a deferred-close record and
  keeps the handle dirty.
- Any decrease in `downlink_pending.len()` after the deferred close refreshes the
  no-progress deadline.
- A deferred close with pending bytes is reapable only after the pending length
  has failed to decrease for `DEFERRED_CLOSE_PENDING_GRACE_SECS`.
- Once pending drains to zero, the deferred close is finished through the normal
  rearm path and bumps `conn_epoch`.
- Non-deferred inactive sockets remain reapable to avoid permanent slot leaks.

## Acceptance

- Local unit tests cover the new progress-sensitive reap predicate and progress
  bookkeeping.
- `cargo test --lib client_tun`, full `cargo test`, `cargo clippy --all-targets
  -- -D warnings`, and `git diff --check` pass.
- Next VPS run should show no `tcp-handle-close ... reason=dead_slot_reap
  state=Closing pending>0` while send_slice errors and TUN flush failures remain
  zero.
