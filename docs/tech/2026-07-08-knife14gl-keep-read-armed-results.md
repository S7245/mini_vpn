# 2026-07-08 Knife14gl Keep Read Armed Results

## Scope

This stage tested whether the current branch lost `f8765c1`'s data-moving
shape because non-pausing read-credit updates were cancelling an in-flight TUIC
ordered stream read.

Non-goals stayed unchanged: no VPS tuning, no iperf3 changes, no MTU/PLPMTUD
work, no stale-pool work, no unordered production path, and no broad QUIC
window changes.

## Code Change

- Commit: `18ed16ef` (`fix(knife14gl): keep relay reads armed across credit updates`)
- File changed: `src/client_tun.rs`
- Main behavior: `run_relay_reader` no longer races a normal
  `remote_reader.read(...)` against `read_credit_rx.changed()`. Credit updates
  are still drained before the next read and still gate reads when credit is
  paused, but they no longer cancel/recreate an already pending remote read.

## TDD Gates

The focused test first failed on the pre-fix implementation because a
non-pausing credit update repolled the pending stream read:

- `cargo test --lib relay_read_credit_update_keeps_inflight_remote_read_armed`

After the code change, these local gates passed:

- `cargo test --lib relay_read_credit_update_keeps_inflight_remote_read_armed`
- `cargo test --lib relay_read_credit_pause_stops_remote_reads_until_resumed`
- `cargo test --lib relay_pressure_credit_caps_first_awaited_remote_read`
- `cargo test --lib relay_read_credit`
- `cargo test --lib relay_remote`
- `rustfmt --edition 2024 --check src/client_tun.rs`
- `cargo build --release`
- `git diff --check`

## Acceptance Artifacts

- Remote report:
  `/tmp/conn/mvpn_knife14gl_keep_read_armed_safe1200_p1_30_usclient_suite_20260708_164002.md`
- Remote bundle:
  `/tmp/conn/mvpn_knife14gl_keep_read_armed_safe1200_p1_30_usclient_suite_20260708_164002.tar.gz`
- Local report:
  `/tmp/mini_vpn/knife14gl_keep_read_armed_safe1200_p1_30/mvpn_knife14gl_keep_read_armed_safe1200_p1_30_usclient_suite_20260708_164002.md`
- Local bundle:
  `/tmp/mini_vpn/knife14gl_keep_read_armed_safe1200_p1_30/mvpn_knife14gl_keep_read_armed_safe1200_p1_30_usclient_suite_20260708_164002.tar.gz`
- Client log:
  `/tmp/conn/mvpn_accept_20260708_164002.log`

The `.27` acceptance worktree was detached at `18ed16ef` and clean.

## Preflight Health

The suite used explicit ordered/safe1200 settings:

- `MINI_VPN_TUIC_TCP_UNORDERED_REASSEMBLY=0`
- `MINI_VPN_TUIC_MTU_POLICY=safe1200`
- `MINI_VPN_TUIC_MTU_MODE=safe1200`
- `MINI_VPN_TUIC_TCP_POOL=2`
- `MTU=1200`

Startup confirmed:

- `relay_mode=ordered_join`
- `QUIC MTU policy=safe1200`
- `dg_max=Some(1166)`

Direct baselines were healthy:

- `.27 -> .77` forward: `318 Mbit/s` sender, `274 Mbit/s` receiver.
- `.27 <- .77` reverse: `290 Mbit/s` sender, `259 Mbit/s` receiver.
- `.33 -> .77` forward: `309 Mbit/s` sender, `280 Mbit/s` receiver.
- `.33 <- .77` reverse: `316 Mbit/s` sender, `283 Mbit/s` receiver.

## Acceptance Outcome

The focused safe1200 reverse-first P1 did not reach data-moving throughput:

- iperf sender: `280 Kbit/s`
- iperf receiver: `16.2 Kbit/s`
- `remote_to_global_rx_bytes=60704`
- `remote_reads=11`
- `remote_read_service_ticks=11`
- `remote_read_service_len_min=65536`
- `remote_read_service_len_max=65536`

The code change did improve the earliest ordered-stream behavior:

- `knife14gj` current before the fix:
  - receiver: `63.3 Kbit/s`
  - data stream first useful read: `first_rx_ms=17266`
  - `remote_to_global_rx_bytes=237264`
  - `max_remote_read_gap_ms=6839`
- `knife14gl` after the fix:
  - receiver: `16.2 Kbit/s`
  - data stream first useful read: `first_rx_ms=3`
  - `remote_to_global_rx_bytes=60704`
  - `max_remote_read_gap_ms=17096`
- `knife14gk` `f8765c1` A/B in the same service window:
  - receiver: `14.8 Mbit/s`
  - data stream first useful read: `first_rx_ms=3`
  - `remote_to_global_rx_bytes=55472855`
  - `remote_reads=7008`

Clean local and QUIC surfaces remained clean in `knife14gl`:

- `tun_tx_dropped_delta=0`
- `send_slice_zero=0`
- `send_slice_errors=0`
- `tun_flush_tx_failures=0`
- `pending_total=0`
- `terminal_pending_reap=0`
- `pressure_credit_debt_bytes=0`
- `pressure_credit_blocked_bytes=0`
- `headroom_deferred_bytes=0`
- QUIC loss/congestion/tx-blocking/rx-blocking deltas were zero in the shown
  clean window.

The dominant repeated signal was still ordered stream prefix starvation:

- `pending_cause=connection_stream_frames_pending`
- `conn_rx_stream_frames_since_read` stayed non-zero while ordered reads were
  pending.
- `self_wake_armed/self_wake_fired` climbed into the thousands.
- The data stream stalled at `60704B`, which is below
  `RELAY_ACK_DRAIN_HINT_MIN_DATA_BYTES=64KiB`; therefore the current
  `relay_ack_drain_hint_due(...)` gate never produced a gap hint on the data
  stream (`ack_drain_hint_due=0`, `ack_drain_hint_sent=0`).

## Interpretation

The TDD slice was valid but too narrow for the `100+ Mbit/s` goal. It removed
one current-vs-`f8765c1` regression: a non-pausing credit update can no longer
cancel the active TUIC ordered stream read, and `first_rx_ms` returned to
`f8765c1`-like timing.

However, restoring the first read did not restore sustained data movement. The
next layer is not local pressure-credit yet: `knife14gl` has effectively no
local pressure, no pending backlog, no TUN drops, and no QUIC loss/blocking.
Instead, the ordered stream enters a long `connection_stream_frames_pending`
gap after only `60704B`, just below the current 64KiB ACK-drain hint data
threshold.

This explains why the earlier local TDD did not achieve `20M -> 100M+`: the
tests covered relay-reader cancellation semantics and pressure-credit slices,
but not the early ordered-stream gap where useful data is below the current
gap-hint minimum. VPS acceptance exposed that missing case.

## Stopped Next Step

Per the operator request, this stage stops here. The next stage should not run
automatically.

The next code plan, when resumed, should be another narrow TDD slice:

1. Add a deterministic test proving an active ordered relay with
   `connection_stream_frames_pending`, available local egress, and any
   non-zero remote progress below `64KiB` still schedules bounded ACK/window
   service.
2. Keep the existing safeguards: no reads while paused, bounded staging,
   terminal close accounting, and no hard pressure-credit escalation without
   true pressure.
3. Re-run one focused safe1200 reverse-first P1 only after that test passes.

