# 2026-07-08 Knife14fy Ordered Read-Service Restore Plan

## Goal

Restore the current branch from the Knife14fu no-data shape toward the
Knife14fw/f8765c1 data-moving shape before resuming local pressure-credit work.

This stage is intentionally narrow:

- keep VPS, sing-box, iperf3, MTU/PLPMTUD, stale pool, and broad QUIC windows
  out of scope;
- remove the rejected unordered TUIC reassembly experiment from the TCP data
  path rather than keeping it as a default-code branch;
- add diagnostics that let the next reverse-first run distinguish stream read
  service, relay receive cadence, self-wake behavior, startup pool cleanliness,
  and local egress progress.

## Evidence Entering The Stage

Knife14fu on current `6eb52e9`:

- ordered-default reverse-first P1 collapsed to `0.349/0.046 Mbit/s`;
- local pending/headroom/TUN/QUIC loss/blocking surfaces stayed clean;
- active data stream showed `connection_stream_frames_pending`, sparse reads,
  and `remote_to_global_rx_bytes=176260`;
- auxiliary TCP pool slot 1 required a startup retry before recovery.

Knife14fw on `f8765c1`:

- reverse-first P1 moved real data at `19.900/18.700 Mbit/s`;
- remaining shape was local-pressure-credit, not no-data;
- `remote_to_global_rx_bytes=70141010`, `remote_reads=18375`, and clean QUIC
  loss/blocking surfaces.

Knife14fx mature sing-box client:

- same `.27/.33/.77` service window reached `173/173 Mbit/s`;
- the remaining blocker is mini_vpn client/data-plane behavior.

## Design Tree

Rejected branches for this stage:

- VPS service or socket-buffer tuning: rejected by Knife14fx mature-client
  `173 Mbit/s`.
- iperf3, direct path, sing-box liveness, MTU/PLPMTUD, stale pool, or broad
  QUIC window tuning: rejected by the current FU/FW/FX bundle.
- local pressure-credit as the first repair: FU had clean local pressure and
  only `176260` downlink bytes; it must first return to data-moving.
- join-vs-wrapper and unordered reassembly: Knife14fs/ft rejected both as fixes.

Remaining branch:

- restore the current TUIC TCP data path to a single ordered `tokio::io::join`
  reader/writer path, while adding diagnostics to prove whether the next no-data
  run is due to unserviced relay reads, QUIC stream pending without wake
  progress, startup auxiliary-slot recovery, or egress feedback never becoming
  relevant.

## Code Changes

- `src/tuic.rs`
  - Removed the rejected unordered `RecvStream::read_chunk(false)` reassembly
    branch and its env gate.
  - Kept TUIC TCP as a single ordered `tokio::io::join(recv, send)` path.
  - Extended `tuic-open-tcp` with `stream`, `relay_mode=ordered_join`, and
    `startup_auth_attempts`.
  - Extended TUIC stream pending/close diagnostics with `self_wake_armed` and
    `self_wake_fired`.

- `src/client_tun.rs`
  - Fixed `remote_read_service_ticks` accounting by incrementing it when the
    relay is actually about to await a non-zero remote read window.
  - Added `remote_read_service_len_min` and `remote_read_service_len_max` to
    live/close relay diagnostics.

## TDD And Local Gates

Focused tests added/updated:

- `relay_read_service_diag_records_awaited_read_window_range`
- `format_tuic_tcp_open_line_includes_target_pool_and_id`
- `format_tuic_tcp_stream_diag_lines_include_first_rx_and_gaps`
- `format_tuic_tcp_stream_pending_line_includes_transport_delivery_counters`
- relay live/close diagnostic assertions for read-service len min/max

Local gates passed:

- `cargo test --lib format_tuic_tcp_open_line_includes_target_pool_and_id`
- `cargo test --lib tuic`
- `cargo test --lib relay_read_service_diag_records_awaited_read_window_range`
- `cargo test --lib`
- `cargo build --release`
- `git diff --check`
- `rustfmt --edition 2024 --check src/tuic.rs src/client_tun.rs`

Note: full `cargo fmt --check` is noisy in the current repository because
pre-existing non-target files are not rustfmt-clean. Do not use it as a stage
gate unless the stage intentionally accepts full-repo formatting churn.

## Next VPS One-Shot

Run one focused reverse-first P1 with the current branch after syncing this
code to `.27`.

Preflight checks:

- `.33`:
  - `net.core.rmem_max=16777216`
  - `net.core.wmem_max=16777216`
  - `net.core.rmem_default=1048576`
  - `net.core.wmem_default=1048576`
  - `sudo systemctl status sing-box --no-pager`
- `.77`:
  - `systemctl status iperf3 --no-pager`
- `.27`:
  - clean worktree or explicit test worktree;
  - `MINI_VPN_TCP_DIAG=1`;
  - `MINI_VPN_TUIC_TCP_POOL=2`;
  - no unordered reassembly env exists anymore.

Acceptance interpretation:

- If `remote_read_service_ticks` rises continuously, `self_wake_armed` roughly
  tracks `self_wake_fired`, and data moves into tens of MB, this stage restored
  the data-moving shape. Resume local pressure-credit work.
- If `connection_stream_frames_pending` persists while `self_wake_armed` rises
  but `self_wake_fired` does not, investigate self-wake scheduling/waker loss.
- If `remote_read_service_ticks` remains low while relay is active, investigate
  relay receive cadence before touching local egress credit.
- If only recovered auxiliary startup slots show no-data, repeat once with a
  first-attempt-clean slot before changing pool behavior.
- If all diagnostics look healthy but no data moves, re-evaluate TUIC stream
  interoperability below mini_vpn's local relay layer.

