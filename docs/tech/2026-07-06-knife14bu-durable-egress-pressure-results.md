# Knife14bu Durable Egress Pressure Results

## Summary

Commit `fd3f2ed` added a bounded 25ms effective downlink pressure hold after a
raw high-watermark smoltcp tx-queue observation. The local TDD/regression gates
passed, but the scoped `.27` VPS acceptance failed. Reverse-first P1 stayed in
the low-average band and regressed from Knife14bt's `17.4 Mbit/s` receiver
result to `15.3 Mbit/s`.

The useful result is diagnostic: the hold removed the prior TUN tx drop signal
and terminal late payload disappeared, but throughput did not improve. The
remaining root is no longer hidden app pending, terminal reap, TUN syscall/drop,
QUIC loss/congestion, sing-box auth, or direct path capacity. It is still local
TCP downlink lifecycle / receive-window / close-drain behavior around egress
queue pressure.

## Local Gates

- Red/green TDD:
  - `cargo test --lib client_tun::tests::downlink_backpressure_holds_recent_high_tx_queue_after_flush`
- Focused checks:
  - `cargo test --lib client_tun::tests::downlink_backpressure`
  - `cargo test --lib client_tun::tests::tun_egress_feedback`
- Regression:
  - `cargo test --lib`
  - `git diff --check`

All passed before commit `fd3f2ed`.

## VPS Run

- Out dir: `/tmp/mini_vpn/knife14bu_durable_20260706`
- Bundle:
  `/tmp/mini_vpn/knife14bu_durable_20260706/mvpn_knife14bu_durable_usclient_suite_20260706_190244.tar.gz`
- Local extracted copy:
  `/tmp/mini_vpn/knife14bu_durable_20260706_local/`
- Commit on `.27`: `fd3f2ed4`
- Suite config: downlink high/low `<auto>`
- Startup log: `high=524288B low=131072B`
- TUIC startup: connected to `.33` sing-box successfully.

Release build warning:

- `warning: method observe_pressure is never used`
- This is a cleanup item for the next code slice. It did not stop the build or
  acceptance run, but release builds should stay warning-clean for future VPS
  evidence.

Direct baselines were healthy:

- `.27 -> .77`: `317/277 Mbit/s`
- `.27 <- .77`: `312/282 Mbit/s`
- `.33 -> .77`: `314/284 Mbit/s`
- `.33 <- .77`: `320/297 Mbit/s`

Tunnel reverse-first P1 failed:

- sender: `61.9 MBytes / 17.3 Mbit/s`
- receiver: `54.8 MBytes / 15.3 Mbit/s`
- interval profile: many zero-throughput seconds with short bursts, still
  `shape=low_average`

## Key Signals

- `downlink_backpressure=63/62`, up from Knife14bt's `26/26`.
- `max_tx_queue_bytes=587645`, `max_pressure_bytes=587645`.
- `tun_flush_deferred=62` in the probe summary and `63` on the data handle
  close line.
- `tun_tx_dropped_delta=0`, improved from Knife14bt's `172`.
- `terminal_pending_reap=0`.
- `terminal_late_remote_payload=0B/0`.
- `pending_at_close=0`, `send_slice_zero=0`, `send_slice_errors=0`,
  `tun_flush_failures=0`.
- QUIC loss, congestion, and blocking deltas all stayed `0`.
- `global_rx_pressure=0`, but `global_rx_queue_used_max=212/1024`, much higher
  than Knife14bt's `12/1024`.
- Data stream read/pending gaps stayed multi-second:
  `data_read_gap_max_ms=4330`, `data_pending_gap_max_ms=3436`.
- The parser attribution remained `local_downlink_backpressure`.

The close-boundary line is now the most important discriminator:

- data handle close reason: `uplink_channel_closed`
- `tcp_state=CloseWait`, `active=true`, `can_send=true`, `can_recv=false`
- `may_send=true`, `may_recv=false`
- `pending=0`, `terminal_pending_reap_bytes=0`,
  `terminal_late_remote_payload_bytes=0`
- `send_queue=524288`
- `may_recv_false=8`

This means the low-average run is not hiding bytes in app-owned pending or
terminal reaping. The local TCP receive side is closed while a full high-water
egress queue still exists.

## Server Evidence

The collected `.33`/`.77` evidence does not support an auth or service root for
this run:

- `.33` and `.77` were time-synchronized (`NTPSynchronized=yes`, client/exit
  epoch matched and target differed by about one second).
- `.33` sing-box logs contained current TUIC inbound/direct outbound entries
  from `.27` to `.77:5201` during the probe window.
- The collected current TUIC evidence did not show a `fail auth` event for the
  probe. Older unrelated REALITY scan noise was present in the sing-box tail and
  is not evidence against TUIC auth.
- `.77` iperf journal matched the same stop/go low-average shape.

## Interpretation

Knife14bu rejects the simple 25ms durable-pressure hold as a throughput fix. It
likely reduced or masked kernel TUN drops, but it increased pause/resume churn
and deferred flushes, and receiver throughput fell.

The accepted learning is narrower and stronger:

- TUN drops are not required for the low-average failure.
- Hidden terminal pending, terminal late remote payload, close-time pending,
  send-slice failure, TUN flush failure, and clean-window QUIC loss/congestion
  are all absent in this run.
- The root now sits at the close/receive-window boundary: mini_vpn is still
  pausing downlink around tx-queue pressure, but the local TCP side enters
  `CloseWait` / `may_recv=false` while `send_queue` remains at the high
  watermark.

Do not fix this by simply lengthening the hold. The next slice should first make
raw-vs-effective pressure and close-time egress state explicit, then test a
bounded close/egress drain rule for sockets that are still send-capable and have
queued downlink bytes at local receive close.

## Next Plan Requiring Confirmation

1. Cleanup the release warning by removing or test-gating the unused
   `observe_pressure` wrapper.
2. Add behavior-neutral observability for raw pressure, effective held pressure,
   hold expiry, and close-time `send_queue`/`may_recv=false` classification.
3. Add a focused deterministic test for a send-capable `CloseWait` socket with
   `send_queue >= high`, `pending=0`, and `may_recv=false`.
4. Implement only a bounded close/egress drain behavior if the test proves the
   current lifecycle can stop useful downlink flush while queued bytes remain.
5. Rerun local gates, then one scoped VPS reverse-first P1 only after review.

