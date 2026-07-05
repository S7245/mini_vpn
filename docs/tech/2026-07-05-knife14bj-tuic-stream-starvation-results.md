# Knife14bj TUIC Stream Starvation Results

Date: 2026-07-05

## Code Under Test

- Code commit: `4dc79af`
- `.27` repo commit during VPS run: `4dc79af`
- Bundle:
  `/tmp/mini_vpn/knife14bj_poll_20260705_131659/mvpn_knife14bj_poll_usclient_suite_20260705_131659.tar.gz`
- Extracted locally:
  `/tmp/mini_vpn/knife14bj_poll_20260705_131659/`

## Local Gates

Passed before the VPS run:

- `cargo test --lib tuic::tests::`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `cargo clippy --lib -- -D warnings`
- sandbox-external `cargo test --lib`
- `git diff --check`

Notes:

- Sandboxed `cargo test --lib` failed only the QUIC endpoint bind tests because
  this environment rejects UDP bind with `Operation not permitted`.
- `.27` non-login SSH shells do not have `cargo` on `PATH`; the release build
  succeeded through `bash -lc`.

## VPS Preflight

- `.27 -> .77` direct baseline:
  - forward receiver about `282 Mbit/s`;
  - reverse receiver about `279 Mbit/s`.
- `.33 -> .77` exit-to-target baseline:
  - forward receiver about `279 Mbit/s`;
  - reverse receiver about `280 Mbit/s`.
- `.33` sing-box was active, passed config check, and listened on UDP `8443`.
- `.77` iperf3 was active.
- `.27`, `.33`, and `.77` times were within seconds of each other.
- `.33` logs during the run showed TUIC inbound connections from `.27` to
  `.77:5201`.
- No `fail auth` appeared in the post-run `.33` log sample.

## Scoped Reverse-First P1 Result

The scoped suite was run with `STOP_AFTER_REVERSE_FIRST_P1=1`,
`MINI_VPN_TUN_RX_DRAIN_BUDGET=0`, and default adaptive downlink backpressure.

Throughput:

- reverse P1 sender: `0.210 Mbit/s`;
- reverse P1 receiver: `0.019 Mbit/s`.

Key local counters:

- `downlink_backpressure pause_edges=0`;
- `global_rx_pressure events=0`;
- `local_write_pressure events=0`;
- `tun_tx_dropped_delta=0`;
- runtime TUN egress `drop_delta_total=0`;
- QUIC clean-window loss/congestion/blocking deltas all `0`;
- `min_cwnd=12000`;
- `tun_rx_drain attempts=0`.

Close/pending:

- `terminal_pending_reap events=1 bytes=21504`;
- `pending_at_close events=1 bytes=21504`;
- the terminal pending tail is small relative to the failure and appears after
  the data stream already starved.

Stream diagnostics:

- data stream first RX: `17344ms`;
- data stream max read gap: `20516ms`;
- data stream pending gap max: `20516ms`;
- data stream bytes max: `137520`;
- data stream poll gap max: `5000ms`;
- data stream polls max: `123`.

Parser attribution:

```text
terminal_pending_reap+pending_at_close+tuic_stream_first_byte_slow+tuic_stream_read_gap+tuic_stream_read_pending+relay_remote_first_byte_slow+relay_remote_read_gap
```

## Interpretation

Knife14bj did not fix throughput; it narrowed the failure.

The new poll-cadence signal shows that the data stream was still being polled
periodically while it was pending: `data_pending_gap_max_ms=20516` but
`data_poll_gap_max_ms=5000`. This argues against "the relay task was not polled
for the whole starvation gap." The task was at least re-polled by the periodic
diagnostic wakeups, and no local downlink/TUN/global_rx pressure appeared.

The failure is also not a `.33` auth/config/time problem in this run:
authentication reached data-plane traffic, `.33` accepted TUIC inbound
connections, and there was no `fail auth` in the sampled sing-box log.

The remaining active branches are:

1. sing-box/target send-side starvation or early stream shutdown;
2. TUIC/quinn stream wake/readiness behavior where data is not delivered until
   late despite periodic polls;
3. local TCP ACK/window behavior that causes the target/sing-box side to stop
   sending without surfacing as local pending/backpressure.

## Next Plan

Do not change close-drain, TUN egress, stale pool, egress pacer, iperf3, or
sing-box tuning based on this bundle.

Before the next behavior patch, collect or add one server-side/send-side
diagnostic step that can answer whether `.77` sent only a small amount, whether
sing-box received but stopped forwarding, or whether the QUIC stream remained
idle from mini_vpn's point of view. The likely next coherent task is to extend
the scoped suite/parser, not the hot data-plane path, to capture bounded `.33`
and `.77` log windows around the reverse-first P1 run.
