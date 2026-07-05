# Knife14bg TUN RX Drain Cadence Results

Date: 2026-07-05

## Artifacts

- Code commit: `356e2d2`
- Stage spec:
  `docs/tech/2026-07-05-knife14bg-tun-rx-drain-cadence-spec.md`
- Stage plan:
  `docs/tech/2026-07-05-knife14bg-tun-rx-drain-cadence-plan.md`
- VPS bundle:
  `/tmp/mini_vpn/mvpn_knife14bg_tun_rx_drain_usclient_suite_20260705_085637.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14bg_tun_rx_drain_20260705_085637/`

## Local Verification

Passed before VPS:

- `cargo test --lib tun_rx_drain`
- `cargo test --features harness loopback_try_recv_rx --lib`
- `cargo test --lib client_tun`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `git diff --check`
- `cargo test --lib` outside the restricted sandbox
- `cargo test --features harness --lib` outside the restricted sandbox
- `cargo clippy --all-targets --features harness -- -D warnings`

The two local QUIC endpoint bind tests still fail only inside the managed
sandbox and pass outside it, matching the existing project learning.

## VPS Preflight

- `.27` ran commit `356e2d2` from a clean worktree.
- `.27 -> .77` direct preflight was healthy:
  - forward receiver: `279 Mbit/s`
  - reverse receiver: `280 Mbit/s`
- `.33 -> .77` direct preflight was healthy:
  - forward receiver: `260 Mbit/s`
  - reverse receiver: `283 Mbit/s`
- `.33` sing-box remained `active (running)` after the suite. The recent log
  tail showed TUIC inbound and direct outbound connections to `.77:5201` for
  this run, with no TUIC auth failure or service crash.

## Clean Reverse-First P1

The stage failed acceptance.

Clean reverse-first P1:

- iperf sender: `0.315 Mbit/s`
- iperf receiver: `0.025 Mbit/s`
- attribution: `tuic_stream_read_gap+relay_remote_read_gap`

Important counters:

- `tun_rx_drain`: `attempts=6 packets=25 tcp=25 dns=0 udp=0 budget_exhausted=3 would_block=3 errors=0`
- `downlink_backpressure`: `pause_edges=0 resume_edges=0 max_pressure_bytes=0`
- `downlink_flush`: `accepted_bytes=92048`, `send_queue_max=1`, `zero=0`, `errors=0`,
  `tun_flush_failures=0`, `tun_flush_deferred=0`
- `terminal_pending_reap`: `events=0 bytes=0`
- `pending_at_close`: `events=0 bytes=0`
- `tun_drops`: `tun_rx_dropped_delta=0 tun_tx_dropped_delta=0`
- `runtime_tun_egress`: `drop_events=0 drop_delta_total=0`
- `quic`: `max_lost_bytes_delta=0`, `max_congestion_events_delta=0`,
  `inherited_conns=none`
- `relay_remote_timing`: `data_streams=1`, `data_first_read_max_ms=4`,
  `data_max_read_gap_ms=17243`, `data_rx_bytes_max=92044`
- `tuic_tcp_stream`: `data_streams=1`, `data_first_rx_max_ms=4`,
  `data_read_gap_max_ms=20562`, `data_rx_bytes_max=131272`

## Interpretation

Knife14bg did not prove the TUN RX cadence hypothesis. The new drain path did
run in the clean window and processed TCP packets without errors, but the data
stream still delivered only about `92 KiB` into mini_vpn during the summary.
There was no downlink backpressure, no app pending, no terminal pending, no TUN
egress drop, no TUN flush failure, no `send_slice` error, and no clean-window
QUIC loss or congestion.

This means the clean reverse-first failure is no longer hidden behind
close-drain, pending accounting, smoltcp tx-queue pressure, or TUN RX drain
starvation. The active clean-window signal is now before local downlink drain:
the remote/TUIC stream has long read gaps and tiny delivered bytes.

The later forward and full probes are polluted by QUIC congestion and local
write pressure. They are useful for showing broader instability, but they
should not replace the clean reverse-first attribution.

## Rejected Next Moves

Do not continue tuning these paths for this clean reverse root without new
evidence:

- TUN RX drain budget
- TUN qdisc length
- close/reap grace
- terminal pending accounting
- downlink egress pacer
- stale TCP pool slots
- iperf3 or sing-box service availability
- connection pool

## Proposed Knife14bh Plan

Because the repair failed, this plan needs confirmation before behavior edits:

1. Add behavior-neutral stream-gap evidence before another throughput patch.
   The goal is to distinguish whether the data stream is idle because mini_vpn
   is not polling the stream, quinn has no stream data ready, sing-box is not
   forwarding target bytes, or the target sender is backpressured through the
   TUIC stream.
2. Add a narrow reverse-only acceptance mode that stops after the clean
   reverse-first P1 window, so later forward QUIC congestion cannot pollute the
   diagnostic run.
3. Add a config gate for Knife14bg TUN RX drain, defaulting to the current safe
   value only after an A/B proves it is not a regression source. A temporary
   disable or revert should be considered if the next clean A/B reproduces the
   `0.025 Mbit/s` receiver result only with drain enabled.
4. Re-run one scoped clean reverse-only VPS acceptance and compare:
   - current `356e2d2` with drain enabled;
   - the same code with drain disabled, or the previous accepted code path if
     the gate is not yet available.

Stop condition: if stream-gap instrumentation shows mini_vpn is continuously
polling an idle TUIC stream while `.33 -> .77` direct reverse remains healthy,
escalate to a TUIC/sing-box interoperability or cross-layer QUIC flow-control
architecture review instead of applying another local TUN lifecycle patch.
