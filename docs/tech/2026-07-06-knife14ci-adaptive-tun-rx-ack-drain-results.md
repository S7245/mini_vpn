# Knife14ci Adaptive TUN RX ACK Drain Results

Date: 2026-07-06 / VPS local time 2026-07-07 02:08-02:14 CST

## Scope

Knife14ci added a default-safe, pressure-triggered TUN RX drain path:

- explicit `MINI_VPN_TUN_RX_DRAIN_BUDGET>0` remains an override;
- default `0` stays off below the downlink egress credit edge;
- when remote payload work and `send_queue >= tx_queue_credit_high` coexist,
  the remote-payload branch drains a small guard/MTU-derived ACK budget.

The goal was to process local TCP ACK/window updates near the smoltcp egress
credit edge without re-enabling the rejected unconditional TUN RX drain.

## Commits And Local Verification

- Code/spec/plan commit: `554d3b4`
  (`fix(knife14ci): adaptively drain tun rx under egress pressure`)

Passed locally before VPS:

- `cargo test tun_rx_drain --lib`
- `cargo test tun_rx_pressure --lib`
- `cargo test downlink_egress_clock --lib`
- `cargo test pressure_credit --lib`
- `cargo test credit_debt --lib`
- `cargo test drop_credit --lib`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib`
- `cargo test`
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `git diff --check`

`.27` focused verification after rsync also passed:

- `cargo test tun_rx_drain --lib`
- `cargo test tun_rx_pressure --lib`
- `cargo build --release`

## Startup Auth Incident

The first suite attempt failed before P1:

- Remote bundle:
  `/tmp/mini_vpn/knife14ci_adaptive_ack_drain_20260707_0208/mvpn_knife14ci_adaptive_ack_drain_usclient_suite_20260707_020847.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14ci_adaptive_ack_drain_20260707_0208_local/`
- Client error:
  `tuic auth finish: sending stopped by peer: error 0`

This was not treated as a Knife14ci performance rejection:

- `.27 <-> .77` and `.33 <-> .77` iperf baselines were healthy.
- `.33` sing-box was active, running, `NRestarts=0`.
- Client and exit time skew was `0s`; `.33` NTP was synchronized.
- `sing-box -c /etc/sing-box/config.json check` passed.
- A no-secret comparison proved `.27` env and `.33` config matched for
  UUID/password/ALPN/SNI by exact equality and expected lengths.
- A 20s client-tun startup smoke immediately afterward connected successfully.

Interpretation: the startup failure was a transient TUIC/QUIC entry failure, not
a password, clock, or sing-box config mismatch.

## VPS Retry

- Suite tag: `knife14ci_adaptive_ack_drain_retry`
- Remote bundle:
  `/tmp/mini_vpn/knife14ci_adaptive_ack_drain_retry_20260707_0212/mvpn_knife14ci_adaptive_ack_drain_retry_usclient_suite_20260707_021233.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14ci_adaptive_ack_drain_retry_20260707_0212_local/`
- Binary SHA256:
  `695f7b95f38a03807471ecb3f04a7a1726f818ba79e06c495da6e9add7433343`

Preflight baselines were healthy:

- `.27 -> .77` forward receiver: `279 Mbit/s`
- `.27 <- .77` reverse receiver: `279 Mbit/s`
- `.33 -> .77` forward receiver: `270 Mbit/s`
- `.33 <- .77` reverse receiver: `283 Mbit/s`

Startup confirmed the intended default path:

```text
TUN RX drain budget: 0 packets/pass
pressure-adaptive=21 packets near tx_queue_credit_high
bounded global_rx receive window: disabled
```

## Result

Knife14ci failed VPS acceptance.

- Reverse-first P1 sender: `18.8 Mbit/s`
- Reverse-first P1 receiver: `17.9 Mbit/s`
- Shape: `low_average`
- Attribution: `local_tun_egress_drop+local_tun_egress_feedback+local_drop_credit`

The adaptive TUN RX drain path engaged, proving the code path was active:

```text
tcp-tun-rx-drain attempts=5 packets=105 tcp=105 dns=0 udp=0 budget_exhausted=5 would_block=0 errors=0
```

But it engaged too late and too shallow to prevent the local egress burst:

```text
tun_tx_dropped_delta=82
runtime_tun_egress drop_events=1 drop_delta_total=82 max_delta=82
tun_egress_feedback pause_edges=1 resume_edges=1 max_pressure_bytes=892928
send_queue_max=892928
hard_edge_guard_limited=51
tun_flush_deferred=12
```

Clean surfaces:

- `.27/.33/.77` direct baselines were healthy.
- QUIC loss/congestion/blocking deltas were clean:
  `max_lost_bytes_delta=0`, `max_congestion_events_delta=0`,
  `max_tx_blocked_*_delta=0`, `max_rx_blocked_*_delta=0`.
- `send_slice_zero=0`, `send_slice_errors=0`, `tun_flush_failures=0`.
- No terminal pending reap, no terminal late remote payload.
- `global_rx_pressure events=0`, `queue_used_max=27/1024` during the clean
  summary window.

Close-tail evidence still shows active send-capable local egress backlog:

```text
tcp-handle-close handle=SocketHandle(1)
pending=525514
pending_high=525514
close_pending_class=active_send_capable
close_egress_bytes=892928
close_egress_drain_candidate=true
tcp_state=CloseWait
can_send=true
may_send=true
send_queue=892928
```

The data stream still had multi-second read gaps:

```text
data_read_gap_max_ms=3679
data_pending_gap_max_ms=3678
data_rx_bytes_max=66995800
```

## Interpretation

Knife14ci proved the ACK/TUN-RX hypothesis is real but incomplete:

- Ready TCP ACK packets existed at the pressure edge (`budget_exhausted=5`).
- Processing them after payload accept/flush is too late; TUN egress drops had
  already occurred.
- Because the drain is only remote-payload-triggered, it does not maintain ACK
  progress during multi-second remote read gaps.

The next code stage should not increase a static env budget. It should move
the same pressure-gated algorithm earlier and keep it active at maintenance
points:

1. pre-payload pressure drain before accepting/flushing new remote bytes when
   the socket is already near `tx_queue_credit_high`;
2. pressure-maintenance drain while dirty downlink remains and
   `send_queue >= tx_queue_credit_high`;
3. focused tests proving the drain remains off below pressure and cannot become
   an unconditional scan.

## Progress

Overall Knife14 progress remains `82%`.

It does not move down because Knife14ci produced a sharper causal split:
ACK backlog exists and the current post-payload trigger is too late. It does
not move up because the scoped VPS acceptance still stayed in the `10-20
Mbit/s` class and reintroduced TUN egress drops plus active send-capable close
backlog.
