# Knife14ex active-flow ACK/window service results

Date: 2026-07-07

Branch: `codex/knife14d-downlink-reap-open`

## Goal

Implement the recommended ACK/window service lane after Knife14ew showed
pressure-free reverse no-data. The stage had two parts:

- classify TUIC TCP stream pending by connection-level receive progress;
- promote the bounded recent-active TUN RX drain from an opt-in A/B knob into a
  default, headroom-gated ACK/window service lane.

Acceptance target remained clean reverse-first P1 receiver `100+ Mbit/s` with
`pending_at_close=0`, `terminal_pending_reap=0`, `tun_tx_dropped_delta=0`, and
QUIC loss/blocking `0`.

## Local changes tested

TUIC stream pending diagnostics now include `pending_cause`:

- `no_transport_sample`
- `no_connection_rx`
- `connection_rx_no_stream_frames`
- `connection_stream_frames_pending`

The low-RTT and US-client suite parsers now summarize:

```text
tuic_stream_pending_causes: connection_stream_frames_pending=... connection_rx_no_stream_frames=... no_connection_rx=... no_transport_sample=...
```

The active-flow ACK/window lane changed from disabled-by-default to a bounded
default:

- `DEFAULT_TUN_RX_ACTIVE_FLOW_TIMER_DRAIN_MS=10`;
- `MINI_VPN_TUN_RX_ACTIVE_FLOW_TIMER_MS=0` still disables it explicitly;
- recent-active drain is capped by the existing egress headroom curve;
- it shrinks to one packet at the credit edge and stops at the hard pause edge
  or when TUN egress feedback is paused.

## Local gates

Passed:

- `cargo test --lib tuic_tcp_stream_pending --quiet`
- `cargo test --lib recent_active_timer_drain_budget --quiet`
- `cargo test --lib parse_tun_rx_active_flow_timer_ms --quiet`
- `cargo test --lib runtime_config_defaults --quiet`
- `cargo test --lib tun_rx_drain --quiet`
- `cargo test --lib relay_read_credit --quiet`
- `cargo test --lib downlink_credit_controller --quiet`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib --quiet`
- `cargo test --features harness --quiet`
- `cargo clippy --all-targets --features harness --quiet -- -D warnings`
- `cargo build --release --quiet`
- `rustfmt --edition 2024 --check src/client_tun.rs src/tuic.rs`
- `git diff --check`

Remote `.27` focused gates also passed with the same focused tests, script
self-tests, release build, and clippy.

## VPS run

Local bundle:
`/tmp/mini_vpn/knife14ex_active_ack_lane_p1_30/mvpn_knife14ex_active_ack_lane_p1_30_usclient_suite_20260707_231150.tar.gz`

Remote bundle:
`/tmp/conn/mvpn_knife14ex_active_ack_lane_p1_30_usclient_suite_20260707_231150.tar.gz`

Preflight was healthy:

- client `.27 -> .77` forward receiver: `274 Mbit/s`
- client `.27 -> .77` reverse receiver: `274 Mbit/s`
- exit `.33 -> .77` forward receiver: `280 Mbit/s`
- exit `.33 -> .77` reverse receiver: `285 Mbit/s`

The new service lane was active at startup:

```text
TUN RX active-flow ACK/window service: 10ms
```

Tunnel result failed acceptance:

- iperf sender: `17.8 Mbit/s`
- iperf receiver: `16.7 Mbit/s`
- shape: `low_average`, `local_pressure=1`, `no_data=0`

This was materially better than Knife14ew's `0.979/0.046 Mbit/s no_data`, but
still far below the `100+ Mbit/s` target.

## Key evidence

The ACK/window service lane ran and increased TUN RX servicing substantially:

- `timer_active_flow_attempts=598`
- `tun_rx_drain attempts=11770`
- `tun_rx_drain packets=14090`
- all drained packets were TCP ACK/window traffic in this run

The previous pressure-free no-data shape changed:

- `remote_to_global_rx_bytes=63282888`
- `send_slice_accepted=63147386`
- `send_slice_zero=0`
- `send_slice_errors=0`
- `tun_flush_failures=0`

Clean surfaces stayed clean:

- `tun_rx_dropped_delta=0`
- `tun_tx_dropped_delta=0`
- runtime TUN egress drops: `0`
- `pending_at_close=0`
- `egress_at_close=0`
- `terminal_pending_reap=0`
- QUIC loss/congestion/blocking deltas: `0`
- safe1200 remained active with `dg_max=Some(1166)` and
  `plpmtud(sent=0,lost=0,black_holes=0)`

The new discriminator showed the main stream was not waiting for connection RX:

- `tuic_stream_pending_causes: connection_stream_frames_pending=18`
- `connection_rx_no_stream_frames=7`
- `no_connection_rx=0`
- `no_transport_sample=0`

Local egress pressure returned once the stream started moving:

- `send_queue_max=557386`
- `may_recv_false=13443`
- `headroom_limited=13431`
- `headroom_deferred_bytes=16754655`
- `pending_total_max=212852`
- `pressure_credit_blocked_bytes=338144`

The data stream still had multi-second read gaps even with clean QUIC:

- `data_max_read_gap_ms=4219`
- `data_pending_gap_max_ms=4219`
- `relay gap hints=53`
- `max_cadence_floor=19200`

## Classification

ACK/window service lane: partially validated. It converted the run from
pressure-free no-data to low-average transfer and raised TUN RX ACK servicing by
orders of magnitude.

ACK/window service lane as the final fix: rejected. More ACK drain did not
produce stable high throughput.

Payload credit only: rejected. Once the service lane let more data through, the
local egress/headroom controller again became the limiter.

QUIC path/server issue: still rejected for this run. Direct and exit baselines
were healthy, and client-side QUIC loss/congestion/blocking stayed zero.

## Decision

Do not keep increasing active-flow timer duration, TUN RX drain budget,
cadence floor, payload-token cap, TUN qlen, MTU/PLPMTUD, stale-pool logic,
iperf3, or sing-box settings for this evidence.

Knife14ex shows the next useful branch is not "more ACK drain"; it is a
stability split:

1. make relay remote reads less sensitive to timer/hint churn, or otherwise
   prove that the TUIC `RecvStream` pending future is being serviced
   continuously while connection-level stream frames advance;
2. keep the ACK/window service lane bounded, because it helped, but do not
   widen it as the main fix;
3. add an egress target/headroom controller that stops letting send_queue hover
   at the credit edge (`~557386B`) while preserving enough read service for
   QUIC stream progress;
4. run the next acceptance only after a deterministic test covers the chosen
   read-service or headroom-stabilization invariant.

This stage advanced the diagnosis from `97%` toward a more precise remaining
root, but acceptance is still not complete.
