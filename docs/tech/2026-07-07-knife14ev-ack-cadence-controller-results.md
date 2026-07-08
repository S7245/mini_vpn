# Knife14ev ACK cadence controller results

Date: 2026-07-07

Branch: `codex/knife14d-downlink-reap-open`

## Goal

Run one safe1200 reverse-first P1 after adding a short-lived ACK/window cadence
boost driven by current-epoch relay read-gap hints.

The acceptance target remained clean reverse-first P1 receiver `100+ Mbit/s`
with bounded local pending, no terminal pending reap, no TUN drops, and no
QUIC loss/blocking.

## Local changes tested

The implementation added a narrow split-controller step:

- `DownlinkCreditController` now has a short-lived
  `ack_cadence_boost_bytes` path;
- current-epoch `RelayEvent::AckDrainHint` can publish a bounded multi-MTU
  pressure ACK/window floor while below the hard pause edge;
- the boost is consumed on publish and collapses under hard pause/no-progress
  pressure;
- the low-RTT parser now reports `relay_gap_hints` with
  `max_cadence_floor`.

Local gates passed before the VPS run:

- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`
- `cargo test --lib gap_hint_boost --quiet`
- `cargo test --lib --quiet`
- `cargo test --features harness --quiet`
- `cargo clippy --all-targets --features harness --quiet -- -D warnings`
- `cargo build --release --quiet`

Remote `.27` focused gates also passed:

- `cargo test --lib gap_hint_boost --quiet`
- `cargo test --lib downlink_credit_controller --quiet`
- low-RTT probe self-test
- US-client suite self-test
- release build

## VPS run

Local bundle:
`/tmp/mini_vpn/knife14ev_ack_cadence_p1_30/mvpn_knife14ev_ack_cadence_p1_30_usclient_suite_20260707_210539.tar.gz`

Remote bundle:
`/tmp/conn/mvpn_knife14ev_ack_cadence_p1_30_usclient_suite_20260707_210539.tar.gz`

Preflight was healthy:

- client `.27 -> .77` forward receiver: `279 Mbit/s`
- client `.27 -> .77` reverse receiver: `296 Mbit/s`
- exit `.33 -> .77` forward receiver: `280 Mbit/s`
- exit `.33 -> .77` reverse receiver: `285 Mbit/s`

Tunnel result failed acceptance:

- iperf sender: `16.0 Mbit/s`
- iperf receiver: `14.9 Mbit/s`
- shape: `low_average`, `local_pressure=1`, `no_data=0`,
  `tail_collapse=0`

## Key evidence

The new cadence hook was active:

- `relay_gap_hints events=59`
- `max_gap_ms=4153`
- `max_budget=240`
- `cadence_events=59`
- `max_cadence_floor=19200`

The clean surfaces stayed clean:

- QUIC loss/congestion/blocking: `0`
- `rx_blocked(data=0,stream=0)`
- `tx_blocked(data=0,stream=0)`
- safe1200 active with `dg_max=Some(1166)` and
  `plpmtud(sent=0,lost=0,black_holes=0)`
- `tun_tx_dropped_delta=0`
- `send_slice_zero=0`
- `send_slice_errors=0`
- `tun_flush_failures=0`
- `terminal_pending_reap=0`
- `pending_at_close=0`
- `egress_at_close=0`

The pressure loop still dominated:

- `downlink_backpressure pause_edges=1 resume_edges=0`
- `send_queue_max=557386`
- `max_pressure_bytes=557386`
- `max_total_pressure_bytes=625782`
- `may_recv_false=14693`
- `headroom_limited=14653`
- `headroom_deferred_bytes=18732361`
- `pressure_credit_debt_bytes=83736`
- `pressure_credit_blocked_bytes=138257`

The read gap stayed pre-FIN and data-stream-local:

- `data_max_read_gap_ms=4607`
- `data_pending_gap_max_ms=4607`
- `data_poll_gap_max_ms=250`
- `data_max_read_gap_before_finish_ms=4607`
- `data_max_read_gap_after_finish_ms=0`
- data stream received `56401448B`

The decisive controller signal is that a `19200B` cadence floor was published
59 times, but the relay still closed with `remote_batch_limit_bytes_min=1200`
and `read_credit_limit_bytes_min=1200`. Near the end, repeated
`projected_payload_credit_edge` debts pushed local pressure beyond the credit
edge, then `tcp-relay-read-credit ... paused=true max_batch_bytes=0` stopped
payload reads.

## Classification

Environment issue: rejected. Direct and exit-to-target baselines were healthy.

QUIC/MTU issue: rejected as the primary root. QUIC loss, congestion,
blocking, PLPMTUD black holes, and TUN drops stayed zero.

FIN deferral issue: rejected again. The data gap happened before local finish.

Short ACK cadence boost: insufficient. It reduced neither the low-average
shape nor the local-pressure collapse. The boost was active, but it was too
episodic and still tied to the same pressure edge that later forced
one-MTU/paused read credit.

## Decision

Stop the short-boost path here per the Knife14ev plan stop condition: "the run
stays near `20 Mbit/s` with the boost clearly active."

Do not continue by increasing `cadence_floor`, TUN RX drain budget, safe1200
MTU policy, iperf3, sing-box, or stale pool logic.

The next valid branch should be an architectural change to make ACK/window
progress a continuously serviced lane, not a one-shot relay-read boost. The
promising direction is:

1. split relay payload drain from ACK/window drain explicitly;
2. keep payload staging under the existing pending/send_queue hard edge;
3. allow a tiny continuous non-payload ACK/window service cadence while QUIC is
   clean and the flow is active;
4. spend payload read credit only when measured TUN egress progress has moved
   send_queue/pending below a safe target;
5. add parser metrics for effective read-credit min/max over time, not only
   final `read_credit_limit_bytes_min`.

This stage did not complete throughput acceptance. It completed its
discriminator role by proving the hook is wired and by rejecting short-lived
ACK cadence boost as the final 3% solution.
