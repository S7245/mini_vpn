# Knife14az results - local TCP socket policy acceptance

Date: 2026-07-04

## Build

- Code commit: `f21e782`
- VPS tag: `knife14az_tcp_policy`
- Remote bundle:
  `/tmp/conn/mvpn_knife14az_tcp_policy_usclient_suite_20260704_224324.tar.gz`
- Local bundle:
  `/tmp/mini_vpn/mvpn_knife14az_tcp_policy_usclient_suite_20260704_224324.tar.gz`
- Extracted report directory:
  `/tmp/mini_vpn/knife14az_tcp_policy_224324`
- Binary SHA256:
  `ac0105d15320d204d85c21fdb457dc5c4168f0bb6069f350818a95bce81e5e39`

The suite ran on `.27` at commit `f21e782` with a clean git worktree. `.33`
`sing-box` and `.77` `iperf3` were active before the run.

## Local Verification

Local gates passed before VPS acceptance:

```text
cargo test --lib client_tun
bash scripts/knife14b-lowrtt-probe.sh --self-test
bash scripts/knife14b-usclient-tunnel-suite.sh --self-test
git diff --check
cargo test
cargo test --features harness
cargo clippy --all-targets --features harness -- -D warnings
```

The first sandboxed `cargo test` attempt failed only the known local QUIC
endpoint bind tests. The same full command passed outside the restricted
sandbox.

## Baselines

`.27 -> .77` direct preflight was healthy:

```text
forward receiver: 282 Mbit/s
reverse receiver: 267 Mbit/s
```

Because the tunnel summary attributed the failed reverse probe to sender
backpressure, `.33 -> .77` was checked directly after the suite. That path was
also healthy:

```text
forward receiver: 231 Mbit/s
reverse receiver: 232 Mbit/s
```

## Outcome

Knife14az failed VPS acceptance. The clean reverse-first P1 probe did not escape
the low band; it regressed to a tiny reverse transfer:

```text
iperf sender: 0.245 Mbit/s
iperf receiver: 0.009 Mbit/s
```

The probe delivered no useful data for the first 20 seconds, then received one
small burst:

```text
20.00-21.00 sec  34.4 KBytes  282 Kbits/sec
```

The per-probe summary was:

```text
downlink_backpressure: pause_edges=0 resume_edges=0
max_pending_bytes=0
max_tx_queue_bytes=0
max_pressure_bytes=0
downlink_flush: accepted_bytes=35248 send_queue_max=1
tcp_lifecycle: transitions=5 closed_edges=0 terminal_candidates=0
terminal_pending_reap: events=0 bytes=0
pending_at_close: events=0 bytes=0
tun_tx_dropped_delta=0
runtime_tun_egress: drop_delta_total=0
quic: max_lost_bytes_delta=0 max_congestion_events_delta=0 min_cwnd=12000
attribution: reverse_sender_backpressured
```

The post-reverse quiet wait did not reach zero active relays within 20 seconds,
so the suite skipped standard P1 and the full sweep to avoid polluted evidence.

## Interpretation

The local socket policy change is locally correct and guarded by tests, but it
is not a sufficient Knife14 fix.

This run is not the previous "smoltcp tx queue is full while app pending is
zero" shape:

- `send_queue_max` was only `1` in the clean summary.
- app-owned pending stayed `0`.
- downlink backpressure had no pause/resume edges.
- terminal pending and close-time pending were both `0`.
- TUN drops and TUN flush failures were `0`.
- QUIC loss/congestion deltas were `0`.

The new discriminator is earlier in the reverse path: the tunnel received only
about `35 KiB` of remote-to-local data during the clean summary, while iperf's
target-side sender also reported only `896 KiB` over the whole 30s run. The
parser correctly attributed this as `reverse_sender_backpressured`.

The healthy `.33 -> .77` direct reverse baseline rules out a simple exit-target
network bottleneck. The remaining branch is tunnel-side upstream/TUIC-stream
first-byte and read-gap behavior: why the reverse data stream supplies only a
tiny burst to mini_vpn despite healthy direct paths and no visible local
downlink capacity pressure.

## Next Plan Requiring Confirmation

Do not make another behavior patch from this failed acceptance without
confirmation.

Proposed next stage:

1. Add behavior-neutral diagnostics for TUIC TCP stream first-byte latency and
   remote read gaps, separated by control/data connection where possible.
2. Add parser fields for reverse sender backpressure, first remote data time,
   max inter-read gap, and post-local-finish remote bytes.
3. Run one scoped reverse-first acceptance with `.33 -> .77` preflight enabled
   in the suite so the report carries both public-path baselines.
4. Only after that decide whether the next behavior patch belongs in TUIC stream
   read scheduling, relay lifecycle after local finish, or a broader
   architecture review.

