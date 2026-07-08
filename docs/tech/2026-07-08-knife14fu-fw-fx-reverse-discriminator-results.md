# 2026-07-08 Knife14fu/fw/fx Reverse Discriminator Results

## Goal

Finish the final Knife14 discriminator after Knife14ft showed that unordered
TUIC chunk reassembly must not stay on the default data path.

Questions:

1. Does the restored ordered mini_vpn path recover reverse-first P1?
2. Does the known Knife14fp-era commit still reproduce `100+ Mbit/s` under the
   current VPS/service window?
3. Can the current VPS/sing-box configuration still reach `100+ Mbit/s` with a
   mature TUIC client?

## Code State

Current code commit:

- `6eb52e9` (`fix: gate unordered TUIC reassembly behind diagnostic env`)

The default TUIC TCP relay path is ordered stream reading again. The unordered
`RecvStream::read_chunk(..., false)` reassembly branch is diagnostic-only behind:

```text
MINI_VPN_TUIC_TCP_UNORDERED_REASSEMBLY=1
```

Local gates passed before the VPS run:

- `cargo test tuic_tcp_relay_mode_defaults_to_ordered_and_gates_unordered_reassembly --lib`
- `cargo test tuic --lib`
- `cargo test --lib`
- `cargo build --release`
- `git diff --check`

## Environment

Topology:

- Client: `.27`
- Exit: `.33`
- Target: `.77`

Exit `.33` still had the Knife14fp high socket-buffer setting:

```text
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.core.rmem_default = 1048576
net.core.wmem_default = 1048576
```

`sing-box` and `iperf3` were active. No current-window TUIC auth failure was
observed around these runs.

## Knife14fu: Current Ordered Default

Bundle:

- Local: `/tmp/mini_vpn/knife14fu_ordered_default_p1_30/mvpn_knife14fu_ordered_default_p1_30_usclient_suite_20260708_105526.tar.gz`
- Remote: `/tmp/conn/mvpn_knife14fu_ordered_default_p1_30_usclient_suite_20260708_105526.tar.gz`

Direct baselines were healthy:

| Path | Receiver |
| --- | ---: |
| `.27 -> .77` | `279 Mbit/s` |
| `.77 -> .27` | `277 Mbit/s` |
| `.33 -> .77` | `278 Mbit/s` |
| `.77 -> .33` | `282 Mbit/s` |

Reverse-first P1:

| Metric | Value |
| --- | ---: |
| iperf sender | `0.349 Mbit/s` |
| iperf receiver | `0.046 Mbit/s` |

Shape and attribution:

```text
throughput_shape=shape=no_data tail_collapse=0 local_pressure=0 no_data=1 stable_high=0
attribution=tuic_stream_read_gap+tuic_stream_read_pending+relay_remote_read_gap+target_sender_stalled+tuic_stream_starved
```

Clean surfaces:

- `pending_total_max=0`
- `may_recv_false=0`
- `headroom_deferred_bytes=0`
- `pending_at_close=0`
- `terminal_pending_reap=0`
- `tun_tx_dropped_delta=0`
- `send_slice_zero=0`
- `send_slice_errors=0`
- QUIC loss/congestion/blocked/rx-blocked deltas `0`

Stream starvation signals:

- `remote_to_global_rx_bytes=176260`
- data `remote_reads=12`
- `data_max_read_gap_ms=13696`
- `connection_stream_frames_pending=21`
- active QUIC connection `udp_rx=660/921132B`
- active QUIC connection `rx_stream=220`

Interpretation: the default ordered path was restored and the local
pending/headroom surfaces stayed clean, but the current branch collapsed into a
no-data stream-read/starvation shape.

## Knife14fw: Known Knife14fp-era Commit Clean Reverse A/B

The known high-throughput commit was checked out in a detached worktree on
`.27`:

```text
/home/ubuntu/mini_vpn_accept_f8765c1
```

Commit:

- `f8765c1`

Bundle:

- Local: `/tmp/mini_vpn/knife14fw_fp_commit_reverse_first_p1_30/mvpn_knife14fw_fp_commit_reverse_first_p1_30_usclient_suite_20260708_110632.tar.gz`
- Remote: `/tmp/conn/mvpn_knife14fw_fp_commit_reverse_first_p1_30_usclient_suite_20260708_110632.tar.gz`

Direct baselines were healthy:

| Path | Receiver |
| --- | ---: |
| `.27 -> .77` | `278 Mbit/s` |
| `.77 -> .27` | `299 Mbit/s` |
| `.33 -> .77` | `292 Mbit/s` |
| `.77 -> .33` | `286 Mbit/s` |

Reverse-first P1:

| Metric | Value |
| --- | ---: |
| iperf sender | `19.900 Mbit/s` |
| iperf receiver | `18.700 Mbit/s` |

Shape:

```text
throughput_shape=shape=low_average tail_collapse=0 local_pressure=1 no_data=0 stable_high=0
```

Key mini_vpn metrics:

- `pending_total_max=0`
- `pending_high=263541`
- `remote_to_global_rx_bytes=70141010`
- `send_queue_max=458752`
- `may_recv_false=0`
- `headroom_limited=5`
- `headroom_deferred_bytes=425984`
- `drain_credit_granted_bytes=69057941`
- `pressure_credit_blocked_bytes=236701`
- `terminal_pending_reap=0`
- `pending_at_close=0`
- `tun_tx_dropped_delta=0`
- QUIC loss/congestion/blocked/rx-blocked deltas `0`
- `data_max_read_gap_ms=3400`
- `connection_stream_frames_pending=7`
- `attribution=local_pressure_credit`

Interpretation: the exact Knife14fp-era commit did not reproduce the reported
`114 Mbit/s` result in this current service window, but it did move real data
and remained much better than the current branch's no-data shape. This
separates a current-branch regression from the broader reverse-path controller
gap that still exists in mini_vpn.

## Knife14fx: Mature sing-box Client Current A/B

A mature sing-box client was run on `.27` with a temporary TUN inbound routing
only `.77/32` through TUIC to `.33`. The route to `.33` stayed on the normal
interface to avoid a proxy loop. Runtime config was generated in `/tmp` and
removed after the run.

Local pulled artifacts:

- `/tmp/mini_vpn/knife14fx_singbox_client_current/iperf3-reverse-30s.txt`
- `/tmp/mini_vpn/knife14fx_singbox_client_current/sing-box-client.log`

Client:

- sing-box `v1.13.14`
- TUN address field used the current sing-box v1.13 schema:
  `address = ["172.19.0.1/30"]`

Route checks:

```text
.77 via 172.19.0.2 dev sbtun0 table 2022 src 172.19.0.1
.33 via eth0
```

Reverse P1 result:

| Metric | Value |
| --- | ---: |
| iperf sender | `173 Mbit/s` |
| iperf receiver | `173 Mbit/s` |

Per-second intervals stayed in the `147-216 Mbit/s` band. The client log showed
TUN inbound startup and TUIC outbound connections to `.77:5201`, with no error
in the pulled tail.

Interpretation: the current `.27/.33/.77` VPS configuration can still reach
`100+ Mbit/s` through the `.33` sing-box TUIC exit. The latest low mini_vpn
reverse-first runs are therefore mini_vpn client/data-plane failures, not a
VPS socket-buffer, sing-box liveness, iperf3, direct-path, MTU/PLPMTUD, QUIC
loss, or QUIC blocking failure.

## Conclusion

VPS configuration is not the remaining blocker. With the exit-side high socket
buffers in place, the current mature sing-box client reached `173/173 Mbit/s`.

The current code branch is also not allowed to keep unordered reassembly on by
default. `6eb52e9` restored the ordered default and keeps unordered staging only
as an explicit diagnostic mode.

The remaining mini_vpn acceptance gap is now narrower:

- current branch: ordered-default, clean local surfaces, but no-data stream
  starvation (`0.046 Mbit/s` receiver);
- known `f8765c1`: real data movement but still low reverse throughput
  (`18.700 Mbit/s` receiver) with local-pressure-credit shape;
- mature sing-box client: same current VPS/service window reaches
  `173 Mbit/s`.

Next work should stay inside mini_vpn's reverse TUIC TCP data path:

1. Diff the current branch against `f8765c1` around TUIC stream read service,
   relay receive cadence, and diagnostic/self-wake changes to explain the
   regression from `18.7 Mbit/s` to no-data.
2. Add a deeper discriminator for ordered stream readiness and application
   progress: QUIC stream readable state, ordered offset progress, remote read
   future lifetime, and local egress progress in the same timeline.
3. Only after the current branch returns to the `f8765c1` data-moving shape
   should the controller work resume on the remaining local-pressure-credit
   problem.

Do not lower the product target to `30 Mbit/s`, and do not spend the next stage
on VPS config, iperf3, stale pool slots, egress pacer constants, MTU/PLPMTUD,
or broad QUIC window tuning unless new evidence contradicts the mature-client
A/B.
