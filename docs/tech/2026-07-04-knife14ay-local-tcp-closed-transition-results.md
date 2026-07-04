# Knife14ay results - local TCP Closed transition acceptance

Date: 2026-07-04

## Build

- Code commit: `67a8c46`
- VPS tag: `knife14ay_tcp_lifecycle`
- Remote bundle:
  `/tmp/conn/mvpn_knife14ay_tcp_lifecycle_usclient_suite_20260704_222042.tar.gz`
- Local bundle:
  `/tmp/mini_vpn/mvpn_knife14ay_tcp_lifecycle_usclient_suite_20260704_222042.tar.gz`
- Extracted report directory:
  `/tmp/mini_vpn/knife14ay_tcp_lifecycle_222042`

The suite ran on `.27` at commit `67a8c46` with a clean git worktree. `.33`
`sing-box` and `.77` `iperf3` were active before the run. Direct `.27 -> .77`
preflight baselines were healthy:

```text
forward receiver: 283 Mbit/s
reverse receiver: 276 Mbit/s
```

## Outcome

Knife14ay met the observability goal but failed throughput acceptance. The
clean reverse-first P1 probe stayed in the low band:

```text
iperf_sender_mbps: 9.150
iperf_receiver_mbps: 8.180
```

The compact reverse-first summary, including the 2s post-iperf close-tail
settle window, was:

```text
downlink_backpressure: pause_edges=12 resume_edges=12
max_tx_queue_bytes=586088
max_pending_bytes=552440
downlink_flush: accepted_bytes=30769407 send_queue_max=521968
tcp_lifecycle: transitions=5 closed_edges=1 terminal_candidates=0
terminal_pending_reap: events=1 bytes=552440
pending_at_close: events=1 bytes=552440
tun_tx_dropped_delta=0
runtime_tun_egress: drop_delta_total=0
quic: max_lost_bytes_delta=0 max_congestion_events_delta=0
attribution: local_downlink_backpressure+terminal_pending_reap+pending_at_close
```

The new lifecycle line exposed the pre-terminal edge:

```text
source=dirty_relay
prev_state=Established state=Closed
prev_can_send=true can_send=false
prev_may_send=true may_send=false
pending=0
pending_high=65536
remote_to_global_rx_bytes=30811002
send_slice_accepted=30811002
flush_attempts=2951
terminal_candidate=false
```

Immediately after that, the remote relay closed and the remaining remote bytes
became terminal pending:

```text
tcp-relay-close reason=remote_eof remote_to_global_rx_bytes=34433032
tcp-handle-close reason=uplink_channel_closed pending=552440
close_pending_class=terminal_closed_no_send
terminal_pending_reap_bytes=552440
tcp_state=Closed active=false can_send=false can_recv=false
may_send=false may_recv=false
send_queue=8799
```

## Interpretation

The lifecycle branch produced the missing discriminator. The terminal pending
bytes were not the hidden pre-close loss point in this run. The reverse data
socket had already transitioned from `Established` to `Closed` during
`dirty_relay` with `pending=0`; the terminal pending appeared after that close
edge when the relay delivered its remote EOF/closed event.

This also rejects another close-drain-only patch. Once the smoltcp socket is
`Closed && !can_send && !may_send`, the late pending bytes are not deliverable
through that socket.

The active clean-window limiter is local TCP downlink drain:

- QUIC loss/congestion delta was zero and `cwnd` stayed at `12000`.
- TUN tx drops and runtime TUN egress drops were zero.
- TUN flush syscall failures and deferrals were zero.
- `send_slice_zero` and `send_slice_errors` were zero.
- The relay channel did not show global_rx pressure.
- `send_queue_max` was about `522 KiB`, and tx-queue backpressure repeatedly
  paused/resumed remote reads.

The reverse flow accepted about `30.8 MiB` into smoltcp over about `2945`
flush attempts, or roughly `10 KiB` per attempt. The visible symptom is not
that the remote side cannot supply data; it is that the local smoltcp TCP
send queue drains in bursts, producing long zero-throughput intervals at the
local iperf receiver.

Later windows are not clean reverse-root evidence. The standard forward P1 was
polluted by QUIC/local-write pressure:

```text
forward P1 receiver: 0.829 Mbit/s
attribution: quic_loss_congestion+local_write_pressure+local_tun_egress_drop
```

The following reverse P1 inherited that QUIC congestion:

```text
reverse P1 receiver: 16.100 Mbit/s
attribution: inherited_quic_congestion+local_downlink_backpressure+terminal_pending_reap+pending_at_close
```

The full reverse window reached `24.100 Mbit/s` receiver but still carried
inherited congestion and the same terminal-pending-after-Closed shape.

## Proposed Next Patch

Do not modify close/reap behavior next. Do not tune stale pool, iperf3,
sing-box, TUN queue length, connection pool, or the egress pacer without new
evidence.

The next code stage should target local smoltcp TCP receive-window / tx-queue
drain behavior:

1. Add focused tests for local TCP socket construction and rearm so listener
   sockets consistently apply the intended local-link TCP policy.
2. Add a small diagnostic or focused test that reports tx-queue drain rate
   while app-owned pending is zero, so `send_queue` high-water is not confused
   with terminal pending.
3. Evaluate disabling Nagle (`set_nagle_enabled(false)`) and possibly delayed
   ACK (`set_ack_delay(None)`) for local virtual-link TCP sockets. These are
   smoltcp-local socket policy changes, not TUIC or server-side changes.
4. Run local gates and one scoped reverse-first VPS acceptance before broader
   regression.

Per the self-correction rule, this is a plan only. Behavior code changes should
wait for explicit confirmation after this failed acceptance.
