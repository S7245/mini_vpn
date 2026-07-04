# Knife14ar results - close-safe egress pacing still fails acceptance

Date: 2026-07-04

Code commit: `3baf476`

Local bundle:
`/tmp/mini_vpn/mvpn_knife14ar_close_safe_default_usclient_suite_20260704_164651.tar.gz`

Extracted bundle:
`/tmp/mini_vpn/knife14ar_close_safe_164651/`

Remote bundle:
`/tmp/conn/mvpn_knife14ar_close_safe_default_usclient_suite_20260704_164651.tar.gz`

## Verdict

Knife14ar failed VPS acceptance, but it falsified the previous egress-pacer
hypothesis.

The local change did what it was supposed to do:

- the default `MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES` was restored to
  `16777216` instead of the rejected Knife14aq `65536`;
- startup logged that pending backlog forces immediate flush;
- the primary clean reverse-first window had `tun_flush_deferred=0`;
- the primary clean reverse-first window had `tun_tx_dropped_delta=0`;
- there were no clean-window QUIC loss/congestion deltas.

Throughput still stayed below the Knife14ap baseline and the close boundary
still showed non-empty pending bytes with the local TCP socket already closed.
The remaining failure is therefore no longer "pacer deferred the immediate
flush" or "TUN qdisc dropped clean-window packets." It is now in mini_vpn's
local TCP downlink lifecycle / receive-window / close-drain behavior.

## Baselines

The suite recorded healthy direct paths before starting the tunnel:

- `.27 -> .77` forward receiver: `283 Mbit/s`
- `.27 -> .77` reverse receiver: `271 Mbit/s`
- `.33 -> .77` forward receiver: `295 Mbit/s`
- `.33 -> .77` reverse receiver: `285 Mbit/s`

This run should not be explained away as an unhealthy iperf3 service, unhealthy
sing-box service, or generally slow exit-to-target path.

## Primary Signal

Clean reverse-first P1 remains the primary signal because it avoids inherited
QUIC congestion from an earlier forward run.

Knife14ar clean reverse-first P1:

- iperf: sender `17.2 Mbit/s`, receiver `16.2 Mbit/s`
- attribution: `local_downlink_backpressure`
- downlink backpressure: `pause_edges=8`, `resume_edges=8`,
  `max_pending_bytes=583624`, `max_total_pending_bytes=583624`
- downlink flush:
  - `attempts=6337`
  - `no_send_capacity=127`
  - `send_slice_calls=6210`
  - `accepted_bytes=60775137`
  - `send_slice_zero=0`
  - `send_slice_errors=0`
  - `budget_limited=842`
  - `max_accepted_bytes=161240`
  - `tun_flush_calls=5118`
  - `tun_flush_failures=0`
  - `tun_flush_deferred=0`
  - `remote_to_global_rx_bytes=60775137`
- TUN drops: `tun_rx_dropped_delta=0`, `tun_tx_dropped_delta=0`
- QUIC: `max_lost_bytes_delta=0`, `max_congestion_events_delta=0`,
  `max_start_lost_bytes=0`, `max_start_congestion_events=0`,
  `min_start_cwnd=248272`

Compared with Knife14ap clean reverse-first:

- Knife14ap receiver: `22.0 Mbit/s`
- Knife14ar receiver: `16.2 Mbit/s`
- Knife14ap clean TUN tx drops: `366`
- Knife14ar clean TUN tx drops: `0`

Interpretation:

- Knife14ar removed the clean-window TUN drop signal.
- Knife14ar removed pacer deferral as the local egress blocker.
- The low throughput and zero-throughput iperf intervals remain.
- The only clean-window attribution left is local downlink backpressure.

## Close Boundary Signal

Immediately after the clean reverse-first window, the next report captured the
same connection being reaped with pending downlink bytes:

```text
reason=dead_slot_reap state=Relaying pending=224765 pending_high=583624
remote_to_global_rx_bytes=61136600 send_slice_accepted=60911835
no_send_capacity=229 tun_flush_deferred=0
tcp_state=Closed active=false can_send=false can_recv=false
```

The useful distinction from Knife14aq is `tun_flush_deferred=0`: the pending
tail did not survive because the new pacer skipped immediate flush. It survived
until a terminal local socket state where mini_vpn had no remaining local send
capacity.

Later reverse windows also show terminal pending on `uplink_channel_closed`,
for example:

```text
pending=524638 remote_to_global_rx_bytes=54057385
send_slice_accepted=53532747 tun_flush_deferred=0
```

and:

```text
pending=534136 remote_to_global_rx_bytes=68157459
send_slice_accepted=67623323 tun_flush_deferred=0
```

Those later windows are polluted by inherited QUIC congestion, so they are not
the primary throughput result. They are still useful lifecycle evidence because
the same non-empty pending close pattern repeats without deferred pacing.

## Secondary Windows

The standard and full forward windows reached high forward throughput:

- standard forward P1 receiver: `191 Mbit/s`
- full forward P1 receiver: `191 Mbit/s`

Both also generated local write pressure, TUN egress drops, and heavy QUIC
loss/congestion:

- standard forward attribution:
  `quic_loss_congestion+inherited_quic_congestion+local_write_pressure+local_tun_egress_drop`
- full forward attribution:
  `quic_loss_congestion+inherited_quic_congestion+local_write_pressure+local_tun_egress_drop`

The following reverse windows inherited that congestion:

- standard reverse P1 receiver: `14.3 Mbit/s`,
  attribution `inherited_quic_congestion`
- full reverse P1 receiver: `18.0 Mbit/s`,
  attribution `inherited_quic_congestion`

These windows should not drive the next design. The clean reverse-first window
is enough to keep the investigation on the local downlink lifecycle path.

## Rejected Explanations

- iperf3 server problem: rejected by healthy direct `.27 -> .77` and
  `.33 -> .77` baselines.
- sing-box or exit path problem: rejected by healthy exit-to-target baselines
  and clean reverse-first QUIC stats.
- clean-window QUIC congestion/loss: rejected by zero clean-window deltas.
- failed `send_slice` or TUN flush syscall: rejected by
  `send_slice_zero=0`, `send_slice_errors=0`, and `tun_flush_failures=0`.
- Knife14aq-style egress deferral: rejected by `tun_flush_deferred=0`.
- clean-window TUN qdisc drops: rejected by `tun_tx_dropped_delta=0`.

## Next Design Direction

Knife14as should stay code-side and close/lifecycle focused.

Candidate design tree:

1. Model terminal local-close versus deliverable pending explicitly. If
   `can_send=false`, distinguish "expected terminal tail after app close" from
   "premature local close while remote bytes were still arriving."
2. Add deterministic tests around pending bytes at local `Closed`/inactive
   reap boundaries. The test should prove bytes are either drained, retained by
   a live relay, or counted as terminal loss.
3. Add explicit metrics for terminal pending loss, for example
   `downlink_pending_terminal_drop_bytes` or `pending_reap_bytes`, so this class
   does not hide inside a close log line.
4. Inspect smoltcp receive-window and close timing for the reverse path. The
   clean window has no QUIC loss, no TUN drops, and no pacing deferral, but
   still oscillates at the downlink high watermark and reaches local `Closed`
   with pending bytes.
5. Use a narrow reverse-only confirmation run only after deterministic tests and
   instrumentation explain whether the pending tail is terminal after the iperf
   duration or a cause of the low receiver throughput.

Do not keep tuning the downlink egress pacer until this lifecycle branch is
resolved.
