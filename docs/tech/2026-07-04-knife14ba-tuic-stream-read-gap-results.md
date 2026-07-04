# Knife14ba results - TUIC stream first-byte and read-gap diagnostics

Date: 2026-07-04

## Commit and artifacts

- Code commit: `7e25e92` (`fix(knife14ba): add tcp stream read-gap diagnostics`)
- VPS client: `.27` (`43.172.75.27`)
- Exit: `.33` (`43.153.32.33`)
- Target: `.77` (`43.130.32.77`)
- Remote bundle:
  `/tmp/conn/mvpn_knife14ba_stream_timing_usclient_suite_20260704_231537.tar.gz`
- Local bundle:
  `/tmp/mini_vpn/mvpn_knife14ba_stream_timing_usclient_suite_20260704_231537.tar.gz`
- Local extract:
  `/tmp/mini_vpn/knife14ba_stream_timing_231537`
- Release binary SHA256:
  `a3912da4749920ac2b0c0d860558d3215bdc303e2d9cd61b35a23817694c521e`

## Preflight

- `.27 -> .77` direct iperf was healthy:
  - forward receiver: `275 Mbit/s`
  - reverse receiver: `291 Mbit/s`
- `.33 -> .77` exit-target direct iperf was healthy:
  - forward receiver: `281 Mbit/s`
  - reverse receiver: `287 Mbit/s`
- `.33` sing-box and `.77` iperf3 were active before the suite.
- `.27` worktree was clean at `7e25e92`.

## Clean reverse-first P1

Result:

- iperf sender: `31.000 Mbit/s`
- iperf receiver: `28.700 Mbit/s`
- attribution summary:
  `local_downlink_backpressure+terminal_pending_reap+pending_at_close+tuic_stream_read_gap+relay_remote_read_gap`

Key clean-window signals:

- TUIC first byte was not slow:
  - control stream first_rx: `3ms`
  - data stream first_rx: `3ms`
- Data stream close snapshot:
  - `rx_bytes=110790062`
  - `reads=9637`
  - `max_read_gap_ms=3763`
- Relay data-stream close snapshot:
  - `remote_to_global_rx_bytes=110790062`
  - `remote_reads=9637`
  - `first_remote_read_ms=3`
  - `max_remote_read_gap_ms=3763`
- Local downlink/terminal signals:
  - `downlink_backpressure pause_edges=16 resume_edges=16`
  - `max_pending_bytes=542302`
  - `max_tx_queue_bytes=574824`
  - `pending_at_close events=1 bytes=542302`
  - `terminal_pending_reap events=1 bytes=542302`
  - close line had `tcp_state=Closed active=false can_send=false can_recv=false`
  - close line also had `may_send_false=183 may_recv_false=183`
- Clean-window negatives:
  - QUIC loss/congestion delta: `0`
  - inherited QUIC congestion: `none`
  - TUN drop delta: `0`
  - runtime TUN drop delta: `0`
  - global_rx pressure: `0`
  - local_write pressure: `0`
  - `tun_flush_failures=0`
  - `tun_flush_deferred=0`

## Parser caveat found by this stage

The new stream-gap parser correctly exposed stream timings, but its attribution
currently treats all TUIC TCP streams equally. In this run, the 30s read-gap came
from the small iperf control stream:

- control stream close: `rx_bytes=343`, `max_read_gap_ms=30147`
- data stream close: `rx_bytes=110790062`, `max_read_gap_ms=3763`

Therefore the `tuic_stream_read_gap` and `relay_remote_read_gap` labels are too
broad for this bundle. The data-stream evidence does not support "slow first
byte" and does not show a >5s data-stream read gap in the clean reverse-first
window.

## Secondary probes

The later probes are useful as contamination checks, not as the clean root:

- Standard forward P1 fell to `5.450/2.100 Mbit/s` and showed QUIC
  loss/congestion, local write pressure, and TUN tx drops.
- The standard probe's reverse half was `24.800/23.600 Mbit/s`, but it inherited
  QUIC congestion from the forward half.
- The final P1 forward recovered to `78.900/69.100 Mbit/s`; its reverse half was
  `19.100/17.900 Mbit/s` and also inherited prior QUIC congestion.

## Conclusion

Knife14ba rules out the next suspected first-byte branch for the clean
reverse-first window:

- TUIC stream open and first receive were fast (`3ms`).
- The data stream delivered about `110MB` into mini_vpn.
- QUIC, TUN drops, global_rx, and local writer pressure were quiet in the clean
  reverse-first window.

The remaining root is local TCP downlink delivery under receive-window /
tx-queue pressure:

- local tx queue crossed the downlink backpressure threshold;
- the run ended with terminal pending bytes on a closed, non-send-capable local
  TCP socket;
- close/reap is now visible rather than hidden, and it is not the original
  first-byte cause.

## Next plan

1. Patch the parser to split or filter tiny control streams, so read-gap
   attribution is based on data streams rather than idle iperf control streams.
2. Start Knife14bb focused on local tx-queue / receive-window pressure:
   - preserve the current first-byte/read-gap diagnostics;
   - add deterministic accounting/tests around tx-queue-driven pause and
     terminal pending;
   - investigate why clean reverse-first accumulates `~524-574KB` in local send
     queue and reaps `~542KB` terminal pending despite no TUN drops or QUIC loss.
3. Do not return to stale pool, iperf3, sing-box, blunt egress pacing, or TUN
   queue tuning unless Knife14bb produces contrary evidence.
