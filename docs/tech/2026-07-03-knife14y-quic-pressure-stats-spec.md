# Knife14y spec - QUIC pressure stats for TCP stalls

## Grounding

- Input bundle: `/tmp/mvpn_knife14x_usclient_suite_20260703_120013.tar.gz`.
- Tested commit: `ee2e83a`.
- The suite reached the full tunnel path and completed. `.33` reachability,
  `.77:5201` direct iperf service, cargo discovery, and build were healthy.
- The new client QUIC window log is present:
  `bidi=512 uni=4096 stream_rx=8388608B conn_rx=33554432B send=33554432B`.
- Direct `.77` iperf receiver was about 285 Mbit/s, but tunnel forward stayed
  around Kbit/s to low Mbit/s and reverse remained Kbit/s-scale or timed out.
- `tcp-local-write-pressure` remained hot: 104 pressure lines, average wait
  about 5.1s, max wait about 37.8s. `global_rx_pressure_events` stayed zero.

## Problem

Knife14x made the client-side QUIC windows explicit, so the next unknown is not
whether those values were installed. The unresolved question is why QUIC stream
writes still block for seconds:

- peer/server flow-control may be limiting client-to-server stream writes;
- Cubic/path loss may be collapsing the congestion window;
- the `.33 -> .77` egress path or sing-box server config may be the bottleneck.

Current logs show the symptom but not the reason. Continuing to tune relay
batching or client receive windows would be blind.

## Goal

Emit gated QUIC connection stats during acceptance runs so each future
`tcp-local-write-pressure` run can be attributed to flow-control, congestion, or
server/path behavior.

## Non-goals

- Do not change TUIC framing, TCP relay lifecycle, TCP pooling, or congestion
  controller defaults.
- Do not change the client QUIC flow-control values from knife14x.
- Do not make production logs noisy when TCP diagnostics are disabled.

## Design

- Add a TUIC QUIC stats interval to `TuicClientConfig`.
- Default behavior:
  - production: stats disabled;
  - acceptance: `MINI_VPN_TCP_DIAG=1` enables stats;
  - stats period follows `MINI_VPN_METRICS_SECS`, defaulting to 30s;
  - `MINI_VPN_TUIC_QUIC_STATS_SECS=0` explicitly disables stats.
- Spawn a stats logger per TUIC connection and re-spawn it after reconnect.
- Log both signal families:
  - flow-control: `tx_blocked`, `rx_blocked`, `tx_window`, `rx_window`;
  - congestion/path: `rtt`, `cwnd`, `lost`, `lost_bytes`,
    `congestion_events`.

## Acceptance

- Unit tests cover stats interval parsing and stats line formatting.
- Local tests and clippy pass.
- Next VPS bundle contains `TUIC QUIC stats` lines near the same period as
  `tcp-local-write-pressure`.
- If `tx_blocked(data|stream)` rises with low loss, focus on sing-box/server
  flow-control. If loss/congestion rises and cwnd stays small, test BBR or VPS
  path. If neither rises while writes still stall, inspect TUIC server behavior
  and `.33 -> .77` egress directly.
