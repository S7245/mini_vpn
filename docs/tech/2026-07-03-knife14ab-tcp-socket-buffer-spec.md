# Knife14ab spec - TCP socket buffer BDP

## Grounding

- Input bundle: `/tmp/mini_vpn/mvpn_knife14aa_usclient_suite_20260703_162932.tar.gz`.
- Tested commit: `7621bc6`.
- Direct target baselines were healthy:
  - forward direct receiver: 257 Mbit/s;
  - reverse direct receiver: 299 Mbit/s.
- Tunnel reverse was much lower even with direct reverse proven healthy:
  standalone P1 reverse reached 8.25 Mbit/s, and full reverse reached
  18-27 Mbit/s.
- Client diagnostics showed downlink behavior rather than VPS service failure:
  `tcp-downlink-backpressure` fired, reverse close logs had high
  `remote_to_global_rx_bytes`, and close snapshots still had pending downlink
  bytes. QUIC `tx_blocked` stayed zero.
- The smoltcp TCP socket tx buffer is fixed at 65,535 bytes today. For reverse
  traffic, that is the local advertised/in-flight window between mini_vpn and
  the receiving application.

## Problem

The reverse path has a healthy direct server-to-client link, but the tunnel can
only inject remote bytes into a 64KiB local TCP send buffer per flow. Extra
bytes spill into `downlink_pending`, triggering the suite's downlink
backpressure and producing a sawtooth.

Hard-coding a larger buffer globally would raise the worst-case listener memory
budget. The project needs to tolerate different VPS and device profiles, so the
buffer should be explicit configuration with conservative defaults.

## Goal

Make smoltcp TCP listener rx/tx buffer sizes configurable while preserving the
existing default behavior. The US-client acceptance suite should opt into 1MiB
rx/tx buffers so the next VPS run can test whether reverse throughput is limited
by the 64KiB local TCP socket window.

## Non-goals

- Do not change TUIC congestion control defaults.
- Do not remove downlink backpressure.
- Do not raise product defaults until acceptance proves the memory/throughput
  tradeoff.
- Do not change relay lifecycle or FIN handling in this stage.

## Design

- Add `TcpSocketBufferConfig { rx_bytes, tx_bytes }`.
- Defaults remain `65_535` for both directions.
- `MINI_VPN_TCP_RX_BUFFER_BYTES` and `MINI_VPN_TCP_TX_BUFFER_BYTES` accept values
  from 4KiB through 16MiB; invalid values fall back to defaults.
- `ListenerRegistry` stores the socket buffer config and uses it for both
  initial listener pools and elastic spare listener creation.
- Startup logs print the effective TCP socket buffer sizes.
- The US-client suite exports and passes 1MiB rx/tx values by default.

## Acceptance

- Unit tests prove parsing defaults/bounds and listener socket capacities.
- `bash -n` passes for modified shell scripts.
- `cargo test` and `cargo clippy --all-targets -- -D warnings` pass.
- The next VPS report startup log includes
  `TCP socket buffers: rx=1048576B tx=1048576B`.
- If reverse tunnel throughput improves materially and
  `tcp-downlink-backpressure` drops, keep this as an explicit high-throughput
  profile knob. If not, inspect ACK scheduling/global_rx fairness next.
