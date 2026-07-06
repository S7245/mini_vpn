# Knife14bs Backpressure Auto Env Spec

## Grounding

Knife14bq showed a high-average reverse-first P1 run with a deterministic tail
collapse:

- tunnel reverse P1 receiver: `149 Mbit/s` average;
- final six seconds: about `16 Mbit/s`;
- direct `.27 <-> .77` and `.33 <-> .77` baselines were healthy;
- QUIC loss/congestion/blocking deltas were zero;
- mini_vpn recorded `downlink_backpressure=693/693`,
  `tun_flush_deferred=693`, `tun_tx_dropped_delta=14804`, no
  `terminal_pending_reap`, and no app-owned pending at close.

The same bundle also showed the acceptance config:

- `MINI_VPN_TCP_TX_BUFFER_BYTES=1048576`;
- `MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=524288`;
- `MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=131072`.

`client_tun.rs` already contains auto defaults that scale downlink
backpressure to the TCP tx buffer when high/low are empty. Therefore the bq run
did not actually exercise the intended Knife14bp auto receive-window behavior:
an inherited `.env` or shell value pinned the suite to the old 512 KiB / 128 KiB
watermarks.

## Design Tree

1. Dirty-handle tx-queue pressure is missing.
   - Rejected. `downlink_pressure_stats` already samples dirty TCP
     `send_queue`, and bq reported `max_tx_queue_bytes=589819`.

2. Product auto-scaling is absent.
   - Rejected. `parse_downlink_backpressure_config_for_tx_buffer(None, None,
     1048576)` returns high `1048576` and low `262144`.

3. Acceptance inherits stale explicit watermarks from `.env`.
   - Accepted for this stage. The report and runtime log show the stale
     `524288/131072` pair despite a 1 MiB tx buffer.

4. Tune TUN queue length, egress pacer, sing-box, iperf3, or stale pool slots.
   - Rejected for Knife14bs. bq evidence already ruled these out for the
     current branch.

## Stage Goal

Knife14bs must make the US-client suite run on the binary's auto-scaled
downlink backpressure defaults when it detects the old product-default
`524288/131072` pair under a larger TCP tx buffer.

## Non-Goals

- Do not change TUIC auth, sing-box config, iperf3, stale pools, TUN queue
  length, or egress pacer behavior.
- Do not remove the ability to run explicit backpressure A/B tests.
- Do not store credentials or one-off secrets in repository files.

## Invariants

- Empty or unset `MINI_VPN_DOWNLINK_BACKPRESSURE_*` still means binary auto.
- Deliberate non-legacy high/low values must be preserved.
- The suite must provide an escape hatch for deliberate legacy-value A/B runs.
- Reports must show whether stale legacy values were normalized.
- Terminal pending, terminal-late payload, and close-drain accounting remain
  unchanged.

## Acceptance

Local:

- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`.
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`.
- Existing Knife14 low-RTT parser self-test still passes.
- `git diff --check`.

VPS:

- A scoped reverse-first P1 run shows the suite config as
  `MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=<auto>` and
  `MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=<auto>` when only the legacy pair
  was inherited.
- The mini_vpn startup log shows high/low scaled from the 1 MiB tx buffer,
  expected `high=1048576B low=262144B`.
- Parse bundle and compare to bq: tail collapse, downlink pause/resume churn,
  TUN drops, and terminal-late payload should decrease or become clearly
  explained by new evidence.
