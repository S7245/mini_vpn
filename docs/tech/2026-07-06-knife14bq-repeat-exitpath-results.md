# Knife14bq Repeat Exit-Path Results

Date: 2026-07-06

## Code Under Test

- Commit: `d5d8542` (`fix(knife14bp): default known server evidence ssh`)
- Client VPS: `.27` (`43.172.75.27`)
- Exit VPS: `.33` (`43.153.32.33`)
- Target VPS: `.77` (`43.130.32.77`)
- Remote bundle:
  `/tmp/mini_vpn/knife14bp_repeat_exitpath_20260706/mvpn_knife14bp_repeat_exitpath_d5d8542_usclient_suite_20260706_155856.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14bp_repeat_exitpath_20260706_155856/`

## Stage Goal

Repeat the Knife14bp no-data run in the same reverse-first P1 shape, keep
server evidence defaults enabled, and add a same-window `.33 <-> .77` iperf3
path check. The goal was evidence, not a behavior change.

## Run Settings

- `RUN_REVERSE_FIRST_P1=1`
- `STOP_AFTER_REVERSE_FIRST_P1=1`
- `SERVER_EVIDENCE_CHECK=1`
- `EXIT_TO_TARGET_IPERF_CHECK=1`
- `EXIT_TO_TARGET_IPERF_REQUIRED=0`
- `MINI_VPN_TUIC_CC=cubic`
- `PARALLEL_SET=1`
- `DURATION=30`
- `MINI_VPN_TCP_DIAG=1`
- `MINI_VPN_TUN_MTU=1200`
- `MINI_VPN_TCP_RX_BUFFER_BYTES=1048576`
- `MINI_VPN_TCP_TX_BUFFER_BYTES=1048576`
- `MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=524288`
- `MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=131072`
- `MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES=262144`
- `MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES=16777216`
- `MINI_VPN_TUN_RX_DRAIN_BUDGET=0`

The run deliberately unset explicit `EXIT_SSH_*` and `TARGET_SSH_*` values
after sourcing `.env`, so the suite again exercised the Knife14bp known-topology
server evidence defaults.

## Preflight

- `.27` repo was clean at `d5d85429`; `.env` and the VPS SSH key were present.
- `.33` sing-box was active and NTP synchronized.
- `.77` iperf3 was active and NTP synchronized.
- Direct `.27 -> .77`: `339/281 Mbit/s`.
- Direct `.27 <- .77`: `309/284 Mbit/s`.
- Exit `.33 -> .77`: `315/282 Mbit/s`.
- Exit `.33 <- .77`: `315/283 Mbit/s`.

The added exit-target check rejects a broken `.33 <-> .77` path as the cause of
this run.

## Tunnel Result

Reverse-first P1 over mini_vpn:

- tunnel sender: `537 MBytes / 150 Mbit/s`
- tunnel receiver: `531 MBytes / 149 Mbit/s`

The first 24 seconds ran mostly in the `126-268 Mbit/s` range. The final six
seconds collapsed to about `15.7-16.8 Mbit/s`.

The target `.77` iperf3 journal matched the tunnel sender:

- `.77` sender total: `537 MBytes / 150 Mbit/s`
- `.77` sender tail: about `11.5-24.1 Mbit/s` for seconds `24-30`

This repeat invalidates the prior hypothesis that the no-data shape is stable.
It also proves the current code can leave the earlier `10-20 Mbit/s` primary
band on average, but it does not satisfy stable throughput acceptance because
the tail still collapses.

## Key Signals

Server side:

- `.33` showed current TUIC inbound connections from `.27`.
- `.33` showed direct outbound connections to `.77:5201`.
- `.33` did not show current TUIC `fail auth`.
- Background VLESS/REALITY invalid-connection log noise was present, but it was
  not the TUIC acceptance path for this run.

mini_vpn:

- `iperf_sender_mbps=150.000`.
- `iperf_receiver_mbps=149.000`.
- `downlink_backpressure pause/resume=693/693`.
- `max_tx_queue_bytes=589819`.
- `downlink_flush accepted_bytes=550507645`.
- `pending_total_max=0`.
- `send_slice_zero=0`.
- `send_slice_errors=0`.
- `tun_flush_failures=0`.
- `tun_flush_deferred=693`.
- `terminal_pending_reap=0`.
- `pending_at_close=0`.
- `terminal_late_remote_payload=1474528B` across `835` events.
- `tun_tx_dropped_delta=14804`.
- `runtime_tun_egress drop_events=5 drop_delta_total=14804 max_delta=5472`.
- `tun_egress_feedback pause/resume=5/5 max_pressure_bytes=589816`.
- QUIC loss/congestion deltas: `0/0`.
- QUIC tx/rx blocked deltas: `0`.

Close line for the main reverse flow:

- `pending=0`
- `terminal_pending_reap_bytes=0`
- `terminal_late_remote_payload_bytes=1474528`
- `tcp_state=Closed`
- `can_send=false`
- `send_queue=2816`

## Classification

Knife14bq did not produce a repeat no-data result. The new shape is:

- healthy `.27 <-> .77` direct baselines;
- healthy `.33 <-> .77` exit-target baselines;
- current `.33` TUIC inbound/direct outbound with no TUIC fail-auth;
- high average mini_vpn reverse throughput at `149 Mbit/s`;
- deterministic-looking late tail collapse;
- local TUN egress drops and downlink backpressure in the same window;
- no terminal pending reap and no app-owned pending at close;
- terminal-late payload correctly counted after terminal no-send state;
- no QUIC loss/congestion or blocked-window signal.

Therefore the next branch is not iperf3, sing-box auth, stale TUIC pool slots,
or exit-target path health. The remaining branch is the local downlink/TUN
egress pressure and close-tail interaction: specifically why tx-queue pressure
and TUN drops appear during a run that otherwise has no app-owned pending and no
QUIC congestion.

## Next Plan Proposal

Do not make a behavior patch from this repeat alone. Start a small Knife14br
stage that first writes spec/TDD for tail-collapse attribution:

1. Add evidence that correlates per-second iperf tail rate with
   `tcp-tun-egress`, downlink pause/resume, `tun_flush_deferred`, and close-tail
   terminal-late bytes.
2. Keep the accepted terminal-pending invariant: `Closed && !can_send` late
   payload must stay outside app-owned pending and be counted explicitly.
3. Test the local accounting/parser path so future bundles distinguish
   stable-throughput, no-data, and tail-collapse shapes without manual log
   reading.
4. Only after that evidence patch, choose a behavior fix for local TUN egress
   pressure/backpressure timing. Avoid returning to blunt egress-pacer tuning.
