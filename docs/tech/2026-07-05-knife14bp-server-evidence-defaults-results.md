# Knife14bp Server Evidence Defaults Results

Date: 2026-07-05

## Code Under Test

- Commit: `d5d8542` (`fix(knife14bp): default known server evidence ssh`)
- Client VPS: `.27` (`43.172.75.27`)
- Exit VPS: `.33` (`43.153.32.33`)
- Target VPS: `.77` (`43.130.32.77`)
- Remote bundle:
  `/tmp/mini_vpn/knife14bp_evidence_defaults_20260705/mvpn_knife14bp_evidence_defaults_d5d8542_usclient_suite_20260705_193204.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14bp_evidence_defaults_20260705_193204/`

## Local And Remote Gates

Local:

- TDD self-test first failed on missing
  `apply_server_evidence_ssh_defaults`.
- `scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `git diff --check`
- staged `git diff --cached --check`

Remote `.27`:

- Deployed `d5d8542` by git bundle.
- `scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo build --release`

## Run Settings

- `RUN_REVERSE_FIRST_P1=1`
- `STOP_AFTER_REVERSE_FIRST_P1=1`
- `SERVER_EVIDENCE_CHECK=1`
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

The run deliberately unset `EXIT_SSH_HOST`, `TARGET_SSH_HOST`,
`EXIT_SSH_KEY`, and `TARGET_SSH_KEY` after sourcing `.env` so the suite had to
exercise the Knife14bp defaults.

## Script Acceptance

Knife14bp script acceptance passed.

The suite report resolved:

- `EXIT_SSH_HOST=ubuntu@43.153.32.33`
- `EXIT_SSH_KEY=<set>`
- `TARGET_SSH_HOST=ubuntu@43.130.32.77`
- `TARGET_SSH_KEY=<set>`

The generated bundle included:

- `mvpn_knife14bp_evidence_defaults_d5d8542_server_evidence_mtu1200_reverse_first_p1_20260705_193204.md`
- `.33` time check with `exit_time_status=0`
- `.77` time check with `target_time_status=0`
- `.33` sing-box log extraction with `exit_log_status=0`
- `.77` iperf3 journal extraction with `target_journal_status=0`

No `fail auth` line was present in the bundle.

## Preflight

- `.27` `.env` present; `.27` VPS SSH key file present.
- `.33` sing-box active; NTP synchronized.
- `.77` iperf3 active; NTP synchronized.
- Direct `.27 -> .77`: `338/279 Mbit/s`.
- Direct `.27 <- .77`: `313/280 Mbit/s`.

## Tunnel Result

Reverse-first P1 over mini_vpn failed:

- tunnel sender: `1.00 MBytes / 280 Kbit/s`
- tunnel receiver: `9.65 KBytes / 2.64 Kbit/s`

This is not the clean Knife14bo `25 Mbit/s` tx-queue-only shape. It is a
near-no-data / delayed-stream shape.

## Key Signals

Server side:

- `.33` showed current TUIC inbound connections from `.27` and direct outbound
  connections to `.77:5201`.
- `.33` did not show current TUIC `fail auth`.
- `.77` iperf3 journal showed the target sender itself at
  `1.00 MBytes / 280 Kbit/s` with zero-throughput seconds after the first
  second.

mini_vpn:

- `terminal_pending_reap=0`.
- `pending_at_close=0`.
- `terminal_late_remote_payload=28544B` across `2` events.
- `tun_tx_dropped_delta=0`.
- `downlink_backpressure pause/resume=0/0`.
- QUIC loss/congestion deltas: `0/0`.
- Data stream first RX: `17142ms`.
- Data stream read-gap max: `20524ms`.
- Data stream pending-gap max: `20522ms`.
- Data stream RX max: `96296B`.

Close accounting worked as intended: terminal-late payload was rejected before
entering app-owned pending, and no terminal pending was reaped.

## Classification

Knife14bp completed its script goal: server evidence no longer depends on
manual SSH host envs for the known `.33/.77` topology.

The throughput run did not advance Knife14 acceptance. The evidence rejects:

- hidden high-rate `.77` sender loss inside mini_vpn;
- current `.33` TUIC fail-auth;
- TUN qdisc drops;
- clean-window QUIC loss/congestion;
- app-owned terminal pending reap.

The active failure shape is a no-data / stream-readiness / receive-window
stall with terminal-late payload correctly accounted at close. Because this run
is not the clean Knife14bo tx-queue-only branch, do not make a behavior change
from it alone.

## Next Plan Proposal

Do not patch behavior until this plan is confirmed.

1. Run one same-window evidence repeat on `d5d8542`, still reverse-first P1,
   with `SERVER_EVIDENCE_CHECK=1` and default SSH evidence.
2. Enable `EXIT_TO_TARGET_IPERF_CHECK=1` with `EXIT_TO_TARGET_IPERF_REQUIRED=0`
   to capture the `.33 -> .77` path baseline inside the same suite without
   failing the run if `.33` lacks an optional tool.
3. If the repeat again shows `.77` sender near-zero while direct baselines are
   healthy and `.33` has no auth/service issue, start a Knife14bq
   evidence-only patch that instruments stream-open/control-vs-data mapping and
   local TCP window/ACK state before another behavior change.
4. If the repeat returns to the Knife14bo tx-queue-only shape, resume the
   receive-window / tx-queue instrumentation plan from the clean branch.
