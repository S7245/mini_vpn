# Knife14bh Stream-Gap A/B Spec

Date: 2026-07-05

## Stage Goal

Knife14bh must separate two remaining explanations for the clean reverse-first
P1 failure:

1. the Knife14bg TUN RX drain cadence change altered the clean path; or
2. reverse downlink is already starved before local TUN egress, at the TUIC TCP
   stream / relay remote-read boundary.

This stage is intentionally diagnostic-first. It must not tune sing-box,
iperf3, stale TUIC pool slots, TUN qdisc length, downlink egress pacing, or
close/reap grace unless new evidence invalidates the current branch.

## Grounding

Knife14bg ran commit `356e2d2` and produced bundle
`mvpn_knife14bg_tun_rx_drain_usclient_suite_20260705_085637.tar.gz`.

Clean reverse-first P1 failed at `0.315/0.025 Mbit/s` while:

- direct `.27 -> .77` and `.33 -> .77` baselines were healthy;
- `.33` sing-box stayed active and accepted TUIC/direct flows;
- clean-window QUIC loss/congestion was zero;
- TUN drops, TUN flush failures, `send_slice` errors, terminal pending, and
  pending-at-close were zero;
- `tcp-tun-rx-drain` processed TCP packets but did not restore throughput;
- the data stream delivered only about 92 KiB into mini_vpn before the long
  gap.

Therefore Knife14bh should make the bg drain change switchable and add direct
stream pending evidence before another behavioral fix.

## Non-Goals

- No new throughput optimization.
- No change to TUIC authentication, sing-box config, iperf server behavior, or
  stale pool handling.
- No broad close/reap rewrite.
- No default behavior change except extra diagnostics and an env-configurable
  drain budget whose default preserves Knife14bg behavior.

## Invariants

- Default `MINI_VPN_TUN_RX_DRAIN_BUDGET` must remain `8`, preserving the bg
  runtime behavior.
- Setting `MINI_VPN_TUN_RX_DRAIN_BUDGET=0` must disable opportunistic drain
  without touching downlink flush, relay close, or TCP socket buffer behavior.
- TUIC stream pending diagnostics must be behavior-neutral: log only, no wakeup
  or backpressure side effects.
- Suite reverse-only mode must still gather preflight, startup, metrics, final
  snapshots, and bundle artifacts.

## Acceptance

Local:

- focused Rust tests for drain budget parsing and TUIC stream pending accounting;
- low-RTT parser self-test covering the new pending summary and attribution;
- suite self-test covering the new reverse-only stop flag and drain budget help;
- existing relevant client-tun / TUIC diagnostic tests;
- `git diff --check`, shell syntax, and clippy.

VPS:

- run a scoped reverse-first P1 bundle with default drain budget;
- run a second scoped reverse-first P1 bundle with
  `MINI_VPN_TUN_RX_DRAIN_BUDGET=0`;
- both runs use `STOP_AFTER_REVERSE_FIRST_P1=1` to avoid later polluted probes;
- parse each bundle and compare receiver throughput, `tcp-tun-rx-drain`, and
  `tuic-tcp-stream-pending` lines.

Decision:

- If drain budget `0` restores throughput, Knife14bg introduced a local cadence
  regression and the next patch must narrow or revert that behavior.
- If both runs remain low and show long TUIC pending gaps, next work moves to
  TUIC stream/receive-window/remote-read behavior.
- If both runs remain low without pending evidence, re-evaluate relay polling
  architecture before more suffix tuning.
