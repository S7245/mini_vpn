# Knife14aa spec - direct reverse preflight

## Grounding

- Input bundle: `/tmp/mvpn_knife14z_usclient_suite_20260703_154429.tar.gz`.
- Tested commit: `1b6f2d8`.
- The CC sweep harness worked: Cubic and BBR ran as isolated child suites with
  separate reports, bundles, and `mvpn_accept_<cc>_<timestamp>.log` files.
- Direct `.77:5201` forward preflight was healthy but lossy in both child
  suites: receiver was about 279-280 Mbit/s, with thousands of retransmits.
- BBR materially improved forward tunnel throughput on this path:
  standalone P1 reached 192 Mbit/s, full P2 reached 192 Mbit/s, and full P4
  reached 150 Mbit/s. Cubic stayed around 1-27 Mbit/s.
- BBR did not solve the whole acceptance problem: P8 collapsed to 18.6 Mbit/s,
  and reverse remained Kbit/s-scale for both Cubic and BBR.

## Problem

The suite currently checks only direct forward iperf service before routing the
target into the TUN. When reverse tunnel results collapse, the returned bundle
does not prove whether `.77 -> client` direct reverse is healthy outside the
tunnel.

Without that baseline, the next stage could confuse a target/path/service issue
with a relay lifecycle or QUIC congestion issue.

## Goal

Before starting `client-tun`, record a direct `iperf3 -R` baseline against the
target VPS, next to the existing direct forward iperf check.

Also fail early if `OUT_DIR` cannot be created or written by the current user,
so `/tmp/conn` ownership mistakes produce a clear fix command instead of later
`tee: Permission denied` noise.

## Non-goals

- Do not change the runtime data plane.
- Do not change the default congestion controller.
- Do not enforce a throughput threshold in the script; the report should capture
  the measured direct reverse value, and only fail when the command itself fails
  unless explicitly configured otherwise.

## Design

- Add `DIRECT_IPERF_REVERSE_CHECK=1` to the US-client suite.
- Add `DIRECT_IPERF_REVERSE_REQUIRED=1` so command failure stops the suite before
  VPS time is spent on un-attributable reverse tunnel tests.
- Add an `OUT_DIR` creation/write probe before report paths are opened.
- Keep the check short by reusing `DIRECT_IPERF_DURATION` and
  `DIRECT_IPERF_TIMEOUT`.
- Allow operators to continue sampling with
  `DIRECT_IPERF_REVERSE_REQUIRED=0` if they intentionally want a degraded
  reverse baseline in the report.

## Acceptance

- `bash -n scripts/knife14b-usclient-tunnel-suite.sh` passes.
- The help output documents the new direct reverse preflight knobs.
- A local smoke run without TUIC secrets still reaches the existing env guard
  and does not require Linux-only network setup.
- The next VPS bundle contains a `Target VPS iperf3 Reverse Baseline` section
  before `Start mini_vpn client-tun`.
- If `OUT_DIR` is not writable, the script exits before writing the report and
  prints a `sudo chown` fix command.
