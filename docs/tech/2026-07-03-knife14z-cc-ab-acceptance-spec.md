# Knife14z spec - congestion-control A/B acceptance

## Grounding

- Input bundle: `/tmp/mini_vpn/mvpn_knife14y_usclient_suite_20260703_143704.tar.gz`.
- Tested commit: `f7deb9b`.
- The suite reached the full tunnel path and completed. `.33` reachability,
  `.77:5201` direct iperf service, cargo discovery, build, and TUIC startup all
  passed.
- Knife14y QUIC stats were present and useful. During stalls,
  `tx_blocked(data|stream)` stayed zero, while loss and congestion rose sharply:
  examples include `lost=75744/2058977`, `lost_bytes=95844092`,
  `congestion_events=48725`, and `cwnd=2904`.
- Direct `.77` iperf still showed a lossy path (`Retr=2750` in the 1s
  preflight), but the direct receiver was around 280 Mbit/s. Tunnel forward and
  reverse improved relative to the earlier Kbit/s runs, but full-sweep Cubic
  still had long zero-rate windows, especially at P8.

## Problem

The current acceptance blocker is no longer attributable to missing client QUIC
windows or peer flow-control blocked frames. The live evidence points to
congestion/path behavior under Cubic on this specific Client -> Exit -> Target
path.

Changing the product default CC would be premature because earlier UDP datagram
acceptance found Cubic safer for live UDP behavior. The next useful acceptance
step is a controlled same-script A/B run, where Cubic and BBR are tested from
fresh client-tun processes with identical target, MTU, pool, diagnostics, and
preflight checks.

## Goal

Teach the US-client tunnel suite to run one or more congestion-control variants
in a single report bundle, with isolated client logs and probe artifacts per
variant.

## Non-goals

- Do not change the runtime default `MINI_VPN_TUIC_CC=cubic`.
- Do not change TUIC framing, relay lifecycle, TUN MTU, TCP pool sizing, or
  backpressure thresholds.
- Do not hide VPS service failures behind A/B logic; the existing preflight
  checks remain mandatory by default.

## Design

- Add `CC_SWEEP` to `scripts/knife14b-usclient-tunnel-suite.sh`.
- Default behavior stays unchanged:
  - if `CC_SWEEP` is empty, run exactly one variant using
    `MINI_VPN_TUIC_CC=${MINI_VPN_TUIC_CC:-cubic}`;
  - existing output names remain compatible for the single-variant path.
- When `CC_SWEEP` is set, run each CC as an isolated variant:
  - stop old tunnel before each variant;
  - create a per-variant client log;
  - start `client-tun` with that variant's `MINI_VPN_TUIC_CC`;
  - route the target into the current TUN;
  - run standalone P1 and, if quiet, the full sweep;
  - include all variant logs/probe reports in the final tarball.
- Label variant sections and artifact filenames with the CC name so the returned
  bundle can be compared without guessing which run produced which stats.

## Acceptance

- `bash -n` passes for the modified shell scripts.
- A local smoke run with `CC_SWEEP="cubic bbr"` must dispatch a child suite and
  fail cleanly at the first environment guard available on that host. On the
  developer macOS host this is the Linux guard; on Ubuntu/VPS it should reach
  TUIC env validation if secrets are intentionally omitted.
- Next VPS bundle contains both Cubic and BBR sections, with separate
  `mvpn_accept_<cc>_<timestamp>.log` files and probe reports.
- If BBR reduces zero-rate windows/loss and raises forward P8 without damaging
  reverse, consider a per-profile TCP CC option or path heuristic. If BBR is not
  better, inspect `.33 -> .77` path/server behavior before touching relay code.
