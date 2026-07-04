# Knife14ap spec - observe bounded downlink flush progress

Date: 2026-07-04

## Grounding

Knife14ao commit `0d0765f` promoted the 512 KiB / 128 KiB TCP downlink
backpressure watermarks and aligned the US-client suite defaults. The `.27`
acceptance run reached the iperf window with those defaults active, but clean
reverse-first P1 still reported only 21.4 Mbit/s receiver. The direct paths were
healthy, sing-box and iperf3 were healthy, QUIC showed no loss/congestion in
that first reverse window, and pending was bounded near the new high watermark:

- bundle: `/tmp/mini_vpn/mvpn_knife14ao_default_bp512_128_flush256_tty_usclient_suite_20260704_144755.tar.gz`
- clean reverse-first P1: `22.5/21.4 Mbit/s`
- downlink: `pause_edges=11`, `resume_edges=10`,
  `max_pending_bytes=589698`
- TUN: `tun_tx_dropped_delta=496`
- QUIC: no loss/congestion delta, no inherited congestion
- attribution: `local_tun_egress_drop+local_downlink_backpressure`

This falsifies the narrow claim that promoting the watermarks is sufficient. It
does not falsify the local downlink/TUN branch.

## Grill / Design Tree

1. Keep lowering watermarks immediately.
   - Rejected for this stage. The same nominal 512/128 KiB values produced one
     high no-code A/B result and one low acceptance result, so a further blind
     tune risks chasing variance.
2. Blame iperf3, sing-box, or VPS path.
   - Rejected. Both client-target and exit-target direct baselines were healthy,
     and the clean reverse-first tunnel window had no QUIC loss/congestion.
3. Rewrite the downlink scheduler now.
   - Deferred. We do not yet know whether the low window is dominated by
     repeated `can_send=false`, zero/short `send_slice`, flush budget clipping,
     or TUN/qdisc behavior after successful smoltcp acceptance.
4. Add a focused downlink flush progress signal.
   - Accepted. This is small, testable, and keeps data-plane behavior unchanged
     while making the next acceptance bundle decisive.

## Goal

Add per-window diagnostics that answer whether bounded pending is failing to
turn into local TCP/TUN egress progress:

- count pending flush attempts;
- count attempts skipped because smoltcp `can_send` was false;
- count `send_slice` calls, zero accepts, errors, accepted bytes, and maximum
  single-call accepted bytes;
- count flush attempts clipped by `MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES`;
- print an aggregate `tcp-downlink-flush` line during the regular metrics tick;
- teach the low-RTT probe to summarize those fields as `downlink_flush:`.

## Non-Goals

- No behavior change to watermarks, flush budget, TUN queue length, QUIC
  congestion control, TUIC TCP pool, or sing-box.
- No sleeps or pacing algorithm in this stage.
- No claim that this fixes throughput.

## Invariants

- `downlink_pending` remains lossless.
- `flush_downlink` preserves existing return semantics.
- New counters must be per-slot and reset when a slot is rearmed, like the
  existing downlink diagnostics.
- The probe parser must remain backward compatible with older logs that do not
  contain `tcp-downlink-flush`.

## Acceptance

Local:

- RED/GREEN Rust test covers the new flush progress counters and aggregate
  formatter.
- RED/GREEN low-RTT probe self-test covers `downlink_flush:` parsing.
- `cargo test --lib client_tun`
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`

VPS:

- Next clean reverse-first report includes `downlink_flush:`.
- If throughput remains low, the report must distinguish at least one of:
  `can_send=false` pressure, `send_slice` zero/errors, repeated budget clipping,
  or successful smoltcp acceptance followed by TUN drops.
