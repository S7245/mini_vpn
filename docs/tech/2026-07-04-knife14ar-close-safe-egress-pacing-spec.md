# Knife14ar spec - close-safe TCP downlink egress pacing

Date: 2026-07-04

## Grounding

Knife14aq commit `212ce26` added remote-payload egress pacing with
`MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES=65536` as the default. The `.27`
acceptance run rejected that default:

- clean reverse-first receiver fell from Knife14ap `22.0 Mbit/s` to
  `13.8 Mbit/s`;
- `tun_flush_deferred=1057` proved the pacer was active;
- clean-window QUIC loss/congestion stayed zero;
- `send_slice_zero=0`, `send_slice_errors=0`, and `tun_flush_failures=0`;
- a relay later hit `dead_slot_reap` with `pending=576827`,
  `tcp_state=Closed`, `can_send=false`, and `can_recv=false`.

Relevant code path:

1. `handle_remote_payload` extends `downlink_pending`.
2. `flush_downlink` pushes accepted bytes into the smoltcp TCP tx buffer.
3. Knife14aq may skip immediate `iface.poll + device.flush_tx` after accepted
   bytes if the per-timer immediate budget is exhausted.
4. `reap_dead_slots` will immediately reap non-empty pending if the smoltcp
   socket is inactive and `can_send=false`.

The failed run implies the pacer reduced TUN write pressure but allowed useful
downlink bytes to sit too long near local close/reap boundaries.

## Goal

Make egress pacing safe by default:

- do not enable the failed `65536B/tick` pacer as the product default;
- preserve the env knob for explicit A/B runs;
- do not defer remote-payload egress when `downlink_pending` remains after
  `flush_downlink`;
- keep close/reap diagnostics rich enough to identify deferred flushes and
  pending-at-close in the next VPS bundle.

## Non-Goals

- Do not tune iperf3, sing-box, TUIC congestion control, or TUN queue length.
- Do not remove the egress pacing code; keep it available as an explicit
  diagnostic/A/B knob.
- Do not make an unbounded drain loop on close.
- Do not change backpressure watermarks or TCP socket buffer sizes in this
  stage.

## Invariants

- `downlink_pending` still owns bytes not accepted by smoltcp.
- Default runtime behavior must not use the rejected `65536B/tick` immediate
  budget.
- A remote-payload event with any remaining `downlink_pending` must force an
  immediate `iface.poll + flush_tx` attempt, even when the budget would otherwise
  defer.
- Pacing may only defer immediate egress after the current payload made progress
  and left no app-owned downlink backlog.
- Relay close/reap logs must include enough fields to distinguish "deferred
  pacing" from "no local send capacity" and "terminal close".

## Acceptance

Local:

- TDD red/green for default-off behavior.
- TDD red/green for pending-backlog forcing immediate egress.
- `cargo test --lib downlink_egress`
- `cargo test --lib tcp_downlink`
- `cargo test --lib client_tun`
- `cargo test --lib`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `git diff --check`

VPS:

- Run `.27` with writable TTY from the beginning.
- Use clean reverse-first P1 as the primary signal.
- Direct `.27 -> .77` and `.33 -> .77` reverse baselines must remain healthy.
- Default run must show `MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES` is no longer
  `65536`.
- No clean-window `dead_slot_reap` with large non-empty pending.
- No `send_slice_zero`, `send_slice_errors`, or `tun_flush_failures` regression.
- Throughput must not be worse than Knife14ap clean reverse-first.
