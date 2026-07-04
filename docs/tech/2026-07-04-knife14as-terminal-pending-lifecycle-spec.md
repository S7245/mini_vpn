# Knife14as spec - terminal pending lifecycle accounting

Date: 2026-07-04

## Grounding

Knife14ar failed VPS acceptance after removing the rejected close-unsafe egress
pacing default:

- direct `.27`, `.33`, and `.77` baselines were healthy;
- clean reverse-first QUIC loss/congestion deltas were zero;
- clean reverse-first TUN RX/TX drop deltas were zero;
- `send_slice_zero=0`, `send_slice_errors=0`, and `tun_flush_failures=0`;
- `tun_flush_deferred=0`, so the Knife14aq pacer was not the reason bytes stayed
  pending;
- the flow later reaped with `pending=224765`, `tcp_state=Closed`,
  `active=false`, and `can_send=false`.

That leaves one narrow branch: mini_vpn local TCP downlink lifecycle,
receive-window behavior, close-drain, and terminal pending accounting.

## Problem

The current close diagnostic exposes terminal pending only as raw fields in a
single `tcp-handle-close` line. That is enough for manual inspection, but not
enough for a repeatable acceptance decision:

- terminal pending after the app has closed may be expected cleanup;
- terminal pending can also be the visible tail of premature local close or
  receive-window behavior that throttled reverse throughput before close;
- the throughput summaries do not yet count terminal pending events or bytes, so
  the class can disappear into raw logs.

Knife14as should make the lifecycle branch measurable before changing pacing,
TUN queue length, connection pooling, or iperf/sing-box settings again.

## Goal

Add deterministic accounting that distinguishes terminal undeliverable pending
from still-deliverable pending:

- every TCP handle close diagnostic includes the local socket snapshot before
  abort/relisten;
- closed, inactive, not-send-capable pending bytes are explicitly reported as
  terminal pending reap bytes;
- the low-RTT probe summary parses terminal pending events, total bytes, and max
  bytes per event;
- local tests lock both the Rust classifier and the shell parser.

## Non-Goals

- Do not change downlink egress pacing, flush budgets, backpressure watermarks,
  TUN queue length, TUIC pool selection, QUIC congestion control, or iperf3
  configuration in this step.
- Do not defer cleanup for `Closed && !can_send` pending; smoltcp cannot deliver
  from that state.
- Do not claim terminal pending is the root cause by itself. It is a lifecycle
  discriminator that must be interpreted alongside backpressure, late-remote,
  QUIC, TUN, and sender-side evidence.

## Design Tree

1. Treat the terminal pending tail as the direct root cause and tune around it.
   Rejected. Once the local socket is `Closed && !can_send`, the bytes are no
   longer deliverable. The cause, if any, happened earlier.

2. Reintroduce a grace window for `Closed && !can_send` pending.
   Rejected. Knife14u already proved not-send-capable inactive pending should be
   reaped immediately to avoid stale flow pollution.

3. Add receive-window or close propagation changes immediately.
   Deferred. Knife14as first needs a repeatable discriminator showing whether
   low reverse throughput correlates with premature local close, sender-side
   backpressure, downlink pending oscillation, or expected post-duration cleanup.

4. Add terminal pending accounting and parser support first.
   Selected. This is behavior-neutral, TDD-friendly, and gives the next scoped
   VPS run a clean answer to "expected terminal tail or earlier lifecycle bug?"

## Invariants

- `Listening` slots are never reaped.
- Active pending downlink is not terminal pending.
- Inactive send-capable pending remains protected by the existing bounded grace.
- Inactive `Closed && !can_send` pending is immediately reapable and counted as
  terminal pending.
- Close diagnostics must include `tcp_state`, `active`, `can_send`, and
  `can_recv` for all TCP close reasons, not only `dead_slot_reap`.
- Parser accounting must handle both new explicit
  `terminal_pending_reap_bytes=` logs and older logs that only have
  `pending>0 tcp_state=Closed can_send=false`.

## Acceptance

Local:

- Rust unit tests cover terminal pending classification.
- Probe self-test covers explicit terminal pending summary parsing.
- `cargo test --lib client_tun`
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`

VPS:

- Run one scoped reverse-first acceptance only after local gates pass.
- The report must include `terminal_pending_reap`.
- If throughput is still low, the summary must show whether terminal pending
  coexists with downlink backpressure, late remote after local finish, QUIC
  pressure, TUN drops, or reverse sender backpressure.
