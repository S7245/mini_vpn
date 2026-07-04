# Knife14ba spec - TUIC stream first-byte and read-gap diagnostics

Date: 2026-07-04

## Grounding

Knife14az applied the local smoltcp TCP socket policy and proved it locally, but
VPS acceptance failed in a different shape:

- clean reverse-first P1 was `0.245/0.009 Mbit/s`;
- mini_vpn accepted only about `35 KiB` from the reverse data stream;
- `send_queue_max=1`, app pending was `0`, and downlink backpressure stayed
  idle;
- terminal pending and close-time pending were `0`;
- TUN drops and QUIC loss/congestion deltas were `0`;
- `.27 -> .77` and `.33 -> .77` direct baselines were healthy.

The parser attribution was `reverse_sender_backpressured`. The missing evidence
is earlier than local downlink drain: whether the TUIC TCP stream delays first
remote bytes, reads in sparse bursts, or only receives useful data after local
finish.

## Goal

Add behavior-neutral diagnostics that make reverse-first sender backpressure
actionable:

- relay-level first remote read wait and remote read-gap timing, tied to
  `handle`/`epoch`;
- TUIC TCP stream first receive and read-gap timing, tied to
  `target`/`conn`/QUIC stable id/stream id;
- low-RTT parser summary fields and attribution labels for slow first byte and
  remote read gaps;
- suite report coverage for `.33 -> .77` exit-target direct baseline when the
  stage needs it.

## Non-goals

- Do not change TUIC stream scheduling or reconnect behavior in this stage.
- Do not change close/reap, terminal pending, local socket buffers, egress
  pacing, downlink backpressure watermarks, or TUN queue length.
- Do not re-open stale pool or connection-pool tuning unless diagnostics prove
  the current single-connection path is the active limiter.
- Do not store credentials, passwords, UUIDs, or private keys in docs, learning
  memory, logs, or summaries.

## Invariants

- Diagnostics must be gated by the existing TCP diagnostic switch where they can
  add log volume.
- Timing fields must be monotonic, bounded, and formatted as integers in
  milliseconds so shell parsers remain simple.
- Relay timing must remain useful for TUIC and REALITY streams.
- TUIC timing must preserve the TCP pool lease semantics already handled by
  `TrackedRelayStream`.
- Parser additions must keep old logs parseable; absent new fields should
  summarize as zero or `n/a`.

## Acceptance

Local acceptance:

- focused Rust tests cover relay timing formatting and TUIC TCP stream diagnostic
  formatting;
- low-RTT probe self-test covers the new parser fields and attribution label;
- US-client suite self-test still passes;
- `cargo test --lib client_tun`, focused `cargo test --lib tuic`, full
  `cargo test`, `cargo test --features harness`, clippy, and `git diff --check`
  pass, with QUIC endpoint bind checks rerun outside the restricted sandbox if
  needed.

VPS acceptance:

- run one scoped `.27` reverse-first P1 on the patched commit;
- enable `.33 -> .77` exit-target direct baseline in the suite report;
- parse relay timing, TUIC stream timing, QUIC, TUN, downlink, lifecycle, and
  pending signals;
- decide the next behavior patch from timing evidence, or escalate to
  architecture review if the evidence remains incoherent.

