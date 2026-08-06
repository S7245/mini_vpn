# Knife15 Exit-to-Target Forwarding Observability Implementation Plan

Date: 2026-08-06

Spec:
`docs/tech/2026-08-06-knife15-exit-target-forwarding-observability-architecture-spec.md`

## Task 1: Freeze the failed artifact and direct controls

- Record exact source/binary/runner provenance, SLO failure, client/Target
  intervals, data-plane invariants, and the two Exit-to-Target control hashes.
- Classify the one-turn startup implementation as reached but insufficient.

Acceptance: result document contains no credentials and every numerical claim
is reproducible from the immutable artifacts.

## Task 2: Add a red qualification schedule test

- Add a compressed deterministic test for one exact phase order:
  forward TCP, reverse TCP, reverse UDP, short forward TCP, DNS, real-client.
- Require a non-formal success status and prove the formal M2 verdict is absent.
- Require data-quality, command, and health failures to stop immediately.

Expected RED: no qualification action/schedule exists.

## Task 3: Implement the one-cycle qualification action

- Reuse formal M2 source, baseline, direct, IPv6, full-tunnel, real-client,
  quiescence, and identity gates.
- Keep evidence and status distinct from formal `m2`.
- Run exactly one frozen-rate mixed cycle and record `PASS_NON_ACCEPTANCE`.
- Extend help, status, summary, mutual exclusion, and runbook instructions.

Acceptance: deterministic Task 2 test becomes green without changing formal M2
counts or acceptance.

## Task 4: Add red observer lifecycle tests

- Use fake SSH, tcpdump, `ss`, process table, timeout, and checksum commands.
- Cover exact filter/snaplen/ring arguments, already-running refusal, stale PID,
  SSH disconnect survival, status, graceful stop, forced cleanup, capture-drop
  report, secret failure, and bundle refusal while live.

Expected RED: no observer helper exists.

## Task 5: Implement the bounded Exit observer

- Add a separate operations script with `start`, `status`, `stop`, and `bundle`.
- Default only the known `.33`/`.77:5201` topology while allowing explicit safe
  overrides.
- Use a two-hour watchdog, 96-byte header-oriented packet snapshots, fixed
  rotation, and a target-filtered TCP_INFO sampler.
- Never print key material or TUIC credentials.

Acceptance: Task 4 becomes green; shell syntax and secret checks pass.

## Task 6: Review and full local gates

- Run focused shell tests first, then Knife15 and Knife14 shell suites.
- Run established Rust root/main/integration/release/Clippy lanes and vendored
  Quinn/proto tests even though Rust behavior is unchanged.
- Run docs, fmt, diff, and secret checks.
- Perform code review for process ownership, quoting, destructive cleanup,
  bounded disk/time, route/DNS lifecycle, false acceptance, and regression.

Acceptance: no unresolved P0/P1 and every established gate passes.

## Task 7: Deploy one observer and run one Mac qualification

- Start the reviewed observer on `.33`.
- On the Mac, use a fresh transaction through IPv6 check, baseline, direct,
  start, smoke, qualification, status, and stop.
- Stop/bundle the Exit observer immediately after the Mac result.

Acceptance: matching Mac and Exit evidence selects one discriminator row.

## Task 8: Select the next architecture

- Exit TCP retransmission/RTO: repair or replace the Exit-to-Target path/service
  environment; do not patch client pacing.
- Exit socket healthy but application-copy gaps: open mature-server copy
  scheduling/backpressure research and test sing-box/Mihomo behavior behind an
  explicit interoperability seam.
- Client QUIC ACK stall: open a bounded transport service architecture with a
  deterministic multiplexer replay; do not tune the existing priority delta.
- Observer mismatch: repair evidence before any data-plane change.

Formal M2 and M3 remain blocked until the selected architecture passes its
local gate, review, one short qualification, and cleanup.
