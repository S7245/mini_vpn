# Knife15 M2 Ambient-Traffic Recovery Implementation Plan

Date: 2026-08-05

Status: **LOCAL IMPLEMENTATION/REVIEW COMPLETE; REAL MACOS M2 PENDING**

Source of truth:
`2026-08-05-knife15-m2-ambient-traffic-recovery-architecture-spec.md`.

## Constraints

Preserve every frozen data-plane, recovery-bound, M2 schedule, route/DNS,
quality, resource, and cleanup value. Make no Apple-domain exception and do
not require system-daemon termination. Keep behavior behind the existing pure
Endpoint recovery and runner lifecycle-replay seams.

## Tasks

### Task 1 — Freeze the bundle diagnosis

- [x] Verify SHA-256, exact source, runner/binary provenance, baseline/direct,
  start/smoke/full-tunnel/real-client gates, bounded early stop, and cleanup.
- [x] Reconstruct exact active targets at the failed sample.
- [x] Prove all controlled preflight relays closed and Apple Push remained.
- [x] Correlate fifteen `no_rx`, `udp_active=false`, zero-writer-pressure
  migrations with repeated 37-byte QUIC TX.

### Task 2 — Lock TCP-only no-RX RED

- [x] Add one artifact-shaped fake-time test: active TCP ownership, no pending
  writer, transport TX/no RX beyond the existing bound.
- [x] Require no rebind and no retained generic no-RX episode.
- [x] Run only the focused test and record the expected RED.

### Task 3 — Demand-qualify generic no-RX

- [x] Keep exact TCP writer ACK-stall evaluation first.
- [x] Permit generic no-RX arming only after the existing UDP activity
  timestamp advances and while `udp_active` remains true.
- [x] Clear generic state for TCP-only samples without an eligible writer.
- [x] Reclassify existing no-RX fixtures as UDP-demand fixtures and preserve
  one-shot/current-socket recovery behavior.

### Task 4 — Lock controlled lifecycle replay RED

- [x] Add pure shell fixtures for ambient-only, controlled-open,
  gauge/replay boundary differences, duplicate lifecycle, state Closing, and
  valid handle reuse.
- [x] Require exact controlled target matching; prove Apple/system targets are
  neither privileged nor rejected.
- [x] Require BSD/macOS Bash 3.2 and awk compatibility.

### Task 5 — Implement controlled drain evidence

- [x] Replay target open, exact epoch engine installation, state Closing, and
  final close at every data-plane sample.
- [x] Extend pre-schedule evidence with replayed/global/controlled/invalid
  values and schema v4.
- [x] Accept numeric ambient pool/relay/fake-IP observations while requiring
  controlled zero, Endpoint zero debt/conservation, fresh samples, valid
  replay, and zero DNS drops.
- [x] Replace the operator-error message with the exact controlled-drain
  discriminator.

### Task 6 — Extend all six checkpoints

- [x] Add active leases and replay fields without removing global observations.
- [x] Use the existing bounded smoke timeout after each 600-second drain to
  obtain a fresh qualifying sample.
- [x] Update checkpoint envelope/SLO and positive/negative fixtures.
- [x] Keep RSS/FD/thread, labels, Endpoint, DNS, and conservation gates.

### Task 7 — Complete local gates

- [x] Focused Endpoint recovery tests.
- [x] Knife15 internal and wrapper self-tests plus Bash syntax.
- [x] Root all-targets, main, integration, release, and Clippy.
- [x] Vendored Quinn/proto and doc tests.
- [x] Knife14 shell suites, formatting, diff, links, and secret scan.
- [x] Exact 32MiB Endpoint capacity remains `>170 Mbit/s` with final
  conservation `61,440/0/0B`.

### Task 8 — Review, results, memory, commit, and push

- [x] Review recovery authority, UDP reachability, writer coverage, replay
  ordering, handle reuse, malformed evidence, log cost, macOS portability,
  checkpoint timing, old-path/SLO drift, and cleanup.
- [x] Repair every P0/P1 with a focused test; review found none unresolved.
- [x] Write local results and update runbook, `AGENTS.md`, `HANDOFF.md`,
  `TODO.md`, and `.learnings/`.
- [ ] Commit coherent changes, scan for secrets, and push the current branch.

### Task 9 — Fresh real-macOS discriminator

- [ ] Pull the reviewed descendant and rebuild release.
- [ ] Run one fresh `m2-ipv6-check -> baseline -> direct-discriminator ->
  start -> smoke -> m2 -> status -> stop`.
- [ ] Preserve `status/snapshot/stop` on failure.
- [ ] Accept M2 only after the complete schedule and cleanup pass. M3 remains
  blocked until then.

## Stop Rules

- Expected RED enters only its corresponding minimum GREEN.
- Unexpected regression is diagnosed against the architecture contract; no
  timer, constant, workload, or SLO tuning.
- Replay ambiguity fails closed. Global gauge/replay differences remain
  observations because the real log proves their lifecycle boundaries differ.
- Local capacity `<=170 Mbit/s` rejects the repair.
- No agent-run macOS TUN; after local gates/review, push under the standing
  authorization and hand the exact user sequence back.
