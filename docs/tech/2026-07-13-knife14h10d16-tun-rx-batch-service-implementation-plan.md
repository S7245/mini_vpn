# Knife14h10d16 TUN RX Batch Relay Service Implementation Plan

Date: 2026-07-13
Status: **IMPLEMENTED; VPS SAFETY FAIL; BRANCH CLOSED**

Source of truth:
`docs/tech/2026-07-13-knife14h10d16-tun-rx-batch-service-architecture-spec.md`.

Goal: remove per-packet `process_dirty_relay` amplification from bounded TUN
drains without changing D16, endpoint pacing, product parameters, or protocol
behavior.

## Task 1: Preserve The VPS Failure Evidence

- [x] Archive the sanitized VPS bundle and SHA-256.
- [x] Record throughput, TUN drops, QUIC loss, endpoint conservation, pool
  attribution, lifecycle, preflight, and cleanup.
- [x] Apply the endpoint architecture stop rule: no pacing or constant tuning.

## Task 2: Focused RED Reachability Tracer

- [x] Add a test-only TUN device with multiple ready TCP packets and a counting
  `MetricsSink`.
- [x] Call the production bounded drain seam with `N > 1` packets.
- [x] Assert packet consumption/order and expect RED because relay enters equal
  packet count rather than one batch pass.
- [x] Preserve a default-path single-packet test.

## Task 3: Extract One-Packet Ingest

- [x] Extract `ingest_ready_tun_rx_packet` from
  `process_ready_tun_rx_packet`.
- [x] Keep UDP/DNS bypass, SYN inspection, `Interface::poll`, flush, dirty
  marking, and return kind unchanged.
- [x] Keep `process_ready_tun_rx_packet` as ingest plus one dirty-service pass
  for the direct wait branch.
- [x] Run focused TCP, UDP, DNS, SYN, lifecycle, and default tests.

## Task 4: Batch Dirty-Service Boundary

- [x] Make `drain_ready_tun_rx` use ingest-only calls inside its existing
  bounded loop.
- [x] After a non-empty TCP batch, call `process_dirty_relay` exactly once with
  the same pause/admission arguments.
- [x] Do not service TCP listeners for DNS/UDP-only batches.
- [x] Keep the enclosing actor's post-drain phase/admission pass; prove at most
  two relay passes per actor cycle.
- [x] Turn the Task 2 tracer GREEN.

## Task 5: Observability

- [x] Add TUN RX batch counters without per-packet logging.
- [x] Extend the existing diagnostic formatter and parser tests.
- [x] Assert `tcp_batches == batch_relay_passes` and nonzero avoided passes in
  the modeled pressure harness.

## Task 6: Forward Bounded-Ring Harness

- [x] Add a full local forward TCP generator -> bounded TUN ring -> production
  event loop -> native D16 writer -> receiver scenario.
- [x] Use the frozen `500`-packet ring, `1200` MTU, pool `2`, `1 MiB` socket
  buffers, D16 capacities/quantum, and no self-wake override.
- [x] Prove exact bytes, zero modeled TUN drops, no queue/lifecycle leak, and
  nonzero batch amplification removal.
- [x] Preserve the existing reverse/control/starvation harnesses.

## Task 7: Exact Real Local Capacity

- [x] Run the existing exact `32 MiB` real-Quinn/D16 path.
- [x] Add or reuse an exact forward `32 MiB`, `64 KiB` chunk path through the
  batch seam if Task 6 does not already cover real Quinn.
- [x] Require strictly `>170 Mbit/s`, exact bytes, clean EOF, zero modeled TUN
  drops, endpoint conservation, no old sender/cap, and no residual ownership.
- [x] If `<=170`, stop; do not tune constants.

## Task 8: Full Local Gates

- [x] Focused batch/TCP/UDP/DNS/TUN/lifecycle suites.
- [x] Vendored Quinn-proto and Quinn unit/doc suites.
- [x] Root library and harness suites.
- [x] Explicit concurrency `64/256/1024` and UDP sweep.
- [x] `cargo check --all-targets --features harness`.
- [x] Root fmt, focused vendor fmt, shell `bash -n`, suite self-test, and diff
  checks.

## Task 9: Code Review And Learning

- [x] Review packet order/exactly-once, mixed TCP/UDP/DNS batches, SYN setup,
  D16 ownership, channel backpressure, lifecycle, no-busy-wake, endpoint
  accounting, default equivalence, and observability.
- [x] Resolve every P0/P1.
- [x] Update `.learnings/LEARNINGS.md`; update `.learnings/ERRORS.md` for any
  failure that changes future behavior.
- [x] Update HANDOFF/TODO/AGENTS and result docs.

## Task 10: Commit And VPS Acceptance

- [x] Commit one coherent reviewed batch-service change; do not stage secrets
  or temporary VPS artifacts.
- [x] Deploy an isolated source snapshot and verify hashes.
- [x] Run one target-only, forward-only P1 with the entire Gate profile frozen
  and EndpointWindowV1 enabled.
- [x] Require receiver `>170 Mbit/s`, zero TUN RX/TX drops, QUIC loss no greater
  than `16 MiB`, exact batch counters, endpoint conservation, clean pool and
  lifecycle, and healthy client/Exit sockets. Throughput, QUIC loss, batch,
  conservation, and lifecycle passed; zero-drop safety failed at `0/38`.
- [x] Classify the remaining TUN drops as architecture failure and close this
  branch without tuning.
- [x] No macOS TUN.

Result:
`docs/tech/2026-07-13-knife14h10d16-tun-rx-batch-service-vps-results.md`.
