# Knife14h10d16 Endpoint Pacing Service Implementation Plan

Date: 2026-07-13

Status: **IMPLEMENTED; full local gate and code review PASS, VPS acceptance pending.**

Source of truth:
`docs/tech/2026-07-13-knife14h10d16-endpoint-pacing-service-architecture-spec.md`.

Goal: implement the smallest reversible endpoint-shared Quinn pre-accounting
service that proves the specified byte-window, socket-outstanding, control,
idle-borrowing, fairness, lifecycle, and `>170 Mbit/s` local contracts without
changing frozen product parameters.

## File Map

- Vendor exact `quinn 0.11.11` under `third_party/` with crates.io checksum
  `0c1a41e437b6bbd489372cd4971de128e85c855f56c57f283d20ff016cf7c0a8`.
- Extend the existing pinned `third_party/quinn-proto-0.11.16` patch; keep its
  accepted checksum and patch manifest.
- `quinn-proto/src/config/mod.rs`: optional endpoint service Parameter Object.
- `quinn-proto/src/endpoint.rs`: create one fresh service per endpoint and
  pass stable connection handles.
- `quinn-proto/src/connection/endpoint_pacing.rs`: pure service Module,
  reservation, DRR, control reserve, deadline/waker, and snapshots.
- `quinn-proto/src/connection/mod.rs`: pre-build reservation, per-datagram
  settle, migration/cancel/detach, pending socket batch.
- `quinn-proto/src/connection/packet_builder.rs`: no policy; only the minimal
  actual-byte settlement handoff if required.
- `quinn/src/connection.rs`: current waker Adapter and socket outcome callback.
- `src/quic.rs`: `QuinnDefault`/`EndpointWindowV1` composition and validation.
- `src/tuic.rs`: startup/pool attribution and aggregate snapshot formatting.
- Runner changes only after product composition tests are green.
- Current specs, result docs, TODO/HANDOFF, and `.learnings/` after review.

Do not modify D16, TCP queue, TUN, DNS, UDP relay, REALITY, failover, MTU,
windows, pool, chunk, Cubic, GSO, or self-wake behavior.

## Task 1: Freeze And Preserve The Diagnostic Replay

- [x] Run the existing configured-cap debt test green.
- [x] Add a one-millisecond acceptance assertion and observe RED at exactly
  `259 > 64` datagrams.
- [x] Convert it to the default-enabled characterization
  `configured_cap_refills_multiple_buckets_inside_one_millisecond`.
- [x] Run the entire pinned proto suite before service code: `276/276` unit
  tests and `3/3` doc tests passed.

Stop if the full suite is not green for reasons attributable to the replay.

## Task 2: Vendor Quinn And Prove Default Equivalence

- [x] Copy exact crates.io `quinn 0.11.11`; verify checksum/source manifest.
- [x] Add `[patch.crates-io]` without changing versions.
- [x] RED a default endpoint construction/driver trace comparison.
- [x] Add only an optional callback/waker Interface; `None` must execute the
  exact old `poll_transmit -> try_send` path.
- [x] Run upstream Quinn and proto tests.

The original preparation-stage commit restriction was superseded by the
user's explicit implementation and commit authorization.

## Task 3: Pure Endpoint Byte-Window Module

- [x] RED any-window property tests for `B + R*dt`, including 1ms/10ms exact
  bounds, fake time rollback, saturation, rounding, and overflow.
- [x] Introduce `EndpointPacingServiceConfig` and pure service state.
- [x] Add RAII `DatagramReservation`; planned bytes are charged before build,
  short build refunds exactly once, and drop refunds exactly once.
- [x] Keep the mutex outside packet construction, socket I/O, wakes, and await.
- [x] GREEN the pure property and conservation tests.

## Task 4: Socket Outstanding Contract

- [x] RED: reserve/build one full burst, advance fake time during repeated
  `WouldBlock`, accept the buffered batch, and prove a second burst cannot be
  emitted immediately.
- [x] Track `outstanding_bytes` through the existing one-buffered-Transmit
  driver lifecycle.
- [x] Add success, transient `WouldBlock`, fatal error, driver drop, short GSO
  batch, and two-connection blocked-batch tests.
- [x] Assert `tokens + live reservations + outstanding <= burst` after every
  transition.

Do not implement delay by returning synthetic socket `WouldBlock`.

## Task 5: Endpoint Construction And Connection Lifecycle

- [x] RED: two connections from one proto endpoint do not share a service.
- [x] GREEN: one endpoint creates one service; all its connections receive the
  same instance, while a second endpoint gets a fresh instance.
- [x] Key waiters by `ConnectionHandle + path_generation`.
- [x] Test attach, no-send cancel, connection drain, endpoint drop, migration,
  stale generation, and exact outstanding abandonment.

## Task 6: Idle Borrowing And Two-Connection Fairness

- [x] RED single-active capacity and two-active lead-bound tests.
- [x] Add byte DRR with `20,480B` quantum and one bounded waiter per key.
- [x] Prove one busy connection uses full `30.72 MB/s` when its peer is idle.
- [x] Prove two continuously backlogged connections differ by at most one
  quantum plus one maximum datagram after both register.
- [x] Prove idle/cancelled head removal wakes and lends service to the peer.

## Task 7: Control Reserve And Protocol Semantics

- [x] RED classification tests for handshake, pure ACK, close, loss probe,
  keepalive, path validation, MTU probe, bulk STREAM/DATAGRAM, and mixed data.
- [x] Add the `10,240B` reserve within the aggregate burst and idle borrowing.
- [x] Prove control priority never exceeds the aggregate window and cannot
  starve bulk after its bounded reserve quantum.
- [x] Run all ACK, handshake, loss, PTO, close, MTU, migration, and GSO tests.
- [x] Prove stateless responses are sent only with immediate service or dropped
  with a counter; formal client tests require zero such responses.

## Task 8: Deadline And Waker Composition

- [x] RED two-driver fake-runtime tests: endpoint denial must park, a turn
  handoff must wake the peer, and path+endpoint deadlines choose the later.
- [x] Pass the current driver waker through the thin Quinn Adapter.
- [x] Deduplicate/replace wakers, invoke outside the mutex, and remove on
  cancel/migration/detach.
- [x] Prove no immediate self-wake loop and no timer-only millisecond cooldown.
- [x] Test a missed timer plus peer wake without service loss or unfairness.

## Task 9: Connection Poll/Packet Integration

- [x] RED: no connection-generated UDP datagram can reach
  `finish_and_track` without a live service reservation.
- [x] Reserve once per UDP datagram before builder/accounting; settle actual
  bytes once per datagram.
- [x] Sum per-datagram outstanding bytes for one returned GSO `Transmit`.
- [x] Keep Quinn's per-path Pacer accounting and congestion/loss semantics
  unchanged.
- [x] Test early returns, GSO truncation, coalesced packets, loss probes, path
  challenge, MTU probe, close, ACK-only, and empty poll.

## Task 10: mini_vpn Branch By Abstraction

- [x] Add `EndpointWindowV1` default-off and keep `QuinnDefault` production.
- [x] Reject composition with `PacerCap64` and bounded UDP send service.
- [x] Apply endpoint config once in `client_endpoint_with_udp_send_service`.
- [x] Log one canonical startup fingerprint and both stable pool connection
  service counters.
- [x] Add parser, fallback, mutual exclusion, default Debug, and endpoint
  isolation tests.
- [x] Only then add runner validation/report/launch/startup verification.

## Task 11: Deterministic And Real Local Capacity Gate

- [x] Run pure fake-time 1ms/10ms, two-connection fairness, control, cleanup,
  and no-busy-wake suites first.
- [x] Run exact GSO-enabled `32 MiB`, fixed `64 KiB` chunk, QuinnDefault local
  Pacer plus EndpointWindowV1.
- [x] Require exact bytes/pattern, clean EOF, application sender strictly
  `>170 Mbit/s`, service delay activity, no old sender/cap, zero reservation or
  outstanding leak, and observed bound attribution.
- [x] If throughput is `<=170`, stop and review. Do not change constants or
  frozen parameters.

## Task 12: Full Local Gates And Code Review

- [x] Vendored proto and Quinn upstream suites/doc tests.
- [x] Full library and harness suites.
- [x] Explicit concurrency `64/256/1024`.
- [x] UDP payload sweep and fake-IP/TUN lifecycle local tests.
- [x] Default/harness check, root fmt, focused vendor format/diff manifests,
  shell syntax/self-tests, and `git diff --check`.
- [x] Review correctness, performance, lifecycle, boundedness, control
  liveness, fairness, D16/TCP/UDP/TUN regression, secrets, and missing tests.
- [x] Fix every P0/P1 and rerun affected gates.
- [x] Update LEARNINGS/ERRORS and current project memory.

The original preparation-stage restriction was superseded by the user's
explicit authorization for implementation, commit, and VPS acceptance.
macOS TUN remains prohibited on the current host.

## Repair-Failure Rule

An expected RED is part of TDD. Unexpected failures are analyzed against the
spec before repair. The user subsequently authorized safe, in-scope repairs
without repeated confirmation; architecture failure, frozen-parameter change,
or new authority still requires an explicit stop.

## Local Completion Evidence

- Exact GSO-enabled `32 MiB` / `64 KiB` upload: `240.466 Mbit/s`, exact bytes,
  zero pattern errors, clean EOF, endpoint delay activity, no old bounded
  sender or cap64 path, and zero live/outstanding leak at endpoint idle.
- Endpoint snapshot: fixed `30,720,000B/s`, `61,440B` burst, `10,240B`
  control reserve, `20,480B` quantum; `34,322,409B` / `23,641` connection
  datagrams accepted by the socket, zero abandoned bytes, and final
  `available=61,440B`, `live=0`, `outstanding=0`, `records=0`.
- Vendored `quinn-proto`: `309/309` unit and `3/3` doc tests. Vendored Quinn:
  `29/29` nonignored unit tests (`3` expected ignored) and `1/1` doc test.
- mini_vpn library: `622/622` nonignored tests (`3` expected ignored).
  Harness checks, explicit `64/256/1024` concurrency, and UDP payload sweep
  passed. Root and focused-vendor format, shell syntax/self-test, all-target
  harness check, and diff checks passed without warnings.
- Final code review fixed one fail-open migration edge: an impossible target
  generation collision now retains the old paced adapter instead of
  detaching and sending unpaced. The focused lifecycle test and all affected
  suites pass. No unresolved P0/P1 remains.
