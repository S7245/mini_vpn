# Knife15 Endpoint Socket-Rebind Recovery Implementation Plan

Date: 2026-07-16

Status: **TASKS 1-7 COMPLETE LOCALLY; TASK 8 SHENZHEN ACCEPTANCE PENDING**

Architecture:
`2026-07-16-knife15-endpoint-socket-rebind-recovery-architecture-spec.md`.

## Task 1 — Lock The Pure One-Shot Policy RED

Status: **COMPLETE**

- Add the smallest fake-time test for active TX without RX.
- Cross the RTT-derived bound and require exactly one `Rebind`.
- Continue sampling the same episode and require no second action.
- Run only this test and record the expected RED before implementation.

Commit boundary: none; RED is part of the implementation commit.

## Task 2 — Implement The Pure Policy And Safety Cases

Status: **COMPLETE**

- Add per-stable-ID TX/RX baselines.
- Add the bounded RTT-derived deadline.
- Add RX recovery/rearm and inactive-workload reset.
- Test other-connection RX, no workload, no TX, connection replacement, and
  the floor/ceiling boundaries.

Acceptance: focused policy tests pass without socket or timer I/O.

## Task 3 — Extract The Reusable UDP Socket Adapter

Status: **COMPLETE**

- Extract wildcard bind, buffer configuration, runtime wrap, and optional send
  adapter from initial endpoint construction.
- Preserve the initial endpoint behavior and log fingerprint.
- Allow the bounded adapter to reuse one accounting state across rebind
  generations.
- Add a rebind helper returning old/new local addresses.

Acceptance: existing endpoint-construction tests remain green; focused socket
adapter tests pass.

## Task 4 — Prove Live Two-Connection Rebind

Status: **COMPLETE**

- Create a local server and one EndpointWindowV1 client Endpoint.
- Establish two simultaneous QUIC connections.
- Record the client UDP port and pacing snapshot.
- Rebind to a new ephemeral port.
- Exchange data on both existing connections after rebind.
- Prove the port changed and endpoint pacing conservation/state remained on
  the same Endpoint.

Acceptance: deterministic local integration passes repeatedly. Failure rejects
the architecture before production wiring.

## Task 5 — Wire The Endpoint-Owned Monitor

Status: **COMPLETE**

- Add once-only monitor startup from the existing UDP-driver lifecycle.
- Sample all pool slots with `try_lock`; skip incomplete samples.
- Include active TCP leases and recent UDP uplink in workload activity.
- Call the pure policy and rebind adapter.
- Emit trigger/recovered/error generation logs.
- Use a weak owner reference and bounded four-Hz timer.

Acceptance: no relay hot-path await, no duplicate monitor, and no repeated
rebind in one no-RX episode.

## Task 6 — Local Gates And Review

Status: **COMPLETE**

- Focused recovery and rebind tests.
- Existing TUIC pool, reconnect, endpoint pacing, D16 close, and runner tests.
- Vendored Quinn-proto tests and doc tests required by the pacing stage.
- Root all-target tests/check, release, Clippy, Knife15/Knife14 shell suites,
  fmt, and tracked/untracked diff checks.
- Review correctness, concurrency, lifecycle, bounded resources, TUN/TCP/UDP
  regression, observability, and missing-test risk.

An unexpected regression gets causal analysis and a scoped repair plan; no
constant tuning.

Review found one P1 before acceptance: Quinn temporarily retains the previous
socket, so aggregate connection RX could falsely label old-socket traffic as
post-rebind recovery. The repair adds a vendored Endpoint generation that
advances only when an existing-connection packet arrives on the current
socket. Focused Quinn and policy tests lock the distinction. No unresolved
P0/P1 remains.

## Task 7 — Stage Memory, Commit, And Push

Status: **COMPLETE EXCEPT FINAL COMMIT/PUSH IDENTIFIERS**

- Update the M0 result, architecture/result status, `HANDOFF.md`, `TODO.md`,
  `.learnings/LEARNINGS.md`, `.learnings/ERRORS.md`, and stable `AGENTS.md`
  guidance where appropriate.
- Commit coherent code/tests, then coherent docs if the diff size warrants two
  commits.
- Push the current branch under the standing authorization.

## Task 8 — Shenzhen Acceptance

Status: **PENDING USER-RUN MACOS TUN REPLAY**

- Synchronize the pushed source and build release.
- Take a fresh physical baseline and fresh 300-second direct discriminator.
- Run bounded start/smoke/M0 only after those gates pass.
- Preserve any failure with status/snapshot/stop.

M0 must have no receiver-zero interval, at most one rebind per no-RX episode,
and positive post-rebind RX if a rebind occurs. A failing discriminator rejects
the architecture; it does not authorize liveness or throughput tuning.
