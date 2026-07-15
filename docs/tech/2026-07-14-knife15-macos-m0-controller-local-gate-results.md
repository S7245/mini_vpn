# Knife15 macOS M0 Controller Local Gate Results

Date: 2026-07-14
Status: **RECEIVER-EVIDENCE SEMANTICS REPAIRED; FRESH M0 PENDING**

## Goal And Boundary

Convert the qualified target-only macOS HITL runner into a deterministic
formal M0 driver. The controller must create mixed TCP/UDP/DNS traffic for two
hours, prove an idle-to-active transition and final ownership drain, preserve
exact evidence, and stop workload generation if the TUN path becomes unsafe.

This stage changes the test controller and evidence parser only. H10d16,
EndpointWindowV1, MTU1200, pool, QUIC windows, chunks, Cubic, GSO, queue/FIFO/
batch bounds, driver bound, and self-wake remain frozen.

## Selected Workload

A fresh same-target direct TCP baseline is mandatory. The controller validates
the target, TCP protocol, forward/reverse direction, positive receiver result,
and every nonzero interval, then derives:

- sustained forward TCP: `50%` of direct forward receiver rate;
- sustained reverse TCP and reverse UDP: `50%` of direct reverse receiver
  rate;
- short forward/reverse TCP bursts: `80%` of the corresponding direct rate;
- UDP application payload: fixed `1160B` inside MTU1200.

The formal `7,200s` plan is:

- `6,780s` active traffic split equally around idle;
- `300s` idle drain;
- `120s` final drain;
- eight complete `840s` cycles plus a `30s` capped forward TCP tail on each
  side of idle.

Each complete cycle contains `300s` forward TCP, `300s` reverse TCP, `180s`
reverse UDP, six alternating `10s` short TCP connections, and fake-IP DNS.
Across M0 this gives eight complete mixed cycles, 48 short connections, eight
DNS checks, one explicit idle/resume proof, and one final drain proof.

## Fail-Closed And Evidence Contracts

- Formal `m0` rejects duration overrides; compressed schedules exist only in
  self-tests.
- Every iperf JSON must have the expected protocol, positive sender/receiver
  byte evidence, and structured evidence from both endpoints. Forward phases
  validate Target receiver intervals; reverse phases validate local receiver
  intervals. Receiver intervals must all be positive. Sender intervals remain
  numeric diagnostics: a zero is counted and requires final review but does
  not erase continuous receiver delivery. UDP also requires a numeric loss
  field. A command exit code of zero alone is insufficient.
- Every DNS epoch must return an address in fake-IP `198.18.0.0/15`.
- The identity-verified controller tracks its current traffic, idle, or final-
  drain child. It checks mini_vpn liveness, target-to-utun routing, and Exit
  route non-recursion every two seconds throughout the formal timeline.
- Signal, health, command, or evidence failure terminates the active workload,
  records `failed`/`interrupted`, and leaves the TUN running for
  `status`/`snapshot`/`stop` evidence.
- `stop` terminates an identity-verified workload before mini_vpn and route
  cleanup. Bundling refuses mutable workload state.
- The workload profile stores both baseline hashes, exact derived rates,
  topology, UDP shape, and schedule.
- `summary.md` records M0 status and event counts, RSS/FD/thread first/last/max
  envelopes, utun packet/byte deltas, interface errors, endpoint conservation
  maximum, final live/outstanding ownership, maximum TCP sender/receiver byte
  gap, maximum UDP loss percentage, result-file/DNS-file correspondence, and
  the unique idle/resume/final-drain timeline.
- Any mini_vpn log compaction fails the running workload at its next health
  check and makes the evidence summary `REVIEW`, because discarded early log
  bytes would otherwise hide historical conservation or failure samples.

## TDD Evidence

The macOS/BSD shell fixture covers:

- strict baseline JSON parsing and wrong-target/direction rejection;
- exact `50%` sustained and `80%` burst derivation;
- fixed `1160B` UDP command shape;
- persistent forward/reverse TCP, reverse UDP, short-flow, and DNS commands;
- idle, resume, final drain, and completion markers;
- zero-traffic and missing-byte iperf JSON rejection despite exit status zero;
- success/failed workload states and stop-before-idle behavior;
- formal `7,200s` configuration rejection for shortened runs;
- resource, utun, endpoint, M0 result/DNS correspondence, timeline integrity,
  log-compaction downgrade, and summary parsing on BSD-compatible tools.

Final local gates:

- root Rust: `632 passed`, `3 ignored`; binary tests `2 passed`;
- release build: PASS (only the established vendored smoltcp warnings);
- Knife14 low-RTT, US-client-suite, and sing-box-control self-tests: PASS;
- Knife15 internal and external macOS runner self-tests: PASS;
- root format, shell syntax, and diff checks: PASS;
- local iperf3 `3.21` TCP/UDP schema probe and the fresh HK baseline parser:
  PASS.

The final code review found no unresolved P0/P1. Review repairs added strict
result/DNS/timeline correspondence, final zero ownership, tracked pause/drain
children, TERM-then-KILL cleanup, identity-mismatch refusal, and runtime
failure on lossy log compaction before the formal Mac run.

## Decision

The controller and evidence seam are locally sufficient for one formal
user-executed HK or Shenzhen M0. A local test cannot establish hours-long
resource plateaus, real utun behavior, path attribution, or stop/re-create
rearm. Those remain the next acceptance evidence and must follow the runbook:
fresh direct baseline, start/smoke, formal M0, status/stop bundle, then fresh
start/smoke/stop rearm bundle.
