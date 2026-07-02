# mini_vpn Project Memory

This file is project-level memory for Codex/agent sessions. Read it before making
plans, code changes, test requests, or architecture decisions in this repository.

## Product Goal

mini_vpn is the data-plane core of a future cross-platform VPN product. The
future App is the control plane and communicates with mini_vpn through APIs. The
target App platforms are iOS, Android, macOS, and Windows.

The user-facing requirements come from `Rules.md`:

- Local devices must connect to remote servers through VPN for TCP traffic.
- Local devices must use VPN for video/live-streaming UDP traffic.
- Local devices must support high-concurrency VPN connections.
- The hard target is high concurrency, high throughput, long duration, and
  stable quality.

## Technical Direction

- Support transparent TCP and UDP data-plane behavior, not only a narrow demo.
- Support arbitrary IP/domain/port targets. Do not hard-code remote targets.
- Keep TUN, fake-IP DNS, dynamic listener ports, relay lifecycle, and TUIC/QUIC
  behavior consistent as one system.
- Current mature transport direction is TUIC v5 over QUIC, interoperable with
  mature servers such as sing-box/Mihomo.
- TCP should ride QUIC streams/TUIC Connect. UDP should ride QUIC
  datagrams/TUIC Packet where available.
- Different VPS providers, RTTs, MTUs, congestion-control choices, and server
  configs must be handled through explicit config, preflight checks,
  observability, and bounded backpressure rather than one-off assumptions.

## Engineering Bar

- System stability is more important than pretty code.
- Measure before guessing. Prefer logs, counters, harnesses, and true endpoint
  acceptance over intuition.
- Preserve lifecycle correctness before optimizing throughput.
- A single failed relay/session must not tear down unrelated sessions.
- Avoid hot-path panics and unchecked `unwrap`/`expect`.
- Avoid duplicated handshake/protocol logic when a shared abstraction already
  exists.
- Bound queues, buffers, pending downlink data, and backpressure paths.
- Keep regressions visible: TCP, UDP, TUN, fake-IP DNS, TUIC, and scripts should
  not silently break each other.

## Work Rhythm

For meaningful changes, follow this sequence:

1. Grounding: read the relevant code, docs, specs, plans, and latest logs.
2. Grill/design tree: list plausible failure branches and reject weak guesses.
3. Spec: define the exact stage goal, non-goals, invariants, and acceptance.
4. Plan: split the work into small tasks.
5. TDD: add or update focused tests/harness checks before or alongside fixes.
6. Commit per coherent task when requested by the workflow.
7. Stage code-review: review for bugs, regressions, missing tests, and
   operational risk before asking for another VPS run.

Small related issues found during review should be fixed together before a
concentrated integration test, instead of running a full VPS suite after every
tiny edit.

## Test Strategy

- Use local deterministic tests and harnesses for code-logic bugs.
- Use VPS integration suites for real timing, pressure, TUIC/QUIC behavior,
  sing-box interoperability, MTU/RTT/path effects, and service-state issues.
- Before asking the user to run a VPS suite, include a concrete one-shot test
  checklist and preflight service checks for the relevant VPS hosts, especially
  `.33` and `.77` when those hosts are part of the test.
- If a VPS service looks unhealthy, tell the user to inspect or restart it
  before running the expensive test.

## Current Acceptance Bias

When investigating performance or reliability, prefer acceptance criteria that
show the system survives realistic pressure:

- high-concurrency TCP without event-loop stalls;
- UDP/live-streaming quality with low loss over sustained duration;
- bounded pending/downlink backlog and observable backpressure;
- no unexpected relay reaping while useful buffered data remains;
- clear metrics showing whether a bottleneck is local loop CPU, QUIC path,
  server config, VPS network, or application logic.
