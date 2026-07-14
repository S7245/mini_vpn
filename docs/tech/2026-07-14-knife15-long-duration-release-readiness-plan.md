# Knife15 Long-Duration Release-Readiness Plan

Date: 2026-07-14
Status: **Short HK HITL qualification PASS; 2-hour M0 not yet executed**

## Stage Goal

Prove that the accepted Knife14 H10d16 data plane remains bounded, recoverable,
and useful under hours-long mixed TCP/UDP traffic and real client lifecycle
events. Knife15 is a release-readiness stage, not another peak-throughput or
pacing-tuning stage.

Knife15 has two complementary evidence lanes:

1. the existing capable Linux/VPS topology remains the architecture and peak-
   throughput reference;
2. macOS provides the real-client, cross-region, resource-stability, utun, and
   recovery lane: the HK development Mac may run the first user-controlled
   target-only qualification, while the dedicated Shenzhen machine remains the
   preferred long-duration host.

The HK/Shenzhen-to-US paths are expected to be below `200 Mbit/s`. They must
not be used to reject the accepted H10d16 capacity architecture on an absolute
Mbps threshold. Their value is duration, client realism, network variability,
and macOS-specific evidence.

## Accepted Knife14 Baseline

- Exact source `5f9da90` passed the capable `.111:8443` reverse P8 at
  `188 Mbit/s` receiver with `60/60` nonzero intervals.
- Reverse/live-streaming UDP passed at `90 Mbit/s` with a valid `1160B`
  payload and zero application/internal loss in that direction.
- Endpoint accounting obeyed
  `available + live + outstanding <= 61,440B`.
- TUN drops, ingress-pump waits/errors, terminal pending/reap, and formal QUIC
  loss/congestion/blocking were zero in the accepted reverse P8.
- The accepted chain is
  `c55737e -> d934f12 -> 20a0f8c -> e201403 -> 5f9da90`.

## Scope

Knife15 owns:

- 2-hour, 8-hour, and 24-hour staged soak gates;
- mixed persistent TCP, short-lived TCP, DNS, and video-like UDP workloads;
- process and TUN resource trends on macOS;
- idle-to-active recovery and clean stop/rearm;
- one-variable-at-a-time Wi-Fi, sleep/wake, address/path-change, client restart,
  and authorized Exit-restart experiments;
- secret-free manifests, event timelines, evidence bundles, and automatic
  summaries;
- discriminators that separate client leaks/stalls from cross-region path
  congestion.

## Non-Goals And Frozen Decisions

- Do not use either macOS lane as a `170/200 Mbit/s` gate.
- Do not reopen bounded sender, PacerCap64, GSO-only, D3 self-wake, or parameter
  tuning.
- Keep the accepted H10d16, EndpointWindowV1, MTU1200, `1160B` UDP payload,
  pool, QUIC windows, chunk, Cubic, GSO default, and queue/FIFO/batch bounds.
- Do not treat CLI-created utun as final Network Extension, App sandbox,
  background-execution, battery, iOS, Android, or Windows acceptance.
- The HK development Mac may run macOS TUN only through the reviewed HITL
  target-only shell runner, with the user explicitly executing every `sudo`
  command. Agent-started TUN and ad-hoc route mutation remain prohibited.
- Do not reuse the historical full-route macOS Knife3.5/8/9 scripts unchanged.
  They predate H10d16 and do not provide the required target-only, provenance,
  resource, conservation, and fail-closed evidence.

## Test Topology And Attribution

### Linux/VPS reference lane

The capable Shoes/Quinn `.111` topology and the accepted Linux harness remain
the source of truth for peak capacity and architecture regressions. A future
Linux Knife15 soak must preserve the same provenance and capable-peer checks
before interpreting an Mbps result.

### HK/Shenzhen macOS lane

Start with target-only routing. The TUIC Exit address must remain outside the
utun route to prevent recursion. The HK Mac is limited to user-executed HITL
qualification unless a later plan expands it. Only after target-only lifecycle
and cleanup pass may the dedicated Shenzhen machine run a controlled
full-tunnel real-application soak.

For each macOS run, first measure direct/control bandwidth `B` in both
directions. Use approximately:

- `40-60% of B` for sustained mixed traffic;
- `75-85% of B` for short bursts;
- `1160B` application payload for MTU1200 UDP.

The exact offered rate belongs in the run manifest. If direct/control degrades
in the same window, attribute the event to the path unless internal counters
contradict that conclusion.

## macOS Runner Safety Contract

The H10d16-aware HITL runner and shell fixtures are now implemented at
`scripts/knife15-macos-soak.sh` and
`scripts/knife15-macos-soak-self-test.sh`. Before any long run, they must keep
passing tests for:

- exact source commit, binary SHA-256, runner SHA-256, and sanitized profile;
- discovery of the newly created utun and proof that the target enters it;
- proof that the TUIC Exit does not enter it;
- saved route/DNS state and idempotent restoration on success, failure, signal,
  timeout, and machine restart where feasible;
- a watchdog and explicit emergency-stop command;
- bounded logs, rotation, disk-space guard, and evidence finalization;
- no credentials, UUID, password, private key, or sensitive environment values
  in commands, logs, manifests, or repository files;
- macOS/BSD-compatible parsing and shell self-tests;
- no dependence on Linux `ip`, `/sys/class/net/*/tx_dropped`, or Linux-only
  qdisc semantics.

Use macOS interface counters plus mini_vpn's internal TUN counters. An unknown
Linux-only `tx_dropped` field is not a pass; it must be replaced by explicit
macOS evidence and internal pump/read/write/flush accounting.

## Evidence Bundle

Sample long-run metrics every 30 seconds unless a focused short discriminator
requires a smaller interval. A 24-hour run then produces about `2,880`
periodic samples per metric family.

Each sanitized bundle must include:

- `manifest.txt`: source/binary/runner hashes, macOS/hardware, topology, MTU,
  timestamps, and non-secret configuration;
- `mini_vpn.log`: `data-plane`, loop-profiler, QUIC, endpoint-pacing, TUN,
  relay, and close-tail lines;
- `process.csv`: RSS, CPU, file descriptors, threads, and process state;
- `interface.csv`: utun packets, bytes, and available error/drop counters;
- `network.csv`: direct/control RTT, loss, and throughput samples;
- `events.tsv`: workload, idle, sleep, network, restart, and recovery markers;
- `summary.md`: deltas, high-water marks, slopes, invariant results, fault
  recovery times, and attribution;
- bundle checksum and a secret scan result.

## Workload Matrix

### M0 — 2-hour target-only qualification

- persistent TCP in both useful directions without saturating the path;
- video-like UDP plus periodic DNS;
- short TCP connection waves;
- at least one idle window followed by resumed traffic;
- stop, route/DNS restoration, fresh create, and rearm.

M0 proves the runner and evidence loop. It is not a release soak by itself.

### M1 — 8-hour mixed soak

- repeat the M0 mix with quiet, steady, and burst epochs;
- include high connection churn without requiring high aggregate Mbps;
- verify resource plateaus and post-idle cleanup;
- keep event markers sufficient to correlate every change with the client,
  workload, or path.

### M2 — 24-hour real-client soak

- controlled full-tunnel browsing, video/live-streaming, downloads, DNS, and
  idle periods on the dedicated machine;
- bounded log/disk behavior;
- no unexplained process death, route leak, DNS leak, relay accumulation, or
  resource slope.

### M3 — recovery matrix

Run each event in an independent, explicitly marked window:

- Wi-Fi disconnect/reconnect;
- sleep/wake;
- Wi-Fi-to-hotspot or equivalent path/address change;
- mini_vpn stop/restart and utun re-create;
- authorized Exit restart.

Do not combine failure injections until every single-variable outcome and
steady-state recovery is understood.

## Acceptance Invariants

- The process remains alive unless the test intentionally stops it.
- `available + live + outstanding <= 61,440B` for every endpoint sample.
- After a completed workload and sufficient idle drain, live reservations and
  outstanding socket bytes return to zero; connection/relay/fake-IP state does
  not grow without corresponding live work.
- RSS, file descriptors, threads, relays, and queues reach a workload-specific
  plateau rather than an unbounded positive slope. M0 establishes the warm
  baseline envelope before M1/M2 receive hard thresholds.
- No unexplained TUN read/write/flush errors, pump closure, or sustained full
  waits occur. Any available macOS interface errors are correlated with the
  internal counters.
- Clean traffic windows have no terminal pending/reap or stranded eligible
  DrainOnly flow. Fault windows may contain classified cancellation/close
  events, but all ownership must close exactly.
- DNS forge/drop and fake-IP state agree with the generated workload.
- Idle, network, and process recovery times are measured and bounded by SLOs
  selected after M0; recovery is not inferred from process liveness alone.
- Stop restores route and DNS state and leaves no mini_vpn-owned utun/process.

## Failure Discriminators

- Rising RSS/FD/relay/fake-IP state after the workload returns to idle points
  to client lifecycle or ownership leakage.
- Nonzero endpoint live/outstanding state after idle points to reservation,
  socket-completion, cancellation, or migration cleanup.
- Rising endpoint delay/service-gap with low path loss and high loop activity
  points to local scheduling or runtime starvation.
- Ingress pump high-water/full waits/read errors point to macOS utun ingress
  service or consumer cadence.
- TUN flush/write failures or interface errors point to macOS egress behavior.
- Terminal pending, late payload, or stranded DrainOnly points to relay
  close-tail/recovery semantics.
- QUIC RTT/loss degradation that matches direct/control degradation, without
  internal pressure or ownership failure, is a path event rather than a
  constant-tuning request.

## Task Order

1. Freeze Knife15 SLO vocabulary, manifest schema, workload epochs, and
   discriminator rules.
2. Add shell/TDD fixtures for a new fail-closed macOS target-only runner.
3. Implement resource/interface/event collectors and bounded log rotation.
4. Add a deterministic summary parser for slopes, deltas, conservation, TUN,
   QUIC, endpoint, relay, and cleanup evidence.
5. Run local syntax, parser self-tests, Rust regressions, fmt, diff, and review;
   no real TUN is required for these gates.
6. Run the short user-controlled target-only qualification on the HK or
   Shenzhen Mac, then run M0 on the dedicated Shenzhen Mac and define measured
   warm envelopes and recovery SLOs from valid evidence.
7. Run M1 only after M0 and cleanup pass.
8. Run M2 only after M1 shows bounded resources and exact ownership.
9. Run M3 one event at a time, with rollback and cleanup checks after each.
10. Run the complementary capable Linux/VPS soak and compare invariants rather
    than requiring identical cross-region Mbps.
11. Complete code review, learning/error memory, operational runbook, and the
    Knife15 release-readiness decision.

## Stop And Repair Rules

- A failed direct/control or incapable peer invalidates Mbps attribution; it
  does not authorize product tuning.
- A conservation, ownership, lifecycle, TUN, or resource-bound failure is a
  product discriminator. Preserve its artifact, rank falsifiable hypotheses,
  add the smallest correct replay/test seam, and repair under TDD.
- Do not change frozen H10d16 constants to make a Shenzhen-to-US throughput
  number look better.
- A 2-hour failure blocks the 8-hour run; an 8-hour failure blocks the 24-hour
  run. Longer repetition is not a substitute for diagnosis.

## Readiness Score

At plan acceptance, long-duration release readiness is `6.5/10`: peak Linux
capacity, core invariants, and short formal regressions are strong, but the new
macOS runner, resource trend evidence, recovery SLOs, and staged 2/8/24-hour
results do not yet exist. Reaching `10/10` requires Tasks 1-11 to pass with no
unresolved P0/P1 and with complete fail-closed cleanup evidence.

## 2026-07-14 Execution Update

The first HK user-controlled target-only qualification passed on exact source
`2a85fd4`: TCP forward/reverse and fake-IP DNS completed, endpoint conservation
held at or below `61,440B`, macOS interface errors were zero, pump high-water
was `317/500` with zero waits/errors, and process/utun/routes cleaned up. This
qualifies the runner, not the 2-hour M0. Results:
`docs/tech/2026-07-14-knife15-macos-hitl-short-qualification-results.md`.

HK is now permitted to run target-only M0 through the same user-executed HITL
boundary; Shenzhen remains preferred for M1/M2 and recovery. Before M0,
complete the repaired summary/log-density gates and the mixed-workload plus
idle-drain controller. No frozen data-plane constant changed.
