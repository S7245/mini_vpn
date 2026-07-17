# Knife15 Long-Duration Release-Readiness Plan

Date: 2026-07-14
Status: **REAL RECEIVER INTERRUPTION REPEATED; 300S PHYSICAL DIRECT
CONTINUITY GATE IMPLEMENTED; M0 AND M1 BLOCKED**

## Latest M0 Discriminator

Exact source `239acba` completed two mixed M0 cycles, then cycle 3 forward
contained three complete Target receiver zero-throughput seconds while still
delivering exact `313,786,368B / 8.363 Mbit/s` over the full 300 seconds.
User operation was correct. Endpoint conservation, TUN/pump service, relay
lifecycle, resource bounds, physical-interface errors, and coarse Exit/gateway
controls remained clean. The active bulk QUIC connection accumulated loss and
congestion with an 8.9-second writer wait while its sibling remained stable.

The old 20-second baseline sized the load but did not validate forward Target
receiver intervals or prove direct 300-second continuity. The local runner now
requires a direction-aware baseline plus a non-root 300-second direct forward
gate at the exact 50% M0 rate. Formal M0 accepts only matching source/runner/
binary/baseline/result evidence completed within 15 minutes. Direct failure
blocks TUN; direct PASS followed by another clean-internal M0 interruption
selects an architecture failure and blocks another identical run. Result:
`docs/tech/2026-07-15-knife15-macos-m0-direct-continuity-discriminator-results.md`.

Previous discriminator:

Exact source `b0fcb76` repeated a real Target receiver interruption in cycle 1
forward: five complete zero-byte seconds plus one short tail, with
`208,142,336B / 5.547 Mbit/s` delivered over the full `300s` command from a
`14.425 Mbit/s` offer. Baseline/provenance and user operation were correct.
Internal ownership, TUN/pump, lifecycle, and resource signals remained clean;
bulk QUIC loss/congestion, cwnd contraction, and a `10.05s` writer wait were
the positive discriminator.

Exit/gateway controls were complete and broadly stable around the failure, but
the physical-interface portion had a false PASS. A BSD physical Link row
included an Address field absent from the utun fixture, shifting every counter
while preserving 27 CSV columns. Commit `524139b` parses both shapes and fails
closed unless all physical MTU/counter semantics are numeric. Local shell,
Rust/release, Knife14, fmt/diff, and review gates pass. The independent rearm
lifecycle passed but has the same old physical-row defect. Run one fresh
repaired-source start/smoke/stop validation before another formal Shenzhen M0.
Result:
`docs/tech/2026-07-15-knife15-macos-m0-physical-counter-observer-repair-results.md`.

Earlier discriminator:

The 2026-07-15 M0 bundle from exact source `b5c3963` completed two mixed
cycles, then cycle 3 forward contained four real Target receiver zero-byte
seconds. Endpoint ownership, TUN/pump service, relay lifecycle, and resources
remained clean, while QUIC loss/congestion rose and cwnd contracted to
`25,174B` before recovery. This is a genuine continuity failure, not user
operation or the earlier sender-evidence defect. The old runner lacked the
required same-window physical-path controls, so external-path versus
tunnel-only attribution remains unproven.

The runner now samples direct Exit and physical-gateway RTT/loss plus physical
interface counters/rates every 30 seconds, checks control freshness during M0,
and fails closed on missing per-process network evidence. The independent
stop/re-create/smoke/stop rearm passed. No data-plane or workload constant
changed. Fresh M0 must use the dedicated Shenzhen Mac before M1. Result:
`docs/tech/2026-07-15-knife15-macos-m0-network-control-discriminator-results.md`.

Earlier discriminator:

The 2026-07-15 fresh M0 crossed 90 seconds and completed its first 300-second
forward transfer at `14.420 Mbit/s` receiver, but the old runner aborted after
one client sender zero interval even though the Target receiver remained
positive in all `301` interval/tail rows. Direction-aware structured receiver
evidence is now the SLI; sender zeros remain review diagnostics. Result:
`docs/tech/2026-07-15-knife15-macos-m0-receiver-evidence-results.md`.

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
- one-shot immutable bundle publication: a valid archive/checksum pair cannot
  be overwritten by repeated stop, snapshot, bundle, or event actions;
- a positive direct Target transaction immediately before any TUN rearm, so a
  server still occupied by a failed test cannot contaminate the next run;
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

Current long-duration release readiness is `8.5/10`: peak Linux capacity, core
invariants, the macOS runner, exact receiver semantics, same-window controls,
the valid two-hour HK M0 main run, and its independent fresh rearm are proven.
M0 is complete. M1/M2/M3 and the complementary long Linux soak remain pending;
define explicit M1 resource/recovery SLOs from the accepted M0 envelope before
starting the eight-hour run.
Reaching `10/10` requires Tasks 1-11 to pass with no unresolved P0/P1 and with
complete fail-closed cleanup evidence.

## 2026-07-17 Execution Update

Independent rearm bundle `...063948.tar.gz` (SHA-256 `f4e0f649...`) created a
fresh target-only utun, kept Exit physical, passed both TCP directions and
fake-IP DNS, and then proved process/TUN/route cleanup. It used the exact
main-run binary and runner hashes; its newer source commit contained only the
main-run evidence documents. Endpoint ownership ended with zero live and
outstanding bytes, and review found no unresolved P0/P1. M0 is complete. Next
define M1 SLOs and runner TDD before an eight-hour execution. Result:
`docs/tech/2026-07-17-knife15-hk-m0-rearm-acceptance-results.md`.

Exact source `8bc7b7c` completed the first accepted two-hour M0 main run on HK:
eight full mixed cycles plus both forward bookends, `74` exact result files,
`8/8` DNS checks, one idle/resume sequence, final drain, zero phase/health
failures, and zero direction-aware receiver-zero intervals. Endpoint
conservation held at or below `61,440B` and final live/outstanding ownership
was zero. Resource, interface, log, secret-scan, stop, and route cleanup
evidence passed; phase-aware close-tail review found no unresolved P0/P1.

At that point the independent fresh `start -> smoke -> stop` rearm remained
mandatory before M1. It did not require another baseline, direct discriminator,
or two-hour workload. Main-run result:
`docs/tech/2026-07-17-knife15-hk-m0-main-run-results.md`.

## 2026-07-14 Execution Update

The first HK user-controlled target-only qualification passed on exact source
`2a85fd4`: TCP forward/reverse and fake-IP DNS completed, endpoint conservation
held at or below `61,440B`, macOS interface errors were zero, pump high-water
was `317/500` with zero waits/errors, and process/utun/routes cleaned up. This
qualifies the runner, not the 2-hour M0. Results:
`docs/tech/2026-07-14-knife15-macos-hitl-short-qualification-results.md`.

The formal M0 controller is now locally complete. It derives sustained rates
at `50%` and short bursts at `80%` of a fresh same-target direct baseline,
validates every iperf interval and fake-IP DNS answer, runs `6,780s` active +
`300s` idle + `120s` final drain, and fails closed on workload/process/route
health. The summary records workload completion, resource envelopes, utun
deltas, and endpoint final ownership. Shell fixtures prove success, zero-
traffic rejection, failure stop, idle/resume, and summary parsing without a
real TUN.

HK is now permitted to execute the formal target-only M0 through the same
user-controlled HITL boundary; Shenzhen remains preferred for M1/M2 and
recovery. M0 still requires a fresh direct baseline, user-executed TUN, two-
hour workload, stop bundle, and fresh create/smoke/stop rearm bundle. No frozen
data-plane constant changed.

The latest `239acba` M0 remains failed after two complete cycles because cycle
3 forward contained three Target receiver zero seconds. Before any new TUN,
the user must rebuild the reviewed runner, take a new structured baseline, and
pass `direct-discriminator`. Export its directory and begin `start`/`smoke`/
`m0` within 15 minutes of direct completion. A direct failure blocks TUN; a
direct PASS plus another clean-internal M0 receiver interruption stops this
test branch and opens connection-health isolation/failover architecture work.
