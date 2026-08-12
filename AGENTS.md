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
8. Stage learning: after every meaningful stage, write a short self-improvement
   summary to `.learnings/LEARNINGS.md`. If a command, test, script, or VPS run
   failed in a way that changes future behavior, also record it in
   `.learnings/ERRORS.md`.

Small related issues found during review should be fixed together before a
concentrated integration test, instead of running a full VPS suite after every
tiny edit.

## Skill-Assisted Workflow

Use skills as explicit engineering tools, not as optional labels. For Knife14
throughput work, the default assistant set is:

- `diagnose`: build a feedback loop first, rank falsifiable hypotheses, and map
  each probe to a prediction before changing code.
- `tdd`: add one focused red/green tracer-bullet test or harness check at a time
  for the behavior being changed. Avoid bulk speculative tests.
- `improve-codebase-architecture`: when the current seam cannot reproduce or
  lock down the bug, identify deepening opportunities before adding more logic.
- `code-review`: after each coherent code stage, review for correctness,
  performance, lifecycle, bounded-backpressure, TUN/UDP/TCP regression, and
  missing-test risk before requesting VPS acceptance.
- `self-improving-agent`: after each meaningful stage, update
  `.learnings/LEARNINGS.md`; update `.learnings/ERRORS.md` for failures that
  change future behavior.

For architecture-replacement stages, additionally use:

- `clean-architecture`: keep TUIC/anti-censorship transport details, TCP relay
  hot path, TUN/smoltcp integration, and local backpressure behind clear seams.
- `refactoring-patterns`: prefer branch-by-abstraction, parallel change, and
  behavior-preserving extraction over big-bang rewrites.
- `system-design`: state throughput, latency, queue, lifecycle, observability,
  and acceptance requirements before selecting mechanisms.
- `ddia-systems`: reason about bounded queues, flow-control, backpressure,
  fault tolerance, and data-plane consistency under pressure.

For engineering discipline and release cleanup, use:

- `pragmatic-programmer`: favor tracer bullets, reversible steps, and explicit
  contracts over speculative complexity.
- `clean-code`: keep hot-path names and functions readable; remove diagnostic
  noise that no longer distinguishes active hypotheses.
- `git-commit`: when committing, keep one coherent task per conventional commit
  and never stage secrets.
- `release-it`: after `100+ Mbit/s` is stable, organize resilience,
  observability, regression, and release-readiness checks.

When studying sing-box or other Go clients for design comparison, use:

- `go-code-review`: read Go changes and mature-client code with concrete
  correctness, performance, lifecycle, and logging checks.
- `go-concurrency`: inspect goroutine lifetimes, copy loops, channel/pool
  backpressure, and shutdown paths.
- `go-context`: inspect cancellation, timeout propagation, and request/flow
  lifecycle management.

Skill gates for performance stages:

- A stage that claims it can improve throughput must start with `diagnose` and
  `tdd`: either a deterministic local feedback loop, a focused replay/harness,
  or an explicit statement that no correct seam exists yet.
- If no correct seam exists, the next step is architecture extraction with
  `improve-codebase-architecture` and `refactoring-patterns`, not another
  parameter tweak.
- Do not claim a design can reach `30 Mbit/s` or `100+ Mbit/s` unless the code
  reachability gate, capacity math, and tests show a plausible sufficient path.
- After a failed run, compare expected invariants to observed counters and
  propose the next modification plan before editing code again.

## Performance Architecture Gate

For any throughput stage, especially one that predicts `>30 Mbit/s`,
`>100 Mbit/s`, or claims to be an architecture change, the agent must complete
a code-level reachability review before implementation or VPS acceptance. Do
not rely on "try it and see" as the primary design method.

Required gate contents:

- Target capacity math: translate the target into bytes/sec and compare it
  with the planned loop, batch, flush, queue, and timer capacities.
- End-to-end hot path inventory: list the exact code path from remote TUIC read
  through buffering, local TCP/smoltcp write, `iface.poll`, `flush_tx`, and TUN
  egress. Name the functions that will provide continuous progress.
- Necessary vs sufficient classification: explicitly state whether the change
  fixes only a necessary precondition or is intended to be sufficient for the
  next Mbps gate. A necessary-only fix must not be described as likely to reach
  `30 Mbit/s` or `100+ Mbit/s`.
- Old-path audit: list the old throttling, pacing, credit, timer, or writer
  paths that remain active. If the old local egress path remains active, do not
  claim a full data-plane architecture replacement.
- Failure discriminators: define which logs/counters will prove the bottleneck
  is remote read service, local buffer growth, local writer/egress cadence,
  smoltcp/TUN drain, QUIC/path behavior, or lifecycle/close-tail handling.
- TDD plan: add deterministic tests or harness checks for the code-level
  invariant before asking a VPS run to quantify throughput.
- Stop rule: if the gate cannot show a plausible sufficient path to the next
  Mbps target, say that directly, write the limitation down, and do not proceed
  with implementation as if the target were likely.

VPS acceptance is still required for the final Mbps number, because real QUIC,
kernel, TUN, smoltcp, RTT, and scheduler behavior cannot be proven statically.
However, VPS acceptance must validate a design that already passed the
code-level reachability gate; it must not substitute for that gate.

After a failed performance run, compare the expected gate invariants with the
observed counters before modifying code again. Do not continue with the same
class of tweak if the run proves that class of fix was only necessary, not
sufficient.

## Current Agent Role And Knife15 Position

The agent's role in this repository is data-plane engineering for the
mini_vpn core: read evidence, design small testable stages, implement Rust and
script changes, review risk, run or prepare VPS acceptance, and preserve
learning memory. The agent is not owning GUI, mobile App, or backend control
plane work in this route; those belong to separate product/control-plane
sessions. Current work must stay tied to the `Rules.md` data-plane goals:
transparent TCP, UDP/live-streaming, high concurrency, high throughput, long
duration, and stable quality.

For cold-start grounding, read this file, `Rules.md`, `HANDOFF.md`, `TODO.md`,
latest `.learnings/LEARNINGS.md` / `.learnings/ERRORS.md`, and the relevant
`docs/tech/2026-*.md` files. The older numbered `docs/tech/*.md` files are
historical background; do not load all of them by default. For current
release-readiness work, prioritize the latest Knife15 result, long-duration
plan, and macOS HITL runbook. Use the Knife14 endpoint-pacing and VPS completion
documents as the frozen architecture/capacity baseline.

Current Knife15 summary, as of 2026-08-12:

- Exact-source `579fff4` paired qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260812_092030.tar.gz` (SHA-256
  `c6d9c339...`) passed baseline `38.274/61.255 Mbit/s`, direct
  `19.125 Mbit/s`, smoke, every preflight, two cycles/eight phases, two DNS
  and real-client checks, and cleanup. Target receiver-zero was zero, maximum
  TCP gap was `7,733,248B`, and maximum UDP loss was `1.954378%`. Verdict is
  `PASS_NON_ACCEPTANCE`; formal M2 was not run.
- D16 terminal ownership, Endpoint conservation, routes, DNS, process,
  physical interface, recovery evidence, and cleanup were clean. Four
  `Stopped(0)` writes were timed-transfer tails. Endpoint terminal
  live/outstanding was `0/0B` and socket would-block was zero.
- No generation replacement or path reset occurred. This proves regression
  cleanliness without independently proving the rare current-service handoff
  branch. Do not repeat qualification; formal M2 is reopened as the decisive
  branch/effectiveness gate.
- Paired Exit SHA-256 `95afe240...` captured `12,235,760` packets with zero
  kernel drops. It was frozen/bundled after qualification, its observer state
  and nftables ownership are gone, and sing-box remains active.
- Formal M2 now rejects `85d8772` and requires reviewed source `579fff4` or a
  descendant. Focused source-floor RED/GREEN and the complete runner self-test
  pass. No data-plane code, workload, SLI, or frozen value changed. Result:
  `docs/tech/2026-08-12-knife15-m2-replacement-current-service-handoff-macos-qualification-results.md`.
- Pull/rebuild the pushed reviewed descendant and take exactly one fresh
  formal `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke
  -> fresh .33 observer start -> m2 -> status -> stop` while the Mac and VPSs
  can remain uninterrupted for about 25 hours. Sync both bundles. M3 remains
  blocked until formal M2 and cleanup pass.

- Previous accepted position: exact-source `85d8772` formal artifact
  `/tmp/mini_vpn_knife15_macos_20260812_034701.tar.gz` (SHA-256
  `9d33b1af...`) passed baseline `17.776/64.868 Mbit/s`, direct, smoke,
  matching observer admission, every preflight, fifteen complete cycles, and
  cleanup. Cycle 16 `short-forward-5` lost its first complete Target receiver
  interval. Formal M2 remains failed; M3 remains blocked.
- Paired Exit SHA-256 `643a019a...` was independently copied and verified.
  The exact Target flow ACKed continuously at about `1..5ms` RTT, but the Exit
  received only `113,261B` during the first `1.001031s`, below one `131,072B`
  application block. D16, TUN, Endpoint, observer, routes, DNS, process, and
  cleanup were clean.
- Direct degraded-generation replacement surrendered a predecessor at current
  `361,778B` cwnd while inheriting only its old `26,338B` installation floor.
  The fresh successor proved `26,338B` and immediately owned the failed flow.
  This rejects path-reset-only service inheritance as sufficient; do not
  repeat or tune `85d8772`.
- Reviewed local implementation atomically samples the exact current
  generation under its slot mutex and publishes
  `max(previous_owned_floor,current_generation_cwnd)` before successor
  handshake/proof. Existing sequential exact ACK-owned turns reach the floor
  under the unchanged five-second deadline; install CAS rechecks it. Frozen
  data-plane values, workload, and SLIs are unchanged.
- Root `707+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell, runner/observer self-tests, vendored Quinn
  `40+3 ignored` plus doc `1`, quinn-proto `330` plus docs `3`, root docs,
  fmt/diff/provenance/secret, exact `239.536 Mbit/s` capacity, and review pass
  with no unresolved P0/P1. Result:
  `docs/tech/2026-08-12-knife15-m2-replacement-current-service-handoff-local-results.md`.
- Pull/rebuild the pushed reviewed descendant and take one fresh paired
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
  observer start -> m2-qualification -> status -> stop`. Do not run formal M2
  or repeat/tune unchanged. A recurrence after proof reaches the handoff floor
  rejects the mechanism; a clean qualification only reopens formal M2.

- Previous accepted position: exact-source `166c390` artifact
  `/tmp/mini_vpn_knife15_macos_20260812_030150.tar.gz` (SHA-256
  `692c6286...`) passed start/smoke and the gates before observer admission,
  then formal M2 stopped before traffic with an empty observer-status file.
  Cleanup passed and formal M2 remained `NOT_RUN`.
- The matching `.33` v2 observer was active and healthy. `start_runner()` had
  omitted the parsed TUIC `server_port` from root-owned state; formal M2 read
  an empty value and its silent input guard returned before SSH. This was a
  local runner state-contract bug, not VPS, network, observer, or operator
  failure.
- Reviewed repair `1cdb17c` persists the complete start network binding and
  writes sanitized evidence for invalid observer inputs. Focused RED/GREEN
  runner tests pass. Formal M2 rejects source older than `1cdb17c`; no Rust
  production code, workload, SLI, or frozen value changed. The unused
  observer was frozen/bundled as SHA-256 `78542720...`. Result:
  `docs/tech/2026-08-12-knife15-formal-m2-observer-state-binding-repair-local-results.md`.
- Pull/rebuild the pushed reviewed descendant and take one fresh
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
  observer start -> m2 -> status -> stop`. Do not reuse the stopped run or
  frozen observer. M3 remains blocked.

- Previous accepted position: exact-source `0e44a939` formal artifact
  `/tmp/mini_vpn_knife15_macos_20260811_074703.tar.gz` (SHA-256
  `a0c268c1...`) completed fourteen cycles before cycle 15 reverse UDP lost
  `3.934088%`, above the frozen `3%` limit. TCP Target receiver-zero remained
  zero and cleanup passed. Target sent every datagram; local TUN/D16/Endpoint,
  backpressure, interface, and gateway evidence was clean. A later exact
  same-rate `.77 -> .33` reverse-UDP discriminator lost zero of `562,214`
  datagrams. Classify this as a transient external UDP/QUIC path event, but
  keep formal M2 failed because no same-window observer can split the path.
- The v2 Exit observer runs for at most `93,600s`, keeps a bounded recent
  Target/TUIC pcap ring, and records four-direction nftables packet/byte
  counters every second. Its table has exact ownership and cannot be bypassed
  by bundling after process timeout. Formal M2 now requires an exact matching,
  fully healthy observer no older than `900s`, binds SSH to the recorded Exit,
  and automatically freezes/bundles evidence on success, failure, signal, or
  unexpected exit.
- Formal M2 rejects source older than reviewed observer commit `2707873`.
- Local runner/observer self-tests and a real `.33` lifecycle probe pass. The
  probe counted reverse UDP, removed all observer state/table ownership, left
  sing-box active with zero restarts, and produced bundle SHA-256
  `fff3011e...`. No Rust production code, workload, SLI, or frozen value
  changed. Result:
  `docs/tech/2026-08-11-knife15-formal-m2-udp-path-attribution-observer-local-results.md`.
- Do not repeat qualification or tune. Pull/rebuild the reviewed pushed
  descendant, start one fresh v2 observer after smoke, and take exactly one
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
  observer start -> m2 -> status -> stop`. Sync both Exit and Mac bundles.
  Formal M2 and M3 remain blocked until the transaction and cleanup pass.

- Previous accepted position: concentrated formal-M2 self-audit removed three locally preventable
  long-run risks without changing Rust production code or frozen behavior.
  Formal M2 now shares the qualification's exact recovery contract, rejects
  source older than reviewed inheritance `de4d170`, and refuses a
  stale/symlinked release binary or dirty tracked worktree before traffic.
  The HITL runbook now points only to formal `m2` and mandatory cleanup.
- Root `705+3 ignored`, main `2`, integration `10+4 ignored`, focused
  successor `9+1`, release full-TUN, release build, established Clippy, fmt,
  shell, runner/observer self-tests, and diff checks pass. Release batch
  capacity was `1,406.229 Mbit/s`, with zero ring drops/full waits. Review has
  no unresolved P0/P1; readiness to start formal M2 is `10/10`. Result:
  `docs/tech/2026-08-11-knife15-formal-m2-preflight-self-audit-local-results.md`.
- Pull/rebuild the pushed reviewed descendant and take exactly one clean
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke -> m2
  -> status -> stop` while `.33` Exit and `.77` Target can remain up for about
  25 hours. Do not repeat qualification. M3 remains blocked until formal M2
  and cleanup pass.

- Previous accepted position: exact-source `25ea39c` paired qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260811_054300.tar.gz` (SHA-256
  `23a49673...`) passed baseline `11.858/55.917 Mbit/s`, direct at
  `5.928 Mbit/s` without zero intervals, smoke, every preflight, two exact
  cycles/eight phases, two DNS/real-client checks, and cleanup. Target
  receiver-zero intervals were zero; one sender interval was zero, maximum
  TCP gap was `5,636,096B`, and maximum UDP loss was `1.830049%`.
- The immutable artifact records `failed/MISMATCH` because the runner asserted
  terminal safety immediately after the final real-client probe. Its latest
  Endpoint line was temporarily `60,031/1,409/0B`; the next ordinary sample
  about 14 seconds later was `61,414/0/0B` and stayed zero-owned. Exact-prefix
  replay is dirty before that drain record and PASS after it. Do not rewrite
  the archive status; classify it as a runner false negative.
- Paired Exit SHA-256 `6f745e9f...` captured `3,270,562` packets with zero
  kernel drops. The short-forward connection containing the one Mac
  sender-zero supplied `2,817,041B/10.182s` with a maximum `263.147ms` gap,
  so Target remained continuous. Three remote-write failures were completed
  timed-transfer close tails with zero D16 queued/leased/reserved ownership.
- No path reset, successor replacement, or inherited floor occurred. The run
  is regression-clean but does not claim inheritance-branch reachability;
  formal M2 is decisive. Reviewed runner fix `3a8a796` waits under the existing
  `duration + 30` bound, rejects persistent dirt, then rechecks process,
  watchdog, routes, DNS, full-tunnel, and network health. Full runner
  self-test, artifact replay, shell, diff, secret scan, and review pass with
  no unresolved P0/P1. Result:
  `docs/tech/2026-08-11-knife15-m2-successor-forward-service-inheritance-macos-qualification-results.md`.
- Do not repeat or tune qualification. Pull/rebuild the pushed reviewed
  descendant and take one fresh `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> m2 -> status -> stop` when the
  Mac and `.33` services can remain uninterrupted for about 25 hours. A
  Target receiver-zero after a logged nonzero inherited floor rejects the
  mechanism. M3 remains blocked until formal M2 and cleanup pass.

- Previous accepted position: exact-source `e531b30` formal artifact
  `/tmp/mini_vpn_knife15_macos_20260811_014220.tar.gz` (SHA-256
  `2d8b4e6e...`) passed baseline/direct, smoke, every preflight, five complete
  M2 cycles, most of cycle 6, and cleanup. Cycle-6 `short-forward-3` lost its
  first complete Target receiver interval after sender admission of
  `10,878,976B`; Target received `4,194,304B` and sender retransmits were zero.
- D16, TUN, Endpoint, path identity, ACK progress, VPS probes, resources,
  routes, DNS, and cleanup were healthy. No paired Exit observer existed. The
  exact predecessor reset at `41,301 -> 12,000B` cwnd; its successor installed
  after one exact service turn at `26,424B` and owned the failed flow. This
  rejects one-turn readiness as sufficient. Do not repeat or tune that build.
- Reviewed implementation `de4d170` stores the positive pre-reset cwnd as a
  monotonic exact generation-owned forward-service floor. A successor performs
  sequential ACK-owned service turns on one path until it reaches the floor,
  all within the unchanged five-second whole-replacement deadline. Loss, path
  change, close, timeout, or no progress fails closed. Floor publication and
  path reset are atomic; install CAS rechecks the latest owned floor.
- Root `705+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`,
  quinn-proto `330` plus docs `3`, root docs, fmt/diff/local-patch/secret,
  capacity, and review pass with no unresolved P0/P1. Exact Endpoint capacity
  was `240.349 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-11-knife15-m2-successor-forward-service-inheritance-local-results.md`.
- Do not run another formal M2 yet. Pull/rebuild the pushed reviewed descendant
  and take exactly one fresh paired `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> m2-qualification -> status ->
  stop`. A receiver-zero after a nonzero inherited floor rejects the
  architecture without tuning/repetition. A clean qualification reopens
  formal M2. M3 remains blocked until formal M2 passes.

- Previous accepted position: exact-source `c6ffa3a` paired qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260810_101221.tar.gz` (SHA-256
  `6121b8b8...`) passed baseline `12.791/44.901 Mbit/s`, direct at
  `6.393 Mbit/s` with no zero interval, smoke, every preflight, two cycles/eight
  phases, two DNS/real-client checks, and cleanup. All six TCP results had zero
  sender/receiver intervals; maximum TCP gap was `5,767,168B`, maximum UDP
  loss was `1.830894%`. Verdict is `PASS_NON_ACCEPTANCE`; formal M2 was not run.
- Conn1 `gap_ack_reinforcements` advanced `0 -> 8 -> 22` under smoke pressure
  and remained exactly `22` through both cycles and final drain. This proves
  the exact production branch was reached and did not create a persistent or
  self-sustaining ACK stream.
- Paired Exit artifact SHA-256 `03d51370...` captured `2,971,792` packets with
  zero kernel drops. Cycle-1/2 reverse maximum supply gaps were only
  `251.174/342.575ms`, no qualification flow had a gap >=500ms, and the longest
  zero receive windows were `249.901/341.525ms`, versus the previous
  `1,158.373ms` supply pause.
- Endpoint high/final was `61,440B / 61,414/0/0B`; interface errors and socket
  would-block were zero. Two peer `Stopped(0)` writes were timed-transfer close
  tails with D16 queued/leased/reserved `0/0/0B`. Process, DNS, routes, TUN
  ownership, secret scan, and cleanup passed.
- Retain reviewed implementation `300fb16`. Do not repeat or tune
  qualification. Formal M2 is reopened on the reviewed `c6ffa3a`
  production-code tree and its docs-only pushed descendant, with the frozen
  workload/config: take one fresh `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> m2 -> status -> stop`. M3 remains
  blocked until formal M2 and cleanup pass. Result:
  `docs/tech/2026-08-10-knife15-m2-reverse-gap-ack-reinforcement-macos-qualification-results.md`.

- Previous accepted position: exact-source `673d13d` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260810_075621.tar.gz` (SHA-256
  `25847808...`) passed baseline `23.502/64.236 Mbit/s`, direct, smoke, every
  preflight, cycle 1, cycle-2 forward, and cleanup. Cycle-2 reverse then lost
  one complete Target receiver interval; formal M2 was not run.
- Conn1 generation 2 stream 2 remained Ready without replacement or migration.
  Its ordered reader stopped at `385,286,941B` behind a missing `1,386B`
  prefix while same-stream buffered tail grew `5,876,229 -> 7,552,771B`;
  another `2,982` datagrams and `2,981` STREAM frames arrived, and the prefix
  recovered after `1,759ms`.
- Paired Exit SHA-256 `5c26edfd...` saw the exact reverse socket with zero
  capture/kernel drops. Its TCP receive window reached zero, supply paused
  `1,158.373ms`, and the window reopened immediately before supply resumed.
  D16, TUN, Endpoint, certificate admission, Target, routes, process, and
  cleanup were healthy. This selects QUIC ordered-loss recovery plus
  stream-credit exhaustion; transient WAN loss/RTT growth is a contributor.
- Reviewed pushed `300fb16` authorizes one bounded ACK reinforcement only from
  exact `STREAM_DATA_BLOCKED` pressure plus an ordered gap and buffered tail
  in the same open receive stream. Packet-number history alone is ineligible;
  normal ACK progress replaces the opportunity, reinforcement cannot
  self-rearm, and range collapse disarms it. The exact public counter is
  `gap_ack_reinforcements`.
- The 64KiB flow-control replay blocks the writer beyond the receive window,
  drops the sole gap ACK, and recovers the prefix through one reinforcement
  before sender loss detection/PTO. Root `700+3 ignored`, main `2`, integration
  `10+4 ignored`, release, established Clippy, shell, vendored Quinn `40+3
  ignored` plus doc `1`, quinn-proto `330` plus docs `3`, root docs,
  fmt/diff/vendor/secret, capacity, and review pass with no unresolved P0/P1.
  Exact 32MiB Endpoint capacity was `239.815 Mbit/s`, final `61,440/0/0B`,
  zero socket would-block. Result:
  `docs/tech/2026-08-10-knife15-m2-reverse-gap-ack-reinforcement-local-results.md`.
- Pull/rebuild `300fb16`. When the Mac is ready, start one fresh bounded `.33`
  observer and take exactly one `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> m2-qualification -> status ->
  stop`. Do not run formal M2 or repeat/tune unchanged. A recurrence with
  nonzero reinforcement rejects this mechanism as sufficient; zero
  reinforcement preserves only the exact reachability branch. Formal M2 and
  M3 remain blocked.

- Previous accepted position: exact-source `80df179` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260810_055556.tar.gz` (SHA-256
  `9d291f6c...`) passed baseline `23.658/61.795 Mbit/s`, direct at
  `11.822 Mbit/s` without zero intervals, smoke, every preflight, seven mixed
  phases, and cleanup. Cycle-2 short forward then lost one complete Target
  receiver interval; formal M2 was not run.
- Paired Exit SHA-256 `6d5538c9...` saw `1,506,514B` over `10.210s`, maximum
  supply gap `280.203ms`, Target ACK RTT about `1..6ms`, zero sender TCP
  retransmit growth, and zero capture/kernel drops. D16, TUN, Endpoint,
  routes, process, and cleanup were healthy.
- The selected successor proved a `24,800B` post-turn cwnd floor but was later
  admitted at `17,360B`, about `173ms` RTT, and zero black holes. This selects
  stale successor service readiness, not operator, Target, Exit-to-Target,
  local data-plane ownership, or parameter tuning.
- Reviewed pushed `3737dee` stores an exact immutable
  identity/logical-generation/path-generation/post-turn-cwnd certificate.
  Ready requires the same identity/path and current cwnd at least the proved
  floor. Stale auxiliaries reuse the bounded fresh replacement/CAS/drain;
  initial/incomplete evidence stays `Unknown`, and fallback remains bounded.
- Review found and fixed a possible replacement-decision spin when exact epoch
  evidence was absent. Root `700+3 ignored`, main `2`, integration `10+4
  ignored`, release, established Clippy, shell, vendored Quinn `40+3 ignored`
  plus doc `1`, quinn-proto `326` plus docs `3`, root docs,
  fmt/diff/vendor/secret, and review pass with no unresolved P0/P1. Exact
  Endpoint capacity was `237.868 Mbit/s`, final `61,440/0/0B`, zero socket
  would-block. Result:
  `docs/tech/2026-08-10-knife15-m2-successor-service-certificate-local-results.md`.
- Pull/rebuild the pushed reviewed descendant. When the Mac is ready, start
  one fresh bounded `.33` observer and take exactly one `m2-ipv6-check ->
  baseline -> direct-discriminator -> start -> smoke -> m2-qualification ->
  status -> stop`. Do not run formal M2 or repeat/tune unchanged. A recurrence
  rejects this architecture. Formal M2 and M3 remain blocked.

- Previous accepted position: exact-source `b437b93` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260810_021421.tar.gz` (SHA-256
  `33dddf00...`) passed baseline `25.429/54.539 Mbit/s`, direct at
  `12.715 Mbit/s` without a complete zero interval, smoke, every preflight,
  cycle 1, cycle 2 long forward/reverse TCP and reverse UDP, and cleanup. The
  cycle-2 short forward then lost one complete Target receiver interval;
  formal M2 was not run.
- Paired Exit artifact SHA-256 `2bc46085...` saw the exact business socket
  receive about `4.23MB` without a supply gap above `165.430ms`; Target ACK
  RTT was about `1..4ms`, sender TCP retransmit growth and capture/kernel drops
  were zero. D16 accepted `6,296,563B` into Quinn and waited up to
  `3,820,792us`; generation-3 cwnd grew only `12,947 -> 223,724B`. D16, TUN,
  Endpoint, routes, process, and cleanup were healthy. This selects
  client-to-Exit congestion ownership.
- Production `run_relay_writer` yields on its async Progress signal after a
  successful partial write. Quinn can packetize those bytes before the next
  retry reaches Blocked, after the old demand bit was cleared. The prior test
  immediately retried and did not reproduce this worker turn.
- Reviewed `c218d8a` retains an exclusive per-stream accepted-offset
  boundary until cumulative ACK or terminal lifecycle. Packets for another
  stream, control traffic, and later same-stream bytes cannot borrow it. The
  production-order RED was `24,000 -> 1,772,034B < 1,990,080B` and is GREEN.
  No D16, MTU, pool, window, chunk, Cubic, GSO, Endpoint, self-wake, retry,
  workload, or SLI changed.
- Root `689+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`,
  quinn-proto `325` plus docs `3`, root docs, fmt/diff/vendor/secret, and
  review pass with no unresolved P0/P1. Exact 32MiB Endpoint capacity was
  `239.784 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-10-knife15-m2-transport-write-demand-flight-ownership-local-results.md`.
- Pull/rebuild the pushed reviewed descendant. When the Mac is ready, start
  one fresh bounded `.33` observer and take exactly one `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> m2-qualification -> status ->
  stop`. Do not run formal M2 or repeat/tune unchanged. A recurrence rejects
  this architecture. Formal M2 and M3 remain blocked.

- Previous accepted position:

- Exact-source `eb2185f` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260808_092053.tar.gz` (SHA-256
  `100d3163...`) passed baseline `17.521/65.771 Mbit/s`, direct at
  `8.758 Mbit/s` without a complete zero interval, smoke, every preflight,
  long forward/reverse TCP, reverse UDP, and cleanup. The cycle-1 short
  forward then had two sender-zero intervals and one complete Target
  receiver-zero interval; formal M2 was not run.
- The exact stream used successor generation 2 at `24,886B` cwnd and about
  `165ms` RTT. Quinn accepted `6,300,119B`, writer Pending reached
  `2,367,062us`, sampled QUIC ACKs reached `3,849,795B`, and Target received
  `4,194,304B`. D16, TUN, Endpoint conservation, routes, process, and cleanup
  were healthy. No fresh paired Exit observer existed; do not claim one.
- Reviewed `f9c3c23` preserves a stream's proven write-Blocked demand across
  Writable delivery, then tags only packets actually carrying that stream's
  frames. A cancelled/deferred writer cannot lend cwnd authority to another
  stream. Pre-start successor adoption also excludes only the exact active
  PLPMTUD probe while authentication packet loss remains fail-closed.
- Root `689+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`,
  quinn-proto `323` plus docs `3`, root docs, fmt/diff/vendor/secret, and
  review pass with no unresolved P0/P1. Exact 32 MiB Endpoint capacity was
  `240.079 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-08-knife15-m2-transport-write-demand-local-results.md`.
- Pull/rebuild the pushed reviewed descendant. When the Mac is ready start
  one fresh bounded `.33` observer and take exactly one
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
  m2-qualification -> status -> stop`. Do not run formal M2 or repeat/tune
  unchanged. Formal M2 and M3 remain blocked.

- Previous accepted position: review before the next Mac run found that TUIC
  Authenticate could already
  be an in-flight Quinn Data packet when the successor service turn started,
  leaving a protocol prerequisite outside its exact ACK/loss ownership. An
  adopted PLPMTUD probe also used a special loss branch that left the turn
  nonterminal.
- Reviewed `6aefd19` adopts every existing ACK-eliciting,
  congestion-accounted Data packet at turn start. Delivered authentication
  succeeds only after exact ACK ownership; withheld authentication and owned
  PLPMTUD loss both fail `PacketLost`. Turn size/deadline, MTUD/congestion
  policy, payload, retry, Endpoint, admission, and frozen values are unchanged.
- Root `689+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`,
  quinn-proto `320` plus docs `3`, root docs, fmt/diff/vendor/secret, and
  review pass with no unresolved P0/P1. Exact 32 MiB Endpoint capacity was
  `240.300 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-08-knife15-m2-successor-authentication-flight-ownership-local-results.md`.
- The accepted Mac artifact remains exact-source `a7cd603`; no new Mac result
  is claimed. Pull/rebuild the pushed reviewed descendant. When the Mac is
  ready start one fresh bounded `.33` observer and take exactly one
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
  m2-qualification -> status -> stop`. Do not run formal M2 or repeat/tune
  unchanged. Formal M2 and M3 remain blocked.

- Previous accepted position: exact-source `a7cd603` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260808_065723.tar.gz` (SHA-256
  `6fae3611...`) passed baseline `28.490/52.003 Mbit/s`, direct at
  `13.970 Mbit/s` without gaps, smoke, every preflight, cycle-1 long
  forward/reverse TCP, reverse UDP, and cleanup. The short forward then lost
  one complete Target receiver interval; formal M2 was not run.
- Conn1 generation 2 passed the corrected service turn at
  `12,000/12,800/12,800/0B` and installed with `24,800B` cwnd. Its business
  writer remained Pending and ACK-progressing, but the first 128KiB took
  about `1.21s` to reach the Exit. Paired Exit SHA-256 `ffb39a48...` captured
  `2,075,057` packets with zero kernel drops and about `1ms` Target ACK RTT.
  This selects client-to-Exit ordinary cwnd growth, not operator, Target,
  Exit-to-Target, D16, TUN, Endpoint capacity, cleanup, or a frozen value.
- Reviewed `1d08565` prevents an empty transmit poll blocked only by Endpoint
  `Bulk` reservation from publishing application idle. Endpoint Control,
  real idle, partial batches, Quinn pacing/congestion, Cubic, tagged turns,
  loss/path/close, Endpoint accounting, and all frozen behavior are unchanged.
  The real-pair RED was `12,000 -> 12,000B` across 120 waits and is GREEN at
  one full growth round.
- Root `689+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`,
  quinn-proto `317` plus docs `3`, root docs, fmt/diff/vendor/secret, and
  review pass with no unresolved P0/P1. Exact 32 MiB Endpoint capacity was
  `239.487 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-08-knife15-m2-endpoint-blocked-app-limited-local-results.md`.
- Pull/rebuild the pushed descendant. When the Mac is ready start a fresh
  bounded `.33` observer and take exactly one `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> m2-qualification -> status ->
  stop`. Do not run formal M2 or repeat/tune unchanged. Formal M2 and M3
  remain blocked.

- Previous accepted position: exact-source `f693d0d` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260808_054225.tar.gz` (SHA-256
  `0e6d8ee6...`) passed baseline `21.003/42.391 Mbit/s`, direct at
  `10.497 Mbit/s` without gaps, smoke, every preflight, the long forward,
  reverse TCP, reverse UDP phases, and cleanup. The first ten-second short
  forward lost one complete Target receiver interval; formal M2 was not run.
- Conn1 generation 2 passed its successor service turn at
  `12,000/12,800/12,800/0B`, but installed with only `13,280B` cwnd and stayed
  cold through the reverse phases. Paired Exit artifact SHA-256 `4879b790...`
  captured `1,569,331` packets with zero kernel drops; Target ACKed every
  Exit-supplied short-flow byte at about `1..3ms`. This selects client-to-Exit
  initial supply, not operator, Target, Exit-to-Target, D16, Endpoint, TUN,
  cleanup, or a frozen value. The observer is stopped and bundled.
- Reviewed `c3c264a` preserves non-application-limited congestion ownership
  for only the existing tagged service-turn ACKs when a later empty Quinn
  transmit poll changes the global app-limited snapshot. Ordinary packets,
  Cubic, turn size/deadline/failure, D16, MTU, pool, windows, chunk, Endpoint,
  GSO, recovery, workload, and SLIs are unchanged.
- The realistic `164ms` RTT replay was RED at
  `initial=12,000B, acked=13,068B, final=23,616B` and is now GREEN. Root
  `689+3 ignored`, main `2`, integration `10+4 ignored`, release, established
  Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`, quinn-proto `316`
  plus docs `3`, root docs, fmt/diff/vendor/secret, and review pass with no
  unresolved P0/P1. Exact 32 MiB Endpoint capacity was `237.701 Mbit/s`,
  final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-08-knife15-m2-successor-service-turn-app-limited-local-results.md`.
- Pull/rebuild the pushed descendant. When the Mac is ready start a fresh
  bounded `.33` observer and take exactly one `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> m2-qualification -> status ->
  stop`. Do not run formal M2 or repeat/tune unchanged. Formal M2 and M3
  remain blocked.

- Previous accepted position: exact-source `b04cb65` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260808_030357.tar.gz` (SHA-256
  `e5c866f6...`) passed baseline `27.973/63.814 Mbit/s`, direct at
  `13.970 Mbit/s` without gaps, smoke, every preflight, exact forward, and
  cleanup. Cycle 1 reverse never connected and hit the unchanged 330-second
  child timeout; formal M2 was not run.
- Conn1 predecessor black holes advanced `0 -> 5`. Its successor service turn
  correctly failed closed at target/sent/acked/lost
  `12,000/13,058/10,326/1,366B`, but the maintenance error propagated to the
  triggering business open as `handshake_failed -> rearm`. A later independent
  open passed at `12,800/12,800/0B` and installed generation 2. Paired Exit
  capture SHA-256 `21b08483...` had `547,358` packets, zero kernel drops, and
  no new Target socket after forward completion. The observer is stopped.
- Reviewed `68c7271` preserves the failed successor/predecessor contract,
  excludes the failed slot for that open, and admits only another qualified
  current generation. No same-open second replacement is possible; no safe
  fallback preserves both exact errors; later independent opens retain fresh
  authority. All frozen behavior remains unchanged.
- Root `689+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`,
  quinn-proto `315` plus docs `3`, root docs, fmt/diff/vendor/secret, and
  review pass with no unresolved P0/P1. Exact 32 MiB Endpoint capacity was
  `240.470 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-07-knife15-m2-replacement-failure-business-fallback-local-results.md`.
- The prior observer is stopped and bundled. Pull/rebuild the pushed reviewed
  descendant; when the Mac is ready start a fresh bounded `.33` observer and
  take exactly one `m2-ipv6-check -> baseline -> direct-discriminator -> start
  -> smoke -> m2-qualification -> status -> stop`. Do not run formal M2 or
  repeat/tune unchanged. Formal M2 and M3 remain blocked.

- Exact-source `7131de4` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260807_091815.tar.gz` (SHA-256
  `7c749fb9...`) passed baseline `35.336/61.978 Mbit/s`, direct at
  `17.659 Mbit/s` without gaps, start/smoke, every preflight, the long
  forward/reverse TCP/reverse UDP phases, and cleanup. The first short
  forward then lost one complete Target receiver interval; formal M2 was not
  run.
- Service-normalized admission correctly selected conn1 generation 2 at
  `12,000B/163.342ms` over conn0 at `8,400B/163.153ms`. The selected
  authenticated replacement had never supplied forward bulk and remained at
  its initial window, while its predecessor had reached
  `780,994B/164.107ms`. This selects authentication-only successor install,
  not selector, operator, TUN, D16, Endpoint, or parameter tuning.
- Reviewed `541fbfb` requires one current-cwnd, Endpoint-bulk Quinn service
  turn after TUIC authentication and before generation install. Every tagged
  encrypted packet byte must be ACKed on the same path generation; any loss,
  path change, close, or the original shared five-second replacement deadline
  fails without retry and leaves the predecessor current. The existing
  identity/activity/generation CAS remains authoritative.
- Root `688+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`,
  quinn-proto `315` plus docs `3`, root docs, fmt/diff/vendor/secret, and
  review pass with no unresolved P0/P1. Exact 32 MiB Endpoint capacity was
  `240.291 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-07-knife15-m2-quinn-successor-service-turn-local-results.md`.
- The intended prior `.33` observer expired before the Mac run and captured no
  matching packet; do not overclaim that evidence. A fresh observer is active
  at `/tmp/mini_vpn_knife15_exit_target_observer_20260807_102734`. Pull/rebuild
  the pushed descendant and take exactly one
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
  m2-qualification -> status -> stop`. Do not run formal M2 or repeat/tune
  unchanged. Formal M2 and M3 remain blocked.

- Previous accepted position: exact-source `12e845f` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260807_054320.tar.gz` (SHA-256
  `916db92b...`) passed baseline `26.054/60.199 Mbit/s`, direct at
  `13.020 Mbit/s` without gaps, start/smoke, every preflight, exact transfer,
  and cleanup. Its first qualification forward had two complete sender and
  receiver zero intervals in the first six seconds; formal M2 was not run.
- Paired Exit artifact SHA-256 `936e8a16...` captured `515,691` packets with
  zero kernel drops. Exact Exit supply never paused more than `223.987ms` and
  Target ACKed around `1ms`; writer ACKs, D16, TUN, Endpoint, routes, process,
  and cleanup stayed healthy. This selects cold pool placement, not operator,
  network, Exit-to-Target, recovery, or a frozen parameter.
- Control chose warm conn1 from equal active `2:2`; its reservation changed
  data ownership to `2:4`, and raw least-active placement chose cold conn0
  despite current path service `12,000B/164.215ms` versus
  `871,763B/163.853ms`. Reviewed `6e78d23` keeps qualification/replacement
  first and exactly orders admitted busy known candidates by
  `active * RTT / cwnd`. Idle/unknown/all-degraded fallbacks and all payload,
  connection, recovery, and frozen behavior remain unchanged.
- Root `686+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`,
  quinn-proto `311` plus docs `3`, root docs, fmt/diff/vendor/secret, and
  review pass with no unresolved P0/P1. Exact 32 MiB Endpoint capacity was
  `240.313 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-07-knife15-m2-service-normalized-admission-local-results.md`.
- The `.33` observer is active at
  `/tmp/mini_vpn_knife15_exit_target_observer_20260807_064333`. Next pull and
  rebuild the pushed descendant, then take exactly one fresh
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
  m2-qualification -> status -> stop`. Preserve evidence after failure; do
  not run formal M2 or repeat/tune unchanged. Formal M2 and M3 remain blocked.

- Previous accepted position: exact-source `f8c0639` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260807_031530.tar.gz` (SHA-256
  `60c60b7f...`) passed baseline `24.026/58.984 Mbit/s`, direct, start/smoke,
  every preflight, long forward/reverse TCP/reverse UDP, and cleanup. Cycle 1
  short forward then had two complete initial Target receiver-zero intervals;
  formal M2 was not run.
- The ordered-gap predicate caused six Endpoint rebinds on the passing long
  forward while each missing prefix coexisted with continuing multi-megabyte
  tail growth. The failed short flow was uplink-only, accepted `6,296,649B`,
  waited `4,493,917us`, and had no business read-gap signal. This rejects
  active ordered-gap migration as unsafe and insufficient; do not tune it.
- Reviewed `847a5d7` removes ordered gaps from recovery authority. The pure,
  bounded `RecoveryEvidenceObserver`, enabled only by the existing TCP
  diagnostics switch, emits one ordered-gap observation and exact writer
  Pending start/end ACK aggregates without mutation. Existing ACK-stall
  rebind, writer plus PLPMTUD path reset, and UDP-demand recovery are
  unchanged. The runner rejects legacy ordered-gap actions and malformed
  evidence.
- Root `684+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`,
  quinn-proto `311` plus docs `3`, fmt/diff/vendor/secret, and review pass with
  no unresolved P0/P1. The exact 32 MiB Endpoint gate reached
  `240.370 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-07-knife15-m2-recovery-evidence-observer-local-results.md`.
- Next pull/rebuild the pushed descendant. With the `.33` Exit observer active,
  take exactly one fresh `m2-ipv6-check -> baseline -> direct-discriminator ->
  start -> smoke -> m2-qualification -> status -> stop`. It normally takes
  about 30 minutes and can only produce `PASS_NON_ACCEPTANCE`. Preserve
  `status/snapshot/stop` after failure. Do not run formal M2 or repeat/tune
  unchanged. Formal M2 and M3 remain blocked pending paired evidence.

- Previous accepted position: exact-source `0a71ebe` formal-M2 artifact
  `/tmp/mini_vpn_knife15_macos_20260806_104632.tar.gz` (SHA-256
  `da12448c...`) passed baseline `46.328/59.947 Mbit/s`, direct
  `23.077 Mbit/s` without gaps, start/smoke, every preflight, cycle 1, cycle
  2 forward, and cleanup. Cycle 2 reverse then lost eleven complete receiver
  intervals about 25 minutes into the schedule; the later fourteen hours were
  evidence hold time, not a completed soak.
- The exact conn1/stream21 ordered reader stopped at `569,624,937B` for
  `12,630ms` while the connection received `111` datagrams, `96,732B`, and
  `62` STREAM frames by its first Pending report. Historical evidence is
  connection-wide, so it selects an exact same-stream observer rather than
  proving same-stream buffered bytes retrospectively. D16, TUN, Endpoint
  conservation, routes, process, and cleanup stayed healthy; no frozen value
  is implicated.
- Reviewed implementation `4d168a1` exposes a read-only Quinn assembler
  progress handle. A live D16 ordered reader whose unchanged consumed prefix
  has exact same-stream bytes buffered beyond a missing prefix for two
  observations and one full existing `250ms` sample interval consumes one
  episode authority and reuses the existing Endpoint rebind. It retains the
  QUIC identity, stream, Target TCP, payload, pool, and old-socket lifecycle;
  writer ACK-stall recovery keeps precedence and generic TCP silence remains
  ineligible.
- Public `m2-qualification` now runs exactly two mixed cycles, validates 8
  phases, 2 DNS/real-client results, unchanged TCP/UDP SLIs, Endpoint/D16
  terminal ownership, and complete rebind lifecycle, and can emit only
  `PASS_NON_ACCEPTANCE`. Root `684+3 ignored`, main `2`, integration `10+4
  ignored`, release, established Clippy, shell, vendored Quinn `40+3 ignored`
  plus doc `1`, quinn-proto `311` plus docs `3`, root docs, fmt/diff/secret,
  and review pass with no unresolved P0/P1. Exact 32 MiB Endpoint capacity was
  `240.403 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-06-knife15-m2-ordered-gap-path-migration-local-results.md`.
- Next pull/rebuild the pushed descendant and take exactly one fresh
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
  m2-qualification -> status -> stop`. It normally takes about 30 minutes.
  Do not run formal `m2` or repeat/tune unchanged. Formal M2 and M3 remain
  blocked pending qualification plus cleanup.

- Previous accepted position: exact-source `0c521fa` artifact
  `/tmp/mini_vpn_knife15_macos_20260806_101142.tar.gz` (SHA-256
  `2a45314d...`) passed baseline `34.964/8.262 Mbit/s`, direct
  `13.539 Mbit/s` without gaps, start/smoke, every qualification preflight,
  all four phases, DNS/real-client, and cleanup. TCP had zero sender/receiver
  intervals and max gap `7,208,960B`; UDP loss was `0.223184%`. Verdict is
  `PASS_NON_ACCEPTANCE`; formal M2 remains `NOT_RUN`.
- No path reset fired. Conn0 advanced black holes `0 -> 12` but its exact
  writer wait was only `274,915us`, below the unchanged `2s` minimum bound;
  conn1 remained at zero. This is the correct healthy comparator and
  false-positive result. The selected failure remains distinct at
  `7,001,335us` writer Pending plus `144` same-connection black holes.
- Endpoint max/final was `61,440B / 61,403/0/0B`, with zero interface errors
  and complete route/DNS/TUN/process cleanup. Two peer `Stopped(0)` writes
  occurred only at timed-transfer close tails with D16
  queued/leased/reserved `0/0/0B`; they explain the summary `REVIEW` and are
  not active-transfer failures. Result:
  `docs/tech/2026-08-06-knife15-m2-connection-local-path-state-recovery-macos-qualification-results.md`.
- Next pull/rebuild the pushed descendant and take exactly one fresh formal
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke -> m2
  -> status -> stop`. Keep every other VPN off, preserve
  `status/snapshot/stop` after failure, and do not rerun qualification or tune.
  Knife15 M2 requires the full schedule plus cleanup; M3 remains blocked.

- Exact-source `bfaba9e` artifact
  `/tmp/mini_vpn_knife15_macos_20260806_083422.tar.gz` (SHA-256
  `74300b2e...`) passed baseline `32.551/58.899 Mbit/s`, direct
  `16.268 Mbit/s` without gaps, start/smoke, every preflight, exact
  `610,402,304B` forward completion, and cleanup. The qualification forward
  had three complete Target receiver-zero intervals around seconds
  101/103/105; the exact writer waited up to `7,001,335us`.
- Owning conn1 advanced PLPMTUD black holes `0 -> 144`, QUIC loss
  `2,147,556B`, and congestion events `537`, while healthy conn0 had no
  comparable growth. Paired Exit observer artifact
  `/tmp/mini_vpn_knife15_exit_target_observer_20260806_065748.tar.gz`
  (SHA-256 `e95880ae...`) had zero capture/kernel drops, Target TCP RTT
  `1..9ms`, no retransmit growth, and full ACKs for Exit-supplied bytes while
  QUIC application supply decayed. This selects client-to-Exit
  per-connection QUIC path-state degradation; do not tune a frozen value.
- Reviewed implementation `0e94e56` owns one path-state reset per live stable
  identity. Exact writer Pending for the existing RTT-derived bound plus
  same-window black-hole growth resets only that connection's configured
  congestion/RTT/MTUD state through the existing proto mechanism. It retains
  the QUIC identity, stream, Target TCP, shared socket, and payload. Exact
  ACK-stall Endpoint rebind retains precedence and cannot overlap the reset.
- Root `679+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, Knife15/Knife14 shell, vendored Quinn `39+3 ignored`
  plus doc `1`, quinn-proto `310` plus docs `3`, root docs, fmt/diff/secret,
  and review pass with no unresolved P0/P1. Exact 32 MiB Endpoint capacity was
  `240.348 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-06-knife15-m2-connection-local-path-state-recovery-local-results.md`.
- Next pull/rebuild the pushed descendant. After the `.33` observer is active,
  take exactly one fresh `m2-ipv6-check -> baseline -> direct-discriminator
  -> start -> smoke -> m2-qualification -> status -> stop`. Do not run formal
  M2. Qualification can produce only `PASS_NON_ACCEPTANCE`; an applied reset
  plus another receiver-zero interval rejects the architecture without
  tuning/repeat. Formal M2 and M3 remain blocked.

- Exact-source `727f00b` artifact
  `/tmp/mini_vpn_knife15_macos_20260806_035626.tar.gz` (SHA-256
  `4013a05b...`) passed baseline `38.747/47.278 Mbit/s`, direct
  `19.361574 Mbit/s` without gaps, start/smoke, every preflight, cycle-1 long
  TCP/reverse TCP/reverse UDP, and cleanup. Its first 10-second short forward
  then had two complete Target receiver-zero intervals; Mac/Target bytes were
  `10,092,544/4,325,376B`.
- The data stream consumed the one-turn startup service and the Exit ACKed
  QUIC data. D16, smoltcp, Endpoint conservation (`61,414/0/0B` final), TUN,
  gateway/Exit controls, routes, process, and cleanup stayed healthy. This
  rejects the startup turn as sufficient and selects the post-QUIC
  Exit-to-Target forwarding seam; do not tune any frozen value.
- Exact-order bare Exit-to-Target control passed all four phases. Sixty fresh
  short connections passed 600/600 receiver intervals with one retransmit.
  These reject a persistent/readily recurring bare path limit but do not
  classify the historical mature-server copy boundary.
- Reviewed `4d02355` adds public `m2-qualification` for one exact
  `300 + 300 + 180 + 10` second cycle and a bounded Exit observer with an
  exact target filter, 96-byte snapshots, 340,000,000-byte ring, 250ms
  TCP_INFO, two-hour timeout, identity-safe cleanup, and version/drop/secret/
  checksum evidence. Qualification success is `PASS_NON_ACCEPTANCE`; formal
  M2 remains `NOT_RUN` and blocked.
- Root `673+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, Knife15/Knife14 shell, vendored Quinn `38+3 ignored`
  plus doc `1`, quinn-proto `310` plus docs `3`, root docs, fmt/diff/secret,
  and review pass with no unresolved P0/P1. Exact 32 MiB Endpoint capacity was
  `237.737 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-06-knife15-exit-target-forwarding-observability-local-results.md`.
- Next pull/rebuild the pushed descendant. After the observer is active on
  `.33`, take exactly one fresh `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> m2-qualification -> status ->
  stop`, then stop/bundle the observer. Do not run formal M2. Matching evidence
  must select Exit path/kernel, mature-server copy service, client QUIC, or
  observer mismatch before another architecture. M3 remains blocked.

- Exact-source `f570353` artifact
  `/tmp/mini_vpn_knife15_macos_20260805_111712.tar.gz` (SHA-256
  `b0d3815e...`) passed baseline `34.319/52.001 Mbit/s`, direct
  `17.151374 Mbit/s` without gaps, start/smoke, every preflight, seven mixed
  cycles, and cleanup. Cycle 8 forward then had a complete initial Target
  receiver-zero interval after the sender had already admitted `2,228,224B`;
  the exact 300-second transfer later completed.
- Control/data streams used installed conn1 generation 2. Same-window
  gateway/Exit, routes, interfaces, process, TUN, D16, Endpoint conservation,
  and recovery remained healthy. The auxiliary-replacement stop rule fired:
  retain its bounded lifecycle protection but reject it as sufficient
  initial-stream service. This is not operator, ambient traffic, outage, TUN,
  pacing, or a frozen-parameter branch.
- Reviewed implementation `f7260ee` arms every TUIC TCP stream at relative
  Quinn priority `original + 1` through Connect and the first accepted
  nonempty business write, then restores the exact original priority
  atomically under the Quinn connection lock. Blocked/empty writes do not
  consume authority. Generic/native/D16 modes share one writer; UDP,
  admission, queues, capacity, timers, and all frozen values are unchanged.
- Root `685+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, Knife15/Knife14 shell, vendored Quinn `38+3 ignored`,
  quinn-proto `310`, docs, fmt/diff/secret, and review gates pass with no
  unresolved P0/P1. The exact 32 MiB Endpoint gate reached `240.076 Mbit/s`
  with final `61,440/0/0B` and zero socket would-block. Result:
  `docs/tech/2026-08-05-knife15-m2-quinn-new-stream-startup-service-local-results.md`.
- Next pull the pushed descendant, rebuild release, keep Clash-TUN/every
  other VPN off, avoid deliberate heavy non-test traffic, and take exactly
  one fresh `m2-ipv6-check -> baseline -> direct-discriminator -> start ->
  smoke -> m2 -> status -> stop`. Normal Apple Push/iCloud may remain.
  Preserve `status/snapshot/stop` after failure. If startup consumption is
  visible but the same installed-successor healthy-control receiver zero
  recurs, reject this architecture and open transport first-payload
  ACK/failover research. M3 remains blocked pending full M2 plus cleanup.

- Exact-source `a1e22ca` artifact
  `/tmp/mini_vpn_knife15_macos_20260805_080607.tar.gz` (SHA-256
  `f5f6d933...`) passed baseline `19.183/50.639 Mbit/s`, direct
  `9.584823 Mbit/s` without gaps, start/smoke, IPv6/full-tunnel/real-client
  gates, the bounded pre-schedule stop, and cleanup. The sole remaining relay
  was `28-courier.push.apple.com:5223`; the operator had closed user Apps.
- That silent Apple Push relay caused fifteen endpoint migrations in about
  fifty seconds. Every action was generic `no_rx`, `udp_active=false`, no
  exact writer pressure, and only `37B` aggregate QUIC TX. This is a recovery
  authority defect plus an observer ownership defect, not operator error,
  path outage, or a frozen-parameter branch. It supersedes the earlier global
  zero/quitting-all-Apps premise.
- Reviewed implementation `1debaff` demand-qualifies generic no-RX with the
  existing UDP application-activity timestamp; exact TCP writer/stream ACK
  stall remains unchanged. The M2 runner replays exact open/install/Closing/
  close lifecycle for its iperf/api.ipify/example.com workload and requires
  only controlled ownership to drain. Ambient leases/relays/fake-IP remain
  numeric observations; Endpoint zero debt/conservation, DNS drops, replay
  validity, all quality/resource/route/cleanup gates, and every frozen value
  remain fail-closed/unchanged.
- Focused recovery `9/9`, root `672+3 ignored`, main `2`, integration `10+4
  ignored`, release, established Clippy lane, Knife15/Knife14 shell, vendored
  Quinn `37+3 ignored` plus doc `1`, quinn-proto `309` plus doc `3`, fmt/diff/
  secret, and review gates pass with no unresolved P0/P1. The exact 32 MiB
  Endpoint gate reached `235.232 Mbit/s` with final `61,440/0/0B`. Result:
  `docs/tech/2026-08-05-knife15-m2-ambient-traffic-recovery-local-results.md`.
- Next pull the pushed descendant, rebuild release, keep Clash-TUN/every other
  VPN off, avoid deliberate heavy non-test traffic, and take exactly one fresh
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke -> m2
  -> status -> stop`. Normal Apple Push/iCloud traffic may remain. Preserve
  `status/snapshot/stop` after failure. M3 remains blocked pending full M2 plus
  cleanup acceptance.

- Previous accepted position: exact-source `798c1a5` artifact
  `/tmp/mini_vpn_knife15_macos_20260805_015011.tar.gz` (SHA-256
  `386ecafc...`) passed baseline `45.914/26.363 Mbit/s`, direct
  `22.908 Mbit/s` with no gaps, start/smoke, IPv6/full-tunnel/real-client
  gates, the complete four-hour steady-a window, 17 mixed cycles plus cycle
  18 forward, and cleanup. Across 154 results it had zero Target receiver
  gaps, `10,354,688B` max TCP gap, `0.604884%` max UDP loss, and Endpoint
  max/final `61,440B / 61,403/0/0B`.
- The auxiliary replacement occurred exactly once: degraded conn1 generation
  1 advanced PLPMTUD black holes `192 -> 193`; authenticated generation 2 was
  installed in `689ms` and served new opens. The predecessor retained one
  existing Google push flow and correctly remained bounded drain-only. No
  installed-successor receiver-zero discriminator occurred, so the data-plane
  architecture is retained.
- M2 failed only at the first 600-second idle checkpoint with four active
  relays and fake-IP `7/19`. Exact targets were Apple/Google push, WeChat, and
  Cursor. The runbook did not require quitting all network Apps, and the old
  runner discovered this global-quiescence prerequisite only after four
  hours. This is not operator command misuse, a path outage, or a data-plane
  regression; do not weaken the idle checkpoint.
- Commit `4ceb2ed` makes formal M2 reuse the existing smoke timeout (normally
  `50s`) before the 86,400s schedule and requires fresh data-plane plus
  Endpoint samples with
  zero leases/relays/fake active/live/outstanding ownership, fake registered
  `1..2`, zero DNS drops, and conservation at or below `61,440B`. Structured
  evidence distinguishes dirty traffic, unhealthy process, and evidence I/O.
  Runner/runbook only; no Rust or frozen value changed. Result:
  `docs/tech/2026-08-05-knife15-m2-full-tunnel-quiescence-fail-fast-local-results.md`.
- Next pull the pushed reviewed descendant, rebuild release, quit Cursor,
  WeChat, browsers, mail/sync/chat/media clients and every VPN, then take one
  fresh `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke
  -> m2 -> status -> stop`. If quiescence fails it must fail before the long
  schedule; preserve `status/snapshot/stop`. M3 remains blocked pending full
  M2 plus cleanup acceptance.

- Exact-source `fd6c34f` xiaoou bundle
  `/tmp/mini_vpn_knife15_macos_20260804_095117.tar.gz` (SHA-256
  `889cdd276c...`) passed baseline `29.369/44.838 Mbit/s`, the 300-second
  direct discriminator at `14.675 Mbit/s` without gaps, start/smoke,
  IPv6/full-tunnel/real-client gates, cycle-1 long TCP/reverse TCP/reverse
  UDP, and cleanup. Its first short forward then had one complete initial
  Target receiver-zero interval.
- Busy-epoch qualification ran correctly: degraded conn1 advanced from zero
  to sixteen PLPMTUD black holes and was excluded; both new opens used
  qualified conn0. Conn0 stayed at zero black holes but already owned fourteen
  lease halves, had `5,140B/175ms` service, and its data writer waited
  `1,169,717us`. This falsifies selector-only isolation and selects bounded
  auxiliary generation replacement. The independent reverse-UDP result was
  `3.391937%`, above the frozen `3%` SLI.
- Runner commit `addc54d` now rejects formal UDP loss above `3%` immediately.
  Implementation `5533d15` owns connection, exact generation activity,
  write-pressure, open/auth state, and drain lifecycle in each logical slot.
  A degraded busy auxiliary predecessor becomes drain-only; one authenticated
  successor becomes the only new-open generation and receives the triggering
  reservation. Existing streams are never replayed, migrated, or reset.
  Primary/UDP/health remain conn0, eligible pool remains two, and bounded
  overlap is at most one predecessor/three live generations.
- Focused `31/31`, root `671+3 ignored`, main `2`, integration `10+4 ignored`,
  release, Clippy, Knife15/Knife14 shell, vendored Quinn/proto, fmt/diff/secret,
  and review gates pass with no unresolved P0/P1. The exact 32 MiB Endpoint
  gate reached `239.967 Mbit/s` with final conservation `61,440/0/0B`. Result:
  `docs/tech/2026-08-04-knife15-m2-auxiliary-generation-replacement-local-results.md`.
- Next take exactly one fresh real-Mac `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> m2 -> status -> stop` from the
  pushed reviewed descendant and rebuilt release. Do not tune or repeat
  unchanged. If an installed successor still has a healthy-control complete
  receiver-zero interval, reject this architecture and open Quinn
  initial-stream scheduling/failover research. M3 remains blocked pending
  complete M2 plus cleanup acceptance.

- Exact-source `99e56b0` xiaoou Ethernet bundle
  `/tmp/mini_vpn_knife15_macos_20260803_093012.tar.gz` (SHA-256
  `804b96c5...`) passed baseline at `34.527/60.550 Mbit/s`, the 300-second
  direct discriminator at `17.264 Mbit/s` with zero sender/receiver gaps,
  start/smoke, IPv6/full-tunnel/real-client gates, cycle-1 long TCP, reverse
  TCP, and reverse UDP. Its first short forward then lost two complete Target
  receiver intervals while Ethernet gateway/Exit controls, TUN, D16, Endpoint
  conservation (`61,412/0/0B`), routes, resources, and cleanup stayed healthy.
- Both failed-phase opens selected strictly less-loaded conn1, so the prior
  equal-busy path-service hypothesis is rejected. Conn1 nevertheless had the
  greater current service (`12,887B/176ms` versus `6,665B/176ms`), waited
  `4,214,880us`, and advanced from zero to ten Quinn PLPMTUD black-hole
  detections during the same nonzero TCP-ownership epoch; conn0 remained at
  zero. The paired Wi-Fi artifact's passing first short used its zero-debt
  conn0 and is comparator evidence only.
- Reviewed implementation `b40aa75` deepens TCP-open admission with exact
  busy-epoch forward qualification. Active zero commits the current
  identity/black-hole anchor only after reservation CAS; same-identity counter
  advance makes a busy slot degraded for new opens. Proven degraded candidates
  are isolated while a qualified/unknown alternative exists; all-degraded
  fallback preserves bounded least-active/path-service/stable ordering.
  Current flows, UDP, reconnect, pool size, MTU, pacing, windows, D16, and all
  frozen values remain unchanged.
- Focused `27/27`, root `678+3 ignored`, main `2`, integration `10+4 ignored`,
  release, Clippy, Knife15/Knife14 shell, vendored Quinn/proto, fmt/diff/secret,
  and review gates pass with no unresolved P0/P1. The 32 MiB Endpoint gate
  reached `240.154 Mbit/s` with final conservation `61,440/0/0B`. The isolated
  observer repair is `3ad7128`. Result:
  `docs/tech/2026-08-03-knife15-m2-busy-epoch-forward-qualification-local-results.md`.
- Next take exactly one fresh HK `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> m2 -> status -> stop` from the
  pushed reviewed descendant with a rebuilt release binary. Keep every other
  VPN off, restore IPv6 only at the runbook boundary, and do not tune or repeat
  unchanged. M3 remains blocked pending complete M2 plus cleanup acceptance.

- Exact-source `6231048` HK directories
  `/tmp/mini_vpn_knife15_macos_baseline_20260803_035615` and
  `/tmp/mini_vpn_knife15_macos_direct_20260803_035733` bind the reviewed
  source/runner/binary and exact baseline hashes. Baseline receivers passed at
  `12.930/49.357 Mbit/s` forward/reverse with no zero intervals.
- The 300-second direct Target discriminator ran at the derived
  `6.462 Mbit/s` but failed seven complete receiver intervals in two
  nonterminal episodes (`62–65s`, `152–156s`). Sender continuity failed at the
  same boundaries; 533 retransmits and cwnd collapse to `1,344B` prove a real
  pre-TUN physical Target-path stall, not a command-tail observer or slow-rate
  failure.
- Do not execute `start`. Restore the physical-service IPv6 immediately using
  the runbook pre-start branch; no status/snapshot/stop is required because no
  TUN/route/DNS/process ownership began. Do not repeat unchanged in the same
  network window. After a materially later or repaired path, take fresh
  `m2-ipv6-check -> baseline -> direct` evidence and continue only on direct
  PASS. No code or frozen value changed; M3 remains blocked. Result:
  `docs/tech/2026-08-03-knife15-hk-m2-direct-continuity-preflight-failure-results.md`.

- Recovered exact-source `6f1df4a` HK bundle
  `/tmp/mini_vpn_knife15_macos_20260801_053127.tar.gz` (SHA-256
  `d4fc3bc6...`) proves the `cebf30d` kernel-route-reap cleanup repair passed:
  stale low/high/fake markers released without foreign route mutation, DNS
  restored, process stopped, cleanup finalized, and secret scan passed.
- Formal M2 remains failed. Cycle 2 `short-forward-1` had a first complete
  `1.001225s` Target receiver interval of `0B`; this was not a partial tail.
  Exact-window Exit/gateway controls, routes/interfaces, TUN, Endpoint
  conservation (`61,404/0/0B`), D16 closure, resources, and authenticated QUIC
  progress stayed healthy.
- Control selected conn1 at `active_before=60`; data then saw an equal busy
  `62:62` tie and stable-index selected conn0. Current stats were conn0
  `10,124B/163ms` versus conn1 `23,842B/163ms`, and the failed conn0 writer
  waited `3,355,211us`, 4.4x the comparable passing conn1 phase. This selects
  equal-lease placement without current path-service evidence, not operator,
  outage, TUN, pacing, MTU, window, chunk, or pool-size tuning.
- The reviewed selector keeps lease count primary and idle `conn0 -> conn1`,
  but breaks equal nonzero ties by exact known `cwnd/RTT`; unknown/equal falls
  back stable. Stats sampling is nonblocking, refreshed after preparation
  waits, and cannot authorize reconnect or alter the payload hot path.
  Focused `22/22`, root `673+3 ignored`, main `2`, integration `10+4
  ignored`, release, Clippy, Knife15/Knife14 shell, vendored Quinn/proto,
  32 MiB >170 Mbit/s capacity, fmt/diff/secret, and review gates pass with no
  unresolved P0/P1. Result:
  `docs/tech/2026-08-02-knife15-m2-path-service-aware-pool-selection-local-results.md`.
- Next take exactly one fresh HK `m2-ipv6-check -> baseline -> direct -> start
  -> smoke -> m2 -> status -> stop` from the pushed reviewed source with a
  rebuilt release binary. Do not tune or repeat unchanged. M3 remains blocked
  pending complete M2 plus cleanup acceptance.

- Exact-source `6f1df4a` real HK M2 run
  `/tmp/mini_vpn_knife15_macos_20260801_053127` passed baseline/direct,
  start/smoke, exact IPv6 preflight, full-tunnel/real-client preflight, and one
  complete mixed cycle. Cycle 2 failed `short-forward-1` with
  `receiver_zero_interval`; preserve this as an independent bundle-review
  failure and do not tune or repeat yet.
- Stop then failed before bundling. Diagnostic
  `/tmp/mini_vpn_knife15_cleanup_diag_20260801_053127.txt` (SHA-256
  `146eb0e1...`) proves `utun4` absent, every route back on exact
  `en0/192.168.133.1`, and current/saved Ethernet DNS both `EMPTY`, while only
  the low/high/fake ownership markers remained one. DNS and Exit-route
  ownership were already zero. This selects kernel route reap plus stale
  runner markers, not Clash, operator, DNS, or data-plane failure.
- Cleanup now deletes only a route still using the owned utun. If that utun is
  absent and the probe exactly matches the recorded physical interface and
  gateway, it records kernel-reap evidence, performs no route mutation, and
  clears the stale marker. Live/reused utun, foreign interface/tunnel, changed
  gateway, missing observation, DNS mismatch, and IPv6 mismatch remain
  fail-closed. Result:
  `docs/tech/2026-08-02-knife15-m2-kernel-route-reap-cleanup-local-results.md`.
- Next pull the pushed cleanup repair on the test Mac and run only repeated
  `stop`, then `bundle`, to recover the existing immutable artifact. Do not
  rerun baseline/M2. Replay the bundle before classifying the independent
  receiver-zero failure. M3 remains blocked.

- Exact-source `19b5ceb` HK bundle
  `/tmp/mini_vpn_knife15_macos_20260801_044032.tar.gz` (SHA-256
  `651f977b...`) followed the documented physical-service
  `IPv6: Automatic -> Off` sequence, passed target-only start/smoke, and then
  hit the same pre-M2 IPv6 error. M2 remained `not_run`; cleanup passed.
- The exact formal probe reproduced locally with
  `route_status=0` plus `route: writing to routing socket: not in table`.
  The old runner recognized that positive absence text only for nonzero
  status, then rejected the status-zero output because it had no `interface:`.
  This selects an observer false negative, not a physical leak or operator
  failure. The first runbook repair also used a different probe from formal
  M2.
- One shared four-state classifier now treats the exact `not in table` line
  with no interface as `safe_absent` regardless of status, accepts only
  `lo0`/`utun*` as `safe_tunnel`, rejects other interfaces as
  `unsafe_physical`, and keeps all other outcomes `unknown`/fail-closed. A
  public read-only `m2-ipv6-check` uses the exact formal probe before baseline.
  Formal M2 persists raw status/text/class/interface and exposes them in
  status/summary.
  No Rust data plane, frozen value, SLO, route scope, or IPv6 non-goal changed.
  Result:
  `docs/tech/2026-08-01-knife15-m2-ipv6-route-status-observability-local-results.md`.
- Next pull the pushed repair on the HK Mac, disable the exact physical
  service IPv6, and run only `m2-ipv6-check`. Do not take a baseline unless it
  reports `safe_absent`/`safe_tunnel` and PASS. Restore IPv6 immediately if
  the cheap check fails. M3 remains blocked pending real M2 acceptance.

- Exact-source `753691a` HK bundle
  `/tmp/mini_vpn_knife15_macos_20260801_041142.tar.gz` (SHA-256
  `93d384a...`) passed target-only start/smoke, then formal M2 reported its
  physical-IPv6 error before any M2-owned route or DNS mutation. The bundle
  did not preserve raw route status/text, so it could not prove that a
  routable physical path existed. M2 status/full-tunnel/acceptance remained
  `not_run/not_run/NOT_RUN`; this was not a smoke or data-plane failure.
- Smoke forward/reverse receivers were `63.428/49.695 Mbit/s`, pool ownership
  drained, Endpoint conservation ended `61,414/0/0B`, and controls/cleanup
  passed. The one `Stopped(0)` close had zero D16 queued, leased, and reserved
  ownership.
- Current M2 is deliberately IPv4-only, so the physical IPv6 gate must remain
  fail-closed. The later exact-probe reproduction selected the runner false
  negative described above; it supersedes the earlier physical-route
  inference without weakening the invariant. Do not reuse rejected evidence
  or expand this stage into IPv6 tunnelling.
  Result:
  `docs/tech/2026-08-01-knife15-hk-m2-ipv6-precondition-results.md`.
- Superseded next action: the repaired public `m2-ipv6-check` must pass before
  taking one fresh uninterrupted HK transaction. Restore immediately on a
  pre-start failure; after `start`, restore only after stop. M3 remains
  blocked pending real M2 acceptance.

- Knife15 M2 local implementation is complete at `4dac87c`. The reviewed
  public `m2` action owns a controlled IPv4 full tunnel/system DNS for an exact
  `86,400s` schedule, with six active windows, five idle/resume boundaries,
  final drain, 93 DNS/real-client cycles, `934` TCP plus `95` UDP results,
  `1,029` phase results, and six lifecycle/resource checkpoints.
- M2 pins the TUIC Exit to the recorded physical gateway, routes both IPv4
  halves and `198.18.0.0/15` through the owned utun, changes only the matching
  physical service DNS, blocks a physical IPv6 route, and proves public egress
  plus real HTTPS through the system resolver/fake-IP path. Cleanup is
  compare-before-remove/restore and formal acceptance remains
  `PENDING_CLEANUP` until `stop`.
- The accepted M1 receiver/TCP-gap/UDP-loss, Endpoint/D16, rebind, pool,
  resource, disk, log, route/DNS, and cleanup gates remain fail-closed.
  Fake-IP checkpoints require zero active ownership and a stable one/two-entry
  registered cache because the frozen `1,800s` TTL exceeds the `600s` drain.
  No Rust data-plane or frozen value changed.
- Root `667+3 ignored`, main `2`, integration `10+4 ignored`, release,
  Clippy, Knife15/Knife14 shell, syntax, fmt/diff/secret, vendored Quinn
  `37+3 ignored` plus doc `1`, quinn-proto `309` plus doc `3`, and review
  gates pass with no unresolved P0/P1. Result:
  `docs/tech/2026-07-30-knife15-m2-24h-real-client-soak-local-results.md`.
- Next take exactly one fresh user-run HK
  `baseline -> direct-discriminator -> start -> smoke -> m2 -> status -> stop`
  from `4dac87c` or a descendant using
  `docs/tech/2026-07-30-knife15-m2-macos-hitl-runbook.md`. Disable Clash-TUN
  and every other VPN before baseline and through stop. On failure preserve
  `status/snapshot/stop`; do not tune or repeat unchanged. M3 remains blocked
  until the real M2 bundle is accepted.

- Exact-source `ee1a423` HK formal M1 bundle
  `/tmp/mini_vpn_knife15_macos_20260729_105127.tar.gz` (SHA-256
  `c263507c...`) completed the exact frozen `28,800s` schedule: five active
  windows, 30 cycles/DNS checks, three idle/resume pairs, final drain, `302`
  TCP plus `30` UDP results, and four clean checkpoints.
- The old `acceptance SLO mismatch` occurred after `m1 complete` and is an
  observer false negative. It treated unchanged lifetime `en0` errors
  (`16 -> 16`) as 1,539 failed samples and rejected two clean relay snapshots
  with `19,456B`/`18,944B` already leased to smoltcp, although each same
  handle proved an equal local-EOF queue, terminal send queue/reap of zero,
  and no reuse before drain.
- The runner now treats the first physical-error row as the run baseline and
  fails on later movement or reset. Its D16 validator follows nonzero clean
  leases across the exact same-handle drain sequence. Open terminal queues,
  queued/reserved ownership, unequal/missing drain, terminal ownership, and
  cross-epoch reuse remain fail-closed. No Rust data plane or frozen value
  changed.
- Immutable-bundle replay reports `formal_m1_acceptance=PASS`: Target receiver
  zero intervals `0`, TCP gap max `10,485,760B`, UDP loss max `2.446087%`,
  Endpoint conservation max/final `61,440B / 61,403/0/0B`, stable resources,
  and clean cleanup. Eighty-eight `Stopped(0)` boundaries and 22 sender-zero
  intervals remain visible `CLASSIFIED_REVIEW` evidence.
- Root `667+3 ignored`, main `2`, integration `10+4 ignored`, release,
  Clippy, Knife15/Knife14 shell, syntax, fmt/diff/secret, real-bundle replay,
  and review gates pass with no unresolved P0/P1. M1 is complete and must not
  be repeated. M2 spec/plan work is unblocked; keep M3 recovery events
  sequenced and single-variable. Result:
  `docs/tech/2026-07-30-knife15-hk-m1-formal-acceptance-results.md`.

- Exact-source `0b43141` HK M1 diagnostic bundle
  `/tmp/mini_vpn_knife15_macos_20260727_085340.tar.gz` (SHA-256
  `0fdfbfd4...`) was operated correctly and ran 7h34m43s before cycle 32
  reverse TCP ended with an incomplete iperf result:
  `control socket has closed unexpectedly`.
- The Exit changed from `3/3`, 0% ICMP loss at `16:28:33Z` to `0/3`, 100%
  loss at `16:29:04Z`, while the gateway stayed `3/3`, 0% loss. The Exit
  remained at 100% loss for 861 consecutive samples through `00:04:07Z`;
  TUIC rebind/reconnect could not recover an unreachable endpoint.
  `utun1024` appeared only about nine hours after the failure, immediately
  before stop, and was not causal.
- Before the outage, 305 completed phase results, 27 DNS results, and all
  three idle/resume checkpoints were valid. The diagnostic ledger was empty;
  maximum UDP loss was `2.127049%`, maximum TCP gap `12,451,840B`, and
  Endpoint checkpoints were `61,403/0/0B`. Conservation, D16, pool idle,
  TUN, resources, and interfaces passed. About 39 minutes remained.
- This is an external Exit VPS/upstream outage and a valid fail-closed
  diagnostic stop, not a product, operator, pacing, or frozen-value tuning
  branch. It is not diagnostic completion or formal M1 acceptance. Do not
  repeat the diagnostic. Once Exit uptime/TUIC stability is confirmed, take
  fresh baseline/direct evidence and run one formal M1. M2/M3 remain blocked.
  Result:
  `docs/tech/2026-07-28-knife15-hk-m1-diagnostic-exit-outage-results.md`.

- Exact-source `5c3cd95` HK bundle
  `/tmp/mini_vpn_knife15_macos_20260727_074752.tar.gz` (SHA-256
  `27f105d8...`) completed only `start -> smoke -> stop` in about 49 seconds.
  The repaired pool-idle barrier reached `active_leases=0`; both TCP
  directions, Endpoint/D16 conservation, controls, resources, routes, secret
  scan, and cleanup passed. No half-closed-idle block occurred.
- M1 diagnostic did not run: status/mode remained `not_run`, with no
  controller, result directory, checkpoint, event, or violation ledger.
  `DNS_TARGET` was disabled in the immutable start-owned state. The paired
  baseline/direct passed at `30.885/52.530 Mbit/s` and `15.412 Mbit/s` with
  zero receiver gaps, but direct was 1,875 seconds old at start versus the
  frozen 900-second bound. A later DNS export cannot repair the current run.
- Accept this as post-`44ff086` real-Mac smoke non-regression only. It does not
  exercise the exact static-ownership timeout branch, complete M1 diagnostic,
  accept M1, or unblock M2/M3. No code or frozen value changed. Result:
  `docs/tech/2026-07-27-knife15-hk-post-repair-smoke-only-results.md`.
- Next take a fresh uninterrupted HK
  `baseline -> direct-discriminator -> start -> smoke -> m1-diagnostic ->
  status -> stop`. Export `DNS_TARGET=8.8.8.8` before start and enter
  `m1-diagnostic` within 900 seconds of direct completion. Do not stop after
  smoke.

- Exact-source `a3ebe42` HK bundle
  `/tmp/mini_vpn_knife15_macos_20260724_105822.tar.gz` (SHA-256
  `b5ce3af4...`) was operated correctly. Both smoke TCP directions and fake-IP
  DNS completed, but the TCP-pool idle barrier failed before M1 diagnostic:
  reverse-data handle 1 epoch 3 retained one native lease and exactly
  `524,288B` queued through four identical 10-second half-close windows.
- Endpoint conservation ended `61,414/0/0B`; three Endpoint rebinds recovered,
  while TUN/pump/interfaces/resources and cleanup passed. The relay still ran
  roughly 19,000 no-progress egress windows because ownership presence alone
  unconditionally re-armed its timer. This is a local bounded-lifecycle defect,
  not operator, path, pacing, or frozen-value tuning evidence.
- Commit `44ff086` publishes monotonic queued-to-leased/permit-release progress
  from the D16 queue. The first owned half-close window is preserved; later
  windows re-arm only after real local progress. Static ownership reaches the
  existing timeout. Root `667+3 ignored`, main `2`, integration `10+4
  ignored`, release, Clippy, shell, vendored Quinn/proto, 32 MiB capacity,
  fmt/diff/secret, and review gates pass with no unresolved P0/P1. No frozen
  value changed. Result:
  `docs/tech/2026-07-26-knife15-hk-smoke-half-close-progress-results.md`.
- Next pull the pushed repair, rebuild release, and take a fresh user-run HK
  `baseline -> direct-discriminator -> start -> smoke -> m1-diagnostic ->
  status -> stop`. Keep every other VPN/TUN off through stop; preserve
  status/snapshot/stop on safety failure. M1 diagnostic remains longitudinal
  evidence only and cannot unblock M2/M3; formal `m1` must still pass.

- M1 diagnostic continuation is locally complete at `b675540`. The new public
  `m1-diagnostic` action runs the unchanged `28,800s` schedule but records and
  continues valid Target receiver-zero, reverse-UDP loss above `3%`, and final
  TCP gap above `16MiB`. Formal `m1` remains fail-fast and is still the only
  M1 acceptance action.
- The typed violation ledger records cycle/phase/value/ranges and source JSON.
  Final summary replays every row and scans all `332` results for missing
  observations. Receiver-zero continuation uses a complete second validator
  that relaxes only the receiver-positive predicate; malformed/missing
  evidence, command/DNS/health/checkpoint/resource/ownership/recovery failures
  remain fail-closed.
- Root `666+3 ignored`, main `2`, integration `10+4 ignored`, release, Clippy,
  Knife15/Knife14 shell, syntax, fmt/diff/secret, and code-review gates pass
  with no unresolved P0/P1. No Rust data-plane or frozen value changed.
  Result:
  `docs/tech/2026-07-24-knife15-m1-diagnostic-continuation-local-results.md`.
- Next take one fresh user-run HK `m1-diagnostic` from the pushed source with
  rebuilt release and fresh baseline/direct. Keep every other VPN/TUN off
  through `stop`; preserve status/snapshot/stop on safety failure. This run is
  longitudinal evidence only and cannot unblock M2/M3. A separate formal `m1`
  must still pass.

- Exact-source `297dee9` HK bundle
  `/tmp/mini_vpn_knife15_macos_20260724_034941.tar.gz` (SHA-256
  `182cc896...`) was operated correctly. Fresh baseline/direct and the new
  smoke pool-idle barrier passed. M1 completed `steady-a`, `idle-1`, `quiet`,
  and `idle-2`, then failed about 3h20m into the run on the first `steady-b`
  forward phase with three complete Target receiver-zero seconds.
- Pool ownership and placement were correct: both checkpoints ended at
  `61,403/0/0B`, and cycle 15 control/data split conn0/conn1 from an idle pool.
  The data writer waited `9,256,932us`; conn1 added `783` lost packets,
  `898,964B` loss, `366` congestion events, and `18` PLPMTUD black holes.
  A direct Exit control lost one of three probes inside the interruption while
  the gateway and local interface remained clean.
- Two reverse-UDP windows also exceeded the frozen `3%` SLO (`3.356208%`,
  `3.408435%`). Each aligned with direct Exit degradation; mini_vpn internal
  UDP drops/backpressure were zero. Endpoint conservation, D16 close
  ownership, resources, and cleanup passed.
- This is a path-attributed M1 failure, not a regression in `297dee9` and not
  permission to tune a threshold, pool, MTU, pacing value, workload, or SLI.
  M2/M3 remain blocked. Do not repeat in the same network window. One fresh
  later-window HK M1 remains allowed; a future healthy-control TCP repeat
  opens connection isolation/failover, and a healthy-control UDP repeat opens
  UDP/TUIC quality architecture. Result:
  `docs/tech/2026-07-24-knife15-m1-hk-path-quality-failure-results.md`.

- Exact-source `f60926e` HK bundle
  `/tmp/mini_vpn_knife15_macos_20260722_110059.tar.gz` (SHA-256
  `9071ec0f...`) was operated correctly. Fresh baseline/direct passed; M1's
  first forward phase then had one complete initial Target receiver-zero
  second despite sustained sender throughput.
- Smoke completed and M1 began in adjacent seconds. Its old conn1 reverse-data
  relay still owned two native half leases; M1 control occupied conn0, the
  resulting `2:2` tie placed M1 data on conn0 too, and the old relay reaped
  only after that open. Three comparable earlier starts drained first, split
  control/data across conn0/conn1, and had positive first intervals.
- The Endpoint monitor now publishes transition-only TCP-pool `active_leases`.
  Smoke waits within its existing hard timeout for exact zero; missing,
  malformed, or nonzero evidence fails closed. Formal M0/M1 independently
  require zero. No fixed sleep, SLI waiver, selector change, or frozen-value
  tuning was added.
- Root `666+3 ignored`, main `2`, Quinn `37+3 ignored + doc 1`, quinn-proto
  `309 + doc 3`, release, Clippy, shell, fmt/diff/secret, and code-review gates
  pass. The 32 MiB gate reached `240.322 Mbit/s` with final Endpoint
  conservation `61,440/0/0B`. No unresolved P0/P1 remains. Result:
  `docs/tech/2026-07-22-knife15-m1-post-smoke-pool-idle-local-results.md`.
- This partial run is not M1 acceptance. Next take one fresh user-run HK M1
  from the pushed repair with rebuilt release and fresh baseline/direct. Keep
  every other VPN/TUN off through `stop`; preserve status/snapshot/stop on
  failure. M2/M3 remain blocked.

- Exact-source `e479013` HK bundle
  `/tmp/mini_vpn_knife15_macos_20260722_094608.tar.gz` (SHA-256
  `7443769e...`) was operated correctly. Fresh baseline/direct passed; M1
  cycle 1 forward then had three complete Target receiver-zero seconds while
  seven `tcp_write_stall` rebinds all reported connection-level recovery.
- The previous Pending-only trigger was over-broad. Repeated source-port
  migration occurred while the owning business writer remained blocked;
  current-socket RX did not prove ACK progress for that stream. TUN, physical
  controls, Endpoint conservation, resources, and cleanup stayed healthy.
- Vendored Quinn/proto now exposes exact-stream acknowledged bytes. TUIC
  permits TCP-write recovery only when both writer Pending age and that
  stream's ACK-stall age reach the unchanged RTT-derived bound. Same-stream
  progress suppresses false rebind; unrelated stream ACKs cannot hide a real
  stall; one rebind covers every sampled writer episode.
- Root `653+3 ignored`, main `2`, Quinn `37+3 ignored + doc 1`, quinn-proto
  `309 + doc 3`, release, Clippy, shell, fmt/diff, secret, and code-review
  gates pass. The 32 MiB gate reached `240.472 Mbit/s`, ending at
  `61,440/0/0B`. No frozen value changed and no unresolved P0/P1 remains.
  Result:
  `docs/tech/2026-07-22-knife15-m1-stream-ack-qualified-rebind-local-results.md`.
- This partial run is not M1 acceptance. Next take one fresh user-run HK M1
  with rebuilt release and fresh baseline/direct. Keep every other VPN/TUN off
  through stop and preserve `status/snapshot/stop` on failure. M2/M3 remain
  blocked.

- Exact-source `b33a3f6` HK M1 bundle
  `/tmp/mini_vpn_knife15_macos_20260722_075828.tar.gz` (SHA-256
  `f8b5b2ea...`) was operated correctly. Fresh baseline/direct, start/smoke,
  and cycle 1 passed; cycle 2 forward failed one complete Target receiver
  interval at `151.001053s -> 152.001047s`.
- The sender paused for most of `149s -> 167s`. Conn1 added `356` lost packets,
  `480,190B` lost bytes, and `112` congestion events; its D16 writer blocked
  for `11,279,769us`. ACK/control RX continued, so the old Endpoint-wide no-RX
  recovery discriminator did not arm.
- TUIC now tracks continuous `poll_write -> Pending` ownership per TCP pool
  connection. At the unchanged RTT-derived bound, it triggers the existing
  socket rebind despite unrelated RX; one rebind covers every pending pool
  episode. Ready/error/drop clears ownership, and existing no-RX plus
  authenticated set-wise current-generation recovery remain intact.
- Root `650+3 ignored`, main `2`, TUIC `100`, Quinn `36+3 ignored + doc 1`,
  quinn-proto `309 + doc 3`, release, Clippy, shell, fmt/diff, secret, and
  code-review gates pass. The 32 MiB gate reached `240.585 Mbit/s` with exact
  conservation. No frozen value changed and no P0/P1 remains. Result:
  `docs/tech/2026-07-22-knife15-m1-tcp-write-stall-rebind-local-results.md`.
- The failed partial run is not M1 acceptance. Next use the pushed repair for
  one fresh user-run HK M1 with rebuilt release and fresh baseline/direct.
  Keep every other VPN/TUN off through stop; preserve `status/snapshot/stop`
  on failure. M2/M3 remain blocked.

- Exact-source `1c587ba` HK M1 bundle
  `/tmp/mini_vpn_knife15_macos_20260721_110225.tar.gz` (SHA-256
  `62280ba0...`) was operated correctly, passed fresh baseline/direct, and ran
  about 6h43m before cycle 28 `steady-c/short-reverse-6` failed with eight
  complete local receiver-zero seconds. This is a real failure, not the
  repaired partial-tail observer class.
- Conn1 business RX paused `8,301ms` while conn0 control RX paused `10,733ms`.
  Endpoint rebind triggered, but vendored Quinn released the previous socket
  after the first pooled connection authenticated on the current socket. TUN,
  pump, Endpoint conservation, routes, network controls, resources, and
  cleanup remained healthy.
- Quinn now snapshots every rebind-time live handle and retains one previous
  socket until each member authenticates on the current generation or drains.
  Old-socket, routed-but-unauthenticated, stale-generation, and post-snapshot
  traffic cannot complete the set. mini_vpn requires every sampled pool
  connection generation before logging recovery. The first implementation's
  unauthenticated-routing P1 was repaired; no unresolved P0/P1 remains.
- Root `646+3 ignored`, main `2`, Quinn `36+3 ignored + doc 1`, quinn-proto
  `309 + doc 3`, release, Clippy, shell, fmt/diff, and secret gates pass. The
  32 MiB capacity gate reached `232.164 Mbit/s` with exact conservation. No
  frozen H10d16 or M1 value changed. Result:
  `docs/tech/2026-07-21-knife15-m1-multi-connection-rebind-retention-local-results.md`.
- The same partial run independently exceeded the reverse-UDP `3%` SLO in two
  windows (`4.421980%`, `4.127822%`). The lifecycle repair does not waive or
  claim to fix that result. Next use the pushed repair for one fresh user-run
  HK M1 with rebuilt release and fresh baseline/direct. Keep other VPN/TUN off
  through stop and preserve `status/snapshot/stop` on failure. M2/M3 remain
  blocked.

- The first real HK M1 bundle
  `/tmp/mini_vpn_knife15_macos_20260720_090636.tar.gz` (SHA-256
  `5fd183b1...`) used exact source `dc8cfb1` and was operated correctly. It
  completed five full mixed cycles; cycle 6 forward then transferred
  `292,945,920B` over the full `300s` command but the runner reported
  `receiver_zero_interval`.
- All `300` complete Target receiver intervals were positive. The only zero
  was the final `0.163918s` iperf command-tail row. Target/Exit routes, TUN,
  Endpoint conservation (`61,440B` max, `61,403/0/0B` final), resources, and
  cleanup were healthy. This selects an observer false negative, not operator,
  Clash, route, or mini_vpn data-plane failure.
- The runner now excludes a zero interval only when numeric timing proves it
  is the final sub-`0.5s` row at the command boundary. Complete, nonterminal,
  missing-timing, malformed, or negative rows still fail closed. Real artifact
  replay and all shell/Rust/release/Clippy/fmt/diff/secret gates pass; review
  has no unresolved P0/P1. Result:
  `docs/tech/2026-07-20-knife15-m1-partial-tail-observer-repair-results.md`.
- The partial M1 is not an eight-hour acceptance. After the repair is pushed,
  the next action is one fresh user-run HK M1 with a rebuilt release and fresh
  baseline/direct artifacts. Keep every other VPN/TUN disabled through stop;
  on failure preserve `status/snapshot/stop`. M2/M3 remain blocked.

- Knife15 M1 local implementation is complete and pushed at `2cca535`. The
  target-only runner now implements the exact `28,800s` mixed schedule, five
  active windows, three idle/resume boundaries, four fresh post-drain
  checkpoints, `302` TCP + `30` UDP + `30` DNS results, hard child deadlines,
  resource/rebind SLOs, and fail-closed cleanup. All local shell, Rust,
  release, Clippy, fmt, diff, and secret gates pass; review has no unresolved
  P0/P1. Result:
  `docs/tech/2026-07-17-knife15-m1-eight-hour-soak-local-results.md`.
- No real M1 TUN run occurred during implementation. The next action is one
  user-run HK macOS M1 using the exact reviewed source and fresh M1 baseline
  and direct artifacts. Clash-TUN and every other VPN/TUN must be completely
  disabled before baseline and remain disabled through Knife15 `stop`. On a
  failure preserve `status/snapshot/stop`; do not tune frozen constants or
  immediately repeat unchanged. M2/M3 remain blocked pending bundle review.
- Knife15 M0 is complete. Independent rearm bundle
  `/tmp/mini_vpn_knife15_macos_20260717_063948.tar.gz` (SHA-256
  `f4e0f649...`) used source `d3f7b13` with the exact accepted main-run binary
  and runner hashes. It created fresh `utun4`, kept Exit on `en1`, passed both
  TCP directions and fake-IP DNS, then cleaned process, TUN, and routes.
- Rearm conservation ended `61,277/0/0B`; interface errors, abandoned bytes,
  stranded ownership, and log compactions were zero. Its one `Stopped(0)` and
  bounded application-first close are the accepted REVIEW classes. No
  unresolved P0/P1 remains. Together with main bundle `...035908.tar.gz`
  (SHA-256 `1ecee823...`), this closes M0. Do not repeat baseline, direct, or
  M0. Next define M1 eight-hour SLOs and a deterministic runner/TDD plan before
  M1 execution. Result:
  `docs/tech/2026-07-17-knife15-hk-m0-rearm-acceptance-results.md`.

- Exact source `8bc7b7c` produced the first accepted two-hour M0 main bundle:
  `/tmp/mini_vpn_knife15_macos_20260717_035908.tar.gz` (SHA-256
  `1ecee823...`). Its fresh direct gate passed at `18.101918 Mbit/s` with zero
  sender/receiver zero intervals. M0 completed eight full mixed cycles, both
  planned 30-second forward bookends, idle/resume, final drain, `74` result
  files, and `8/8` DNS checks with zero phase/health failures and zero
  direction-aware receiver-zero intervals.
- Endpoint conservation held at or below `61,440B` and ended
  `61,403/0/0B`; resource envelopes were bounded, interface errors and log
  compactions were zero, secret scan passed, and stop cleaned process, TUN,
  and routes. The close-tail `REVIEW` contains boundary `Stopped(0)` events
  with zero D16 queue ownership and one pre-M0 smoke local-close release equal
  to the accepted bounded `524288B + 27840B` case. No unresolved P0/P1 remains.
- At that point the M0 gate still required one independent fresh `start -> smoke -> stop`
  rearm bundle. Do not repeat baseline/direct/two-hour M0 for that gate, and do
  not enable another VPN before stop. M1 remains blocked until rearm passes;
  then define its explicit eight-hour SLOs from this M0 envelope. Result:
  `docs/tech/2026-07-17-knife15-hk-m0-main-run-results.md`.

- The HK bundle `/tmp/mini_vpn_knife15_macos_20260717_033923.tar.gz`
  (SHA-256 `a189b848...`) fixed the preceding missing-DNS operation and passed
  forward/reverse TCP plus fake-IP DNS smoke. Its one canonical forward
  `Stopped(0)` at the iperf boundary had zero D16 queued/leased/reserved
  ownership, Endpoint conservation passed, and the reverse flow rearmed.
  This is the already accepted `REVIEW` close-tail class, not an automatic M0
  stop. Do not use a generic `remote_write_failed` grep as a gate. The run was
  stopped before M0 because of that incorrect manual instruction, so a fresh
  300-second direct PASS is required. Keep every other VPN off until Knife15
  `stop` completes; `utun1024` appeared after smoke and contaminated the final
  route sample. Result:
  `docs/tech/2026-07-17-knife15-hk-smoke-stopped0-classification-results.md`.

- The first three post-`fe3ec83` Shenzhen physical baseline attempts failed
  before TUN/mini_vpn execution. `...134708` established iperf control/data
  sockets but produced zero intervals. `...135653` completed at forward
  `2.598 Mbit/s` / reverse `0.098 Mbit/s`, but had two/four receiver-zero
  intervals and 24/50 retransmits. Reverse used the accepted `1KiB` observer;
  three consecutive zeros and one-to-two-segment cwnd prove real physical
  discontinuity, not low speed or quantization. `...140906` then repeated a
  complete forward receiver-zero second at `2.444 Mbit/s` with 31 retransmits
  and cwnd down to `1,388B`, rejecting a tail-only validator bug. Formal
  direct/start/M0 is blocked until a different window or Mac passes the
  unchanged baseline. Do not tune mini_vpn or relax the SLI. Result:
  `docs/tech/2026-07-16-knife15-shenzhen-post-rebind-physical-baseline-results.md`.

- Previous exact source `8e9daf9` bundle
  `/tmp/mini_vpn_knife15_macos_20260716_110535.tar.gz` (SHA-256
  `f9799711...`) was operated correctly. Smoke and M0 cycle 1 passed; cycle 2
  forward failed itself after about `65s`. The user did not interrupt M0.
- Fresh direct physical continuity passed `300s` at `15.716047 Mbit/s` with no
  receiver zero. Conn0 and conn1 then timed out together after their shared
  Endpoint/source-port identity had served about 18 minutes. Reconnect reused
  that identity and timed out, while TUN, pacing, resources, physical controls,
  sing-box, and iperf remained healthy. The selected boundary is endpoint-wide
  UDP receive/service, not operator procedure or a frozen data-plane setting.
- Commit `0460886` adds a bounded endpoint-owned socket rebind before the
  15-second QUIC idle timeout. The `250ms` monitor uses active workload plus TX-
  without-RX and `clamp(8 * max_rtt, 2s, 7s)`, with at most one rebind per
  continuous episode. It preserves the Endpoint, live connections/streams,
  optional adapter accounting, and EndpointWindowV1 conservation.
- After rebind, only a known connection packet received on the current socket
  generation proves recovery. Traffic on Quinn's retained previous socket is
  not sufficient. This P1 review repair is locked in vendored Quinn, policy,
  integration, and runner tests.
- Final root `646 + 3 ignored`, main `2`, Quinn-proto `309 + 3` doc, Quinn
  `29 + 3 ignored + 1` doc, release, check/Clippy, shell, fmt/diff, and review
  gates pass with no unresolved P0/P1. All frozen values and strict SLI remain.
  Next use a fresh Shenzhen build/baseline/direct/start/smoke/M0. A failed
  recovery rejects the architecture; it does not authorize tuning. Results:
  `docs/tech/2026-07-16-knife15-macos-m0-cycle2-endpoint-timeout-results.md`
  and
  `docs/tech/2026-07-16-knife15-endpoint-socket-rebind-recovery-local-results.md`.

- Previous exact source `c50613c` clean rearm bundle
  `/tmp/mini_vpn_knife15_macos_20260716_100056.tar.gz` (SHA-256
  `baee8f2f...`) was operated correctly. The target-only route remained on
  `utun5`, Exit remained `en0`, and Shenzhen remained the default public
  egress as designed.
- The smoke hang is a deterministic local close-lifecycle defect. D16 queue
  closure queued a local FIN under `remote_eof`, then a later same-epoch
  `remote_read_failed` event immediately aborted/rearmed the smoltcp socket
  before `iface.poll + flush_tx`. The local iperf client therefore saw neither
  FIN nor RST and waited forever. `.33` captured bidirectional UDP on the fresh
  source port, rejecting an old-five-tuple permanent blackhole; the path can
  still be lossy and is not yet accepted as stable.
- Commit `9f68435` coalesces the second terminal event into the existing
  bounded close and preserves the first non-clean cause. A full in-memory TUN
  + dual-smoltcp RED/GREEN test locks local failure propagation. The macOS
  smoke runner hard-bounds each iperf at `duration+30s`, preserves output, and
  returns `124` on timeout. Full Rust, release, Clippy, shell, fmt/diff, and
  review gates pass with no unresolved P0/P1; frozen H10d16 settings and SLI
  are unchanged.
- Next get `9f68435` onto Shenzhen, rebuild release, then take a fresh baseline
  and 300-second direct discriminator because source/runner/binary changed.
  A direct PASS permits one bounded start/smoke; smoke PASS permits M0. A
  failure must preserve status/snapshot/stop evidence and cannot authorize
  tuning. Result:
  `docs/tech/2026-07-16-knife15-macos-smoke-close-lifecycle-results.md`.

- Exact copied Shenzhen archive `...154130.tar.gz` (SHA-256 `49c4b71a...`)
  proves the 16KiB observer reached reverse iperf. Physical forward was
  `31.798980 Mbit/s` with all Target intervals positive and `9,420` sender
  retransmits. Physical reverse was `0.131071 Mbit/s`, five receiver zeros, 45
  retransmits, `169-184ms` RTT, and max cwnd `8,328B`. mini_vpn/TUN was absent:
  slow/loss are environment profile, not product bugs.
- Positive reverse intervals remained 16KiB multiples while useful delivery
  was about 16KiB/s. Commit `e49d83c` uses 1KiB only for reverse TCP baseline/
  M0 and prints direction-aware baseline rates/zero counts before verdict.
  Forward/direct/short-forward, UDP1160, rates/durations, strict SLI, and all
  frozen mini_vpn constants remain unchanged. Shell/fmt/diff/review gates pass;
  no P0/P1 remains.
- Next run fresh build and Shenzhen physical-`en0` baseline. Slow values have
  no failure threshold. Positive 1KiB intervals permit direct/M0; remaining
  zeros block formal M0 as physical continuity evidence, not a mini_vpn bug.
  Do not tune data-plane constants. Result:
  `docs/tech/2026-07-15-knife15-macos-low-rate-reverse-observer-results.md`.

- Source `5c127eb` bundle
  `/tmp/mini_vpn_knife15_macos_20260715_104417.tar.gz` (SHA-256
  `5743b352...`) is exact and correctly operated. Its fresh 300-second direct
  prerequisite passed all Target receiver intervals at `6.626752 Mbit/s`.
  Sustained forward/reverse and reverse UDP passed; first short forward failed
  with two initial Target receiver-zero seconds and `3,670,016/10,616,832B`.
- The accepted root is global TCP pool parity: the one-TCP-control UDP phase
  shifted the cursor, inverting the next control/data pair from conn0/conn1 to
  conn1/conn0. Conn0 data waited `3.864382s`; endpoint conservation, TUN/pump,
  resources, same-window network controls, server Connect timing, and
  connection liveness remained clean. Do not blame operator procedure or tune
  frozen settings.
- Commit `c945a41` replaces the cursor with least-active atomic reservation,
  stable primary-first ties, and a per-slot RAII preparation gate through
  mutex/probe/reconnect/clone. Review found and repaired a same-slot overtaking
  P1 in the initial counter-only design. Focused pool `15/15`, all-target
  library `640 + 3 ignored`, main `2/2`, release, Clippy, Knife15/Knife14 shell,
  fmt, and diff gates pass; no unresolved P0/P1 remains.
- Next user-run sequence is fresh release build, baseline, and 300-second direct
  discriminator, then start/smoke/M0 only on direct PASS. M0 must show short
  control/data conn0/conn1 and no receiver-zero interval. Correct placement plus
  another failure selects the flow-specific QUIC/path architecture branch and
  forbids tuning/repeat. A passing M0 requires independent rearm before M1.
  Result:
  `docs/tech/2026-07-15-knife15-macos-m0-directpass-pool-parity-results.md`.

- Exact source `5e8846f` passed the repaired physical-observer short gate in
  bundle `/tmp/mini_vpn_knife15_macos_20260715_084113.tar.gz` (SHA-256
  `85dd7a66...`). All `5/5` physical rows were semantically numeric with zero
  interface errors, `185,341,406/106,914,245B` RX/TX deltas, meaningful rates,
  complete zero-loss Exit/gateway controls, and clean process/utun/route/
  ownership teardown. The generic REVIEW is one forward `Stopped(0)` close
  tail repeated under three log labels, not three failures.
- Follow-up commit `2c8030a` counts the canonical D16 terminal relay once in
  `remote_write_failures` and preserves the three raw/derived diagnostic lines
  separately. Knife15 and Knife14 shell gates pass. It changes summary
  semantics only, so the accepted observer gate remains valid.
- This closes only the repaired-observer prerequisite; it is not formal M0.
  Take a fresh physical-route baseline on the dedicated Shenzhen Mac, run the
  formal M0, then run an independent start/smoke/stop rearm. M0 acceptance and
  M1 remain blocked until those gates pass.
- Exact source `b0fcb76` repeated a genuine receiver continuity failure in
  cycle 1 forward: five complete Target receiver zero-byte seconds plus one
  short tail row, `208,142,336B` over `300.175s`, and only `5.547 Mbit/s`
  from a `14.425 Mbit/s` offer. The user sequence and baseline provenance were
  correct; delayed final sudo input cannot cause an earlier completed phase to
  fail.
- Internal ownership, TUN/pump, lifecycle, and resources remained clean. The
  failure aligned with bulk QUIC loss/congestion, cwnd contraction, and a
  `10.05s` writer wait. Exit/gateway ICMP controls were complete and broadly
  stable around the zero seconds, but cannot isolate one-second or flow-
  specific path events.
- The new network-v2 observer had a separate false PASS: a physical BSD
  `netstat` Link row included an Address field absent from the utun fixture,
  shifting every counter while preserving 27 CSV columns. Commit `524139b`
  parses both row shapes and requires semantic numeric validity before
  physical samples, errors, rates, or freshness can pass. Focused shell TDD,
  root `635+3 ignored`, main `2`, release, Knife14 shell, fmt/diff, and review
  gates pass with no unresolved P0/P1.
- The independent rearm lifecycle passed and cleaned process, utun, routes,
  and ownership, but its physical controls share the old observer defect.
  The later exact-source short validation above closes that observer defect.
  Result:
  `docs/tech/2026-07-15-knife15-macos-m0-physical-counter-observer-repair-results.md`.

- Exact source `b5c3963` completed two full macOS M0 mixed cycles, then cycle
  3 forward contained four genuine Target receiver zero-byte seconds. The
  direction-aware SLI correctly failed; this was not user operation or the
  earlier sender-versus-receiver evidence defect.
- Endpoint ownership, TUN/pump service, D16 lifecycle, and resources remained
  clean. The failure window instead had QUIC loss/congestion growth and cwnd
  contraction to `25,174B` before recovery. Do not tune frozen data-plane or
  workload constants from this result.
- Exact external-path versus tunnel-only attribution was not possible because
  the old `network.csv` recorded routes but omitted the plan's same-window
  controls. The runner now collects direct Exit and physical-gateway RTT/loss,
  physical-interface counters/rates, raw network logs, and fail-closed
  completeness/freshness correspondence.
- The independent stop/re-create/smoke/stop rearm passed with zero final
  ownership and clean route/TUN cleanup. Local Rust, release, shell, fmt/diff,
  and review gates pass with no unresolved P0/P1.
- M0 remains failed and blocks M1. The next formal M0 belongs on the dedicated
  Shenzhen Mac after a fresh physical-route baseline. If continuity fails,
  correlate the same seconds across Exit/gateway controls, physical counters,
  and QUIC deltas. Result:
  `docs/tech/2026-07-15-knife15-macos-m0-network-control-discriminator-results.md`.

Current Knife14 summary, as of 2026-07-14:

- Knife14 H10d16 Task 12 step 4 is complete. Exact repair source `5f9da90`
  passed the capable Shoes/Quinn `.111:8443` target-only reverse P8 at
  `188 Mbit/s` receiver, `60/60` nonzero intervals, and `149 Mbit/s` minimum.
  All eight flows exceeded one D16 quantum.
- The P8 had TUN drops `0/0`, pump `129/500` with zero full waits/read errors,
  zero formal QUIC loss/congestion/blocking, zero terminal pending/reap, and
  exact endpoint conservation. The largest observed conservation sample was
  `61,406 <= 61,440B`.
- Do not use `.33` reverse throughput as an H10d16 architecture discriminator.
  Its external sing-box/quic-go reverse sender is independently limited to
  low single-digit throughput on this topology; the capable Shoes/Quinn Exit
  is the accepted reverse gate.
- Corrected UDP payload `1160B` stayed within TUN MTU1200. The required
  reverse/live-streaming direction delivered `90 Mbit/s` with `0/290,950`
  loss and zero mini_vpn/TUN/client-QUIC window loss. Forward delivered
  `83.7 Mbit/s` from a `90 Mbit/s` offer with `7%` application loss but no
  mini_vpn/TUN/within-window client QUIC drop. Exclude the prior `1200B`
  payload because its `1228B` IP packet forced fragmentation.
- Linux fake-IP DNS and TUN rearm passed in two independent process cycles:
  arbitrary resolver queries returned `198.18.0.2`, metrics reported
  `DNS forge=1/drop=0`, both stops removed TUN/routes, and a fresh create
  succeeded.
- Post-VPS gates passed: root `632+3 ignored`, harness `643+3 ignored`,
  integration `10+4 ignored`, quinn-proto `309+3`, Quinn `29+1`, smoltcp
  `290+3`, all-target check, fmt, shell self-tests, and diff checks. No
  unresolved P0/P1 remains.
- Cleanup is complete. `.27` has no client/TUN/test route. `.111` has no
  Shoes service, restore timer/service, UDP8443 listener, transient binary,
  or runtime directory; all four socket buffers are restored to `212992`.
  No macOS TUN ran.
- Accepted chain: `c55737e -> d934f12 -> 20a0f8c -> e201403 -> 5f9da90`.
  Gate A/B remain accepted. Do not reopen bounded sender, cap64, GSO-only,
  D3 self-wake, or frozen-parameter tuning. Any later work needs a new
  explicitly scoped stage. Result:
  `docs/tech/2026-07-14-knife14h10d16-endpoint-pacing-service-vps-completion-results.md`.

Earlier Knife15 plan and qualification context, as of 2026-07-14:

- The formal macOS M0 controller is locally complete. It derives `50%`
  sustained and `80%` burst rates from a fresh same-target direct baseline,
  preserves UDP `1160B`, and fixes the formal timeline at `6,780s` active +
  `300s` idle + `120s` final drain.
- The controller validates every iperf interval, byte/loss evidence, and fake-
  IP DNS answer; identity-tracks traffic/idle/drain children; checks process/
  target/Exit/lossless-log health throughout the timeline; and stops workload
  generation before cleanup on failure. Summary exposes M0 event/result/
  timeline status, resource envelopes, utun deltas, conservation max, and
  final live/outstanding ownership.
- Local shell TDD passes; no new real TUN or two-hour run occurred. The next
  action is user-executed formal M0 followed by fresh create/smoke/stop rearm.
  Result:
  `docs/tech/2026-07-14-knife15-macos-m0-controller-local-gate-results.md`.

- The first HK user-executed target-only qualification passed on source
  `2a85fd4`: direct receiver `9.045/26.790 Mbit/s`, TUN receiver
  `31.444/48.490 Mbit/s`, all `20/20` nonzero. Endpoint conservation stayed
  `<=61,440B`, pump high was `317/500` with zero waits/errors, macOS interface
  errors were zero, DNS passed, and PID/utun/routes cleaned up.
- One forward iperf-tail `Stopped(0)` remains `REVIEW`, but D16
  queue/lease/reservation closed at zero and the slot rearmed for reverse; it
  is not a leak. M0 must keep idle-drain and byte-gap checks.
- Real evidence drove TDD fixes for macOS-awk summary counts, remote-write
  classification, and high-rate permit-release logging. Reporting/log density
  changed; no frozen data-plane constant changed. The short qualification
  authorizes user-run target-only M0 on HK or Shenzhen, but 2-hour M0 has not
  run. Result:
  `docs/tech/2026-07-14-knife15-macos-hitl-short-qualification-results.md`.

- The next stage is long-duration release readiness, not another pacing or
  peak-throughput tuning stage. The capable Linux/VPS topology remains the
  H10d16 architecture and peak-capacity reference.
- macOS is the real-client utun, resource-stability, mixed TCP/UDP/DNS,
  idle/resume, and recovery lane. HK may run the first user-controlled
  target-only qualification; the dedicated Shenzhen machine remains the
  preferred long-duration host. Neither cross-region path is an absolute
  `170/200 Mbit/s` discriminator.
- The H10d16-aware target-only HITL runner and shell fixture are implemented at
  `scripts/knife15-macos-soak.sh` and
  `scripts/knife15-macos-soak-self-test.sh`. They provide provenance,
  Exit-route recursion protection, owned-route cleanup, watchdog, bounded
  logs, process/utun/network/event collection, secret scans, and BSD-compatible
  self-tests. Historical macOS full-route scripts are not accepted unchanged.
- Stage gates are `2h -> 8h -> 24h`, then one-variable Wi-Fi, sleep/wake,
  path-change, client-restart, and authorized Exit-restart recovery windows.
  HK TUN is authorized only through the reviewed HITL runner and only when the
  user explicitly executes every `sudo` command. Agent-started TUN remains
  prohibited. Current read-only preflight sees `utun1024`; exit that existing
  VPN/proxy before a qualification run.
- Keep every accepted H10d16/EndpointWindowV1/MTU/pool/window/chunk/Cubic/GSO/
  queue/driver/self-wake decision frozen. Plan:
  `docs/tech/2026-07-14-knife15-long-duration-release-readiness-plan.md`.
  User runbook:
  `docs/tech/2026-07-14-knife15-macos-hitl-m0-runbook.md`.

Previous Knife14 summary, as of 2026-07-13:

- The first fresh frozen reverse P8 from `55792b3` failed at
  `0.103 Mbit/s`. All eight data relays opened, but each accepted only about
  one `128 KiB` D16 quantum. TUN drops were `0/0`, pump high was `177/500`
  with zero waits/errors, smoltcp/pending stayed bounded, QUIC loss was zero,
  endpoint accounting was exact, and the guard completed `6/6` pause/resume
  edges. Final phases were `Running=0`, `DrainOnly=8`, `Recovery=1`.
- The selected root is lost per-flow ACK-completion evidence. A zero send-queue
  snapshot cleared the ACK barrier while hard pressure/debt still suppressed
  Recovery; after pressure cleared, the already-zero queue could not produce a
  new cycle-local drain event.
- Repair commit `5f9da90f734b1754fd8c41bcb70fa4c8b6ae9f74` retains the
  barrier through hard pressure, drop debt, and terminal no-send, then consumes
  the proven nonzero-to-zero history on the first clean eligible snapshot.
  Focused `2/2`, D16 `60/60`, root `632+3 ignored`, harness `10+4 ignored`,
  concurrency `64/256/1024`, UDP four-size `500/500`, fmt/diff/shell checks,
  and controlled all-target Clippy pass. No P0/P1 remains.
- Next run one exact-source, target-only, reverse-only fresh P8 after
  secret-free hash verification and rehearsal. VPS and commits are authorized;
  macOS TUN is prohibited. Keep every frozen input unchanged and require
  `>170 Mbit/s`, `60/60`, zero drops, pump below `500`, all eight flows beyond
  one quantum, no eligible DrainOnly strand, exact ownership, and cleanup. A
  failure is architecture failure, not permission to tune. Results:
  `docs/tech/2026-07-13-knife14h10d16-{reverse-p8-failure-results,ack-barrier-recovery-local-gate-results}.md`.
- Task 12 step 4's accepted H10d16 chain is EndpointPacingService `c55737e`,
  batch relay `d934f12`, bounded TUN ingress `20a0f8c`, and local TCP
  receive-credit service `e20140340f7f949fa8bad9e960ce94451d2c0229`.
- The accepted repair preserves `1 MiB` physical smoltcp RX/TX storage but
  bounds H10d16 advertised and accepted receive credit at `368,640B`.
  Advertisement is `min(storage, limit) - queued`, and segment acceptability
  retains the fixed `application_consumed_seq + min(storage, limit)` right
  edge. Default and non-H10 sockets retain exact upstream behavior.
- Its exact local 32 MiB real-Quinn gate passed at `302.246 Mbit/s`, exact
  bytes and clean EOF, zero modeled drops, ring/pump `15/500`, zero full
  waits/read errors, and `recv_queue_max=39,440B`. Vendored smoltcp passed
  `290+3 docs`; quinn-proto `309+3 docs`; Quinn `29+1 doc`; root/harness
  `630/641` nonignored; integration `10/10`; explicit `64/256/1024` and the
  four-size zero-loss UDP sweep passed. No unresolved P0/P1 remains.
- The single frozen target-only forward VPS P1 is **PASS**: receiver
  `191 Mbit/s`, `20/20` intervals, tail average/minimum `201.333/192 Mbit/s`,
  TUN drops `0/0`, and aggregate QUIC lost-byte delta
  `11,459,701B <= 16 MiB`.
- The reachability proof matched the real path. `recv_queue_max` was exactly
  `368,640B`; `ceil(368,640/1,160)=318`, and the pump reached exactly
  `318/500`, with zero full waits/read errors. `623,222` TCP packets became
  `4,022/4,022/4,022/4,022` batch/relay/poll/flush services, avoiding
  `619,200` repeated calls.
- Endpoint final state was `available=61,403B`, `live=0`, `outstanding=0`;
  `500,763,062B granted - 59,480B refunded = 500,703,582B sent`. There was no
  abandoned byte, pool reconnect, pacing migration/leak, flow-control block,
  terminal pending/reap, TUN flush failure, residual client, or residual TUN
  route.
- The sanitized evidence bundle is under
  `/private/tmp/mini_vpn_local_uplink_window_e201403/`, SHA-256
  `769dadd36d1b6db8ba0ff4aad3bd7efc16699b5cc01530932e81cdfb018f18d4`.
  Keep H10d16, EndpointWindowV1 constants, MTU, kernel/FIFO/batch capacities,
  pool, QUIC windows, chunk, Cubic, GSO default, Quinn sender/driver bound, and
  self-wake frozen. Do not reopen bounded sender, cap64, GSO-only, or parameter
  tuning. The forward P1 blocker is closed; resume at the repaired fresh
  reverse P8 gate, then UDP/live-streaming, Linux fake-IP DNS, and TUN
  stop/rearm. No macOS TUN ran. Source: the 2026-07-13 local-uplink-window
  service architecture, implementation, local-gate, and VPS-results documents.

Earlier endpoint-preparation summary from the same date:

- EndpointPacingService implementation and the full local Task 12 gate pass.
  The pinned quinn-proto service owns pre-build planned bytes, actual GSO
  settlement, socket-blocked outstanding bytes, two-connection DRR/control,
  deadline/waker composition, and lifecycle cleanup; pinned Quinn supplies
  only the current-waker and real socket-outcome adapter.
- The fixed candidate remains `30,720,000B/s`, `61,440B` burst, `10,240B`
  control reserve, and `20,480B` quantum. The exact GSO-enabled `32 MiB` local
  gate reached `240.466 Mbit/s`, exact delivery, clean EOF, and final zero
  live/outstanding/record leaks. `EndpointWindowV1` is default-off and
  `QuinnDefault` remains production default.
- Final gates: quinn-proto `309/309 + 3/3` docs; Quinn `29/29` nonignored plus
  `1/1` doc; mini_vpn `622/622` nonignored; explicit `64/256/1024`; UDP sweep;
  harness check; root/focused-vendor format; runner shell/self-test; diff
  checks. Code review fixed fail-open behavior on an impossible migration-key
  collision by retaining the old paced adapter; no P0/P1 remains.
- Commit and one VPS endpoint-window-v1 forward acceptance are authorized.
  macOS TUN is prohibited. Keep D16, MTU/PLPMTUD, pool=2, QUIC windows,
  `64 KiB` chunk, Cubic, GSO enabled, the 20-datagram driver bound, and
  self-wake frozen. A `<=170 Mbit/s` discriminator is architecture failure;
  do not tune constants. Local result:
  `docs/tech/2026-07-13-knife14h10d16-endpoint-pacing-service-local-gate-results.md`.

The bullets below preserve earlier stage history and are superseded where
they describe implementation or authorization as pending.

- The confirmed post-cap64 design-preparation stage is complete. A fake-time
  `RTT=200us`, `cwnd=40000B`, `MTU=1280` replay proved that the stored cap64
  Pacer can issue exactly `259` datagrams inside `1ms`; the default vendored
  test now preserves this as a mechanism characterization rather than a
  known-negative gate.
- The proposed successor is an endpoint-owned pre-accounting byte service at
  `30.72 MB/s` with a `61,440B` burst, giving formal connection-datagram
  bounds of `92,160B/1ms` and `368,640B/10ms` while retaining about
  `239.167 Mbit/s` measured application capacity. Its proof includes live
  reservations and socket-blocked outstanding bytes, two-connection DRR,
  control reserve, idle borrowing, combined deadlines/wakers, and
  cancel/migration cleanup. Spec and plan:
  `docs/tech/2026-07-13-knife14h10d16-endpoint-pacing-service-{architecture-spec,implementation-plan}.md`.
- No coordinator implementation, VPS, macOS TUN, commit, or notification ran
  in that preparation stage. Vendored Quinn-proto passed `276/276` plus
  `3/3` doc tests; root fmt and tracked/untracked diff-checks passed. Task 12
  step 4 remains stopped before P8. The
  next possible action is local TDD implementation only after explicit review
  and confirmation of the new spec/plan; a later VPS would still require a
  separate explicit authorization.
- Gate A and Gate B remain accepted, but Task 12 step 4 is stopped before P8.
  The one allowed same-window pacer-cap64 forward discriminator ran: sing-box
  control passed at `191.928 Mbit/s` receiver with zero socket drops; mini_vpn
  reached `185 Mbit/s` with `20/20` intervals but added `29` TUN TX drops and
  `50,621,275B` formal QUIC loss.
- cap64 was reachable and attributable: the data connection carried
  `99.9998%` of TX bytes and one snapshot reduced `327680B` to
  `81920B = 64*1280`; there was no migration, reconnect, blocking, or extreme
  cwnd. It nevertheless peaked at `267 packets/1ms` and `1337/10ms`, no better
  than the prior Quinn-default failure class.
- Code review rejects stored-token capacity as a temporal burst bound. Quinn's
  preserved `1.25*cwnd/rtt` refill can replenish multiple buckets inside 1ms
  on a sub-ms path. Do not retry cap values, bounded socket cooldown, GSO-only,
  or frozen-parameter tuning. The next possible stage is a deterministic
  sub-ms refill replay plus a new pre-accounting endpoint time-window/service
  architecture spec, and it requires explicit confirmation before code or
  VPS work. Result:
  `docs/tech/2026-07-13-knife14h10d16-pacer-cap64-forward-discriminator-results.md`.

Earlier Knife14 summary, as of 2026-07-08:

- Knife14fp changed the server-side preflight: exit-side Linux socket buffers
  are mandatory for `100+ Mbit/s` TUIC acceptance. With the old `.33` defaults
  (`212992B` caps/defaults), both mini_vpn and a mature sing-box client stayed
  in the `20-30 Mbit/s` band. Raising `.33` to
  `rmem_max/wmem_max=16777216` and
  `rmem_default/wmem_default=1048576`, then restarting sing-box, moved the
  mature sing-box client to `185.242 Mbit/s` receiver.
- The `.33` setting is now persisted in
  `/etc/sysctl.d/99-mini-vpn-quic.conf`. Future VPS acceptance must check
  `net.core.rmem_max`, `net.core.wmem_max`, `net.core.rmem_default`, and
  `net.core.wmem_default` before blaming mini_vpn credit, QUIC MTU, pool size,
  sing-box version, or TUIC single-stream behavior. The production install
  guidance is `docs/tech/2026-07-08-vps-install-and-optimization-guide.md`.
- Do not lower the target to `30 Mbit/s`; keep `100+ Mbit/s` as the target.
  However, Knife14fq proved the final 1% is not closed yet: a clean
  `f8765c1` repeat with `IPERF_TIMEOUT_SECS=120` exited normally and cleaned
  close-tail accounting (`pending_at_close=0`, `terminal_pending_reap=0`,
  `tun_tx_dropped_delta=0`, QUIC loss/blocking `0`), but throughput regressed
  to `35.7 Mbit/s` receiver with local pressure/headroom gating. The earlier
  Knife14fp mini_vpn `114 Mbit/s` run was high-throughput evidence but exited
  by timeout and is not sufficient final acceptance by itself.
- Next Knife14 work must be a focused repeat/A-B and then a local pressure-edge
  fix if the low result repeats. Do not restart stale pool, iperf3, sing-box
  liveness/auth, QUIC MTU/PLPMTUD, receive-window shrink, or broad pool work
  unless new evidence contradicts the Knife14fp/fq bundle pair.

Historical Knife14 summary, as of 2026-07-04:

- Stale TUIC TCP pool slot diagnosis is closed. The accepted fix was
  `7c683b0` plus acceptance record `afb18f5`, with the key signal
  `tuic-tcp-pool-reconnect conn=1 reason=stale_tcp_pool_slot` and no later
  stale-slot timeout.
- Knife14ar code commit `3baf476` made downlink egress pacing close-safe by
  default: product/default egress immediate budget is no longer the rejected
  `65536`, non-empty pending backlog forces immediate flush, and close logs
  include `tun_flush_deferred`.
- Knife14ar result commit `ba9225f` recorded the failed VPS acceptance. The
  clean reverse-first window had healthy direct baselines, no clean-window QUIC
  loss/congestion, `tun_tx_dropped_delta=0`, `tun_flush_deferred=0`,
  `send_slice_zero=0`, `send_slice_errors=0`, and `tun_flush_failures=0`, but
  reverse throughput was still low (`17.2/16.2 Mbit/s`) and later closed with
  `dead_slot_reap ... pending=224765 ... tcp_state=Closed ... can_send=false`.
- Therefore the next root is not iperf3, sing-box, stale pool slots, blunt
  egress pacing, clean-window QUIC loss/congestion, TUN syscall failure, or
  clean-window TUN qdisc drops. The remaining branch is mini_vpn local TCP
  downlink lifecycle / receive-window / close-drain / terminal pending
  accounting.

Next stage bias:

- Build the Knife15 macOS evidence loop before asking the Shenzhen machine for
  a long run: safe runner, provenance, collectors, event markers, parser, shell
  self-tests, and fail-closed cleanup.
- Use `2h` to establish warm resource envelopes and recovery SLOs, then admit
  `8h` and `24h` only when the previous gate and cleanup pass.
- Diagnose conservation, ownership, TUN, lifecycle, or resource-slope failures
  at a deterministic seam before repair. Do not convert cross-region path
  bandwidth into a frozen-constant tuning request.
- Keep the Linux/VPS capable-peer lane as the independent capacity reference
  and compare invariant classes rather than identical Mbps across platforms.

## Stage Learning Memory

Use the `self-improving-agent` pattern for project-local memory:

- At the end of each stage, summarize what happened, the outcome, what worked,
  what failed or was rejected, the likely root cause, and the next reusable rule.
- Include relevant commit IDs, log archive paths, scripts, and acceptance
  signals when they matter.
- Keep `.learnings/LEARNINGS.md` for mini_vpn-specific stage lessons.
- Keep `.learnings/ERRORS.md` for failures that should change future debugging,
  testing, or implementation behavior.
- Do not put secrets, TUIC passwords, private keys, or one-off noisy logs into
  learning memory.
- Promote a learning into this `AGENTS.md` file or `docs/tech/` only when it
  becomes stable project guidance rather than a single-stage observation.

## Test Strategy

- Use local deterministic tests and harnesses for code-logic bugs.
- Use VPS integration suites for real timing, pressure, TUIC/QUIC behavior,
  sing-box interoperability, MTU/RTT/path effects, and service-state issues.
- Before asking the user to run a VPS suite, include a concrete one-shot test
  checklist and preflight service checks for the relevant VPS hosts, especially
  `.33` and `.77` when those hosts are part of the test.
- If a VPS service looks unhealthy, tell the user to inspect or restart it
  before running the expensive test.

## VPS Acceptance Test Access

Current acceptance hosts:

- Client VPS `.27` (`43.172.75.27`): run mini_vpn from
  `/home/ubuntu/mini_vpn`.
- Exit VPS `.33` (`43.153.32.33`): sing-box/TUIC host. Check service with
  `sudo systemctl status sing-box`; log file is `/var/log/sing-box.log`.
- Target VPS `.77` (`43.130.32.77`): iperf3 host. Check service with
  `systemctl status iperf3`; inspect logs with `journalctl -u iperf3`.
- Independent Exit VPS `.111` (`43.173.101.111`): authorized for direct agent
  access and Knife14 TUIC capability/Gate A work. UDP `8443` is externally
  allowed.

SSH access:

- From the Mac mini, use `ssh -i ~/.ssh/vpn ubuntu@<host>` for `.27`, `.33`,
  `.77`, and `.111`.
- From `.27`, use `ssh -i ~/.ssh/vpn ubuntu@43.153.32.33` and
  `ssh -i ~/.ssh/vpn ubuntu@43.130.32.77`. The same key may be used for
  `.111` after reachability is verified.

Operational rules:

- The agent may run smoke tests, pressure tests, and collect logs directly from
  these VPS hosts when the task calls for it.
- The agent may access `.111` and operate temporary TUIC services on its
  allowed UDP `8443` without requesting per-command authorization. Preserve
  fail-closed cleanup and do not persist credentials or private keys.
- Do not store TUIC UUIDs, passwords, private keys, or other secrets in the
  repository, specs, learning memory, or final summaries.
- When a `.27` VPS suite may need `sudo -v`, start it in a true writable TTY
  session from the beginning and enter the sudo password only at the prompt.
  Do not rerun the same suite through non-TTY SSH after sudo has already asked
  for a password, and do not place sudo passwords in commands, scripts, docs,
  logs, repository files, reports, or summaries.
- Standing authorization: commits produced by authorized mini_vpn work may be
  pushed to the current working branch without requesting separate or per-push
  confirmation, including in future stages and sessions. Normal non-force push
  is part of stage completion. Do not force-push, publish unrelated local
  commits, or change the repository's configured `origin` without separate
  authorization.
- For GitHub push from the Mac mini, do not keep retrying HTTPS origin when it
  fails with an interactive credential prompt. If SSH auth works, use a
  one-shot SSH push URL such as `git@github.com:S7245/mini_vpn.git`; the
  configured `origin` remains unchanged.
- sing-box logs on `.33` can grow quickly; inspect or truncate them deliberately
  when needed, and mention destructive log cleanup before doing it.

## Current Acceptance Bias

When investigating performance or reliability, prefer acceptance criteria that
show the system survives realistic pressure:

- high-concurrency TCP without event-loop stalls;
- UDP/live-streaming quality with low loss over sustained duration;
- bounded pending/downlink backlog and observable backpressure;
- no unexpected relay reaping while useful buffered data remains;
- clear metrics showing whether a bottleneck is local loop CPU, QUIC path,
  server config, VPS network, or application logic.


# Agent Behavior Guidelines
- **Model Consistency:** Keep using the currently selected advanced Codex model (such as GPT-5.5). Downgrading the model to a smaller or simplified version without my explicit authorization is strictly prohibited.
- **Quality Standard:** If you encounter difficulties, prefer using the "deep thinking" or "extended reasoning" mode. Prohibit giving incomplete code due to speed or quota constraints.


# Session & Context Management
- **Frequent Refreshes:** If the task takes too long to run or multiple repair failures occur, please automatically re-evaluate the current architecture.
- **Source of Truth:** Always refer to the project root's `AGENTS.md` and architecture design files. Do not rely on short-term memory. If the context is too large, ask me if a new session tree is needed to maintain code quality.
- **50% Context Handoff:** When the current session context has exceeded roughly 50%, do not interrupt an in-progress task solely for that reason. At the end of the current task, explicitly recommend opening a new session and provide a ready-to-copy opening handoff for it. The handoff must name the current Knife/stage/task, the exact stop or accepted position, the required source-of-truth files to read, the next authorized actions, frozen parameters/non-goals, test/stop rules, and whether VPS, macOS TUN, commits, or Slack notification are authorized.

# Execution Plans
- **Self-Correction:** If the repair test fails, do not immediately begin random modifications. You must first analyze the reasons for the failure and output a proposed modification plan, then wait for my confirmation before proceeding to the next step.
