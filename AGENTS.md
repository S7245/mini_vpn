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

## Current Agent Role And Knife14 Position

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
historical background; do not load all of them by default. For the current TCP
throughput branch, prioritize the 2026 Knife14 documents, especially the
US-client results, downlink/backpressure/lifecycle specs, and the latest
results documents.

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

Current Knife15 plan, as of 2026-07-14:

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
- For GitHub push from the Mac mini, do not keep retrying HTTPS origin when it
  fails with an interactive credential prompt. If SSH auth works, use a
  one-shot SSH push URL such as `git@github.com:S7245/mini_vpn.git`; ask before
  changing the repository's configured `origin`.
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
