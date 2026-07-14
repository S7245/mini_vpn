# HANDOFF — mini_vpn core 路线（达成 Rules.md 用户使用目标）

给后续 **逐刀接力的新 session**。每刀单独开 session（省 token），按本文件冷启动。

## Next Planned Stage — Knife15 Release Readiness (2026-07-14)

- Knife14 is complete. Knife15 will prove hours-long bounded operation,
  recovery, and operational evidence rather than reopen peak-throughput
  tuning.
- Preserve two distinct lanes: the capable Linux/VPS topology owns H10d16
  architecture and peak-throughput regression; a dedicated long-running
  macOS machine in Shenzhen owns real-client utun, resource stability,
  mixed TCP/UDP/DNS traffic, idle/resume, and network-lifecycle evidence.
- Shenzhen-to-US bandwidth below `200 Mbit/s` is expected and is not an
  architecture failure. Establish direct/control bandwidth `B`, use roughly
  `40-60% of B` sustained and `75-85% of B` bursts, and judge internal
  invariants, resource trends, recovery, and relative path behavior.
- First implementation task is a new H10d16-aware, target-only, fail-closed
  macOS runner. It must prove exact source/binary provenance, keep the Exit
  route outside utun, restore route/DNS on every exit, bound logs, collect
  process/utun/network/event evidence, avoid secrets, and have BSD-compatible
  self-tests. Do not reuse the historical full-route macOS scripts unchanged.
- Planned gates are `2h -> 8h -> 24h`, followed by independent Wi-Fi,
  sleep/wake, path-change, client-restart, and authorized Exit-restart recovery
  windows. A shorter failure blocks the longer run until diagnosed.
- The current HK development Mac remains prohibited for test TUN. The accepted
  lane is the dedicated Shenzhen machine. This update planned the stage only;
  it did not implement a runner or execute macOS TUN/VPS traffic.
- Keep H10d16, EndpointWindowV1, MTU1200, `1160B` UDP shape, pool, QUIC
  windows, chunk, Cubic, GSO default, queue/FIFO/batch bounds, driver bound,
  and self-wake frozen. Do not reopen bounded sender, cap64, or GSO-only.
- Plan:
  `docs/tech/2026-07-14-knife15-long-duration-release-readiness-plan.md`.

## Current Override — Knife14h10d16 Byte-Owned Egress (2026-07-14)

This section overrides the older G7/G8/GV next-step text below.

### Latest accepted position (2026-07-14)

- Knife14 H10d16 Task 12 step 4 is complete. Exact source `5f9da90` passed
  target-only reverse P8 through the capable Shoes/Quinn `.111:8443` Exit at
  `188 Mbit/s` receiver, `60/60` nonzero intervals, and `149 Mbit/s` minimum.
  All eight flows exceeded one D16 quantum.
- The P8 had TUN drops `0/0`, pump `129/500`, zero full waits/read errors,
  zero formal QUIC loss/congestion/blocking, zero terminal pending/reap, and
  exact endpoint accounting. The endpoint maximum conservation sample was
  `61,406 <= 61,440B`.
- The earlier `.33` reverse result is invalid as an architecture discriminator:
  its external sing-box/quic-go sender is independently restricted to low
  single-digit throughput on this topology. The capable Shoes/Quinn topology
  is the accepted reverse gate.
- Corrected UDP payload `1160B` stayed inside MTU1200. Reverse/live-streaming
  delivered `90 Mbit/s` with `0/290,950` loss and zero mini_vpn/TUN/client-QUIC
  window loss. Forward delivered `83.7 Mbit/s` from a `90 Mbit/s` offer with
  `7%` application loss but no mini_vpn/TUN/within-window client QUIC drop.
  Exclude the earlier `1200B` payload because its `1228B` IP packet forced
  fragmentation.
- Linux fake-IP DNS and TUN lifecycle passed in two fresh process cycles:
  arbitrary `8.8.8.8:53` queries returned `198.18.0.2`, both metrics reported
  `DNS forge=1/drop=0`, both stops removed TUN/routes, and re-create succeeded.
- Post-VPS local gates passed again: root `632/632`, harness `643/643`,
  integration `10/10`, quinn-proto `309+3`, Quinn `29+1`, smoltcp `290+3`,
  all-target check, fmt, three shell self-tests, and diff checks. Final review
  has no unresolved P0/P1.
- Cleanup is complete. `.27` has no client/TUN/test route. `.111` Shoes,
  restore service/timer, UDP8443 listener, transient binary/runtime are gone;
  all four socket buffers are restored to `212992`. No macOS TUN ran.
- Accepted chain: `c55737e -> d934f12 -> 20a0f8c -> e201403 -> 5f9da90`.
  Gate A/B remain accepted. Do not reopen bounded sender, cap64, GSO-only,
  D3 self-wake, or frozen tuning. Start a new explicitly scoped stage for any
  later work.
- Result:
  `docs/tech/2026-07-14-knife14h10d16-endpoint-pacing-service-vps-completion-results.md`.

### Previous accepted position (2026-07-13)

- The first fresh frozen reverse P8 from `55792b3` is an architecture failure,
  not an accepted throughput result. All eight data relays opened across the
  two healthy pool connections, but each admitted only about one `128 KiB`
  D16 quantum. iperf timed out at `0.103 Mbit/s` after the first aggregate
  interval; almost every later interval was zero.
- The failure was cleanly localised. TUN drops were `0/0`, pump high was
  `177/500` with zero full waits/read errors, smoltcp capacity stayed
  `1 MiB`, pending ended at zero, QUIC loss/congestion was zero, and endpoint
  conservation was exact. The guard completed `6/6` pause/resume edges, but
  the final phases were `Running=0`, `DrainOnly=8`, `Recovery=1` after data
  sockets later reached `send_queue=0`.
- Root cause: `next_d16_egress_phase_for_snapshot` cleared a per-flow ACK
  barrier on a zero snapshot even while hard pressure/debt still forced
  DrainOnly. The positive nonzero-to-zero proof was discarded; once pressure
  cleared, an already-zero queue could not generate new cycle-local drain
  evidence.
- Repair commit `5f9da90f734b1754fd8c41bcb70fa4c8b6ae9f74` retains the
  barrier while hard pressure, drop debt, or terminal no-send dominates and
  consumes it only on the first eligible clean zero snapshot. It changes no
  queue, quantum, timer, capacity, pacing, or wake constant.
- Local gates pass: focused `2/2`, D16 `60/60`, root `632 passed / 3 ignored`,
  harness `10 passed / 4 ignored`, concurrency `64/256/1024`, UDP four-size
  `500/500`, check/fmt/diff/shell tests, and controlled all-target Clippy. Code
  review has no unresolved P0/P1. Strict Rust 1.95 Clippy still exposes 19
  pre-existing baseline lints outside this repair.
- Next action is one fresh target-only, reverse-only P8 from exact `5f9da90`
  after secret-free export/hash verification and profile rehearsal. VPS and
  commit are authorized; macOS TUN remains prohibited. Require `>170 Mbit/s`,
  `60/60`, zero TUN drops, pump below `500`, all eight flows beyond one
  quantum, no eligible DrainOnly strand, exact ownership, and cleanup. A new
  failure is architecture failure; do not tune constants.
- Failure/local results:
  `docs/tech/2026-07-13-knife14h10d16-{reverse-p8-failure-results,ack-barrier-recovery-local-gate-results}.md`.

- The accepted H10d16 chain is now complete through the frozen VPS P1:
  EndpointPacingService `c55737e`, batch relay `d934f12`, bounded TUN ingress
  `20a0f8c`, and the independent local TCP receive-credit service
  `e20140340f7f949fa8bad9e960ce94451d2c0229`.
- The selected repair preserves the physical `1 MiB` smoltcp RX/TX storage but
  limits H10d16 advertised and accepted receive credit to `368,640B`. The TCP
  right edge is `application_consumed_seq + min(storage, limit)`; queued bytes
  reduce advertised free credit and cannot move that edge forward.
- Local TDD and regressions pass. The exact real-Quinn 32 MiB gate reached
  `302.246 Mbit/s`, exact bytes, zero modeled drops, ring/pump `15/500`, zero
  full waits/read errors, and `recv_queue_max=39,440B`. Vendored smoltcp passed
  `290+3 docs`; quinn-proto `309+3 docs`; Quinn `29+1 doc`; root/harness
  `630/641` nonignored; integration `10/10`; explicit `64/256/1024` and the
  four-size zero-loss UDP sweep passed. No unresolved P0/P1 remains.
- The one frozen target-only forward VPS P1 is **PASS**: receiver `191 Mbit/s`,
  `20/20` intervals, tail average/minimum `201.333/192 Mbit/s`, TUN drops
  `0/0`, and formal aggregate QUIC-loss delta `11,459,701B <= 16 MiB`.
- Reachability was exact. The socket `recv_queue_max` reached but did not
  exceed `368,640B`; the packet conversion predicted `318`, and the real pump
  high-water was exactly `318/500`, with zero full waits/read errors. `623,222`
  TCP packets became `4,022/4,022/4,022/4,022` batch/relay/poll/flush services,
  avoiding `619,200` repeated calls.
- Endpoint final state was `available=61,403B`, `live=0`, `outstanding=0`;
  `500,763,062B granted - 59,480B refunded = 500,703,582B sent`. There was no
  abandoned byte, reconnect, migration, pacing leak, flow-control block,
  terminal pending/reap, TUN flush failure, or unbounded close owner.
- Cleanup is complete: no VPS client remains and the target route is back on
  `eth0`. The sanitized five-member bundle is archived at
  `/private/tmp/mini_vpn_local_uplink_window_e201403/` with SHA-256
  `769dadd36d1b6db8ba0ff4aad3bd7efc16699b5cc01530932e81cdfb018f18d4`.
  No macOS TUN ran.
- Task 12 step 4's local-uplink-window successor is accepted. Keep H10d16,
  EndpointWindowV1 constants, MTU, queue/FIFO/batch bounds, pool, QUIC windows,
  chunk, Cubic, GSO default, Quinn sender/driver bound, and self-wake frozen.
  Do not reopen bounded sender, cap64, GSO-only, or parameter-tuning branches.
  The forward P1 blocker remains closed; resume Task 12 step 4 at the repaired
  fresh reverse P8, then UDP/live-streaming, Linux fake-IP DNS, and TUN
  stop/rearm.
  Source:
  `docs/tech/2026-07-13-knife14h10d16-local-uplink-window-service-{architecture-spec,implementation-plan,local-gate-results,vps-results}.md`.

The older entries below are chronological stage history and do not override
this position.

- The user-confirmed post-cap64 design-preparation stage is complete. The
  deterministic fake-time replay uses `RTT=200us`, `cwnd=40000B`, `MTU=1280`,
  one driver poll per `50us`, and the real 20-datagram poll bound. Its initial
  acceptance assertion failed exactly at `259 > 64` datagrams inside `1ms`;
  the default vendored test now characterizes that refill mechanism.
- A new endpoint pre-accounting architecture/capacity spec and implementation
  plan are prepared. The fixed candidate is `30,720,000 wire B/s`, `61,440B`
  burst, `10,240B` control reserve, and `20,480B` two-connection DRR quantum.
  It proves `<=92,160B/1ms` and `<=368,640B/10ms` for finalized and
  socket-accepted connection datagrams, including live reservations and
  socket-blocked outstanding bytes, while measured wire overhead leaves
  `239.167 Mbit/s` application capacity. Source of truth:
  `docs/tech/2026-07-13-knife14h10d16-endpoint-pacing-service-architecture-spec.md`
  and the sibling implementation plan.
- Code-level decision is **GO for local TDD only after explicit confirmation
  of that spec/plan**. The correct seam spans endpoint-shared Quinn-proto
  policy plus a thin pinned Quinn driver waker/socket-outcome Adapter; neither
  the per-connection Pacer nor public socket wrapper alone can prove the
  contract. No coordinator implementation, VPS, P8, macOS TUN, commit, or
  external notification ran. Vendored Quinn-proto `276/276` plus doc `3/3`,
  root fmt, and tracked/untracked diff-checks passed. Frozen values and Gate
  A/B remain unchanged.

- The architecture spec's one authorized same-window forward discriminator is
  complete and **FAIL**. The Gate-aligned sing-box control passed at
  `192.567/191.928 Mbit/s`, target-only, with client/Exit UDP socket drops
  `0/0`. The same Shoes PID and frozen profile then carried one fresh cap64
  mini_vpn P1 at `196/185 Mbit/s`, `20/20` nonzero intervals, but it added
  `29` TUN TX drops, `50,621,275B` formal QUIC loss, and `15,417` congestion
  events. The `16 MiB` loss ceiling and zero-drop gate both failed.
- Pool attribution was exact: conn `1` carried `99.9998%` of QUIC TX bytes and
  `99.9961%` of datagrams, conn `0` remained auxiliary, IDs did not change,
  flow-control blocking stayed zero, and every formal `cwnd` was far below
  `u32::MAX`. The data Pacer did bind once at `81920B = 64*1280`, but only one
  of five formal snapshots was cap-active.
- Bilateral captures had zero kernel drops. mini_vpn client egress peaked at
  `267 packets/1ms` and `1337/10ms`, versus same-window sing-box `100/605` and
  prior Quinn-default mini_vpn `261/1259`. Code review therefore rejects the
  stored-token cap as a sufficient temporal bound: at sub-ms RTT, Quinn's
  retained `1.25*cwnd/rtt` refill slope can replenish multiple buckets inside
  1ms. This is not a policy, pool, GSO, migration, extreme-cwnd, D16, or
  stacked-sender reachability failure.
- Task 12 step 4 remains **STOPPED before P8**. Do not retry cap constants,
  reopen bounded `AsyncUdpSocket` cooldown/GSO-only branches, or tune D16,
  MTU, pool, QUIC windows, chunk, Cubic, or self-wake. The proposed next work
  is a red sub-ms refill replay plus a new architecture/capacity spec for a
  true endpoint pre-accounting time-window/service contract; implementation
  requires explicit confirmation. Gate A/B remain PASS. VPS cleanup is
  complete, `.77` iperf3 remains active, and no macOS TUN, commit, or Slack
  notification ran. Result:
  `docs/tech/2026-07-13-knife14h10d16-pacer-cap64-forward-discriminator-results.md`.

- Task 12 step 4's four confirmed P1 repairs and the full local regression gate
  are now **PASS**. The known-negative fixed `48 then 2ms` replay is explicit
  and ignored by default; the optional Quinn cap derives from one upstream
  capacity computation; formal QUIC stats expose both `current_mtu` and
  `pacing_mtu`; and the runner validates, rejects incompatible combinations,
  propagates, reports, and independently verifies
  `MINI_VPN_TUIC_PACING_POLICY=quinn|pacer-cap64` at startup.
- The final local evidence is vendored Quinn `275/275` plus doc tests `3/3`;
  GSO-enabled fixed `32 MiB` cap64 delivery at `621.573 Mbit/s` with exact
  bytes, clean EOF, active cap/runtime attribution, and no socket sender;
  library `620 passed / 0 failed / 3 ignored`; normal harness `10 passed / 4
  ignored`; explicit concurrency `64/64`, `256/256`, and `1024/1024`; and the
  UDP sweep with `500/500` at every payload size and zero loss. Default and
  harness checks, root fmt, all three shell syntax/self-tests, and diff-check
  passed.
- Final code review found no open P0/P1. The existing unused
  `SendBatch::try_reserve` warning remains in the frozen default-off bounded
  diagnostic path and is not a product/gate failure. Gate A and Gate B remain
  PASS. No VPS or macOS TUN ran, no commit was created, and D16, MTU, pool,
  QUIC windows, chunk, Cubic, and self-wake remain unchanged. The next possible
  action is the architecture spec's single same-window forward control plus
  cap64 mini_vpn P1, but it requires a new explicit authorization. The required
  stage-stop Slack notification was sent after local review completed.

- Task 12 step 4 local implementation reached its narrow capacity gate but is
  **STOPPED before the remaining full local regressions and before VPS** under
  the user's repeat-failure rule. The vendored exact `quinn-proto 0.11.16`
  suite passed `274/274`; the GSO-enabled fixed `32 MiB` / `64 KiB` local
  upload passed exact bytes, clean EOF, and `537.106 Mbit/s`. Its active
  snapshot proved `307200B` uncapped, `76800B = 64*1200` effective,
  `cap_active=true`, `delay_events=12`, and `cwnd <= u32::MAX` with the old
  bounded socket sender absent.
- Full `cargo test --lib` stopped at `620 passed / 1 failed / 2 ignored`. The
  sole failure is the already rejected fixed `48 then 2ms` bounded-sender
  real replay, which still unconditionally requires `>170 Mbit/s`; it reached
  `101.084 Mbit/s`. This is a regression-suite classification defect, not
  evidence that pacer-cap64 missed its local gate.
- Post-stop review found four P1 repairs required before resuming: make the
  known-negative bounded replay explicit/ignored; derive the optional cap from
  one upstream capacity computation so the default Quinn hot path does not
  repeat RTT/window division; log both `current_mtu` and `pacing_mtu`; and add
  `MINI_VPN_TUIC_PACING_POLICY` validation/export/fingerprint checks to the
  acceptance runner. Await user confirmation before those code/script edits.
  No VPS or macOS TUN ran; no commit was created; all frozen knobs remain
  unchanged.

- Task 12 step 4 remains after the accepted Gate B; it has not rolled back to
  Gate B. The post-failure architecture/capacity gate is now **CONDITIONAL GO
  for local TDD implementation only**. The selected tracer is a pinned
  `quinn-proto 0.11.16` per-connection pacer cap of `64` paced
  MTU-equivalents. Upstream `256 * mtu` behavior remains exact by default; the
  frozen pool=2 has a static stored-token ceiling of `128` MTU-equivalents but
  no endpoint-wide sliding-window or fairness theorem.
- The public `AsyncUdpSocket` deadline/debt variants are rejected because they
  act after Quinn records packets sent and therefore double-pace. The Quinn
  driver has no pre-accounting endpoint scheduler seam, and Linux
  `SO_MAX_PACING_RATE`/`sch_fq` is not the cross-platform product design. A
  full shared pre-accounting coordinator is deferred until the narrow
  single-flow tracer proves it is necessary. This was the prior local-only
  design authorization; the implementation checkpoint and stop now recorded
  above supersede its no-implementation wording. Source of truth:
  `docs/tech/2026-07-13-knife14h10d16-quinn-pacer-burst-cap-architecture-spec.md`.

- Task 12 step 4 remains **stopped before VPS after the confirmed bounded
  send-service measurement gate failed**. The measurement-only tracer and
  runner pool-aggregate loss repair passed deterministic tests. The real
  GSO-enabled `32 MiB` upload again delivered exact bytes/pattern and clean
  EOF, but reached only `98.311 Mbit/s` versus the required `>170 Mbit/s`.
- Mean wire payload was healthy at `1199.953B`; the selected root is the
  serialized send/cooldown/timer period. A 48-datagram batch averaged
  `4.573ms`, including `1.340ms` mean rearm lateness. Even with lateness set to
  zero, the fixed full cooldown plus observed batch work allows only about
  `142.5 Mbit/s`, so another constant retry is rejected.
- Post-failure review finds the wrapper double-paces Quinn's existing private
  token bucket. The new architecture/capacity spec and conditional local-only
  decision are now recorded above. The historical measurement result is:
  `docs/tech/2026-07-13-knife14h10d16-bounded-udp-send-service-measurement-results.md`.
  No VPS or macOS TUN ran. D16, MTU, pool, QUIC windows, chunk, CC, and
  self-wake remain frozen; P8 and later regressions remain unspent.

- Task 12 step 4 remains **stopped after the GSO-disabled forward
  discriminator failed**. The public Quinn policy seam and real 32 MiB upload
  passed locally with GSO disabled, exact delivery, clean EOF, and
  `>170 Mbit/s`; the production default remains enabled. No D16, MTU, pool,
  QUIC-window, chunk, congestion-control, or self-wake value changed.
- The corrected same-window sing-box control was `177.303/175.102 Mbit/s`
  with target-only routing and zero client/Exit socket or capture drops. The
  fresh disabled-GSO mini_vpn P1 reached `206/193 Mbit/s`, but again added `30`
  TUN TX drops, `67,826,613B` of formal QUIC loss and `40,887` congestion
  events. Its final counters were `73,572,300B` lost and `44,198` events.
- Bilateral capture measured control client-out/Exit-in at
  `453,361,295B/442,185,588B` (2.47% gap) and disabled-GSO mini_vpn at
  `565,156,612B/492,300,734B` (12.89% gap), with tcpdump kernel drops zero.
  Disabling GSO reduced the prior mini_vpn peak from `261` to `163 packets/ms`
  but did not remove or improve the product loss edge.
- Post-failure code review explains why: disabled GSO limits one Quinn-proto
  `poll_transmit` to one datagram, but Quinn still loops to 20 datagrams per
  driver poll, immediately self-wakes, and retains a 256-packet pacer capacity.
  The switch changes UDP syscall aggregation, not inter-poll pacing. Review
  also found that the suite discards standard-P1 status with `|| true` and the
  low-RTT report prints full-tunnel curl/DNS expectations under target-only
  routing.
- Await confirmation of a red-first runner repair plus one shared bounded QUIC
  UDP egress service at Quinn's public `AsyncUdpSocket` seam. The proposed
  fixed capacity is 48 wire-datagram equivalents per 2 ms: `30.72 MB/s` raw at
  1280B, above the `21.25 MB/s` required for 170 Mbit/s while bounding the
  observed burst. Do not spend another GSO run or resume P8 before this local
  seam, full regression gate, and code review pass. Result:
  `docs/tech/2026-07-13-knife14h10d16-gso-disabled-forward-discriminator-results.md`.

- Task 12 steps 1-3 / Gate B **passed** from clean `a54fb17`. The corrected
  same-window sing-box control was `144.519/143.228 Mbit/s` with target-only
  routing and zero client/server UDP socket drops. The three exact `20s`
  reverse-first P1 receiver results were `192`, `188`, and `191 Mbit/s`;
  median `191 Mbit/s` exceeds the absolute `170 Mbit/s` gate and every run
  exceeds `150 Mbit/s`. All runs had zero TUN drop, actor bypass, send/flush
  errors, pressure/drop debt, QUIC loss/blocking, reconnect, and terminal
  pending reap.
- The single post-median fixed `64 MiB` A-clean completed at `179/179 Mbit/s`,
  delivered exactly `67108864B`, and closed through `clean_queue_lifecycle`
  with queue/reserved/leased, pending/inflight, close pending/egress, terminal
  reap/drop, TUN drop, and bypass all zero. No macOS TUN test ran.
- Gate B is closed, but the final stable-170 claim is not yet authorized.
  Continue with Task 12 step 4 only: sustained `60s` reverse TCP, TCP
  multi-flow/concurrency, UDP/live-streaming, fake-IP DNS, and TUN lifecycle
  regressions. Keep D16, MTU, pool, QUIC windows, chunk, self-wake, and old-path
  cleanup frozen until that gate passes. Result:
  `docs/tech/2026-07-13-knife14h10d16-gate-b-results.md`.
- Operational override: when a future run fails only in preflight/configuration
  and the cause is evidenced, correct it and continue without asking again;
  preflight-only exits do not consume a measurement sample. A real product or
  gate failure still follows the stop/analyze/plan rule.

- Composite Gate A **passed** from clean source `79b41b3` against the capable
  Shoes `v0.2.7` / Quinn `0.11.9` Exit on `.111:8443`.
- A-capacity completed the exact `20s` reverse-first P1 at `192/188 Mbit/s`.
  TUN drops, actor bypass, send/flush errors, pressure debt, and QUIC
  loss/congestion/blocking were zero; active data-stream gaps were below one
  second. Its only timed-boundary terminal was the approved exact
  `local_to_remote/local_socket_terminal`, with one bounded `524288B` D16
  ownership release and `27840B` terminal smoltcp egress.
- A-clean then completed exactly `64 MiB` at `179/179 Mbit/s` on the same
  binary/profile/tunnel and closed through remote EOF plus
  `clean_queue_lifecycle`; queue, reserved, leased, pending, inflight,
  terminal-drop, close-egress, TUN-drop, bypass, and error counters were zero.
- This was the first accepted H10d16 Gate A and closed stage 8. It unlocked the
  Gate B execution now recorded above; preserve it as the Gate A source rather
  than treating its former next-step text as current.
- Result:
  `docs/tech/2026-07-13-knife14h10d16-shoes-composite-gate-a-results.md`.

- H10d15 produced the first strong parity-capacity proof: reverse-first P1
  reached `186 Mbit/s` sender and `185 Mbit/s` receiver with the native permit
  path and service-sized read progress. Active read gaps fell below one second.
- H10d15 is not accepted: its tail reported `tx_dropped_delta=229`,
  `global_rx_paused=true`, `terminal_closed_no_send`, and
  `close_egress_bytes=327272`.
- A follow-up code review found three architecture contract breaks:
  1. the TUIC native ordered pump stages payload in a message-count channel
     before the D6 byte queue and ignores its `_max_len` input;
  2. D3 mode still reaches `send_slice` through legacy dirty/timer/TUN-RX
     paths outside the actor service window;
  3. hard pause returns before ACK/TUN RX, `iface.poll`, and `flush_tx`, so it
     stops recovery drain and depends on the old timer path.
- The approved next architecture is H10d16: one RAII byte ledger beginning
  before Quinn reads, a per-flow leased byte queue, readiness-only global
  events, actor-exclusive D16 admission, and
  `Running -> DrainOnly -> Recovery` feedback.
- Approved source of truth:
  - `docs/tech/2026-07-09-knife14h10d16-byte-owned-egress-architecture-spec.md`
  - `docs/tech/2026-07-09-knife14h10d16-byte-owned-egress-implementation-plan.md`
  - `docs/tech/2026-07-09-knife14h10d16-session-handoff.md`
- Stage position: nominally stage 8 because capacity passed but clean Gate A
  failed. D16 code baseline `8496b8f` and ACK-capacity repair `f7847dd` are
  pushed on `codex/knife14d-downlink-reap-open`. Stages 3-7 and mandatory Task
  11A are now closed locally: the 64 MiB
  bounded-ring production seam passed 50 consecutive capacity-qualified
  repeats with complete delivery, about `224 Mbit/s` local receiver capacity,
  at most 24 payload packets per flush, zero modeled drop/bypass, and clean
  EOF/close-tail accounting. The closed design uses a two-epoch device guard,
  a per-flow ACK-completion
  barrier, and an MTU-derived sliding admission window that deducts the current
  smoltcp send queue.
- The single authorized ACK-capacity replacement Gate A has now run and
  failed throughput at `19.2/17.9 Mbit/s` sender/receiver. The repaired local
  path stayed clean: `tx_dropped_delta=0`, actor bypass `0`, pressure/backlog
  edges `0`, send/flush errors `0`, and observed pending/egress/terminal-tail
  counters `0`. The data flow remained active at final snapshot, so natural
  EOF/close was not established. Repeated ordered read gaps reached `3548ms`
  despite active polling, while local actor drain and QUIC loss/congestion/
  blocking surfaces were healthy. Gate B remains frozen.
- The same-stream discriminator and full local composition gate are now closed
  at `1bf1f78`. Real loopback Quinn tests cover the direct ordered reader, its
  RAII reservation/readiness queue, and the full `run_event_loop` TCP/smoltcp/
  TUN actor path. The full path delivered `32 MiB` above `170 Mbit/s`, bounded
  each flush to 24 payload packets, and closed with zero modeled drop, actor
  bypass, and EOF tail. It passed `30/30` repeats and all local gates.
- A transient full-path timeout was a stale harness observation, not retained
  ownership: the generator had all bytes and was already `CloseWait`, while the
  sink had not sampled the final control-only dirty-relay transition. Harness
  snapshots now refresh after each `process_dirty_relay` pass; production EOF,
  queue, permit, actor, and socket behavior did not change.
- Gate A is currently protected by a mature-client precondition. Same-window
  sing-box controls reached only `14.207 Mbit/s`, `1.363 Mbit/s`, and
  `1.182 Mbit/s` with
  correct routing, `16 MiB` client UDP buffers, and socket drop `0`, while
  direct `.27 -> .77` and `.33 -> .77` reverse baselines remained about
  `217/219 Mbit/s`. This is burst/idle TUIC-window evidence shared by both
  clients, so no post-`1bf1f78` Gate A and no Gate B has run.
- Current result:
  `docs/tech/2026-07-10-knife14h10d16-real-quinn-local-and-control-results.md`.
- `.33` sing-box was restarted once under the persistent high-buffer settings.
  The next MTU1200 control improved only to `17.301 Mbit/s`; an MTU1500 A/B was
  worse at `3.146 Mbit/s`. Client/server UDP sockets remained `16 MiB` with
  drop `0`, service logs showed normal opens, and bidirectional 100-packet ICMP
  checks had `0%` loss at about `0.5ms`. Restart and MTU are rejected as
  sufficient explanations.
- Gate-process review tasks R1-R4 are closed at `ec112a9`. The exact safe1200
  real-Quinn path retained `32 MiB >=170 Mbit/s` capacity in `30/30` repeats;
  the D16 suite and mature MTU1500 control are versioned with self-tests and
  hashes. A clean `.27` worktree built the release binary, connected both TUIC
  pool slots, verified the exact H10d16 fingerprint, MTU1200 and target-only
  route, observed TUN drop `0`, and cleaned the process/TUN/route without
  running iperf. Result:
  `docs/tech/2026-07-11-knife14h10d16-gate-process-r1-r4-results.md`.
- Next stage: in a genuinely new external TUIC service window, run the
  versioned historical-MTU1500 mature control exactly once. Require receiver
  `>150 Mbit/s` and both UDP socket drops `0`; only then spend one `20s`
  safe1200 reverse-first P1 Gate A. Do not edit D16 or repeat control in the
  already-proven incapable window. Gate B remains frozen until Gate A passes.
- That versioned control ran from clean `044eccb` on 2026-07-11 and the window
  was still incapable: `.27 -> .77` direct reverse was `216.172 Mbit/s`,
  `.33 -> .77` was `213.865 Mbit/s`, but sing-box TUIC control was only
  `13.472/11.219 Mbit/s` sender/receiver. Target/Exit routes were correct, both
  UDP sockets were `16 MiB` with drop `0`, and the stream was burst/idle with
  many zero-rate seconds. Gate A and Gate B were not run. Do not repeat this
  unchanged control until an independent external-window change occurs. Result:
  `docs/tech/2026-07-11-knife14h10d16-versioned-control-results.md`.
- A bilateral capture and role-reversal review then isolated the incapable
  window to the `.33` sing-box/TUIC service itself: both captures had identical
  packet timing, and reversing `.27/.77` roles through `.33` remained slow.
  A temporary same-version TUIC Exit on `.77` restored the mature control to
  `163.786 Mbit/s` receiver and unlocked the one authorized Gate A.
- The clean `5884ac0` safe1200 Gate A reached `187/183 Mbit/s` sender/receiver
  with stable sub-second read service, TUN drops `0`, actor bypass `0`, and
  clean QUIC loss/blocking surfaces. It still failed the literal tail gate:
  the iperf data peer reset directly from `Established` to `Closed` at 20s
  while the D16 reservoir held exactly `524288B` and smoltcp held `27840B`
  unacknowledged. This was local abort, not remote EOF; the control flow closed
  cleanly. Code review also found that the exact terminal drop is recorded but
  the relay summary incorrectly reports `clean_queue_lifecycle`.
- Do not tune the 170M architecture from this result. The approved TDD
  correction is complete: `879e904` removed dead close-reap plumbing and
  `7a7ca04` models `Open / RemoteEof / Terminal(cause)`, preserves the first
  local terminal cause through relay teardown, adds close-cause reporting, and
  versions fixed-byte reverse `iperf3 -n` evidence. Default library `588/588`,
  harness library `597/597`, integration `2/2`, harness targets `10 passed/4
  ignored`, checks, focused fmt, diff-check, and both runner self-tests pass.
  Gate B remains frozen. Result:
  `docs/tech/2026-07-11-knife14h10d16-alternate-exit-gate-a-results.md`.
- Next run one composite Gate A from clean committed source on `.27`: the
  `20s` reverse-first P1 must exceed `150 Mbit/s` with zero TUN/bypass/error and
  exact bounded terminal classification; after it becomes quiet, the same
  tunnel runs one `64 MiB` fixed-byte reverse proof that must end in
  `clean_queue_lifecycle` with every queue/tail counter zero. Gate A passes only
  when both windows pass. Gate B is three timed repeats with a median target of
  `170 Mbit/s`, one same-build fixed-byte clean-close repeat, and a same-window
  sing-box parity fallback.
- The clean `f1627bc` composite Gate A ran in a window where the mature
  sing-box control passed at `157.650 Mbit/s` receiver and the direct reverse
  baseline was `212.607 Mbit/s`. Pool-2 mini_vpn nevertheless collapsed to
  `0.0265 Mbit/s` receiver in A-capacity and the fixed `64 MiB` A-clean flow
  timed out after about `3.68 MiB`. TUN drop, actor bypass, local pressure,
  send/flush error, and QUIC loss/blocking surfaces stayed zero. Target-side
  evidence showed that the TUIC stream stopped supplying data upstream of the
  D16 actor.
- A same-server pool-1 discriminator improved to `115 Mbit/s` receiver with
  zero local drop/pressure but a `3.807s` data-read gap and tail collapse. Code
  review found a destructive auxiliary idle-reconnect policy, but later
  artifact correction showed that the failed capacity flow itself had not
  reconnected; a later A-clean flow did. Pool 1 is not the fix. Result:
  `docs/tech/2026-07-11-knife14h10d16-composite-gate-a-pool-lifecycle-results.md`.
- Task 11C is complete and pushed at `6209910`. Idle auxiliary slots now use a
  bounded Heartbeat/ACK health probe, slot locking precedes lease reservation,
  reconnect has a ready barrier, failed opens invalidate state, and selection
  diagnostics expose generation/probe/reconnect evidence. Default library
  `591/591`, harness library `600/600`, integration `2/2`, concurrency harness
  `10 passed/4 ignored`, checks, runner self-tests, and focused formatting pass.
  No real macOS TUN test was run.
- The scoped A/B failed its `>150 Mbit/s` requirement. A temporary capable Exit
  carried mature sing-box at `195.033 Mbit/s` receiver, while clean `6209910`
  pool 2 used auxiliary `conn=1`, generation `1`, without probe/reconnect and
  reached `108 Mbit/s`. D16/TUN/QUIC error surfaces were zero; the middle
  window sustained `188-190 Mbit/s`, but startup and tail suffered multi-second
  starvation. Stale reconnect is rejected as the active capacity root. Gate A
  and Gate B remain frozen. Next isolate TUIC stream service/frontier progress;
  do not modify D16 actor/queue/EOF, MTU, broad windows, chunk size, or
  self-wake. Result:
  `docs/tech/2026-07-11-knife14h10d16-pool-health-probe-results.md`.
- The stream/frontier follow-up is complete through `bdaa19c`. `99ff0f4`'s
  application-owned service-sized `AsyncRead` buffer was disproved by a clean
  scoped run that stopped after `18356B`; `4bc847b` restored cancel-safe,
  transport-owned ordered Quinn chunks. Against a fresh mature control of
  `172.167 Mbit/s`, clean `4bc847b` reached `38.5 Mbit/s`: local D16/TUN gates
  remained clean, but ordered read gaps reached `5.219s` while connection-level
  STREAM frames arrived. A bounded unordered-frontier prototype was rejected
  locally because a gap consumed the full `512 KiB` per-flow ownership cap
  before retransmission, so it is not a product path.
- `bdaa19c` fixes a separate deterministic cancellation bug: non-pausing
  Running credit updates no longer refund and resize an already armed RAII
  read reservation; pause/close/stop remain authoritative. Default `591/591`,
  harness `600/600`, and both checks pass. Its scoped VPS A/B has not run: after
  a fresh temporary-Exit rebuild, the mandatory mature control was only
  `18.873 Mbit/s` receiver despite `217.011 Mbit/s` direct and socket drop `0`.
  Composite Gate A was not spent and Gate B remains frozen. Result:
  `docs/tech/2026-07-12-knife14h10d16-stream-frontier-and-armed-read-results.md`.
- The next clean `ce5a87c` requalification control also did not authorize the
  scoped `bdaa19c` run. With a same-version temporary `.77` Exit and `.33` as
  the role-reversed iperf target, direct receivers were `216.801 Mbit/s`
  (`.27 -> .33`) and `212.398 Mbit/s` (`.77 -> .33`), and both TUIC UDP socket
  drops were zero. Mature sing-box nevertheless reached only `0.192 Mbit/s`.
  Eighteen of twenty one-second intervals were exactly zero; the only bursts
  were `3.143` and `0.695 Mbit/s`. This is another incapable external TUIC
  window, not evidence about `bdaa19c`. Do not run scoped mini_vpn or composite
  Gate A from this window.
- An isolated same-version second service on `.33` was then started cleanly on
  UDP `9443` while the original `8443` service remained active. Its mature
  control was not a capacity result: the client timed out before a TUIC stream
  opened. A simultaneous `.33` capture received the one-byte probe sent to
  `8443` but received nothing sent to `9443`, proving that the new port is
  blocked before the host. The second service was removed and original `8443`
  stayed healthy. Continuing this discriminator requires a confirmed
  maintenance swap where the minimal service temporarily owns the already-open
  `8443`; do not interpret the `9443` timeout as mini_vpn evidence.
- The confirmed `8443` maintenance swap is complete and falsifies the original
  sing-box process/full-config hypothesis. A watchdog-protected minimal
  same-version service temporarily owned the already-open port. Clean mature
  control had `218.688 Mbit/s` direct, `214.285 Mbit/s` `.33 -> .77` direct,
  and zero UDP socket drops, but only `11.953 Mbit/s` TUIC receiver. Thirteen
  of twenty one-second intervals were zero and the nonzero intervals were
  isolated bursts. Client errors were zero; the server only logged the expected
  timed remote cancel. Original sing-box was restored and verified active.
  This leaves the `.33` host/Client-to-Exit QUIC path, not mini_vpn or the old
  service process, as the external blocker. Scoped `bdaa19c`, Gate A, and Gate
  B remain unspent.
- A fourth independent Exit `.111` (`43.173.101.111`) was authorized, added to
  `AGENTS.md` at `559f7a8`, and tested with the same sing-box `1.13.14` binary
  and FIFO-only service. `.111 -> .77` direct reached `212.013 Mbit/s`; the
  versioned control direct receiver was `214.092 Mbit/s`; both UDP socket drops
  were zero. Mature TUIC still reached only `4.928 Mbit/s`, with thirteen of
  twenty intervals at zero. This removes `.33`-specific host/process state as
  a sufficient explanation.
- Follow-up code review found a Gate-process P1: the sole authorization control
  is hard-coded to mature-client `BBR + MTU1500`, while the product Gate profile
  is mini_vpn `Cubic + safe1200`; `src/tuic.rs` explicitly records BBR as an
  experimental override that can underperform Cubic. The historical control
  remains valid historical evidence but is no longer a sound sole necessary
  predicate after repeated independent-Exit false negatives. Before another
  VPS run, TDD a fixed versioned gate-aligned mature control (`Cubic + MTU1200`)
  while retaining the historical profile as a reported diagnostic. It must
  still exceed `150 Mbit/s` with socket drop zero before `bdaa19c` scoped runs.
- The correction is implemented and pushed at `1b83004`. The historical
  profile now reports diagnostic-only results; the new authorization profile
  is fixed at mature-client `Cubic + MTU1200`, self-tests exact route/profile/
  floor semantics, and only it can emit Gate A `PASS`. Its first clean `.111`
  run had a healthy `217.220 Mbit/s` direct receiver, correct route and MTU,
  `16 MiB` UDP sockets, and drop `0`, but TUIC receiver was only `3.041
  Mbit/s`; fourteen of twenty intervals were zero. This matches the historical
  `.111` burst/idle shape and rejects BBR/MTU mismatch as the active root.
  `bdaa19c` scoped, composite Gate A, and Gate B remain unspent. Next change
  the client host in one same-window Gate-aligned control while holding `.111`,
  `.77`, binary, and profile constant; do not edit D16 or repeat `.27` control
  against unchanged external state. Result:
  `docs/tech/2026-07-12-knife14h10d16-gate-aligned-control-results.md`.
- The approved client-host discriminator is also complete. Holding `.111`,
  `.77`, sing-box `1.13.14`, Cubic/MTU1200, and the authorization floor fixed,
  moving the mature client from `.27` to `.33` still produced only `5.347
  Mbit/s` receiver. Direct receiver was `218.285 Mbit/s`, both UDP drops were
  zero, routing was correct, and thirteen of twenty intervals were zero. The
  `.33` production sing-box remained active. This rejects `.27` as a sufficient
  root and rejects moving Gate A to `.33`. Next test raw reverse UDP on the
  same allowed `.111:8443` port from both clients; only if raw UDP is clean
  should a minimal QUIC-without-TUIC benchmark follow. D16, scoped `bdaa19c`,
  composite Gate A, and Gate B remain frozen. Result:
  `docs/tech/2026-07-12-knife14h10d16-client-host-discriminator-results.md`.
- The same-port raw UDP discriminator is complete. Iperf3 was unusable because
  the cloud ACL passes UDP8443 but blocks its required TCP8443 control; fixed
  iperf2 `2.1.9` UDP reverse was installed temporarily and purged afterward.
  At 100 Mbit/s, both `.27` and `.33` received about `105 Mbit/s` for every
  interval with zero loss. At 200 Mbit/s both received about `198 Mbit/s`
  overall with no zero interval, but shared a `.111` edge: five seconds at
  `210 Mbit/s/0%`, then about `193 Mbit/s/7.9%` loss. Client UDP buffer/error
  deltas were zero. Raw UDP is sufficient for Gate A and 170M Gate B and does
  not reproduce TUIC burst/idle. Next TDD a test-only minimal Quinn
  Cubic/safe1200 reverse-stream cross-host probe; do not change product/D16.
  Result: `docs/tech/2026-07-12-knife14h10d16-raw-udp-path-results.md`.
- The versioned minimal Quinn discriminator is complete at `8ea8925`; probe
  shutdown cleanup is `21dd361`. The exact Cubic/safe1200 reverse ordered
  stream delivered `489095168B` from `.111` to `.27` in `20.315s`, or
  `192.597 Mbit/s`. All 21 intervals carried data, the minimum non-empty
  interval was `183.934 Mbit/s`, pattern errors were zero, EOF was clean, and
  the client reported zero loss, congestion events, and data blocking. Server
  loss at the shared raw-path edge did not create stalls. Raw UDP and minimal
  Quinn therefore have sufficient continuous Gate A/Gate B capacity and do
  not reproduce TUIC burst/idle. Next TDD a test-only direct TUIC Connect
  relay through `.111` to `.77`: `tcp_pool=1`, generic ordered `open_tcp`, and
  bounded loopback iperf3 control/data sockets, bypassing auxiliary-pool
  policy, TUN, smoltcp, native readers, and D16. Use its result to distinguish
  TUIC/client/server stream service from the product integration. Do not
  reopen D16 or spend Gate A yet. Result:
  `docs/tech/2026-07-12-knife14h10d16-minimal-quinn-path-results.md`.
- The direct TUIC discriminator is complete at `0f07406`. With the exact same
  release test binary, pool 1, generic OrderedJoin, client Cubic/safe1200,
  `.111` sing-box, and `.77` target, server BBR reached only `3.775 Mbit/s`
  receiver with `14/20` zero client intervals and `3441ms` maximum data-read
  gap. Changing only the temporary server CC to Cubic reached only `2.674
  Mbit/s`; client delivery became continuous with a `225ms` maximum read gap,
  but `.77` still sent in roughly five-second bursts and had `15/20` zero
  sender intervals. Client Quinn loss/congestion/blocking remained clean.
  This excludes TUN/smoltcp/native D16/pool-2 and rejects server BBR as the
  capacity root; BBR only worsens the burst shape. Next run one `.111`
  host-local TUIC loopback discriminator with the same Cubic service and exact
  probe. Do not change D16 or run Gate A/B. Result:
  `docs/tech/2026-07-12-knife14h10d16-direct-tuic-server-cc-results.md`.
- The host-local discriminator passed with the exact `0f07406` probe and same
  sing-box `1.13.14` binary on `.111`: receiver was `199.639 Mbit/s`, all
  `20/20` intervals were nonzero, the minimum interval was `169.868 Mbit/s`,
  both Connect relays completed, and Quinn/socket error surfaces were zero.
  The same target sender that had `15/20` zero intervals on the external Cubic
  run was continuous at `201 Mbit/s` host-local. This rejects an intrinsic
  sing-box TUIC/copy limit and locks the active boundary to the current
  sing-box/quic-go external sender's interaction with `.111 -> .27`. Preserve
  D16. Next compare one alternate mature TUIC server implementation/version on
  the same `.111:8443` cross-host path with the exact probe and floor. Gate A/B
  remain frozen. Result:
  `docs/tech/2026-07-12-knife14h10d16-host-local-tuic-results.md`.
- The first alternate mature server A/B also failed the external path. Official
  Mihomo `v1.19.28` with Cubic reached only `3.460 Mbit/s` receiver and had
  `7/20` client zero intervals; `.77` had `14/20` zero sender intervals and
  `5.18 Mbit/s`. Direct receivers were `217.010 Mbit/s` from `.27` and
  `216.591 Mbit/s` from `.111`; client Quinn loss/congestion/blocking and
  server UDP drops were zero. This rejects a sing-box-application-specific
  root but not the quic-go lineage: Mihomo and sing-box use separate forks of
  quic-go `0.59.x`. Gate A/B remain frozen. Next review and TDD an independent
  QUIC-stack TUIC v5 server, then run one synchronized bilateral-capture A/B;
  do not change D16. Result:
  `docs/tech/2026-07-12-knife14h10d16-mihomo-alternate-server-results.md`.
- The independent Rust/Quinn reference-server discriminator removed the
  burst/idle shape but still missed the floor. Official `tuic-server 1.0.0`
  with Quinn `0.10.1` reached `112.559 Mbit/s` receiver; all `20/20` intervals
  were nonzero, minimum interval was `103.805 Mbit/s`, and maximum data-read
  gap was `27ms`. Direct receivers were `217.640/216.382 Mbit/s`, target sender
  was continuous at `119 Mbit/s`, client/server drop surfaces were zero, and
  server lifetime CPU was only about `2.73s`. This isolates quic-go external
  starvation from the old reference server's insufficient continuous capacity.
  Gate A/B remain frozen and D16 stays unchanged. Next review/TDD maintained
  Shoes `v0.2.7` (TUIC v5, Quinn `0.11.9`) and run one corrected 120-second
  capture-backed strict A/B. Result:
  `docs/tech/2026-07-12-knife14h10d16-rust-quinn-reference-server-results.md`.
- The maintained Shoes `v0.2.7` discriminator closed the remaining transport
  capacity question. Its Quinn `0.11.9` TUIC server carried the exact direct
  probe at `192.666 Mbit/s` receiver; `20/20` intervals were nonzero and the
  minimum was `153.099 Mbit/s`. Direct receivers were `231.306/221.523
  Mbit/s`, both pcaps dropped zero packets, Exit UDP errors were zero, and
  client UDP buffer-error delta was zero. Modern Quinn/TUIC is sufficient for
  Gate A and the 170M Gate B target; do not reopen D16 or transport tuning.
  The direct test process still failed because timed iperf ended its data
  Connect with one reset. Review found a gate-process mismatch: this probe
  rejects every relay reset, while approved composite Gate A deliberately
  separates timed capacity with exact terminal classification from fixed-byte
  clean EOF. Gate A did not run. Next TDD that classification, replay the
  captured result without another 20-second discriminator, then obtain user
  confirmation before one Shoes-backed composite Gate A. Result:
  `docs/tech/2026-07-12-knife14h10d16-shoes-modern-quinn-results.md`.
- The worktree is intentionally dirty and overlapping D16 files contain older
  experiments. Do not revert user changes and do not commit whole files without
  first showing which pre-D16 diffs would be included.

## 当前状态（基线）

- **Stage 13 + 刀1 + 刀2 + 刀3 + 刀3.5 + 刀4 + 刀5 全部已在 `main`**（`e589767`，2026-06-22 fast-forward 合入，与 origin 同步）。
  数据面 = **client-only TUIC over quinn → sing-box**（ADR-0004）；UDP 默认 **native datagram + Cubic**（刀3.5）；
  **拦截加密 DNS** 逼回落明文 → fake-IP（刀4，ADR-0006）；**拦全 :53 裸包 DNS 劫持**——任意 resolver(如 8.8.8.8:53)的明文
  查询都本地伪造 fake-IP(裸包构造,src=被查询的 resolver)、废 smoltcp DNS socket，fake-IP 不再依赖系统 DNS 指向 198.18.0.1
  （刀5，ADR-0007）。见下「刀5 完成」。
- **Stage 13 全部完成**：13a TCP via TUIC Connect ✅、13b UDP via TUIC Packet ✅、13c 按需 heartbeat（0-RTT 撞 quinn 0.10 墙、deferred）✅、13d 退役 legacy（删 yamux/自研 server/双轨开关/6 个依赖）✅。
- **刀1/2/3/3.5 完成**（见下各「已完成」段）：并发压测 harness + 大并发优化（脏集合 + 弹性扩容 + fake-IP 回收）+ UDP 直播硬化（quic-stream 兜底 + 分片重组）+ 高码率 UDP（BBR/Cubic 可切 + quinn 插桩 + quic-relay-mode；**纠偏：刀3「5.3M datagram 天花板」实为链路 cap 假象**）。
- **刀6 已在 `main`（`b7785a2`，2026-06-22 fast-forward 合入）**：正交线 A REALITY 第二 Transport 的**第一片**——
  离线 auth 密码学 + TLS 1.3 ClientHello（手写 TLS 1.3，ADR-0008，sans-IO 无 acceptance）。REALITY 是 mini-project（刀6→刀9，见上）。
- **刀7 已在 `main`（`14258e4`，2026-06-23 fast-forward 合入）**：REALITY 第二片——离线 ServerHello 解析 +
  TLS 1.3 key schedule + record AEAD（手写，全程 RFC 8448 §3 KAT 字节级验证，sans-IO 无 acceptance，ADR-0009）。见下「刀7 完成」。
- **刀8 已完成（2026-06-24）+ 真出口 acceptance ✅，已 ff 合入 main（`a9172a0`）**：REALITY 收官片——实 TCP 握手 + 解密 server flight + 证书 HMAC + Finished + VLESS + `RealityUpstream` + env 选择器；**VLESS over REALITY over TCP 在真 sing-box 上端到端跑通**（HTTP 200 三端闭环）。见下「刀8 完成」。
- **刀9 已完成 + 真出口 acceptance ✅，已 ff 合入 main（`831afe3`，2026-06-25）**：REALITY mini-project 收尾 = auto-failover 主链。
  F2 分离 TCP/UDP 上游 + F3 M3 握手并发化 + F1 不对称 failover + F4 idle 超时。全链路 acceptance 通过（~10s 切 REALITY 200 / ~62s 切回 TUIC 200）；
  acceptance 逼出并修了 4 个检测坑（**主动 udp_rx 黑洞探测为主机制**，检测从 >80s→~10s）；两次对抗式 review（零正确性 bug）。**F5 KeyUpdate 拆到刀10**。见下「刀9 完成」。
- **刀10 已完成（2026-06-25）+ ✅ 已 ff 合入 main（`47b69bd`，2026-06-26）**：REALITY mini-project 的最后一片 = F5
  **TLS 1.3 KeyUpdate 密钥轮换**（RFC 8446 §4.6.3/§7.2/§5.3）。`RealityStream` 收到 post-handshake KeyUpdate 从刀8 占位
  loud-fail 改为正确轮换：`HandshakeOutput` 透出 `{s,c}_ap_secret` → 流持两 secret；`decode_one` 内层 `0x16` 逐 message 切，
  KeyUpdate(`0x18`) 调 `on_key_update`（步骤 A 总轮接收 `ExpandLabel(secret,"traffic upd","",32)`；`update_requested(1)`
  则 B1 旧 send key 封回发→B2 轮发送，铁律 B1<B2；`update_not_requested(0)` 只轮接收；非法值前置 Err 零 mutation）。
  poll_read 顶部机会性 flush（AsyncRead 补 `W:AsyncWrite` bound）使纯下载也即时回发。record.rs 不改。
  测试 T16 KAT/T17 时序铁律(crypto-evidence)/T18 recv-only+非法值/T19 端到端 loopback/T20a coalesced record；
  质量门：lib+harness 180 绿 / clippy --all-targets --features harness 0 / release 绿。对抗式 review(5 lens)+/code-review+test-rigor
  **零正确性 bug**（修 1 个 minor：coalesced `[NST][KeyUpdate]` 逐 message 切；1 nit + 注释诚实化）。
  **acceptance**：server-initiated KeyUpdate 不可由客户端诱发、生产服务端极少发 → 以 T19 loopback（真 read/write 路径 + 真
  KeyUpdate + 双向轮换）为高保真替身；真出口 KeyUpdate 未触发，如实记录（brief §8 T20「尽力而为」）。spec=`docs/tech/2026-06-25-knife10-keyupdate-spec.md`，gap 收口见 ADR-0010。
  **刀8 泄漏凭据已服务端轮换（2026-06-26）——安全遗留项关闭。**
- **REALITY mini-project（刀6→刀10）全部完成。刀11 数据面可观测性（observability）✅ 全部完成 + 已 ff 合入 main `9de0604`（2026-06-26，代码 + 两轮 review 零 bug + 真出口 acceptance ✅）**——见下「刀11 完成」。
  **刀12（多核逼近 100M，quantify-only）已完成 + 已 ff 合入 main `68b5e56`（2026-06-27）**（见下「刀12 完成」）——
  LoopProfiler 仪器 + 真出口归因 → **#4（单核 smoltcp poll = 天花板）实测推翻、取消事件循环分片**；当前墙是 WAN 跨太平洋路径，
  100M 此路不可达；#3 连接池留低 RTT 胖链路再测（ADR-0013）。
- **刀13 已完成 + 已 ff 合入 main `8be4141`（2026-06-28）**：主循环热路径净化（见 `docs/tech/2026-06-27-knife13-loop-hotpath-spec.md`）。
  ① 热路径 `println!` 由 `MINI_VPN_TRACE` 门控，默认不再每包/每连接同步写 stdout；② TCP uplink 改非阻塞
  `try_reserve`，Full 时不 `recv`、不分配、保持 smoltcp rx buffer 字节，靠 TCP 窗口端到端背压，修复一条慢流
  HoL 阻塞整个事件循环的问题。质量门：`cargo test` / `cargo test --features harness` / clippy / release 绿；
  新 harness `stalled_tcp_uplink_does_not_block_other_flows` 覆盖慢流不阻塞快流。**下一刀：刀14a 文档收口 + 刀14b 低 RTT 胖链路 #3 量化 gate**
  （spec/plan：`docs/tech/2026-06-28-knife14b-lowrtt-cc-pool-quantify-{spec,plan}.md`；probe：
  `scripts/knife14b-lowrtt-probe.sh`）。
- **2026-06-30 刀14b 真 US-client 测试已跑出决定性结果**：Client=`43.172.75.27`、Exit=`43.153.32.33`、
  Target=`43.130.32.77:5201`，路由和 TUIC 都正确；MTU 1500 forward P1 只有 `476 Kbit/s`，MTU 1200 forward P1
  提升到约 `29-33 Mbit/s`，但 reverse P1 仍只有 `2.06 Mbit/s`，P2+ 出现 iperf result/control reset。
  **裁决：先不要做 connection pool**；P1 reverse 已坏，先做 TCP downlink/backpressure + MTU/MSS 方向。证据和任务树见
  `docs/tech/2026-06-30-knife14b-usclient-results.md`。
- **刀14c 已完成并合到 main (`bda254a`)**：TUN MTU 与 smoltcp capabilities 对齐，补 `MINI_VPN_TUN_MTU` / TCP downlink
  diagnostics，并记录 US-client handoff。14c 后本地仍缺新的 US-client bundle，不能声明 reverse/P2 已修复。
- **刀14d 已完成本地代码 + gates（分支 `codex/knife14d-downlink-reap-open`，commit `75b92af`）**：纯 TUIC `open_tcp`
  不再被视为 cheap，统一走已有 async remote-open 状态机；新增 slow-open harness 锁住“一条慢 open 不阻塞另一条 flow”，
  并补 reap epoch 守卫。下一步是复跑 US-client suite 验证 reverse/P2。
  **一个分支只能一个 writer**，每次 commit 后立即 `git push`（曾发生过并发会话 clobber commit）。
- **2026-07-08 Knife14gq B7 stopped below gate（分支 `codex/knife14d-downlink-reap-open`，commit `8a85ce7`）**：
  feature-flagged buffered downlink controller passed local/remote gates and
  fixed the old `1200B` read-service collapse in focused safe1200
  reverse-first P1 (`remote_read_service_len_min=65536`,
  `remote_batch_limit_bytes_min=524288`, `read_credit_pause_updates=0`), but
  B7 still reached only `18.3/17.2 Mbit/s`, below the `>30 Mbit/s` gate.
  Direct baselines were healthy (`297 Mbit/s` forward receiver,
  `278 Mbit/s` reverse receiver), TUN/QUIC/close-tail/send-error surfaces were
  clean, and attribution stayed `local_downlink_backpressure`. Stop condition
  honored: no B8/B9 was started. Current result doc:
  `docs/tech/2026-07-08-knife14gq-buffered-downlink-results.md`.
- **2026-07-09 Knife14gt G7 passed `30M` and reached `100M+`
  （分支 `codex/knife14d-downlink-reap-open`，commit `4caf60a`）**：
  relay dispatch was aligned with one local egress service window
  (`64KiB -> 128KiB`). The focused safe1200 reverse-first P1 on `.27/.33/.77`
  reached `147/144 Mbit/s`, proving the current branch can move data in the
  `100+ Mbit/s` band. This is not final clean acceptance: the tail logged
  `tx_dropped_delta=783`, `global_rx_queue_used_max=1019/1024`, and
  `terminal_pending_reap_bytes=2653878`
  (`close_pending_class=terminal_closed_no_send`). Next work is to preserve
  this dispatch/egress cadence and make the tail clean. Current result doc:
  `docs/tech/2026-07-09-knife14gt-dispatch-window-g7-results.md`.
- **2026-07-09 Knife14gu G8 failed after RX-edge cleanup attempt
  （分支 `codex/knife14d-downlink-reap-open`，commit `1a3c5cb`）**：
  the first close-tail cleanup stopped extra relay ready-burst reads at the
  `global_rx` critical edge. Local/remote gates passed, but focused safe1200
  reverse-first P1 regressed to `20.2/19.2 Mbit/s`, below `30M`. Tail-drop
  surfaces improved (`tx_dropped_delta=0`, data relay
  `global_rx_queue_used_max=210/1024`), yet the new limiter did not activate
  (`remote_batch_limited=0`). The failure returned to multi-second ordered TUIC
  stream read gaps (`max_remote_read_gap_ms=3610`) and
  `tcp-local-egress-service accepted_bytes=0`. Stop condition: do not start
  another code change from this result alone. Next step is A/B repeat:
  `1a3c5cb` once, then parent `4caf60a` in the same suite shape if the repeat
  stays low. Current result doc:
  `docs/tech/2026-07-09-knife14gu-rx-edge-g8-results.md`.
- **2026-07-09 Knife14gv A/B completed; G7 is not yet reproducible**：
  no code changed. `1a3c5cb` repeat reached `41.4/39.9 Mbit/s`, above `30M`
  but still below `100M`; the RX-edge limiter still did not activate
  (`remote_batch_limited=0`) and the tail showed local downlink backpressure.
  Parent `4caf60a` under the same suite shape did not reproduce G7; it
  collapsed to `0.349/0.151 Mbit/s` with
  `tuic_stream_read_gap+tuic_stream_read_pending+relay_remote_read_gap+target_sender_stalled+tuic_stream_starved`.
  The current active branch is TUIC ordered stream service stability, not
  `global_rx` queue-edge cleanup. Result doc:
  `docs/tech/2026-07-09-knife14gv-ab-repeat-results.md`.

## 目标（唯一北极星）：`Rules.md`

```
① TCP 连接   ② UDP 视频直播   ③ 大并发连接
```
- ① 基本达标（curl HTTPS 端到端 TLS、~415KB 反复下载无 bad-decrypt；TUIC/REALITY 两腿均真出口通过）。
- ② 基本达标于当前真出口条件（刀3/3.5：oversized UDP stream 兜底 + native/cubic 高码率；YouTube 4K soak 通过）。
  仍需在新网络/新服务端上按 acceptance 复测。
- ③ 大并发主路径已修两类 client-side 问题（刀2 脏集合/弹性扩容/fake-IP 回收，刀13 非阻塞 uplink 去跨流 HoL）。
  剩余吞吐杠杆是 **#3 单 QUIC connection / connection pool**，但只在低 RTT、端到端 >100M 胖链路上才值得量化。

> 范围边界：前端/桌面/移动 App + 云端 backend 在**独立仓 `mini_vpn_app`**（契约先行，另一个 session 设计架构）。**core 仓只做数据面**，不碰 GUI/backend；library 化 / `local-control` 接入由前端 session 主导，**不在本路线内**——本路线只把 Rules.md 三目标做达标。

## First: ground yourself

- 读 **`Rules.md`**（三目标）、**本 HANDOFF**、`docs/adr/0004-tuic-protocol-data-plane.md`、`TODO.md`（"Scale & reconnection"、"fake-IP / DNS"、"Mobile readiness" 段）、`.learnings/LEARNINGS.md`（尤其 Stage 12 的并发/echo/定位教训）。
- 关键源（用符号定位，行号会变）：
  - `src/client_tun.rs`：`start_tun_proxy`（单 `tokio::select!` 主循环：`global_rx` TCP 回程 / `device.wait_for_rx` rx 分流 / `tuic_downlink_rx` UDP 下行 / `udp_sweep` / `timer`）；`ListenerRegistry`（SYN inspector 动态建端口池，`MAX_INTERCEPTED_PORTS=64`，每端口 `pool_size` 默认 2）；`process_listener_activity` / `handle_local_payload` / `spawn_remote_relay`（TCP relay 通用回程）；`handle_tuic_udp_uplink`（UDP 上行）。
  - `src/tuic.rs`：`TuicUpstream`（**单条** QUIC 连接，`live_conn` 自重连，`open_tcp` 开 Connect bi-stream，`send_udp`，`start_udp` 下行泵+按需 heartbeat）；`AssocTable`（u16 assoc-id per UDP 4-tuple）；`encode_packet`/`decode_packet`。
  - `src/udp_relay.rs`：`FourTuple`/`FlowEntry`/`parse_inbound_udp`/`build_udp_ip_packet`/`MAX_UDP_FLOWS=1024`/`UDP_FLOW_IDLE_SECS=60`。
  - `src/quic.rs`：client QUIC config（keepalive 5s / idle 30s / initial_mtu 1280 / early_data toggle）。
  - `src/fake_ip.rs`：198.18.0.0/15 池，alloc/resolve，**永不回收**。`src/dns.rs`：本地 fake-A 应答（仅 198.18.0.1:53）。

## Core 路线（按此逐刀，每刀新 session）

```
主线（Rules.md 三目标）
 ├─ 刀1  大并发压测 harness（先定位真瓶颈，事实先行）  ✅ 完成（见下「刀1 已完成」）
 ├─ 刀2  大并发优化（#1 脏集合 + #2 弹性扩容 + fake-IP 引用计数回收）  ✅ 完成（见下「刀2 已完成」）
 ├─ 刀3  UDP 直播硬化（quic-stream fallback + 吞吐压测 + MSS/MTU）  ✅ 完成 + 真出口 acceptance（见下「刀3」）
 ├─ 刀3.5 高码率 UDP（quinn 插桩 + CC 调优）  ✅ 完成 + 真出口 acceptance（见下「刀3.5」）；纠偏：5.3M「天花板」实为链路 cap 假象
 ├─ 刀4  连接成功率（拦截加密 DNS DoT/DoH/DoQ/DoH3）  ✅ 完成 + 真出口 acceptance（见下「刀4」）；first-SYN 已确认 knife2 修复、关闭
 ├─ 刀5  拦全:53 裸包 DNS 劫持（任意 resolver 明文→fake-IP，废 smoltcp DNS socket）  ✅ 完成 + 真出口 acceptance（见下「刀5」，ADR-0007）；已合 main
 ├─ 刀11 数据面可观测性（DNS forge 计数 + datagram drop/背压 + 统一快照 MetricsSnapshot）  ✅ **完成（代码 + 两轮 review 零 bug + 真出口 acceptance ✅）+ 已 ff 合入 main `9de0604`**（见下「刀11 完成」）
 ├─ 刀12 多核逼近 100M：量化定位（quantify-only，LoopProfiler 仪器）  ✅ 完成 + 真出口归因（见下「刀12 完成」，ADR-0013）；**#4 实测推翻、取消分片**
 ├─ 刀13 主循环热路径净化  ✅ 完成 + 已合 main `8be4141`：`MINI_VPN_TRACE` 门控热路径日志 + 非阻塞 TCP uplink（try_reserve，Full 保留 smoltcp 字节）消除跨流 HoL
 ├─ 刀14a 文档/接力收口：把刀13 从候选改为已完成，修 stale TODO/HANDOFF/ADR 指针  ✅ 已完成
 ├─ 刀14b 低 RTT 胖链路 #3 量化 gate：probe/spec/acceptance + US Client VPS 实测  ✅ 已完成；结论是不进 pool
 ├─ 刀14c TCP downlink/backpressure instrumentation + MTU/MSS fix  ✅ 已完成；本地代码/脚本 ready，US-client 复测缺 bundle
 └─ 刀14d async TCP open：TUIC open 移出主循环，保护 downlink/timer/reap progress  ✅ 本地完成；等待 US-client 复测

正交线 A（抗封锁韧性，不阻塞主线；QUIC 被 GFW 封时才必需）= VLESS+REALITY 第二 Transport（手写 TLS 1.3，ADR-0008）
 ├─ 刀6  REALITY auth 密码学 + TLS 1.3 ClientHello 构造（sans-IO，100% 离线 TDD）  ✅ 完成（见下「刀6」，ADR-0008）；已合 main
 ├─ 刀7  ServerHello 解析 + TLS 1.3 key schedule（RFC 8448 向量）+ record-layer AEAD  ✅ 完成（见下「刀7」，ADR-0009）；已合 main
 ├─ 刀8  server-flight 解密 + HMAC 证书校验 + client Finished + 实 TCP 握手 + VLESS 帧 + RealityUpstream(ProxyUpstream) + env 选择器 + 真出口 acceptance
 ├─ 刀9  auto-failover（健康感知 TUIC↔REALITY；分离 TCP/UDP 上游；M3 握手并发化；L2 idle）  ✅ 完成 + 真出口 acceptance ✅（已合 main `831afe3`）
 └─ 刀10 KeyUpdate 密钥轮换（拆出，与 failover 主链零耦合）  ✅ 完成 + 已 ff 合入 main `47b69bd`（loopback acceptance，真出口 KeyUpdate 难诱发如实记录）→ REALITY mini-project 收官
```
- 优先级与关联：**fake-IP 池回收**属"大并发长稳"（并入刀2）；**DoH 拦截**是"真实场景能连上"的前置（刀4，可视情提前——真机浏览器场景不修则 fake-IP 形同虚设）。**A（REALITY）正交**：当前 QUIC 能连，不阻塞三目标达标；TCP-based，替代不了 UDP 直播。

## 后续任务池（post-14d）

1. **复跑 US-client suite**：用同一 Client/Exit/Target 环境复测 14c+14d，目标是 forward 不回退、reverse P1 不再塌缩、P2 不 reset；上传生成的 markdown/tar bundle。
2. **按 bundle 决策下一刀**：若 reverse/P2 仍坏，先读 knife14c diagnostics + loop profile 定位；不要直接写 connection pool。
3. **#3 connection-pool spike**：仅在复测证明 single TUIC/QUIC connection 是墙时开始；否则不加 pool complexity。
4. **移动端/产品化 core 接缝**：packet I/O trait、library config struct、`cc`/`udp_mode` 等旋钮从 env-only 补到可注入配置。
5. **0-RTT / 弱网恢复**：升级 quinn/rustls 以支持 early exporter，并与 adaptive keepalive / mobile radio-sleep 一起评估。
6. **DNS 边界硬化**：IPv6 DNS、split-horizon/internal domains、multi-question DNS、hardcoded-IP app、`hickory-proto` 迁移时机。
7. **抗封锁韧性增强**：QUIC 被封时是否需要 UDP-over-VLESS/TCP fallback；这是延迟/复杂度权衡，不是默认路线。
8. **Scale / Ops**：多 Upstream/service discovery/weighted health/graceful drain/metrics alerting/multi-region。
9. **更远期产品模式**：Multi-Hop、L3 tunnel mode、REALITY Vision flow / 0x1302/0x1303 指纹恢复、出口 IP reputation。

## 刀1 已完成（2026-06-12）：大并发压测 harness + 瓶颈定位

**交付**（分支 `claude/knife1-concurrency-harness`，从 main 起，已逐 commit push；未合 main）：
- 重构：`start_tun_proxy` 抽成 `run_event_loop<D: TunIo, U: ProxyUpstream+DatagramUpstream, M: MetricsSink>`
  （生产/测试同一份循环，零回归）；新增 `TunIo`(device.rs)/`DatagramUpstream`(upstream.rs)/`MetricsSink`。
  client_tun/device/dns/fake_ip 搬进 library（tests/ 整合测试可达）。
- harness：`src/harness.rs`（feature `harness`）= 内存回环 device + mock echo 上游 + 第二 smoltcp 流量发生器，
  对外高层 `run_tcp_scenario`/`run_udp_echo_scenario` → `Report`。`tests/concurrency_harness.rs` 跑 N sweep。
  跑法：`cargo test --features harness --test concurrency_harness -- [--ignored] --nocapture`。
- **定位结论：`docs/tech/2026-06-12-knife1-bottleneck-findings.md`**（spec/plan 同目录 `2026-06-12-knife1-*`）。

**瓶颈裁决（指向刀2）**：
1. ✅ **P0 #1 `all_handles()` O(总 listener 槽数) 全量 sweep**（主因）：relay/call 线性于 `端口×pool`、与活跃连接无关。
2. ✅ **P0 #2 每端口 `pool_size` 硬并发上限**：单端口 pool=2 下 256 路只完成 2/256（热门端口 stall，与 first-SYN-refused 重叠）。
3. ⏸ **#3 单条 QUIC 连接** mock 测不到（无网络拥塞）→ deferred，findings 附端到端 sing-box probe 配方。
4. ✅ **P1 #4 单线程 select 上限**（吞吐随每-tick 开销跌；与 #1 强耦合）。
5. ✅ **P2 #5 128KB/socket**（2048 槽≈256MB，多为 #1 空扫的空闲槽）。

## 刀2 已完成（2026-06-15）：大并发优化 + fake-IP 引用计数回收

**交付**（分支 `claude/knife2-concurrency-opt`，从 main 起，逐 commit push；未合 main）：
- **#1 脏集合驱动**：主循环 relay 段从每 tick 全量 `all_handles()` O(总槽) sweep → 只处理脏集合
  （`dirty: HashSet<SocketHandle>`，inbound TCP 包按 dst_port 标脏 + 回程残留 pending 标脏）。
- **#2 弹性扩容**：`ensure_spare_listeners` 按需补足空闲 Listen 槽（看 smoltcp `state()==Listen`），
  全局 `MAX_TOTAL_LISTENERS=4096` 兜底；打掉每端口 pool 硬上限。
- **fake-IP 引用计数回收**：`FakeIpPool` 每映射 refcount+last_used；TCP（`SocketCtx.fake_ip`）/
  UDP（`AssocTable` id→fake-IP）两条 flow 打通 acquire/release；周期 sweep（60s tick，TTL=1800s）；
  `reap_dead_slots`（1s tick）回收本地关闭/开远端失败的死槽 → 释放 refcount + 槽复用。
- spec/plan/findings 续篇：`docs/tech/2026-06-15-knife2-concurrency-opt-*` + findings「刀2 优化结果」。

**量化（harness，优化前→后）**：#1 relay 段不再随总槽线性翻倍（N=1024 relay 1618ms→71ms，
吞吐 2.50→6.22 Mb/s）；#2 单端口 256 路 done 2/256(20s stall)→256/256(266ms)。
`/code-review`（high effort）8 条 findings 已全部修复（核心：teardown 死槽回收修 refcount/槽泄漏）。

**真出口 acceptance ✅（2026-06-15，深圳 client → 47.251.188.205 sing-box，IP 直连 1.1.1.1:443）**：
- ① TCP+TLS：curl HTTPS `TLS_verify=0`，三端日志闭环（client `relay→rearm` / server `inbound→outbound to 1.1.1.1:443`）。
- ③ 大并发：200 路并发**全压单端口 :443** → `200 301` 全成功、零 `000` 超时（#2 弹性扩容真实生效；优化前此处 stall 2/256）。
- #3 probe：200 路 `time_total` p50=0.379 / p95=0.491 / max=0.557s（max≈1.47×p50，分布极平）→
  **单条 QUIC 连接在此负载下无队头/拥塞瓶颈，暂不需连接池**；更高负载/真直播大流量再评估（归刀3）。

**未做（deferred）**：#4 多线程化（#1 后 poll/smoltcp 段成新瓶颈，留后续评估）；#3 连接池视刀3 更高负载/真直播再定。
CloseWait+远端 keepalive 的半关闭已被 `reap_dead_slots` 覆盖（CloseWait 视为应用关闭 teardown）。

## 刀3 实现完成（2026-06-16）：UDP 直播硬化（quic-stream fallback + 分片重组 + 吞吐压测）

**交付**（分支 `claude/knife3-udp-streaming`，从 main 起，逐 commit push；未合 main）：
- **上行 stream 兜底**：`send_udp` 按 `udp_send_plan(max_datagram_size, len)` 主动分流——装得下走 native
  datagram，超上限/不可用走 **per-packet uni-stream**（`open_uni`/`write_all`/`finish`，复用同一 `encode_packet`
  字节）；datagram `TooLarge` 竞态二次兜底。新增 `udp_stream_fallbacks` 计数。持续大流量直播不再丢大包。
- **下行接收 + 分片重组**：`start_udp` 增 `accept_uni` 分支（有界 `Semaphore`=256，超额丢弃防 flood）；
  `decode_packet_meta`（frag 感知）+ `FragReassembler`（纯状态机，主循环独占）重组 server native 模式的
  大下行分片（FRAG_TOTAL>1）。datagram + uni-stream 两路汇同一下行 channel，主循环统一 decode+重组。
  重复 frag last-writer-wins（缓解跨重连 pkt_id 复用串味，残余 TTL=10s sweep 兜底）。
- **MTU/datagram**：维持 `initial_mtu/min_mtu=1280` floor（不黑洞）；`client_endpoint` 经 `Endpoint::new`
  显式设 `max_udp_payload_size=1472`（接收 headroom，可调）；连上 log `max_datagram_size()`（真上限）；
  每 30s 打 stream 兜底/丢弃统计行。**诚实结论**：发送 datagram 上限主约束是 MTU/PLPMTUD，非 max_udp_payload_size。
- **harness UDP 吞吐**：MockUpstream 加分片回灌模式（模拟 server 分片）；`run_udp_throughput_scenario`
  逐字节核对完整性。常驻测：分片 4000B/4 帧 + 直通各 16/16 intact；ignored sweep 500/500 intact（含 8000B/7 帧）。
- spec/plan：`docs/tech/2026-06-16-knife3-udp-streaming-{spec,plan}.md`；acceptance 配方续写 knife1 findings 末节。

**质量**：80 测全绿、`clippy --all-targets --features harness` 0 warning、release build 绿。
`/code-review`（high effort，7 角度）findings 已修（last-writer-wins 跨重连、去冗余 frag_total 字段、entry API、harness >255 帧断言）。

**真出口 acceptance ✅（2026-06-17，深圳 client → 47.x sing-box → 43.x iperf3，IP 直连，详见 findings 末节）**：
- ✅ **上行 quic-stream 兜底实锤**：1400B 包（>datagram 上限 N=1332）全走 uni-stream → **50Mbps / 0.037% 丢**
  （改造前这些包 100% 被 TooLarge 丢）。刀3 核心目标达成。
- ✅ **典型直播码率达标**：≤5Mbps native datagram 下行 1.7% 丢（视频可用）。
- ❗ **native datagram 有 ~5.3Mbps 硬天花板**（上/下行两方向都卡，与 offered 无关；stream 同链路跑满 50M）
  → 是 QUIC 不可靠 datagram 的传输特性（高 RTT + 不重传 + 无背压），非客户端小改可解。
  **试过下行批量 flush（摊销每包 syscall）→ 零效果，已 revert**（坐实瓶颈不在我方消费端）。
- **观测盲点**：datagram 丢包 quinn 不报错（`send_datagram` 仍 Ok），`udp_drops` 看不到 → 需后续补背压可观测。

**新发现 → 刀3.5（高码率 UDP）**：高码率（>5M）直播需要「高码率流走 stream / datagram 加 pacing+背压 / 评估连接池」，
带 quinn 级 instrumentation（RTT/cwnd/datagram drop）量化后定方向。**#3 裁决**：单连接非连接数瓶颈（stream 跑满 50M），
瓶颈是 datagram 传输特性。

**harness 边界**：测不到真 quinn 的 datagram TooLarge / stream 兜底 / 真分片 / datagram 吞吐天花板（同 #3，需真出口）。

## 刀3.5 代码完成（2026-06-17）：高码率 UDP 硬化（BBR + 插桩 + quic-relay-mode）

**交付**（分支 `claude/knife35-highrate-udp`，从 main 起，逐 commit push；**已 fast-forward 合入 main `591a629`**）：
- **接 BBR**：`congestion_control` 字段（存而未用）→ `quic_transport_config` 的 `congestion_controller_factory`
  （`bbr→BbrConfig`、`cubic→CubicConfig`、未知→Cubic+告警）；env `MINI_VPN_TUIC_CC` 可切（A/B 归因）。已查证 quinn-proto 0.10.6 导出 BBR。
- **quic-relay-mode 接线**：`UdpRelayMode{Native,Quic}` + mode 感知 `udp_send_plan`（`Quic`→恒 uni-stream；
  `Native`→刀3 size-based）；`udp_relay_mode` 字段（存而未用）→ env `MINI_VPN_TUIC_UDP_MODE` 可切。
  **设计依据**（SPEC 已查证）：server 按 assoc **首包** mode 镜像下行 → `Quic` 全 UDP 首包即 stream → 下行也镜像 stream，摆脱 datagram 天花板。下行接收（`accept_uni`/`FragReassembler`）刀3 已就绪、不改。
- **抬 `max_concurrent_uni_streams` 100→4096**：避 TUIC issue #221（per-packet uni-stream 耗尽配额 → 下行塌缩）。
- **quinn 级插桩**：30s `📊` 行加 `RTT/cwnd/lost/sent`（`conn.stats().path`）+ `send_buffer_space` 背压代理信号
  （补刀3 盲点：datagram 缓冲溢出丢最老不报错）；连上打实际生效 CC + mode。
- spec/plan：`docs/tech/2026-06-17-knife35-highrate-udp-{spec,plan}.md`；acceptance 配方续写 knife1 findings 末节（T-A~T-H）。

**质量**：82 lib 测 + 6 harness 常驻测全绿、`clippy --all-targets --features harness` 0 warning、release build 绿。
`/code-review`（high effort，7 角度）findings 已修 3 条（A: fallback 计数只算 Native 真兜底，避 quic 模式 `📊` 误读；
B: 背压警告门控 Native；C: 去重 MTU floor 常量）。

**真出口 acceptance ✅（2026-06-17，深圳 client → 47.x sing-box → 43.x iperf3，**两端链路升到 80M**）**：
- **🔑 最大纠偏**：刀3「~5.3M datagram 硬天花板」是 **5M VPS 链路 cap 的测量假象**，非 QUIC datagram 限制。
  80M 链路下 native datagram 下行 **39.8M/0.25%**、上行 37.5M/4.5%。**插桩（cwnd/RTT/loss）揭穿真相**（先量化、别凭猜）。
- **CC 裁决**：datagram 数据面 **Cubic 完胜 BBR**（40M 下 0.25% vs 24%；BBR cwnd 暴涨 245K/RTT bufferbloat、
  对不可靠 datagram 过驱）。→ **默认改 cubic**（`MINI_VPN_TUIC_CC=bbr` 仍可显式选）。
- **mode 裁决**：**默认 native（datagram）**——4K(25M) 富余且低延迟；quic 全 stream 模式高码率灾难（40M→7M/71%，cwnd 4.5MB）。
  **quic 模式保留为可配置选项**（代码完成+测过；抗封锁场景或有用，非高码率推荐）。
- **多 flow gate**：2 路并发单连接聚合 ~34M ≥ 33M → **连接池 defer 坐实**。
- **carve-out 不需要**：默认 native → DNS/小流本就走低延迟 datagram。
- ADR：`docs/adr/0005-cubic-over-bbr-datagram.md`（CC 选择 + 天花板假象纠偏）。findings 末节有完整数据表。
- **T-H 真实 soak ✅**（专用测试机，native+cubic）：YouTube **4K 不卡顿**；累计丢包 ~0.31%、`丢弃=0`、RTT ~170ms 稳、
  无重连风暴/映射丢弃洪水；末尾一次 PMTU/拥塞事件被大包 uni-stream 兜底优雅吸收。carve-out 不需要（DNS/小流走 datagram）。
- acceptance helper 入库：`scripts/knife35-acceptance.sh`（可移植，start/soak/stop/soak-stop，凭据读 env）。

**本刀的真实价值**（前提被纠偏后）：① quinn 级插桩（揭穿假象 + 纠正 CC）；② CC 调优（默认 cubic）；
③ 证实 native datagram 本就够高码率、避免上线不必要的全-stream 复杂度；④ quic-relay-mode 能力（备用/抗封锁）。

**code-review defer（非本刀阻塞，后续按需）**：
- `from_sources` 未收 `cc`/`udp_mode` 参数 → 仅 env 可切；**前端/移动端经 file/FFI 注入 config 时需补**（`TuicClientConfig` 字段注释已述 FFI 注入计划）。
- `parse_cc`（返回 `(choice,bool)`）与 `UdpRelayMode::parse`（返回 `Option`）双 idiom + connect() 两段近似 warn 块 → 可统一（纯美化）。
- `max_concurrent_uni_streams=4096` 经共享 `quic_transport_config` 也作用于 legacy `client_quic_config`（仅测试用、无害；ceiling 非预分配）。
- `udp_drops` 混合 datagram-send-fail 与 uni-stream-fail 两类（acceptance 归因时留意）。
- **acceptance 后**：按 gate 定默认 mode → 补 `docs/adr/0005-*`；按 T-F/T-H 定是否补 DNS/小流 carve-out。

## 刀4 代码完成（2026-06-18）：连接成功率（拦截加密 DNS）

**交付**（分支 `claude/knife4-connect-success`，从 main 起，逐 commit push；**已 fast-forward 合入 main `cd9ff62`**）：
- **对症**：浏览器/系统用**加密 DNS**(DoH:443/DoT:853/DoQ:UDP853/DoH3:QUIC443)拿真实 IP → 绕过 fake-IP →
  真实 IP 没进隧道 → GFW 墙 → **连接失败**。
- **做法**：新 `src/dns_block.rs`（`is_encrypted_dns_port`/`is_doh_domain`/`is_doh_ip` + 内置 DoH 域名/IP 名单）；
  `resolve_target` 加 **`Block`** 变体(:853 任意 IP / :443+fake-IP 域名∈DoH名单 / :443+非fake IP∈DoH-IP名单)，
  一处决策天然覆盖 TCP+UDP 两路径。**TCP→RST**(复用 `rearm_socket`)、**UDP→静默丢包**(热路径勿 println)。
  逼应用回落明文 :53 → 我方伪造 fake-IP → 进隧道。:443 **仅按名单精确判**，不碰普通 HTTPS/QUIC。
- **质量**：87 lib 测绿、`clippy --all-targets --features harness` 0 warning。`/code-review`(9 角度)findings 已处理
  （真 bug：UDP Block 逐包 println 洪水 → 改静默丢弃；补 dns.google.com）。
- **设计文档**：`docs/tech/2026-06-18-knife4-connect-success-{spec,plan}.md`；ADR `docs/adr/0006-block-encrypted-dns.md`。

**deferred（grill 决策）**：
- ~~**拦全 :53**（任意 resolver 明文查询都伪造）~~ → ✅ **刀5 已做**（裸包 DNS 路径、废 smoltcp DNS socket、ADR-0007）；
  无缝 on/off 不依赖系统 DNS 的关键拼图就位（配合前端 NE）。
- **first-SYN-to-fresh-fake-IP refused**：静态分析表明已被 **knife2 同帧 `ensure_port`+`ensure_spare_listeners` 修**
  （HANDOFF 原条目疑陈旧）→ 仅 acceptance 探针验证(`curl rc=7≈0`)，复现才回头查。
- **harness Block 端到端**：harness 连固定 TARGET_IP、FakeIpPool 不可注入 DoH 映射 → 降级 acceptance（Block 决策已全分支单测）。

**真出口 acceptance ✅（2026-06-18，深圳测试机）**：
- **K4-A DoH 拦截**：Chrome 开「安全 DNS=Cloudflare」→ `🛡️ 阻断加密 DNS cloudflare-dns.com(@fake-IP:443) → RST` 命中
  (域名识别经 fake-IP 真生效)→ 浏览器回落明文 → fake-IP → 正常上网。
- **K4-C 回归**：DoH 关 → 明文 DNS 健康(FB/IG/YT 全 `🪪→fake-IP`)、无误伤。
- **K4-D first-SYN**：探针 375 总 / rc=7=**0** → 竞态不复现、**确认 knife2 已修**(HANDOFF 原条目陈旧、关闭)。
- 小改：TCP block 日志显**解析域名**(便于核对/调名单)。**→ 刀4 完成**（代码+单测+ADR-0006+acceptance）。

## 刀5 代码完成（2026-06-22）：拦全 :53 裸包 DNS 劫持

**交付**（分支 `claude/knife5-dns-hijack`，从 main 起，逐 commit push；**已 fast-forward 合入 main `e589767`**）：
- **对症**：刀4 逼应用回落明文 DNS，但应用回落到的是**它自己配的 resolver**（如 `8.8.8.8:53`），非 198.18.0.1。
  原 `classify_inbound` 仅伪造 `198.18.0.1:53`、其它 :53 隧道转发真 DNS → 真实 IP 绕过 fake-IP（仅"模型 a 系统 DNS=198.18.0.1"下不漏）。
- **做法**（grill 4 裁决 + ADR-0007）：① **裸包**——`classify_inbound` 任意 `:53`→`Dns`，新 `forge_dns_reply`(纯)
  伪造 fake-IP 回包(`src=被查询的 resolver`)，`handle_dns_hijack` 经 `inject_ip_packet`+`flush_tx` 注入（复用 UDP relay 下行机制，
  smoltcp 无法为无界 resolver IP 设 src）。② **废 smoltcp DNS socket**——删 `dns_handle`/`bind`/接口 IP `198.18.0.1/32`/`drain_dns`/
  `FAKE_DNS_RESOLVER`，统一一条裸包路径（含 198.18.0.1）。③ **全劫持**不按 dst 过滤。④ **TCP :53 → RST**：
  `dns_block::is_dns_relay_port`(53||853) → `resolve_target` Block（不变量：UDP :53 已被 classify 截走 → port==53 只命中 TCP）。
- **质量**：93 lib 测（含 forge_dns_reply 5 测）+ 6 harness 测绿、`clippy --all-targets --features harness` 0 warning、release build 绿。
  `/code-review`(high effort,7 角度) **零正确性 bug**（独立追踪确认 UDP :53 永不到 resolve_target）；唯一动手=在 classify_inbound 标注 load-bearing 不变量。
- **设计文档**：`docs/tech/2026-06-22-knife5-dns-hijack-{spec,plan}.md`；ADR `docs/adr/0007-hijack-all-plaintext-dns.md`；CONTEXT.md 词汇表更新。

**真出口 acceptance ✅（2026-06-22，测试机，native+cubic 全局隧道，系统 DNS=8.8.8.8 非我方 resolver）**：
- **K5-1 核心**：`dig @8.8.8.8 example.com` → `198.18.0.36`(fake-IP) → **系统 DNS≠198.18.0.1 时任意 :53 仍被劫持，北极星达成**。
- **K5-2**：`dig +tcp @8.8.8.8` → connection reset、无 IP（TCP :53 RST，无 real-IP 泄漏）。
- **K5-3**：`curl https://example.com` → HTTP/2 200（fake-IP→DomainPort→隧道）。
- **K5-4**：google/github/cloudflare 全 fake-IP，零逃逸。**K5-5**：apple/icloud/google 等真实 app `🪪→fake-IP`。
- **刀4↔刀5 闭环**：`dns.google` 明文解析→fake-IP，该 fake-IP:443 再命中刀4 DoH Block。
- helper：`scripts/knife35-acceptance.sh soak-knife5`（DNS=8.8.8.8 + alt-resolver 路由进 TUN）。
- **已知限制**（未触发）：split-horizon/内网域名走出口解析、exotic 多 question 查询丢弃(不泄漏)、IPv6 :53 不劫持(crate ipv4-only)。
- **→ 刀5 完成**（代码+单测+ADR-0007+acceptance）。详见 findings 末节「刀5」。

## 刀6 代码完成（2026-06-22）：REALITY 第二 Transport — 离线 auth + ClientHello（mini-project 第一片）

**交付**（分支 `claude/knife6-reality-transport`，从 main 起，逐 commit push；**已 fast-forward 合入 main `b7785a2`**；本片 **sans-IO、100% 离线**，无真握手/无 acceptance）：
- **背景**：正交线 A = 给 Upstream 加第二 Transport（VLESS over REALITY over TCP，抗封锁 fallback）。REALITY 把 auth 藏进 TLS ClientHello `session_id`，stock TLS 库不让写 → **手写 TLS 1.3**（shoes 蓝本），RustCrypto 仅作原语（不破 ADR-0003 单 rustls）。grill 决策：**boring/craftls 均否决**（boring 写不了 session_id 需 patch C；craftls 只给指纹）→ 手写（ADR-0008）。
- **本片做了**（`src/reality/{auth,client_hello}.rs`）：① `auth`：x25519 ECDH(RFC 7748 KAT)、`derive_auth_key`=HKDF-SHA256(salt=random[0..20],info="REALITY",32B)、session_id 16B 布局、`seal/open_session_id`=**AES-256-GCM 完整 32B key**(nonce=random[20..32],AAD=session_id 清零的 ClientHello)、`verify_server_cert`=HMAC-SHA512(同 32B key, ed25519 pubkey)；② `client_hello`：手写 TLS 1.3 ClientHello(Chrome-like:GREASE+X25519 keyshare+ALPN+扩展序;supported_versions 仅 1.3)、`build_authed_client_hello`(建零 session_id→seal→回写 offset 39..71)。
- **质量**：12 reality 单测（含 RFC 7748 KAT + **server-view round-trip**：ECDH→derive→encode→seal→AAD清零→解封全链 + 篡改 ClientHello→解封失败）+ 105 lib 测全绿、clippy 0 warning。`/code-review`(high effort)：零正确性 bug，修了过时 AES-128 文档(实为 AES-256)、命名 session_id 偏移常量、x25519 低阶点安全注记。
- **🔑 查证锁定的互通关键（刀7/8 别再踩）**：REALITY session_id AEAD = **AES-256-GCM + 完整 32B AuthKey（不截断！）**；用 AES-128/截断会让 sing-box 静默拒绝回落 decoy。HKDF salt=random[:20]、info="REALITY"、L=32。AAD = handshake message（含 4B 头），session_id 区 32B 清零。证书校验 HMAC-SHA512 用同一 32B key。
- **设计文档**：`docs/tech/2026-06-22-knife6-reality-transport-{spec,plan}.md`；ADR-0008；CONTEXT.md 加 Transport/VLESS/REALITY。
- **deferred（刀7/8/9）**：ServerHello+key schedule+record（刀7）；server-flight 验证+Finished+实握手+VLESS+RealityUpstream+acceptance（刀8，需服务端 VLESS+REALITY inbound 空 flow）；failover（刀9）。**刀7 起 x25519 用于网络 server keyshare → 必须加 contributory/全零检查**（见 auth.rs 注）。

## 刀7 代码完成（2026-06-23）：REALITY 握手核心 — 手写 TLS 1.3 ServerHello/key schedule/record（第二片）

**交付**（分支 `claude/knife7-reality-handshake`，从 main 起，逐 commit push；**已 fast-forward 合入 main `14258e4`**；**sans-IO、100% 离线，无 acceptance**）：
- **设计输入**：understand-phase research **workflow**（5 路并行研究 + 综合，brief 见 session）→ spec/plan/ADR-0009。
- **本片做了**（`src/reality/{key_schedule,record,server_hello}.rs` + `testutil.rs`）：
  - `key_schedule`：HKDF-Expand-Label/Derive-Secret/Extract/transcript_hash；`derive_handshake_keys`（Early→derived→Handshake(from ECDHE)→{c,s}_hs→key/iv，**全零 ECDHE 拒**）；`compute_finished_verify_data`；`derive_application_keys`（derived2→Master→{c,s}_ap）。
  - `record`：AES-128-GCM record AEAD（per-record nonce、5B 头 AAD、inner type+剥尾零、读/写独立 seq）。
  - `server_hello`：`parse_server_hello`（提 cipher/key_share/version + 拒 HRR/downgrade/compression/version/echo/**长度字段**/**cipher≠0x1301**）。
- **质量**：31 reality 单测（全 **RFC 8448 §3 字节级 KAT**：HkdfLabel、握手+应用密钥链、finished_key、server Finished verify_data、**server-flight record open golden KAT**、ServerHello 解析 + tls-parser 交叉验证）+ 124 lib 测全绿、clippy 0 warning。`/code-review` high effort：cipher/长度 guard + ADR-0009 如实化（修了「泛型骨架」overclaim）；x25519 全零检查等经 verify REFUTED。
- **🔑 刀8 别再踩**：TLS 握手 ECDHE = x25519(client 临时, **server 临时** keyshare from SH)，≠ 刀6 AuthKey 的 (client × server **静态** pbk)；**x25519 全零拒已在 `derive_handshake_keys`**；record cipher = AES-128-GCM（≠ 刀6 session_id seal 的 AES-256）；**cipher≠0x1301 已在 parse 层拒**（0x1302/0x1303 是 ADR-0009 gap）。
- **设计文档**：`docs/tech/2026-06-23-knife7-reality-handshake-{spec,plan}.md`；ADR-0009（cipher 范围 + echo≠auth 不变量）。
- **deferred（刀8）**：实 TCP 握手 + 读写循环 + 跳明文 dummy CCS（不进 read-seq）；用 record/key_schedule 解密真 server flight；X.509 DER 提 ed25519 pubkey+sig → 刀6 `verify_server_cert`（REALITY auth 决策）；CertificateVerify ed25519 检；server-Finished MAC 验 + 发 client Finished；app keys（刀7 已就绪）；VLESS 帧（空 flow）；`RealityUpstream`(ProxyUpstream open_tcp) + env 选择器 + 真出口 acceptance（需 sing-box VLESS+REALITY inbound 空 flow）。可能需 x509 parser crate。

## 刀8 代码完成（2026-06-24）：REALITY 收官 — 实握手 + VLESS + RealityUpstream + **真出口 acceptance ✅**

**交付**（分支 `claude/knife8-reality-live-handshake`，从 main 起，逐 commit push；**已 ff 合入 main `a9172a0`**；**REALITY mini-project（刀6→9）的收官片，VLESS over REALITY over TCP 端到端跑通**）：
- **设计输入**：understand-phase 研究 workflow（5 路并行 + 20 条互通-critical 断言对抗验证）→ brief；grill 6 裁决（见 spec §2）。
- **新增**（`src/reality/{handshake,vless,cert}.rs` + `src/reality_upstream.rs`）：
  - `vless`：`encode_vless_request`（空 flow，**PortThenAddress** + ATYP v4=01/domain=02/v6=03，**不复用 tuic**）+ `VlessResponseStripper`（动态 2+addons_len 首读剥）。
  - `cert`：`extract_ed25519_pubkey_and_sig`——**手解 DER** 扫 ed25519 SPKI marker 取裸 32B 公钥 + 取 leaf DER 末 64B 签名（**不碰 Validity**；见下 acceptance 真因）。
  - `handshake`：`drive<S:AsyncRead+AsyncWrite>`（编排 spec §5 时序）+ `RecordReader`（逐 record，跨 read 缓冲）+ `HandshakeReassembler`（内层 0x16 跨 record 重组）。**H1 cert-seen 守卫**：无通过校验的 Certificate 不许完成握手（防 EE+Finished 的 decoy 绕过 auth）。
  - `reality_upstream`：`parse_pbk`（base64url+std→强断言 32B）+ `RealityClientConfig`（脱敏 Debug）+ `RealityStream`（AsyncRead/Write over TLS1.3 app record + VLESS 响应 strip + post-handshake drop + KeyUpdate loud-fail）+ `RealityUpstream`（`ProxyUpstream::open_tcp` 每 TCP 一次完整握手 + 10s 超时；`DatagramUpstream::send_udp` no-op）。
  - `client_tun`：`MINI_VPN_UPSTREAM=tuic|reality` 选择器（默认 tuic，零回归）+ reality 空 downlink channel（持 tx 永不 send）。
- **质量**：161 lib 测全绿（每个互通-critical 字节 KAT；RFC 8448 §3 握手 drive e2e；**loopback 全 REALITY 握手 e2e**=测试内 REALITY server 模拟器走通真 verify_server_cert + VLESS 往返）+ clippy 0 warning + release 绿。`/code-review`（多 agent 对抗式，16 confirmed findings）已修：H1(auth bypass 守卫)/H2(握手超时)/M1(KeyUpdate loud-fail)/M2(relay shutdown)/M4(截断报错)/L1-L7；deferred → 刀9：M3(握手并发化,H2 超时止血)/L2(relay idle 超时)。
- **🔑 真出口 acceptance ✅（2026-06-24，深圳 client → 47.x sing-box VLESS+REALITY inbound，借用站 gateway.icloud.com）**：curl HTTPS 经 REALITY 隧道 **HTTP 200**（cloudflare trace 见 VPS 出口 IP=三端闭环）+ client 日志 `🔐 REALITY 握手成功（证书 HMAC 校验通过）`（**真 HMAC，非 echo 充数**）；多目标并发握手成功；force-reality 下 UDP no-op 符合预期。
- **🔑 acceptance 抓出的两个互通 bug（离线测全绿但真出口才暴露 —— "宽容方收、严格方拒"，坐实真出口纪律必要）**：
  1. **重复 GREASE 扩展类型**：两个 GREASE 扩展都用 type 0x0a0a（违反 RFC 8446 §4.2）→ Apple/tls-parser 宽容但 sing-box 的 Go-tls 严格解析器**拒整个 ClientHello → REALITY auth 前回落 decoy**。修：尾部 GREASE 改 0x1a1a（真 Chrome 同法）+ 回归守卫 `no_duplicate_extension_types`。
  2. **GeneralizedTime 证书**：真 sing-box 临时证书 Validity 用 GeneralizedTime（notAfter≥2050）→ x509-cert 严格 RFC 5280 拒。修：改回**手解 DER 定点提取**（不碰 Validity，反转 grill 裁决 a、印证 brief 原判；去掉 x509-cert 依赖）+ GeneralizedTime fixture 回归测试。
  - **诊断链**（教学价值）：用真服务端私钥在 Rust 证明客户端密码学 100% 正确（AuthKey 匹配、session_id 可解）→ 排除凭据/AAD/keypair → 锁定"CH 被严格 Go 解析器拒" → dump CH 发现重复 GREASE → 修 → 穿过 decoy → 撞 GeneralizedTime → 手解 DER。
- **设计文档**：`docs/tech/2026-06-23-knife8-reality-live-handshake-{spec,plan}.md` + `2026-06-23-knife8-research-brief.md` + `knife8-singbox-server-setup.md`；ADR-00010（CertVerify defer + KeyUpdate gap + cert 提取反转）；ADR-0009 修订（收紧 cipher offer 0x1301）。acceptance helper `scripts/knife8-reality-acceptance.sh`（preflight/soak/smoke/soak-stop + openssl 0x1301 出口预检）。
- **deferred（刀9）**：auto-failover（健康感知 TUIC↔REALITY）；分离 TCP/UDP 上游；UDP-over-VLESS；连接复用（每 TCP 一次握手）；握手并发化（M3，移出主循环 spawn）；relay idle 超时（L2）；KeyUpdate 密钥轮换（ADR-0010 gap）；0x1302/0x1303（ADR-0009 gap）；Vision flow。
- **⚠️ 安全 note**：acceptance 期间一份服务端凭据（reality private_key/uuid/short_id）曾被误提交进 `docs/tech/knife8-singbox-server-setup.md`（commit 5ded2a2）并推 origin → 已 force-push 重写历史清除（HEAD a928125）+ 文件改占位符；**该 keypair 须在服务端轮换**（私钥上过远端=已暴露）。

## 刀9 完成（2026-06-25）：auto-failover 主链 + M3 + L2 + 真出口 acceptance ✅

**交付**（分支 `claude/knife9-auto-failover`，从 main `a9172a0` 起，逐 commit push；**未合 main**；REALITY mini-project 收尾）：
- **设计输入**：understand-phase research **workflow**（5 路并行研究 + 3 路对抗式核验 + 综合落盘 `docs/tech/2026-06-24-knife9-research-brief.md`）+ grill 4 裁决。spec/plan：`docs/tech/2026-06-24-knife9-auto-failover-{spec,plan}.md`。ADR-0011。
- **F2 分离 TCP/UDP 上游**（`src/failover.rs`，commit `423d79d`）：`FailoverUpstream<T,R>`（泛型，贴合本仓单态化惯用法 + 可注入 mock）impl `ProxyUpstream`（open_tcp 选腿）+ `DatagramUpstream`（**send_udp 恒走 tuic**，F2 硬约束一处钉死）。`MINI_VPN_UPSTREAM=failover`（**opt-in**，默认/未设仍纯 TUIC 零回归；`tuic`/`reality` 作强制单腿旁路）。
- **F4 relay idle 超时（L2）**（commit `9ea70f1`）：`spawn_remote_relay` 抽 `run_relay` + select 加 idle 分支（90s 双向静默 → 退出 + `stream.shutdown`，防慢/卡死上游泄漏）。dev-dep tokio 加 `test-util`（start_paused 确定性测；"full" 不含，已知坑）。
- **F1 不对称 auto-failover**（commit `d64d514`）：`HealthProbe` trait（probe=live_conn 非浅探 / is_dead=close_reason）TuicUpstream impl；`FailoverState` 决策方法收 `now_secs`（可注入时钟确定性单测）；**down 快路（连接死 is_dead）1 次切 / 慢路连续 3 次切 + 成功清零**；**up 连续 3 探针成功 + 60s 冷却切回**（不对称迟滞防 flap）；后台 `spawn_health_probe`（仅 REALITY 当班、30s 节奏）。**铁律**：send_udp 永不读 state（结构性）。
- **F3 M3 握手并发化**（commit `d19c482`）：把昂贵的 REALITY 多-RTT 握手 spawn 出单任务 select 主循环。`ProxyUpstream::open_is_cheap()`（默认 true=inline 零回归；REALITY=false；**FailoverUpstream 恒 false**=失败模式 TUIC reconnect 也不廉价 + 消除 TOCTOU，见下 review）。`SocketState::HandshakePending`（spawn 在飞态，与 inline `OpeningRemote` 区分→reap 不误杀在飞握手）+ `conn_epoch` 防串话（进 +1、rearm +1，`handle_handshake_done` **先比 epoch** 再看状态）+ `uplink_buffer`（256KB 上限，握手期上行缓存、成功后按序 flush）+ `HandshakeDone` channel（cap 128）回灌。fake-IP：spawn 时 acquire、rearm 时 release（平衡）。
- **质量**：176 lib 测 + 6 harness 测全绿、clippy `--all-targets --features harness` 0 warning、release 绿。**对抗式 code-review workflow（41 agent / 7 角度 / 1-vote 核验，commit `546e715`）5 findings 全修**：F1 TOCTOU 深修（FailoverUpstream `open_is_cheap` 改恒 false → 所有 open 含 seamless 重试/黑洞 reconnect 都 spawn 出主循环，纯 TUIC 默认仍 inline）；F5 switch 用 `compare_exchange`（恒 spawn 后 record_tuic_failure 可并发）；F2 try_send 失败 log+rearm 不静默丢；F3 spawn 入口 buffer_uplink 检返回；F4 reap 两次 sockets.get() 合一。
- **🔑 真出口 acceptance ✅（2026-06-25，深圳 client → 47.x VPS，TUIC :8443 + REALITY :443 两腿）**：全链路闭环通过——
  ① TUIC 当班 curl HTTPS 200（出口 IP=VPS）；② pfctl 双向封 TUIC UDP → **主动黑洞探测 ~10s** 日志 `🔀 TUIC 黑洞... → 切到 REALITY`；
  ③ 切后 curl **HTTP 200**（`🔐 REALITY 握手成功` + `▶ leg=REALITY`，DNS 不饿死）；④ 恢复 TUIC → **~62s 切回**（冷却迟滞）；
  ⑤ 切回后 curl 200（`▶ leg=TUIC`）。**🔑 acceptance 抓出 4 个离线测不到的检测坑（idle/open-success 对 QUIC 黑洞不可靠）**，
  全修并坐实「主动 udp_rx 探测才是可靠主机制」（见 ADR-0011 §3b + 下「检测修订」）。helper：`scripts/knife9-failover-acceptance.sh`。
- **第二次对抗式 review（检测修复 diff，23 agent / 含并发-死锁专项，commit `a8bfb9f`）**：**零正确性 bug**（try_lock/CAS/检测状态机扛住），4 条 cleanup/altitude 全修（注释陈旧 idle/rx_datagrams 也改 try_lock 非阻塞/常量澄清/reset 注释）。
- **🔑 检测修订（acceptance 复测 4 轮逼出，commit `8287cb5`→`79ef068`）**：idle/open-success 检测被 ① open 写小 Connect 头黑洞下乐观返 Ok
  （重置慢路计数）② keepalive 架空 idle（close_reason >80s，keepalive 不能删=保活长连接）双重架空。**主修=主动黑洞探测**：
  quinn `stats().udp_rx.datagrams` 当存活信标（健康每 ~5s 有 keepalive ACK→rx 增；黑洞→停滞），`BlackholeDetector` rx 停滞 ≥10s
  → 切 REALITY（~10-13s）。配套：重连 5s 超时 + open_tcp 5s 超时 + idle 30s→15s（备机制）；**`send_udp` 改 `current_conn`
  非阻塞**（try_lock+不重连，黑洞期不 stall 主循环饿死 DNS，重连交后台 start_udp）；`spawn_health_probe`=down(rx 停滞)+up(探针)统一任务。
- **（原 runbook 验证项，已全过）**：`MINI_VPN_UPSTREAM=failover` + 两腿凭据 → 跑 client-tun。验证：
  1. **F1 down**：TUIC 正常 curl HTTPS 200 → 人为打断 TUIC（client 侧 pfctl 封 outbound UDP 到 VPS:8443，或 server 侧停 QUIC）→ 日志 `🔀 failover：TUIC ... → 切到 REALITY` + curl 仍 200（cloudflare trace 见 VPS 出口）；
  2. **F1 up**：恢复 TUIC → 60s+ 后日志 `🔀 切回 TUIC 主腿`；
  3. **UDP**：TUIC 当班 `dig` over QUIC datagram 通；REALITY 当班 UDP 丢（符合预期，UDP 永绑 TUIC）；
  4. **F3 不 stall**：REALITY 当班多并发 curl，一条慢握手不拖垮其余（对比 inline 基线）；
  5. **F4 idle**：relay 静默 90s 自动清理。
  helper：**`scripts/knife9-failover-acceptance.sh`**（`soak`/`cut-tuic`/`restore-tuic`/`smoke`/`udp-check`/`status`/`soak-stop`；
  两腿 env + pfctl 按端口阻断 TUIC UDP 不碰 REALITY TCP）。流程印在 `soak` 末尾。
- **deferred（刀10+）**：**F5 KeyUpdate 密钥轮换**（与 failover 主链零耦合、单独成刀；brief §6 有 V1 字节级核验的精确规范：label `"traffic upd"`/seq 归 0/收 update_requested 必回发且**旧 send key 先封装再换密钥**/`AppKeys` 已暴露 c/s_ap_secret）；UDP-over-VLESS；连接复用；指数退避；0x1302/0x1303。

## 刀11 完成（2026-06-26）：数据面可观测性 — Arc<Metrics> + MetricsSnapshot 契约 + 30s 📊 快照

**交付**（分支 `claude/knife11-observability`，从 main `6ba6d42` 起，逐 commit push；**已 ff 合入 main `9de0604`**；主线量化底座）：
- **设计输入**：grounding workflow（5 接缝并行核实）+ 设计综合（seed §4 五开放问题逐一裁决）→ spec/plan/ADR-0012。
- **新 `src/metrics.rs`**：进程级 `Arc<Metrics>`（原子，唯一桥接 run_event_loop task ↔ TuicUpstream::start_udp task）=
  累计 counter（`inc_*` fetch_add Relaxed）+ 发布式 gauge（`set_*` store；loop 30s tick 从单写者 socket_ctxs/fake_pool 重算后发布）；
  `MetricsSnapshot` 纯值 Copy 契约（前端用，无 serde）；`note_pressure_edge` 纯沿 helper。**不扩 MetricsSink**（计时正交，NoopSink 仍零开销）。
- **指标**：DNS `dns_forged`/`dns_dropped`（`handle_dns_hijack`，纯函数 `forge_dns_reply` 不碰）；UDP 下行 `udp_drops_down`
  （accept-uni 溢出 + read None，**与上行 udp_drops 严格分离**）；`datagram_pressure_events`（背压 false→true 上升沿，task-local latch）；
  `relays_spawned`（`spawn_remote_relay` 唯一入口）；gauge `active_relays`（state==Relaying）/`fake_ip_active`/`fake_ip_total`
  （`FakeIpPool::usage()`）/`failover_leg`（`ProxyUpstream::failover_leg_u8()` 默认方法，非 failover→`NO_FAILOVER`）。
- **发射**：run_event_loop 新 30s `metrics_tick` → `publish_gauges` → `snapshot` → **无门控**打统一 `📊` 行；既有 start_udp
  UDP-path `📊` 行原样保留、各司其职（ADR-0012 §5）。
- **质量**：193 lib + harness 测全绿、`clippy --all-targets --features harness` 0 warning、release 绿。**两轮 review 零正确性 bug**：
  对抗式 review workflow（5 维度 × 逐条对抗式核验，28 agent / default-refute → 23 findings 全 not-a-bug）+ `/code-review` high effort
  （8 角度 → 仅 cleanup 建议，逐条权衡后不动：扩 blast radius / 耦合 feature gate / 纯偏好，稳定优先）。
- **设计文档**：`docs/tech/2026-06-26-knife11-observability-{spec,plan,seed}.md`；ADR-0012；CONTEXT.md「Metrics snapshot」词汇；
  findings 末节「刀11」（含 `📊` 行格式 + acceptance 配方）。
- **真出口 acceptance ✅ PASS（2026-06-26，深圳真机 → 47.x sing-box，TUIC+REALITY 两腿）**：`📊` 行真负载下全部指标非 0 且单调/正确——
  `dns_forged` 147→171→210 / 153→176→198、`relays_spawned`(累计) 95→129、`active_relays` 17~43、`fake_ip 在册` 35→60、
  `failover_leg` 纯TUIC=`-`·cut后=`REALITY`(+4 真 REALITY 握手)、`udp_drops_up`=5(cut 封锁窗口吻合)；`udp_drops_down`/背压=0
  （刀3.5 已证 native+cubic datagram 够用、未触发，如实记录）。两处仅采样时机漏（短突发+`sleep<周期`、切回冷却~90s>sleep70），非失败。
  **测试单**=`docs/tech/2026-06-26-knife11-acceptance-checklist.md`；env 旋钮 `MINI_VPN_METRICS_SECS`（默认 30，acceptance 设 5）。
  详见 findings 末节「刀11」。
- **deferred / 已知边界**：① UDP 下行 drop/背压的 I/O 触发点 harness mock 不覆盖 → 归 acceptance；② NODATA（AAAA）按 `Some=forge`
  计入 `dns_forged`，如需区分留 `dns_nodata`（破纯性，defer）；③ 前端读取通道（IPC/local-control）留前端 session（本刀只导出 snapshot 值）。

## 刀12 完成（2026-06-27）：多核逼近 100M 量化定位 — LoopProfiler + 真出口归因（quantify-only）

**交付**（分支 `claude/knife12-multicore-100m`，从 main `460a349` 起，逐 commit push；**已 ff 合入 main `68b5e56`（2026-06-27）**；**纯量化、零热路径行为改动**）：
- **设计输入**：grill 拍板「量化-only + ADR 定瓶颈」（非「量化+干预」）；understand workflow（5 接缝并行深挖 + 路线可行性综合）。
  spec/plan/acceptance：`docs/tech/2026-06-26-knife12-multicore-quantify-{spec,plan}.md` + `2026-06-26-knife12-acceptance-checklist.md`。
- **`LoopProfiler`**（新 `src/loop_profiler.rs`）：knife1 `MetricsSink` **计时**接缝的生产实现，量主循环 **poll/relay/loop-active**
  三段 wall-fraction（loop-active = 1−park/wall）。env `MINI_VPN_PROFILE_LOOP=1` 开 → 每 `MINI_VPN_METRICS_SECS` 打 `🔬` 行；
  **默认 `NoopSink` 零开销逐字不变**（trait 加 `loop_park_begin/end`/`report` default-空方法，8 arm 首行 park_end + 循环底 park_begin
  + metrics_tick report）。harness 多核就绪 spike 证仪器正确（注入 on-loop CPU → loop-active 0.706→0.996/poll 0.170→0.692）。
- **质量**：205 lib + 8 harness 测全绿、`clippy --all-targets --features harness` 0 warning、release 绿。**对抗式 review workflow
  撞 session 限额失败 → 改 inline 逐维度自评（零开销/数学/插桩语义/harness）零正确性 bug**，仅修一处 doc（`enter_relay` 注释陈旧）。
- **🔑 真出口归因（深圳 macOS client → 47.x sing-box，iperf3）→ #4 实测推翻（ADR-0013）**：
  - **poll 段处处 ≤3.8%、多数 0.1%**（直连 + 隧道、各负载）→ **单核 smoltcp poll 不是 100M 天花板**（brief 承重假设证伪，
    同刀3.5 推翻「5.3M 天花板」）。
  - **当前墙是 WAN 跨太平洋路径**（单流 ~22M / 并行聚合 ~46M、重传一次达 63509、RTT 限）→ **100M 此路物理不可达**，
    客户端在任何可达负载下接近空闲（loop-active 多 0.1%、park 99.9%）。
  - 唯一 on-loop 成本是**建连瞬态的 relay 段**（P=4 setup 窗口 relay=66%/poll=3.8%）→ 若主循环有瓶颈是 relay 调度/inline open，非 poll。
  - **早先绕过隧道**（裸跑 client-tun 不配路由 → 流量走 en0；`curl ipinfo.io` 显示本地 IP 是金标准症状）。
- **🔑 干净隧道实测（2026-06-27，`soak` 路由修好，44.5M 隧道）→ 裁决站得住 + 挖出 HoL bug（ADR-0013「Update」）**：
  稳态 `🔬` = **loop-active≈93% / poll≈8% / relay≈81% / park≈7%**。读代码坐实：上行 `tx.send().await`（有界 1024 channel，
  client_tun.rs:1350）在 QUIC 上游拥塞时**阻塞主循环等上游** → relay=81% 是**背压等待非 CPU**，poll=8% 是真 CPU。
  **主循环 upstream-bound 非 CPU-bound，墙仍是 QUIC 上游/WAN（#3），裁决不翻。** 仪器局限：loop-active 混 CPU 与 arm 内
  `.await` 背压（纯 CPU 需 OS `sample`）。**真 bug**：阻塞上行使一条拥塞慢流 **HoL 阻塞整个事件循环**（大并发混合流下慢流拖死快流）。
- **🔑 OS `sample` 确认（2026-06-27，压测期）→ 裁决锁死 + 挖出 println**：栈采样 top `__psynch_cvwait 29149/kevent 7283`
  （parked/空等）压倒，真 CPU 极小（smoltcp poll 仅 17）→ **进程非 CPU-bound、#4 二次证伪、#3 锁死**。**💎 主循环 #1 on-CPU
  成本=热路径 `println!`**（`process_listener_activity→_print→write` ~183 采样；`📬` 行 client_tun.rs:658 等）远超 poll(17)——
  22000 事件/秒每次阻塞 write、纯浪费 + 加重 HoL。
- **裁决（ADR-0013）→ 刀13**：**取消事件循环分片（route a）**（loop 非 CPU-bound，分片无用）；**#3 连接池**留**低 RTT 胖链路**再测。
  刀13 已按 sample 的 cheap→结构性顺序完成：①热路径 `println!` 由 `MINI_VPN_TRACE` 门控；②上行发送改
  `try_reserve`，Full 时留 smoltcp 字节 + 保持 dirty，端到端 TCP 背压，消除跨流 HoL。`LoopProfiler` 留作复测工具。
- **deferred / 可选**：启动首条 `🔬`（`wall≈0ms` tokio interval 首 tick）退化——**已加 guard 跳过**（commit `33d9418`）。

## Rhythm（每刀都遵守）

1. 新 session → 读本 HANDOFF + `Rules.md` → 先 **grill**（用 `/grill-with-docs` 或 brainstorm，对齐设计与本刀范围）→ 出 **spec + plan**（docs/tech/，TDD 分解）。
2. **TDD per task**：写失败测试 → red → 实现 → green → commit；**每次 commit 后 `git push`**；一个分支一个 writer。
3. 收尾：**`/code-review`** over the diff → 修 → 跨机/压测 **acceptance**。
4. **真实数据测试协作模式**：凡是需要用户在真实环境采集性能/连通性数据，优先在 `./scripts/` 增加可复跑脚本，
   同时给出测试步骤、前置检查、日志路径和判据；用户按指导测试并贴回日志后，再基于日志分析和优化。不要让用户手拼长命令。
5. **cwd 陷阱**：Bash cwd 可能在 call 之间被重置到别的 worktree——每条 git/cargo 命令前 `cd` 到本 worktree 并用绝对路径编辑；`git branch --show-current` 应是本分支。
6. 文档/教学叙述（teaching note、LEARNINGS）由用户另行通过代码+commit 生成；**本路线只产 spec/plan + 代码 + commit + 必要的 TODO 状态**（除非用户另说）。
7. 用**中文**回复（代码/术语/commit 保留英文）。

## 已知坑 / deferred（接力时别重新踩）

- **0-RTT**：quinn 0.10 / rustls 0.21 在 0-RTT 阶段无法 `export_keying_material`，TUIC auth 必失败回落 1-RTT → `MINI_VPN_TUIC_ZERO_RTT` 默认关。真 0-RTT 需 quinn 升级（归移动端 stage），见 TODO 13c。
- **quic-stream UDP fallback 已完成**：刀3 已做 oversized packet uni-stream 兜底；刀3.5 后默认仍是 native/cubic。
- **加密 DNS/fake-IP 绕行主链已关闭**：刀4 阻断已知 DoH/DoT/DoQ/DoH3，刀5 拦全 plaintext :53。
- ~~**fake-IP 池永不回收**（198.18/15）~~——✅ 刀2 已修（引用计数活跃 flow + 60s sweep + 死槽回收）。
- **first-SYN-to-fresh-fake-IP refused 竞态已关闭**：刀4 acceptance 确认 knife2 已修。
- 出口是 VPS datacenter IP → Google/Meta 风控（协议无关，记录即可）。

## Not in git（用户提供；真实/UDP 直播 acceptance 时需要）

- sing-box 互通参数（env）：`MINI_VPN_TUIC_SERVER=<VPS_IP>:8443`、`MINI_VPN_TUIC_UUID=<uuid>`、`MINI_VPN_TUIC_PASSWORD=<pass>`、`MINI_VPN_TUIC_SNI=example.com`、`MINI_VPN_TUIC_CA_PATH=certs/dev/ca-cert.pem`、`MINI_VPN_TUIC_ALPN=h3`。（向用户要实际 UUID/password/IP，**勿入库**。）
- 启动：`sudo MINI_VPN_TUIC_* ./target/debug/mini_vpn client-tun`（13d 起 `MINI_VPN_UPSTREAM` 已删，恒 TUIC；`MINI_VPN_TUN_POOL_SIZE` 可调端口池）。
- **刀3.5 新增旋钮**（非凭据，可入库默认；env 覆盖）：`MINI_VPN_TUIC_CC=bbr|cubic`（默认 cubic）、`MINI_VPN_TUIC_UDP_MODE=native|quic`（默认 native）。
- 刀1 若走 mock-upstream 隔离压测，则**不需要** sing-box。
- **刀5 acceptance**：`sudo -E bash scripts/knife35-acceptance.sh soak-knife5`（设系统 DNS=8.8.8.8 非我方 resolver + 路由进 TUN，
  验证任意 :53 仍被劫持）；`soak-stop` 自动还原。`K5_RES` env 可换 alt-resolver。需同上 `MINI_VPN_TUIC_*` 凭据。
