# Learnings

## 2026-07-13 - Kernel-facing read ownership and actor batch service are separate requirements

- Coalescing dirty-relay traversal alone left the real TUN fd actor-owned and
  the VPS poll-dominated; the frozen P1 stayed at `194 Mbit/s` but TUN drops
  increased from `10` to `38`. A useful batch does not by itself guarantee that
  the upstream kernel queue is drained while the actor is busy.
- The successor gives H10d16 a continuous, bounded TUN reader pump and keeps all
  classification, smoltcp, relay, and write state actor-owned. A distinct raw
  slot and smoltcp-staged FIFO prevent prefetch from bypassing DNS/UDP/SYN
  inspection while allowing one poll/flush per bounded TCP batch.
- The final exact 32 MiB repeat reached `293.488 Mbit/s`, with ring high
  `38/500`, pump high `56/500`, zero full waits/drops, and `28,933` packets
  serviced by `11,817` poll/flush/relay batches. Terminal D16 ownership and EOF
  tail were zero.
- Review found that a bounded producer is still unsafe if its closed receiver
  becomes an always-ready select branch. Terminal TUN errors now stop the event
  loop fail-closed; a dedicated test proves no busy wake.
- Reusable rule: for a kernel-to-actor boundary, prove both continuous ingress
  ownership and bounded actor service. Keep raw classification ownership
  separate from protocol-stack staging, and make terminal channel state end the
  consumer loop rather than become a retry source.
- Result:
  `docs/tech/2026-07-13-knife14h10d16-tun-ingress-service-local-gate-results.md`.

## 2026-07-13 - TUN packet ingestion and relay service need separate batch ownership

- The H10d16 TUN RX path previously traversed dirty relays once per ingested TCP
  packet and then again from the actor loop. A focused RED observed `8` packets
  causing `8` relay entries; the batch service GREEN ingests the same packets and
  performs exactly one dirty-relay pass.
- The real-Quinn `32 MiB` forward tracer delivered exactly `33,554,432B` at
  `319.455 Mbit/s`, with zero modeled TUN drops, exact pattern, clean EOF, and a
  bounded ring high-water of `29/500`. It serviced `28,934` TCP packets in
  `4,093` batches and avoided `24,841` redundant relay traversals.
- Reachability matters at the first ready packet: the H10d16 direct-ready branch
  initially bypassed the batch drain, so a function-level GREEN did not make the
  product path GREEN. The accepted path routes the already-ready packet and its
  ready followers through the existing fixed 48-packet drain while preserving
  the default non-H10 adapter.
- Reusable rule: keep packet classification/ingress ownership separate from
  downstream relay service. Prove both the batch invariant and full-path
  reachability before treating a local optimization as VPS-eligible.
- Result:
  `docs/tech/2026-07-13-knife14h10d16-tun-rx-batch-service-local-gate-results.md`.

## 2026-07-13 - A bounded TUN drain probe creates a single-slot ownership obligation

- The drain-budget probe may prefetch one packet and leave it in `rx_buffer` for
  the next actor turn. The old `wait_for_rx` implementations read again even
  when that slot was populated, so a two-packet RED proved the first packet could
  be overwritten by the second.
- `VirtualTunDevice` and the deterministic harness device now return immediately
  when `rx_buffer` is already populated. This preserves the prefetched packet
  without changing MTU, queue capacity, drain budget, or self-wake behavior.
- Reusable rule: when readiness probing stores data in a caller-owned slot, the
  next wait must treat a populated slot as ready. A bounded drain is not correct
  unless every prefetched packet has explicit ownership across turns.

## 2026-07-13 - Throughput tests need serialized ownership of local capacity

- The full harness suite initially produced false `94-164 Mbit/s` rate failures
  while seven CPU- and socket-heavy localhost tests ran concurrently. Every
  exact-byte, lifecycle, and pattern invariant passed, and each rate test passed
  in isolation.
- A test-only crate-local capacity mutex now serializes those rate-sensitive
  localhost tests. The complete harness gate then passed `636` tests with no
  product-path scheduling or pacing change.
- Reusable rule: a localhost Mbps threshold is meaningful only when the fixture
  owns the relevant host capacity. Serialize rate gates, but keep exact delivery,
  conservation, boundedness, and lifecycle assertions independently mandatory.

## 2026-07-13 - Endpoint-owned pre-accounting closes the local burst/capacity gate

- The complete `EndpointWindowV1` path passed an exact GSO-enabled `32 MiB`
  upload at `240.466 Mbit/s`, with `33,554,432B` delivered, zero pattern
  errors, clean EOF, endpoint delay activity, and no cap64 or bounded sender.
  Final endpoint state was `available=61,440B`, `live=0`, `outstanding=0`, and
  `records=0`.
- Real accounting matched the designed seam: `34,324,276B` was granted,
  `1,867B` refunded, and `34,322,409B` accepted by the UDP socket across
  `23,641` datagrams. The service therefore owns planned bytes before packet
  construction and retains actual bytes through the real socket outcome; it
  is not another post-accounting cooldown.
- The regression surface stayed intact: quinn-proto `309/309 + 3/3` docs,
  Quinn `29/29` nonignored + `1/1` doc, mini_vpn `622/622` nonignored, explicit
  `64/256/1024` concurrency, and the UDP sweep all passed. Default Quinn
  remains byte-equivalent when the endpoint policy is absent.
- Reusable rule: a throughput architecture is locally eligible for VPS only
  when one test joins the theorem to the real driver/socket lifecycle and
  proves both capacity and zero accounting leak. Pure token tests or a fast
  loopback alone are insufficient.
- Result:
  `docs/tech/2026-07-13-knife14h10d16-endpoint-pacing-service-local-gate-results.md`.

## 2026-07-13 - Lifecycle anomalies must preserve pacing fail-closed

- Quinn removes a `ConnectionHandle` only after connection `State::drop`
  abandons/detaches endpoint pacing and sends the `Drained` event, so normal
  slab reuse cannot collide with an attached service key. Migration
  tombstones retain only already charged old-path batches and are removed by
  their exact socket outcome.
- Code review still found a defensive failure-mode defect: if a target path
  generation were unexpectedly occupied, the connection detached pacing and
  continued unpaced. The repair keeps the old attached adapter on migration
  rejection. Aggregate safety and service continue even though diagnostic
  generation attribution stays on the last valid key.
- Reusable rule: internal uniqueness assertions may remain diagnostic, but an
  impossible lifecycle branch must not disable a safety service in release
  mode. Prefer stale-but-conservative attribution over fail-open throughput.

## 2026-07-13 - Pre-accounted bytes make the endpoint window testable before Quinn integration

- The Task 3 pure module now charges planned wire bytes before construction,
  moves only actual bytes to socket outstanding on settlement, refunds short
  builds or dropped reservations exactly once, and releases outstanding bytes
  only after a real socket-success transition.
- Integer refill retains sub-byte-time remainder while service is unsaturated,
  discards stale credit at saturation, ignores time rollback, and saturates
  extreme arithmetic. Deterministic arbitrary-window traces cover datagram
  sizes from `1B` through the full `61,440B` burst.
- The frozen `30,720,000B/s`, `61,440B` candidate emits exactly
  `92,160B/1ms` and `368,640B/10ms` under 50us fake-time polling. Every tested
  transition preserves
  `available_tokens + live_reservation_bytes + outstanding_bytes <= burst_bytes`.
- Reusable rule: prove the byte theorem in a pure fake-time module before
  connection construction, packet classification, fairness, wakers, or socket
  Adapter integration. Current dead-code warnings are expected only until that
  Parallel Change reaches its real endpoint/driver consumers.

## 2026-07-13 - A temporal egress proof must include reservations and socket backlog

- A deterministic Quinn fake-time replay made the cap64 limitation exact:
  with `RTT=200us`, `cwnd=40000B`, `MTU=1280`, 20 datagrams per driver poll,
  and 50us poll cadence, the Pacer issued `259` datagrams inside `1ms` even
  though stored capacity stayed exactly `64*MTU`.
- A shared token bucket at packet-build time is still incomplete if bytes
  accepted by Quinn accounting can wait behind socket `WouldBlock` while the
  bucket refills. The temporal invariant must keep both live reservations and
  finalized-but-unsent bytes charged:
  `tokens + live_reserved + outstanding <= burst`.
- The resulting proposed endpoint contract uses `30.72 MB/s` wire rate and a
  `61,440B` burst: `<=92,160B/1ms`, `<=368,640B/10ms`, and about
  `239.167 Mbit/s` application capacity after measured QUIC overhead. The
  policy belongs in Quinn-proto before packet accounting; pinned Quinn only
  supplies waker and socket-outcome callbacks.
- Reusable rule: a cross-connection time-window claim needs a theorem over
  actual finalized/socket-accepted bytes, not gross planned grants, one
  connection's stored tokens, or a post-accounting socket delay. Include
  short-build refunds, `WouldBlock`, fairness, control liveness, migration,
  cancellation, and detach before implementation.
- Source:
  `docs/tech/2026-07-13-knife14h10d16-endpoint-pacing-service-architecture-spec.md`.

## 2026-07-13 - A Pacer token ceiling is not a time-window burst ceiling

- The cap64 VPS data connection was exact and attributable: it carried
  `99.9998%` of TX bytes, had stable identity and bounded cwnd, and one formal
  snapshot reduced `327680B` upstream capacity to `81920B = 64*1280`.
  Nevertheless client egress peaked at `267 packets/1ms` and `1337/10ms`,
  slightly above the prior Quinn-default edge, while TUN drops and formal QUIC
  loss remained `29` and `50,621,275B`.
- Quinn deliberately preserves `1.25*cwnd/rtt` refill. At sub-ms RTT, elapsed
  time inside one observed millisecond can refill and spend multiple stored
  buckets. The cap was active in only one of five formal data snapshots, and
  delay events stopped increasing while bulk transmission continued.
- Reusable rule: distinguish stored capacity from a temporal service
  invariant. Before claiming a burst bound, test sub-ms RTT with fake time and
  state the exact 1ms/10ms or sliding-window contract; capacity math and one
  cap-active snapshot are not sufficient.
- Result:
  `docs/tech/2026-07-13-knife14h10d16-pacer-cap64-forward-discriminator-results.md`.

## 2026-07-13 - Same-window pcap separates mechanism failure from path loss

- Both current control and mini_vpn captures had about a `50 MB` bilateral UDP
  byte gap, so this window cannot assign all raw packet loss uniquely to
  mini_vpn. The mature control still sustained `191.928 Mbit/s` with zero
  socket drops, while mini_vpn added local TUN drops and exceeded its formal
  loss gate.
- The cadence comparison stayed decisive despite shared path loss: control
  client egress was `100 packets/1ms` and `605/10ms`, versus cap64
  `267/1337`. Reusable rule: keep same-window capacity, endpoint drops,
  bilateral bytes, and client-side cadence as separate discriminators; do not
  infer a product root from one of them alone.

## 2026-07-13 - Acceptance knobs need one canonical end-to-end fingerprint

- The cap64 path already passed its real capacity gate, but review found that
  the acceptance runner did not own the new policy as an end-to-end contract.
  Validation alone was insufficient: the policy now has a canonical value
  set, an incompatible-pacer veto, an explicit env assignment used by both the
  report and launch argv, and an independent startup-log verifier that runs
  even when the H10d16 profile gate is disabled.
- Formal runtime stats now keep path `current_mtu` separate from the Pacer's
  `pacing_mtu`. This prevents an active-cap assertion from silently attributing
  a capacity computed for one MTU to the path's current MTU.
- Reusable rule: every performance policy must be validated, composition-safe,
  propagated through the real launcher, reported, and verified from the
  process's own startup fingerprint. Profile-specific verification cannot be
  the only proof of a transport policy that exists outside that profile.

## 2026-07-13 - Known-negative measurements must not remain default gates

- The rejected fixed `48 then 2ms` replay was still a normal library test with
  a `>170 Mbit/s` assertion, so it stopped the cap64 regression even though its
  own architecture had already been measured and rejected. Making the real
  replay explicit/ignored preserves it as a measurement without confusing it
  with a production invariant; its deterministic conservation, timer, GSO,
  and wake tests remain active.
- The Quinn cap now computes `optimal_capacity` once and applies the optional
  ceiling to that result. This keeps default behavior equivalent while making
  the derivation itself testable and avoiding duplicate RTT/window division.
- Reusable rule: classify tracer tests when their architecture is accepted or
  rejected. Keep deterministic mechanism invariants in the default suite, but
  make environment-sensitive known-negative throughput replays opt-in.

## 2026-07-13 - Reverse-only parity does not prove forward-then-reverse health

- The repaired exact build sustained `60s` reverse at `186 Mbit/s` receiver,
  all `60/60` intervals nonzero, zero TUN drops, and no stall. A fresh
  forward-first process then added `54` TUN TX drops and about `61.3 MB` of
  QUIC lost bytes during P1 forward; its following reverse inherited `63.7
  MB` lost bytes / `22033` congestion events and averaged only `136 Mbit/s`.
- The inherited reverse eventually recovered to a `190.167 Mbit/s` tail, so a
  healthy tail alone does not erase poisoned per-connection history. Product
  acceptance must exercise direction transitions and classify each subwindow
  against its own start counters.
- The same flow later hit `half_closed_idle_timeout` with `524288B` still
  D16-owned and `27736B` in an active send-capable egress candidate. Timeout
  policy must treat useful owned/drainable bytes as a hard inhibit, independent
  of whether the preceding transport loss is client-specific or path-wide.
- Reusable rule: after reverse parity passes, run forward-first before broad
  concurrency. Use a same-window mature forward control before changing QUIC
  code or parameters, and do not reuse a contaminated connection as the sole
  reverse discriminator.
- Result:
  `docs/tech/2026-07-13-knife14h10d16-product-regression-repair-and-forward-failure.md`.

## 2026-07-13 - Conditional once-per-pass aggregation restores the 1024-flow gate

- The focused scan seam proved D16/non-buffered credit needs zero global
  pending scans, while buffered credit needs one aggregate per dirty pass and
  can update it from per-handle pending deltas.
- Clean `1024/1024` now completes in `12.294s` with `4.272s` in relay, replacing
  the `733/1024` timeout with `101.271s` in relay. Normal tests, UDP sweep,
  ownership checks, and the sustained reverse VPS run all stayed green.
- Reusable rule: a performance fix should remove the disproved cost only at
  its consumer boundary. Preserve retained-mode semantics and validate the
  exact patch from a clean detached build instead of formatting or staging
  unrelated dirty files.

## 2026-07-13 - A global pressure scan inside a per-flow loop silently restores O(n squared)

- Clean `a54fb17` passed `609` lib tests and `10` normal concurrency harness
  tests, including `64/64`, but the explicit sweep completed only `733/1024`
  at its `120s` limit. All `1024` mock upstream opens occurred, while the
  relay segment consumed `101.271s` and late relays reached the `90s` idle
  timeout.
- Historical Knife2 evidence completed the same `1024/1024` shape in about
  `2.3s` with roughly `70.8ms` in relay. The regression is the aggregate
  `downlink_pressure_stats` scan added inside every handle iteration of
  `process_dirty_relay`: it scans dirty handles and all sockets, producing
  O(active²) work.
- The accepted D16/default path has buffered-downlink disabled, so its credit
  branch never consumes this aggregate pending value. Reusable rule: aggregate
  observability/backpressure calculations must be once-per-pass, incremental,
  or conditional on their consumer; normal small-N tests cannot substitute
  for the explicit 1024-flow gate.
- Result:
  `docs/tech/2026-07-13-knife14h10d16-product-regression-concurrency-failure.md`.
- Resolution: the confirmed conditional/once-per-pass repair now passes
  `1024/1024`; the later VPS stop is a separate forward-first transport and
  lifecycle failure.

## 2026-07-13 - Gate B needs both a median capacity proof and one natural EOF proof

- Clean `a54fb17` produced three exact reverse-first P1 receiver results of
  `192`, `188`, and `191 Mbit/s`; median `191 Mbit/s` passed the absolute
  `170 Mbit/s` gate. All 60 one-second intervals carried data, while TUN drop,
  actor bypass, send/flush error, pressure/drop debt, reconnect, QUIC
  loss/blocking, and terminal pending reap stayed zero.
- The same-window mature control was only `143.228 Mbit/s`, but its target-only
  route and both UDP socket drop counters were clean. The documented relative
  threshold was `128.9052 Mbit/s`; mini_vpn passed both it and the stricter
  absolute median target. A control runner's Gate-A-oriented `INCAPABLE` label
  must not replace the distinct Task 12 Gate B formula.
- The one post-median fixed `64 MiB` flow delivered exactly `67108864B` at
  `179 Mbit/s` and closed with `clean_queue_lifecycle`, empty queued/leased/
  reserved ownership, pending/inflight `0/0`, and zero close/TUN tail. Timed
  capacity and natural EOF remain separate proofs even when timed shutdown
  reports no terminal cause.
- Reusable rule: count measurement samples only after mini_vpn and iperf enter
  the declared window. Evidence-backed preflight/configuration errors may be
  corrected under the user's standing override without changing frozen
  product parameters; a real gate failure still stops for analysis and a plan.
- Result: `docs/tech/2026-07-13-knife14h10d16-gate-b-results.md`.

## 2026-07-13 - Gate controls require an explicit server ALPN fingerprint

- Clean `a54fb17`, the accepted mini_vpn binary, exact Shoes `v0.2.7`, exact
  sing-box `1.13.14`, healthy `214.494 Mbit/s` direct reverse, MTU1200,
  target-only routing, and full zero-drop UDP sockets were not sufficient to
  make the control valid. The reconstructed Shoes config omitted its explicit
  `h3` ALPN list, so TLS failed before TUIC or iperf data.
- A running listener plus correct cert/key/auth is not the complete transport
  fingerprint. For QUIC acceptance, the ephemeral server config must lock and
  validate ALPN alongside implementation version, endpoint count, worker
  count, port, socket buffers, and credentials.
- Reusable rule: assert non-secret protocol fields in the in-memory rendered
  config before feeding one-shot secret FIFOs. If a control fails before a
  receiver result, preserve it as invalid setup, do not spend product repeats,
  and do not classify the window as incapable. Under the current user override,
  correct the evidenced configuration fault and rerun without another
  confirmation; do not change frozen product parameters.

## 2026-07-12 - Modern Quinn closes the external TUIC capacity question

- Shoes `v0.2.7` / Quinn `0.11.9` delivered `192.666 Mbit/s` receiver through
  `.111 -> .27`, with `20/20` nonzero intervals and a `153.099 Mbit/s`
  minimum. Both pcaps had zero kernel drops and UDP buffer-drop surfaces stayed
  clean.
- The same raw path and TUIC client reached `2-5 Mbit/s` through quic-go forks,
  `112.559 Mbit/s` through old Quinn `0.10.1`, and `192.666 Mbit/s` through
  maintained Quinn `0.11.9`. Transport lineage and version are causal axes;
  D16 is not the active capacity root.
- Reusable rule: a timed throughput flow and a clean EOF flow prove different
  contracts. Preserve exact terminal-reset classification for the capacity
  flow, and require a separate finite-byte flow for zero-tail EOF; never make a
  timed generator's reset silently stand in for clean close.

## 2026-07-12 - Independent transport separated starvation from capacity

- Moving the TUIC server from quic-go forks to Rust/Quinn `0.10.1` raised the
  receiver from `2-5` to `112.559 Mbit/s`, removed every zero interval, and
  reduced the maximum data-read gap to `27ms`.
- The result still failed `>150 Mbit/s`. The target sender was continuously
  backpressured near `119 Mbit/s`, UDP drops were zero, and the server consumed
  only about `2.73s` CPU over its whole lifetime. This is insufficient server
  transport efficiency/capacity, not the old starvation and not D16.
- Reusable rule: performance failures need both shape and aggregate capacity.
  Eliminating stalls is causal progress but does not authorize the next gate;
  keep the strict floor and move to a maintained implementation on the already
  proven transport generation.

## 2026-07-12 - Server implementation A/B must audit transport lineage

- Official Mihomo `v1.19.28` reproduced the external burst/idle failure at
  `3.460 Mbit/s` receiver with `7/20` client zero intervals. The target sender
  had `14/20` zero intervals even though both direct baselines exceeded `216
  Mbit/s` and all client/server drop surfaces were zero.
- This rejects sing-box application code, but review found that Mihomo and
  sing-box use separate forks of the same quic-go `0.59.x` lineage. Changing a
  product name is not necessarily changing the transport mechanism.
- Reusable rule: before calling a server A/B independent, inventory its QUIC,
  TLS, protocol, and copy-loop dependencies. When the application changes but
  the transport lineage remains, classify the result at that narrower boundary
  and choose an actually independent stack or add transport telemetry next.

## 2026-07-12 - Host-local TUIC isolates the external sender/path interaction

- The exact pool-1 direct-TUIC probe and sing-box binary reached `199.639
  Mbit/s` receiver on `.111` loopback, with `20/20` nonzero intervals and a
  `169.868 Mbit/s` minimum interval. Both Connect relays completed and
  Quinn/socket error counters were zero.
- The same target sender that had `15/20` zero intervals and about `5.08
  Mbit/s` through external TUIC delivered every interval at `201 Mbit/s`
  host-local. The tested sing-box TUIC ingress/copy has capacity; its external
  quic-go sender/path interaction is the remaining boundary.
- Reusable rule: pair a cross-host protocol failure with an exact host-local
  protocol control before blaming internal copy code. When host-local and
  minimal cross-host transport both pass, preserve downstream product
  architecture and change the protocol-server implementation/version next.

## 2026-07-12 - Direct TUIC removes D16 and server CC from the capacity root

- A pool-1 generic OrderedJoin probe reproduced the low result without TUN,
  smoltcp, native readers, D16, or the product event loop: server BBR reached
  `3.775 Mbit/s` and server Cubic reached `2.674 Mbit/s`.
- Server Cubic removed client-visible zero intervals and reduced data-read gaps
  from `3441ms` to `225ms`, but the target TCP sender still had `15/20` zero
  intervals. Congestion control changed buffering/pacing shape, not capacity.
- Target-side sender intervals are a stronger boundary signal than client read
  intervals after an intermediary can buffer. Continuous client delivery does
  not prove continuous upstream service.
- Reusable rule: once a direct protocol relay reproduces failure and the
  target sender itself stalls, preserve the downstream product architecture.
  Move the next discriminator to the protocol server or a host-local path
  before changing client queues, readers, wakeups, MTU, or windows.

## 2026-07-12 - Minimal Quinn crosses the raw-path shaping edge continuously

- A test-only Quinn Cubic/safe1200 reverse ordered stream sustained `192.597
  Mbit/s` from `.111` to `.27`; all 21 intervals carried data, the minimum
  non-empty interval was `183.934 Mbit/s`, integrity errors were zero, and EOF
  was clean.
- Client Quinn loss, congestion, and data-blocked counters were zero. The
  server observed loss at the known raw UDP edge but adapted without the
  multi-second zero-rate intervals seen in TUIC controls.
- This removes raw UDP and minimal Quinn from the active root set. The first
  untested boundary is direct TUIC Connect/sing-box stream service; only after
  that seam passes should diagnosis move into mini_vpn pool/open/relay
  coupling.
- Reusable rule: protocol-layer burst/idle must not be assigned to QUIC merely
  because it rides QUIC. Hold CC, MTU, direction, windows, and host path fixed,
  then test the smallest real ordered-stream layer before changing product
  backpressure architecture.

## 2026-07-10 - D16 local capacity closure moves Gate A upstream

- Code commits: `f7847dd` repairs ACK-driven actor re-entry and the feedback
  service budget; `67c5122` records the local capacity closure.
- Local outcome: the exact 64 MiB production seam improved from about
  `56 Mbit/s` to `224 Mbit/s` and passed 50 consecutive capacity-qualified
  repeats with zero modeled drop, actor bypass, and EOF/tail bytes.
- VPS outcome: the single authorized 20-second reverse-first P1 reached only
  `19.2/17.9 Mbit/s`. It nevertheless kept TUN drops, local pressure/backlog,
  actor bypass, send/flush failures, and QUIC loss/congestion/blocking at zero.
- Discriminator: the actor admitted all bytes delivered to it, while the
  ordered data reader showed repeated `1.6-3.5s` read gaps despite active
  polling. This moves the active bottleneck upstream of the local egress actor.
- Evidence limit: connection-global STREAM frame progress does not prove
  contiguous bytes were deliverable on this exact data stream. The next seam
  must add sustained same-stream evidence before assigning the defect to Quinn
  wake, ordered reassembly, or TUIC/server service.
- Lifecycle limit: zero observed close-tail counters are not a natural EOF
  proof when the data handle remains active at the final snapshot.
- Reusable rule: a local architecture can be both necessary and capacity-safe
  without being the final end-to-end limiter. When its pressure and drain
  surfaces are clean, preserve it and move the RED seam one boundary upstream
  instead of reopening its budgets.
- Result:
  `docs/tech/2026-07-10-knife14h10d16-ack-capacity-gate-a-results.md`.

## 2026-07-09 - Promote skill gates before the next Knife14 architecture slice

- Stage: post-Knife14gv self-review and project-rule update.
- Changed file: `AGENTS.md`.
- Outcome: project memory now treats `diagnose`, `tdd`,
  `improve-codebase-architecture`, `code-review`, and `self-improving-agent` as
  the default skill-assisted workflow for throughput work, with additional
  architecture, refactoring, systems, release, and Go-reading skills for their
  relevant stages.
- What worked: the rule now requires a feedback loop, capacity/reachability
  gate, and focused TDD seam before claiming a stage can reach `30 Mbit/s` or
  `100+ Mbit/s`.
- Reusable rule: after a code-review says the current seam cannot prove the
  bottleneck, the next step is architecture extraction and a deterministic
  stream-service harness, not another local pressure-credit or queue-window
  tweak.

## 2026-07-08 - Knife14go self-wake improves polling but not ordered delivery

- Result doc:
  `docs/tech/2026-07-08-knife14go-active-conn-rx-self-wake-results.md`
- Code commit:
  `39112ca` makes TUIC pending reads arm bounded self-wake for active
  connection RX, not only for `connection_stream_frames_pending`.
- Log bundle:
  `/tmp/mini_vpn/knife14go_active_connrx_selfwake_20260708/mvpn_knife14go_active_connrx_selfwake_safe1200_p1_30_usclient_suite_20260708_213110.tar.gz`
- Outcome: focused safe1200 reverse-first P1 completed but reached only
  `24.6/22.4 Mbit/s`, below the `>30 Mbit/s` stage target.
- What worked: data-stream poll service improved; the final summary showed
  `data_poll_gap_max_ms=43`, `self_wake_armed=11702`, and
  `self_wake_fired=8726`, with no TUN drops and no QUIC loss/congestion or
  tx/rx blocking.
- What failed: ordered delivery still had multi-second gaps:
  `data_read_gap_max_ms=3872`, `data_pending_gap_max_ms=3006`, and
  `connection_stream_frames_pending=20`. A late catch-up burst also
  reintroduced one local pressure edge:
  `downlink_backpressure pause_edges=1`, `read_credit_pause_updates=1`, and
  `pressure_credit_blocked_bytes=122727`.
- Reusable rule: once self-wake reduces poll gaps but read gaps remain in the
  seconds range, stop treating the root as a sleeping relay reader. The next
  slice must separate connection-level STREAM frame progress from current-stream
  deliverable progress before tuning pressure-credit constants.

## 2026-07-08 - Knife14gn clears local pressure but leaves ordered stream gaps

- Result doc:
  `docs/tech/2026-07-08-knife14gn-accepted-flush-progress-results.md`
- Code commit:
  `97cb55e` feeds downlink credit-controller feedback after `send_slice`, so
  bytes accepted into smoltcp count as useful local egress progress and can wake
  read-credit publishers.
- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14gn_accepted_flush_progress_safe1200_p1_30_usclient_suite_20260708_200258.tar.gz`
- Outcome: focused safe1200 reverse-first P1 completed but reached only
  `18.8/18.0 Mbit/s`, below the `>30 Mbit/s` T11 target.
- What worked: the local pressure-credit surface was cleaned in this run:
  `local_pressure=0`, `downlink_backpressure pause_edges=0`,
  `global_rx_receive pause_edges=0`, `read_credit_pause_updates=0`,
  pressure/drop debt `0`, TUN drops `0`, and QUIC loss/blocking `0`.
- What failed: throughput stayed low-average and burst/idle. The data stream
  still had `connection_stream_frames_pending=10`,
  `data_read_gap_max_ms=3405`, and `data_pending_gap_max_ms=3405` despite open
  read credit and clean local pressure.
- Reusable rule: after accepted-flush progress clears local pressure, do not
  keep changing pressure-credit constants. The next slice should test and fix
  ordered TUIC stream receive cadence/self-wake when transport has stream-frame
  progress since the last application read but the relay reader remains in a
  multi-second pending gap.

## 2026-07-08 - Knife14fu/fw/fx proves VPS can 100M while mini_vpn remains low

- Result doc:
  `docs/tech/2026-07-08-knife14fu-fw-fx-reverse-discriminator-results.md`
- Code commit:
  `6eb52e9` restored ordered TUIC stream reading as the default and gated
  unordered chunk reassembly behind
  `MINI_VPN_TUIC_TCP_UNORDERED_REASSEMBLY=1`.
- Current-branch bundle:
  `/tmp/mini_vpn/knife14fu_ordered_default_p1_30/mvpn_knife14fu_ordered_default_p1_30_usclient_suite_20260708_105526.tar.gz`
- Known-good A/B bundle:
  `/tmp/mini_vpn/knife14fw_fp_commit_reverse_first_p1_30/mvpn_knife14fw_fp_commit_reverse_first_p1_30_usclient_suite_20260708_110632.tar.gz`
- Mature-client artifacts:
  `/tmp/mini_vpn/knife14fx_singbox_client_current/iperf3-reverse-30s.txt`,
  `/tmp/mini_vpn/knife14fx_singbox_client_current/sing-box-client.log`
- Outcome: current mini_vpn ordered-default reverse-first P1 collapsed to
  `0.349/0.046 Mbit/s` with clean local pending/headroom/TUN and QUIC
  loss/blocking surfaces; the `f8765c1` fp-era clean worktree reached
  `19.900/18.700 Mbit/s`; a mature sing-box `v1.13.14` client in the same
  current VPS window reached `173/173 Mbit/s`.
- What worked: the unordered branch is no longer a default-path risk, and the
  mature-client A/B gives a decisive answer to the VPS question: `.33` high
  socket buffers plus current sing-box can still carry `100+ Mbit/s`.
- What failed: mini_vpn still does not meet clean `100+ Mbit/s` reverse-first
  acceptance. The current branch is worse than `f8765c1` because it shows a
  no-data TUIC stream-read/starvation shape; the fp-era code still shows the
  older local-pressure-credit throughput gap.
- Reusable rule: when mature sing-box reaches `100+` in the same current window
  and mini_vpn does not, stop changing VPS config, iperf3, stale pools,
  MTU/PLPMTUD, or broad QUIC windows. First restore the current branch to a
  data-moving ordered-stream shape, then resume the mini_vpn controller work.
- Overall Knife14 estimate: `99%` for diagnosis/ops hardening, but final
  product acceptance is not `100%` until mini_vpn itself repeats clean
  `100+ Mbit/s`.

## 2026-07-08 - Knife14fq cleans timeout tail but does not close final acceptance

- Stage docs:
  `docs/tech/2026-07-08-knife14fq-timeout120-clean-tail-regression-results.md`,
  `docs/tech/2026-07-08-vps-install-and-optimization-guide.md`
- Log bundle:
  `/tmp/mini_vpn/knife14fq_socketbuf_timeout120_p1_30/mvpn_knife14fq_socketbuf_timeout120_p1_30_usclient_suite_20260708_094207.tar.gz`
- Outcome: the clean `f8765c1` repeat used `IPERF_TIMEOUT_SECS=120` and iperf
  exited normally, but reverse-first P1 regressed to `37.0/35.7 Mbit/s`.
- What worked: the timeout-driven close-tail was cleaned:
  `pending_at_close=0`, `terminal_pending_reap=0`, `egress_at_close=0`,
  `tun_tx_dropped_delta=0`, and QUIC loss/blocking/congestion stayed `0`.
- What failed: throughput was burst/idle and local pressure returned:
  `downlink_backpressure pause_edges=1 resume_edges=0`,
  `may_recv_false=13594`, `headroom_deferred_bytes=17512861`,
  `pressure_credit_blocked_bytes=923353`, and
  `hard_edge_guard_deferred_bytes=10524`.
- What stayed true: exit-side socket buffers are still a mandatory production
  and acceptance preflight. The mature sing-box client A/B remains valid server
  evidence, but the mini_vpn high-throughput run must be repeated cleanly before
  final completion.
- Reusable rule: do not treat a `100+ Mbit/s` run that exits by external iperf
  timeout as final acceptance. Require normal iperf exit plus clean close-tail,
  then only chase local pressure edges that repeat under the high-buffer
  preflight.
- Overall Knife14 estimate after this run: `99%`; remaining work is focused
  repeat/A-B plus a small local pressure-edge fix if the low result repeats.

## 2026-07-06 - Knife14cr repeats stable high reverse P1 with timer drain off

- Stage docs:
  `docs/tech/2026-07-06-knife14cr-repeat-stability-spec.md`,
  `docs/tech/2026-07-06-knife14cr-repeat-stability-plan.md`,
  `docs/tech/2026-07-06-knife14cr-repeat-stability-results.md`
- Log bundle:
  `/tmp/mini_vpn/knife14cr_repeat_stability_20260707_041152/mvpn_knife14cr_repeat_stability_usclient_suite_20260707_041152.tar.gz`
- Outcome: without code changes after Knife14cq, the repeat reverse-first P1
  stayed high at `174/172 Mbit/s` with `throughput_shape=stable_high`.
- What worked: the default-disabled timer path stayed disabled
  (`TUN RX active-flow timer drain: 0ms`,
  `timer_active_flow_attempts=0`), while event-driven drain handled
  ACK/window traffic (`attempts=65834`, `would_block=65752`).
- What stayed clean: `pending_at_close=0`, `egress_at_close=0`,
  `terminal_pending_reap=0`, `terminal_late_remote_payload=0`,
  `tun_tx_dropped_delta=0`, `drop_delta_total=0`, no send-slice or TUN flush
  errors, no QUIC loss/congestion/blocking, and no current `.33` TUIC
  `fail auth`.
- What to watch: the final one-second iperf interval dipped to `4.19 Mbit/s`,
  but six-sample tail stayed high (`tail_avg_mbps=153.865`) and
  `tail_collapse=0`.
- Reusable rule: after a high-throughput recovery, require at least one repeat
  with the same binary and default env before declaring the P1 root closed.
  Knife14 can now move to broader TCP regression rather than more P1
  lifecycle patches.
- Overall Knife14 estimate after this repeat: `93%`.

## 2026-07-06 - Knife14cq restores stable high reverse P1 by disabling timer drain default

- Stage docs:
  `docs/tech/2026-07-06-knife14cq-recent-active-timer-opt-in-spec.md`,
  `docs/tech/2026-07-06-knife14cq-recent-active-timer-opt-in-plan.md`,
  `docs/tech/2026-07-06-knife14cq-recent-active-timer-opt-in-results.md`
- Log bundle:
  `/tmp/mini_vpn/knife14cq_timer_optin_off_20260707_040635/mvpn_knife14cq_timer_optin_off_usclient_suite_20260707_040635.tar.gz`
- Outcome: local gates passed and reverse-first P1 recovered to stable high
  throughput: `182/181 Mbit/s`, `overall_avg_mbps=180.833`,
  `tail_avg_mbps=179.333`, `tail_collapse=0`.
- What worked: `MINI_VPN_TUN_RX_ACTIVE_FLOW_TIMER_MS` defaults to `0`, startup
  printed `TUN RX active-flow timer drain: 0ms`, and runtime diagnostics kept
  `timer_active_flow_attempts=0`.
- What stayed clean: direct `.27/.33 <-> .77` baselines were healthy, current
  `.33` checks had no TUIC `fail auth`, QUIC loss/congestion/blocking stayed
  zero, `tun_tx_dropped_delta=0`, `drop_delta_total=0`, `pending_at_close=0`,
  `terminal_pending_reap=0`, and `terminal_late_remote_payload=0`.
- What remains visible: `egress_at_close=443898` was still reported as
  `active_send_capable` and `close_egress_drain_candidate=true`, but this was
  not terminal pending/reap loss and did not prevent stable high receiver
  throughput.
- Reusable rule: when a VPS A/B proves a timer/background drain path runs and
  regresses throughput, keep it opt-in and restore the event-driven default.
  A clean stable-high P1 with explicit close-tail accounting is stronger than
  another hidden timer heuristic.
- Overall Knife14 estimate after this run: `91%`; remaining work is repeat
  stability and broader TCP regression, not another close/reap root hunt.

## 2026-07-06 - Knife14cp rejects recent-active timer ACK drain as default

- Stage docs:
  `docs/tech/2026-07-06-knife14cp-recent-active-ack-drain-spec.md`,
  `docs/tech/2026-07-06-knife14cp-recent-active-ack-drain-plan.md`,
  `docs/tech/2026-07-06-knife14cp-recent-active-ack-drain-results.md`
- Code commit: `5a3f4e9`.
- Valid log bundle:
  `/tmp/mini_vpn/knife14cp_recent_active_timer_retry_20260706_035508/mvpn_knife14cp_recent_active_timer_retry_usclient_suite_20260707_035508.tar.gz`
- Outcome: local gates passed and the new timer source engaged, but
  reverse-first P1 regressed to `19.2/18.0 Mbit/s`.
- What worked: the timer path was observable and bounded:
  `timer_active_flow_attempts=1321`, with clean pressure and lifecycle
  surfaces (`pause_edges=0`, `tun_tx_dropped_delta=0`, `pending_at_close=0`,
  `terminal_pending_reap=0`).
- What stayed clean: direct `.27/.33 <-> .77` baselines were healthy, current
  `.33` logs had no TUIC `fail auth`, QUIC loss/congestion/blocking stayed
  zero, and no send-slice or TUN flush errors appeared.
- What did not work: throughput was worse than Knife14co's `25.5/24.3 Mbit/s`,
  tail average fell to `5.950 Mbit/s`, and attribution remained
  `no_pressure_signal`.
- Reusable rule: do not keep adding below-pressure ACK-drain timer variants
  once diagnostics prove they run and throughput gets worse. Restore the
  cleaner active-flow-only default and move the next investigation to the
  no-pressure burst/idle stream scheduling or wake/read cadence branch.

## 2026-07-06 - Knife14co removes local pressure from P1 but exposes burst/idle without pressure

- Stage docs:
  `docs/tech/2026-07-06-knife14co-active-flow-ack-drain-spec.md`,
  `docs/tech/2026-07-06-knife14co-active-flow-ack-drain-plan.md`,
  `docs/tech/2026-07-06-knife14co-active-flow-ack-drain-results.md`
- Code commit: `3a3bdbf`.
- Log bundle:
  `/tmp/mini_vpn/knife14co_active_flow_ack_20260706_034114/mvpn_knife14co_active_flow_ack_usclient_suite_20260707_034114.tar.gz`
- Outcome: local gates passed and reverse-first P1 improved to
  `25.5/24.3 Mbit/s`, but still failed as `low_average`.
- What worked: active-flow TUN RX ACK/window drain ran below the credit edge
  (`attempts=7506`, `packets=24055`, `would_block=7491`) and removed local
  pressure during the attribution window: `tun_tx_dropped_delta=0`,
  `downlink_backpressure pause_edges=0`, `send_queue_max=427496`.
- What stayed clean: pending/close/reap accounting remained clean, QUIC
  loss/congestion/blocking stayed zero, `send_slice` errors stayed zero, and a
  current `.33` log check showed no `fail auth`.
- What did not work: throughput was still burst/idle with many zero intervals
  and attribution became `no_pressure_signal`, leaving stream read/pending gaps
  despite clean local pressure surfaces.
- Reusable rule: once active-flow drain reaches `would_block` and local
  pressure disappears, stop treating TUN drops/debt as the primary root. The
  next repair should cover post-burst ACK/window drain on a bounded
  recent-active timer, because a payload-triggered drain cannot run during the
  idle gaps it is meant to prevent.

## 2026-07-06 - Knife14cl removes hidden local-close rearm but exposes pressure debt recovery

- Stage docs:
  `docs/tech/2026-07-06-knife14cl-local-uplink-close-pending-deferral-spec.md`,
  `docs/tech/2026-07-06-knife14cl-local-uplink-close-pending-deferral-plan.md`,
  `docs/tech/2026-07-06-knife14cl-local-uplink-close-pending-deferral-results.md`
- Code commit: `877c05a`.
- Log bundle:
  `/tmp/mini_vpn/knife14cl_local_close_pending_20260706_1852/mvpn_knife14cl_local_close_pending_usclient_suite_20260707_025232.tar.gz`
- Outcome: local gates passed and reverse-first P1 improved to
  `32.9/31.9 Mbit/s`, but still failed as `low_average local_pressure=1`.
- What worked: `uplink_channel_closed` with pending downlink now defers close
  explicitly (`tcp-deferred-close-pending ... pending=528364`) instead of
  rearming. Parser close accounting reported `pending_at_close=0` and
  `egress_at_close=0`.
- What improved: the run left the former 10-20 Mbit/s band.
- What did not work: pending remained dirty after deferral and TUN drop
  feedback paused without recovery (`drop_delta_total=4334`,
  `drop_credit_debt_paid_bytes=0`, `pressure_credit_debt_paid_bytes=0`,
  `send_queue_max=892928`, `may_recv_false=8333`).
- Reusable rule: after hidden close/rearm is removed, do not keep editing close
  accounting. If useful pending is dirty and send-capable but drop/pressure
  debt is never paid, the next repair belongs in pressure credit recovery.

## 2026-07-06 - Knife14ck closes the ACK-drain budget branch

- Stage docs:
  `docs/tech/2026-07-06-knife14ck-ack-sized-pressure-drain-budget-spec.md`,
  `docs/tech/2026-07-06-knife14ck-ack-sized-pressure-drain-budget-plan.md`,
  `docs/tech/2026-07-06-knife14ck-ack-sized-pressure-drain-budget-results.md`
- Code commit: `d69ee27`.
- Log bundle:
  `/tmp/mini_vpn/knife14ck_ack_sized_drain_20260707_0240/mvpn_knife14ck_ack_sized_drain_usclient_suite_20260707_024026.tar.gz`
- Outcome: local gates passed and ACK-sized adaptive pressure drain behaved as
  intended, but reverse-first P1 stayed low at `16.5/15.7 Mbit/s`.
- What worked: the adaptive budget reached `would_block` instead of exhausting
  every pass (`attempts=764`, `budget_exhausted=1`, `would_block=763`), and
  the new source counters proved pre-payload, remote-payload, and maintenance
  drain paths all ran.
- What improved: runtime TUN egress drops fell from Knife14cj's thousands-level
  drop feedback to one event with `drop_delta_total=273`.
- What did not work: throughput stayed in the 10-20 Mbit/s band and the close
  tail still had active send-capable backlog:
  `close_pending_bytes=524906`, `close_egress_bytes=892928`,
  `tcp_state=CloseWait`, `can_send=true`, `may_send=true`.
- Reusable rule: once pressure TUN RX drain reaches `would_block`, do not keep
  increasing the ACK budget. The next repair must focus on local egress
  dirty-retention/flush cadence and active send-capable close drain.

## 2026-07-06 - Knife14ci proves ACK drain is real but post-payload is too late

- Stage docs:
  `docs/tech/2026-07-06-knife14ci-adaptive-tun-rx-ack-drain-spec.md`,
  `docs/tech/2026-07-06-knife14ci-adaptive-tun-rx-ack-drain-plan.md`,
  `docs/tech/2026-07-06-knife14ci-adaptive-tun-rx-ack-drain-results.md`
- Code commit: `554d3b4`.
- Log bundles:
  `/tmp/mini_vpn/knife14ci_adaptive_ack_drain_20260707_0208/mvpn_knife14ci_adaptive_ack_drain_usclient_suite_20260707_020847.tar.gz`,
  `/tmp/mini_vpn/knife14ci_adaptive_ack_drain_retry_20260707_0212/mvpn_knife14ci_adaptive_ack_drain_retry_usclient_suite_20260707_021233.tar.gz`
- Outcome: local gates passed and the retry VPS suite reached P1, but
  reverse-first stayed low at `18.8/17.9 Mbit/s`.
- What worked: the default-safe pressure adaptive path engaged:
  `tcp-tun-rx-drain attempts=5 packets=105 tcp=105 budget_exhausted=5`.
  This proves ready ACK/window traffic exists at the egress credit edge.
- What did not work: post-payload drain was too late. The run still saw
  `tun_tx_dropped_delta=82`, `send_queue_max=892928`, multi-second TUIC data
  read gaps, and close-tail active send-capable backlog
  (`pending=525514`, `close_egress_bytes=892928`).
- Reusable rule: do not solve this by raising a static TUN RX drain knob.
  The next repair should use the same pressure gate but drain before accepting
  more remote payload and at pressure-maintenance points while dirty downlink
  remains.

## 2026-07-06 - Knife14ch redirects pressure work toward adaptive ACK drain

- Stage docs:
  `docs/tech/2026-07-06-knife14ch-adaptive-pressure-credit-debt-spec.md`,
  `docs/tech/2026-07-06-knife14ch-adaptive-pressure-credit-debt-plan.md`,
  `docs/tech/2026-07-06-knife14ch-adaptive-pressure-credit-debt-results.md`
- Code commit: `0cc6d31`.
- Log bundles:
  `/tmp/mini_vpn/knife14ch_adaptive_pressure_20260707_0142/mvpn_knife14ch_adaptive_pressure_usclient_suite_20260707_014241.tar.gz`,
  `/tmp/mini_vpn/knife14ch_adaptive_pressure_repeat_20260707_0149/mvpn_knife14ch_adaptive_pressure_repeat_usclient_suite_20260707_014902.tar.gz`
- Outcome: local TDD/regression gates passed, but VPS acceptance failed. The
  first run was `no_data` and did not exercise adaptive pressure debt. The
  repeat was actionable: reverse-first P1 was `16.0/15.5 Mbit/s` with healthy
  `.27 -> .77` and `.33 -> .77` baselines.
- What worked: adaptive pressure debt stayed bounded and no TUN drops were
  observed in the repeat (`runtime_tun_egress drop_events=0`). Current-window
  `.33` TUIC auth was clean and QUIC loss/congestion/blocking deltas were zero.
- What did not work: `pressure_credit_debt_bytes=0` because the repeat touched
  the credit spend edge (`send_queue_max=892928`) without crossing the
  backpressure pause edge. The run still closed with
  `terminal_late_remote_payload_bytes=1834980` and
  `hard_edge_guard_limited=47`.
- Reusable rule: do not keep adding pressure-debt variants just to make the
  guard edge quieter. When TUN drops are zero but send_queue rides the credit
  guard and throughput is low, prioritize adaptive local ACK/TUN-RX drain so
  smoltcp send capacity is freed by processing ACK/window updates, not by
  reading less remote data.

## 2026-07-06 - Knife14cg rejects bounded global receive decoupling as default

- Stage docs:
  `docs/tech/2026-07-06-knife14cg-bounded-global-rx-receive-window-spec.md`,
  `docs/tech/2026-07-06-knife14cg-bounded-global-rx-receive-window-plan.md`,
  `docs/tech/2026-07-06-knife14cg-bounded-global-rx-receive-window-results.md`
- Log bundle:
  `/tmp/mini_vpn/knife14cg_global_rx_receive_20260707_0126/mvpn_knife14cg_global_rx_receive_usclient_suite_20260707_012632.tar.gz`
- Outcome: local TDD/regression gates passed, but the scoped VPS A/B failed.
  Reverse-first P1 stayed low at `20.0/18.7 Mbit/s`.
- What worked: the new `tcp-global-rx-backpressure` metric and parser summaries
  proved the receive window engaged at the intended bound
  (`receive_high=2097152`, `max_pending_bytes=2123091`), and final suite
  parsing exposed the same close-tail receive-window state.
- What did not work: decoupling TUIC/global receive from local TUN egress
  pressure increased app-owned pending instead of restoring throughput. The
  run closed with `pending=2123091`, `close_egress_bytes=892928`, and final TUN
  egress drops totaling `4051`.
- Clean surfaces: direct `.27 -> .77` and `.33 -> .77` baselines stayed
  healthy, current-window `.33` TUIC auth was clean, QUIC loss/congestion
  deltas were zero, `send_slice` errors were zero, and TUN flush failures were
  zero.
- Code decision: keep bounded global receive decoupling as an explicit A/B via
  `MINI_VPN_BOUNDED_GLOBAL_RX_RECEIVE_WINDOW=1`; the default path uses the
  prior safe receive gate.
- Reusable rule: do not fix the current bottleneck by increasing read-ahead or
  receive-window space. If bounded pending fills while local egress is stuck at
  `send_queue_max=892928`, the next repair must improve local egress
  drain/cadence rather than letting more remote bytes accumulate.

## 2026-07-06 - Knife14cf pressure debt reduces drops but not burst/stall throughput

- Stage docs:
  `docs/tech/2026-07-06-knife14cf-proactive-egress-credit-gate-spec.md`,
  `docs/tech/2026-07-06-knife14cf-proactive-egress-credit-gate-plan.md`,
  `docs/tech/2026-07-06-knife14cf-proactive-egress-credit-gate-results.md`
- Log bundle:
  `/tmp/mini_vpn/knife14cf_pressure_credit_20260707_0112/mvpn_knife14cf_pressure_credit_usclient_suite_20260707_011213.tar.gz`
- Outcome: local TDD/regression gates passed, and proactive pressure credit
  engaged on VPS, but clean reverse-first P1 still failed at
  `20.5/19.5 Mbit/s`.
- What worked: pressure debt was visible in `tcp-egress-credit-debt` and
  `tcp-downlink-flush`; probe TUN drops fell from Knife14ce's `2813` to `539`,
  and feedback recovered in the probe (`pause_edges=1 resume_edges=1`).
- What did not work: local tx-queue pressure still reached
  `send_queue_max=892928`, the close tail still had active send-capable backlog
  (`pending=574203`, `close_egress_bytes=892928`), and the data TUIC stream
  still showed multi-second read gaps (`max_read_gap_ms=4325`).
- Clean surfaces: no current-window TUIC `fail auth`, healthy `.27 -> .77` and
  `.33 -> .77` baselines, no QUIC loss/congestion/blocking deltas, no
  `send_slice` zero/errors, no TUN flush failures, and no terminal pending reap.
- Reusable rule: proactive debt/threshold gating can reduce damage signals but
  is not the full Knife14 fix when throughput remains burst/stall. Stop adding
  more static debt as the primary repair; evaluate bounded receive-path
  decoupling so TUIC stream/window progress is not coarsely tied to local TUN
  egress bursts.

## 2026-07-06 - Knife14ce proves post-drop credit debt is too late

- Stage docs:
  `docs/tech/2026-07-06-knife14ce-drop-aware-egress-credit-spec.md`,
  `docs/tech/2026-07-06-knife14ce-drop-aware-egress-credit-plan.md`,
  `docs/tech/2026-07-06-knife14ce-drop-aware-egress-credit-results.md`
- Log bundle:
  `/tmp/mini_vpn/knife14ce_drop_credit_20260707_0049/mvpn_knife14ce_drop_credit_usclient_suite_20260707_005007.tar.gz`
- Outcome: local TDD and regressions passed, but scoped VPS acceptance failed.
  Reverse-first P1 fell back to `23.6/21.5 Mbit/s` while direct `.27 -> .77`
  and `.33 -> .77` baselines stayed healthy.
- Key signal: TUN feedback installed global debt
  (`drop_credit_generation=1 drop_credit_debt_bytes=196608`), but
  `tcp-downlink-flush` and final lifecycle still reported
  `drop_credit_debt_bytes=0 drop_credit_debt_paid_bytes=0
  drop_credit_blocked_bytes=0`. The first positive TUN drop feedback arrived
  near the close tail after the harmful local tx-queue burst had already
  happened.
- Clean surfaces: default pool=2 stayed active with `conns=0,1`, no current
  TUIC `fail auth`, no QUIC loss/congestion/blocking deltas, no send-slice
  zero/errors, no TUN flush failures, and no terminal pending/egress-at-close
  hidden loss.
- Reusable rule: post-drop credit debt is a valid invariant but not an adequate
  first control signal. The next behavior patch must move credit denial earlier
  to local egress pressure edges and prove feedback pause can resume from raw
  low pressure.

## 2026-07-06 - Knife14bw narrows reverse failure to local tx-queue cadence

- Code commit: `4a12b18`
- Result doc:
  `docs/tech/2026-07-06-knife14bw-stream-starvation-diagnostics-results.md`
- Log bundle:
  `/tmp/mini_vpn/knife14bw_starvation_diag_20260706/mvpn_knife14bw_starvation_diag_usclient_suite_20260706_212704.tar.gz`
- Outcome: diagnostic-only VPS acceptance still failed the throughput target
  (`19.8/18.9 Mbit/s`), but the result was no longer a complete no-data
  window. The reverse flow showed burst/idle intervals and about `72.7MB`
  delivered on the TUIC data stream.
- Key signal: new `tcp_reverse_window` samples showed live local TCP accepted
  reverse payloads with `send_capacity=1048576`, `pending=0`, `active=true`,
  `can_send=true`, and `may_recv=true` until the close tail. At the same time
  `downlink_backpressure` toggled `51/51` times on smoltcp tx-queue pressure
  (`max_tx_queue_bytes=588901`) while app-owned pending stayed `0`.
- Clean surfaces: current-window TUIC auth succeeded, direct `.27/.33 -> .77`
  reverse baselines were healthy, QUIC loss/congestion/blocking deltas were
  `0`, TUN drops were `0`, send-slice errors were `0`, and terminal pending
  reap stayed `0`.
- Reusable rule: if reverse throughput is low with non-tiny data-stream bytes,
  full local send capacity, no app pending, and many tx-queue backpressure
  pause/resume edges, stop pursuing TUIC stream starvation and close-drain
  roots. The next behavior patch should smooth local tx-queue pressure cadence
  while preserving bounded read-ahead.

## 2026-07-06 - Knife14bw adds reverse stream starvation diagnostics before behavior changes

- Stage docs:
  `docs/tech/2026-07-06-knife14bw-stream-starvation-diagnostics-plan.md`
- Outcome: added behavior-neutral `tcp-reverse-window` diagnostics on accepted
  reverse payloads, rate-limited to first payload plus a five-second refresh
  cadence, with urgent refresh on inactive/no-send/no-recv-window states. The
  state is cleared on `rearm_socket` so listener slot reuse cannot inherit old
  diagnostic cadence.
- Parser update: low-RTT summaries now report `tcp_reverse_window` and label
  clean no-data reverse windows as `target_sender_stalled`; a small data stream
  with long TUIC pending/read gaps also gets `tuic_stream_starved`; close-tail
  late remote bytes recorded on the close line get
  `terminal_closed_late_payload`.
- TDD signal: focused tests cover the reverse-window log line, rate limiting,
  and rearm cleanup. The low-RTT self-test includes a Knife14bv-shaped no-data
  sample requiring `tcp_reverse_window`, `tuic_stream_starved`,
  `target_sender_stalled`, and `terminal_closed_late_payload`.
- Verification passed: low-RTT probe self-test, US-client suite self-test,
  shell syntax checks, focused reverse-window tests, `cargo test`,
  `cargo test --features harness`, `cargo build --release`,
  `cargo clippy --all-targets --features harness -- -D warnings`, and
  `git diff --check`.
- Reusable rule: when a clean reverse-first run has tiny TUIC data-stream bytes
  and no local pressure, do not patch close-drain or pacing first. Add a live
  local TCP window sample at the exact remote-payload boundary, then let the VPS
  result decide between TUIC server-to-client stream starvation and local
  TCP ACK/receive-window behavior.

## 2026-07-05 - Knife14bg rejects TUN RX drain cadence as the clean reverse root

- Code commit: `356e2d2`
- Result doc:
  `docs/tech/2026-07-05-knife14bg-tun-rx-drain-cadence-results.md`
- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14bg_tun_rx_drain_usclient_suite_20260705_085637.tar.gz`
- Outcome: the bounded nonblocking TUN RX drain patch passed local gates but
  failed VPS acceptance. Clean reverse-first P1 collapsed to
  `0.315/0.025 Mbit/s`.
- Key signal: the new drain path was active in the clean window
  (`tun_rx_drain packets=25 tcp=25 errors=0`), but `downlink_backpressure`,
  terminal pending, close-time pending, TUN drops, TUN flush failures,
  `send_slice` zero/errors, global_rx pressure, and clean-window QUIC
  loss/congestion were all zero. The data stream delivered only about `92 KiB`
  and the attribution moved to `tuic_stream_read_gap+relay_remote_read_gap`.
- Baseline check: `.27 -> .77` and `.33 -> .77` direct reverse preflights were
  healthy (`280` and `283 Mbit/s` receiver), and `.33` sing-box stayed active
  with normal TUIC inbound/direct outbound log lines.
- Reusable rule: when clean reverse-first has tiny remote-to-local bytes,
  active `tun_rx_drain`, and no local downlink pressure, stop changing TUN,
  pending, close/reap, or tx-queue behavior. Add behavior-neutral TUIC stream
  gap evidence and run a scoped reverse-only A/B before the next behavior
  patch.

## 2026-07-04 - Knife14az shifts the failed reverse root before local downlink drain

- Code commit: `f21e782`
- Result doc:
  `docs/tech/2026-07-04-knife14az-local-tcp-socket-policy-results.md`
- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14az_tcp_policy_usclient_suite_20260704_224324.tar.gz`
- Outcome: the local smoltcp socket policy patch passed local gates but failed
  VPS acceptance. Clean reverse-first P1 regressed to `0.245/0.009 Mbit/s`,
  and the suite skipped later windows because an active relay did not clear
  within the quiet wait.
- Key signal: clean summary showed `send_queue_max=1`, pending `0`, no
  downlink backpressure edges, no terminal or close-time pending, no TUN drops,
  and no QUIC loss/congestion. The attribution was
  `reverse_sender_backpressured`, with only about `35 KiB` accepted from the
  reverse data stream during the summary.
- Baseline check: `.33 -> .77` direct forward/reverse remained healthy
  (`231/232 Mbit/s` receiver), so the failure is not a simple exit-target path
  bottleneck.
- Reusable rule: when reverse-first fails with
  `reverse_sender_backpressured` and mini_vpn receives only tiny downlink bytes,
  stop tuning local tx-queue/pending/close-drain. Instrument TUIC TCP stream
  first-byte latency and remote read gaps before the next behavior patch.

## 2026-07-04 - Knife14az locks local TCP socket policy at listener and rearm

- Code commit: this Knife14az local policy stage commit.
- Stage docs:
  `docs/tech/2026-07-04-knife14az-local-tcp-socket-policy-spec.md`,
  `docs/tech/2026-07-04-knife14az-local-tcp-socket-policy-plan.md`
- Outcome: added a single local virtual-link TCP socket policy helper that
  disables smoltcp Nagle and delayed ACK for listener sockets. The same helper
  runs again before `listen` during rearm so a reused socket cannot drift back
  to smoltcp defaults after a flow closes.
- TDD signal: the listener test first failed on `nagle_enabled()`, and the
  rearm test first failed after deliberately restoring Nagle and delayed ACK
  before `rearm_socket`. Both turned green after applying the helper in the two
  required paths.
- Verification passed: focused listener/rearm tests,
  `cargo test --lib client_tun`, low-RTT probe self-test, US-client suite
  self-test, `git diff --check`, full `cargo test`, `cargo test --features
  harness`, and `cargo clippy --all-targets --features harness -- -D warnings`.
  QUIC endpoint-bind tests were confirmed outside the restricted sandbox.
- Reusable rule: local smoltcp socket options are part of lifecycle state.
  Apply them both at first listener construction and immediately before rearm
  `listen`; otherwise later connections can silently differ from the intended
  virtual-link TCP policy.

## 2026-07-04 - Knife14ay closes the terminal-pending ambiguity

- Code commit: `67a8c46`
- Result doc:
  `docs/tech/2026-07-04-knife14ay-local-tcp-closed-transition-results.md`
- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14ay_tcp_lifecycle_usclient_suite_20260704_222042.tar.gz`
- Outcome: the behavior-neutral lifecycle diagnostics worked. Clean
  reverse-first P1 still failed (`9.15/8.18 Mbit/s`), but the new transition
  line showed the reverse data socket moved from `Established` to `Closed` in
  `dirty_relay` with `pending=0`, `terminal_candidate=false`, and about
  `30.8 MiB` already accepted into smoltcp. The later `552440` pending bytes
  were produced after that close edge when the remote relay delivered EOF.
- Key signal: clean-window QUIC loss/congestion, TUN drops, TUN flush
  failures, deferred TUN flush, `send_slice_zero`, and `send_slice_errors`
  were all zero. The active limiter was local downlink tx-queue/receive-window
  behavior: tx-queue backpressure paused/resumed remote reads while app-owned
  pending stayed zero until the close tail.
- Reusable rule: when lifecycle transition reports `terminal_candidate=false`
  at `Established -> Closed` and terminal pending appears only afterward, stop
  changing close-drain/reap policy. Focus the next patch on smoltcp local TCP
  socket send policy and tx-queue drain rate.

## 2026-07-04 - Knife14ay makes local TCP Closed transitions observable

- Code commit: this Knife14ay stage commit.
- Stage docs:
  `docs/tech/2026-07-04-knife14ay-local-tcp-closed-transition-spec.md`,
  `docs/tech/2026-07-04-knife14ay-local-tcp-closed-transition-plan.md`
- Outcome: added behavior-neutral per-handle TCP lifecycle observations. The
  first observation seeds state, later changes in TCP state or send/receive
  capability emit `tcp-lifecycle-transition` with previous source/state,
  current state, send/recv capability, pending bytes, remote-to-local bytes,
  local FIN fields, and `terminal_candidate`. The observation is reset on
  rearm, and close/reap/backpressure/TUIC/TUN behavior is unchanged.
- Parser update: low-RTT summaries now report `tcp_lifecycle` transitions,
  closed edges, terminal candidates, max pending, max remote-to-local bytes,
  sources, and states. A terminal transition adds the
  `local_tcp_terminal_transition` attribution label.
- Verification passed: focused Rust lifecycle diagnostic test,
  `cargo test --lib client_tun`, low-RTT probe self-test, US-client suite
  self-test, shell syntax check, `cargo test`, `cargo test --features
  harness`, `cargo clippy --all-targets --features harness -- -D warnings`,
  and `git diff --check`. The full cargo tests that bind local QUIC endpoints
  were rerun outside the restricted sandbox and passed.
- Reusable rule: when terminal pending appears with tiny reverse downlink
  volume, do not infer the root from the final close line alone. Capture the
  source immediately before `Closed` so the next behavior patch can distinguish
  local peer close, relay close, dirty-pass transition, and reaper discovery.

## 2026-07-04 - Knife14ax rejects tx-queue backpressure as the clean reverse root

- Code commit: `58f847d`
- Result doc:
  `docs/tech/2026-07-04-knife14ax-tx-queue-backpressure-results.md`
- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14ax_tx_queue_backpressure_usclient_suite_20260704_215053.tar.gz`
- Outcome: tx-queue-aware backpressure passed local gates but failed VPS
  acceptance. Clean reverse-first P1 did not complete (`iperf3: unable to
  receive results`). The new summary fields were active but stayed zero:
  `pause_edges=0`, `max_tx_queue_bytes=0`, and `max_pressure_bytes=0`.
- Key signal: the reverse data flow only reached about `221884` remote-to-local
  bytes before the local TCP socket was reaped as
  `tcp_state=Closed active=false can_send=false`, with `41208` terminal pending
  bytes. Clean-window QUIC loss/congestion, TUN drops, TUN flush failures, and
  global_rx pressure were all zero.
- Reusable rule: when reverse fails with tiny `remote_to_global_rx_bytes` and no
  tx-queue/global_rx/TUN/QUIC pressure, stop tuning backpressure. Instrument the
  local TCP state transition into `Closed` and prove whether the local peer
  closed early, a FIN/RST was mishandled, or the relay stopped reading before
  useful downlink arrived.

## 2026-07-04 - Knife14ax keeps tx-queue pressure visible to global_rx backpressure

- Stage docs:
  `docs/tech/2026-07-04-knife14ax-tx-queue-backpressure-spec.md`,
  `docs/tech/2026-07-04-knife14ax-tx-queue-backpressure-plan.md`
- Outcome: changed downlink backpressure from app-pending-only to
  tx-queue-aware pressure. Handles that accept downlink bytes into smoltcp now
  stay dirty while their socket tx queue remains above the low watermark, and
  `global_rx_paused` uses the max of app-owned pending and smoltcp tx queue
  pressure. The low-RTT parser now reports tx-queue pressure in
  `downlink_backpressure`.
- Verification passed: focused downlink TDD red/green test,
  `cargo test --lib client_tun`, low-RTT probe self-test, US-client suite
  self-test, shell syntax checks, `cargo test`, `cargo test --features
  harness`, `cargo clippy --all-targets --features harness -- -D warnings`,
  and `git diff --check`.
- Reusable rule: if `send_slice` accepts bytes into smoltcp, app-owned
  `downlink_pending` is no longer a sufficient backpressure signal. Keep the
  handle observable until the smoltcp tx queue also drains below the resume
  watermark.

## 2026-07-04 - Knife14aw identifies smoltcp tx queue pressure as the missing feedback edge

- Code commit: `ef7364c`
- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14aw_send_window_globalrx_usclient_suite_20260704_204329.tar.gz`
- Result doc:
  `docs/tech/2026-07-04-knife14aw-send-window-globalrx-results.md`
- Outcome: the behavior-neutral send-window/global_rx diagnostics worked, but
  clean reverse-first P1 still failed at `16.4/15.0 Mbit/s`. Direct `.27` and
  `.33` baselines to `.77` were about `196-198 Mbit/s`, so the public path was
  not in the failed throughput band.
- Key signal: `pending_total=0` while `send_queue_max=1048576`,
  `send_capacity_min=max=1048576`, `global_rx_queue_used_max=258/1024`,
  `send_slice` zero/error was zero, and clean-window QUIC loss/congestion was
  zero. The close tail was explicitly terminal
  (`tcp_state=Closed active=false may_send=false`) and accounted as
  `terminal_pending_reap=524627`.
- Reusable rule: when app-owned pending is empty but smoltcp tx queue is full,
  `global_rx_paused` must treat the tx queue as downlink pressure. Otherwise the
  report can show `pending_total=0` while local TCP/TUN egress is still the
  limiter.

## 2026-07-04 - Knife14aw exposes send-window and relay queue state before changing behavior

- Stage docs:
  `docs/tech/2026-07-04-knife14aw-send-window-globalrx-spec.md`,
  `docs/tech/2026-07-04-knife14aw-send-window-globalrx-plan.md`
- Outcome: added behavior-neutral diagnostics for smoltcp downlink send-window
  state and relay `global_rx` queue occupancy. `tcp-downlink-flush` now
  reports `send_window_samples`, `send_capacity_min/max`, `send_queue_max`,
  `recv_queue_max`, `may_send_false`, `may_recv_false`,
  `no_send_streak_max`, and `no_send_pending_max`. Relay live/close lines now
  report `global_rx_queue_used_max` and `global_rx_queue_capacity`, and close
  logs include `may_send`, `may_recv`, `send_capacity`, `send_queue`, and
  `recv_queue`.
- Verification passed: focused Rust tests, `cargo test --lib client_tun`, low
  RTT probe self-test, US-client suite self-test, shell syntax checks,
  `cargo test`, `cargo test --features harness`,
  `cargo clippy --all-targets --features harness -- -D warnings`, and
  `git diff --check`.
- Reusable rule: when a large bounded channel or socket buffer can hide queueing
  without crossing a wait threshold, log occupancy/capacity directly. Wait
  counters alone are not enough to distinguish "not blocked" from "buffered but
  not yet blocked".

## 2026-07-04 - Knife14av closes the reporting gap but not the reverse throughput gap

- Code commit: `b7e10b1`
- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14av_post_iperf_close_tail_usclient_suite_20260704_202011.tar.gz`
- Result doc:
  `docs/tech/2026-07-04-knife14av-post-iperf-close-tail-results.md`
- Outcome: the post-iperf settle window worked. The clean reverse-first raw
  `tcp-handle-close pending>0` line was included in the per-probe
  `terminal_pending_reap` and `pending_at_close` summaries.
- Throughput still failed: reverse-first P1 was `11.4/10.1 Mbit/s`, with no
  clean-window QUIC loss/congestion, no local write pressure, no
  `send_slice` zero/error, no TUN flush failure, and no immediate flush
  deferral.
- Remaining blind spot: `global_rx_pressure=0` only means the relay task did
  not wait beyond the pressure threshold; it does not prove the channel had no
  backlog. `no_send_capacity` also lacks `send_capacity`, `send_queue`,
  `recv_queue`, `may_send`, and no-send streak detail.
- Reusable rule: before another close/reap or pacing change, add
  behavior-neutral smoltcp send-window and relay queue occupancy diagnostics so
  a low reverse run can show where bytes are waiting before terminal close.

## 2026-07-04 - Knife14at makes TUN egress drops runtime-observable, but not sufficient root cause

- Code commit: `e8a7c41`
- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14at_tun_egress_feedback_usclient_suite_20260704_191427.tar.gz`
- Result doc:
  `docs/tech/2026-07-04-knife14at-tun-egress-feedback-results.md`
- Outcome: the scoped reverse-first P1 window reached `185/183 Mbit/s`, so the
  Knife14as low reverse result (`22.2 Mbit/s` receiver) did not reproduce.
  The clean window had `terminal_pending_reap=0`, no relay-late-remote signal,
  no local/global write pressure, no QUIC loss/congestion deltas,
  `send_slice_zero=0`, `send_slice_errors=0`, `tun_flush_failures=0`, and
  `tun_flush_deferred=0`.
- New discriminator: runtime `tcp-tun-egress` worked and matched probe-level
  accounting in the clean reverse-first window:
  `runtime_tun_egress drop_delta_total=376` and
  `tun_tx_dropped_delta=376`.
- Corrected assumption: local TUN egress drops plus downlink backpressure are
  real, but they are not sufficient to explain low reverse throughput by
  themselves. In this run reverse throughput stayed high while those signals
  were present.
- Separate branch: forward P1 remained bad (`5.56/2.06 Mbit/s`) with
  `quic_loss_congestion+local_write_pressure+local_tun_egress_drop`, and later
  reverse windows inherited QUIC congestion but still reached `184/180 Mbit/s`.
- Reusable rule: after a runtime drop signal is observable, do not treat the
  label as causal without throughput correlation. Split reverse downlink egress
  from forward/uplink QUIC congestion before changing TUN queues, watermarks,
  downlink pacing, or close-drain logic again.

## 2026-07-04 - Knife14as separated terminal pending from active reverse bottleneck

- Code commits: `575a448`, `2205734`
- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14as_terminal_pending_reversefirst2_usclient_suite_20260704_182136.tar.gz`
- Result doc:
  `docs/tech/2026-07-04-knife14as-terminal-pending-lifecycle-results.md`
- Outcome: VPS throughput still failed, but the lifecycle branch got a clear
  discriminator. In the clean reverse-first window, receiver throughput was
  `22.2 Mbit/s`, `terminal_pending_reap=0`, QUIC loss/congestion deltas were
  zero, `send_slice_zero=0`, `send_slice_errors=0`,
  `tun_flush_failures=0`, and `tun_flush_deferred=0`; the active attribution was
  `local_tun_egress_drop+local_downlink_backpressure`.
- Close-boundary signal: after the clean measurement, the same reverse flow
  closed with `terminal_pending_reap_bytes=555611` and
  `tcp_state=Closed active=false can_send=false`. That makes terminal pending a
  post-close accounting outcome, not the direct clean-window throughput root.
- Reusable rule: when terminal pending appears after a reverse throughput
  window, first ask whether it was present during the measurement summary. If
  `terminal_pending_reap=0` in-window, shift the next design to the earlier
  active egress/backpressure path instead of adding more closed-socket grace.
- Next rule: do not tune queue length, TUIC pool, iperf3, sing-box, or the old
  immediate-flush pacer again before designing local TUN egress feedback or
  product-safe shaping that ties remote downlink reads to actual local egress
  capacity.

## 2026-07-04 — Knife14as starts with terminal pending accounting, not behavior tuning

Commit `575a448` added the first Knife14as slice: design/spec docs,
behavior-neutral TCP close diagnostics, and parser accounting for terminal
pending downlink bytes. All TCP close paths now log a pre-abort smoltcp socket
snapshot, and `Closed && active=false && can_send=false` pending bytes are
reported as `terminal_pending_reap_bytes`. The low-RTT probe summary now emits
`terminal_pending_reap: events=... bytes=... max_bytes=...` and keeps backward
compatibility with older logs that only had `pending`, `tcp_state=Closed`, and
`can_send=false`.

Outcome: local gates passed (`cargo test --lib client_tun`, full
`cargo test --lib`, full `cargo test`, harness test, clippy with harness,
low-RTT probe self-test, US-client suite self-test, shell syntax checks, and
`git diff --check`). No data-plane send, pacing, close, backpressure, TUIC, or
TUN behavior was intentionally changed in this slice.

Reusable rule: when a close-boundary tail appears after low reverse throughput,
do not immediately tune around the tail. First separate "terminal and no longer
deliverable" from "still deliverable but prematurely closed or window-starved"
with explicit accounting in both the process log and acceptance summary.

## 2026-07-04 — Knife14ar falsifies egress deferral as the remaining reverse root

Commit `3baf476` was tested from `.27` with the close-safe egress pacing
default. Bundle:
`/tmp/mini_vpn/mvpn_knife14ar_close_safe_default_usclient_suite_20260704_164651.tar.gz`.
The suite proved the code change took effect: default
`MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES=16777216`, startup logged that pending
backlog forces immediate flush, and the clean reverse-first attribution had
`tun_flush_deferred=0`.

The acceptance still failed. Clean reverse-first P1 reached only
`17.2/16.2 Mbit/s`, below the Knife14ap receiver result of `22.0 Mbit/s`, even
though direct `.27 -> .77` and `.33 -> .77` reverse baselines were healthy
(`271 Mbit/s` and `285 Mbit/s`). The clean reverse window had no new QUIC
loss/congestion, no TUN drops, no `send_slice` zero/error, and no TUN flush
failure. The only remaining primary attribution was local downlink
backpressure.

The decisive close-boundary signal moved: after the clean reverse window,
`dead_slot_reap` still appeared with `pending=224765`,
`pending_high=583624`, `tcp_state=Closed`, `active=false`,
`can_send=false`, and `tun_flush_deferred=0`. Later polluted reverse windows
also closed with roughly `524-534 KiB` pending and no deferral. Reusable rule:
stop modifying the egress pacer for this symptom. The next stage must model
local TCP close/drain semantics and explicitly account pending bytes at terminal
reap boundaries before another throughput acceptance run.

## 2026-07-04 — Knife14ar makes downlink egress pacing close-safe by default

Knife14aq proved that a small default remote-payload egress budget was useful
as a probe but unsafe as a product default. Knife14ar therefore keeps the
`MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES` knob but changes the default from
`65536` to the maximum bounded value (`16777216`) so normal runs preserve the
old immediate-flush behavior instead of reusing the rejected pacing default.
The `.27` suite default now matches Rust so acceptance does not accidentally
override the product default.

The pacer decision now receives the current downlink pending size. It may defer
only when accepted bytes leave no app-owned backlog; any non-empty
`downlink_pending` forces an immediate `iface.poll + flush_tx` attempt, even
with a zero or exhausted budget. Close/reap diagnostics also include
`tun_flush_deferred`, so the next VPS bundle can correlate pending-at-close
with pacing decisions.

Verification passed locally with focused downlink egress tests, `client_tun`,
full `cargo test`, low-RTT probe self-test, US-client suite self-test, and
`git diff --check`. Reusable rule: when pacing an accepted downlink path,
never let the pacing gate outrank lifecycle safety. Backlog ownership and
close-boundary drain/progress must stay visible and preferred over drop-count
reduction.

## 2026-07-04 — Knife14aq rejects blunt remote-payload egress pacing as a default

Commit `212ce26` passed local tests and was run from `.27` with
`MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES=65536`. Bundle:
`/tmp/mini_vpn/mvpn_knife14aq_egress_pacing_default_usclient_suite_20260704_160317.tar.gz`.
Direct baselines were healthy: `.27 -> .77` reverse receiver was
`295 Mbit/s`, and `.33 -> .77` reverse receiver was `283 Mbit/s`.

The clean reverse-first window proved the pacer was active and not a server
problem: `tun_flush_deferred=1057`, no clean QUIC loss/congestion delta,
`send_slice_zero=0`, `send_slice_errors=0`, and `tun_flush_failures=0`. It did
reduce TUN egress drops versus Knife14ap (`366 -> 65`), but receiver throughput
fell from `22.0 Mbit/s` to `13.8 Mbit/s`, and downlink backpressure still
oscillated around the high watermark.

The important new discriminator is the close boundary. After the clean reverse
window, a relay was reaped with `reason=dead_slot_reap`, `pending=576827`,
`tcp_state=Closed`, `can_send=false`, and `can_recv=false`. Later reverse
windows also closed with roughly `534 KiB` pending. The likely root cause is
that skipping immediate `iface.poll + flush_tx` after remote payload acceptance
can delay local egress enough for useful pending bytes to meet local close/reap.

Reusable rule: do not ship blunt remote-payload immediate-flush deferral as the
default fix. The next design must be close-safe first: pending downlink bytes
must either drain, remain owned by a live relay, or be explicitly accounted as a
terminal loss before the relay is reaped. Pacing should move closer to the TUN
write cadence or include forced drain/progress around close boundaries.

## 2026-07-04 — Knife14aq adds observable remote-payload egress pacing

Knife14ap showed the clean reverse limiter had moved after smoltcp acceptance:
`send_slice` succeeded, TUN flush calls returned success, but Linux TUN/qdisc
still reported egress drops. Knife14aq therefore avoids another watermark or
server-side guess and adds a product-side egress pacer for TCP downlink
remote-payload events.

The stage gates immediate `iface.poll + flush_tx` after remote payload
acceptance with `MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES` (default 65536 bytes
per timer interval, `0` for timer-only). Accepted bytes are still preserved in
smoltcp/downlink pending; only the immediate TUN write is deferred to the
existing timer poll/flush path when the budget is exhausted. The aggregate
`tcp-downlink-flush` diagnostic now includes `tun_flush_deferred`, and the
low-RTT probe summarizes it in `downlink_flush:`.

Verification passed locally with `cargo test`, both low-RTT and US-client suite
self-tests, and `git diff --check`. This is not yet a throughput conclusion:
the next reusable rule is to require a clean `.27` reverse-first acceptance run
before deciding whether egress pacing reduced TUN drops/backpressure or needs a
different local queue strategy.

## 2026-07-04 — Promote repeated environment traps into default operating rules

User feedback after the Knife14ap run clarified that repeated sudo/TTY and
GitHub credential failures should not be rediscovered every session. When an
environment issue has a known working path, promote it into `AGENTS.md` and use
that path directly on future runs.

Reusable rule: for `.27` suites that may need `sudo -v`, start with a true
writable TTY and enter the password only at the prompt. For Mac mini GitHub
pushes where HTTPS origin cannot read credentials, use the working SSH push URL
instead of retrying HTTPS. Do not write passwords or secrets into commands,
scripts, docs, reports, learning memory, or final summaries.

## 2026-07-04 — Knife14ap acceptance pins the remaining clean reverse limiter after smoltcp acceptance

Commit `27aa73f` ran from `.27` with the new downlink flush diagnostics active.
Bundle:
`/tmp/mini_vpn/mvpn_knife14ap_flushdiag_default_bp512_128_tty2_usclient_suite_20260704_154215.tar.gz`.
The run used `bbr`, TUIC TCP pool `1`, TUN MTU `1200`, default TUN qlen `500`,
512/128 KiB downlink watermarks, and a 256 KiB flush budget. Direct baselines
were healthy: `.27 -> .77` reverse receiver was 262 Mbit/s and `.33 -> .77`
reverse receiver was 284 Mbit/s.

The clean reverse-first tunnel window still failed throughput acceptance at
`24.2/22.0 Mbit/s`, but it gave the missing discriminator. QUIC had no
loss/congestion delta or inherited congestion, and `downlink_flush:` showed
`send_slice_calls=13301`, `accepted_bytes=72320335`, `zero=0`, `errors=0`, and
`tun_flush_failures=0`. The bad signals were instead
`local_tun_egress_drop+local_downlink_backpressure`,
`pause_edges=8/resume_edges=8`, `max_pending_bytes=584779`, and
`tun_tx_dropped_delta=366`.

Reusable rule: stop treating `send_slice` failure or TUN flush syscall failure
as the leading clean reverse hypothesis. The data is accepted by smoltcp and
flush calls return success, but the local egress path still drops after those
accepted bytes are pushed toward TUN/qdisc. The next design branch should add
local egress feedback or pacing around accepted downlink bytes, not another
blind watermark/default tweak. Post-forward reverse samples in this run were
polluted by inherited QUIC congestion and should remain secondary evidence.

## 2026-07-04 — Knife14ap makes bounded downlink flush progress visible

Knife14ao default-watermark acceptance for commit `0d0765f` did not reproduce
the earlier high no-code A/B result. The `.27` bundle
`/tmp/mini_vpn/mvpn_knife14ao_default_bp512_128_flush256_tty_usclient_suite_20260704_144755.tar.gz`
showed the new defaults were active (`high=524288B`, `low=131072B`,
`flush=262144B`) and direct paths were healthy, but clean reverse-first P1 was
only `22.5/21.4 Mbit/s`. QUIC loss/congestion remained clean in that first
window; the residual signals were local TUN egress drop and downlink
backpressure. The failure is recorded in `.learnings/ERRORS.md` so future work
does not treat 512/128 KiB as accepted.

Knife14ap therefore avoided another blind tuning pass and added a smaller
observability step. `TcpDownlinkDiag` now counts non-empty flush attempts,
`can_send=false` attempts, budget-limited attempts, zero/error `send_slice`
events, total accepted bytes, largest single acceptance, and downlink-triggered
TUN flush calls/failures. The event loop emits an aggregate
`tcp-downlink-flush` line on the regular metrics tick, and the low-RTT probe
summarizes it as `downlink_flush:` in each iperf attribution block.

Verification:
- `cargo test --lib tcp_downlink_flush`
- `cargo test --lib client_tun`
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`

Stage review found no data-plane behavior change: the new counters are
incremented inside existing flush branches, the new aggregate line remains
behind `MINI_VPN_TCP_DIAG`, and the parser stays compatible with old logs by
printing zero-valued `downlink_flush:` fields when no new line exists. The next
VPS run should use clean reverse-first again and read `downlink_flush:` together
with TUN drops to decide whether the next code stage is egress scheduling,
smoltcp send-capacity feedback, or a different local queue strategy.

## 2026-07-04 — Knife14ao promotes 512/128 KiB downlink watermarks as defaults

Knife14ao converted the no-code A/B result into a narrow product default
change. Rust now defaults TCP downlink backpressure to 524288 bytes high and
131072 bytes low, while keeping the existing env overrides and invalid-value
repair behavior. The US-client suite defaults were aligned so a normal `.27`
run exercises the product default instead of silently injecting the old
2097120/524280 byte values.

The TDD loop was intentionally small: first add
`downlink_backpressure_defaults_match_knife14ao_ab`, watch it fail against the
old `2097120` high watermark, then change the defaults and re-run the focused
test. The suite self-test now also checks the script-level defaults and help
text so future acceptance reports do not drift away from the Rust default.

Verification:
- `cargo test --lib downlink_backpressure_defaults_match_knife14ao_ab`
- `cargo test --lib client_tun`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `git diff --check`

Stage review found no blocking issue: the parser still accepts explicit
deployment overrides, invalid high/low values still fall back or repair to a
valid pair, and the data path remains lossless because the change only pauses
remote reads sooner. Remaining risk is operational: the next VPS run must prove
the default is active and reaches a clean reverse-first probe window. If TUIC
startup/auth fails before app-level server logging again, treat that as a
startup boundary issue, not a throughput regression.

## 2026-07-04 — Knife14ao A/B proves lower downlink watermarks are the next code lever

The first no-code A/B for Knife14ao ran commit `246f8fa` from `.27` with
default TUN qlen 500, `MINI_VPN_TUIC_CC=bbr`, `MINI_VPN_TUIC_TCP_POOL=1`,
1MiB TCP socket buffers, `MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES=262144`,
`MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=524288`, and
`MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=131072`. Bundle:
`/tmp/mini_vpn/mvpn_knife14ao_bp512_128_flush256_defaultqlen_tty_usclient_suite_20260704_142413.tar.gz`.

The clean reverse-first P1 improved from the Knife14an 23.5 Mbit/s receiver
result to 157 Mbit/s receiver. QUIC stayed clean for that first reverse window
(`max_lost_bytes_delta=0`, `max_congestion_events_delta=0`,
`inherited_conns=none`), while downlink pending was bounded near the new high
watermark (`max_pending_bytes=589570` instead of roughly 2.15 MiB). This
falsifies the idea that the low reverse throughput is primarily iperf3,
sing-box, or the VPS path: the same binary and path materially improve when the
local downlink watermarks are lowered.

This A/B is not a final acceptance. The clean reverse still reported
`local_tun_egress_drop+local_downlink_backpressure` and
`tun_tx_dropped_delta=4394`, so lower watermarks reduce backlog size and improve
throughput but do not fully eliminate local TUN egress pressure. A stricter
256/64 KiB A/B was attempted next, but it failed during TUIC startup before
app-level server logging and is recorded in `.learnings/ERRORS.md`, not as a
throughput result.

Reusable rule: the next code change should be a narrow default-watermark update
or a stronger local-egress feedback mechanism, guarded by tests and a rerun
that reaches the clean reverse-first probe. Do not keep tuning iperf3,
sing-box, or QUIC congestion control until the local watermark/TUN-drop branch
has been accepted or falsified.

## 2026-07-04 — Knife14an VPS run falsifies per-flush-only downlink pacing

Commits `03f6bdc`, `739c8c0`, and `2261aed` were tested from `.27` with the
default TUN qlen 500, `MINI_VPN_TUIC_CC=bbr`, `MINI_VPN_TUIC_TCP_POOL=1`,
1MiB TCP socket buffers, and `MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES=262144`.
Bundle:
`/tmp/mini_vpn/mvpn_knife14an_flush256_defaultqlen_tty_usclient_suite_20260704_141008.tar.gz`.

The run confirms the implementation and harness wiring worked: the report and
startup log both show the 262144-byte flush budget, `.27 -> .77` direct reverse
was 263 Mbit/s receiver, `.33 -> .77` direct reverse was 283 Mbit/s receiver,
and `tun0` stayed at the default qlen 500. No stale TUIC TCP pool reconnects
appeared.

The product hypothesis did not pass acceptance. The clean reverse-first P1 was
24.7/23.5 Mbit/s and still attributed
`local_tun_egress_drop+local_downlink_backpressure`, with two pause/resume
cycles, `max_pending_bytes=2145742`, and `tun_tx_dropped_delta=594`. This is
not a meaningful improvement over the previous post-restart/downlink-pressure
branch, and the pending high-water stayed pinned near the 2MiB global
backpressure high watermark.

The standard forward P1 still reached 198/191 Mbit/s, but it produced
648 local-write-pressure events plus heavy QUIC loss/congestion
(`lost_bytes_delta=931106142`, `congestion_events_delta=134446`). The following
reverse was correctly labeled `inherited_quic_congestion`, validating the
Knife14am attribution fix. Later full-sweep samples were polluted by that
inherited QUIC state and should not drive a product change.

Reusable rule: bounding a single `send_slice` call is not enough when the
global downlink watermarks still allow roughly 2MiB pending bursts. The next
branch should not keep tuning the per-flush cap first; it should test lower
downlink high/low watermarks or a stronger local-egress/backpressure signal,
preferably as a no-code env A/B before changing defaults.

## 2026-07-04 — Knife14an bounds each downlink flush burst before VPS retest

Commit `03f6bdc` added a configurable TCP downlink flush budget instead of
changing sing-box, iperf3, QUIC congestion control, or the stale TUIC TCP pool
branch. The grounding signal was Knife14al's same-window bundle
`/tmp/mini_vpn/mvpn_knife14al_samewindow_defaultqlen2_usclient_suite_20260704_133130.tar.gz`:
reverse-first tunnel throughput was 13.7/12.4 Mbit/s with
`local_tun_egress_drop+local_downlink_backpressure`, while direct client/exit
paths and sing-box health were normal.

The implementation caps one `flush_downlink` pass to
`MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES`, defaulting to 256 KiB and accepting
4 KiB through 16 MiB. Bytes beyond the cap remain in `downlink_pending`; only
the bytes actually accepted by smoltcp are drained. The US-client suite now
records and passes the active budget so `.27` VPS reports show whether the
default or an A/B override was used.

Verification:
- `cargo test --lib client_tun`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`

Stage review found no blocking bug in the local change: dirty handles remain
dirty while pending downlink exists, parser bounds avoid accidental zero or
oversized budgets, and `MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES=1048576` remains
available for an approximate previous-behavior A/B. Full `cargo fmt --check`
still reports broad pre-existing formatting drift, so avoid a whole-repo
formatting churn inside throughput stages unless formatting is made a separate
task.

Reusable rule: when larger smoltcp TCP buffers improve one direction but local
TUN/qdisc drops appear in the reverse path, first bound the amount admitted into
smoltcp per local egress pass. Do not use a larger TUN queue or lower global
pending watermarks as the first product fix unless the per-flush burst bound is
falsified by a same-window VPS run.

## 2026-07-04 — Knife14am labels inherited QUIC congestion before pacing work

Commit `fec33fe` fixed the low-RTT attribution gap from Knife14al without
touching Rust data-plane behavior. The new parser rule emits
`inherited_quic_congestion` when a probe window starts with a low first cwnd and
large historical QUIC loss/congestion counters, even if the per-window deltas
stay zero.

The embedded self-test now reproduces the post-forward reverse shape from
`/tmp/mini_vpn/mvpn_knife14al_samewindow_defaultqlen2_usclient_suite_20260704_133130.tar.gz`:
11.5/10.7 Mbit/s reverse iperf, `cwnd=5808`,
`lost_bytes=500976100`, `congestion_events=93571`, and zero new
loss/congestion delta. Before the fix this sample reported
`no_pressure_signal`; after the fix it reports `inherited_quic_congestion` and
prints `max_start_lost_bytes`, `max_start_congestion_events`,
`min_start_cwnd`, and `inherited_conns` in the `quic:` line.

Verification:
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`

Reusable rule: before changing pacing or watermarks from a sequential
forward-then-reverse run, first check whether the reverse window inherited a
damaged QUIC state. A clean reverse-first/fresh-connection sample is still the
stronger signal for local downlink/TUN pressure.

## 2026-07-04 — Knife14al same-window run pins the current limiter to local downlink/TUN pressure

Commit `1ab58e3` fixed the suite cleanup self-match bug, then `.27` reran the
default-queue same-window diagnostic from `/home/ubuntu/mini_vpn/.env` with
`MINI_VPN_TUIC_CC=bbr`, `MINI_VPN_TUIC_TCP_POOL=1`, reverse-first P1 enabled,
and exit-to-target preflights required. Bundle:
`/tmp/mini_vpn/mvpn_knife14al_samewindow_defaultqlen2_usclient_suite_20260704_133130.tar.gz`.

The direct paths were healthy in the same run window: client-target reverse
preflight was 269 Mbit/s receiver, exit-target forward was 279 Mbit/s receiver,
and exit-target reverse was 295 Mbit/s receiver. Post-suite direct reverse
checks from both `.27` and `.33` also stayed around the high-200 Mbit/s range.
`.33` sing-box logs showed ordinary TUIC inbound/direct outbound opens for the
target; there was no same-window service failure signal.

The clean reverse-first tunnel probe collapsed to 13.7 Mbit/s sender and
12.4 Mbit/s receiver while reporting `downlink_backpressure` pause/resume
edges, `max_pending_bytes=2153280`, `tun_tx_dropped_delta=404`, and no QUIC
loss/congestion or flow-control blocked deltas. This is no longer an iperf3,
provider baseline, `.env`, stale TCP pool slot, or sing-box-health question. The
current product branch is client-local downlink/TUN egress pressure: the remote
iperf sender slows because TCP backpressure propagates from mini_vpn's local
delivery path back through the TUIC stream.

The standard forward P1 in the same session reached 204/191 Mbit/s but produced
761 local-write-pressure events, `tun_tx_dropped_delta=9`, and heavy QUIC
loss/congestion deltas. The following reverse P1 was therefore not a clean
reverse sample: it inherited a low-cwnd/lossy QUIC state and ran at
11.5/10.7 Mbit/s. Reusable rule: when diagnosing directional throughput, trust
reverse-first or fresh-connection samples more than a reverse probe after a
lossy forward burst, and teach attribution to flag preexisting low cwnd/large
absolute loss counters instead of only per-window deltas.

Next implementation branch: do not tune iperf3 or chase sing-box first. Design
bounded/paced local downlink flushing and/or better TUN egress backpressure
watermarks, with an attribution upgrade for inherited QUIC congestion state.

## 2026-07-04 — Knife14al suite cleanup must match the executable, not the command line

The first same-window default-queue diagnostic failed before tunnel probing:
`/tmp/mini_vpn/mvpn_knife14al_samewindow_defaultqlen_usclient_suite_20260704_132521.tar.gz`.
The old cleanup used `pgrep/pkill -f '[m]ini_vpn.*client-tun'`. When the suite
was launched from `/home/ubuntu/mini_vpn`, the current shell command line
contained both `mini_vpn` and `usclient-tunnel`, so the cleanup matched and
killed its own SSH/shell process.

Commit `1ab58e3` replaced the broad regex with a `ps` parser that matches only
processes whose `comm` is `mini_vpn` and whose argv contains an independent
`client-tun` token. The script now has `--self-test` cases for the real
`mini_vpn ... client-tun` process and for false positives such as the suite
script path, sudo/env wrapper commands before exec, `pgrep -af`, and helper
probes. Verification passed locally and on `.27`:

- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`

Reusable rule: destructive process cleanup in VPS harnesses must match the
actual executable identity plus argv tokens. Never kill by a broad `-f` regex
that can match the suite path or the SSH command used to launch it.

## 2026-07-04 — Knife14ak qlen A/B shifts the live branch back to reverse sender

Commit `b31b234` added an opt-in `TUN_TX_QUEUE_LEN` suite knob and documented the
Knife14ak A/B plan. Local verification passed:

- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --help | rg 'TUN_TX_QUEUE_LEN|MTU=1200'`
- `git diff --check`

The scoped `.27` VPS run with `TUN_TX_QUEUE_LEN=5000`, `MINI_VPN_TUIC_CC=bbr`,
`MINI_VPN_TUIC_TCP_POOL=1`, and reverse-first P1 produced bundle
`/tmp/mini_vpn/mvpn_knife14ak_tunqlen5000_usclient_suite_20260704_131103.tar.gz`.
The suite proved the harness knob works: `tun0` reported `qdisc fq_codel` and
`qlen 5000`, and the report recorded `tun_tx_queue_len_requested=5000` plus
`tun_tx_queue_len_actual=5000`.

The A/B result did not validate TUN queue depth as the current primary limiter.
Direct preflights were healthy (`.27 -> .77` reverse receiver 274 Mbit/s,
`.33 -> .77` reverse receiver 298 Mbit/s), but tunnel reverse P1 collapsed to
0.314 Mbit/s sender and 0.057 Mbit/s receiver. Attribution was
`reverse_sender_backpressured`: local write pressure, global RX pressure,
downlink backpressure, TUN drops, QUIC loss/congestion, and QUIC blocked-frame
deltas were all zero. The close log showed `pending=0` and only 329,254 bytes
read from the remote side.

Reusable rule: a larger TUN queue can be a useful A/B knob, but if reverse iperf
sender throughput is also low and mini_vpn local pressure is clean, stop tuning
TUN/downlink. The next branch needs paired tunnel/direct reverse sender
diagnostics and exit/server-side evidence around the exact probe window before
changing product pacing or watermarks.

## 2026-07-04 — Knife14aj keeps closed-pending reap and adds TUN drop attribution

Grounding against smoltcp `0.10.0` changed the stage design: `TcpSocket::can_send()`
is `may_send() && !tx_buffer.is_full()`, and `may_send()` is true only in
`Established` and `CloseWait`. Therefore `TcpState=Closed active=false
can_send=false` pending downlink is terminal tail, not temporary local tx-buffer
pressure. Delaying `dead_slot_reap` for that state would not deliver bytes and
risks reopening the stale-flow regression that knife14u fixed.

The safer stage move was to keep the existing lifecycle guard covered by
`reap_predicate` tests and make the suspected pressure source observable. The
low-RTT probe now samples `ip -s link show <tun>` before and after each iperf
probe, prints `tun_rx_dropped_delta` and `tun_tx_dropped_delta`, and labels
positive TX-drop deltas as `local_tun_egress_drop`. The parent suite passes the
discovered `TUN_IF` into the probe and includes `tun_drops:` in the report
summary grep.

Verification:
- `cargo test --lib reap_predicate`
- `cargo test --lib client_tun`
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `git diff --check`

Reusable rule: when a close diagnostic says `tcp_state=Closed active=false
can_send=false pending>0`, treat it as terminal cleanup unless a test proves the
socket can send again. For reverse throughput, inspect TUN/qdisc dropped deltas
before changing the pending reap predicate.

VPS follow-up on `.27` at `6b45d29` produced bundle
`/tmp/mini_vpn/mvpn_knife14aj_tun_drop_env_usclient_suite_20260704_112101.tar.gz`.
The report confirmed the new attribution survives the real suite:

- reverse-first P1: sender 183 Mbit/s, receiver 182 Mbit/s,
  `tun_tx_dropped_delta=3209`, attribution
  `local_tun_egress_drop+local_downlink_backpressure`;
- standard forward P1: sender 177 Mbit/s, receiver 164 Mbit/s,
  `tun_tx_dropped_delta=17`, attribution
  `quic_loss_congestion+local_write_pressure+local_tun_egress_drop`;
- standard reverse P1: sender 124 Mbit/s, receiver 123 Mbit/s,
  `tun_tx_dropped_delta=16661`, attribution
  `local_tun_egress_drop+local_downlink_backpressure`.

The scoped run improved sharply versus the prior post-restart 23.6 Mbit/s
receiver result, but it also proved the local TUN/qdisc drop signal is real.
The next throughput branch should treat TUN egress drops and burst pacing as
first-class, not as a side note hidden behind pending-tail reap logs.

## 2026-07-04 — Knife14ai restart gate isolates next bottleneck to downlink pending

After the pool=4 startup-only smoke reproduced `tuic auth finish: sending
stopped by peer: error 0`, `.33` sing-box was restarted. The service returned
active with `ActiveEnterTimestamp=Sat 2026-07-04 09:11:13 CST`. Re-running the
same pool=4 startup-only smoke immediately passed: all four TUIC TCP pool
connections authenticated, the log printed `TUIC TCP connection pool=4`, and
the UDP relay was ready.

The post-restart scoped suite at commit `34d5cb7` completed with bundle
`/tmp/mini_vpn/mvpn_knife14ai_pool4_after_singbox_restart_usclient_suite_20260704_091224.tar.gz`.
It ran the reverse-first P1 probe and then skipped standard P1/full sweep
because the quiet gate did not observe a fresh zero-active metrics tick. Direct
preflights stayed healthy: client-target and exit-target checks were all roughly
277-284 Mbit/s receiver.

The tunnel reverse-first P1 improved to 88.9 MBytes / 24.8 Mbit/s sender and
84.2 MBytes / 23.6 Mbit/s receiver. The attribution was
`local_downlink_backpressure`: two pause/resume edges, max pending 2,149,256
bytes, no local write pressure, no global_rx pressure, no late remote bytes, and
no QUIC loss/congestion/flow-control blocked deltas. The close tail again showed
`dead_slot_reap` on handle 1 with `tcp_state=Closed active=false can_send=false
can_recv=false` and about 2.1 MiB pending, which is not the old stale pool slot
branch.

Reusable rule: once TUIC auth is restored by restarting sing-box, stop treating
pool=4 startup as the throughput bottleneck. The next code design should focus
on how downlink pending accumulates and is reaped for inactive/closed TCP state,
and on why TUN TX drops/queueing create bursty reverse throughput despite a
clean QUIC path.

## 2026-07-03 — Knife14ai pool A/B separates iperf health from TUIC pool startup

`34d5cb7` was tested from `.27` with two focused acceptance bundles:
`/tmp/mini_vpn/mvpn_knife14ai_pool1_usclient_suite_20260703_234149.tar.gz`
and
`/tmp/mini_vpn/mvpn_knife14ai_pool4_usclient_suite_20260703_234304.tar.gz`.

The pool=1 control completed. Direct preflights were healthy on all relevant
legs, including `.27 -> .77`, `.77 -> .27`, `.33 -> .77`, and `.77 -> .33`
around 275-286 Mbit/s. The tunnel reverse-first P1 improved from the prior
Kbit/s branch to 66.4 MBytes / 18.5 Mbit/s sender and 62.8 MBytes /
17.5 Mbit/s receiver, but the attribution changed to
`local_downlink_backpressure`: pending reached 2,118,593 bytes, paused once,
resumed once, and QUIC showed no loss, congestion, or flow-control blocked
delta. This means the earlier `reverse_sender_backpressured` label was useful:
it distinguishes the prior server-side sender stall from this new local
downlink-backpressure branch.

The pool=4 experiment did not reach tunnel iperf. It failed during TUIC startup
with `tuic auth finish: sending stopped by peer: error 0`. `.33` sing-box was
still systemd-active, and `.77` iperf3 was active; `.77` journal showed the
pool=4 run only performed direct/exit-target preflights and did not receive a
tunnel iperf connection. The `.33` sing-box log window showed the preceding
pool=1 TUIC connects and a stream cancel at 23:42:38 CST, but no later pool=4
TUIC inbound line at the 23:43 startup failure.

Reusable rule: do not treat low reverse throughput as an iperf3 problem when
direct client-target and exit-target preflights are healthy. For pool A/B, add
a startup-only health gate for `MINI_VPN_TUIC_TCP_POOL>1` before comparing
throughput, and keep sing-box/TUIC service state as a first-class branch even
when systemd says the service is active.

## 2026-07-03 — Knife14ai labels reverse sender backpressure in reports

Knife14ah proved a sharper failure branch: reverse receiver throughput was low,
but the target-side iperf sender was also low, while client-side relay,
downlink, and QUIC diagnostics did not show pressure/loss. Knife14ai makes that
branch visible in the low-RTT report instead of requiring manual `.77` journal
correlation.

Changes:

- `scripts/knife14b-lowrtt-probe.sh` now parses and prints
  `iperf_sender_mbps` alongside `iperf_receiver_mbps`.
- TCP reverse probes can emit `reverse_sender_backpressured` when sender and
  receiver throughput are both low and stronger local/client pressure labels
  are absent.
- The attribution path carries a `probe_kind`, so UDP reverse probes do not get
  mislabeled by the TCP sender-backpressure heuristic.
- `scripts/knife14b-usclient-tunnel-suite.sh` includes `iperf_sender_mbps` in
  the parent suite probe summary grep.
- Added
  `docs/tech/2026-07-03-knife14ai-reverse-sender-backpressure-spec.md` and
  `docs/tech/2026-07-03-knife14ai-reverse-sender-backpressure-plan.md`.

Verification passed:

- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `git diff --check`

Stage review found no Rust data-plane changes and no new secret exposure. The
main review catch was that `Reverse mode` also appears in UDP iperf output; the
fix was to gate `reverse_sender_backpressured` behind `probe_kind=tcp` and add
a negative self-test.

Reusable rule: when adding attribution labels to shared probe code, pass the
probe kind explicitly instead of inferring all semantics from iperf text. Iperf
phrases such as `Reverse mode` are shared across TCP and UDP.

## 2026-07-03 — Knife14ah VPS run moved reverse failure to server-side backpressure

`4282682` was tested from `.27` with
`/tmp/mini_vpn/mvpn_knife14ah_usclient_suite_20260703_231523.tar.gz`.
The run used `.27`'s project-local `.evn` silently, `MINI_VPN_TUIC_CC=bbr`,
`MINI_VPN_TUIC_TCP_POOL=4`, and a reverse-first P1 probe.

The direct host/path preflights were healthy: `.27 -> .77` 257 Mbit/s
receiver, `.77 -> .27` 264 Mbit/s receiver, `.33 -> .77` 283 Mbit/s receiver,
and `.77 -> .33` 269 Mbit/s receiver. The tunnel reverse-first P1 was still
Kbit/s-scale: iperf sender reported 3.00 MBytes / 838 Kbit/s with 2 retries,
while the receiver reported 88.2 KBytes / 24.1 Kbit/s.

Knife14ah's new relay counters split the previous late-remote branch further.
During the iperf window, handle 1 had already read 90,312 remote bytes before
local `Finish`, with no local write pressure, no global_rx pressure, no
downlink backpressure, no QUIC loss/congestion, and no QUIC blocked-frame
delta. After local `Finish`, the same relay read another 96,624 bytes and
closed by `half_closed_idle_timeout`; `tcp-handle-close` showed `pending=0`,
`send_slice_accepted=186936`, and zero TUN flush failures.

The decisive new discriminator came from `.77` iperf3 journal for the same
time window: the target-side sender only produced 3.00 MBytes in the first
second, then 29 seconds of 0 bytes with cwnd about 432 KBytes. `.33`
sing-box logs showed both TUIC inbound/direct outbound opens to `.77:5201` at
23:15:55 CST, but no close/error detail in the INFO log. This means the next
question is no longer whether the client local loop dropped received bytes; it
is why the target TCP sender is rapidly backpressured when the path goes
through sing-box/TUIC.

Reusable rule: for reverse no-pressure runs, always correlate three views
before tuning client buffers: client relay accepted bytes, target iperf sender
timeline, and exit/sing-box connection logs. If the target sender itself stops
after a tiny burst while direct exit-target reverse is healthy, inspect
server-side TUIC stream flow-control/write pressure or protocol semantics
before changing local downlink watermarks.

## 2026-07-03 — Knife14ah labels late remote bytes after local Finish

Knife14ag's reverse-first blocker was not stale pool, local pressure, downlink
backpressure, or QUIC loss. The useful discriminator was late timing: remote
bytes appeared only after local `Finish`. Knife14ah turns that manual
correlation into structured diagnostics.

Changes:

- `RelayTaskDiag` now counts `remote_after_local_finish_bytes` and reads after
  local `Finish`, and emits those counters in `tcp-relay-live` and
  `tcp-relay-close` lines under `MINI_VPN_TCP_DIAG=1`.
- `scripts/knife14b-lowrtt-probe.sh` now summarizes
  `relay_late_remote` and emits `late_remote_after_local_finish` attribution.
- The parser supports both new explicit post-finish counters and old logs by
  diffing `remote_to_global_rx_bytes` before/after `tcp-relay-write-half-closed`.
- The parent US-client suite summary now preserves relay live/half-close/close
  lines plus `relay_late_remote`.

Verification passed:

- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `cargo test relay_`
- `cargo test --lib client_tun`
- `git diff --check`
- parser smoke against
  `/tmp/mini_vpn/mvpn_knife14ag_usclient_suite_20260703_225103.tar.gz`, which
  now summarizes `relay_late_remote: post_finish_bytes=43772` and attribution
  `late_remote_after_local_finish`.

Full `cargo test` was attempted and passed 251 tests, but failed the two
`quic::tests::client_endpoint_binds*` local endpoint-bind tests while
`src/quic.rs` had no diff. Record this as environment residual risk, not as
evidence against the late-remote diagnostics.

Reusable rule: when reverse-first has 0 receiver bytes plus no pressure/loss
signals, preserve and summarize half-close lifecycle lines before changing data
plane behavior. If bytes only arrive after local `Finish`, the next diagnostic
belongs at TUIC/sing-box/target stream timing, not at local queue thresholds.

## 2026-07-03 — Knife14ag VPS run split reverse silence from pressure/loss

`fbe030c` was tested from `.27` with
`/tmp/mini_vpn/mvpn_knife14ag_usclient_suite_20260703_225103.tar.gz` after
loading `.27`'s project-local `.evn` file and running the suite under
`sudo -n -E env ... bash`.

The host/path preflights were healthy: `.27 -> .77` 291 Mbit/s receiver,
`.77 -> .27` 273 Mbit/s receiver, `.33 -> .77` 297 Mbit/s receiver, and
`.77 -> .33` 282 Mbit/s receiver. The tunnel reverse-first P1 still failed:
iperf sender reported 2.62 MBytes / 733 Kbit/s, while the receiver reported
0 bytes. The new attribution summary reported `no_pressure_signal`:
no local write pressure, no global_rx pressure, no downlink backpressure,
no QUIC loss/congestion delta, and no flow-control blocked-frame delta.

The decisive discriminator is timing: during the 30s iperf window the data
relay had `remote_to_global_rx_bytes=0`, but after iperf had already finished
and the local side half-closed, the same relay later read 43,772 bytes and then
closed by `half_closed_idle_timeout`. This is not the stale pool slot branch,
and it is not enough evidence for tuning congestion control, socket buffers, or
downlink watermarks.

Reusable rule: when reverse-first yields 0 receiver bytes plus
`no_pressure_signal`, inspect server/TUIC stream lifecycle and late-arriving
remote bytes before changing local pressure/backpressure settings. The next
diagnostic should make control stream vs data stream and server-side target
connect/write timing visible.

## 2026-07-03 — Knife14ag: summarize pressure per iperf window before tuning

After `knife14af2` proved stale TCP pool reconnect acceptance, the next
throughput branch split by probe segment: forward/uplink-heavy runs showed
`tcp-local-write-pressure` plus QUIC loss/congestion, while reverse-heavy runs
showed `tcp-downlink-backpressure` with no QUIC flow-control blocked frames.

Knife14ag adds per-iperf attribution summaries to the low-RTT probe. Each run
now reports receiver Mbps, selected TCP pool connections, stale reconnects,
local write pressure, global_rx pressure, downlink backlog, QUIC loss/congestion
deltas, blocked-frame deltas, and a conservative attribution label. The parent
US-client suite surfaces these summary lines in its probe summaries.

Local verification passed:

- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- parser smoke against the extracted `knife14af2` probe/report files

Reusable rule: for throughput variance, do not tune CC, pools, buffers, or
backpressure thresholds until each iperf window has a structured attribution
summary. Treat `tuic-open-tcp` as a stream-open event, not proof of a new QUIC
connection; QUIC loss/congestion attribution should use the first stats sample
inside the window as its baseline to avoid blaming old cumulative counters on a
new iperf run.

## 2026-07-03 — Knife14af2: stale TCP pool reconnect acceptance passed

`7c683b0` was tested from `.27` with
`/tmp/mini_vpn/mvpn_knife14af2_usclient_suite_20260703_214744.tar.gz` after
restarting `.33` sing-box. The suite exited successfully. The final full reverse
P1 no longer failed with `conn=1 closed=TimedOut` or 0-byte iperf; it refreshed
the stale extra pool slot first:
`tuic-tcp-pool-reconnect conn=1 reason=stale_tcp_pool_slot`.

Observed throughput after the fix:

- reverse-first P1: 30.9 Mbit/s receiver
- standalone forward P1: 2.93 Mbit/s receiver
- standalone reverse P1: 26.7 Mbit/s receiver
- full forward P1: 191 Mbit/s receiver
- full reverse P1: 23.1 Mbit/s receiver

The stale-slot acceptance is satisfied, but the next bottleneck is now a
different branch: throughput variance with local write pressure and QUIC
loss/congestion on one TCP pool connection. Keep the next stage focused on
pressure/loss rather than reopening the stale pool diagnosis.

## 2026-07-03 — Knife14ae3: reverse throughput recovered; stale TCP pool slot remains

After fixing Exit SSH and restarting `.33` sing-box, `ebd3567` was tested with
`/tmp/mini_vpn/mvpn_knife14ae3_usclient_suite_20260703_212412.tar.gz`. The new
Exit-to-Target preflight worked and proved all direct legs were healthy:
`.27 -> .77` 283 Mbit/s receiver, `.77 -> .27` 280 Mbit/s receiver,
`.33 -> .77` 281 Mbit/s receiver, and `.77 -> .33` 275 Mbit/s receiver.

Restarting sing-box changed TUIC startup from `tuic auth finish: sending stopped
by peer` to success, so an active systemd service is not sufficient proof that
TUIC auth is healthy. For future acceptance runs, when startup fails at TUIC auth
while config matches, restart or deeper-check sing-box before changing data-plane
code.

The tunnel itself improved substantially: reverse-first P1 reached 187 Mbit/s
receiver, standalone forward/reverse P1 both reached 155 Mbit/s receiver, and
full forward P1 reached 187 Mbit/s receiver. The remaining failure is narrower:
the final full reverse selected a TCP pool slot that reported `closed=TimedOut`
and transferred 0 bytes. Follow-up: refresh stale TCP pool slots before opening
new TUIC Connect streams, but only when the selected non-primary slot has no
active/opening relay.

## 2026-07-03 — Knife14ae: Exit SSH preflight needs suite-local host-key state

The first `knife14ae` run against `70e59a0` reached the new Exit-to-Target
preflight and failed before tunnel testing:
`/tmp/mini_vpn/mvpn_knife14ae_usclient_suite_20260703_211833.tar.gz`. The
direct Client↔Target checks were healthy, but SSH from `.27` to `.33` failed
with `Host key verification failed` because the suite was running under root via
`sudo -E`.

Follow-up: make the harness pass an explicit Exit SSH host-key policy and a
suite-local known-hosts file. Reusable rule: noninteractive VPS harnesses must
not depend on whichever user's `~/.ssh/known_hosts` happens to exist.

## 2026-07-03 — Knife14ad: reverse now has bytes; missing baseline is Exit↔Target

`33659c9` was tested with
`/tmp/mini_vpn/mvpn_knife14ad_usclient_suite_20260703_183503.tar.gz`. The suite
completed and the new stream-level diagnostics worked. Client-to-target direct
baselines were healthy (`.27 -> .77` 276 Mbit/s receiver, `.77 -> .27` 268
Mbit/s receiver). Tunnel fresh reverse-first improved from the earlier Kbit/s
failure to 22.4 Mbit/s receiver, but it is still far below the direct reverse
baseline. Later reverse P1 samples stayed around 15.8-20.8 Mbit/s, while full
forward P1 reached 191 Mbit/s receiver.

The new `tcp-relay-live` lines prove reverse is no longer "server sends
nothing": the data flow accumulated tens of MB of
`remote_to_global_rx_bytes`, `tcp-global-rx-pressure` did not fire, and
client loop CPU stayed low. The remaining attribution gap is the actual tunnel
reverse pre-exit path: `.77 -> .33`. The suite had only proven `.77 -> .27`,
which is not the same route.

Knife14ad follow-up adds an optional Exit-to-Target SSH preflight to
`scripts/knife14b-usclient-tunnel-suite.sh`. Reusable rule: for a TUIC exit
proxy, direct client-target baselines are necessary but not sufficient. Reverse
tunnel attribution also needs `.33 <-> .77` direct iperf from the exit host
before changing mini_vpn data-plane code.

Stage code-review found and fixed one harness usability edge case: only require
the local `ssh` command when `CHECK_VPS_SERVICES=1` and the Exit-to-Target
preflight is enabled, because disabled service preflight will not run the SSH
check.

## 2026-07-03 — Knife14ac: fresh reverse is still dead; add stream-level diagnostics

`74d3d0a` was tested with
`/tmp/mini_vpn/mvpn_knife14ac_usclient_suite_20260703_180352.tar.gz`. The run
used `RUN_REVERSE_FIRST_P1=1`, `MINI_VPN_TUIC_TCP_POOL=4`, BBR, and 1MiB
smoltcp TCP socket buffers. Direct baselines were healthy (`.77` forward
receiver 282 Mbit/s, reverse receiver 266 Mbit/s), but the fresh tunnel reverse
P1 receiver was only 62.5 Kbit/s.

This rejects two earlier sufficient-cause branches: the 64KiB local socket
window is no longer enough to explain the failure, and forward traffic on the
same QUIC connection is not required to poison reverse because reverse-first
failed before the normal sweep. Client QUIC connection stats showed no loss,
blocked frames, or loop CPU pressure during the reverse probe. The relay for the
data flow only reached `tcp-relay-write-half-closed ... reason=local_finish` and
then remained active without a close line in the captured window.

Knife14ad therefore adds per-TUIC TCP open diagnostics and relay live counters
behind `MINI_VPN_TCP_DIAG=1`, and teaches the low-RTT probe to include those
lines in markdown summaries. Reusable rule: when reverse-first is nearly silent
while client loop/QUIC stats are idle, stop tuning buffers/CC/pool and collect
per-stream evidence that distinguishes "client received no remote bytes" from
".33/sing-box/exit did not send them."

## 2026-07-03 — Knife14ab: 1MiB TCP socket buffers helped forward but not reverse

`3af4f7c` was tested with
`/tmp/mini_vpn/mvpn_knife14ab_usclient_suite_20260703_172934.tar.gz`. The user
message mentioned `7621bc6`, but the report's `repo_commit` and git snapshot
both show `3af4f7c`.

The socket-buffer change was applied correctly: the startup log printed
`TCP socket buffers: rx=1048576B tx=1048576B`. Direct baselines were healthy
again (`.77` forward receiver 276 Mbit/s, reverse receiver 281 Mbit/s). Tunnel
forward improved materially: standalone P1 reached 190 Mbit/s, and full
P2/P4/P8 reached 192/150/163 Mbit/s receiver.

Reverse did not recover: standalone P1 was 20.7 Mbit/s, and full P1/P2/P4/P8
were 17.5/26.2/32.2/26.5 Mbit/s. `send_slice_zero` and `send_slice_errors`
remained zero, so the next attribution branch is not "keep increasing smoltcp
socket buffer." The current probe always runs forward before reverse on the same
client process/TUIC connection, so it cannot distinguish intrinsic `.33 -> .27`
reverse weakness from same-connection congestion-state pollution after forward
pressure.

Knife14ac adds a reverse-only P1 probe order and a parent-suite
`RUN_REVERSE_FIRST_P1` gate. Reusable rule: once enlarged local TCP socket
buffers are confirmed but reverse remains bursty, run a fresh reverse-first
probe and a multi-connection acceptance run before asking for server logs or
changing protocol internals.

## 2026-07-03 — Knife14aa: direct reverse is healthy; test TCP socket buffer BDP next

`7621bc6` was tested with
`/tmp/mini_vpn/mvpn_knife14aa_usclient_suite_20260703_162932.tar.gz`. The new
direct reverse preflight worked and proved `.77 -> client` is not the primary
reverse blocker: direct `iperf3 -R` reached 299 Mbit/s receiver, while tunnel
reverse was still only 8.25 Mbit/s standalone P1 and 18-27 Mbit/s in the full
sweep.

The strongest live signal moved to the client downlink path. The log showed
`tcp-downlink-backpressure` transitions, high `remote_to_global_rx_bytes`, and
pending downlink at close; QUIC `tx_blocked` stayed zero. The smoltcp TCP socket
tx buffer was still fixed at 65,535 bytes, which can cap the local
mini_vpn -> app receive leg and push extra reverse bytes into `downlink_pending`.

Knife14ab makes smoltcp TCP listener rx/tx buffer sizes configurable and teaches
the US-client suite to run high-throughput acceptance with 1MiB rx/tx buffers.
Defaults remain unchanged until VPS evidence proves the throughput/memory
tradeoff.

Reusable rule: once direct `iperf3 -R` is healthy but tunnel reverse remains low
and `downlink_pending`/backpressure grows, inspect the local smoltcp send window
before returning to VPS service state, CC, or QUIC flow-control tuning.

## 2026-07-03 — Knife14z: BBR improves forward, reverse still needs direct -R attribution

`1b6f2d8` was tested with
`/tmp/mvpn_knife14z_usclient_suite_20260703_154429.tar.gz`. The CC sweep harness
worked: Cubic and BBR ran as fresh child suites with separate reports, bundles,
and `mvpn_accept_<cc>_<timestamp>.log` files.

The result split the problem. BBR materially improved forward throughput on this
US-client path: standalone P1 reached 192 Mbit/s, full P2 reached 192 Mbit/s,
and full P4 reached 150 Mbit/s, while Cubic stayed around 1-27 Mbit/s. That
confirms Cubic/path congestion is a real forward bottleneck here. But BBR is not
a complete acceptance fix: full P8 still collapsed to 18.6 Mbit/s, and reverse
remained Kbit/s-scale for both CC variants.

The existing suite only had a direct forward `.77:5201` iperf preflight. Add a
direct `iperf3 -R` baseline before routing `.77` into the TUN so future reverse
failures can be attributed to target/path/service versus tunnel data-plane
logic.

Reusable rule: do not promote BBR to a global default from forward-only wins.
For mixed TCP/UDP VPN goals, keep CC/profile decisions explicit until forward,
reverse, P8, and UDP acceptance all have matching evidence.

## 2026-07-03 — Knife14z: run congestion-control A/B after knife14y stats

`f7deb9b` was tested with
`/tmp/mini_vpn/mvpn_knife14y_usclient_suite_20260703_143704.tar.gz`. The run
completed the full US-client tunnel suite. Service preflight was healthy:
`.33` was reachable, `.77:5201` direct iperf passed, cargo/build worked, and
TUIC startup printed the expected QUIC windows and stats.

Knife14y resolved the attribution question. During the remaining stalls,
`tx_blocked(data|stream)` stayed zero while QUIC loss/congestion climbed and
`cwnd` stayed tiny, for example `lost=75744/2058977`,
`lost_bytes=95844092`, `congestion_events=48725`, `cwnd=2904`. That makes the
current blocker a congestion/path acceptance question, not another client
flow-control or relay lifecycle guess. P1 was improved relative to earlier
Kbit/s runs, but the Cubic full sweep still showed zero-rate windows.

Knife14z adds `CC_SWEEP` to `scripts/knife14b-usclient-tunnel-suite.sh`.
`CC_SWEEP="cubic bbr"` runs each CC through a fresh child suite with isolated
client logs and probe artifacts, while the old single-run behavior remains
unchanged when `CC_SWEEP` is empty. The low-RTT probe summary now also includes
`TUIC QUIC stats`, `tcp-local-write-pressure`, and
`tcp-downlink-backpressure` lines.

Reusable rule: when QUIC stats show no flow-control blocked frames but high
loss/congestion and tiny cwnd, do a same-script Cubic/BBR A/B acceptance run
before changing relay lifecycle, buffers, or product defaults.

## 2026-07-03 — Knife14x: client windows applied, remaining stall needs QUIC stats

`ee2e83a` was tested with
`/tmp/mvpn_knife14x_usclient_suite_20260703_120013.tar.gz`. The suite reached
the full tunnel path and completed. Cargo discovery/build worked, `.33`
reachability and `.77:5201` direct iperf service passed, and the TUIC startup
log confirmed the explicit client QUIC windows:
`bidi=512 uni=4096 stream_rx=8388608B conn_rx=33554432B send=33554432B`.

The result did not recover throughput. Direct `.77` iperf receiver was about
285 Mbit/s, but tunnel forward stayed around Kbit/s to low Mbit/s and reverse
remained Kbit/s-scale or timed out. `tcp-local-write-pressure` remained active:
104 pressure lines, average wait about 5.1s, max wait about 37.8s, while
`global_rx_pressure_events` stayed zero.

Knife14y should not keep tuning client windows or relay batching. Add QUIC
connection stats (`tx_blocked`, `rx_blocked`, `cwnd`, loss, congestion events,
and window-update frame counts) so the next acceptance bundle can distinguish
peer/server flow-control from congestion/path loss.

Reusable rule: once explicit client windows are logged and long write waits
remain, treat the next step as attribution, not another throughput tweak. If
`tx_blocked(data|stream)` climbs with low loss, inspect sing-box/server
flow-control. If loss/congestion climbs or cwnd stays tiny, test congestion
control or the VPS path.

## 2026-07-03 — Knife14w2: writer coalescing exposed real QUIC stream write pressure

`95bfe47` was tested with
`/tmp/mvpn_knife14w2_usclient_suite_20260703_112033.tar.gz`. The suite reached
the full tunnel path: cargo was found at `/home/ubuntu/.cargo/bin/cargo`, `.33`
reachability and `.77:5201` direct iperf preflight passed, and the final suite
status was `COMPLETED`.

The data-plane result is still below target. Direct `.77` preflight receiver was
about 281 Mbit/s, but tunnel forward P1 was 2.83 Mbit/s receiver and reverse P1
was 314 Kbit/s receiver; later reverse P2/P4/P8 timed out. The relay writer
coalescing stage did work mechanically: forward close logs showed 64KiB-class
payloads and far fewer writer calls than the earlier MSS-sized pattern. The new
signal is `tcp-local-write-pressure`: 110 pressure lines appeared, with some
`local_write_wait_max_us` values in the multi-second to tens-of-seconds range.

Knife14x makes the client QUIC transport windows explicit: raise stream receive,
connection receive, send window, and bidi-stream allowance to VPN-sized values,
and print them at TUIC startup. This does not prove the server/path window is
fixed; it removes the client's default transport window as an ambiguous cause
and gives the next log a clearer attribution point.

Reusable rule: once writer batches are 64KiB-class and `global_rx_pressure` is
zero, treat long local writer waits as QUIC stream/connection pressure. First
make local transport windows explicit and observable; if pressure persists, test
congestion control or server-side flow-control rather than more relay lifecycle
changes.

## 2026-07-03 — Knife14w VPS bundle did not test the data plane

`1144fc2` was submitted with
`/tmp/mvpn_knife14w_usclient_suite_20260703_111144.tar.gz`. The archive
contained only the markdown report and no accept log or tunnel artifacts. The
suite ran as root, passed basic env/CA checks, then failed in the build stage
because `BUILD_RELEASE=1` could not find `cargo` in root's `PATH`.

This means the relay-writer coalescing change has not yet been exercised on the
VPS path. The next run must first reach the `.33/.77` preflight and then the
tunnel/iperf sections before its result can say anything about `1144fc2` data
plane behavior.

Knife14w suite follow-up makes cargo discovery explicit: use executable `CARGO`
when provided, then `PATH`, then common rustup paths such as
`/home/ubuntu/.cargo/bin/cargo` and `/root/.cargo/bin/cargo`. The report should
record the chosen path and cargo version.

Reusable rule: when a bundle contains only the suite report, classify it as a
harness/environment result first. Do not infer data-plane success or failure
until client logs and iperf sections exist.

## 2026-07-03 — Knife14w: forward bottleneck moved to relay writer small writes

`c90471c` was tested with
`/tmp/mvpn_knife14v_usclient_suite_20260703_103350.tar.gz`. The run validated
the knife14v reverse fix: standalone reverse P1 reached roughly 181 Mbit/s and
the full reverse P1/P2/P4/P8 sweep completed around 153-183 Mbit/s receiver.
Remaining `dead_slot_reap pending>0` lines were on `tcp_state=Closed
active=false can_send=false can_recv=false`, consistent with undeliverable local
tails rather than useful downlink loss.

The new blocker is forward. Standalone forward P1 was only 2.37 Mbit/s receiver,
and full forward P1/P2/P4/P8 stayed around 1.15-2.33 Mbit/s receiver with long
zero-rate windows. The accept log for forward P1 showed about 10.5 MB written
upstream across 8529 local writer calls, i.e. MSS-sized writes, with no
`remote_write_timeout`.

Knife14w therefore coalesces already queued `RelayCommand::Data` messages inside
`run_relay_writer` before one `write_all`, preserving the existing bounded mpsc
channel as the backpressure boundary. `RelayCommand::Finish` is consumed only as
a `finish_after` flag, so queued data is written first and the upstream write
half is then shut down in FIFO order. Relay close logs now include local writer
wait counters (`local_write_wait_max_us`, `local_write_pressure_events`) to
separate true upstream write backpressure from global_rx/downlink pressure.

Local acceptance passed: `cargo test --lib client_tun`, full `cargo test`,
`cargo clippy --all-targets -- -D warnings`, and `git diff --check`.

Reusable rule: after fixing event-loop read batching, inspect writer-side write
granularity before blaming QUIC, VPS config, or congestion control. A VPN relay
can still behave like one-MSS-per-await even when the main loop drains batches
correctly.

## 2026-07-02 — Knife14v: local Finish should wait for reverse traffic to go quiet

`a57873a` was tested with
`/tmp/mvpn_knife14c_usclient_suite_20260702_223847.tar.gz`. The filename kept
the default `knife14c` tag, but the report recorded `repo_commit: a57873a`.
Knife14u's core change worked: the remaining `dead_slot_reap pending>0` carried
`tcp_state=Closed active=false can_send=false can_recv=false`, so it was an
undeliverable local tail rather than a protected useful buffer.

The run exposed the next independent reverse-path problem. Forward P1 recovered
to 13.6 Mbit/s receiver, but reverse P1 stayed at Kbit/s scale and reverse
P2/P4/P8 timed out or transferred zero. The accept log showed
`tcp-relay-write-half-closed ... reason=local_finish` followed by tiny
`remote_to_global_rx_bytes` and `half_closed_idle_timeout`. Immediate propagation
of local `CloseWait` into upstream write-half shutdown appears to interrupt
reverse traffic on this TUIC/exit path.

Knife14v defers `RelayCommand::Finish`: first `CloseWait` records a pending
local finish, remote-to-local payload refreshes the defer deadline, and Finish is
sent only after remote traffic is quiet for `LOCAL_FINISH_DEFER_SECS`. Cleanup
remains bounded by the existing half-closed idle timeout after Finish.

Reusable rule: for reverse-heavy TCP flows, "local app has no more upload bytes"
is not the same as "safe to send upstream FIN now." Preserve the read direction
while remote data is making progress, then close the write half after a bounded
quiet window.

## 2026-07-02 — Knife14u: pending grace must require local send capability

Knife14t was tested with
`/tmp/mvpn_knife14t_usclient_suite_20260702_215901.tar.gz` on commit
`51072c8`. The bundle proved the VPS path was healthy, but tunnel throughput
regressed sharply: forward fell to low Mbit/s or below, reverse had Kbit/s-scale
windows and timeout cases, and loop active stayed mostly parked. The prior
generic pending grace reduced `dead_slot_reap pending>0` sizes, but kept
inactive stale flows alive for the grace window even when the local smoltcp
socket was no longer send-capable.

Knife14u narrows the lifecycle exception: active pending is still preserved, and
inactive pending gets progress-sensitive grace only if `TcpSocket::can_send()`
is still true. Inactive pending with `can_send=false` is undeliverable tail and
is reaped immediately. `dead_slot_reap` diagnostics now include `tcp_state`,
`active`, `can_send`, and `can_recv` so the next VPS log can separate useful
tail-loss risk from correct cleanup.

Local acceptance passed after removing accidental full-repo formatting churn:
`cargo test --lib reap_predicate -- --nocapture`, `cargo test --lib client_tun`,
full `cargo test`, `cargo clippy --all-targets -- -D warnings`, and
`git diff --check`.

Reusable rule: never protect a pending buffer solely because it exists. For TCP
downlink, the pending bytes must be tied to a local socket state that can still
accept them; otherwise a grace window turns cleanup into stale-flow interference.

## 2026-07-02 — Knife14t: pending downlink grace must be generic, not only deferred close

Knife14s was tested with
`/tmp/mvpn_knife14s_usclient_suite_20260702_180337.tar.gz`. The run proved the
previous established-uplink batching fix helped: the full forward P1/P2/P4/P8
sweep completed around 11-13 Mbit/s, reverse reached roughly 18-27 Mbit/s, and
`remote_write_timeout` stayed absent. The remaining lifecycle signal was still
bad: `dead_slot_reap` closed both `state=Relaying` and `state=Closing` slots
with non-zero `downlink_pending` while `send_slice_errors=0` and
`tun_flush_tx_failures=0`.

Root cause: knife14r protected only `pending_relay_close`. That fixed part of
the `state=Closing` path, but `state=Relaying` with pending bytes and an inactive
smoltcp socket still fell through `!active => reap` before a deferred relay close
record existed.

Knife14t makes pending progress generic. `flush_downlink` now reports accepted
bytes, `SocketCtx` tracks generic downlink pending progress, and the reap
predicate gives all non-empty pending a short progress-sensitive grace. First
observation and accepted bytes refresh the grace; pure growth does not. This
keeps tail delivery bounded without letting a stuck inactive slot live forever.

Local acceptance passed: `cargo test --lib client_tun`, full `cargo test`,
`cargo clippy --all-targets -- -D warnings`, and `git diff --check`.

Reusable rule: if a lifecycle exception is justified by "useful pending bytes
may still drain", attach the exception to the pending buffer itself, not only to
one terminal event that might arrive after another reap branch.

## 2026-07-02 — Knife14s: one payload per dirty pass looked exactly like a 5ms throughput cap

Knife14r was tested with
`/tmp/mvpn_knife14r_usclient_suite_20260702_171341.tar.gz`. Service preflight was
healthy, `remote_write_timeout` stayed absent, but forward throughput stayed in
the 1-2 Mbit/s range with long zero-bps windows. The code matched the number:
established uplink read exactly one smoltcp payload per dirty pass, so quiet
periods fell back to the 5ms timer. One MSS per 5ms is roughly the observed
forward rate.

Knife14s changes the established uplink path to drain a bounded batch per dirty
pass while preserving the old safety rule: reserve relay mpsc capacity before
reading smoltcp. If the relay channel is full, leave bytes in smoltcp so TCP
window backpressure still works.

Reusable rule: when a throughput result is close to `payload_size * tick_rate`,
look for hidden "one item per timer tick" logic before changing transport
settings. Batch hot-path work with an explicit fairness cap, not with unbounded
loops.

The suite now also records the git commit and binary checksum, and
`BUILD_RELEASE=1` builds the release binary before running. Future VPS logs must
prove which commit and binary were tested before we trust performance evidence.

## 2026-07-02 — Knife14r: deferred close grace should follow drain progress

Knife14q was tested with
`/tmp/mvpn_knife14q_usclient_suite_20260702_160853.tar.gz`. It confirmed the
knife14q uplink change: `remote_write_timeout` disappeared and reverse iperf no
longer timed out. The remaining lifecycle signal was still bad for a VPN data
plane: `dead_slot_reap` closed `state=Closing` slots with non-zero
`downlink_pending`.

The reusable rule is that a deferred relay close should not be bounded by the
age of the original close event alone. It should be bounded by lack of drain
progress: while `downlink_pending.len()` keeps decreasing, the local TCP side is
still accepting useful tail bytes and the slot should stay dirty; once pending
stops decreasing for `DEFERRED_CLOSE_PENDING_GRACE_SECS`, the slot can be hard
reaped to avoid leaks.

Local acceptance for this stage: added focused tests for progress-sensitive
reap and progress bookkeeping, then passed `cargo test --lib client_tun`,
`cargo test`, `cargo clippy --all-targets -- -D warnings`, and
`git diff --check`.

## 2026-07-02 — Knife14q: QUIC stream backpressure is not a per-chunk write failure

`480c3e8` / knife14p was tested with
`/tmp/mvpn_knife14p_usclient_suite_20260702_143215.tar.gz`. The run completed,
but it did not meet acceptance: forward P=2 timed out, reverse P=2/4/8 timed
out, reverse P=1 stayed at Kbit/s scale, and the client log still showed
`remote_write_timeout attempted_bytes=1160 timeout=5s`.

The important discriminator was that `global_rx_pressure_events=0` throughout
the run, so the new downlink high/low watermark was not the active mechanism.
The code-review finding was on the opposite direction: `run_relay_writer`
treated a pending TUIC/QUIC stream `write_all` as a hard failure after 5s. That
is wrong for a VPN data plane. A pending stream write can be normal QUIC
flow-control or path backpressure and should propagate back to the local TCP
socket through the bounded relay channel and smoltcp receive window.

Reusable rule: do not put short per-chunk deadlines around data-plane
`write_all` calls unless the deadline represents a protocol invariant. Keep
cleanup bounded at the relay/task level (`idle_timeout`, stop/shutdown abort),
not at the single payload write level.

Follow-up observation: if the next VPS run still shows weak reverse throughput,
focus on downlink/half-close/reap behavior. Knife14q only removes the confirmed
uplink `remote_write_timeout` failure mode.

## 2026-07-02 — Stage workflow now requires self-improvement summaries

Project memory is split into two layers:

1. Stable project goals and operating rules live in `AGENTS.md`, so new Codex
   sessions know mini_vpn's data-plane goal before planning or editing.
2. Stage experience, failed assumptions, test conclusions, and reusable debugging
   lessons live in `.learnings/LEARNINGS.md` or `.learnings/ERRORS.md` using the
   self-improving-agent pattern.

At the end of every meaningful stage, write a compact learning summary before
asking for the next VPS run or closing the turn. Include the stage name, commit
ID or log path when available, outcome, root cause if known, and the reusable
rule for future sessions. Promote only stable, repeated guidance into
`AGENTS.md` or `docs/tech/`; keep raw one-off noise out of memory.

## 2026-06-11 — Stage 13b: UDP over TUIC Packet rides the same authenticated QUIC connection

UDP relay now speaks **TUIC `Packet` (native QUIC datagram)** through the same sing-box exit as 13a's TCP:
Shenzhen client (tuic mode) → sing-box → `dig @1.1.1.1 example.com` returned Cloudflare's real A records,
`dig @1.1.1.1 facebook.com` a second flow, and `curl https://1.1.1.1/` still got `HTTP/2 301` (TCP
non-regression on the *same* connection). Lessons:

1. **A narrowed id space silently breaks an invariant inherited from a wider one.** `AssocTable` mirrors
   Stage-12 `FlowTable`'s "monotonic `next_id`, never reuse an id" scheme (so late datagrams can't cross
   into a new flow) — but in **u16**, `next_id` wraps after 65535 interns. The original copy assigned
   `next_id` unconditionally, so after a wrap it could `insert()` onto a *still-live* id (overwriting an
   active flow's mapping → downlink misroute) and orphan that flow's `tuple_to_id` entry (slow unbounded
   leak). `FlowTable` is safe only because u32 never wraps in practice. Fix: `alloc_id()` skips in-use ids;
   the live set (≤1024) ≪ 65536 so a free id always exists. Lesson: when you copy a table/allocator and
   shrink its key width, re-audit every "the space is so large this never happens" assumption.
2. **TUIC multiplexes TCP (bi-streams) and UDP (datagrams) over ONE authenticated connection.** sing-box
   validates auth per connection, so UDP datagrams must ride the *same* connection that sent Authenticate —
   you can't open a second anonymous QUIC connection for UDP. We funnel both `open_tcp` and `send_udp`/the
   datagram pump through a single `live_conn()` (reconnect serialized by one mutex), which is also why the
   TCP non-regression `curl` is a real test of co-existence, not a separate path.
3. **Downlink applies backpressure (`send().await`), uplink drops (`try`/count).** Dropping a DNS *response*
   on the downlink breaks `getaddrinfo`, so the datagram pump blocks rather than drops; the uplink keeps
   UDP semantics (drop + count on full / TooLarge / dead connection, self-heals on the next packet). Same
   asymmetry as Stage 12's `run_quic_pump`, reused deliberately.
4. **`@1.1.1.1` is the right UDP-relay probe precisely because it dodges the local fake-resolver.** The TUN
   only forges fake-IP answers for 198.18.0.1:53; every other `:53` goes to the relay (stage-12 D1 rule).
   So `dig @1.1.1.1` returning a *real* Cloudflare IP proves the query went out through TUIC Packet, not a
   local forgery — a self-forged answer would have returned a 198.18/15 address.

## 2026-06-10 — Stage 13a: our hand-written TUIC client interoperates with sing-box

The mini_vpn client now speaks the **TUIC v5 protocol** (ADR-0004) and was accepted by a real
**sing-box TUIC server** as the exit: Shenzhen client (tuic mode) → sing-box (US, UDP 8443) →
`curl https://1.1.1.1/` returned `HTTP/2 301` with Cloudflare's genuine TLS cert and `cf-ray …-SJC`
(US egress). Lessons:

1. **Interop is the proof of correctness.** A successful sing-box handshake validates the byte-exact
   parts that unit tests can't fully cover on their own: the Authenticate token derivation
   (`export_keying_material(label=UUID, context=password)`, 32 bytes) and the Connect/Address wire
   layout (TUIC ATYP 0x00 domain / 0x01 IPv4 / 0x02 IPv6 — different from our Stage-12 custom codes).
   We unit-tested the encoders against exact bytes, but "sing-box accepted it" is the real gate.
2. **Implementing a standard protocol > a bespoke one** for this goal: speaking TUIC means the exit is
   a mature, maintained sing-box (best experience, zero server code from us). The canonical TUIC repo is
   spec-only and the reference Rust crate is yanked, so we implemented the (stable) spec on our existing
   quinn — mature *design*, our *code*, full ecosystem interop.
3. **Dual-run keeps zero regression** while swapping the upstream: `MINI_VPN_UPSTREAM=legacy|tuic`
   (default legacy), an `Upstream` enum whose `open_tcp` returns a unified `RelayStream`, and the proven
   legacy path only *wrapped*, not modified.
4. **Concurrent sessions on one branch cause loss.** Another session committed to the same branch and
   clobbered a commit (recovered + pushed). Push after every commit and keep one writer per branch.

## 2026-06-08 — Stage 12 UDP-over-QUIC cross-machine acceptance; field-debugging lessons

UDP relay over a QUIC datagram data plane works end-to-end (Shenzhen client → US exit):
ATYP=1 (IP literal: `dig @1.1.1.1`, IP echo) and ATYP=3 (fake-IP→domain: `udp.zkwcloud.com`)
both relay; 1200-byte (QUIC-initial-sized) datagrams round-trip cold; **160 concurrent flows
= 160/160** with and without DNS. Several non-obvious lessons from the cross-machine bring-up:

1. **quinn defaults bite at three points; all needed tuning for a real long-lived data plane.**
   - `max_idle_timeout` defaults to 10s and `keep_alive_interval` to None → an idle QUIC
     connection drops every 10s and reconnects forever. Set keepalive **5s** (must be well under
     even a stale peer's 10s idle, since negotiated idle = min(both peers) — version skew between
     client and server made a 10s keepalive race the 10s boundary).
   - `initial_mtu` defaults to 1200 → `max_datagram_size` ~1162, too small for a 1200B inner
     payload (~1224 with our header), so a freshly-(re)connected data plane dropped large
     datagrams until PLPMTUD warmed. Set `initial_mtu`/`min_mtu` = **1280** (IPv6 minimum) so it
     fits cold. Note quinn's "MTU" is the UDP payload size, not the IP packet — 1280 is safe on
     real IPv4/1500 paths.
   - Both keepalive and MTU live in the **shared** transport config → BOTH ends must be rebuilt;
     a stale server silently dropped the 1200B *downlink* (1204B) as oversized. Always confirm
     `git log -1` matches on every box before trusting a cross-machine result.

2. **Per-packet `println!` on a single-threaded TUN loop is catastrophic under concurrency.**
   The device dumped the full packet bytes every recv and the whole tx_queue every transmit;
   under an 8-way burst this starved the loop, overflowed the utun buffer, and dropped UDP
   (concurrent 1/160). Removing per-packet/byte-dump logging (keep per-flow + drop/error logs)
   took it to 50/160. Logging on the data-plane hot path must be per-flow, not per-packet.

3. **Localize before fixing — a passing loopback test + a field-isolating test pin the layer.**
   A loopback integration test (one QUIC connection, 100 concurrent flows, ≥90/100) proved the
   relay/server were fine, pointing at the client TUN loop. Then an IP-literal-vs-domain field
   test split DNS contention from relay throughput.

4. **The test harness was the final bottleneck.** `ncat -k -u -e /bin/cat` is connection-oriented
   / forks per peer; our server uses one egress socket per flow, so the echo saw 160 distinct
   peers and choked (15/160). A single-socket Python echo (`recvfrom`/`sendto` loop) → 160/160.
   When a relay test underperforms, suspect the echo endpoint before the relay.

## 2026-06-02 — fake-IP end-to-end reached a GFW-blocked domain; two TUN-UDP source-address gotchas

Stage 11 fake-IP worked end-to-end: Shenzhen `curl https://www.facebook.com/` →
local fake-IP → tunnel → US exit resolves the domain → real Meta server (HTTP/2 200,
genuine `*.facebook.com` cert). First time the project bypassed local DNS poisoning.

Two non-obvious bugs, both about the **source address of replies from an AnyIP TUN**:
1. The fake DNS resolver listens on 198.18.0.1:53 via AnyIP, but a reply must carry
   src=198.18.0.1. Two things were required and BOTH were needed:
   (a) add 198.18.0.1 to the iface ip_addrs (else smoltcp can't egress that src), and
   (b) bind the UDP socket to the concrete 198.18.0.1 (NOT addr:None) — with None,
   smoltcp picks the reply source by subnet match (dst 10.0.0.1 → src 10.0.0.2), the
   OS resolver drops replies whose source != the queried server, and the symptom is
   `curl: Could not resolve host` while our log clearly shows the query arriving.
   The tx queue staying EMPTY (`发货单：[]`) was the tell that the reply never egressed.

Two follow-ups (logged in TODO):
- First TCP SYN to a freshly-allocated fake-IP can hit `connection refused` (curl does
  NOT retry on refused, unlike on timeout) — likely a race between the SYN inspector
  building the listener and the SYN being processed.
- Large HTTP/2 / multiplexed streams can fail mid-transfer with `bad decrypt` — relay
  byte-stream corruption/reordering under load; mechanism is correct (first request got
  a full 200) but high-throughput stability needs investigation.

## 2026-06-01 — Full jitter is observable and correct in the reconnect logs

Stage 10 cross-machine test (kill/restart US server) showed reconnect delays of
409/440/1554/354/5966ms — non-monotonic. That non-monotonicity is the proof that
full jitter is working: plain exponential backoff would be strictly increasing
(500/1000/2000...). The randomness is what spreads 5000 clients' reconnect
moments and prevents a thundering herd. When validating backoff, check that the
delays are NOT monotonic — a monotonic sequence means jitter is broken.

Also confirmed: infinite retry (4 failures then success) + reset-on-success
(second disconnect cycle restarts the attempt counter) + epoch increments per
reconnect (1→2→3). No panic when reconnecting while idle (0 in-flight to reset).

## 2026-06-01 — Byte-level transparent TCP relay preserves end-to-end TLS

Stage 9 cross-machine test against `https://1.1.1.1/` produced a full TLS 1.3
handshake where curl saw Cloudflare's real leaf certificate (`CN=cloudflare-dns.com`,
SSL.com intermediate) — not our dev cert. Confirms that the smoltcp + yamux + TLS
pipeline only carries opaque bytes; the inner TLS session is established between
the original client (curl) and the real target (Cloudflare), with our tunnel
acting as a pure pipe. HTTP/2 multiplexing also works end-to-end. This is the
right property for a VPN: the tunnel must NOT terminate the user's TLS or it
would break SNI / cert pinning / E2E privacy.

## 2026-05-27 — Verify worktree baseline before any design/implementation

The session worktree `claude/frosty-gates-10390f` was 39 commits behind `main`
(client_tun.rs was 133 lines vs main's 867). All early code reading was against
`main`'s files via absolute paths, so the design discussion nearly proceeded on a
stale base. Fixed with `git merge --ff-only main` (worktree HEAD was an ancestor of
main, 0 unique commits, no loss).

- Before designing/implementing in a worktree, check `git -C <worktree> log` and
  `git rev-list --count HEAD..main`. Don't assume the worktree == latest.

## 2026-05-27 — Run git in the worktree, not the main checkout

`cd /…/mini_vpn && git status` runs in the MAIN repo; the session's working tree is
the worktree under `.claude/worktrees/…`. New files written to the worktree won't show
in a `git status` run from main. Use `git -C <worktree-path>` or stay in the default
cwd (the worktree) — avoid `cd`-ing to the main checkout for git ops.

## 2026-05-29 — Always check the binary's startup banner before debugging behavior

A cross-machine test failed mysteriously: many `📡 收到` SYN logs, zero
`🎯 extracted target`, server kept reporting yamux disconnects. Root cause was
NOT code or topology — it was a stale binary. Stage 8 changes lived only on the
worktree branch, but the user was running `./target/debug/mini_vpn` built from
the main checkout (still at pre-Stage 8). The diagnostic tell was the startup
banner: it contained `target=httpbin.org:80` (a field Task 3 had removed),
proving the running binary predated Task 3.

- After ANY code change, first check the startup banner / version line matches
  the expected build. Don't debug behavior against a binary you didn't just compile.
- When work lives on a feature branch / worktree, prefer running the binary from
  that worktree's `target/debug/` until the branch is merged, or merge first.

## 2026-05-27 — Same-machine upstream + a global TUN host route = egress loop

Stage 8 round-trip test failed with the upstream `server` on localhost: the
`route add -host <target> -interface <our-utun>` is machine-global, so the
server's own `connect(<target>)` egress is ALSO diverted into our TUN instead of
reaching the real internet -> `Connection refused` / loop. Target extraction
itself worked (client logged `🎯 extracted target 1.1.1.1:80`, server logged the
correct target); only the outbound hop broke.

- The full byte round-trip can only be validated with the upstream on a
  DIFFERENT host (the real VPN topology — e.g. the US server). On a single
  machine the route inevitably captures the server's egress to the target.
- Always use an IP literal as the test target on dev machines: a co-resident
  fake-ip TUN proxy (Clash/Mihomo) hijacks DNS to 198.18.0.0/15.

Note: `67c466b add` committed the worktree directory itself as a gitlink
(mode 160000 `.claude/worktrees/frosty-gates-10390f`). Recursive/self-referential;
worth cleaning up separately.

## 2026-07-04 — Knife14au terminal-zero is not close-pending-zero

- Stage: Knife14au pending-at-close taxonomy.
- Outcome: Added behavior-neutral close pending classification to
  `tcp-handle-close` diagnostics and low-RTT probe summaries. Local gates passed:
  `cargo test --lib client_tun`, full `cargo test`,
  `cargo clippy --all-targets -- -D warnings`, probe/suite self-tests, shell
  syntax checks, and `git diff --check`.
- Key lesson: `terminal_pending_reap: events=0 bytes=0` only proves no close
  matched `Closed && active=false && can_send=false`; it does not prove there
  were no close-time pending bytes. Knife14at raw logs had
  `tcp_state=Established active=true can_send=false pending>0`, which was
  previously visible only by manual raw-log inspection.
- Reusable rule: every reverse/downlink acceptance report must read
  `pending_at_close` alongside `terminal_pending_reap`. If throughput is low,
  split pending tails by `terminal_closed_no_send`, `active_no_send`,
  send-capable, inactive no-send, and unknown before changing close-drain,
  pacing, TUN queue length, TUIC pool, iperf3, or sing-box.

## 2026-07-04 — Knife14av per-probe summaries need a post-iperf settle window

- Stage: Knife14av post-iperf close-tail reporting.
- Outcome: Knife14au VPS acceptance showed reverse TCP throughput recovered
  (`179/179`, `185/185`, and `183/181 Mbit/s`), but one full-reverse raw
  `tcp-handle-close pending>0` line appeared after that probe's attribution
  summary had already been written.
- Key lesson: a final post-run metric tail is not enough for Knife14 branch
  decisions. If close/reap logs arrive just after `iperf3` exits, the raw log
  may contain the evidence while the per-probe `pending_at_close` attribution
  remains zero.
- Reusable rule: reverse/downlink acceptance probes must wait a short, bounded
  post-iperf settle window before sampling final TUN counters and summarizing
  metrics. Keep the window explicit in the report and configurable for quick
  local debugging.

## 2026-07-04 — Knife14ba needs first-byte/read-gap evidence before behavior patches

- Stage: Knife14ba TUIC stream first-byte and relay read-gap diagnostics.
- Outcome: Added behavior-neutral timing diagnostics for relay remote reads and
  TUIC TCP stream reads, plus low-RTT parser summaries/attribution for
  `tuic_stream_first_byte_slow`, `tuic_stream_read_gap`,
  `relay_remote_first_byte_slow`, and `relay_remote_read_gap`.
- Local gates passed: `cargo test --lib tuic::tests::`,
  `cargo test --lib client_tun::tests::`,
  `scripts/knife14b-lowrtt-probe.sh --self-test`,
  `scripts/knife14b-usclient-tunnel-suite.sh --self-test`, full `cargo test`
  outside sandbox, `cargo test --features harness` outside sandbox,
  `cargo clippy --all-targets --features harness -- -D warnings`, and
  `git diff --check`.
- Key lesson: after Knife14az showed `remote_to_global_rx_bytes` near zero with
  no downlink pending, terminal pending, TUN drop, or clean-window QUIC loss,
  the next useful evidence is stream first-byte latency and read gaps, not
  another close-drain, pacer, pool, iperf3, or sing-box tweak.
- Reusable rule: if reverse-first throughput is low and local downlink/pending
  counters stay quiet, parse `tuic_tcp_stream` and `relay_remote_timing` before
  changing behavior. A large first-byte or read-gap label should drive the next
  patch; contradictory timing evidence should trigger architecture review.

## 2026-07-04 — Knife14ba rules out TUIC first-byte for clean reverse-first

- Stage: Knife14ba VPS acceptance for stream timing diagnostics.
- Bundle: `/tmp/mini_vpn/mvpn_knife14ba_stream_timing_usclient_suite_20260704_231537.tar.gz`
- Code commit: `7e25e92`.
- Outcome: clean reverse-first P1 improved only to `31.0/28.7 Mbit/s`, but
  TUIC first receive was fast (`3ms`) and the data stream delivered about
  `110MB` into mini_vpn with `max_read_gap_ms=3763`. Clean-window QUIC
  loss/congestion, TUN drops, global_rx pressure, and local write pressure were
  all quiet.
- Key lesson: the remaining clean reverse-first root is local TCP downlink
  delivery under tx-queue / receive-window pressure, not delayed TUIC stream
  first byte. The run ended with `max_tx_queue_bytes=574824`,
  `pending_at_close=542302`, and `terminal_pending_reap=542302` on a closed
  non-send-capable local TCP socket.
- Reusable rule: after first-byte/read-gap diagnostics contradict a remote-read
  stall hypothesis, keep those diagnostics but move the next TDD slice to local
  send-queue pressure and terminal pending accounting. Do not revisit stale
  pool, iperf3, sing-box, or blunt pacing without new contradictory evidence.

## 2026-07-04 — Knife14bb parser attribution must separate control and data streams

- Stage: Knife14bb parser data-stream attribution.
- Outcome: Updated `scripts/knife14b-lowrtt-probe.sh` so global stream timing
  maxima remain visible, but reverse TCP read-gap attribution uses data-bearing
  streams/handles only. The parser now reports `data_streams`,
  `data_*_max_ms`, `data_rx_bytes_max`, and `data_rx_min_bytes=65536`.
- Local gates passed: `scripts/knife14b-lowrtt-probe.sh --self-test`,
  `scripts/knife14b-usclient-tunnel-suite.sh --self-test`, and `bash -n` for
  both scripts.
- Key lesson: a large read gap is not enough to identify the throughput root.
  It must be paired with stream byte volume; otherwise idle iperf control
  streams can create false remote-read labels.
- Reusable rule: parser attribution for throughput root cause should use
  data-stream filtered fields, while raw/global maxima remain in the report for
  manual inspection.

## 2026-07-04 — Knife14bc scales tx-queue backpressure with the configured tx buffer

- Stage: Knife14bc tx-window scaled backpressure defaults.
- Outcome: Added adaptive default downlink backpressure for runtime env config:
  when `MINI_VPN_TCP_TX_BUFFER_BYTES` is larger than the legacy `512KiB` high
  watermark and backpressure env values are not explicitly set, the high
  watermark rises to the tx buffer size and the low watermark rises to one
  quarter of that size. Explicit backpressure env values still win.
- Local gates passed: focused RED/GREEN test, `cargo test --lib
  client_tun::tests::`, low-RTT parser self-test, US-client suite self-test,
  full `cargo test` outside sandbox, `cargo test --features harness` outside
  sandbox, and `cargo clippy --all-targets --features harness -- -D warnings`.
- Key lesson: Knife14ba showed useful receiver bytes matched
  `send_slice_accepted`; the terminal pending tail arrived after local close.
  The earlier limiter is the fixed 512KiB tx-queue pause threshold acting as a
  receive-window cap on a VPS RTT path.
- Reusable rule: when TCP socket tx buffer is explicitly enlarged for
  throughput, tx-queue backpressure defaults must scale with that buffer unless
  the operator explicitly configures different watermarks.

## 2026-07-04 — Knife14bd suite must not override adaptive runtime defaults

- Stage: Knife14bd suite adaptive backpressure.
- Outcome: Updated `scripts/knife14b-usclient-tunnel-suite.sh` so default
  downlink backpressure env values are empty and reported as `<auto>`, letting
  the Knife14bc binary scale high/low from `MINI_VPN_TCP_TX_BUFFER_BYTES`.
  Explicit operator-provided high/low env values still pass through.
- Local gates passed: suite self-test and shell syntax check.
- Key lesson: code-side adaptive defaults are ineffective if acceptance scripts
  always export fixed legacy env values.
- Reusable rule: before a VPS run that validates runtime config defaults, grep
  the suite for env exports that may shadow the binary default being tested.

## 2026-07-04 — Knife14bd 1MiB adaptive backpressure is necessary but insufficient

- Stage: Knife14bd VPS acceptance for tx-buffer-scaled downlink backpressure.
- Bundle:
  `/tmp/mini_vpn/knife14bd_scaled_auto_20260704_234307/mvpn_knife14bd_scaled_auto_usclient_suite_20260704_234307.tar.gz`
- Code under test: `9578f5e`.
- Outcome: startup confirmed auto watermarks `high=1048576B low=262144B`, but
  clean reverse-first stayed at `27.8/26.2 Mbit/s`. Clean-window QUIC
  loss/congestion, TUN drops, local/global write pressure, and flush failures
  remained quiet. The data stream first read was `3ms`, data max read gap was
  `3826ms`, `send_queue_max=1048576`, and terminal pending/reap grew to
  `1058416B`.
- Key lesson: moving the high watermark from 512KiB to the configured 1MiB tx
  buffer validates the receive-window branch but does not clear it. The next
  discriminating test is a scoped tx-buffer/receive-window A/B, not another
  stale-pool, sing-box, iperf3, TUN queue, or egress-pacer change.
- Reusable rule: if terminal pending scales with the tx buffer and clean
  throughput remains burst/idle, test whether larger tx-window capacity improves
  useful receiver throughput before changing close-drain behavior again. If it
  only scales the terminal tail, pivot to local TCP/TUN drain cadence.

## 2026-07-05 — Knife14be falsifies simple receive-window capacity

- Stage: Knife14be 4MiB receive-window A/B.
- Bundle:
  `/tmp/mini_vpn/knife14be_tx4m_auto_20260705_065724/mvpn_knife14be_tx4m_auto_usclient_suite_20260705_065724.tar.gz`
- Code under test: `5f1cbfb`.
- Outcome: startup confirmed `tx=4194304B` and auto downlink backpressure
  `high=4194304B low=1048576B`, but clean reverse-first dropped to
  `20.3/18.9 Mbit/s`. Clean-window QUIC loss/congestion remained quiet, but
  TUN egress drops appeared: `tun_tx_dropped_delta=27530`,
  `runtime_tun_egress drop_delta_total=14946`.
- Key lesson: increasing the local TCP tx buffer moved pressure from
  visible 1MiB backpressure/terminal pending into TUN/qdisc loss. The root is
  not simply receive-window capacity; the next useful branch is local TCP/TUN
  drain cadence or TUN-drop-aware feedback.
- Reusable rule: if a larger tx buffer worsens clean reverse and introduces TUN
  egress drops while QUIC stays clean, stop increasing buffers. Treat the larger
  buffer as evidence that burst smoothing / TUN feedback is missing.

## 2026-07-05 — Knife14bf adds TUN-drop feedback as a separate pause reason

- Stage: Knife14bf local TUN-drop-aware downlink feedback.
- Outcome: Added a separate `TunEgressFeedbackState` so runtime TUN egress
  drops can pause `global_rx` without being counted as ordinary
  high-watermark downlink backpressure. The feedback samples Linux TUN
  `tx_dropped` once per second, pauses when a positive drop delta appears with
  local downlink pressure, and resumes once local pressure drains to the
  configured low watermark.
- Parser/result shape: `scripts/knife14b-lowrtt-probe.sh` now reports
  `tun_egress_feedback` separately from `runtime_tun_egress` and avoids
  double-counting `tcp-tun-egress-feedback` as a `tcp-tun-egress` runtime drop
  sample.
- Local gates passed: `cargo test --lib client_tun`,
  `bash scripts/knife14b-lowrtt-probe.sh --self-test`,
  `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`,
  `bash -n` for both scripts, and `git diff --check`.
- Key lesson: Knife14be's new root is not "any TUN drop is fatal"; Knife14at
  already showed high reverse throughput with some drops. The actionable shape
  is "large tx queue below high watermark + clean QUIC + TUN drops + no
  ordinary backpressure," which needs its own feedback/accounting path.
- Reusable rule: when adding a new diagnostic line with a shared prefix, tighten
  parser regexes first. Otherwise derived lines such as
  `tcp-tun-egress-feedback` can be accidentally counted as base
  `tcp-tun-egress` samples.

## 2026-07-05 — Knife14bf feedback did not hit clean reverse-first pressure

- Stage: Knife14bf VPS acceptance for TUN-drop-aware downlink feedback.
- Bundle:
  `/tmp/mini_vpn/knife14bf_tun_feedback_rerun_20260705_073541/mvpn_knife14bf_tun_feedback_rerun_usclient_suite_20260705_073541.tar.gz`
- Code under test: `e661613`.
- Outcome: after a sing-box restart cleared an initial TUIC startup failure,
  clean reverse-first P1 still measured only `22.8/21.6 Mbit/s`. The clean
  window had no QUIC loss/congestion and no TUN flush failures, but still had
  ordinary downlink backpressure (`pause_edges=7`, `max_tx_queue_bytes=1048576`)
  plus terminal pending/reap of `1108466B`.
- Key lesson: the sysfs TUN-drop feedback gate is observable and can fire on
  VPS, but it did not fire in the clean reverse-first window because the drop
  delta was sampled when `dirty_handles=0` and `pending_total=0`. The primary
  clean limiter remains local TCP/TUN drain cadence plus 1MiB tx-queue
  backpressure/terminal pending, not stale pools, server path, larger receive
  windows, or egress pacing.
- Reusable rule: treat Linux TUN drop counters as lagging diagnostics unless
  they are correlated with same-window local pressure. The next Knife14 patch
  should reduce ordinary `downlink_backpressure` and terminal pending directly,
  not only add later drop feedback.

## 2026-07-05 — Knife14bg should test TUN ingress fairness before more tuning

- Stage: Knife14bg grounding/spec for TUN RX drain cadence.
- Outcome: code reading showed remote downlink bytes enter through
  `global_rx.recv()`, while local TCP ACK/window-update packets enter only via
  `device.wait_for_rx()`, one packet at a time. The 5ms timer polls smoltcp but
  does not read new TUN packets. Under a constantly ready remote downlink
  branch, this can delay local ACK/window processing until tx-queue backpressure
  pauses remote reads.
- Key lesson: a full smoltcp tx queue with clean QUIC and zero TUN flush
  failures can be caused by ingress fairness, not just egress write capacity.
- Reusable rule: before changing buffers again, add a bounded nonblocking TUN
  RX drain path and diagnostics that prove whether local ACK/window updates are
  being processed promptly during remote downlink pressure.

## 2026-07-05 — Knife14bh shows bg TUN RX drain is a regression trigger

- Stage: Knife14bh stream-gap A/B.
- Code under test: `7e33d44`.
- Bundles:
  `/tmp/mini_vpn/mvpn_knife14bh_default_usclient_suite_20260705_094918.tar.gz`
  and
  `/tmp/mini_vpn/mvpn_knife14bh_drain0_usclient_suite_20260705_095120.tar.gz`.
- Outcome: default `MINI_VPN_TUN_RX_DRAIN_BUDGET=8` collapsed clean
  reverse-first P1 to `0.245/0.035 Mbit/s` and produced a data stream
  `data_pending_gap_max_ms=24516` with only `209758B` received. Setting
  `MINI_VPN_TUN_RX_DRAIN_BUDGET=0` restored useful flow to
  `26.5/24.5 Mbit/s`, data stream first byte `4ms`, and about `99MB` received.
- Key lesson: the Knife14bg opportunistic TUN RX drain path is not safe as a
  default. It can starve useful reverse downlink rather than improve ACK/window
  fairness.
- Reusable rule: when an opportunistic fairness path runs inside a hot remote
  payload branch, prove it with an off-switch A/B before accepting it as a
  default. If `budget=0` materially improves throughput, flip the default off
  before continuing deeper lifecycle work.

## 2026-07-05 — Drain-disabled path exposes next local limiter

- Stage: Knife14bh drain0 analysis.
- Outcome: with TUN RX drain disabled, clean reverse-first still only reached
  `24.5 Mbit/s` receiver. TUIC data-stream first byte was fast and data pending
  gap was no longer the primary label, while local pressure appeared as
  `downlink_backpressure pause_edges=11`, `send_queue_max=1048576`,
  `tun_tx_dropped_delta=1639`, and pending-at-close `5617B`
  (`active_no_send`).
- Key lesson: after removing the bg drain regression, the remaining Knife14
  branch is local TUN egress/downlink backpressure, not TUIC stream first-byte
  starvation.
- Reusable rule: separate regression removal from next-root optimization. First
  make the default match the known-better drain0 shape, then tune the recovered
  egress/backpressure limiter.

## 2026-07-05 — Knife14bi default-off patch reached VPS startup but not throughput

- Stage: Knife14bi default drain-off.
- Code under test: `76af8dc`.
- Bundle:
  `/tmp/mini_vpn/mvpn_knife14bi_default_usclient_suite_20260705_111824.tar.gz`.
- Outcome: local gates passed and `.27` startup confirmed
  `TUN RX drain budget: 0 packets/pass`, but the scoped VPS suite failed before
  throughput with `tuic auth finish: sending stopped by peer: error 0`.
  Direct `.27 -> .77` and `.33 -> .77` baselines were healthy, `.33` sing-box
  was active/listening/config-valid, certificates were valid, and a no-secret
  exact comparison showed UUID/password/SNI/ALPN all matched.
- Key lesson: this bundle is not throughput evidence and does not invalidate
  the drain default-off patch. It is the same startup-failure class observed in
  Knife14bf before sing-box restart.
- Reusable rule: when TUIC auth-finish startup fails while service health and
  exact no-secret config comparison pass, restart sing-box once and rerun the
  same scoped suite before touching mini_vpn data-plane code.

## 2026-07-05 — Knife14bi restart rerun exposes TUIC stream starvation

- Stage: Knife14bi default drain-off rerun after `.33` sing-box restart.
- Bundle:
  `/tmp/mini_vpn/mvpn_knife14bi_default_rerun_usclient_suite_20260705_113246.tar.gz`.
- Outcome: restart cleared the TUIC auth-finish startup failure, and startup
  confirmed `TUN RX drain budget: 0` with `tun_rx_drain attempts=0`. The
  reverse-first probe still collapsed to `0.280/0.017 Mbit/s`. Local pending,
  TUN drops, terminal pending, downlink backpressure, and QUIC loss/blocking
  were all quiet.
- Key lesson: default drain-off is necessary but not sufficient. The new
  failing shape is data-stream starvation: first RX is fast (`3ms`), but the
  TUIC data stream only receives about `86KB` and reports long
  read/pending gaps around `17s`.
- Reusable rule: when `remote_to_global_rx_bytes` remains tiny while local
  downlink/backpressure counters are quiet, pivot away from close-drain and
  TUN egress. The next diagnostic must instrument TUIC stream read progress,
  receive-window/flow-control state, and server-side send behavior.

## 2026-07-05 — Knife14bj adds TUIC stream poll-cadence diagnostics

- Stage: Knife14bj TUIC stream starvation diagnostics.
- Outcome: Added behavior-neutral poll cadence accounting to TUIC TCP stream
  diagnostics and parser output. `tuic-tcp-stream-pending` /
  `tuic-tcp-stream-close` now include `polls` and `max_poll_gap_ms`, and the
  low-RTT parser emits `tuic_stream_polling` with data-stream-filtered poll
  maxima.
- Local gates passed: `cargo test --lib tuic::tests::`,
  `bash scripts/knife14b-lowrtt-probe.sh --self-test`,
  `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`,
  shell syntax checks for both scripts, `cargo clippy --lib -- -D warnings`,
  sandbox-external `cargo test --lib`, and `git diff --check`.
- Key lesson: the old `pending_gap_ms` signal showed the stream had no bytes,
  but not whether the relay task itself was being polled. Pairing
  `pending_gap_ms` with `max_poll_gap_ms` separates task scheduling/wakeup
  gaps from a consistently polled but idle QUIC stream.
- Reusable rule: for TUIC stream starvation, treat high `pending_gap_ms` plus
  low `data_poll_gap_max_ms` as evidence against local relay-task starvation;
  if both are high, inspect task wakeups/scheduling before blaming sing-box,
  target iperf, close-drain, or TUN egress.

## 2026-07-05 — Knife14bj VPS poll-cadence run narrows starvation away from local polling

- Stage: Knife14bj scoped reverse-first VPS acceptance.
- Bundle:
  `/tmp/mini_vpn/knife14bj_poll_20260705_131659/mvpn_knife14bj_poll_usclient_suite_20260705_131659.tar.gz`
- Code under test: `4dc79af`.
- Outcome: direct `.27 -> .77` and `.33 -> .77` baselines were healthy around
  `279-282 Mbit/s`, `.33` had no sampled `fail auth`, and mini_vpn startup
  confirmed `TUN RX drain budget: 0`. Reverse P1 still collapsed to
  `0.210/0.019 Mbit/s`.
- Key signal: the data stream showed `data_pending_gap_max_ms=20516` and only
  `137520B` received, but `data_poll_gap_max_ms=5000` with `data_polls_max=123`.
  Local downlink backpressure, global_rx pressure, TUN drops, and QUIC
  loss/blocking stayed quiet.
- Key lesson: Knife14bj excludes a full relay-task polling blackout for the
  starvation window. The task is periodically re-polled, but the data stream
  does not become ready for long gaps.
- Reusable rule: after this shape, the next useful diagnostic must capture the
  server/send side or stream readiness boundary. Do not keep changing local
  close-drain, TUN egress, stale pool, egress pacing, or buffer sizes without
  evidence that those paths are active in the clean window.

## 2026-07-05 — Knife14bk adds per-probe server-side evidence capture

- Stage: Knife14bk local implementation for server/send-side attribution.
- Outcome: extended the US-client tunnel suite with opt-in
  `SERVER_EVIDENCE_CHECK=1` artifacts per probe. When enabled, the suite now
  captures bounded `.33` sing-box TUIC/outbound/fail-auth/error lines and `.77`
  iperf3 journal lines for the probe time window, with UUID/password-like
  values redacted and SSH keys reported only as set/unset.
- Local gates passed: `bash scripts/knife14b-usclient-tunnel-suite.sh
  --self-test`, `bash -n scripts/knife14b-usclient-tunnel-suite.sh`, and
  `git diff --check`.
- Key lesson: Knife14 stream-starvation acceptance needs server-side evidence
  inside the bundle, not manual journal inspection after the fact.
- Reusable rule: before applying behavior changes to local downlink lifecycle
  after a starvation run, capture the target sender totals and exit forwarding
  lines in the same probe window. If `.77` itself sends little, pivot to
  ACK/window/send-side analysis; if `.77` sends much more than mini_vpn reads,
  inspect exit forwarding and TUIC stream readiness.

## 2026-07-05 — Knife14bk server evidence re-opens local TUN egress pressure

- Stage: Knife14bk scoped VPS acceptance with server-side evidence.
- Bundle:
  `/tmp/mini_vpn/knife14bk_server_evidence_20260705_134634/mvpn_knife14bk_server_evidence_usclient_suite_20260705_134634.tar.gz`
- Code under test: `804eec1`.
- Outcome: reverse-first P1 remained low at `18.8/17.0 Mbit/s`, while direct
  `.27 -> .77` and `.33 -> .77` reverse baselines were healthy around
  `297` and `284 Mbit/s`. The server evidence artifact showed `.77` iperf3
  itself sent only `67.1 MBytes / 18.8 Mbit/s` over the tunneled reverse path,
  and `.33` logged TUIC inbound/direct outbound opens without a current
  `fail auth` signal.
- Key lesson: this run is no longer the Knife14bj near-zero stream-starvation
  shape. mini_vpn read about `70MB` from the TUIC stream, but local egress
  pressure reappeared: `downlink_backpressure pause_edges=10`,
  `tun_tx_dropped_delta=6070`, `runtime_tun_egress drop_delta_total=5666`, and
  close pending was `active_no_send=16254B` with `terminal_pending_reap=0`.
- Reusable rule: when server-side reverse sender throughput equals tunnel
  sender throughput and direct baselines are healthy, inspect the local ACK /
  receive-window feedback caused by smoltcp send-queue saturation and TUN qdisc
  drops. Do not continue the TUIC stream-starvation branch unless a new run
  again shows low target sender bytes without local egress pressure.

## 2026-07-05 — Knife14bl gates immediate downlink TUN flush on local send-queue pressure

- Stage: Knife14bl local TDD/code before VPS acceptance.
- Outcome: added a pressure-aware remote-payload immediate flush decision. A
  pending downlink backlog still forces immediate progress while the local TCP
  send queue is below the downlink high watermark, but if `send_queue >= high`
  the flush is deferred to the timer/dirty-relay path and accepted bytes still
  debit the immediate budget.
- Code/spec: `src/client_tun.rs`,
  `docs/tech/2026-07-05-knife14bl-tun-egress-pressure-spec.md`, and
  `docs/tech/2026-07-05-knife14bl-tun-egress-pressure-plan.md`.
- Local gates passed: `cargo test downlink_egress_pacer --lib`,
  `cargo test downlink --lib`, `cargo test tun_egress --lib`,
  `scripts/knife14b-usclient-tunnel-suite.sh --self-test`,
  `scripts/knife14b-lowrtt-probe.sh --self-test`,
  sandbox-external `cargo test --lib`, `cargo clippy --lib -- -D warnings`,
  and `git diff --check`.
- Reusable rule: after evidence shows target-side reverse sender throughput is
  also low and local `tun_tx_dropped`/send-queue pressure is present, the first
  behavior fix should reduce remote-payload-driven immediate TUN bursts at the
  local pressure boundary without lowering product-wide egress defaults.

## 2026-07-05 — Knife14bl VPS run escapes 10-20 Mbit/s but does not pass acceptance

- Stage: Knife14bl scoped VPS acceptance for `09bb67c`.
- Bundle:
  `/tmp/mini_vpn/knife14bl_pressure_gate_20260705_152416/mvpn_knife14bl_pressure_gate_usclient_suite_20260705_152416.tar.gz`
- Outcome: reverse-first P1 improved to `131/130 Mbit/s` from Knife14bk
  `18.8/17.0 Mbit/s`, proving the pressure-aware immediate-flush gate helped.
  Direct baselines stayed healthy around `281-282 Mbit/s` receiver on the
  reverse path.
- Remaining blockers: throughput collapsed back to `11-24 Mbit/s` in the last
  seconds; `tun_tx_dropped_delta=2707`; backpressure still toggled
  `143/143`; terminal pending reap returned with `1048938B`.
- Key lesson: the drop sampler saw the TUN qdisc drop only after local pressure
  had drained, so `runtime_tun_egress` counted the drop but
  `tun_egress_feedback` did not pause (`drop_events=0`). Current-pressure-only
  drop attribution is too narrow for this failure shape.
- Reusable rule: for TUN egress feedback, correlate `tx_dropped` with recent
  downlink pressure, not only pressure visible at the exact 1s sample. Keep the
  next patch local and testable before another VPS run.

## 2026-07-05 — Knife14bm latches recent high pressure for TUN drop feedback

- Stage: Knife14bm local TDD/code before VPS acceptance.
- Outcome: added a bounded recent high-pressure latch to
  `TunEgressFeedbackState`. The event loop observes existing
  `downlink_pressure_stats` each iteration; if pressure reaches the high
  watermark, the next 1s TUN drop sample can attribute a positive
  `tx_dropped_delta` even if current pressure has drained to zero.
- Code/spec: `src/client_tun.rs`,
  `docs/tech/2026-07-05-knife14bm-tun-egress-recent-pressure-spec.md`, and
  `docs/tech/2026-07-05-knife14bm-tun-egress-recent-pressure-plan.md`.
- Local gates passed: `cargo test tun_egress --lib`,
  `cargo test downlink --lib`,
  `scripts/knife14b-usclient-tunnel-suite.sh --self-test`,
  `scripts/knife14b-lowrtt-probe.sh --self-test`,
  sandbox-external `cargo test --lib`, `cargo clippy --lib -- -D warnings`,
  and `git diff --check`.
- Reusable rule: keep TUN drop attribution bounded to the next feedback sample;
  only latch high-watermark pressure, not arbitrary small queues, so unrelated
  future drops do not trigger false global_rx pauses.

## 2026-07-05 — Knife14bm VPS run did not exercise the recent-pressure latch

- Stage: Knife14bm scoped VPS acceptance for `3d06bea`.
- Bundle:
  `/tmp/mini_vpn/knife14bm_recent_pressure_20260705_154706/mvpn_knife14bm_recent_pressure_usclient_suite_20260705_154706.tar.gz`
- Outcome: reverse-first P1 regressed to `1.19/0.00 Mbit/s`, but this was not
  the Knife14bl TUN-pressure tail-collapse shape. `.77` iperf3 also sent only
  `4.25 MBytes / 1.19 Mbit/s`, mini_vpn data stream first RX was delayed until
  about `37.6s`, and local egress stayed quiet:
  `tun_tx_dropped_delta=0`, `downlink_backpressure pause_edges=0`,
  `terminal_pending_reap=0`.
- Key lesson: this VPS run cannot validate or reject the recent-pressure latch
  because no TUN drop or local pressure existed to classify. The evidence
  points back to receive-window/ACK-path or stream readiness starvation before
  local egress.
- Reusable rule: when target sender bytes are also low and local egress counters
  are quiet, do not continue TUN feedback tuning. First run a bounded same-window
  A/B or add attribution that separates code regression from VPS stream-readiness
  variance, then choose the next behavior branch.

## 2026-07-05 — Knife14bn A/B clears 3d06bea as the no-data-stream cause

- Stage: Knife14bn same-window VPS A/B.
- Bundles:
  `/tmp/mini_vpn/knife14bn_ab_20260705/mvpn_knife14bn_ab_09bb67c_usclient_suite_20260705_174746.tar.gz`
  and
  `/tmp/mini_vpn/knife14bn_ab_20260705/mvpn_knife14bn_ab_3d06bea_usclient_suite_20260705_175004.tar.gz`.
- Outcome: `09bb67c` also produced a low-byte quiet-egress run
  (`0.210/0.021 Mbit/s`, `.77` sender `768 KiB / 210 Kbit/s`), so the prior
  Knife14bm no-data-stream result was not enough to blame `3d06bea`.
  `3d06bea` returned to a local pressure shape (`19.3/18.2 Mbit/s`) with
  `downlink_backpressure 6/6`, `tun_tx_dropped_delta=1272`, and
  `terminal_pending_reap=809715B`.
- Key lesson: the recent-pressure latch did fire on VPS evidence:
  `tun_egress_feedback pause_edges=1 resume_edges=1 drop_events=1`. Keep it.
  The remaining blocker is not latch attribution but close-drain/terminal
  pending under local downlink pressure.
- Reusable rule: if a same-window A/B shows the older commit can also hit
  no-data-stream while the newer commit reaches the pressure branch, do not
  revert the newer diagnostic/control fix. Continue from the run that activates
  the local pressure path and fix the next visible lifecycle invariant.

## 2026-07-05 — Knife14bo local gate separates terminal-late payload

- Stage: Knife14bo local TDD/code before VPS acceptance.
- Outcome: `handle_remote_payload` now gates remote payload with the current
  smoltcp socket snapshot before appending to `downlink_pending`. Local FIN
  read-only payload remains accepted while the socket is active/send-capable,
  but `Closed && !active && !can_send` payload is rejected and counted as
  `terminal_late_remote_payload`.
- Code/spec: `src/client_tun.rs`,
  `scripts/knife14b-lowrtt-probe.sh`,
  `docs/tech/2026-07-05-knife14bo-terminal-pending-close-drain-spec.md`, and
  `docs/tech/2026-07-05-knife14bo-terminal-pending-close-drain-plan.md`.
- Local gates passed: focused `cargo test --lib remote_payload_`, focused
  TCP downlink diag/aggregate tests, `scripts/knife14b-lowrtt-probe.sh
  --self-test`, sandbox-external `cargo test --lib`, and `git diff --check`.
- Key lesson: terminal pending must be split into pre-terminal backlog and
  terminal-late remote payload. Reaping true terminal no-send pending remains
  correct, but accepting additional remote bytes after that state hides the
  causal loss point.
- Reusable rule: any future close-drain patch must preserve local-FIN read-only
  reverse data while refusing terminal no-send late payload before it enters
  app-owned pending.

## 2026-07-05 — Knife14bo VPS clears terminal pending as current loss point

- Stage: Knife14bo scoped VPS acceptance for `615cf47`.
- Bundle:
  `/tmp/mini_vpn/knife14bo_terminal_late_20260705/mvpn_knife14bo_terminal_late_615cf47_usclient_suite_20260705_184932.tar.gz`
- Outcome: reverse-first P1 remained low at `25.3/24.0 Mbit/s`, but the targeted
  close-drain branch stayed clean: `terminal_pending_reap=0`,
  `terminal_late_remote_payload=0`, `pending_at_close=0`, `tun_tx_dropped_delta=0`,
  and QUIC loss/congestion `0/0`.
- Server-side follow-up: manual `.33` window logs showed TUIC inbound and direct
  outbound opens with no `fail auth`; manual `.77` iperf3 journal showed the
  target sender itself at `90.6 MBytes / 25.3 Mbit/s`, matching the tunnel
  sender.
- Key lesson: Knife14bo did not meet final throughput acceptance, but it proved
  the current low-throughput run is not hiding loss in terminal pending/close
  reap. The next branch is pre-close receive-window / ACK / tx-queue-only
  backpressure behavior.
- Reusable rule: when app-owned pending, terminal pending, terminal-late payload,
  TUN drops, and QUIC loss are all zero while the target sender is low, do not
  continue close-drain or egress-pacer patches. Instrument tx-queue-only
  backpressure and sender stalls next.

## 2026-07-05 — Knife14bp defaults server evidence SSH for known VPS topology

- Stage: Knife14bp local script/spec before VPS evidence acceptance.
- Outcome: `scripts/knife14b-usclient-tunnel-suite.sh` now defaults
  `EXIT_SSH_HOST` and `TARGET_SSH_HOST` for the known Knife14 `.33/.77`
  topology when `SERVER_EVIDENCE_CHECK=1`. It also defaults the VPS key path
  only when that file exists and the corresponding host is one of the known
  acceptance hosts. Explicit SSH env values are preserved.
- Code/spec:
  `docs/tech/2026-07-05-knife14bp-server-evidence-defaults-spec.md`,
  `docs/tech/2026-07-05-knife14bp-server-evidence-defaults-plan.md`, and
  `scripts/knife14b-usclient-tunnel-suite.sh`.
- Local gates passed: TDD self-test first failed on missing
  `apply_server_evidence_ssh_defaults`; after implementation,
  `scripts/knife14b-usclient-tunnel-suite.sh --self-test`,
  `scripts/knife14b-lowrtt-probe.sh --self-test`, `bash -n`, and
  `git diff --check` passed.
- Reusable rule: when a VPS acceptance run needs server-side attribution,
  make the known topology self-contained in the suite and test the defaults
  offline. Do not depend on manual SSH envs for every run.

## 2026-07-05 — Knife14bp VPS proves server evidence defaults and exposes no-data shape

- Stage: Knife14bp scoped VPS evidence acceptance for `d5d8542`.
- Bundle:
  `/tmp/mini_vpn/knife14bp_evidence_defaults_20260705/mvpn_knife14bp_evidence_defaults_d5d8542_usclient_suite_20260705_193204.tar.gz`
- Outcome: the suite resolved `.33/.77` SSH hosts and key paths without explicit
  SSH envs, and the bundle included both sing-box and iperf3 evidence. Script
  acceptance passed.
- Throughput result: reverse-first P1 failed at `280 Kbit/s` sender and
  `2.64 Kbit/s` receiver. Direct `.27 <-> .77` baselines were healthy around
  `279-280 Mbit/s` receiver.
- Key lesson: `.77` iperf3 journal showed the target sender itself at only
  `1.00 MBytes / 280 Kbit/s`; `.33` showed TUIC inbound/direct outbound and no
  `fail auth`. This is a no-data/delayed-stream shape, not hidden local loss of
  a high-rate sender.
- Reusable rule: after a run changes from tx-queue pressure to no-data with
  complete server evidence, do not patch behavior immediately. Repeat or add
  evidence-only instrumentation to separate VPS variance, stream-readiness, and
  local TCP window/ACK behavior.

## 2026-07-06 — Knife14bq repeat rejects stable no-data and exposes tail-collapse pressure

- Stage: Knife14bq same-code repeat for `d5d8542` with default server evidence
  and same-window `.33 <-> .77` iperf path checks.
- Bundle:
  `/tmp/mini_vpn/knife14bp_repeat_exitpath_20260706/mvpn_knife14bp_repeat_exitpath_d5d8542_usclient_suite_20260706_155856.tar.gz`
- Outcome: reverse-first P1 averaged `150/149 Mbit/s`, so the prior no-data
  shape was not stable and the code can leave the old `10-20 Mbit/s` band.
  The final six seconds still collapsed to about `15.7-16.8 Mbit/s`, so this
  is not stable throughput acceptance.
- Evidence: direct `.27 <-> .77` and `.33 <-> .77` baselines were healthy
  (`281-284 Mbit/s` receiver direction), `.33` had current TUIC inbound and
  direct outbound to `.77:5201` with no TUIC `fail auth`, `.77` sender matched
  the tunnel at `537 MBytes / 150 Mbit/s`, and QUIC loss/congestion remained
  `0/0`.
- mini_vpn signals: `tun_tx_dropped_delta=14804`,
  `downlink_backpressure=693/693`, `tun_flush_deferred=693`,
  `terminal_pending_reap=0`, `pending_at_close=0`, and
  `terminal_late_remote_payload=1474528B` across `835` events.
- Key lesson: Knife14 is no longer blocked on the no-data branch, but the
  acceptance blocker is still local downlink/TUN egress pressure and tail
  collapse, not sing-box auth, iperf3, stale pool slots, or the exit-target
  path.
- Reusable rule: after a repeat changes from no-data to high-average
  tail-collapse, record it as a separate failure shape and add correlation
  evidence before behavior changes. Do not claim final acceptance from average
  throughput while tail seconds and TUN drops remain unhealthy.

## 2026-07-06 — Knife14br makes tail collapse machine-readable

- Stage: Knife14br parser/report patch after the Knife14bq repeat.
- Outcome: `scripts/knife14b-lowrtt-probe.sh` now emits
  `iperf_interval_profile` and `throughput_shape` in every attribution summary,
  and the parent US-client suite surfaces those lines in probe summaries.
- TDD: the first self-test fixture reproduced the Knife14bq shape: `150/149
  Mbit/s` aggregate, high first 24 seconds, `~16 Mbit/s` final-six-second tail,
  local downlink/TUN pressure, no QUIC loss/congestion, and terminal-late
  accounting. It initially failed because no interval profile existed, then
  passed after the parser implementation.
- Local gates passed: `bash -n scripts/knife14b-lowrtt-probe.sh`,
  `bash scripts/knife14b-lowrtt-probe.sh --self-test`,
  `bash -n scripts/knife14b-usclient-tunnel-suite.sh`,
  `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`, a Knife14bq
  bundle parser smoke, and `git diff --check`.
- Evidence smoke: the extracted Knife14bq bundle now summarizes as
  `throughput_shape: shape=tail_collapse_local_pressure` and attribution adds
  `iperf_tail_collapse+tail_collapse_local_pressure`.
- Reusable rule: future high-average TCP reverse runs are not acceptable unless
  `throughput_shape` is `stable_high` or a deliberate acceptance note explains
  why the tail is irrelevant. A high aggregate with `tail_collapse_*` remains a
  Knife14 failure shape.

## 2026-07-06 — Knife14bs protects auto backpressure from stale env

- Stage: Knife14bs acceptance config-lifecycle patch after Knife14bq/Knife14br
  showed local downlink/TUN pressure and tail collapse.
- Outcome: the US-client suite now detects the old `524288/131072` downlink
  backpressure pair when TCP tx buffer is larger, blanks it back to binary
  `<auto>`, and reports `downlink_backpressure_auto_reset` so the run cannot
  silently exercise stale receive-window settings.
- TDD: suite self-test first failed on the missing normalization path, then
  passed with coverage for legacy-to-auto normalization, the explicit keep flag,
  and non-legacy explicit A/B preservation. Focused Rust tests still confirm
  binary auto defaults scale a 1 MiB tx buffer to high `1048576` / low
  `262144`.
- Reusable rule: when a suite relies on binary auto defaults, inherited `.env`
  values must be treated as part of the test surface and made visible in the
  report. Always verify both the suite config lines and the mini_vpn startup
  high/low log before interpreting throughput.

## 2026-07-06 — Knife14bs auto high/low is not sufficient

- Stage: Knife14bs VPS acceptance on commit `dd9c6ad`.
- Outcome: suite normalization worked on `.27`: the report showed
  `downlink_backpressure_auto_reset=legacy_512k_for_scaled_tx_buffer`, and
  mini_vpn started with `high=1048576B low=262144B`.
- Result: the clean reverse-first P1 did not pass. Throughput was
  `20.2/18.8 Mbit/s` with a `low_average` shape and repeated zero-throughput
  intervals, despite healthy `.27 <-> .77` and `.33 <-> .77` direct baselines.
- Signals: compared with Knife14bq, downlink pause/resume fell from `693/693`
  to `19/19` and TUN drops fell from `14804` to `586`, but throughput also fell
  from `149 Mbit/s` to `18.8 Mbit/s`. QUIC loss/congestion stayed zero, global
  rx pressure stayed zero, and terminal pending remained non-hidden
  (`terminal_pending_reap=0`, `pending_at_close=0`).
- Reusable rule: do not assume `TCP tx buffer size == safe downlink high
  watermark`. On this topology, a 1 MiB high watermark with TUN qlen `500`
  creates stop/go receive-window behavior. The next model must account for
  TUN/qdisc egress capacity and stream read-gap timing, not just smoltcp
  `send_queue` bytes.

## 2026-07-06 — Knife14bt restores conservative core auto backpressure

- Stage: Knife14bt core default patch after Knife14bs showed that binary auto
  high `1048576` / low `262144` regressed reverse-first P1 to a low-average
  `18.8 Mbit/s` receiver shape.
- Outcome: `client_tun.rs` no longer derives auto downlink backpressure from
  TCP tx buffer size. Empty/unset high/low env now resolve to the conservative
  product default `524288/131072`; explicit high/low env remains honored for
  deliberate A/B tests.
- TDD: the focused parser test first failed with the old `1048576/262144`
  auto value, then passed after the core default change.
- Local gates passed: focused backpressure parser test, focused downlink
  backpressure tests, focused TUN egress feedback tests, full `cargo test
  --lib`, and `git diff --check`.
- Reusable rule: when acceptance shows a larger receive-window/backpressure
  default reduces drops but creates stop/go low-average throughput, remove that
  default from the product path and keep larger watermarks explicit only. The
  remaining Knife14 branch is durable local TUN egress pressure and tail
  collapse, not script env normalization.

## 2026-07-06 — Knife14bt conservative auto still low-average on VPS

- Stage: Knife14bt scoped VPS acceptance on commit `b5752c4`.
- Outcome: the core-default fix was applied on `.27`: suite high/low were
  `<auto>` and mini_vpn started with `high=524288B low=131072B`. The first run
  hit the known one-shot TUIC auth-finish transient; manual no-secret compare
  then showed all auth fields matched, and the no-build retry connected.
- Result: reverse-first P1 still failed at `18.4/17.4 Mbit/s` with
  `throughput_shape=low_average`, repeated zero-throughput intervals, healthy
  direct baselines, and no QUIC loss/congestion/blocking.
- Signals: `downlink_backpressure=26/26`, `max_tx_queue_bytes=586083`,
  `tun_tx_dropped_delta=172`, `tun_egress_feedback=1/1`,
  `terminal_pending_reap=0`, `terminal_late_remote_payload=2890364B/145`,
  `send_slice_zero=0`, `send_slice_errors=0`, and `tun_flush_failures=0`.
- Likely root: the high watermark value is no longer the primary branch.
  smoltcp `send_queue` pressure crosses high and then appears as zero
  immediately after poll/flush, while the remote stream develops multi-second
  read/pending gaps. The gate needs durable local egress pressure, not another
  default high/low tweak.
- Reusable rule: after a low-average reverse run with `terminal_pending_reap=0`
  and QUIC deltas `0`, do not chase sing-box, iperf3, scripts, or stale pools.
  Add a core test that prevents immediate clean resume after a high
  `send_queue` flush before changing behavior.

## 2026-07-06 — Knife14bu makes local egress pressure durable

- Stage: Knife14bu core pressure-gate patch after Knife14bt showed
  high-to-zero `send_queue` transitions with low-average reverse throughput.
- Outcome: `client_tun.rs` now keeps a short bounded effective downlink
  pressure hold after raw pressure reaches high, so the main loop does not
  immediately resume remote reads just because smoltcp moved bytes into
  TUN/qdisc.
- TDD: the focused test first failed because no hold method/constant existed,
  then passed after adding a 25ms local egress pressure hold.
- Local gates passed: focused new test, `downlink_backpressure` tests,
  `tun_egress_feedback` tests, full `cargo test --lib`, and
  `git diff --check`.
- Reusable rule: after evidence shows smoltcp pressure drops to zero
  immediately after a high-watermark flush while remote stream gaps grow,
  preserve recent local egress pressure for a short bounded interval before
  declaring the downlink clean. Keep this separate from 1s sysfs TUN drop
  feedback so pre-drop gating and post-drop attribution do not erase each
  other.

## 2026-07-06 - Knife14bu rejects a simple 25ms pressure hold as the throughput fix

- Stage: Knife14bu scoped VPS acceptance for commit `fd3f2ed`.
- Result doc:
  `docs/tech/2026-07-06-knife14bu-durable-egress-pressure-results.md`
- Bundle:
  `/tmp/mini_vpn/knife14bu_durable_20260706/mvpn_knife14bu_durable_usclient_suite_20260706_190244.tar.gz`
- Outcome: reverse-first P1 still failed at `17.3/15.3 Mbit/s`, worse than
  Knife14bt's `18.4/17.4 Mbit/s`, while direct `.27 <-> .77` and
  `.33 <-> .77` baselines stayed around `277-320 Mbit/s`.
- Signals: `tun_tx_dropped_delta=0`, `terminal_pending_reap=0`,
  `terminal_late_remote_payload=0`, `send_slice_zero=0`,
  `send_slice_errors=0`, `tun_flush_failures=0`, and QUIC
  loss/congestion/blocking all stayed zero. Backpressure churn increased to
  `63/62`, `tun_flush_deferred=62`, and the data close line showed
  `tcp_state=CloseWait`, `may_recv=false`, `send_queue=524288`, and no app
  pending.
- Reusable rule: do not lengthen the pressure hold as the next guess. The hold
  removed the prior TUN drop signal without improving throughput, so the next
  code slice must target close/receive-window/egress-drain accounting for a
  send-capable `CloseWait` socket with queued downlink bytes.

## 2026-07-06 - Knife14bv makes close-time egress pressure visible

- Stage: Knife14bv-a behavior-neutral observability after Knife14bu showed
  `pending=0` but `send_queue=524288` at `CloseWait` close.
- Outcome: close logs now keep app-owned pending accounting and add separate
  `close_egress_class`, `close_egress_bytes`, and
  `close_egress_drain_candidate` fields. Downlink backpressure transition logs
  now include raw pressure and effective held pressure fields so a pressure
  hold can be distinguished from real smoltcp queue state.
- Parser update: low-RTT summaries now include `egress_at_close` and label
  `egress_close_drain_candidate` when a send-capable close still has queued
  egress bytes. This is acceptance visibility, not a script-side product fix.
- Cleanup: the unused `observe_pressure` wrapper is now test-only, so release
  builds are warning-clean again.
- Verification passed: focused close-egress and raw/effective pressure tests,
  downlink backpressure tests, TUN egress feedback tests, low-RTT probe
  self-test, US-client suite self-test, full `cargo test --lib`, release build,
  shell syntax checks, and `git diff --check`.
- Reusable rule: when close logs say `pending=0`, still classify smoltcp
  egress queue separately before claiming close/reap is clean. App pending and
  already-accepted-but-not-egressed bytes are different loss surfaces.

## 2026-07-06 - Knife14bv defers close while smoltcp egress drains

- Stage: Knife14bv-b bounded close-egress drain behavior after Knife14bv-a made
  send-capable close pressure visible.
- Outcome: core TCP lifecycle now treats a relay close with empty app pending
  but high smoltcp `send_queue` as a deferred close candidate. Both
  `uplink_channel_closed` and relay close events install `pending_relay_close`
  instead of immediately rearming, and the dirty loop keeps the handle live
  until the queue drops below the low watermark or the existing 5s grace
  expires.
- Guardrails: app-owned `downlink_pending` still uses its existing drain path;
  remote payload acceptance was not loosened; dead-slot reap preserves the
  deferred egress close only inside the bounded grace window. Script changes
  only surface the new `tcp-deferred-close-egress` evidence in acceptance logs.
- Verification passed: focused deferred-close egress tests, relay close epoch
  and pending-drain tests, reap predicate tests, rearm cleanup test, full
  `cargo test --lib`, release build, low-RTT and US-client suite self-tests,
  shell syntax checks, and `git diff --check`.
- Reusable rule: when `CloseWait can_send=true send_queue>=high pending=0`
  appears at close, do not rearm the listener immediately. Give already
  accepted downlink bytes a bounded local egress drain window, then let the
  existing reap/rearm machinery close the slot if progress stops.

## 2026-07-06 - Knife14bv rejects close-egress drain as the throughput root

- Stage: Knife14bv VPS acceptance for `8f68a89`.
- Result doc:
  `docs/tech/2026-07-06-knife14bv-close-egress-drain-results.md`
- Bundle:
  `/tmp/mini_vpn/knife14bv_close_egress_20260706/mvpn_knife14bv_close_egress_usclient_suite_20260706_194841.tar.gz`
- Outcome: reverse-first P1 failed at `0.315/0.113 Mbit/s` with
  `throughput_shape=no_data`, much worse than the prior 10-20 Mbit/s tier.
- Signals: `.27/.33/.77` preflight and direct baselines were healthy, no
  current-window `.33` TUIC auth failure appeared, QUIC client-side
  loss/congestion/blocking deltas stayed zero, TUN drops were zero, and local
  downlink backpressure never paused.
- Discriminator: no `tcp-deferred-close-egress` line appeared. The only
  close-egress bytes were `14824B` at terminal closed/no-send state, so the new
  send-capable close-drain branch was not the active bottleneck.
- New root branch: reverse data starves before close cleanup. TUIC stream 4
  closed with only `723424B` over `27` reads, `max_read_gap_ms=20567`, and the
  target iperf sender was stuck near `109KB` cwnd with almost all intervals at
  zero throughput.
- Reusable rule: after a no-data reverse run with no local pressure, no TUN
  drops, no QUIC client loss, and no deferred-close-egress candidate, do not
  keep tuning close/reap. Add diagnostics that separate TUIC server-to-client
  starvation from mini_vpn local TCP ACK/receive-window behavior.

## 2026-07-06 - Knife14bx splits tx_queue-only backpressure cadence

- Stage: Knife14bx core behavior patch after Knife14bw showed useful TUIC
  stream bytes, app pending `0`, clean QUIC/TUN/error signals, but low-average
  reverse throughput with downlink pause/resume churn around smoltcp
  `send_queue` pressure.
- Outcome: app-owned pending still uses the conservative high/low guard, while
  tx_queue-only pressure now gets bounded headroom before `global_rx` pauses.
  With the default `524288/131072` config, app pending pauses at `512 KiB`,
  tx_queue-only pauses at `896 KiB`, and tx_queue-only resumes at the soft
  `512 KiB` high watermark.
- TDD: the new tx_queue headroom test first failed because the old logic
  paused immediately at soft high. The implementation then added derived
  tx_queue pause/resume thresholds and a regression test proving soft
  tx_queue-only pressure no longer installs the 25ms egress hold.
- Guardrail: TUN drop feedback still records recent soft-high pressure for
  post-sample drop attribution, but remote-read pressure hold only engages for
  app pending high or tx_queue hard cap.
- Local gates passed: focused backpressure tests, focused TUN feedback tests,
  full `cargo test --lib`, harness test target, low-RTT and US-client suite
  self-tests, shell syntax checks, release build, clippy with harness, and
  `git diff --check`.
- Reusable rule: when app pending is empty and only smoltcp `send_queue`
  touches soft high, do not collapse it into the same hard stop as app-owned
  pending bytes. Preserve drop attribution separately from read-pause cadence.

## 2026-07-06 - Knife14bx partially improves but does not pass VPS

- Stage: Knife14bx scoped VPS acceptance for commit `b4fc5b4`.
- Result doc:
  `docs/tech/2026-07-06-knife14bx-tx-queue-cadence-results.md`
- Bundle:
  `/tmp/mini_vpn/knife14bx_tx_queue_20260706/mvpn_knife14bx_tx_queue_usclient_suite_20260706_214754.tar.gz`
- Outcome: reverse-first P1 improved from Knife14bw's `19.8/18.9 Mbit/s` to
  `27.4/26.4 Mbit/s`, and backpressure churn dropped from `51/51` to `28/28`,
  but the run remained `throughput_shape=low_average` with burst/idle seconds.
- Healthy exclusions: `.27 -> .77` and `.33 -> .77` direct baselines were
  around `253-319 Mbit/s`, `.33` had current-window TUIC inbound/direct
  outbound lines and no `fail auth`, QUIC loss/congestion/blocking stayed zero,
  TUN drops stayed zero, send-slice zero/errors stayed zero, and terminal
  pending reap stayed zero.
- New discriminator: `tun_flush_deferred` rose to `446` while app pending stayed
  `0`. The remote-read hard cap was `917504`, but the egress pacer still
  deferred immediate flushes at the soft high `524288`.
- Reusable rule: after adding tx_queue read headroom, align any tx_queue-only
  flush deferral with the same hard cap before adding broader architecture or
  service changes. Keep app pending and real TUN-drop feedback on their stricter
  paths.

## 2026-07-06 - Knife14by aligns tx_queue-only egress flush headroom

- Stage: Knife14by core behavior patch after Knife14bx showed partial
  improvement but left `tun_flush_deferred=446` with app pending `0`.
- Outcome: `DownlinkEgressPacer` now uses the Knife14bx tx_queue hard cap for
  no-pending immediate flush deferral. App pending behavior stays stricter:
  pending backlog can still force flush below soft high, but send_queue at soft
  high still defers when app-owned backlog remains.
- TDD: the new pacer test first failed because no-pending tx_queue pressure at
  soft high still deferred. The implementation then split the pacer threshold
  by `pending_bytes == 0` vs non-empty pending.
- Local gates passed: focused pacer/backpressure tests, full `cargo test
  --lib`, low-RTT and US-client suite self-tests, shell syntax checks, harness
  test target, release build, clippy with harness, and `git diff --check`.
- Reusable rule: after separating tx_queue-only remote-read backpressure from
  app pending, immediate flush cadence must use the same tx_queue-only hard cap
  or a second soft-high gate will continue producing burst/idle behavior.

## 2026-07-06 - Knife14by reduces flush deferral but exposes TUN edge

- Stage: Knife14by scoped VPS acceptance for commit `ee28d0f`.
- Result doc:
  `docs/tech/2026-07-06-knife14by-egress-flush-headroom-results.md`
- Bundle:
  `/tmp/mini_vpn/knife14by_flush_headroom_20260706/mvpn_knife14by_flush_headroom_usclient_suite_20260706_220250.tar.gz`
- Outcome: reverse-first P1 improved only slightly from Knife14bx's
  `27.4/26.4 Mbit/s` to `30.0/29.0 Mbit/s`, but stayed
  `throughput_shape=low_average` with repeated zero-throughput intervals.
- Positive signal: the intended metric moved strongly; `tun_flush_deferred`
  dropped from `446` to `25`, while app-owned pending, send-slice errors,
  TUN flush failures, QUIC loss/congestion/blocking, terminal pending reap,
  and pending-at-close stayed clean.
- New discriminator: opening no-pending immediate flush all the way to the
  tx_queue hard cap reintroduced small TUN egress loss:
  `tun_tx_dropped_delta=37`, with `max_tx_queue_bytes=975399` and
  `tx_queue_pause_high=917504`.
- Reusable rule: the old soft-high flush gate was real, but the hard-cap gate
  is too permissive for default TUN/qdisc capacity. The next patch should find
  a bounded middle or drop-aware egress-capacity guard, not return to scripts,
  stale pool, iperf3, sing-box, QUIC, or close/reap tuning.

## 2026-07-06 - Knife14bz splits flush headroom from read-pause headroom

- Stage: Knife14bz code patch after Knife14by showed hard-cap no-pending flush
  reduced `tun_flush_deferred` but reintroduced small TUN drops.
- Outcome: core downlink egress now has three explicit thresholds:
  app-owned pending stays strict at `high`, no-pending immediate flush defers at
  a bounded midpoint between `high` and the tx_queue hard cap, and remote-read
  pause still uses the tx_queue hard cap. With default settings that is
  `524288B`, `720896B`, and `917504B`.
- TDD: the new RED test first failed because `send_queue=130` still flushed
  under the old hard-cap rule. The implementation added
  `tx_queue_flush_threshold`, updated pacer expectations, and exposed
  `tx_queue_flush_high` in diagnostics/startup logs.
- Local gates passed: focused pacer/backpressure/TUN feedback tests, full
  `cargo test --lib`, low-RTT and US-client suite self-tests, shell syntax
  checks, harness tests, release build, clippy with harness, and
  `git diff --check`.
- Reusable rule: do not collapse all local egress thresholds into one number.
  App-pending safety, no-pending flush cadence, and remote-read pause capacity
  are separate controls and need separate diagnostics.

## 2026-07-06 - Knife14bz rejects static midpoint egress threshold

- Stage: Knife14bz scoped VPS acceptance for commit `3d88212`.
- Result doc:
  `docs/tech/2026-07-06-knife14bz-bounded-egress-flush-results.md`
- Bundle:
  `/tmp/mini_vpn/knife14bz_bounded_flush_20260706/mvpn_knife14bz_bounded_flush_usclient_suite_20260706_221933.tar.gz`
- Outcome: reverse-first P1 regressed to `20.7/19.5 Mbit/s` and stayed
  `throughput_shape=low_average`.
- Positive signal: startup confirmed the new split thresholds were active:
  app pending high `524288B`, no-pending flush high `720896B`, and tx_queue
  read pause high `917504B`.
- Rejecting signal: even with the bounded flush threshold, tx_queue pressure
  still reached `983022B`, `tun_flush_deferred` rose to `162`, and
  `tun_tx_dropped_delta` rose to `340`.
- Clean exclusions: direct `.27/.33/.77` baselines stayed healthy, `.33` had
  current-window TUIC inbound/direct outbound evidence and no TUIC `fail auth`,
  QUIC loss/congestion/blocking deltas stayed zero, app pending stayed zero,
  send-slice errors stayed zero, and pending/close/reap accounting did not hide
  a pending buffer.
- Reusable rule: after a static midpoint threshold is disproved, stop tuning
  soft/mid/hard numbers. The next core fix should bound remote payload
  acceptance by remaining egress headroom before `send_slice` can push the
  local TUN/qdisc path past clean capacity.

## 2026-07-06 - Knife14ca bounds remote accept by egress headroom

- Stage: Knife14ca code patch after Knife14bz proved static midpoint threshold
  tuning still allowed tx_queue overshoot and TUN drops.
- Outcome: `flush_downlink` now caps each `send_slice` attempt by configured
  per-flush budget, smoltcp send capacity, and remaining tx_queue clean egress
  headroom. Bytes that do not fit remain in `downlink_pending` for dirty/timer
  retry instead of being dropped or pushed past the local TUN/qdisc clean
  capacity.
- Diagnostics: `tcp-downlink-flush` and `tcp-handle-close` now include
  `headroom_limited_calls` and `headroom_deferred_bytes`; the low-RTT parser
  summary exposes these fields as `headroom_limited` and
  `headroom_deferred_bytes`.
- TDD: the RED test first failed because
  `bounded_downlink_flush_len_for_window` did not exist. The GREEN patch added
  the headroom-aware limit, wired it through both remote-payload and
  dirty/timer pending flush paths, and added diagnostic coverage.
- Local gates passed: focused headroom/diag/pacer/backpressure tests, full
  `cargo test --lib` (`299` passed), low-RTT and US-client suite self-tests,
  shell syntax checks, `git diff --check`, release build, harness tests
  (`10` passed, `4` ignored), and clippy with harness.
- Reusable rule: when post-send egress pacing is too late, move the guard to
  the pre-`send_slice` accept boundary and keep overflow as pending data. This
  is an algorithmic capacity-match change, not another threshold-only tweak.

## 2026-07-06 - Knife14ca rejects fixed pre-send headroom caps

- Stage: Knife14ca scoped VPS acceptance for commit `82003b8`.
- Result doc:
  `docs/tech/2026-07-06-knife14ca-headroom-bounded-accept-results.md`
- Bundle:
  `/tmp/mini_vpn/knife14ca_headroom_accept_20260706/mvpn_knife14ca_headroom_accept_usclient_suite_20260706_224238.tar.gz`
- Outcome: reverse-first P1 regressed to `16.1/14.8 Mbit/s`, with burst/idle
  throughput and `throughput_shape=low_average`.
- Positive signal: the new cap worked mechanically. `send_queue_max` stayed at
  `720896B` instead of overshooting toward the hard-pause region, and clean
  P1 summary still had no QUIC loss/congestion/blocking, no send-slice errors,
  and no TUN flush failures.
- Rejecting signal: the cap created a receive-window stall. The final close
  line showed `pending=566509`, `may_recv_false=11897`,
  `headroom_limited_calls=12973`, and
  `headroom_deferred_bytes=3317103134` while the socket remained
  `CloseWait` and send-capable.
- Additional discriminator: final suite logs showed TUN egress feedback
  `drop_delta_total=2691` despite `send_queue_max=720896`, so a static
  occupancy cap neither preserved throughput nor fully prevented late local
  egress loss.
- Reusable rule: do not continue soft/mid/hard threshold tuning. The next
  design must clock downlink acceptance from actual local egress progress and
  must parse final lifecycle/drop snapshots before declaring pending/close/reap
  accounting clean.

## 2026-07-06 - Knife14cb adds suite-level final lifecycle summaries

- Stage: Knife14cb parser/reporting patch after Knife14ca showed low-RTT
  attribution summaries can miss post-probe close/drop lines.
- Outcome: the US-client tunnel suite now emits final lifecycle summaries for
  both the whole suite and the post-reverse-first tail when that probe ran.
- TDD: the RED self-test reproduced Knife14ca-style post-summary evidence:
  `pending=566509`, `close_pending_class=active_send_capable`,
  `headroom_deferred_bytes=3317103134`, and cumulative TUN feedback
  `drop_delta_total=2691`. The GREEN parser reports those fields as
  `final_pending_at_close`, `final_downlink_flush`, and
  `final_tun_egress_feedback`.
- Local gates passed: `bash scripts/knife14b-usclient-tunnel-suite.sh
  --self-test`, `bash scripts/knife14b-lowrtt-probe.sh --self-test`, shell
  syntax checks for both scripts, and `git diff --check`.
- Reusable rule: final acceptance must include lifecycle/drop events emitted
  after the per-probe attribution block. A clean low-RTT summary is not enough
  when final snapshots later reveal pending bytes or TUN egress drops.

## 2026-07-06 - Knife14cb clocks downlink accept from egress progress

- Stage: Knife14cb core behavior patch after fixed pre-send headroom caps
  regressed throughput by creating sticky pending and receive-window stalls.
- Outcome: each TCP flow now has a `DownlinkEgressClock`. Observed decreases
  in smoltcp `send_queue` grant bounded one-shot drain credit, so downlink can
  accept above the clean flush threshold only when local egress has actually
  progressed. Planned accepts remain capped by pending length, per-flush
  budget, smoltcp send capacity, clean headroom plus credit, and the hard
  tx_queue pause threshold.
- TDD: the RED test first failed because `DownlinkEgressClock` and
  `bounded_downlink_flush_limit_for_window_with_clock` did not exist. The
  GREEN patch added the clock, reset it on rearm, and added coverage that
  partial `send_slice` acceptance consumes only actually accepted credit.
- Diagnostics: `tcp-downlink-flush` and `tcp-handle-close` now include
  `drain_credit_granted_bytes`, `drain_credit_planned_bytes`, and
  `drain_credit_used_bytes`; both low-RTT and suite final parsers expose those
  fields.
- Local gates passed so far: focused egress-clock tests, full
  `cargo test --lib` (`301` passed), low-RTT and US-client suite self-tests,
  shell syntax checks, release build, harness tests, clippy with harness, and
  `git diff --check`.
- Reusable rule: do not use queue occupancy alone as a downlink accept clock.
  Allow extra accept above the clean threshold only when observed local egress
  drain grants bounded credit, and consume credit based on actual accepted
  bytes rather than the planned flush length.

## 2026-07-06 - Knife14cb restores reverse throughput but leaves TUN egress edge pressure

- Stage: VPS acceptance for commit `5bf60d9` on `.27 -> .33 -> .77`.
- Result doc:
  `docs/tech/2026-07-06-knife14cb-egress-progress-clocked-accept-results.md`
- Bundle:
  `/tmp/mini_vpn/knife14cb_egress_clock_20260706_1552/mvpn_knife14cb_egress_clock_usclient_suite_20260706_235136.tar.gz`
- Positive result: reverse-first P1 reached `152/152 Mbit/s`, escaping the
  rejected `10-20 Mbit/s` band and exceeding the Knife14by `29.0 Mbit/s`
  reference. QUIC loss/congestion/blocking stayed zero, `send_slice_zero=0`,
  `send_slice_errors=0`, and `tun_flush_failures=0`.
- Lifecycle result: final close/reap accounting was clean. Final summaries
  showed `final_pending_at_close: events=0 bytes=0` and
  `final_terminal_pending_reap: events=0 bytes=0`; the main reverse flow
  closed with `pending=0`, `close_pending_class=none`, and
  `terminal_pending_reap_bytes=0`.
- Remaining risk: local TUN egress still dropped once
  (`tun_tx_dropped_delta=712`, `drop_delta_total=712`) while pressure reached
  `917504B`, and iperf still showed a two-second zero-throughput gap at
  `23-25s`.
- `.33` note: current-window sing-box evidence did not show TUIC `fail auth`;
  clocks were synchronized and TUIC inbound entries matched the `.27` run.
- Reusable rule: progress-clocked accept is the right direction, but credit
  spending must not repeatedly push the local send queue to the hard pause
  edge. The next patch should smooth or make credit debt drop-aware rather
  than only moving thresholds.

## 2026-07-06 - Knife14cc reserves a derived hard-edge guard for drain credit

- Stage: Knife14cc behavior patch after Knife14cb restored `152/152 Mbit/s`
  but still hit `tun_tx_dropped_delta=712` at the hard tx_queue pause edge.
- Design: progress-clocked accept remains enabled, but drain credit now spends
  only up to a derived credit ceiling below hard pause. The guard is based on
  the clean-to-hard credit span, has no environment knob, and is clamped so it
  never reduces clean-headroom accepts when the span is tiny.
- TDD: the RED test first showed current code returned `60` bytes and planned
  `send_queue=160` at the hard edge. The GREEN path returns `57` with a guard
  of `3` for the small test config; the default production guard is `24576B`
  below the hard edge.
- Diagnostics: `tcp-downlink-flush` and `tcp-handle-close` now emit
  `hard_edge_guard_bytes`, `hard_edge_guard_limited_calls`, and
  `hard_edge_guard_deferred_bytes`; low-RTT and suite final parsers expose
  those fields.
- Local gates passed: focused guard/egress-clock tests, full
  `cargo test --lib` (`303` passed), parser self-tests, shell syntax checks,
  `git diff --check`, release build, harness tests (`10` passed, `4`
  ignored), and clippy with harness.
- Reusable rule: after a throughput-restoring credit algorithm, protect the
  local egress hard edge with a derived guard and make that guard visible in
  final lifecycle summaries. Average throughput alone is not acceptance.

## 2026-07-06 - Knife14cd isolates concurrent TUIC TCP streams by default

- Stage: Knife14cd follow-up after Knife14cc pool=1 VPS reruns produced
  repeatable no-data reverse-first windows on commit `7d1f47f`.
- Result docs:
  `docs/tech/2026-07-06-knife14cc-hard-edge-credit-guard-results.md`,
  `docs/tech/2026-07-06-knife14cd-tcp-pool-isolation-spec.md`, and
  `docs/tech/2026-07-06-knife14cd-tcp-pool-isolation-plan.md`.
- VPS discriminator: two pool=1 runs had healthy direct baselines, zero TUN
  drops, zero QUIC loss/congestion/blocking, clean pending/reap accounting, and
  `tcp_pool ... conns=0`; the repeat run showed data stream
  `first_rx_ms=17880` and only about `258 KiB` received.
- A/B signal: rerunning the same binary with `MINI_VPN_TUIC_TCP_POOL=2`
  changed the stream set to `conns=0,1`, data stream `first_rx_ms=3`, and about
  `101 MB` received. Reverse throughput improved to `25.9 Mbit/s`, proving
  pool isolation removes the no-data branch.
- Remaining root after pool=2: local egress/drop/backlog became visible again
  (`tun_tx_dropped_delta=541`, `pending_at_close=553066`,
  `egress_at_close=892928`, `hard_edge_guard_limited=1701`). This is not the
  final Knife14 fix.
- Code direction: product and US-client suite defaults move to pool=2, while
  explicit `MINI_VPN_TUIC_TCP_POOL=1` remains valid for constrained exits and
  single-connection A/B.
- Reusable rule: when reverse-first is no-data with clean local egress and both
  iperf control/data streams on one TUIC connection, test pool isolation before
  editing local drain/close/egress code. Once pool=2 restores immediate data,
  stop increasing pool size and return to the newly visible local limiter.

## 2026-07-06 - Knife14cd default pool=2 restores high throughput but not clean egress

- Code commits: `3135d68` and `f219044`.
- Result doc:
  `docs/tech/2026-07-06-knife14cd-default-pool2-results.md`
- Bundle:
  `/tmp/mini_vpn/knife14cd_default_pool2_20260707_0028/mvpn_knife14cd_default_pool2_usclient_suite_20260707_002800.tar.gz`
- Outcome: the default suite/product pool=2 run reached `180/179 Mbit/s` and
  `throughput_shape=stable_high`, with `tcp_pool conns=0,1` and data stream
  `first_rx_ms=3`. This closes the pool=1 no-data branch under default config.
- Clean surfaces: direct `.27/.33 -> .77` baselines were healthy, current
  sing-box evidence had TUIC inbound/direct outbound entries with no current
  `fail auth`, QUIC loss/congestion/blocking deltas were zero, send-slice
  errors were zero, TUN flush failures were zero, and terminal pending reap
  stayed zero.
- Remaining blocker: local egress/drop pressure was worse at restored
  throughput (`tun_tx_dropped_delta=6754`, `drop_events=8`,
  `max_delta=1723`, `downlink_backpressure pause/resume=317/317`,
  `final_egress_at_close=892928`, and final send-capable pending `7680B`).
- Reusable rule: high average throughput is not Knife14 acceptance if final
  TUN drop and egress-at-close signals remain. After pool=2 default, the next
  patch should make egress drain credit drop-aware instead of changing pool
  size or static thresholds.

## 2026-07-06 - Knife14cj proves ACK drain timing but exposes undersized packet budget

- Code commit: `567d25b`.
- Result doc:
  `docs/tech/2026-07-06-knife14cj-pre-payload-pressure-ack-drain-results.md`
- VPS retry bundle:
  `/tmp/mini_vpn/knife14cj_pre_payload_ack_drain_retry_20260707_0231/mvpn_knife14cj_pre_payload_ack_drain_retry_usclient_suite_20260707_023111.tar.gz`
- Outcome: reverse-first P1 stayed low at `15.5/14.5 Mbit/s` even though
  `.27/.33 -> .77` baselines were healthy and QUIC loss/congestion/blocking
  deltas stayed zero.
- Clean lifecycle surfaces: `terminal_pending_reap=0`, `pending_at_close=0`,
  `egress_at_close=0`, `terminal_late_remote_payload=0`, send-slice errors
  were zero, and TUN flush failures were zero.
- Failure surface: local TUN egress remained the blocker with
  `tun_tx_dropped_delta=7794`, runtime drop events `5`, feedback pause/resume
  `5/4`, and `send_queue_max=892928`.
- Knife14cj-specific signal: pre-payload and timer-maintenance drain engaged
  (`pre_payload_attempts=41`, `maintenance_attempts=11`), but aggregate drain
  was always exhausted (`attempts=252`, `packets=5292`,
  `budget_exhausted=252`, `would_block=0`).
- Reusable rule: pressure-gated TUN RX drain is the right timing, but deriving
  packet budget from `guard_bytes / MTU` underestimates ACK/window traffic. The
  next budget should be ACK-sized and bounded, not a static env override.

## 2026-07-06 - Knife14cm installs pressure debt early but does not couple receive

- Code commit: `e8f0a50`.
- Result doc:
  `docs/tech/2026-07-06-knife14cm-proactive-pressure-credit-edge-results.md`
- Valid VPS bundle:
  `/tmp/mini_vpn/knife14cm_pressure_credit_edge_valid_20260706_1916/mvpn_knife14cm_pressure_credit_edge_valid_usclient_suite_20260707_031619.tar.gz`
- Outcome: local gates passed, and the valid `.27` run exercised the intended
  path, but reverse-first P1 still failed at `20.4/19.1 Mbit/s`.
- Positive signal: pressure debt now appears before the pause edge:
  `tcp-egress-credit-debt reason=pressure_credit_edge installed_bytes=24576`.
- Failure signal: after that debt, remote payload was still accepted while the
  local send queue stayed pinned at `892928B`; pending grew to `541802B`, TUN
  drops reached `4056`, and `pressure_credit_debt_paid_bytes=0`.
- Discriminator: `.33` remained healthy with no current TUIC `fail auth`;
  direct baselines were healthy and QUIC loss/congestion/blocking stayed zero.
- Reusable rule: debt installation is only accounting unless it feeds the
  receive-window decision. The next patch should make active drop/pressure
  debt pause or gate remote receive until observed local egress progress pays
  the debt or pressure recovers.

## 2026-07-06 - Knife14cn bounds pending but exposes pre-pressure progress gaps

- Code commit: `b6981ad`.
- Result doc:
  `docs/tech/2026-07-06-knife14cn-debt-coupled-receive-gate-results.md`
- Valid VPS bundle:
  `/tmp/mini_vpn/knife14cn_debt_receive_gate_env_20260706_1930/mvpn_knife14c_usclient_suite_20260707_032833.tar.gz`
- Outcome: local gates passed and VPS proved the active debt receive gate:
  `tcp-egress-credit-debt reason=pressure_credit_edge installed_bytes=24576`
  was followed by `tcp-downlink-backpressure paused=true` at
  `max_pending=2035 max_tx_queue=892928`.
- Improvement: final useful pending dropped from Knife14cm's `541802B` to
  `2035B`, and close/reap accounting stayed clean (`pending_at_close=0`,
  `egress_at_close=0`, `terminal_pending_reap=0`).
- Remaining failure: reverse-first P1 stayed low at `20.5/19.2 Mbit/s`; TUN
  egress still dropped (`tun_tx_dropped_delta=3524` parser,
  `drop_delta_total=2714` runtime), and `pressure_credit_debt_paid_bytes=0`.
- Reusable rule: debt-coupled receive gating fixes pending growth, but
  throughput now needs a pre-pressure progress-cadence repair. Do not keep
  adding debt or moving thresholds; test bounded TUN RX ACK/window drain while
  reverse data is active before local egress reaches the credit edge.

## 2026-07-07 - Knife14dq closes lifecycle attribution but exposes TUIC read cadence

- Result doc:
  `docs/tech/2026-07-07-knife14dq-tuic-stream-read-gap-results.md`
- Main bundles:
  `/tmp/mini_vpn/knife14do_d85_clean_baseline_p1_30_20260707_101108/mvpn_knife14do_d85_clean_baseline_p1_30_usclient_suite_20260707_101108.tar.gz`,
  `/tmp/mini_vpn/knife14dp_d85_after_singbox_restart_p1_30/mvpn_knife14dp_d85_after_singbox_restart_p1_30_usclient_suite_20260707_101638.tar.gz`,
  and
  `/tmp/mini_vpn/knife14dq_current_after_singbox_restart_pool2_p1_30/mvpn_knife14dq_current_after_singbox_restart_pool2_p1_30_usclient_suite_20260707_101838.tar.gz`.
- Outcome: clean d85 before restart reproduced near no-data, d85 after
  sing-box restart improved only to `20.8/19.9 Mbit/s`, and current code with
  pool=2 reached `30.9/29.9 Mbit/s` but remained low average.
- Closed branch: current code kept the failing reverse-first window clean for
  lifecycle accounting: `pending_at_close=0`, `egress_at_close=0`,
  `terminal_pending_reap=0`, no terminal late remote payload, no send-slice
  errors, no TUN flush failures, no TUN drops, no local/global RX pressure, and
  no QUIC loss/congestion/blocking.
- Remaining signal: the data stream still had second-scale cadence gaps
  (`data_pending_gap_max_ms=3413`, `max_read_gap_ms=3414`) while direct
  `.27/.33 <-> .77` baselines were healthy and `.33` had no current TUIC
  `fail auth`.
- Reusable rule: when close/pending/reap/TUN/QUIC surfaces are clean but
  interval throughput repeatedly hits zero, stop tuning thresholds and inspect
  the relay/TUIC stream read-wakeup path. In particular, do not manually poll a
  live async stream with a no-op waker unless a deterministic test proves the
  real task waker cannot be clobbered.

## 2026-07-07 - Knife14dx caps relay batches but proves mpsc pressure is not enough

- Result doc:
  `docs/tech/2026-07-07-knife14dx-relay-credit-results.md`
- VPS bundle:
  `/tmp/mini_vpn/knife14dx_relay_credit_p1_30/mvpn_knife14dx_relay_credit_p1_30_usclient_suite_20260707_125810.tar.gz`
- Outcome: local tests and `.27` focused tests passed. The clean
  reverse-first P1 still failed at `25.7/24.8 Mbit/s`.
- Improvement: lifecycle accounting stayed clean in the failing window:
  `terminal_pending_reap=0`, `pending_at_close=0`, `egress_at_close=0`, and
  `terminal_late_remote_payload=0`. The relay batch cap held at
  `remote_batch_bytes_max=524288`.
- Discriminator: the relay -> main-loop queue was not the bottleneck:
  `global_rx_queue_used_max=106/1024`, `remote_batch_limited=0`, and
  `remote_batch_limit_bytes_min=524288`.
- Remaining root: local socket/TUN egress pressure returned during the live
  window: `tun_tx_dropped_delta=591`, `send_queue_max=892928`,
  `headroom_limited_calls=336`, and `headroom_deferred_bytes=28883630`, while
  QUIC loss/congestion/blocking remained zero.
- Reusable rule: relay read-side credit must follow per-flow local egress
  state, not only bounded-channel occupancy. The next patch should add an
  explicit main-loop -> relay read-credit signal based on socket send queue /
  TUN feedback, instead of changing pool size, sing-box, iperf3, or static
  threshold values.

## 2026-07-07 - Knife14dy proves read-credit wiring but rejects post-accept policy

- Result doc:
  `docs/tech/2026-07-07-knife14dy-read-credit-results.md`
- VPS bundle:
  `/tmp/mini_vpn/knife14dy_read_credit_p1_30/mvpn_knife14dy_read_credit_p1_30_usclient_suite_20260707_131037.tar.gz`
- Outcome: local and `.27` focused tests passed, but clean reverse-first P1
  remained low at `22.8/21.6 Mbit/s`.
- What worked: the per-flow read-credit channel was live. The data stream
  recorded `read_credit_updates=86`, `read_credit_limit_bytes_min=65536`, and
  `remote_batch_limited=6`.
- What stayed clean: close/lifecycle accounting still reported
  `terminal_pending_reap=0`, `pending_at_close=0`, `egress_at_close=0`, and
  no terminal late remote payload; QUIC loss/congestion/blocking stayed clean.
- Remaining root: the first read-credit policy reacted after payload acceptance,
  so a large batch could still consume stale drain-credit and drive local
  egress to `send_queue_max=892928` with TUN drops
  (`tun_tx_dropped_delta=2973` parser, `2163` runtime).
- Reusable rule: read-credit and flush debt must share projected pressure:
  `send_queue + pending + incoming`. Publish projected credit before handling a
  remote payload and install projected pressure debt before `flush_downlink`
  can spend old smoltcp drain-credit.

## 2026-07-07 - Knife14dz separates projected credit from QUIC receive progress

- Result doc:
  `docs/tech/2026-07-07-knife14dz-projected-credit-results.md`
- VPS bundle:
  `/tmp/mini_vpn/mvpn_knife14dz_projected_credit_p1_30_usclient_suite_20260707_132432.tar.gz`
- Outcome: projected credit/debt engaged, but reverse-first P1 regressed to
  `6.05 Mbit/s` receiver and timed out.
- Closed surface: pending/close/reap accounting stayed clean, with
  `terminal_pending_reap=0`, `pending_at_close=0`, and `egress_at_close=0`.
- Rejected policy: including local pressure debt in the relay read hard pause
  protected egress too aggressively and introduced QUIC stream receive
  blocking (`max_rx_blocked_stream_delta=1`).
- Reusable rule: local flush pressure and QUIC stream receive-window progress
  are separate control loops. Pressure debt may gate smoltcp/TUN flush credit,
  but relay reads should only hard-pause when receive staging is full or TUN
  feedback is paused.

## 2026-07-07 - Knife14ea proves staging drain but exposes ACK/window cadence

- Result doc:
  `docs/tech/2026-07-07-knife14ea-staging-drain-results.md`
- VPS bundle:
  `/tmp/mini_vpn/mvpn_knife14ea_staging_drain_p1_30_usclient_suite_20260707_133922.tar.gz`
- Outcome: removing pressure debt from relay-read hard pause improved
  reverse-first P1 from `6.05` to `26.3 Mbit/s` and cleared QUIC
  receive-blocking.
- Clean surfaces: TUN drops, QUIC loss/congestion/blocking, terminal pending
  reap, and pending at close stayed zero.
- Remaining root: second-scale data stream read/pending gaps persisted with
  repeated iperf zero windows; relay gap hints existed but used the smaller
  active-flow ACK drain budget.
- Reusable rule: when the local egress surfaces are clean but cadence gaps
  remain, treat relay read gaps as ACK/window starvation evidence and let that
  path use pressure-sized drain budget under the same headroom safety curve.

## 2026-07-07 - Knife14eb validates stronger gap ACK drain but needs follow-up

- Result doc:
  `docs/tech/2026-07-07-knife14eb-gap-hint-pressure-drain-results.md`
- VPS bundle:
  `/tmp/mini_vpn/mvpn_knife14eb_gap_hint_pressure_drain_p1_30_usclient_suite_20260707_134432.tar.gz`
- Outcome: relay-gap pressure ACK drain improved reverse-first P1 to
  `38.3/37.4 Mbit/s` while keeping TUN drops, QUIC blocked/loss/congestion,
  headroom debt, pending-at-close, and terminal pending reap clean.
- Remaining root: the data stream still had `data_read_gap_max_ms=3593` and
  `data_pending_gap_max_ms=3437`, with `tun_rx_drain budget_exhausted=180`.
- `.33` discriminator: current-window logs showed TUIC inbound/direct outbound
  for `.27 -> .77`, no TUIC `fail auth`, active sing-box, and synchronized
  time. VLESS REALITY invalid-connection logs were unrelated scanner noise.
- Reusable rule: do not raise the static packet budget blindly. When a valid
  relay-gap ACK drain exhausts its budget, arm a bounded follow-up drain that
  repeats until TUN RX would-blocks or the egress headroom curve suppresses it.

## 2026-07-07 - Knife14ec proves relay-gap follow-up is the wrong trigger

- Result doc:
  `docs/tech/2026-07-07-knife14ec-gap-followup-results.md`
- VPS bundle:
  `/tmp/mini_vpn/mvpn_knife14ec_gap_followup_p1_30_usclient_suite_20260707_135859.tar.gz`
- Outcome: local tests and `.27` focused tests passed, but reverse-first P1
  fell to `23.2/21.5 Mbit/s`.
- Useful negative signal: `relay_gap_hint_followup_attempts=0`, so the new
  follow-up path did not trigger in the failing VPS window.
- Remaining root: `budget_exhausted=210` occurred with many
  `remote_payload_deferred_attempts=812`, and local pressure returned
  (`send_queue_max=724376`, `headroom_limited=706`,
  `pressure_credit_debt_bytes=196608`) without TUN drops or QUIC blocking.
- Reusable rule: a code-level algorithm can be correct but attached to the
  wrong event. For this branch, adaptive escalation belongs to exhausted
  deferred ACK drains after remote payloads, not only to relay-gap hints.

## 2026-07-07 - Knife14en rejects QUIC MTU black-hole as the main root

- Result doc:
  `docs/tech/2026-07-07-knife14en-quic-safe1200-results.md`
- VPS bundle:
  `/tmp/mini_vpn/knife14en_quic_safe1200_tail12/mvpn_knife14en_quic_safe1200_tail12_usclient_suite_20260707_160539.tar.gz`
- Code outcome: added `MtuPolicy` with `default` and `safe1200`, wired
  `MINI_VPN_TUIC_MTU_MODE` / `MINI_VPN_TUIC_MTU_POLICY`, and added per-stream
  transport delivery counters to TUIC TCP pending diagnostics.
- Local and `.27` focused gates passed; release build and script self-tests
  passed.
- VPS outcome: `safe1200` applied (`dg_max=Some(1166)`, PLPMTUD disabled),
  but reverse-first P1 remained low at `24.3/23.1 Mbit/s`.
- Key discriminator: QUIC transport delivery remained active while pending
  gaps occurred. The data connection ended with `frames(rx_stream=68905)`,
  `udp_rx=104392/148771086B`, and `plpmtud(sent=0,lost=0,black_holes=0)`.
- Remaining root: the failure shifted back to local downlink pressure:
  `downlink_backpressure pause/resume=4/3`, `max_pending=725572`,
  `send_queue_max=556664`, `headroom_limited=15691`, and
  `headroom_deferred_bytes=4018847472`, while TUN drops, send-slice errors,
  pending-at-close, egress-at-close, terminal pending reap, QUIC loss,
  congestion, and blocked frames stayed clean.
- Reusable rule: do not keep tuning QUIC MTU/PLPMTUD for Knife14 unless new
  evidence contradicts this run. The next patch should redesign local
  downlink credit so read-credit, flush budget, and headroom debt are coupled
  around observed egress progress instead of passively accumulating headroom
  deferrals.

## 2026-07-07 - Knife14eo reaches 95% with per-flow local downlink credit control

- Result doc:
  `docs/tech/2026-07-07-knife14eo-local-downlink-credit-controller-results.md`
- Outcome: Knife14eo code, deterministic local coverage, local quality gates,
  and `.27` focused gates passed. Full VPS reverse-first acceptance has not
  been run yet; this is the requested 95% stop point.
- Code result: each `SocketCtx` now owns a `DownlinkCreditController` that
  couples relay read credit, flush budget, bounded staging, headroom deferral,
  and observed smoltcp egress progress. Headroom deferral now shrinks future
  read/flush work instead of remaining passive accounting; observed egress
  progress grows credit additively.
- Clean local gates: focused controller/read-credit/pressure-credit/deferred
  ACK/relay burst tests, full `cargo test --lib`, harness tests, clippy with
  harness, script self-tests, release build, and `git diff --check`.
- Remote focused gates on `.27`: controller/read-credit/pressure-credit tests
  and release build passed after rsync excluding `.env`, `.git`, and `target`,
  followed by `touch` on edited source files.
- Reusable rule: local downlink pressure should be treated as a per-flow
  control loop. Keep hard relay-read pauses for receive-window high water or
  TUN feedback pause; use headroom deferral to shrink bounded staging and
  flush/read budgets, then reopen credit only from observed egress progress.

## 2026-07-07 - Knife14eo 97% VPS run proves the controller direction but not acceptance

- Result doc:
  `docs/tech/2026-07-07-knife14eo-local-downlink-credit-controller-results.md`
- VPS bundle:
  `/tmp/mini_vpn/knife14eo_credit_controller_p1_30/mvpn_knife14eo_credit_controller_p1_30_usclient_suite_20260707_171633.tar.gz`
- Outcome: the scoped `.27 -> .33 -> .77` safe1200 reverse-first P1 ran and
  completed, but failed acceptance at `32.3/30.8 Mbit/s` and
  `tun_tx_dropped_delta=117`.
- Useful positive signal: QUIC stayed clean (`lost/congestion/blocked=0`,
  PLPMTUD disabled and clean), and close/lifecycle stayed clean
  (`terminal_pending_reap=0`, `pending_at_close=0`, `egress_at_close=0`).
- Useful controller signal: the new controller reduced the prior passive
  pressure spiral (`may_recv_false=2129` from `14700`,
  `headroom_deferred_bytes=50881438` from `4018847472`), so the design
  direction is not rejected.
- Remaining root: local projected payload debt still lets
  `send_queue + pending` cross the TUN egress edge (`send_queue_max=553848`,
  `pending_high=576836`) before hard clamp, causing a drop-edge feedback pause
  and bursty low-average throughput.
- Reusable rule: the next Knife14 controller change must be predictive at the
  local egress edge. Cap projected payload credit before `tx_queue_pause_high`
  and make fresh pressure/drop debt shrink read credit and flush budget for the
  next control epoch, while preserving bounded ACK/window drain.

## 2026-07-07 - Knife14ep clears the drop edge but exposes ACK/window cadence starvation

- Result doc:
  `docs/tech/2026-07-07-knife14ep-predictive-drop-edge-controller-results.md`
- VPS bundle:
  `/tmp/mini_vpn/knife14ep_predictive_drop_edge_p1_30/mvpn_knife14ep_predictive_drop_edge_p1_30_usclient_suite_20260707_173838.tar.gz`
- Outcome: local TDD, full local gates, `.27` focused gates, and release build
  passed. The scoped safe1200 reverse-first P1 completed but failed acceptance
  at `30.3/28.7 Mbit/s`.
- Useful positive signal: the predictive sub-floor controller removed the TUN
  drop edge (`tun_tx_dropped_delta=0`) while keeping QUIC
  loss/congestion/blocking and close/reap counters clean
  (`terminal_pending_reap=0`, `pending_at_close=0`, `egress_at_close=0`).
- Useful negative signal: the fixed tiny ACK/window floor over-throttled the
  receive cadence. The run still had bursty zero-throughput iperf windows,
  `may_recv_false=8335`, `headroom_deferred_bytes=20226670`,
  `send_queue_max=557240`, and data stream gaps around `3.47s`.
- Reusable rule: after the drop edge is clean, do not keep shrinking relay
  read credit. The next controller should make the ACK/window drain floor
  adaptive: stay tiny only under no-progress edge pressure, then grow to a
  bounded multi-MTU drain when egress progress is observed and TUN drops remain
  zero.

## 2026-07-07 - Knife14eq exposes a no-pressure reverse no-data branch

- Result doc:
  `docs/tech/2026-07-07-knife14eq-adaptive-ack-drain-results.md`
- VPS bundle:
  `/tmp/mini_vpn/knife14eq_adaptive_ack_drain_p1_30b/mvpn_knife14eq_adaptive_ack_drain_p1_30b_usclient_suite_20260707_180324.tar.gz`
- Outcome: adaptive ACK/window drain passed local TDD, full local gates, `.27`
  focused gates, script self-tests, clippy, and release build, but the scoped
  safe1200 reverse-first P1 failed at `0.699/0.030 Mbit/s`.
- Useful discriminator: this was not the Knife14ep pressure-starvation shape.
  Local pressure stayed at zero, backpressure pause/resume stayed `0/0`,
  `headroom_deferred_bytes=0`, `pending_max=0`, and read credit never collapsed
  below the old pressure floor (`read_credit_limit_bytes_min=65536`).
- Clean surfaces remained clean: `tun_tx_dropped_delta=0`, QUIC
  loss/congestion/blocking `0`, `terminal_pending_reap=0`,
  `pending_at_close=0`, and `egress_at_close=0`.
- New root surface: reverse data stalled before local pressure existed, with
  `tuic_stream_pending data_pending_gap_max_ms=19922`,
  `tuic_tcp_stream data_read_gap_max_ms=20491`, and
  `relay_late_remote post_finish_bytes=92288` after one local finish event.
- Reusable rule: when a failed reverse-first run reports no local pressure and
  `read_credit_limit_bytes_min=65536`, do not tune the pressure floor or
  adaptive ACK drain again. The next discriminator belongs around local FIN
  ordering, read-only-after-local-finish behavior, and TUIC stream pending/read
  wakeups while remote payload is still expected.

## 2026-07-07 - Knife14er-es-et separates pressure control from ACK/window cadence

- Result doc:
  `docs/tech/2026-07-07-knife14er-et-downlink-credit-followup-results.md`
- Bundles:
  `/tmp/mini_vpn/knife14er_half_closed_gap_hint_p1_30/mvpn_knife14er_half_closed_gap_hint_p1_30_usclient_suite_20260707_182258.tar.gz`,
  `/tmp/mini_vpn/knife14es_floor_aware_credit_p1_30/mvpn_knife14es_floor_aware_credit_p1_30_usclient_suite_20260707_182841.tar.gz`,
  `/tmp/mini_vpn/knife14et_progress_staging_p1_30b/mvpn_knife14et_progress_staging_p1_30b_usclient_suite_20260707_183653.tar.gz`
- Outcome: local TDD, full local gates, `.27` focused tests, and release build
  passed, but VPS acceptance did not reach the `100+ Mbit/s` receiver target.
- Useful sequence: Knife14er cleared the half-closed no-data shape but returned
  to local pressure (`22.8/21.7 Mbit/s`, `read_credit_limit_bytes_min=24`);
  Knife14es removed tiny residual credit but stayed local-pressure-limited
  (`23.5/21.7 Mbit/s`); Knife14et removed local pressure entirely but fell into
  a no-data stream-starvation shape (`0.769/0.042 Mbit/s`).
- Clean surfaces in the valid final run: direct and exit-to-target baselines
  were healthy, `tun_tx_dropped_delta=0`, QUIC loss/congestion/blocking `0`,
  `pending_at_close=0`, `egress_at_close=0`, and
  `terminal_pending_reap=0`.
- Root refinement: local egress pressure and reverse TCP ACK/window cadence are
  different control loops. A clean local pressure surface does not imply the
  target sender is receiving enough upstream ACK/window progress.
- Reusable rule: pause threshold tuning when the run reports
  `local_pressure=0`, `pending_total_max=0`,
  `headroom_deferred_bytes=0`, and `read_credit_limit_bytes_min=65536` but
  still has large `tuic_stream_pending` / `relay_remote_read_gap` values. The
  next stage needs ACK propagation / FIN-ordering diagnostics or a split
  controller architecture, not another headroom constant.

## 2026-07-07 - Knife14eu rejects FIN deferral and narrows the last branch

- Result doc:
  `docs/tech/2026-07-07-knife14eu-ack-window-discriminator-results.md`
- VPS bundle:
  `/tmp/mini_vpn/knife14eu_ack_window_discriminator_p1_30/mvpn_knife14eu_ack_window_discriminator_p1_30_usclient_suite_20260707_193254.tar.gz`
- Outcome: local gates, `.27` focused gates, release build, and safe1200
  reverse-first P1 completed, but acceptance failed at `20.8/19.8 Mbit/s`.
- Useful discriminator: the data stream had `local_finish_events=0`,
  `remote_after_local_finish_bytes=0`, and
  `data_max_read_gap_after_finish_ms=0`; the largest data read gap
  (`7002ms`) happened before local finish. The optional FIN-deferral A/B is
  therefore not the next code path.
- Remaining root: local pressure returned (`pause_edges=3/2`,
  `may_recv_false=14046`, `headroom_deferred_bytes=41455269`,
  `send_queue_max=557386`) while QUIC remained clean and stream frames were
  present. TUN RX gap-hint drains alone did not prevent multi-second sender
  stalls.
- Reusable rule: after Knife14eu, split payload egress pressure from
  ACK/window cadence. Keep payload staging bounded near the TUN edge, but give
  a separate bounded multi-MTU ACK/window drain path when QUIC is clean and
  stream read/pending gaps grow. Do not spend the next stage on FIN deferral,
  MTU/PLPMTUD, stale pool, sing-box, or iperf3.

## 2026-07-07 - Knife14ev rejects short-lived ACK cadence boost as the final fix

- Result doc:
  `docs/tech/2026-07-07-knife14ev-ack-cadence-controller-results.md`
- VPS bundle:
  `/tmp/mini_vpn/knife14ev_ack_cadence_p1_30/mvpn_knife14ev_ack_cadence_p1_30_usclient_suite_20260707_210539.tar.gz`
- Outcome: local gates, `.27` focused gates, release build, and safe1200
  reverse-first P1 completed, but acceptance failed at `16.0/14.9 Mbit/s`.
- Useful discriminator: the new hook was wired and active
  (`relay_gap_hints events=59`, `max_cadence_floor=19200`), but throughput
  still stayed low and the data relay still reported
  `read_credit_limit_bytes_min=1200`.
- Clean surfaces stayed clean: direct and exit-to-target baselines were
  healthy, QUIC loss/congestion/blocking stayed `0`, `tun_tx_dropped_delta=0`,
  `terminal_pending_reap=0`, `pending_at_close=0`, and `egress_at_close=0`.
- Remaining root: the pressure edge still controls the effective payload read
  lane. Repeated `projected_payload_credit_edge` debt pushed pressure beyond
  the credit edge, then relay credit paused even though the ACK cadence boost
  had fired.
- Reusable rule: do not keep increasing the short-lived cadence boost or TUN RX
  drain budget after Knife14ev. The next branch needs an explicit continuous
  ACK/window service lane separated from payload staging, with payload credit
  granted from measured TUN egress progress rather than one-shot gap hints.

## 2026-07-07 - Knife14ew rejects egress-earned payload credit as the final fix

- Result doc:
  `docs/tech/2026-07-07-knife14ew-egress-earned-payload-credit-results.md`
- VPS bundle:
  `/tmp/mini_vpn/knife14ew_egress_tokens_p1_30/mvpn_knife14ew_egress_tokens_p1_30_usclient_suite_20260707_220735.tar.gz`
- Outcome: local TDD, full local gates, `.27` focused gates, release build, and
  safe1200 reverse-first P1 completed, but acceptance failed at
  `0.979/0.046 Mbit/s`.
- Useful discriminator: the new payload token reservoir earned credit
  (`egress_payload_credit_bytes=122727`) and local pressure was gone
  (`pending_total_max=0`, `headroom_deferred_bytes=0`,
  `pressure_credit_debt_bytes=0`), so the failure was not caused by the local
  pending/backpressure controller being too tight.
- Clean surfaces stayed clean: direct and exit-to-target baselines were
  healthy, QUIC client-side loss/congestion/blocking stayed `0`,
  `tun_tx_dropped_delta=0`, `terminal_pending_reap=0`, `pending_at_close=0`,
  and `egress_at_close=0`.
- Remaining root: pressure-free TUIC stream starvation or ACK/window service.
  The data stream had `read_credit_limit_bytes_min=65536`, but still saw
  `data_max_read_gap_ms=23974`, `data_pending_gap_max_ms=23247`, and
  target-sender-stalled/no-data throughput.
- Reusable rule: when a reverse-first run has clean local pressure and earned
  egress tokens but `conn_udp_rx_since_read` / connection stream-frame counters
  grow while the active stream read remains pending, stop tuning payload credit.
  The next branch needs stream-pending diagnostics and ACK/window uplink service
  evidence, not larger downlink credit caps.

## 2026-07-07 - Knife14ex validates ACK service but rejects widening it as the final fix

- Result doc:
  `docs/tech/2026-07-07-knife14ex-active-flow-ack-window-service-results.md`
- VPS bundle:
  `/tmp/mini_vpn/knife14ex_active_ack_lane_p1_30/mvpn_knife14ex_active_ack_lane_p1_30_usclient_suite_20260707_231150.tar.gz`
- Outcome: local gates, `.27` focused gates, release build, clippy, and
  safe1200 reverse-first P1 completed, but acceptance failed at
  `17.8/16.7 Mbit/s`.
- Useful progress: the bounded default active-flow ACK/window lane was active
  (`10ms`) and raised TUN RX service to `11770` attempts / `14090` TCP packets.
  The run moved from Knife14ew's `no_data` shape to `low_average`.
- Useful discriminator: `tuic_stream_pending_causes` was dominated by
  `connection_stream_frames_pending=18`, with `no_connection_rx=0`. The active
  stream was pending while connection-level stream frames advanced; this is not
  solved by more TUN ACK drain alone.
- Remaining root: once data moved again, local egress/headroom pressure returned
  (`send_queue_max=557386`, `may_recv_false=13443`,
  `headroom_deferred_bytes=16754655`, `pending_total_max=212852`) while QUIC
  loss/congestion/blocking and TUN drops stayed clean.
- Reusable rule: keep the bounded ACK/window lane because it helped, but stop
  widening timer duration, TUN RX budget, cadence floor, or payload-token caps
  for this evidence. The next branch should stabilize relay/TUIC stream read
  service and the local egress target/headroom loop together, with deterministic
  tests before another VPS run.

## 2026-07-08 - Knife14ey localizes read-service and adds an egress target

- Result doc:
  `docs/tech/2026-07-08-knife14ey-read-service-egress-target-local-results.md`
- Outcome: local implementation, full local gates, `.27` focused gates, release
  build, clippy, and safe1200 reverse-first P1 completed. Acceptance still
  failed at `25.5/24.2 Mbit/s`.
- Useful local change: `run_relay_reader` now owns the remote
  `read(...).await` future. Relay supervisor events such as writer signals,
  diagnostics, ACK hint ticks, and ordinary credit updates no longer rebuild
  the pending remote read future. ACK hint timing remains in the supervisor so
  it can request TUN RX service without cancelling stream reads.
- Useful control change: ordinary payload drain credit and projected pressure
  debt now target `tx_queue_egress_target_threshold`, halfway between clean
  flush and the old credit-spend edge. At/above that target, cadence boosts
  collapse to the ACK/window floor instead of allowing multi-MTU payload
  staging.
- Useful VPS improvement: during active transfer the ordinary payload loop was
  better behaved (`send_queue_max=447679`, `may_recv_false=0`,
  `headroom_deferred_bytes=72259` at roughly `72MB` delivered), so the
  read-service split plus egress target is not a no-op.
- Remaining root: the failure moved to the terminal/CloseWait tail. After
  iperf close, local TCP reported `may_recv=false`, pending stayed nonzero,
  and close-drain pushed `send_queue_max` back to the old credit edge
  (`557386`) with `may_recv_false=11961` and
  `headroom_deferred_bytes=14717970`.
- Reusable rule: when active-transfer pressure improves but close-tail pressure
  returns, do not widen ACK cadence or target constants. Make terminal
  CloseWait drain target-aware and driven by measured local egress progress
  before the next VPS acceptance run.

## 2026-07-08 - Knife14fi-fo closes local credit as the primary root

- Result doc:
  `docs/tech/2026-07-08-knife14fi-fo-downlink-credit-stream-gap-results.md`
- Outcome: local TDD, full local gates, `.27` focused gates, release build,
  clippy, direct `.33 <-> .77` baselines, and five scoped reverse-first P1
  suites completed. Acceptance still failed; the best kept code path was
  Knife14fm at `25.9/24.7 Mbit/s`.
- Useful kept changes: progress-sensitive downlink credit, QUIC UDP socket
  buffer configuration, clean empty-staging full-batch read credit, and TUIC
  stream-pending diagnostics/self-wake. Safe1200 keeps normal QUIC receive
  windows.
- Rejected branches: relay read-service ticks did not fix the remote read gap
  and reintroduced pressure; unordered TUIC chunk reads collapsed throughput;
  safe1200 `1MB/4MB` receive windows introduced `rx_blocked`; pool size `1`
  was worse; default MTU/PLPMTUD was not better than safe1200.
- Strong discriminator: after local pressure was clean or near clean
  (`pending_max=0`, `may_recv_false` near `0`, no TUN drops, no QUIC
  loss/congestion/blocking), the active TUIC data stream still had repeated
  multi-second remote read/pending gaps around `3.4s` to `5.6s`.
- Reusable rule: once pending/headroom/may_recv are clean but TUIC stream read
  gaps remain, stop tuning local credit, egress pacer, TUN RX budgets,
  MTU/PLPMTUD, receive-window shrink, or pool size. The next branch must be a
  mature TUIC/sing-box client A/B or protocol-level QUIC stream delivery trace
  on the `.33 -> .27` leg.

## 2026-07-08 - Mature sing-box client A/B is also slow

- Result doc:
  `docs/tech/2026-07-08-knife14fi-fo-downlink-credit-stream-gap-results.md`
- Outcome: the mature-client A/B completed on `.27` with official sing-box
  `v1.13.14`. A TUN inbound routed only `.77/32` through TUIC to `.33`; the
  route to `.33` stayed on `eth0`, avoiding a proxy loop. Temporary runtime
  configs with TUIC credentials were removed after the run.
- Results: sing-box client MTU `1200` reached `18.278/17.196 Mbit/s`; sing-box
  client MTU `1500` reached `29.497/27.751 Mbit/s`; same-window direct
  `.33 -> .77` reverse control reached `209.141/205.814 Mbit/s`.
- Server-side discriminator: `.33` TUIC inbound was configured with
  `congestion_control=bbr` and `zero_rtt_handshake=true`, so the reverse
  QUIC sender was not simply stuck on a conservative cubic-only server default.
- Reusable rule: if a mature sing-box client on the same `.27/.33/.77` topology
  is also capped around `20-30 Mbit/s`, stop treating the last 100M gap as a
  mini_vpn client-local controller bug. Move to sing-box server-side behavior,
  TUIC single-stream delivery limits, provider/path UDP behavior on `.33 -> .27`,
  or a controlled custom-exit architecture.

## 2026-07-08 - Knife14fp unlocks 100M with exit-side socket buffers

- Result doc:
  `docs/tech/2026-07-08-knife14fp-server-socket-buffer-results.md`
- Outcome: the server-side socket buffer A/B succeeded. `.33` defaults were
  only `212992B` for `net.core.rmem_max`, `wmem_max`, `rmem_default`, and
  `wmem_default`. Raising max to `16777216` and defaults to `1048576`, then
  restarting sing-box, moved the mature sing-box client from `27.751 Mbit/s`
  receiver to `185.242 Mbit/s`.
- mini_vpn validation: with the same `.33` high-buffer setting, safe1200
  reverse-first P1 reached a reported `114.000 Mbit/s` receiver and
  `throughput_shape=stable_high`; the 29 non-zero data intervals averaged
  `189.483 Mbit/s`. QUIC loss/congestion and tx blocking stayed zero, and the
  data stream pending/read gap fell to `189ms`.
- Persistent environment change: `.33` now has
  `/etc/sysctl.d/99-mini-vpn-quic.conf` with
  `rmem_max/wmem_max=16777216` and `rmem_default/wmem_default=1048576`.
- Remaining work: the high-rate run was not a fully clean acceptance because
  iperf was timeout-killed after the data phase and the close tail had
  `terminal_pending_reap=349932`, `pending_at_close=349932`,
  `tun_tx_dropped_delta=16`, and `rx_blocked_stream=1`.
- Reusable rule: do not lower the target to `30 Mbit/s`. When `.33` socket
  buffers are high, `100+ Mbit/s` is reachable on this topology. The next mini_vpn
  work is close-tail cleanliness under high throughput, not more local credit,
  MTU/PLPMTUD, pool, or sing-box version chasing.

## 2026-07-08 - Knife14fs rejects the explicit TUIC stream wrapper as the final fix

- Result doc:
  `docs/tech/2026-07-08-knife14fs-explicit-tuic-relay-stream-results.md`
- Code commit:
  `f25c952` (`fix: use explicit TUIC TCP relay stream`)
- VPS bundle:
  `/tmp/mini_vpn/knife14fs_explicit_tuic_stream_p1_30/mvpn_knife14fs_explicit_tuic_stream_p1_30_usclient_suite_20260708_101623.tar.gz`
- Outcome: local TDD, `cargo test tuic --lib`, `cargo test --lib`, release
  build, and the focused `.27 -> .33 -> .77` safe1200 reverse-first P1 ran
  cleanly, but acceptance failed badly at `1.29/0.132 Mbit/s`.
- Useful discriminator: replacing `tokio::io::join(recv, send)` with an
  explicit `TuicTcpRelayStream { recv, send }` did not reduce starvation. The
  active data stream still reported `connection_stream_frames_pending=21`,
  `data_pending_gap_max_ms=17024`, and `data_read_gap_max_ms=13776`.
- Clean surfaces stayed clean: `pending_total_max=0`, `may_recv_false=0`,
  `headroom_deferred_bytes=0`, `pending_at_close=0`,
  `terminal_pending_reap=0`, `tun_tx_dropped_delta=0`, and QUIC
  loss/congestion/blocking/rx-blocked stayed zero.
- Reusable rule: do not keep tuning local downlink credit, egress pacer,
  TUN RX/ACK drain, pool size, MTU/PLPMTUD, or the join-vs-wrapper layer for
  this evidence. The next branch must go below the wrapper layer: quinn
  per-stream readiness/ordered-offset tracing, server-side sending trace, or a
  controlled custom-exit/TUIC data-channel discriminator.

## 2026-07-08 - Knife14ft rejects default unordered TUIC reassembly as the final fix

- Result doc:
  `docs/tech/2026-07-08-knife14ft-unordered-reassembly-results.md`
- Code commits:
  `8b1d86d` reverted the failed explicit wrapper, and `a4bfbe6` added bounded
  unordered `RecvStream::read_chunk(false)` consumption with local offset
  reassembly before exposing ordered bytes to the relay.
- VPS bundle:
  `/tmp/mini_vpn/knife14ft_unordered_reassembly_p1_30/mvpn_knife14ft_unordered_reassembly_p1_30_usclient_suite_20260708_103800.tar.gz`
- Outcome: local TDD, `cargo test tuic --lib`, `cargo test --lib`, release
  build, and the focused `.27 -> .33 -> .77` safe1200 reverse-first P1 ran, but
  acceptance failed badly at `0.280/0.004 Mbit/s` with a no-data shape.
- Useful discriminator: new `tuic-tcp-unordered-staging` logs proved real
  stream-offset gaps (`next_offset=14120`, `chunk_offset=62092`,
  `gap_bytes=47972`) while local staging remained bounded
  (`max_buffered=29702B`, `cap_hits=0`). The active data stream still showed
  `connection_stream_frames_pending=34`, `data_pending_gap_max_ms=23033`, and
  `data_read_gap_max_ms=23935`.
- Clean surfaces stayed clean: `pending_total_max=0`, `may_recv_false=0`,
  `headroom_deferred_bytes=0`, `pending_at_close=0`,
  `terminal_pending_reap=0`, `tun_tx_dropped_delta=0`, and QUIC
  loss/congestion/blocking stayed zero.
- Reusable rule: do not keep `a4bfbe6` as the default data path. Revert it or
  gate it behind an explicit diagnostic env flag before another acceptance run.
  The next repair needs server-side sending cadence / qlog-style evidence,
  quinn ordered-offset readiness tracing, or a custom-exit/TUIC data-channel
  discriminator.

## 2026-07-08 - Knife14fy restores a single ordered TUIC TCP path before pressure-credit work

- Stage doc:
  `docs/tech/2026-07-08-knife14fy-ordered-read-service-restore-plan.md`
- Outcome: local stage only. The rejected unordered `RecvStream::read_chunk(false)`
  TCP branch and `MINI_VPN_TUIC_TCP_UNORDERED_REASSEMBLY` env gate were removed
  from `src/tuic.rs`, leaving TUIC TCP on a single ordered
  `tokio::io::join(recv, send)` path again.
- Diagnostics added: `tuic-open-tcp` now reports `stream`,
  `relay_mode=ordered_join`, and per-slot `startup_auth_attempts`; TUIC stream
  pending/close logs report `self_wake_armed` and `self_wake_fired`; relay
  live/close logs report awaited remote read-service tick count and read-window
  min/max.
- Existing bundle contrast: Knife14fu current branch had clean local
  pressure/headroom/TUN surfaces but only `176260` data bytes and sparse reads,
  while f8765c1 moved `70141010` bytes and failed later as local-pressure-credit.
  Therefore the next acceptance should first prove data-moving restoration
  before changing credit constants.
- Gates passed: `cargo test --lib tuic`, focused relay/read-service tests,
  `cargo test --lib`, `cargo build --release`, `git diff --check`, and
  `rustfmt --edition 2024 --check src/tuic.rs src/client_tun.rs`.
- Reusable rule: after a diagnostic data-channel experiment is rejected by VPS
  evidence, remove it from the default code surface before the next
  high-sensitivity throughput A/B. Keep the next run's log surface narrow enough
  to attribute stream read service, self-wake, relay cadence, startup pool
  recovery, or egress progress without experimental branch ambiguity.

## 2026-07-08 - Knife14fy acceptance was blocked before the data plane

- Result doc:
  `docs/tech/2026-07-08-knife14fy-ordered-read-service-restore-results.md`
- Code commit:
  `653d62bf` (`fix(knife14fy): restore ordered TUIC read diagnostics`)
- VPS bundle:
  `/tmp/mini_vpn/knife14fy_ordered_readsvc_p1_30/mvpn_knife14fy_ordered_readsvc_p1_30_usclient_suite_20260708_115629.tar.gz`
- Outcome: local gates, remote `.27` focused tests, remote release build, and
  `git diff --check` passed in a clean detached worktree. The acceptance did
  not reach reverse-first iperf because `client-tun` failed at TUIC startup with
  `tuic auth finish: sending stopped by peer: error 0`.
- Useful discriminator: direct `.27/.33/.77` baselines were healthy, `.33`
  `sing-box` was active with the expected high-throughput socket buffers, and
  no-secret config matching reported UUID/password/SNI/ALPN matches. The `.33`
  log tail did not show a contemporaneous TUIC inbound record in the failure
  window.
- Reusable rule: do not read this failed suite as a stream read-service,
  relay cadence, self-wake, or pressure-credit result. First run a bounded
  startup-only discriminator on `653d62bf` with pool `1`, then pool `2`, and
  only compare against a clean `f8765c1` startup probe if the failure repeats.

## 2026-07-08 - Knife14fy retry restores data-moving and reopens local pressure-credit

- Result doc:
  `docs/tech/2026-07-08-knife14fy-ordered-read-service-restore-results.md`
- Code commit:
  `653d62bf` (`fix(knife14fy): restore ordered TUIC read diagnostics`)
- VPS bundle:
  `/tmp/mini_vpn/knife14fy_ordered_readsvc_p1_30_retry1/mvpn_knife14fy_ordered_readsvc_p1_30_retry1_usclient_suite_20260708_133658.tar.gz`
- Outcome: startup-only probes with `MINI_VPN_TUIC_TCP_POOL=1` and `2` both
  succeeded, then the focused safe1200 reverse-first P1 retry entered the data
  plane and reached `21.4/20.0 Mbit/s`.
- Useful discriminator: `remote_to_global_rx_bytes=75502079`,
  `remote_reads=3319`, `remote_read_service_ticks=2763`, and
  `tuic-tcp-stream-close self_wake_armed=9921 self_wake_fired=8857` prove the
  ordered path is data-moving again rather than Knife14fu no-data.
- Clean surfaces: `pending_at_close=0`, `terminal_pending_reap=0`,
  TUN rx/tx drops `0`, `send_slice_zero=0`, `send_slice_errors=0`, and QUIC
  loss/congestion/blocking deltas `0`.
- Remaining root: local pressure-credit/downlink backpressure. The run reported
  `downlink_backpressure pause_edges=3`, `may_recv_false=7655`,
  `headroom_deferred_bytes=10188203`, and
  `pressure_credit_blocked_bytes=960157`.
- Reusable rule: after startup-only probes pass, treat a single startup-auth
  close as transient and continue to the data-plane discriminator. The next
  code stage should be narrow pressure-credit TDD, not TUIC stream/read-service,
  VPS, iperf3, MTU, stale-pool, or broad QUIC-window work.

## 2026-07-08 - Knife14fz sing-box comparison narrows the next fix

- Result doc:
  `docs/tech/2026-07-08-knife14fz-singbox-client-comparison-and-pressure-credit-plan.md`
- Evidence compared:
  current branch `653d62bf`, known data-moving `f8765c1`, and the mature
  sing-box client path that reached `173/173 Mbit/s` in the same `.27/.33/.77`
  service window.
- Outcome: the most useful sing-box lesson is not "copy its QUIC knobs" but
  "stop suppressing reverse downlink before real pressure arrives." sing-box
  keeps the TUIC TCP path thin, uses plain full-duplex copy loops, hands
  backpressure to `net.Conn`/gVisor/TUN, and batches Linux TUN writes.
- mini_vpn-specific diagnosis: the current branch installs pressure debt on a
  single target-edge sample and clamps flushes at the derived egress target,
  even while TUN drops stay `0` and the true pause edge is not reached. This
  matches the retry signals `may_recv_false=7655`,
  `headroom_deferred_bytes=10188203`, and
  `pressure_credit_blocked_bytes=960157`.
- Reusable rule: before borrowing architecture from a mature client, first
  remove mini_vpn's own premature local gates. For Knife14 reverse-first,
  target-edge should be treated as a soft warning until sustained pressure,
  actual drops, flush failures, or the hard pause edge justify stronger debt.

## 2026-07-08 - Knife14fz soft target-edge restores data movement but not throughput

- Result doc:
  `docs/tech/2026-07-08-knife14fz-soft-pressure-credit-results.md`
- Code commit:
  `52f2bae0` (`fix(knife14fz): soften target-edge pressure credit`)
- VPS bundle:
  `/tmp/mini_vpn/knife14fz_soft_pressure_p1_30/mvpn_knife14fz_soft_pressure_p1_30_usclient_suite_20260708_141720.tar.gz`
- Outcome: local TDD gates, full local `cargo test --lib`, local release
  build, remote `.27` focused gates, and remote release build passed. The
  focused safe1200 reverse-first P1 entered the data plane and reached
  `24.6/22.9 Mbit/s`, so the current branch is no longer a Knife14fu no-data
  shape.
- Useful discriminator: `remote_to_global_rx_bytes=85881616`,
  `send_slice_accepted=85881616`, `may_recv_false=0`,
  `send_slice_zero=0`, `send_slice_errors=0`, `pending_at_close=0`,
  `terminal_pending_reap=0`, TUN rx/tx drops `0`, and QUIC
  loss/congestion/blocking deltas `0`.
- Remaining root: the low average is burst/idle local cadence, not VPS or QUIC
  loss. The run reported `downlink_backpressure pause_edges=1`,
  `headroom_deferred_bytes=363494`,
  `pressure_credit_blocked_bytes=161309`, and repeated
  `connection_stream_frames_pending` with data-stream read gaps up to
  `3522ms`.
- Reusable rule: softening target-edge pressure is necessary but not sufficient.
  The next code slice should connect local egress progress/pressure recovery to
  TUIC stream read-service wakeups, and should prove with tests that pending
  stream frames plus available local send capacity cause an immediate bounded
  read-service tick.

## 2026-07-08 - Knife14gl keep-read-armed restores first_rx but not data-moving

- Result doc:
  `docs/tech/2026-07-08-knife14gl-keep-read-armed-results.md`
- Code commit:
  `18ed16ef` (`fix(knife14gl): keep relay reads armed across credit updates`)
- VPS bundle:
  `/tmp/mini_vpn/knife14gl_keep_read_armed_safe1200_p1_30/mvpn_knife14gl_keep_read_armed_safe1200_p1_30_usclient_suite_20260708_164002.tar.gz`
- Outcome: local TDD proved and fixed a real current-vs-`f8765c1` regression:
  non-pausing read-credit updates no longer cancel an in-flight ordered stream
  read. The data stream `first_rx_ms` improved from `17266ms` in `knife14gj` to
  `3ms` in `knife14gl`, matching the `f8765c1` shape.
- Acceptance still failed: reverse-first P1 reached only `280 Kbit/s` sender
  and `16.2 Kbit/s` receiver. The stream stalled at
  `remote_to_global_rx_bytes=60704`, with `pending_cause=connection_stream_frames_pending`,
  `max_remote_read_gap_ms=17096`, no local pressure, no TUN drops, and no QUIC
  loss/blocking.
- Reusable rule: the next root is not local pressure-credit yet. The early data
  stream gap can sit below `RELAY_ACK_DRAIN_HINT_MIN_DATA_BYTES=64KiB`, so
  `ack_drain_hint_due` stays `0` even while ordered QUIC stream frames are
  pending. The next TDD slice should cover low-byte ordered-stream gap service
  before another VPS run.

## 2026-07-08 - Knife14gm services low-byte ordered stream gaps locally

- Stage: T1-T5 local TDD for the Knife14gl follow-up.
- Outcome: T1 reproduced the `60704B` ordered-stream gap below
  `RELAY_ACK_DRAIN_HINT_MIN_DATA_BYTES=64KiB`. T2/T3 made both the direct
  ACK-drain due predicate and the relay supervisor polling gate recognize
  payload-shaped streams with active read-service ticks below 64KiB.
- Tests:
  `cargo test relay_ack_drain_hint -- --nocapture` and `cargo test --lib`
  pass locally.
- Guardrails preserved: tiny `512B` control streams still do not emit ACK hints,
  and paused/zero-credit receive credit still suppresses the outer remote-read
  probe gate.
- Open gate note: `cargo clippy --lib -- -D warnings` currently fails on two
  pre-existing `collapsible_if` warnings outside this diff.

## 2026-07-08 - Knife14gm restores data movement but not 30M

- Result doc:
  `docs/tech/2026-07-08-knife14gm-low-byte-gap-results.md`
- Code commit:
  `9e50a32` (`fix(knife14gm): service low-byte ordered stream gaps`)
- VPS bundle:
  `/tmp/mini_vpn/knife14gm_lowbyte_gap_safe1200_p1_30_retry1/mvpn_knife14gm_lowbyte_gap_safe1200_p1_30_retry1_usclient_suite_20260708_185002.tar.gz`
- Outcome: the focused safe1200 reverse-first P1 reached `23.2 Mbit/s` sender
  and `21.2 Mbit/s` receiver. It did not exceed `30 Mbit/s`.
- Useful progress: the data stream is no longer stuck at the Knife14gl
  `60704B` low-byte no-data edge. The retry run moved
  `remote_to_global_rx_bytes` to `79482037`, emitted `204` relay gap hints, and
  kept TUN drops, close-tail accounting, and QUIC loss/blocking clean.
- Remaining root: local pressure-credit / egress-progress feedback is again
  the active branch. The run stayed `low_average` with multi-second
  `connection_stream_frames_pending` gaps and later `projected_payload_credit_edge`
  debt, despite clean QUIC and TUN surfaces.
- Reusable rule: after low-byte ACK hint service, do not keep adding ACK hint
  triggers. The next TDD slice should connect useful local egress progress and
  credit repayment to read-credit publishing/read-service cadence, with the
  immediate acceptance target of clearing `30 Mbit/s` before returning to
  `100+ Mbit/s`.

## 2026-07-08 - Knife14gp post-flush pressure debt did not clear 30M

- Result doc:
  `docs/tech/2026-07-08-knife14gp-post-flush-pressure-debt-results.md`
- Code commit:
  `bd0d264` (`fix(knife14gp): defer projected pressure debt post-flush`)
- VPS bundle:
  `/tmp/mini_vpn/knife14gp_postflush_debt_20260708/mvpn_knife14gp_postflush_debt_safe1200_p1_30_usclient_suite_20260708_222938.tar.gz`
- Outcome: local full gates, focused remote `.27` gates, and the focused
  safe1200 reverse-first P1 completed. The run reached only `15.0 Mbit/s`
  sender and `13.5 Mbit/s` receiver, so it did not exceed `30 Mbit/s`.
- Useful progress: T13 added since-last-pending transport counters, and T14
  moved projected pressure debt from pre-flush transient pending to post-flush
  residual pressure. In the VPS run, `downlink_backpressure pause_edges=0
  resume_edges=0`, while TUN drops, close-tail accounting, send errors, and
  QUIC loss/blocking stayed clean.
- Remaining root: useful read service still collapsed to `1200B` under
  residual pressure/headroom evidence (`may_recv_false=13`,
  `headroom_limited=3`, `hard_edge_guard_limited=3`) even though there were no
  hard drop/failure signals. Repeated pending diagnostics often showed
  `conn_rx_stream_frames_since_pending=0`, so older
  `connection_stream_frames_pending` evidence was stale since the last
  successful stream read.
- Reusable rule: do not keep chasing self-wake or pause-edge counters after
  Knife14gp. The next TDD slice should classify stale repeated STREAM-pending
  samples and preserve a useful read-service floor while accepted egress is
  progressing and hard pressure/drop/failure signals are absent.

## 2026-07-08 - Knife14gq buffered read service fixed 1200B collapse but failed B7

- Result doc:
  `docs/tech/2026-07-08-knife14gq-buffered-downlink-results.md`
- Code commit:
  `8a85ce7` (`fix(knife14gq): add buffered downlink controller`)
- VPS bundle:
  `/tmp/mini_vpn/knife14gq_buffered_downlink_20260708/mvpn_knife14gq_buffered_downlink_safe1200_p1_30_usclient_suite_20260708_235459.tar.gz`
- Outcome: local full gates, remote `.27` focused gates, remote release build,
  and B7 safe1200 reverse-first P1 all executed. B7 failed the `>30 Mbit/s`
  gate with `18.3/17.2 Mbit/s`.
- Useful progress: buffered mode was enabled and fixed the Knife14gp
  read-credit collapse: `remote_read_service_len_min=65536`,
  `remote_batch_limit_bytes_min=524288`, `read_credit_pause_updates=0`,
  `read_credit_limit_bytes_min=524288`.
- Clean surfaces: direct reverse baseline `278 Mbit/s`, TUN rx/tx drops `0`,
  send_slice zero/errors `0`, close-tail accounting `0`, and QUIC
  loss/congestion/blocking deltas `0`.
- Remaining root: throughput stayed burst/idle with
  `attribution: local_downlink_backpressure`, one downlink pressure
  pause/resume edge, `headroom_deferred_bytes=603544`, and data-stream ordered
  gaps up to about `3581ms`.
- Reusable rule: preserving a useful TUIC read-service floor is necessary but
  not sufficient. The next architecture work must target local writer/TUN
  egress cadence directly and prove continuous drain before claiming a path to
  `100+ Mbit/s`.

## 2026-07-09 - Knife14gr performance gate blocks more read-credit work

- Gate doc:
  `docs/tech/2026-07-09-knife14gr-egress-cadence-reachability.md`
- Outcome: applying the new performance architecture gate showed the B7-era
  code has nominal batch capacity but no sufficient local egress cadence path
  as-is. `flush_downlink` can admit large batches, but sustained progress still
  depends on scattered remote-payload, timer, dirty, and TUN-RX drain paths.
- Reusable rule: before another VPS run, implement and test a single bounded
  main-loop egress service lane that alternates TUN RX ACK intake,
  `iface.poll`, dirty downlink flush, and `flush_tx` until progress stops or a
  hard budget is reached. Do not return to read-credit floor, self-wake, VPS,
  MTU, stale-pool, or broad QUIC-window changes as the next step.

## 2026-07-09 - Knife14gs local egress service passes G5

- Result doc:
  `docs/tech/2026-07-09-knife14gs-local-egress-service-g5-results.md`
- Outcome: implemented an explicit bounded local egress service lane in
  `src/client_tun.rs`. The lane runs in the main loop and can repeat TUN RX
  ACK intake, `iface.poll`, `flush_tx`, and dirty downlink flush until target,
  no-progress, hard-pause, no-work, or cycle-budget stop.
- G5 local capacity gate: `128KiB` per `5ms` active window, about
  `25.6 MB/s` / `204 Mbit/s` nominal local service capacity before real VPS
  overhead. This is sufficient to proceed to VPS G6, but is not itself a
  throughput acceptance result.
- Tests/gates passed: `cargo test local_egress_service -- --nocapture`,
  `cargo test --lib`, `cargo test`,
  `cargo test --features harness --test concurrency_harness -- --nocapture`,
  `rustfmt --edition 2024 --check src/client_tun.rs`, `git diff --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`, and
  `cargo build --release`.
- Reusable rule: after local egress service changes, parse
  `tcp-local-egress-service` together with `tcp-tun-rx-drain` and
  `tcp-downlink-flush`. A low VPS Mbps result is not actionable until those
  counters say whether the service lane hit target, no-progress, hard-pause,
  or cycle budget.

## 2026-07-09 - Knife14gs G6 failed below 30M after local egress service

- Result doc:
  `docs/tech/2026-07-09-knife14gs-g6-local-egress-vps-results.md`
- Code commit:
  `4a44a11` (`fix(knife14gs): add local egress service lane`)
- VPS bundle:
  `/tmp/mini_vpn_knife14gs_g6/mvpn_knife14gs_g6_local_egress_safe1200_p1_usclient_suite_20260709_075624.tar.gz`
- Outcome: remote `.27` focused gates passed, then the focused safe1200
  reverse-first P1 reached `28.6/27.6 Mbit/s`, below the `>30 Mbit/s` gate.
- Useful progress: the current branch stayed data-moving and improved over the
  `18-22 Mbit/s` band in several recent runs, with direct reverse baseline
  `280 Mbit/s`, TUN drops `0`, send errors `0`, QUIC loss/blocking `0`, and
  reader credit preserved at `65536B` service / `524288B` batch floor.
- Failed assumption: the local egress lane did not prove continuous throughput.
  It ran often (`windows=6024`, `cycles=1718`) but had `accepted_bytes=0`,
  `no_progress=1620`, and `no_work=4400`. Useful socket admission still came
  from the remote-payload path, not from the new service lane.
- Remaining root: TUIC ordered stream read cadence still has multi-second
  gaps (`max_remote_read_gap_ms=5029`) with repeated
  `connection_stream_frames_pending`, while local TUN/QUIC/drop surfaces stay
  clean.
- Reusable rule: a local egress capacity proof is not sufficient unless the
  same service window also includes useful TUIC remote reads. The next TDD
  slice must couple remote read service and main-loop local injection/egress in
  one measured contract; do not continue with local-only flush, qlen, MTU,
  VPS, stale-pool, broad-window, or self-wake-only changes.

## 2026-07-09 - Knife14gt G7 passed 30M and 100M after dispatch-window alignment

- Result doc:
  `docs/tech/2026-07-09-knife14gt-dispatch-window-g7-results.md`
- Code commit:
  `4caf60a` (`fix(knife14gt): align relay dispatch with egress window`)
- VPS bundle:
  `/tmp/mini_vpn_knife14gt_g7/mvpn_knife14gt_dispatch128_safe1200_p1_usclient_suite_20260709_085329.tar.gz`
- Outcome: the focused safe1200 reverse-first P1 reached `147/144 Mbit/s`,
  clearly passing the previous `>30 Mbit/s` discriminator and entering the
  `100+ Mbit/s` band on the current `.27/.33/.77` topology.
- Useful progress: raising the relay dispatch segment from `64KiB` to one
  local egress service window (`128KiB`) produced
  `remote_batch_bytes_max=131072` and restored stable ordered-stream cadence
  during the main run (`max_remote_read_gap_ms` around `190-203ms` before the
  tail). The main downlink path accepted `539681529` bytes with
  `send_slice_zero=0` and `send_slice_errors=0`.
- Remaining risk: this is not clean final acceptance. The tail had
  `tx_dropped_delta=783`, `global_rx_queue_used_max=1019/1024`, and
  `terminal_pending_reap_bytes=2653878` with
  `close_pending_class=terminal_closed_no_send`.
- Reusable rule: after a high-throughput run, do not stop at iperf Mbps. Parse
  TUN drop feedback, global RX queue edge, and terminal pending. The next slice
  should preserve the new dispatch/egress cadence while making the close-tail
  clean, not return to VPS, MTU, stale pool, broad QUIC windows, or unordered
  reassembly.

## 2026-07-09 - Knife14gu G8 RX-edge guard did not preserve 100M cadence

- Result doc:
  `docs/tech/2026-07-09-knife14gu-rx-edge-g8-results.md`
- Code commit:
  `1a3c5cb` (`fix(knife14gu): stop relay ready bursts at rx edge`)
- VPS bundle:
  `/tmp/mini_vpn_knife14gu_g8/mvpn_knife14gu_rxedge_safe1200_p1_usclient_suite_20260709_092357.tar.gz`
- Outcome: local full gates, remote focused gates, and the focused safe1200
  reverse-first P1 ran. The result regressed to `20.2/19.2 Mbit/s`, so it did
  not preserve G7's `100+ Mbit/s` cadence and did not exceed `30 Mbit/s`.
- Useful progress: tail-drop surfaces were cleaner than G7:
  `tx_dropped_delta=0`, no parsed TUN feedback pause, and data relay
  `global_rx_queue_used_max=210/1024` instead of `1019/1024`.
- Failed assumption: the new near-full limiter was not the active runtime
  factor. The run logged `remote_batch_limited=0` and
  `remote_batch_limit_bytes_min=524288`, while ordered TUIC stream read gaps
  returned to multi-second values up to `3610ms`.
- Reusable rule: do not infer G7's dirty tail root from global RX edge pressure
  alone. After a high-throughput but dirty run, the next cleanup must be A/B
  verified before another code change; otherwise it can clean one tail surface
  while losing the stream-service cadence that produced throughput.

## 2026-07-09 - Knife14gv A/B shows G7 is high evidence, not stable baseline

- Result doc:
  `docs/tech/2026-07-09-knife14gv-ab-repeat-results.md`
- Candidate bundle:
  `/tmp/mini_vpn_knife14gv_ab/mvpn_knife14gu_rxedge_repeat1_safe1200_p1_usclient_suite_20260709_093320.tar.gz`
- Parent bundle:
  `/tmp/mini_vpn_knife14gv_ab/mvpn_knife14gt_parent_ab_safe1200_p1_usclient_suite_20260709_093511.tar.gz`
- Outcome: no code changed. `1a3c5cb` repeated at `41.4/39.9 Mbit/s`, above
  `30M` but below `100M`. Parent `4caf60a` did not reproduce G7 and collapsed
  to `0.349/0.151 Mbit/s`.
- Useful discriminator: `1a3c5cb` still did not activate the RX-edge limiter
  (`remote_batch_limited=0`, data relay `global_rx_queue_used_max=161/1024`),
  so the G8/GV variance is not explained by that limiter firing.
- Parent failure shape: `remote_to_global_rx_bytes=667766`,
  `max_remote_read_gap_ms=10457`, pending/read-gap attribution, clean TUN
  drops, clean local pressure, and `global_rx_queue_used_max=1/1024`.
- Reusable rule: a single high-throughput VPS run must be treated as capability
  evidence, not a stable baseline. Before designing cleanup around a high run's
  tail counters, first repeat the high code point in the same suite shape.

## 2026-07-09 - Knife14gw diagnostics slice did not lift throughput

- Result doc:
  `docs/tech/2026-07-09-knife14gw-stream-service-diagnostics-results.md`
- Code commit:
  `946e2a8` (`feat(knife14): add stream service diagnostics`)
- VPS bundle:
  `/tmp/mini_vpn_knife14gw_stream_service_946e2a8/mvpn_knife14gw_stream_service_946e2a8_usclient_suite_20260709_102512.tar.gz`
- Outcome: local tests, remote focused gates, release build, suite self-test,
  and the focused safe1200 reverse-first P1 ran. The P1 result was
  `17.7/17.1 Mbit/s`, below `30M` and far below `100M+`.
- Useful progress: raw logs now distinguish TUIC pending freshness
  (`connection_fresh_stream_frames_pending` vs
  `connection_stale_stream_frames_pending`) and emit
  `tcp-stream-service-window` records combining remote read service,
  global-rx wait/occupancy, and local-admission progress.
- Root shape: direct baselines were healthy, `.33` socket buffers remained
  high, TUN drops were `0`, QUIC loss/congestion/blocking deltas were `0`,
  `global_rx_queue_used_max=126/1024`, and local send capacity stayed clean.
  Throughput still had multi-second ordered stream gaps
  (`data_max_read_gap_ms=3464`) with bursty iperf intervals.
- Reusable rule: diagnostics/framework slices are not throughput fixes unless
  the VPS gate shows stable cadence. The next code slice should target ordered
  TUIC stream service cadence directly, not more VPS/MTU/stale-pool/global-rx
  tuning.

## 2026-07-09 - Knife14gx thin relay staging did not pass the first threshold

- Result doc:
  `docs/tech/2026-07-09-knife14gx-thin-relay-staging-results.md`
- Code commits:
  `f0944e8` (`test(knife14): parse fresh stale TUIC pending causes`) and
  `1bda549` (`feat(knife14): add thin TCP relay staging gate`)
- VPS bundle:
  `/tmp/mini_vpn_knife14_thin_gate/mvpn_knife14thin_g1_usclient_suite_20260709_112459.tar.gz`
- Outcome: the feature flag was active (`MINI_VPN_THIN_TCP_RELAY=1`,
  `tcp-relay-engine ... engine=thin_staging`), but the focused reverse-first
  P1 reached only `17.6/15.2 Mbit/s`, below the `30 Mbit/s` first threshold.
- Useful discriminator: direct baselines were healthy, TUN drops were `0`,
  QUIC loss/congestion/blocking deltas were `0`, and `global_rx_pressure=0`.
  The parser now reports fresh/stale pending causes correctly:
  `connection_fresh_stream_frames_pending=7`,
  `connection_stale_stream_frames_pending=10`,
  `connection_rx_no_stream_frames=3`.
- Root shape: thin staging removed one possible `global_rx.reserve()` coupling,
  but ordered TUIC stream reads still had multi-second gaps
  (`data_read_gap_max_ms=4038`, `data_pending_gap_max_ms=4006`) while local
  pressure/headroom guards constrained egress (`may_recv_false=4649`,
  `headroom_limited=4609`, attribution `local_pressure_credit`).
- Reusable rule: a per-flow staging/dispatcher split is not sufficient by
  itself. The next design must target the ordered TUIC stream polling and local
  pressure-credit/headroom feedback contract directly, and must have a
  code-level gate for sustained remote-read cadence plus local admission
  progress before another VPS acceptance claim.

## 2026-07-09 - Knife14gy split-poll credit fix was necessary but not sufficient

- Result doc:
  `docs/tech/2026-07-09-knife14gy-split-poll-credit-results.md`
- Code commit:
  `372d6e3` (`fix(knife14): decouple stream polling from local credit`)
- VPS bundle:
  `/tmp/mini_vpn_knife14gy_splitpoll/mvpn_knife14gy_splitpoll_safe1200_p1_usclient_suite_20260709_115932.tar.gz`
- Outcome: local gates, remote focused gates, release build, and focused
  reverse-first P1 ran. The P1 result was `24.1/22.7 Mbit/s`, so it did not
  exceed `30 Mbit/s` and did not approach `100+ Mbit/s`.
- Useful progress: ordered stream polling is no longer capped below the
  dispatch segment by small local pressure credit. The data stream reported
  `remote_read_service_len_min=65536`,
  `remote_read_service_len_max=65536`,
  `remote_batch_bytes_max=131072`, and
  `read_credit_limit_bytes_min=6686`.
- Remaining root shape: the failure is still `local_pressure_credit`.
  The final report had `may_recv_false=5190`, `headroom_limited=5185`,
  `pressure_credit_debt_bytes=122727`, `send_queue_max=557386`, and
  `tcp-local-egress-service accepted_bytes=0`, while TUN drops, global-rx
  pressure, close-tail pending, and QUIC loss/congestion/blocking stayed clean.
- Reusable rule: split the terms carefully. Frequent ordered stream polling and
  large read length are necessary, but they are not acceptance. The next slice
  must make local admission/headroom produce sustained egress progress after
  remote reads; do not spend another VPS run on split-poll-only changes.

## 2026-07-09 - Knife14hz local egress drain progress did not restore throughput

- Result doc:
  `docs/tech/2026-07-09-knife14hz-egress-drain-progress-results.md`
- Code commit:
  `8fc0cdb` (`fix(knife14): count local egress drain progress`)
- VPS bundle:
  `/tmp/mini_vpn_knife14hz_egress_drain/mvpn_knife14hz_egress_drain_p1_usclient_suite_20260709_132051.tar.gz`
- Outcome: H1/H2/H3 completed. Local full gates, remote focused gates, release
  build, and the focused reverse-first P1 ran. The P1 result was
  `15.3/14.3 Mbit/s`, so it did not exceed `30 Mbit/s`.
- Useful progress: local egress and stream-service diagnostics now count
  successful TUN TX queue drain as local progress, so future logs can
  distinguish accepted bytes from drain-only progress.
- Discriminator: the H3 run had clean TUN drops, clean global-rx pressure,
  clean local pressure/headroom debt, clean QUIC loss/congestion/blocking, and
  `pending_at_close=0`, but still showed bursty iperf output and ordered TUIC
  data stream read/pending gaps up to `5086ms`.
- Reusable rule: do not continue local egress-drain or pressure-credit tuning
  when the run shows `accepted_bytes=52039183`, `pressure_credit_debt_bytes=0`,
  `headroom_limited=0`, and `tcp-local-egress-service egress_drain_bytes=0`.
  The next slice must target ordered TUIC stream pending/read self-wake cadence
  directly.

## 2026-07-09 - Knife14h4 ordered chunk adapter was not sufficient

- Result doc:
  `docs/tech/2026-07-09-knife14h4-ordered-chunk-results.md`
- Experimental code:
  local `src/tuic.rs` diff replacing the default ordered TUIC TCP read adapter
  with `TuicOrderedRelayStream` backed by Quinn `read_chunk(max, true)`.
- VPS bundle:
  `/tmp/mini_vpn_knife14h4_ordered_chunk/mvpn_knife14h4_ordered_chunk_p1_usclient_suite_20260709_135541.tar.gz`
- Outcome: local gates, remote focused gates, release build, and focused
  reverse-first P1 ran. The H4 path was active (`relay_mode=ordered_chunk`),
  but P1 reached only `17.1/15.7 Mbit/s`, below the `30 Mbit/s` first
  threshold.
- Useful progress: H4 reduced the worst data read gap versus Knife14hz from
  about `5086ms` to `3685ms`, proving the ordered read adapter is a real seam.
- Discriminator: throughput did not improve materially, while TUN drops,
  global-rx pressure, local write pressure, pressure-credit debt,
  headroom-limiting, close-tail pending, and QUIC loss/congestion/blocking all
  stayed clean. Fresh/stale pending remained
  (`fresh=9`, `stale=10`, `rx_no_stream_frames=2`).
- Reusable rule: do not keep spending VPS runs on ordered chunk size, generic
  read-vs-chunk adapter swaps, or self-wake timing alone. The next design must
  explain the bursty remote delivery/read cadence under clean local and QUIC
  counters before claiming a path to `>30M` or `100+M`.

## 2026-07-09 - Knife14h5 reachability gate reframed the next slice as a continuous pump

- Design doc:
  `docs/tech/2026-07-09-knife14h5-continuous-pump-reachability.md`
- Outcome: froze the HZ/H4 evidence and capacity math before coding. The
  decisive observation is that mini_vpn already has enough nominal batch size
  for `100+ Mbit/s`; the failing signal is multi-second active-flow
  read/pending gaps under otherwise clean TUN, global-rx, local pressure,
  close-tail, and QUIC counters.
- Architecture rule: the next implementation must be a Rust-native equivalent
  of sing-box's continuous per-flow download copy contract:
  `QUIC read pump -> bounded per-flow byte queue -> smoltcp-owner drain`.
  Remote reads should pause only for queue-full, EOF, cancellation, or hard
  terminal lifecycle state, not for predictive headroom/debt credit in the
  normal fast path.
- Reusable rule: do not run another VPS suite or edit relay parameters before a
  local tracer-bullet harness can prove active-window remote read gaps stay
  below `50ms` while queue capacity exists. If that seam cannot be built, the
  next task is seam extraction, not another throughput tweak.

## 2026-07-09 - Knife14h6 added the continuous pump tracer-bullet harness

- Result doc:
  `docs/tech/2026-07-09-knife14h6-continuous-pump-harness-results.md`
- Code:
  `src/tcp_downlink_pump.rs`, exposed through `src/lib.rs`.
- Outcome: Stage A from the H5 reachability gate is complete locally. The new
  `ByteBoundedFlowQueue` and `ContinuousPumpProbe` model the target
  `QUIC read pump -> bounded per-flow byte queue -> smoltcp-owner drain`
  contract without wiring it into the product relay path yet.
- Gates passed:
  `cargo test -q tcp_downlink_pump`, `cargo fmt --check`, `cargo check -q`,
  and `cargo test -q --lib` (`441` tests).
- Useful proof: the tracer-bullet test simulates `30s` of `64KiB` reads every
  `5ms` (`~104.9 Mbit/s`) and proves the model has no capacity-window read
  gap above the read cadence. A separate `200ms` drain-stall test proves a
  `4MiB` queue absorbs short local drain jitter without remote read gaps, while
  a tiny-queue test proves pauses are explicit queue-full backpressure.
- Reusable rule: Stage B may now implement the real async/product path, but
  only behind a gate and only if it preserves the same invariant with real
  waiters: when the byte queue can accept the planned read chunk, one remote
  read future must remain armed. If that cannot be tested through
  `run_relay_reader`, extract the seam first.

## 2026-07-09 - Knife14h7 added the async byte-queue seam

- Result doc:
  `docs/tech/2026-07-09-knife14h7-async-byte-queue-results.md`
- Code:
  `src/tcp_downlink_pump.rs`.
- Outcome: Stage B0 is complete locally. `AsyncByteBoundedFlowQueue` turns the
  H6 pure model into a real async Rust primitive with byte-capacity waits,
  async producer/consumer wakeups, `wait_read_len`, and close wakeups.
- Gates passed:
  `cargo test -q tcp_downlink_pump`, `cargo fmt --check`, `cargo check -q`,
  and `cargo test -q --lib` (`446` tests).
- Useful proof: a producer blocks only at byte-capacity full, draining bytes
  wakes it, `wait_read_len` blocks while read capacity is zero and returns
  available read size after drain, `recv_up_to` releases byte capacity, and
  `close` releases full-queue producers with an explicit `Closed` error.
- Limitation: the product path is still unchanged. `run_relay_reader` still
  uses `RelayReadCredit`, and `RelayEvent::Data`/`SocketCtx.downlink_pending`
  do not yet hold or release byte-capacity permits. Therefore H7 is a necessary
  seam, not a throughput fix, and VPS acceptance would not exercise it.
- Reusable rule: the next stage must be feature-gated product wiring with a
  local test showing normal local-admission read-credit pauses no longer stop
  remote reads while the byte queue has capacity. Do not run a VPS throughput
  suite until that product-path invariant is proven locally.

## 2026-07-09 - Knife14h8 wired a feature-gated continuous relay engine

- Result doc:
  `docs/tech/2026-07-09-knife14h8-continuous-pump-product-gate-results.md`
- Code:
  `src/client_tun.rs`, using `src/tcp_downlink_pump.rs`.
- Outcome: Stage B1 is complete locally. `MINI_VPN_CONTINUOUS_TCP_RELAY=1`
  selects a new `continuous_pump` relay engine. It uses
  `run_relay_continuous_reader -> AsyncByteBoundedFlowQueue ->
  run_continuous_relay_dispatcher -> RelayEvent::Data`, while legacy and thin
  staging remain unchanged by default.
- Gates passed:
  `cargo test -q continuous_relay_reader`,
  `cargo test -q tcp_relay_engine_selector`, `cargo test -q relay_task_diag`,
  `cargo test -q relay_close_diag`,
  `bash -n scripts/knife14b-usclient-tunnel-suite.sh`,
  `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`,
  `cargo fmt --check`, `cargo check -q`, `cargo test -q --lib`
  (`450` tests), and `git diff --check`.
- Useful proof: local product-path tests now prove paused normal
  `RelayReadCredit` does not block ready remote bytes under the continuous
  engine, and that the reader stops/resumes at the explicit byte-queue-full
  edge. The async queue close/drain test locks that close stops new producer
  work but still lets already queued bytes drain. The engine selector test
  locks continuous-pump priority over thin staging when both experimental env
  flags are set.
- Observability: relay live/close diagnostics now include
  `continuous_queue_wait_max_us` and `continuous_queue_wait_events`, plus a
  `tcp-continuous-queue-wait` log when queue-capacity wait crosses the
  threshold.
- Suite support: the US-client VPS suite now exposes and explicitly forwards
  `MINI_VPN_CONTINUOUS_TCP_RELAY` through `sudo -E env`, so a smoke run can
  prove it is exercising the new engine instead of silently falling back to
  legacy.
- Remote prep: the minimal H8 file set was synced to `.27`; remote
  `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`,
  `cargo check -q`, `cargo test -q continuous_relay_reader`,
  `cargo test -q tcp_relay_engine_selector`, and
  `cargo test -q async_byte_queue_close_allows_draining_already_queued_bytes`
  passed after using `bash -lc` for Cargo.
- VPS status: the first reverse-first smoke did not reach mini_vpn or iperf.
  It failed at the suite `sudo -v` preflight because the launch was non-TTY;
  the follow-up `ssh -tt` probe asked for a sudo password and was interrupted
  without entering one. Artifact
  `/tmp/conn/mvpn_knife14h8_continuous_pump_p1_usclient_suite_20260709_173738.tar.gz`
  is preflight-failure evidence only, not throughput evidence.
- VPS result: after rerunning through a real TTY, H8 did reach the tunnel
  window with `engine=continuous_pump`. Reverse-first P1 reached only
  `30.7/29.6 Mbit/s` and the suite remained `FAILED`. Artifact:
  `/tmp/conn/mvpn_knife14h8_continuous_pump_p1_usclient_suite_20260709_174648.tar.gz`;
  local copy:
  `/tmp/mini_vpn_knife14h8_continuous_pump/mvpn_knife14h8_continuous_pump_p1_usclient_suite_20260709_174648.tar.gz`.
- Discriminator: `continuous_queue_wait_events=0` and
  `continuous_queue_wait_max_us=101`, while data stream gaps remained high:
  `data_read_gap_max_ms=3823`, `data_pending_gap_max_ms=3408`, and
  `data_poll_gap_max_ms=3406`. The run also had clean `global_rx_pressure`,
  `local_write_pressure`, TUN drops, close-tail pending, and QUIC
  loss/congestion/blocking.
- Interpretation: B1 falsified ordinary local `RelayReadCredit` as the main
  remaining root. The continuous byte queue was active and not full, but TUIC
  still reported `connection_stream_frames_pending` and multi-second active
  data gaps. Continuous pump is useful as an opt-in diagnostic/product seam,
  not an accepted throughput fix.
- Limitation: B1 releases byte capacity when the dispatcher pops into
  `RelayEvent::Data`; it does not yet hold capacity through
  `SocketCtx.downlink_pending` until smoltcp/`flush_downlink` accepts the
  bytes. Therefore B1 can test the remote-read clocking hypothesis, but it is
  still not the full `QUIC read pump -> byte queue -> smoltcp-owner drain`
  architecture.
- Reusable rule: do not keep iterating on relay-reader read credit, chunk size,
  self-wake timers, broad QUIC windows, VPS buffers, MTU/PLPMTUD, iperf3,
  stale pool, or sing-box liveness for this branch. The next design must
  explain the coupling between `tuic.rs` ordered-stream pending/read readiness
  and `client_tun.rs` local downlink backpressure / `flush_downlink` /
  `pressure_credit_debt` transitions.

## 2026-07-09 - Knife14h9/D2 defines the stream-readiness x local-egress coupling gate

- Design doc:
  `docs/tech/2026-07-09-knife14h9-d2-stream-egress-coupling-design.md`
- Outcome: design-only stage. No runtime code was changed. The D2 plan turns
  the H8 failure into a falsifiable branch: correlate `tuic.rs` ordered stream
  `poll/read/pending` cadence with `client_tun.rs` `downlink_backpressure`,
  `flush_downlink`, `pressure_credit_debt`, headroom, and TUN flush transitions.
- Key gate: D2 must first decide the H4 ordered-chunk state
  (revert/default-off diagnostic gate) and add a per-flow timeline plus local
  coupling harness before another canonical VPS acceptance.
- Capacity rule: `100 Mbit/s` needs about one `64KiB` read every `5.2ms`; any
  active-flow `poll/read/pending` gap above `500ms` is already incompatible
  with a stable `100+ Mbit/s` design. Local harness gates should aim below
  `50ms` under synthetic pressure.
- Reusable rule: H8's byte queue proves reader-to-dispatcher capacity, not
  end-to-end smoltcp/TUN admission. Do not interpret
  `continuous_queue_wait_events=0` as proof that downstream local egress is
  healthy; the permit lifetime currently ends at `RelayEvent::Data`, before
  `SocketCtx.downlink_pending` and `flush_downlink`.

## 2026-07-09 - Knife14h9/D2.0 gates H4 ordered chunk as diagnostic-only

- Updated design doc:
  `docs/tech/2026-07-09-knife14h9-d2-stream-egress-coupling-design.md`
- Code:
  `src/tuic.rs`,
  `scripts/knife14b-usclient-tunnel-suite.sh`
- Outcome: D2.0 is complete locally. TUIC TCP relay mode now reports
  `ordered_join` for the default canonical path, `ordered_chunk` only when
  `MINI_VPN_TUIC_TCP_ORDERED_CHUNK=1` is set, and
  `unordered_reassembly_diag` for the existing unordered diagnostic. The suite
  help/report/sudo launcher now preserves the new ordered-chunk diagnostic
  flag.
- Gates passed:
  `cargo test -q tuic_tcp_relay_mode_defaults_to_ordered_join_and_gates_diagnostics --lib`,
  `cargo test -q format_tuic_tcp_open_line_includes_target_pool_and_id --lib`,
  `cargo test -q ordered_relay_stream_ --lib`,
  `cargo test -q tuic_tcp_relay_mode --lib`,
  `bash -n scripts/knife14b-usclient-tunnel-suite.sh`,
  `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`,
  `cargo fmt --check`, `cargo check -q`,
  `cargo test -q tuic::tests:: --lib`, `cargo test -q --lib`
  (`450` tests), and `git diff --check`.
- Reusable rule: canonical D2 VPS evidence must show `relay_mode=ordered_join`
  unless the run is explicitly marked as an H4 ordered-chunk diagnostic with
  `MINI_VPN_TUIC_TCP_ORDERED_CHUNK=1`. This avoids mixing the rejected H4
  adapter into future default-path conclusions.

## 2026-07-09 - Knife14h9/D2.1a exposes the missing TUIC-to-local join key

- Updated design doc:
  `docs/tech/2026-07-09-knife14h9-d2-stream-egress-coupling-design.md`
- Code:
  `scripts/knife14b-lowrtt-probe.sh`,
  `scripts/knife14b-usclient-tunnel-suite.sh`
- Outcome: the low RTT probe now parses `tcp-stream-service-window` and emits
  `stream_service_window` plus `d2_flow_timeline` summaries next to the
  existing TUIC stream timing metrics. The parent VPS suite report preserves
  those summaries and the raw `tcp-stream-service-window` lines.
- Gates passed:
  `bash scripts/knife14b-lowrtt-probe.sh --self-test`,
  `bash -n scripts/knife14b-lowrtt-probe.sh scripts/knife14b-usclient-tunnel-suite.sh`,
  `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`,
  `cargo test -q stream_service_window_diag_line_includes_remote_and_local_progress --lib`,
  `cargo fmt --check`, `cargo check -q`, `cargo test -q --lib`
  (`450` tests), and `git diff --check`.
- Key finding: existing logs cannot prove a per-flow causal timeline because
  TUIC stream timing is keyed by `conn/id/stream`, while local stream-service
  windows are keyed by `handle/epoch`. The D2 self-test therefore intentionally
  reports `joinable=0 reason=missing_tuic_handle_epoch_bridge`.
- Reusable rule: before another canonical VPS throughput run, add an
  observability-only bridge from local `handle/epoch` to TUIC `conn/id/stream`
  and require the parser self-test to reach `joinable=1`. Do not use
  time-neighbor correlation alone to justify a throughput architecture change.

## 2026-07-09 - Knife14h9/D2.1b adds an observability-only TUIC/local bridge

- Updated design doc:
  `docs/tech/2026-07-09-knife14h9-d2-stream-egress-coupling-design.md`
- Code:
  `src/upstream.rs`,
  `src/client_tun.rs`,
  `src/tuic.rs`,
  `scripts/knife14b-lowrtt-probe.sh`
- Outcome: local TCP `handle/epoch` is now passed to the TUIC `open_tcp`
  diagnostic path through a task-local `TcpRelayOpenDiag`. `tuic-open-tcp`
  lines include `handle=... epoch=...` only when the caller scopes the open
  with that diagnostic context. The low RTT probe can now mark
  `d2_flow_timeline: joinable=1 reason=joined_by_bridge` in the bridge
  self-test.
- Gates passed:
  `cargo test -q tcp_relay_open_diag_context_is_task_scoped --lib`,
  `cargo test -q format_tuic_tcp_open_line --lib`,
  `bash scripts/knife14b-lowrtt-probe.sh --self-test`,
  `bash -n scripts/knife14b-lowrtt-probe.sh scripts/knife14b-usclient-tunnel-suite.sh`,
  `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`,
  `cargo fmt --check`, `cargo check -q`, `cargo test -q --lib`
  (`452` tests), and `git diff --check`.
- Reusable rule: when a generic boxed stream boundary loses transport-specific
  diagnostic identity, prefer a scoped observability context over changing the
  stream data-plane type. The bridge must be tested at both ends: context
  scoping in Rust and `joinable=1` in the log parser.
- Next discriminator: a focused canonical VPS run should first prove real
  reports contain bridged `tuic-open-tcp` lines and
  `d2_flow_timeline: joinable=1`; only then interpret whether low throughput
  aligns with local service-window/backpressure windows or remains a TUIC
  readiness issue.

## 2026-07-09 - Knife14h9/D2.1c VPS bridge gate joins the flow but throughput collapses

- Updated design doc:
  `docs/tech/2026-07-09-knife14h9-d2-stream-egress-coupling-design.md`
- Remote bundle:
  `/tmp/conn/mvpn_knife14h9_d2_bridge_p1_usclient_suite_20260709_190629.tar.gz`
- Local bundle copy:
  `/tmp/mini_vpn_knife14h9_d2_bridge/mvpn_knife14h9_d2_bridge_p1_usclient_suite_20260709_190629.tar.gz`
- Outcome: the focused canonical VPS gate succeeded as an observability
  discriminator and failed as throughput acceptance. Real logs showed bridged
  `tuic-open-tcp` lines with `conn/id/stream` plus `handle/epoch`, and the
  probe emitted `d2_flow_timeline: joinable=1 reason=joined_by_bridge`.
- Throughput: reverse-first P1 was only `1.500/0.615 Mbit/s`
  sender/receiver with `throughput_shape=no_data`.
- Discriminator: data stream poll cadence was healthy
  (`data_poll_gap_max_ms=4`), but reads and pending still had multi-second
  gaps (`data_read_gap_max_ms=6837`, `data_pending_gap_max_ms=6010`) while
  pending causes were fresh/stale connection stream-frame samples. QUIC
  loss/congestion/blocking, TUN drops, global-rx pressure, pressure credit
  debt, headroom limiting, TUN flush failures, and close-tail pending were all
  clean in the summary.
- Local side: stream-service windows were joined and active
  (`stream_service_windows=1018`, `local_accepted_bytes_max=131072`), but
  `local_egress_drain_bytes_max=0` and
  `last_blocked_reason=local_admission_no_progress` point to a sharper
  discriminator: separate actual TUN/smoltcp egress drain progress from merely
  admitting bytes into the local TCP send queue.
- Reusable rule: after D2.1c, do not describe the root as "relay task not
  polling." The stream is polled frequently. The next stage must explain why a
  frequently polled Quinn stream remains pending with connection-level stream
  frames present, and whether those frames belong to the joined stream or are
  unrelated connection traffic.

## 2026-07-09 - Knife14h9/D2.2a makes the joined-flow discriminator falsifiable

- Updated design doc:
  `docs/tech/2026-07-09-knife14h9-d2-stream-egress-coupling-design.md`
- Code:
  `scripts/knife14b-lowrtt-probe.sh`,
  `scripts/knife14b-usclient-tunnel-suite.sh`
- Outcome: the low RTT parser now emits `tuic_stream_frame_delta` from
  `conn_rx_stream_frames_since_read` and
  `conn_rx_stream_frames_since_pending`, emits aggregate
  `local_egress_service`, and includes `local_egress_drain_bytes_max` in
  `d2_flow_timeline`. The parent suite preserves the new summary/raw metrics.
- Gates passed:
  `bash scripts/knife14b-lowrtt-probe.sh --self-test`,
  `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`,
  `bash -n scripts/knife14b-lowrtt-probe.sh scripts/knife14b-usclient-tunnel-suite.sh`,
  `cargo fmt --check`, `cargo check -q`, `cargo test -q --lib`
  (`452` tests), and `git diff --check`.
- Re-summarized artifact:
  `/tmp/mini_vpn_knife14h9_d2_bridge/d2_2a_resummary.md`
- Key finding from the existing D2.1c bundle: the joined data stream had
  `data_poll_gap_max_ms=4` and fresh connection stream-frame deltas
  (`conn_rx_stream_frames_since_pending_max=1201`,
  `fresh_nonzero_since_pending=10`), but local egress drain remained absent
  (`local_egress_drain_bytes_max=0`,
  `local_egress_service.egress_drain_bytes_max=0`) while local admission still
  reached `131072` bytes.
- Reusable rule: a future throughput design must improve or explain one of
  three primary discriminators: local egress drain progress, delivery of fresh
  connection stream frames to the joined data stream, or proof that those fresh
  frames are unrelated to the joined stream. Poll cadence alone is no longer a
  sufficient acceptance signal.

## 2026-07-09 - Knife14h9/D2.2b proves same-stream offset gaps but not a fix

- Updated design doc:
  `docs/tech/2026-07-09-knife14h9-d2-stream-egress-coupling-design.md`
- Code:
  `src/client_tun.rs`,
  `scripts/knife14b-lowrtt-probe.sh`,
  `scripts/knife14b-usclient-tunnel-suite.sh`
- Remote bundle:
  `/tmp/conn/mvpn_knife14c_usclient_suite_20260709_193528.tar.gz`
- Local bundle copy:
  `/tmp/mini_vpn_knife14h9_d2_2b_unordered_diag/mvpn_knife14c_usclient_suite_20260709_193528.tar.gz`
- Outcome: D2.2b fixed a local egress observability blind spot and ran one
  focused `unordered_reassembly_diag` VPS discriminator. Throughput remained
  low (`18.400/17.500 Mbit/s`), so the diagnostic path is not an accepted fix.
- Key evidence: the joined TUIC stream itself produced out-of-order chunks:
  initial `next_offset=0 chunk_offset=14102 gap_bytes=14102`, later
  `max_gap_bytes=1169774`, `max_buffered=1057134B`,
  `unordered_chunks=47070`, `out_of_order_chunks=30287`, `cap_hits=0`.
  This proves fresh connection stream-frame samples can belong to the joined
  data stream; they are not only unrelated connection noise.
- Local egress reinterpretation: the corrected snapshot showed nonzero drain
  (`stream_service_window.local_egress_drain_bytes_max=9215`,
  `local_egress_service.egress_drain_bytes_max=672066`), so D2.2a's
  zero-drain result was an observability false negative. It is still too small
  to explain a healthy `100+ Mbit/s` pipeline.
- Sing-box contrast: official sing-box `f3b0c77` uses a TUIC `net.Conn` and
  two independent copy goroutines via `route/conn.go` and
  `bufio.CopyWithIncreateBuffer`; mini_vpn still spreads reverse data across
  TUIC read task, global queue, TUN/smoltcp main loop, downlink pending,
  `flush_downlink`, dirty handles, and local egress service.
- Reusable rule: the next plausible design is a bounded per-flow copy contract
  with a pure same-stream reassembly state machine, explicit downstream
  permits, and lifecycle/half-close accounting. Do not present
  `MINI_VPN_TUIC_TCP_UNORDERED_REASSEMBLY=1` as a product fix; use it as a
  discriminator until the bounded contract passes local TDD and the VPS
  `>30 Mbit/s` first gate with sub-`500ms` active data gaps.

## 2026-07-09 - Knife14h9/D2.3 permit pump falsified early queue-release as sufficient root

- Updated design doc:
  `docs/tech/2026-07-09-knife14h9-d2-stream-egress-coupling-design.md`
- Code:
  `src/tcp_downlink_pump.rs`, `src/client_tun.rs`, `src/lib.rs`,
  `scripts/knife14b-lowrtt-probe.sh`,
  `scripts/knife14b-usclient-tunnel-suite.sh`
- Remote bundle:
  `/tmp/conn/mvpn_knife14h9_d23_permit_p1_retry1_usclient_suite_20260709_202637.tar.gz`
- Local copy:
  `/tmp/mini_vpn_knife14h9_d23_permit_p1/mvpn_knife14h9_d23_permit_p1_retry1_usclient_suite_20260709_202637.tar.gz`
- Outcome: local gates passed, but VPS reverse-first P1 failed badly:
  sender `0.699 Mbit/s`, receiver `0.489 Mbit/s`.
- Key evidence: `permit_pump` and `unordered_reassembly_diag` were active, and
  downstream permits were not stuck (`downstream_permit_bytes=1867790`,
  `downstream_permit_released_bytes=1867790`). Local pressure stayed clean
  (`global_rx_pressure=0`, `downlink_backpressure=0`,
  `pressure_credit_debt_bytes=0`, `headroom_limited=0`,
  `tun_flush_deferred=0`), TUN drops were `0/0`, QUIC loss/congestion/blocking
  were `0`, and close-tail pending was `0`.
- Falsified branch: returning queue capacity at dispatcher pop was a real
  architectural weakness, but not the sufficient root for the current
  `15-30 Mbit/s` ceiling. The first gate failed with
  `data_read_gap_max_ms=6836` and `data_pending_gap_max_ms=6006` while
  `data_poll_gap_max_ms=3`.
- Reusable rule: do not tune D2.3 permit queue size, permit release cadence, or
  read-credit coupling next. The next design must isolate TUIC/Quinn stream
  readiness and same-stream reassembly ownership below `client_tun.rs`; the
  proof target is continuous contiguous output from the joined TUIC stream
  before local admission is allowed to be blamed again.

## 2026-07-09 - Knife14h9/D2.4 raw TUIC sink proves 100M transport capacity

- Updated design doc:
  `docs/tech/2026-07-09-knife14h9-d2-stream-egress-coupling-design.md`
- Code:
  `src/main.rs` diagnostic mode
  `mini_vpn tuic-tcp-sink-probe <host:port> [duration_secs]`
- Local gates:
  `cargo fmt --check`, `cargo test -q --bin mini_vpn`,
  `cargo test -q --lib`, and `cargo check -q`.
- VPS diagnostic: `.77` iperf3 was briefly stopped, a one-shot byte source was
  bound to the already-open `.77:5201`, and iperf3 was restarted and confirmed
  active/listening afterward.
- Result: with `MINI_VPN_TUIC_TCP_POOL=1`,
  `MINI_VPN_TUIC_TCP_UNORDERED_REASSEMBLY=1`, and a 30s raw sink read,
  mini_vpn received `477101276` bytes at `127.227 Mbit/s`; first receive was
  `3ms`, max read gap was `196ms`, and EOF was false. The target byte source
  reported `481820672` sent bytes.
- Conclusion: mini_vpn's Rust/TUIC/Quinn TCP receive path has demonstrated
  `100+ Mbit/s` capacity on the same `.27 -> .33 -> .77` route. The persistent
  reverse-first low throughput is now attributed to the local TCP/TUN egress
  architecture, not to raw TUIC stream read capacity, VPS buffers, MTU/PLPMTUD,
  stale pools, sing-box, iperf3, or broad QUIC windows.
- Reusable rule: the next throughput design should preserve the D2.4 raw-stream
  cadence through local admission, smoltcp/TUN egress, lifecycle, and bounded
  backpressure. Do not spend the next stage on chunk-size/self-wake/permit-size
  tuning unless new counters contradict this gate.

## 2026-07-09 - Knife14h10/H10a created executable TCP/TUN egress actor contract

- Added spec:
  `docs/tech/2026-07-09-knife14h10-tcp-tun-egress-actor-spec.md`
- Added pure contract model:
  `src/tcp_egress.rs`
- Exported module:
  `src/lib.rs`
- Outcome: no production hot-path behavior changed. The new model defines the
  intended egress actor semantics before wiring it into `client_tun.rs`.
- Local gates passed:
  `cargo test -q tcp_egress --lib`, `cargo fmt --check`,
  `cargo check -q`, `cargo test -q --lib`, and `git diff --check`.
- New tests cover five invariants: repeated backlog admission until window
  target, drain-then-admit in the same service window, no-progress preserving
  pending/lease bytes, explicit drop releasing lease bytes, and rejecting zero
  budgets before service starts.
- Reusable rule: the next implementation step should be H10b branch-by-
  abstraction: wrap existing `flush_downlink`/pending/permit behavior behind a
  production-facing facade that matches `TcpEgressFlowModel`, while keeping
  default behavior unchanged. Do not jump straight to a VPS run until the
  facade proves the same ownership and wake invariants locally.

## 2026-07-09 - Knife14h10/H10b extracted pending/permit facade without switching runtime behavior

- Updated spec:
  `docs/tech/2026-07-09-knife14h10-tcp-tun-egress-actor-spec.md`
- Code:
  `src/tcp_egress.rs`, `src/tcp_downlink_pump.rs`, `src/client_tun.rs`
- Outcome: added `TcpEgressLease`, `TcpEgressPendingQueueView`,
  `TcpEgressPendingSnapshot`, `TcpEgressPendingAppend`, and
  `TcpEgressPendingConsume`; implemented `TcpEgressLease` for
  `DownstreamBytePermit`; routed `SocketCtx` append/consume/clear through the
  facade while keeping external callers and default control flow unchanged.
- New local invariants: append retains only payload-sized lease, prefix consume
  releases only admitted bytes, zero consume/no-progress releases nothing, and
  clear/drop releases all remaining lease.
- Gates passed:
  `cargo test -q tcp_egress --lib`, `cargo test -q --lib`,
  `cargo fmt --check`, `cargo check -q`, and `git diff --check`.
- Reusable rule: H10c can now add a feature-gated actor using the facade rather
  than editing every `downlink_pending` call site at once. The next local gate
  should prove immediate backlog service and external-readiness waits before
  any VPS run.

## 2026-07-09 - Knife14h10/H10c added the feature-gated local egress actor entry

- Updated spec:
  `docs/tech/2026-07-09-knife14h10-tcp-tun-egress-actor-spec.md`
- Code:
  `src/client_tun.rs`
- Outcome: added opt-in `MINI_VPN_D3_EGRESS_ACTOR=1`; default remains disabled.
  In actor mode, remote payload handling now appends bytes and downstream
  permits into `downlink_pending`, records remote progress, and returns without
  inline `flush_downlink`/`send_slice`/TUN flush calls.
- Local ownership proof: the production call site still marks a flow dirty when
  pending bytes remain, and `should_drain_tun_rx_after_remote_payload` treats
  pending bytes as immediate local-egress work even when actor mode returns
  `accepted_bytes=0`. This preserves the single smoltcp/TUN owner while
  bypassing the old remote-payload inline writer.
- Observability: `tcp-local-egress-service` now includes actor-subset counters:
  `egress_actor_windows`, `egress_actor_cycles`,
  `egress_actor_admitted_bytes`, `egress_actor_drain_bytes`,
  `egress_actor_no_progress`, `egress_actor_immediate_wake`, and
  `egress_actor_external_wait`.
- Gates passed:
  `cargo test -q d3_egress_actor --lib`,
  `cargo test -q local_egress_service_diag_reports_egress_actor_subset --lib`,
  `cargo test -q tcp_egress --lib`, `cargo test -q --lib` (`471` tests),
  `cargo fmt --check`, `cargo check -q`, and `git diff --check`.
- Reusable rule: H10c is not a throughput proof. It is the first executable
  actor wiring slice that makes H10d VPS first-gate evidence meaningful. If H10d
  fails, compare actor `immediate_wake`, admitted/drain bytes, no-progress, and
  external-wait counters before changing TUIC read cadence, chunk size, VPS
  config, MTU, or queue sizes.

## 2026-07-09 - Knife14h10/H10d proved D3 actor can exceed 100M but did not pass clean acceptance

- Updated spec:
  `docs/tech/2026-07-09-knife14h10-tcp-tun-egress-actor-spec.md`
- Code exercised:
  `src/client_tun.rs`, `src/tcp_egress.rs`, `src/tcp_downlink_pump.rs`,
  `src/lib.rs`, `scripts/knife14b-usclient-tunnel-suite.sh`
- Remote bundle:
  `/tmp/conn/mvpn_knife14h10c_d3_actor_p1_usclient_suite_20260709_215654.tar.gz`
- Local copy:
  `/tmp/mini_vpn_knife14h10c_d3_actor/mvpn_knife14h10c_d3_actor_p1_usclient_suite_20260709_215654.tar.gz`
- Outcome: `MINI_VPN_D3_EGRESS_ACTOR=1` was active and reverse-first P1 reached
  `iperf_receiver_mbps=102.000`; the interval profile showed
  `overall_avg_mbps=170.172` and `prefix_avg_mbps=189.130`.
- Actor proof: `egress_actor_admitted_bytes=620309763`,
  `egress_actor_drain_bytes=139651239`, and
  `egress_actor_immediate_wake=31938`. This is the first Knife14 run where the
  mini_vpn local TCP/TUN path demonstrates `100+ Mbit/s` capability on the
  `.27 -> .33 -> .77` route.
- Non-roots stayed non-root: QUIC loss/congestion/blocking remained clean
  (`max_lost_bytes_delta=0`, `max_congestion_events_delta=0`, no tx/rx blocked
  deltas), and global/local channel pressure stayed clean.
- Not accepted yet: the iperf command exited by timeout (`exit=124`) and the
  tail had `tun_tx_dropped_delta=97`, `pending_at_close=421577`, and
  `terminal_pending_reap=421577`.
- Reusable rule: D3 actor answers the architecture-capacity question: yes,
  mini_vpn can exceed `100M` when remote payload inline flush is removed and
  local egress is serviced by the smoltcp/TUN owner. The next work is not more
  TUIC chunk/read-credit/VPS tuning; it is close-tail and TUN egress-drop
  cleanup under the now-high-throughput actor path.

## 2026-07-09 - Knife14h10/H10d2 added actor clean-headroom local pacing

- Updated spec:
  `docs/tech/2026-07-09-knife14h10-tcp-tun-egress-actor-spec.md`
- Code:
  `src/client_tun.rs`
- Outcome: D3 actor flows now mark their downlink path as clean-headroom-only
  after remote payload bytes are appended into `downlink_pending`. In that
  mode, `flush_downlink` does not spend `DownlinkEgressClock` drain credit past
  `tx_queue_flush_threshold`; legacy/default behavior still spends drain credit
  up to the old credit edge.
- Lifecycle proof: `rearm_socket` clears the clean-headroom flag, so the actor
  pacing policy cannot leak from one flow into the next.
- Local proof: added a policy test showing legacy credit reaches
  `tx_queue_credit_spend_threshold` while D3 actor clean-headroom leaves the
  same bytes pending at `tx_queue_flush_threshold`.
- Gates passed:
  `cargo test -q d3_actor_clean_headroom_policy_does_not_spend_drain_credit_to_credit_edge --lib`,
  `cargo test -q d3_egress_actor --lib`, `cargo test -q tcp_egress --lib`,
  `cargo test -q --lib` (`472` tests), `cargo check -q`,
  `cargo fmt --check`, and `git diff --check`.
- Reusable rule: after H10d, the target is not more raw throughput; it is
  keeping the high-throughput actor path below local TUN/qdisc drop pressure.
  The next VPS run should falsify this directly: if `tun_tx_dropped_delta`,
  `pending_at_close`, and `terminal_pending_reap` do not fall to `0`, the
  bottleneck is not remote-read cadence or legacy credit edge alone.

## 2026-07-09 - Knife14h10/H10d2 VPS falsified pure clean-headroom actor pacing

- Remote bundle:
  `/tmp/conn/mvpn_knife14h10d2_actor_clean_headroom_usclient_suite_20260709_221940.tar.gz`
- Local copy:
  `/tmp/mini_vpn_knife14h10d2_actor_clean_headroom/mvpn_knife14h10d2_actor_clean_headroom_usclient_suite_20260709_221940.tar.gz`
- Outcome: with `MINI_VPN_D3_EGRESS_ACTOR=1`, reverse-first P1 regressed to
  `sender=11.6 Mbit/s` and `receiver=9.72 Mbit/s`.
- What improved: `tun_tx_dropped_delta=0`, `pending_at_close=0`, and
  `terminal_pending_reap=0`.
- What broke capacity: the actor path planned no drain credit
  (`drain_credit_planned_bytes=0`), capped `send_queue_max` at `449999`,
  and accumulated `headroom_limited_calls=13881` /
  `headroom_deferred_bytes=16864356`. H10d had used credit up to
  `send_queue_max=557386`, accepted `620309763` actor bytes, and reached
  `100M+`, but paid with a qdisc drop and terminal pending.
- Reusable rule: the accepted H10 design cannot be "no credit above clean
  headroom." It needs a hybrid actor policy: keep a single local egress owner,
  but spend bounded, recent-drain-backed credit above clean headroom, with fast
  drop/hard-pause backoff. Do not continue with pure clean-headroom-only as a
  product fix.

## 2026-07-09 - Knife14h10/H10d3 added actor hybrid target-edge credit

- Updated spec:
  `docs/tech/2026-07-09-knife14h10-tcp-tun-egress-actor-spec.md`
- Code:
  `src/client_tun.rs`
- Outcome: D3 actor flows now use `ActorHybridCredit`: they can spend recent
  `DownlinkEgressClock` drain credit above clean headroom, but only up to
  `tx_queue_egress_target_threshold`. Legacy/default flows still use the old
  credit edge, while the pure clean-headroom policy remains test-only as the
  H10d2 rejected discriminator.
- Drop safety: active TUN drop debt blocks actor drain-credit planning and
  forces actor admission back to clean headroom. Close-drain terminal expansion
  remains legacy-only.
- Local proof: added focused tests for target-edge actor credit and drop-debt
  backoff.
- Gates passed:
  `cargo test -q d3_actor_hybrid_policy --lib`,
  `cargo test -q d3_egress_actor --lib`, `cargo test -q --lib` (`474` tests),
  `cargo check -q`, `cargo fmt --check`, and `git diff --check`.
- Reusable rule: H10d3 is the first local design after H10d2 that preserves both
  sides of the requirement in code: bounded elasticity for `100M+` capacity and
  an explicit drop-debt path back to clean headroom. The next VPS run should be
  one focused reverse-first P1 with `MINI_VPN_D3_EGRESS_ACTOR=1`.

## 2026-07-09 - Knife14h10/H10d3 target-edge actor hybrid was safe but not sufficient

- Remote bundle:
  `/tmp/conn/mvpn_knife14h10d3_actor_hybrid_target_usclient_suite_20260709_224720.tar.gz`
- Local copy:
  `/tmp/mini_vpn_knife14h10d3_actor_hybrid_target/mvpn_knife14h10d3_actor_hybrid_target_usclient_suite_20260709_224720.tar.gz`
- Outcome: `MINI_VPN_D3_EGRESS_ACTOR=1` reverse-first P1 reached
  `sender=32.2 Mbit/s` and `receiver=29.8 Mbit/s`, improving over H10d2 but
  still far below the `100M+` gate.
- What worked: `tun_tx_dropped_delta=0`, `pending_at_close=0`, and
  `terminal_pending_reap=0`; QUIC loss/congestion/blocking remained non-root.
- What failed: local pressure/backpressure stayed active with
  `send_queue_max=503692`, `pending_total_max=454619`,
  `headroom_limited=18275`, `hard_edge_guard_limited=18275`, and
  `pressure_credit_debt_bytes=122727`.
- Reusable rule: target-edge hybrid is safe but still too conservative. The
  next actor credit design must be more elastic than target-edge but retain
  active-drop-debt clean-headroom backoff and legacy-only close-drain expansion.

## 2026-07-09 - Knife14h10/H10d4 actor adaptive credit did not clear Gate A

- Remote bundle:
  `/tmp/conn/mvpn_knife14h10d4_actor_adaptive_credit_usclient_suite_20260709_231822.tar.gz`
- Local copy:
  `/tmp/mini_vpn_knife14h10d4_actor_adaptive_credit/mvpn_knife14h10d4_actor_adaptive_credit_usclient_suite_20260709_231822.tar.gz`
- Outcome: `MINI_VPN_D3_EGRESS_ACTOR=1` reverse-first P1 reached
  `sender=22.5 Mbit/s` and `receiver=20.6 Mbit/s`, below both H10d3
  (`29.8 Mbit/s`) and the Gate A `>30M` target.
- What worked: `tun_tx_dropped_delta=0`, `pending_at_close=0`, and
  `terminal_pending_reap=0`; QUIC loss/congestion/blocking remained non-root.
- What the run proved: the active limiter was not the target-to-credit actor
  edge. `send_queue_max=390896` stayed below `tx_queue_flush_high=449999`,
  with `headroom_limited=0`, `pressure_credit_debt_bytes=0`, and
  `hard_edge_guard_limited=0`.
- Remaining signal: TUIC data stream cadence still had multi-second gaps
  (`data_read_gap_max_ms=3849`, `data_pending_gap_max_ms=3848`,
  `data_poll_gap_max_ms=3394`) while local pressure, TUN drops, close-tail,
  global_rx pressure, and QUIC path counters were clean.
- Reusable rule: stop spending stages on actor credit threshold variants until
  there is a discriminator for the TUIC data read/pending/poll gap under a
  clean local egress path.

## 2026-07-09 - Knife14h10/H10d15 native permit read floor proved 100M+ capacity

- Code: `src/client_tun.rs`.
- Local proof: added `native_tcp_read_service_floor_lifts_tiny_credit_only_when_enabled`
  and `native_permit_relay_read_floor_services_full_quantum_from_tiny_credit`.
  Focused native permit, D6 permit rearm, ordered permit clamp, `cargo check -q`,
  suite self-test, and non-sandbox `cargo test -q --lib` all passed.
- Remote bundle:
  `/tmp/conn/mvpn_knife14h10d15_native_read_floor_gatea_usclient_suite_20260710_070433.tar.gz`
- Local copy:
  `/tmp/mini_vpn_knife14h10d15_native_read_floor/mvpn_knife14h10d15_native_read_floor_gatea_usclient_suite_20260710_070433.tar.gz`
- Outcome: reverse-first P1 reached `sender=186 Mbit/s` and
  `receiver=185 Mbit/s`, close to the mature sing-box client baseline and well
  above the `100M+` capacity gate.
- Mechanism proven: data flow used `engine=native_permit_pump` with
  `native_read_floor=true`; D6 bounded permit ownership let remote QUIC reads
  stay serviced while local egress drained permits. The maximum data-stream
  read gap dropped to sub-second scale (`tcp-relay max_remote_read_gap_ms=420`,
  `tuic max_read_gap_ms=564`) instead of the earlier multi-second H10d14 gap.
- What stayed healthy during the main throughput window: QUIC loss and
  congestion stayed zero, global_rx pressure stayed zero, local write pressure
  stayed zero, and downlink pressure debt stayed zero.
- Reusable rule: the missing architectural capability was not another VPS,
  MTU, stale-pool, broad-window, or chunk-size tweak. mini_vpn needed a
  sing-box-like separation where the native QUIC read side keeps a service-sized
  drain into a bounded byte/permit queue, instead of letting transient local
  read credit shrink the remote read service down to tiny ACK-sized chunks.
- Not final acceptance yet: the same run ended with a TUN drop/dead-slot tail.
  Treat H10d15 as the first strong capacity proof and proceed to stability
  cleanup plus at least two clean `>100M` repeats before accepting the fix.

## 2026-07-09 - H10d16 architecture approved after post-H10d15 code review

- Approved source of truth:
  `docs/tech/2026-07-09-knife14h10d16-byte-owned-egress-architecture-spec.md`
  and
  `docs/tech/2026-07-09-knife14h10d16-byte-owned-egress-implementation-plan.md`.
- The review changed the next stage from a narrow drop-aware read-floor cleanup
  into an end-to-end ownership closure. H10d15's independent read pump is kept,
  but every Quinn read must reserve bytes before polling and commit them into
  one per-flow leased queue.
- The final actor contract requires readiness-only global events,
  actor-exclusive D16 `send_slice`, and a `DrainOnly` state that stops new reads
  and admission while continuing ACK/TUN RX, poll, flush, and permit release.
- Reusable rule: a message-count bounded channel is not a sufficient byte bound
  for high-concurrency transport payload, especially when the producer ignores
  its requested maximum. Capacity permits must cover all payload staging from
  the first transport read through terminal release.
- Stage position remains stage 8: capacity is proven, but clean Gate A failed.
  Reopen stages 3-7 locally before another VPS run; Gate A now requires one
  clean `>150 Mbit/s` result and Gate B targets a clean three-run median near
  `170 Mbit/s`.

## 2026-07-09 - H10d16 Task 2 closed pre-read byte reservation locally

- Code: `src/tcp_downlink_pump.rs`; reachability test: `src/tuic.rs`.
- Outcome: `AsyncLeasedByteFlowQueue` now counts `reserved_bytes` before a
  remote read can be armed. `ByteQueueReadReservation` refunds on drop/error,
  rejects over-commit, converts only produced bytes into queued ownership, and
  immediately refunds unused reservation capacity.
- Conservation proof: focused tests cover reserve, commit, queue, lease,
  partial release, and drop while asserting
  `reserved + queued + leased <= capacity` throughout.
- Review fix: `reserve_read_up_to` registers and enables its `Notify` waiter
  before inspecting capacity, preventing a `notify_waiters` release from being
  lost between mutex unlock and the first future poll.
- Local gates: `7` reservation tests, `15` `tcp_downlink_pump` tests,
  `cargo test -q --lib` (`525` tests), `cargo check -q`, `cargo fmt --check`,
  `git diff --check`, and suite self-test all passed.
- Commit status: intentionally uncommitted. A staged-tree check proved the
  pre-D16 source baseline depends on separate Cargo/QUIC/REALITY working-tree
  changes, so the user approved continuing local D16 work without broadening
  commit scope.
- Reusable rule: a byte reservation wait must close both safety and liveness:
  count capacity before transport polling, release it by RAII, and register
  capacity wakeups before checking the predicate.

## 2026-07-09 - H10d16 Task 3 added a direct byte-bounded ordered reader

- Code: `src/tuic.rs`, with the transport-neutral reader contract documented
  in `src/upstream.rs`.
- Outcome: `TuicNativeOrderedReader` polls one owner-held Quinn receive stream
  directly through a cancel-safe ordered adapter. It has no nested payload
  task, message-count channel, or shared `Arc<Mutex<RecvStream>>`.
- Byte contract: a `128 KiB` caller limit reaches the adapter unchanged, an
  oversized returned chunk is rejected, and ordered offsets must remain
  contiguous from zero.
- Pending contract: repeated polls operate on one pending read operation; no
  second concurrent read is started while the first is pending.
- Old-path audit: the H10d15 `TuicNativeOrderedPumpReader` and its channel stay
  available only as pre-D16 diagnostic code. The new direct constructor is
  intentionally not selected until the Task 9 D16 composition gate.
- Local gates: four `d16_direct_ordered_reader` tests, legacy native ordered
  tests, H4 ordered relay tests, all `80` TUIC tests, `cargo test -q --lib`
  (`529` tests), check/fmt/diff, and suite self-test passed.
- Reusable rule: ordered transport adapters should propagate the byte budget
  to the actual read call and verify both length and offset at their boundary;
  channel removal is necessary but not production-reachable until composition
  selects the direct reader.

## 2026-07-09 - H10d16 Task 4 made the per-flow queue readiness-owned

- Code: `src/tcp_downlink_pump.rs` and the D16 composition seams in
  `src/client_tun.rs`.
- Outcome: the leased queue now has a nonblocking `Data / Empty / Closed` poll,
  coalesced `wake_pending` state, and idempotent close readiness. `Closed` is
  visible only after queued and reserved bytes are zero; leased bytes remain
  accounted until the local egress path releases their permits.
- Reader contract: the D16 reader reserves before polling, commits directly to
  the per-flow queue, emits only `DataReady`, refunds a pending reservation when
  read credit pauses, and turns clean transport EOF into queue closure rather
  than a payload-bearing or clean terminal event.
- Progress proof: four `64 KiB` remote chunks coalesce to one readiness event;
  that event drives two `128 KiB` actor drains, after which observing open-empty
  rearms the next wake. Ownership remains at or below the `512 KiB` flow cap.
- Old-path audit: legacy `RelayEvent::Data` and the permit dispatcher remain for
  diagnostics, while the D16 reader bypasses both. Production selection remains
  deliberately deferred to the Task 9 composition gate.
- Local gates: `8` D16 focused tests, `19` queue tests, `4` permit tests,
  `cargo test -q --lib` (`534` tests), `cargo check -q`, format/diff checks, and
  suite self-test all passed.
- Reusable rule: readiness coalescing must be tied to observing the queue
  predicate, not merely consuming an event. Otherwise one wake cannot safely
  drive multiple bounded actor quanta or rearm after an exact-quantum drain.

## 2026-07-09 - H10d16 Task 5 made D16 downlink admission actor-exclusive

- Code: `src/client_tun.rs`, with a synchronous queue snapshot seam in
  `src/tcp_downlink_pump.rs` for dirty-work retention.
- Outcome: every downlink admission call now carries an explicit
  `Legacy / EgressActor / ControlOnly` origin. D16 flows may reach the sole
  `send_slice` primitive only through `EgressActor`; non-D16 flows normalize to
  `Legacy` and preserve the existing behavior.
- Accounting: per-flow and aggregate diagnostics expose exact
  `actor_admitted_bytes` and `actor_bypass_admitted_bytes`; the local-egress
  diagnostic line also reports the bypass total. Focused tests prove timer and
  TUN control passes execute no admission, actor accepted bytes equal total D16
  `send_slice` accepted bytes, and bypass remains zero.
- Review fix: `DataReady` initially marked a D16 handle dirty, but a later
  `ControlOnly` pass could remove it because legacy dirty retention inspected
  pending/send-queue state only. Dirty retention now treats queued D16 bytes as
  actor work until the actor drains them.
- Old-path audit: there is one `tcp_socket.send_slice` call, inside
  `flush_downlink`; both listener maintenance and legacy remote-payload fast
  paths enter it through the origin gate. Legacy payload events remain
  diagnostic-only for D16 and do not bypass the guard.
- Local gates: the four planned D16 actor tests plus the dirty-retention test,
  six D3 actor regressions, `cargo test -q --lib` (`540` tests), check/fmt/diff,
  and suite self-test all passed.
- Reusable rule: exclusive ownership requires guarding both the write primitive
  and the scheduler predicate that keeps its work reachable; an origin enum
  alone is insufficient if a control pass can discard readiness before the
  owner runs.

## 2026-07-09 - H10d16 Task 6 separated pressure pause from local drain

- Code: pure phase/permission state machine in `src/tcp_egress.rs`; per-flow
  phase, admission/read-credit coupling, and local-drain integration in
  `src/client_tun.rs`.
- Outcome: D16 now uses `Running -> DrainOnly -> Recovery`. Running permits up
  to `512 KiB`; DrainOnly publishes read credit `paused/0` and blocks queue and
  smoltcp admission while preserving TUN RX, `iface.poll`, `flush_tx`, and
  permit release; Recovery caps each read/admission quantum at `128 KiB` and
  requires four clean cycles before Running.
- Pressure inputs: active drop debt, TUN hard pause, the per-flow smoltcp hard
  pause edge, and terminal closed/no-send enter DrainOnly. Recovery begins only
  below the low watermark after a successful local drain cycle and resets
  immediately on renewed hard pressure or drop evidence.
- Cancellation proof: a phase credit change remains in the pending Quinn-read
  `select!`; entering DrainOnly cancels that future and refunds the pre-read
  reservation before any later read can arm.
- Review fixes: phase-non-Running flows remain dirty until recovery completes,
  preventing an empty DrainOnly pass from losing the only future credit wake.
  A successful poll/flush cycle counts as drain progress even when byte drain is
  zero, so a clean idle flow cannot remain paused forever; failed flushes do not
  advance recovery.
- Observability: aggregate diagnostics expose current Running/DrainOnly/Recovery
  flow counts, transition and recovery-reset counts, DrainOnly cycles, and
  DrainOnly drain bytes.
- Capacity reachability: the `128 KiB` Recovery quantum at the existing `5 ms`
  service cadence is about `209.7 Mbit/s` before overhead, while Running can use
  the available per-flow reservation up to `512 KiB`; this remains a plausible
  sufficient path for the later `170 Mbit/s` gate rather than a timer-limited
  necessary-only fix.
- Local gates: `22` D16 tests, `15` `tcp_egress` tests, `6` D3 actor tests,
  `cargo test -q --lib` (`548` tests), check/fmt/diff, and suite self-test all
  passed.
- Reusable rule: pressure feedback must disable production, not cleanup. A
  drain-only state must keep the scheduler work item alive long enough to
  publish bounded recovery credit after local egress becomes clean.

## 2026-07-09 - H10d16 Task 7 ordered clean EOF behind owned bytes

- Code: queue close/terminal-drop semantics in `src/tcp_downlink_pump.rs` and
  D16 EOF/lifecycle integration in `src/client_tun.rs`.
- Outcome: the D16 reader still emits only coalesced `DataReady` for clean EOF.
  The actor installs `pending_relay_close(remote_eof)` only after observing the
  queue closed with queued and reserved bytes at zero. Local FIN then waits for
  both local pending bytes and TUN-inflight permit bytes to reach zero.
- Half-close proof: a local `Finish` closes only the remote write half; the
  attached reverse queue remains actor-drainable and retains exact permits.
- Terminal proof: explicit local termination closes and drops queued bytes,
  releases pending and inflight leases, and records their exact sum once.
  The deterministic case released `128 KiB` (`64 KiB` queued plus `64 KiB`
  leased); a second drop released and counted zero.
- Epoch proof: rearm terminal-drops the old reservoir, detaches it, resets the
  phase, increments the epoch, and rejects both old-epoch and new-epoch late
  readiness while no D16 queue is attached.
- Observability: periodic and close diagnostics include actor admitted/bypass
  plus `permit_terminal_drop_bytes/events`; close taxonomy is captured before
  ownership release so terminal-pending evidence is not erased by cleanup.
- Local gates: the five planned D16 lifecycle tests, queue close/reservation and
  idempotent terminal-drop tests, `cargo test -q --lib` (`556` tests),
  check/fmt/diff, and suite self-test all passed.
- Reusable rule: transport EOF is queue state, not an independent event. Make
  it visible only after every preceding ownership state has either committed to
  egress or been explicitly and exactly dropped.

## 2026-07-09 - H10d16 Task 8 added the global budget and integrated harness

- Code: shared byte budget in `src/tcp_downlink_pump.rs`, a feature-gated
  production-seam flow driver in `src/client_tun.rs`, and deterministic
  scenarios in `src/harness.rs`.
- Outcome: every shared D16 reservation now counts against both the `512 KiB`
  per-flow cap and a demand-allocated process budget whose default is `64 MiB`.
  Commit moves global reservation to owned bytes; unused reservation, RAII
  cancellation, TUN permit release, terminal queued drop, direct shared push,
  and final queue Drop all refund the exact corresponding state.
- Integrated capacity proof: `6000` virtual `5 ms` steps at `128 KiB` over
  `30 s` delivered the full stream and clean EOF at about `209.7 Mbit/s`, with
  actor bypass, simulated TUN drops, and close egress all zero. Per-flow high
  water stayed below `512 KiB` and global high water below `64 MiB`.
- Pressure/fairness proof: one DrainOnly flow stopped read/admission while
  continuing drain; another flow sharing the budget still admitted data. A
  saturated multi-flow budget kept all waiters pending without spinning, then
  woke every waiter as other flows released ownership. Four clean cycles
  restored Running after pressure cleared.
- Review fix: the inherited direct `push()` path initially bypassed global
  accounting. Shared queues now wait on both enabled flow/global notifications
  and acquire global owned bytes before enqueue, preventing an alternate
  insertion path or final Drop from corrupting another flow's ledger.
- Local gates: `3` global-budget tests, `34` feature D16 tests,
  `cargo test -q --lib` (`559` tests), full `cargo test -q --features harness`
  (`564` library tests plus existing integration targets), normal/feature
  checks, fmt/diff, and suite self-test all passed.
- Reusable rule: a process cap is part of the ownership ledger, not a separate
  advisory metric. Every insertion and every terminal/destructor path must move
  both the per-flow and global counters together.

## 2026-07-10 - H10d16 Task 9 exposed one complete runtime profile

- Code: the single `MINI_VPN_H10D16_BYTE_OWNED_EGRESS=1` gate now selects the
  direct ordered Quinn reader, process-shared byte budget, per-flow owned queue,
  readiness-only relay, D16 actor, phase feedback, and EOF lifecycle without
  requiring D3/D4/D5/D6/D15 flags.
- Startup/suite contract: the client reports the fixed `512 KiB` per-flow cap,
  `64 MiB` global cap, and `128 KiB` actor quantum. The US-client suite defaults
  the gate off, documents it, exports it, records it in the environment summary,
  and passes it through the actual `sudo -E env` launch without exposing secret
  configuration.
- Production reachability: `OpenedTcpRelay::NativeByteOwned` is returned only
  by the D16 TUIC selector, attaches the shared owned queue at the socket epoch,
  and emits `DataReady` rather than payload-bearing `RelayEvent::Data`. The
  readiness branch immediately invokes the bounded local egress actor.
- Focused proof: four `h10d16` configuration/composition tests passed, including
  priority over older diagnostic flags and a mock native relay that attached the
  queue, coalesced payload into one readiness event, and represented clean EOF
  only by closed queue state.
- Reusable rule: a product architecture profile must be one reproducible gate;
  older experimental flags may remain available for diagnostics but cannot be
  hidden prerequisites for the selected production path.

## 2026-07-10 - H10d16 Task 10 local review closed terminal ordering

- Review finding: on writer failure or idle timeout, the D16 supervisor closed
  the queue and published terminal events before cancelling a pending reserved
  Quinn read. A deterministic red test observed `reserved_bytes=65536` after
  queue close, so terminal visibility did not yet imply a closed ownership
  ledger and reader/supervisor producers could race.
- Fix: terminal supervisor paths now stop and join the reader first. RAII refunds
  the pending reservation, any prior readiness send finishes before the join,
  and only then does the supervisor publish its single terminal `Closed` event.
  The activity channel also disables its select branch after sender closure, so
  reader completion cannot create an empty-channel hot loop.
- Review result: every D16 Quinn read is preceded by a per-flow/global byte
  reservation; `global_rx` carries readiness rather than D16 payload; the only
  production `send_slice` remains behind actor-origin admission; DrainOnly keeps
  poll/flush/permit release active; clean EOF waits for queue, pending, and
  inflight ownership to drain; explicit terminal drop remains exact and
  idempotent.
- Final local gates: `36` D16 focused tests, normal library `564/564`, harness
  library `569/569`, integration targets `2/2` and `10 passed` with `4` existing
  non-D16 ignored tests, formatting/check/diff gates, and suite self-test all
  passed.
- Reusable rule: terminal publication is a ledger barrier. Cancel and join the
  read owner before making terminal state visible; do not rely on queue close to
  retroactively refund an in-flight reservation.

## 2026-07-10 - H10d16 Gate A exposed a missing bidirectional harness seam

- Artifact:
  `/tmp/mini_vpn_knife14h10d16_byte_owned_gatea/mvpn_knife14h10d16_byte_owned_gatea_usclient_suite_20260710_133358.tar.gz`.
- Outcome: the single reverse-first P1 had no throughput samples and timed out.
  D16 reached its production selector, received the first remote byte in `3ms`,
  admitted/flushed it with actor bypass and TUN drops both zero, then saw no
  useful remote data for `39995ms`.
- Cross-host proof: sing-box opened the target connection without path/auth
  error; iperf3 accepted it and later reported client termination. QUIC loss,
  congestion, and blocking were zero, so transport capacity was not exercised.
- Local discriminator: the one-byte response was written to TUN and its permit
  released, but the smoltcp socket still closed with `send_queue=1` and
  `close_egress_bytes=1`. The target waited for the next iperf control message,
  so the unproven seam is TUN flush -> local ACK/control -> relay writer.
- Reusable rule: a reverse-throughput harness must first prove a bidirectional
  control handshake. A one-way byte-owned capacity simulation can validate
  ownership math while still missing the ACK/uplink loop required to start the
  data stream.

## 2026-07-10 - H10d16 bidirectional tracer closed the startup loop locally

- Red proof: a full run-event-loop harness used two real smoltcp stacks and a
  native byte-owned mock relay. After the first TCP payload flush it suppressed
  the async TUN wait edge, matching Gate A's first-response/unacknowledged-tail
  shape. The first response arrived and TCP opens stayed `1`, but the second
  control message did not echo within `2s`.
- Root cause: D16 readiness invoked the actor but omitted the existing bounded
  active-flow TUN RX follow-up used by the legacy payload branch. Once the first
  response was flushed and the flow left `dirty`, the ACK/control packet could
  depend entirely on an async wait edge.
- Green fix: useful D16 actor progress now arms the existing `10ms` active-flow
  window. The same test completed the one-byte response plus `1532B` second
  control round-trip intact in about `20ms`. No new timer, value change, D3
  self-wake, MTU, QUIC, chunk, or capacity adjustment was introduced.
- Observability: D16 relay close diagnostics now report writer progress
  events/bytes, max writer wait, and final queued/leased/reserved ledger counts;
  they never log payload.
- Local gates: D16 focused feature tests `40/40`, normal library `564/564`,
  feature library `570/570`, integration targets `2/2` and `10 passed` with `4`
  existing non-D16 ignored tests, plus check/fmt/diff and suite self-test.
- Reusable rule: after actor-owned downlink flush, keep bounded local ingress
  service alive long enough to ingest its ACK/control consequence. Readiness
  robustness must not depend on one cancellable async wait edge.

## 2026-07-10 - H10d16 replacement Gate A isolated the real Quinn wake seam

- Artifact:
  `/tmp/mini_vpn_knife14h10d16_bidirectional_followup_gatea_local/mvpn_knife14h10d16_bidirectional_followup_gatea_usclient_suite_20260710_140124.tar.gz`.
- Outcome: the authorized replacement `20s` reverse-first P1 completed with
  `0.000 Mbit/s` receiver. Gate B was not started. TUN drops, actor bypass,
  QUIC loss, and congestion were zero, but terminal pending was `106710B` and
  close egress was `557386B`.
- Progress over the first Gate A: the control flow received its first byte in
  `3ms`, opened the second iperf data flow, made six writer-progress events,
  and closed with an exact empty D16 queue ledger. The bidirectional startup
  fix therefore moved the failure beyond the original one-byte control stall.
- Decisive discriminator: the target sent `10.5 MiB` in its first second and
  client Quinn metrics accumulated about `18.3 MiB` of receive traffic and
  `7225` then `7692` stream frames, but the D16 data reader's first chunk did
  not complete until `20004ms`. Once awake, it read about `800 KiB` in `596`
  reads and the actor admitted about `693 KiB`.
- Test-gap finding: the test named
  `d16_direct_ordered_reader_keeps_one_pending_read_future` uses a mock whose
  own `in_flight` bit persists across polls. It does not prove delayed wake and
  re-poll behavior for the production Quinn `RecvStream` adapter.
- Reusable rule: connection-level QUIC stream frames plus an application read
  poll gap distinguish transport delivery from application read service. Lock
  this boundary down with a real local Quinn delayed-write test before changing
  any timer, self-wake, window, MTU, pool, or chunk parameter.

## 2026-07-10 - H10d16 Running credit must include its own armed reservation

- Real-Quinn discriminator: the exact direct ordered adapter woke and returned
  payload written `20ms` after its first Pending poll. Quinn wake registration
  was not the active root of the replacement Gate A stall.
- Red production seam: initial Running credit and per-flow capacity were both
  `512 KiB`. After the reader reserved the full credit, feedback computed only
  unreserved availability and published `paused=true, max_batch_bytes=0`.
  Cancelling the read refunded the reservation, but no `DataReady` existed to
  publish resume, reproducing the eventless `20s` stall.
- Fix: D16 feedback now derives the already-armed opportunity as
  `available_bytes + own_reserved_bytes`. The value remains at or below flow
  capacity, and actual reads still require exact per-flow/global reservation.
  Recovery remains capped at `128 KiB`; DrainOnly still cancels and refunds.
- Local proof: D16 focused `39/39`, normal library `567/567`, harness library
  `573/573`, integration `2/2` and `10 passed` with `4` existing ignores, plus
  fmt/check/diff and suite self-test.
- Reusable rule: capacity feedback must distinguish bytes owned by the pending
  operation itself from bytes owned by queued or downstream work. Otherwise an
  exact full-capacity reservation can revoke its own permission and create a
  wake-free deadlock while the transport continues buffering data.

## 2026-07-10 - Target evidence now bypasses the data-plane route under test

- The known Knife14 topology defaults Target management SSH through the Exit
  host using an explicit proxy command that carries Exit identity, port, and
  host-key policy. Target identity and host-key settings remain separate.
- Each raw server-evidence SSH command is wrapped in a `20s` outer timeout, so
  an evidence failure cannot indefinitely block cleanup or bundle creation.
- The suite self-test covers default proxy selection, explicit override
  preservation, disabled/unknown topology behavior, Exit option propagation,
  help text, and timeout reporting.
- Reusable rule: never collect management evidence through the same target
  route being tested. Evidence is observational and must not create additional
  data-plane flows or own the suite lifecycle.

## 2026-07-10 - D16 drop recovery must account for drain across feedback samples

- The credit-rearm Gate A proved the direct reader now starts promptly and the
  single actor admitted 60,928,613 bytes with zero bypass, but one 2,029-packet
  TUN drop moved both flows into DrainOnly and throughput stopped.
- The drop edge recorded 994,674 bytes of total pressure and installed 122,727
  bytes of global drop debt. The next feedback sample recorded zero pressure,
  which is positive aggregate drain evidence, but the old per-flow send-limit
  clocks did not observe that decrease and therefore could not pay the debt.
- DrainOnly then prevented new admission, so no later per-flow queue decrease
  could be created. Both flows remained DrainOnly for 8,283 cycles even though
  queues were empty and feedback had resumed.
- Reusable rule: a global circuit breaker must consume drain evidence at the
  same scope at which pressure is observed. Preserve the drop-episode pressure
  baseline, apply its measured decrease once to debt, and pass the same clean
  drain edge into flow Recovery without minting new admission credit.
- A completed zero-byte poll/flush cycle is service, not drain progress. Keep
  `drain_only_cycles` observable, but require strictly positive drain evidence
  for the ordinary Recovery transition.

## 2026-07-10 - Task 11A closed the bounded TUN RX and EOF gates locally

- Root cause: the first backlog latch and its later one-clean-edge recovery
  tracked only device readiness. They did not prove that payload already
  admitted for each flow had produced and drained its ACK/window feedback. A
  fixed per-flush cap also ignored bytes still outstanding in smoltcp.
- Fix: the device guard now requires two independent clean `WouldBlock`
  observations separated by one admission-free `ControlOnly` poll/flush epoch.
  A pause also arms a per-flow ACK-completion barrier when that flow's
  `send_queue` is nonzero; only that flow remains DrainOnly until its own queue
  reaches zero.
- Running keeps a `512 KiB` read reservoir and Recovery a `128 KiB` read
  opportunity, but actor admission is a separate cumulative sliding window:
  at most 24 MTU-sized payload packets minus the current smoltcp `send_queue`.
  The 64-packet model therefore reserves 48 feedback slots for up to two
  ACK/window-update packets per payload packet and 16 slots for ordinary TUN RX
  drain. Up to eight actor cycles still cover the `128 KiB` service target.
- The integrated 64 MiB/64-packet scenario now records budget exhaustion,
  guard pause/resume, the maximum payload packets per flush, phase state, actor
  bypass, modeled drops, owned/pending/inflight tail, terminal drops, remote
  EOF, and local EOF ordering. A separate two-flow test proves that one slow
  flow does not block a zero-queue peer from Recovery.
- Stable proof: 50 consecutive full scenarios passed with complete 64 MiB
  delivery, at most 24 payload packets per flush, zero modeled drop, zero actor
  bypass, and zero EOF/close-tail bytes. Full gates passed: lib `582/582`;
  harness lib `589/589`; integration `2/2`; harness targets `10 passed/4
  ignored`; both cargo checks; fmt; diff-check; US-client suite self-test; and
  low-RTT probe self-test.
- Reusable rule: device readiness, per-flow ACK completion, and admission
  capacity are different invariants. Close them with a two-epoch device guard,
  per-flow barriers, and a cumulative outstanding-byte window; do not use a
  global all-flows-zero condition or a nominal per-call burst alone.

## 2026-07-10 - ACK capacity gate keeps the backlog guard exceptional

- The first 50/50 Task 11A proof was clean but completed 64 MiB in about
  `9.5s`, only `56 Mbit/s`. A local correctness/stability gate does not prove
  the code-level path can reach the remote throughput target.
- Frozen Tokio time reproduced the missing progress without a timer: after one
  coalesced readiness the flow stopped at exactly one 24-packet admission.
  Re-entering the existing bounded actor on a ready local TCP packet fixed that
  edge without adding self-wake or bypass admission.
- The full-flow limiter was separate: a normal 24-payload window produced more
  than the old 16-packet TUN RX service budget, so every window installed and
  recovered the device guard. `1915` episodes over 64 MiB created an exact 5ms
  pacing pattern.
- Aligning local service with the modeled 48-packet ACK/window-update allowance
  kept the 24-payload admission cap unchanged and moved the same seam to about
  `224 Mbit/s` with ring high-water below 64 and zero drop/tail/bypass.
- Fifty consecutive runs passed the new `>=170 Mbit/s` capacity gate. Reusable
  rule: every stability harness for a performance architecture must assert the
  target service rate, and an emergency guard must be measured as exceptional,
  not merely shown to pause and resume correctly.

## 2026-07-10 - Real Quinn closes the full D16 ownership path locally

- Three progressively deeper tests now use a real loopback Quinn stream: the
  direct ordered reader, the RAII reservation/readiness queue, and the complete
  `run_event_loop` TCP/smoltcp/TUN actor path.
- The full-path test sends `32 MiB`, requires at least `170 Mbit/s`, bounds each
  flush to the 24-payload-packet actor allowance, and requires zero modeled TUN
  drop, zero actor bypass, complete remote EOF, and zero EOF tail.
- An initial repeat timed out after the generator had received all `32 MiB` and
  entered `CloseWait`. TUN queues were empty, QUIC loss/congestion was zero, but
  `Recorded` still showed `34507B` owned/inflight and no local EOF. The actual
  lifecycle was correct; the observation was only refreshed by local-egress
  service windows and could remain stale after a control-only dirty-relay pass.
- The observation snapshot is now shared and refreshed at the end of every
  `process_dirty_relay` pass when the sink explicitly requests D16 harness
  observations. Production sinks return before aggregation, and the EOF state
  machine, queue ownership, permits, and socket behavior are unchanged.
- Stable proof: the full real-Quinn path passed `30/30` repeats; normal library
  `585/585`, harness library `594/594`, concurrency harness `10 passed/4
  ignored`, checks, fmt, diff-check, clippy (existing style warnings only), and
  both script self-tests passed.
- Reusable rule: a lifecycle acceptance gate must sample after every state
  transition path capable of completing the invariant. Never diagnose a stale
  test sink as retained ownership when the endpoint state already proves FIN.

## 2026-07-10 - Protect Gate A with a same-window mature-client precondition

- After local closure, direct reverse baselines remained healthy at about
  `217 Mbit/s` from the client and `219 Mbit/s` from the exit to the target.
- A calibrated sing-box TUIC control used the intended target-only TUN route,
  kept the exit route on `eth0`, obtained `16 MiB` client UDP buffers, and
  recorded socket drop `0`, but delivered only `1.363 Mbit/s`. Earlier
  calibrated controls in the same investigation delivered `14.207 Mbit/s` and
  `1.182 Mbit/s`. All had burst/idle one-second intervals.
- Because the mature client failed the same TUIC window while both direct legs
  stayed above `200 Mbit/s`, a mini_vpn Gate A could not distinguish code from
  shared external service. The one-shot Gate A was not run.
- Reusable rule: before spending a scarce performance acceptance, require the
  same-window mature control to exceed the acceptance floor with comparable
  socket buffers and zero drops. Direct leg capacity alone is insufficient.

## 2026-07-10 - Exit restart and control MTU did not restore the TUIC window

- Restarting `.33` sing-box under the persistent high-buffer configuration
  improved the MTU1200 mature control only from the `1 Mbit/s` band to
  `17.301 Mbit/s`. The server remained active and its UDP socket was `16 MiB`
  with drop `0`.
- Historical `185.242 Mbit/s` mature evidence used TUN MTU1500, but restoring
  that control shape reached only `3.146 Mbit/s`; MTU was therefore not the
  active discriminator in this window.
- Sequential direct TCP baselines stayed above `216 Mbit/s`. Bidirectional
  100-packet ICMP checks between `.27` and `.33` had `0%` loss and roughly
  `0.5ms` RTT, while TUIC remained burst/idle. Service logs showed normal TUIC
  accepts and direct target opens, with no auth or connect failure.
- Reusable rule: once restart, MTU, socket buffers, service logs, direct legs,
  and basic path loss are clean but the mature TUIC client is still burst/idle,
  classify the acceptance window as externally incapable. Do not spend the
  one-shot product gate or keep changing architecture and VPS parameters.

## 2026-07-10 - Gate proof and deployment must share one exact profile

- A clean detached `1bf1f78` worktree compiled, confirming the D16 production
  code is committed. Its suite script does not contain the H10d16 profile,
  while the current functional runner changes remain uncommitted. A clean
  binary plus an unversioned runner is not a reproducible acceptance unit.
- The full real-Quinn test forces a `NativeByteOwned` upstream but constructs
  `TunRuntimeConfig::from_sources`, which uses MTU1500 and leaves the H10d16
  profile flag false. D16 queue presence still auto-selects the actor, so the
  ownership proof remains useful, but it is not an exact safe1200 Gate A proof.
- Reusable rule: a performance gate needs one versioned tuple of source commit,
  runner scripts, runtime profile, binary hash, and external-control shape.
  Exact-profile local TDD and a clean deployment rehearsal must precede the
  one-shot VPS acceptance.

## 2026-07-11 - Exact safe1200 profile preserves D16 local capacity

- R1 added a test-only Gate A profile constructor with pool `2`, TUN MTU
  `1200`, production-derived automatic watermarks, the single H10d16 gate, and
  all legacy/diagnostic D3-D6 flags disabled.
- The real-Quinn full TCP/smoltcp/TUN path now consumes that exact profile
  rather than the default MTU1500 config. It retained complete `32 MiB`
  delivery above `170 Mbit/s`, the 24-payload-packet bound, zero modeled drop,
  zero actor bypass, and clean EOF tail in `30/30` repeats.
- Full R1 gates passed: normal library `586/586`, harness library `595/595`,
  formatting, and diff-check.
- Reusable rule: when an exact acceptance profile remains green, preserve the
  production algorithm and fix proof/deployment reproducibility next; do not
  turn a test-fidelity review into a new tuning branch.

## 2026-07-11 - Version the acceptance profile with its evidence chain

- R2 moved the accumulated H4/H10/D3-D6/D11/D16 runner and parser changes into
  the same versioned unit as the binary they exercise. The suite now records
  the full source commit plus binary, suite, and probe SHA-256 values.
- A D16 run fails before routing traffic unless it sees the exact pool-2,
  MTU1200 runtime line and the byte-owned queue capacities approved for Gate A.
  A startup-only rehearsal mode verifies that tuple, TUN MTU, and target-only
  routing without consuming an iperf acceptance window.
- Both runner self-tests and syntax checks pass. The self-test initially used
  platform-specific `sed -i`; replacing that mutation with a heredoc kept the
  contract portable across macOS development and Ubuntu acceptance hosts.
- Reusable rule: record identity and verify runtime behavior in the artifact;
  a Git commit name alone cannot prove which binary and runner produced a
  performance result.

## 2026-07-11 - Make the mature control a fail-closed, secret-free gate

- R3 adds a versioned sing-box control with the historical MTU1500, one-flow,
  20-second reverse shape. It asserts target-only routing, the excluded Exit
  route, client and Exit UDP buffer/drop evidence, and a strict receiver floor
  above `150 Mbit/s`.
- TUIC configuration is rendered only into a mode-0600 FIFO. Cleanup removes
  the FIFO/TUN and restores all four temporary socket sysctls; artifact scanning
  deletes the bundle if credential material is ever detected. An incapable
  window returns a distinct nonzero status so automation cannot accidentally
  continue to Gate A.
- Code review caught a restoration-order bug: lowering `rmem_max/wmem_max`
  before restoring their defaults could leave the temporary maxima behind.
  Cleanup now restores defaults before maxima and signal handling funnels
  through one idempotent EXIT path.
- Reusable rule: capability controls should fail closed and restore dependency
  order in reverse; operational cleanup is part of the gate contract, not an
  afterthought.

## 2026-07-11 - Clean deployment closes proof-to-runtime drift

- R4 validated `ec112a9` from clean detached worktrees locally and on `.27`.
  Default library `586/586`, harness library `595/595`, concurrency harness
  `10 passed/4 ignored`, both checks, clippy, focused D16 formatting, diff-check,
  and all three runner self-tests passed.
- The remote release recorded source-dirty `0`, binary/suite/probe hashes,
  connected the two TUIC pool slots, and matched the exact safe1200 D16 startup
  fingerprint. MTU1200 and target-only TUN routing were correct; the Exit route
  stayed on `eth0`, idle TUN drop stayed `0`, and cleanup removed the process,
  TUN, and route. No iperf acceptance was consumed.
- Full-repository formatting remains red only in previously committed
  Reality/DNS/failover files outside D16 scope. It is not a runtime or Gate A
  blocker and was not repaired through the user's overlapping dirty files.
- Reusable rule: a clean startup rehearsal should prove commit identity,
  runtime profile, route isolation, and teardown before a scarce performance
  gate; it can expose deployment drift without spending the gate itself.

## 2026-07-11 - Versioned control confirms the shared burst/idle window

- The first clean, versioned MTU1500 control after R1-R4 had healthy direct
  reverse capacity on both legs (`216.172` and `213.865 Mbit/s`), correct
  target-only routing, `16 MiB` client/Exit UDP buffers, and socket drop `0`.
  Mature sing-box nevertheless reached only `11.219 Mbit/s` receiver.
- One-second intervals alternated short bursts with long zero-rate gaps. Exit
  logs showed normal TUIC accepts and direct target opens; the only close error
  was the expected remote stream cancellation at test completion.
- Review of the exact-profile test, runner, control, and D16 ownership diff
  found no new correctness or architecture issue. Gate A and Gate B were
  correctly left unspent.
- Reusable rule: when a versioned mature client fails with healthy direct legs,
  correct routes, full socket buffers, and zero drops, do not use mini_vpn VPS
  acceptance as a diagnostic and do not modify the local egress architecture.

## 2026-07-11 - Alternate Exit separates service incapability from D16 capacity

- Bilateral captures of the slow mature control had identical packet counts,
  byte counts, per-second bins, and long gaps on the Exit and client. The Exit
  itself stopped emitting; the network path did not transform continuous
  traffic into bursts.
- Reversing the client and target roles through the same `.33` TUIC service
  remained slow, while the direct reverse path was above `229 Mbit/s`.
  Disabling GSO worsened the control and Cubic improved it only to `36.752
  Mbit/s`; those were discriminators, not fixes.
- A temporary same-version Exit on `.77` moved the mature control to `163.786
  Mbit/s` receiver. The clean safe1200 mini_vpn run then reached `183 Mbit/s`
  receiver with zero TUN drop, actor bypass, send/flush error, and QUIC
  loss/blocking evidence.
- Reusable rule: when an Exit-service failure survives role reversal and is
  visible in the server-side emission trace, replace/rebuild the Exit service
  before changing the client architecture.

## 2026-07-11 - Timed capacity abort is not a clean EOF proof

- At the 20-second iperf boundary, the data socket changed directly from
  `Established` to terminal `Closed`; the control socket independently followed
  clean remote EOF. The data flow therefore ended by local reset/abort, not by
  the D16 remote-EOF contract.
- The remote-reader surplus was exactly the full `524288B` bounded reservoir,
  and the smoltcp tail was one `27840B` maximum actor batch. The first value
  equaled remote `rx_bytes - actor_admitted_bytes`, proving exact byte-ledger
  accounting rather than an unexplained leak.
- A TCP socket that has processed peer RST and entered `Closed` cannot deliver
  its reservoir or unacknowledged send queue. Shrinking buffers can reduce the
  number but cannot turn abort into graceful drain without sacrificing the
  architecture's independent read capacity.
- Reusable rule: use a timed/abort-capable flow for steady-state capacity and an
  EOF-terminated finite flow for zero-tail lifecycle. If one strict gate needs
  both properties, its traffic generator itself must guarantee graceful EOF.
- The terminal path must preserve one explicit reason through queue cleanup and
  relay supervision; exact byte counts paired with
  `clean_queue_lifecycle` are insufficient observability.

## 2026-07-11 - Explicit closure state keeps capacity and EOF evidence honest

- Code review confirmed the D16 byte-owned reservoir, actor-exclusive
  `send_slice`, readiness-only events, and DrainOnly recovery were not the
  source of the alternate-Exit tail. The defect was loss of terminal cause at
  the queue/relay lifecycle boundary.
- `Open / RemoteEof / Terminal(cause)` makes graceful EOF and local terminal
  abort mutually explicit. The first terminal cause remains authoritative
  across reader stop, writer-channel close, queue drop, and final relay report.
- A timed `iperf3 -t` reverse run remains the comparable sustained-capacity
  shape; fixed `iperf3 -n 64M -R` supplies the missing natural-completion shape.
  Keeping both under one AND gate is stricter and more reproducible than
  demanding incompatible properties from one socket.
- Reusable rule: preserve architecture when capacity, ownership, and actor
  invariants pass. Change the state model and test generator when the failure is
  an observability/acceptance mismatch, not a hot-path capacity defect.

## 2026-07-11 - Pool-1 recovery selects lifecycle policy, not D16 egress

- A capable mature-client window (`157.650 Mbit/s` receiver) and healthy direct
  baseline (`212.607 Mbit/s`) removed the shared service precondition, yet the
  pool-2 D16 flow received only a small burst. Local ownership, actor,
  pressure, TUN drop, and QUIC loss/blocking evidence stayed clean.
- On the same server, pool 1 reached `115 Mbit/s` and moved about `274 MiB`.
  This is a strong connection-selection discriminator but not a fix: it remains
  below Gate A and gives up connection-pool concurrency.
- Artifact correction: the failed capacity flow used auxiliary `conn=1`
  without reconnect; a later A-clean flow triggered the fixed `10s` recycle.
  Time alone is still not sufficient evidence that a QUIC connection is stale,
  but recycle was not proven to cause the capacity collapse.
- Reusable rule: connection selection is not reconnect causality. Record slot,
  generation, probe, and reconnect reason on the exact data flow before
  changing byte ownership, actor cadence, MTU, windows, or queue sizes.

## 2026-07-11 - Evidence-based pool health is necessary, not sufficient

- `6209910` replaced elapsed-idle recycling with a bounded Heartbeat/ACK probe,
  fixed lease/exclusive-open ordering, added a reconnect-ready barrier, and
  made successful Connect the last-success authority. These are reusable
  product correctness improvements even though they did not pass the capacity
  discriminator.
- In one capable temporary Exit window, mature sing-box reached
  `195.033 Mbit/s` receiver while mini_vpn generation-1 auxiliary `conn=1`
  reached `108 Mbit/s` without probe or reconnect. Stale recycle is therefore
  rejected as the active capacity root.
- The same mini_vpn flow sustained about `188-190 Mbit/s` during its middle
  window with zero D16/TUN/QUIC error surfaces. Capacity exists when bytes are
  supplied; multi-second starvation and tail collapse move diagnosis upstream
  to TUIC stream service/frontier progress.
- Reusable rule: keep a correctness repair when its original invariant is
  valid, but do not promote it to a throughput root after an A/B disproves its
  trigger. The next test must distinguish server write starvation, ordered
  frontier blockage, and client reader-service delay without a macOS TUN.

## 2026-07-12 - Transport ownership and armed-read stability

- A large application `AsyncRead` buffer can pass a localhost Mbps test yet
  regress real ordered-frontier progress. The direct D16 seam should retain
  Quinn-owned chunk boundaries unless a byte-owned batch mechanism is proven
  under the real transport.
- Unordered reads can distinguish a blocked contiguous frontier, but bounded
  application reassembly is not automatically a product solution. At high
  rates, one gap filled the entire `512 KiB` per-flow ledger before
  retransmission; correctness then requires either more memory or termination.
- A pending reservation is already-owned capacity. Non-pausing Running credit
  updates must affect the next read, not cancel the current one. Pause/close/
  stop remain the authoritative cancellation edges.
- A temporary Exit process restart is not itself a capability change: fresh
  mature controls ranged from `172.167` to `18.873 Mbit/s` with healthy direct
  paths and zero socket drops. The control floor remains mandatory before every
  mini_vpn scoped run.

## 2026-07-12 - Another clean control confirms the external stop rule

- A clean `ce5a87c` mature control through a temporary same-version `.77` Exit
  reached only `0.192 Mbit/s` receiver even though both direct legs exceeded
  `212 Mbit/s` and both UDP socket drops were zero.
- Eighteen of twenty one-second intervals were exactly zero. The mature client
  had no error and the Exit remained active, so this is the same burst/idle
  service-window discriminator rather than a `bdaa19c` measurement.
- Reusable rule: a freshly reconstructed process is not an independent
  capability change. Never spend the scoped mini_vpn run or Gate A merely
  because deployment is clean; the immediately preceding mature control must
  itself pass the floor.

## 2026-07-12 - Alternate service ports need an arrival gate

- A clean second sing-box service on `.33:9443` was active and listening, but
  the mature client timed out before opening a TUIC stream.
- A simultaneous one-byte UDP A/B proved that `.33` received the `8443` probe
  and did not receive the `9443` probe. The cloud/upstream policy, not the
  process, blocked the alternate port.
- Reusable rule: before treating a new UDP port as an independent service
  discriminator, prove packet arrival at the host. A listen socket and zero
  socket drops cannot distinguish an upstream firewall from no client traffic.

## 2026-07-12 - Same-port minimal service falsifies process configuration

- A watchdog-protected minimal sing-box service temporarily replaced the
  original `.33:8443` process on the already-allowed port. It still reached
  only `11.953 Mbit/s` receiver while both direct paths exceeded `214 Mbit/s`
  and both UDP socket drops were zero.
- Thirteen of twenty intervals were zero. Neither endpoint logged an active
  failure before the expected timed cancellation, so removing full config and
  process history did not remove the burst/idle behavior.
- Reusable rule: once original and minimal fresh processes fail on the same
  allowed port with healthy direct/socket gates, stop process-restart and
  config-reduction work. Require a new Exit or a host-local path discriminator
  before spending client acceptance runs.

## 2026-07-12 - Independent Exit exposes a control-to-product mismatch

- A fresh `.111` Exit had direct capacity above `212 Mbit/s`, verified UDP
  arrival, `16 MiB` sockets, and zero drops, but the historical mature control
  still reached only `4.928 Mbit/s` with thirteen zero-rate seconds.
- The historical control hard-codes BBR/MTU1500 while the actual product Gate
  is Cubic/safe1200. The control remains useful historical evidence, but after
  repeated independent-Exit false negatives it cannot remain the sole
  necessary predicate for the different product profile.
- Reusable rule: a capability precondition must match the mechanism it is
  authorizing closely enough to avoid systematic false negatives. Keep
  historical controls as diagnostics and add a fixed, versioned gate-aligned
  control rather than lowering the floor or bypassing external qualification.

## 2026-07-12 - Gate-aligned control rejects the profile-mismatch root

- `1b83004` made the mature precondition honest: historical BBR/MTU1500 is
  diagnostic-only, while only the fixed Cubic/MTU1200 profile can authorize
  Gate A. Exact route, profile, strict floor, and authorization semantics are
  self-tested.
- The first clean Gate-aligned `.111` control reached only `3.041 Mbit/s`
  receiver despite `217.220 Mbit/s` direct, `16 MiB` client/Exit sockets, zero
  drops, correct target-only routing, and no active client/server error.
  Fourteen of twenty intervals were zero.
- Historical and Gate-aligned `.111` controls have the same burst/idle shape,
  so client BBR and MTU1500 are rejected as the active root. The correction is
  retained as Gate-process hardening, but it is not a throughput fix.
- Reusable rule: when a profile-aligned mature control fails before product
  code executes, do not modify the product hot path. Change one external axis
  at a time; here the next sufficient discriminator is the client host/path
  while Exit, target, binary, profile, and floor remain fixed.

## 2026-07-12 - Client-host change preserves the external burst/idle failure

- Moving the Gate-aligned mature client from `.27` to `.33` while holding
  `.111`, `.77`, sing-box version, MTU, congestion control, and floor fixed
  produced only `5.347 Mbit/s` receiver. Direct was `218.285 Mbit/s`, socket
  drops were zero, routes were correct, and `13/20` intervals were zero.
- The same burst/idle signature now survives two client hosts and two client
  profiles. `.27` host state is not a sufficient root, and moving product
  acceptance to `.33` would only move the symptom.
- Reusable rule: after a protocol failure survives a client-host A/B, stop
  client and product-code work. Measure the same-port raw transport next; only
  add a protocol-specific benchmark if the raw path passes.

## 2026-07-12 - Raw UDP capacity separates path bandwidth from QUIC stalls

- Fixed iperf2 UDP reverse on `.111:8443` delivered `105 Mbit/s` with zero
  loss to both `.27` and `.33` at the 100M setting. At 200M, both remained
  continuous near `198 Mbit/s` overall and had no client UDP buffer errors.
- Both 200M runs shared an exact edge after five clean seconds, settling near
  `193 Mbit/s` with loss. That is provider/sender shaping headroom, not the
  TUIC pattern of many zero-rate seconds at `3-5 Mbit/s`.
- Reusable rule: sufficient raw bandwidth does not clear a protocol stack.
  Move one layer upward to a minimal QUIC probe before blaming TUIC or product
  egress; keep exact CC, MTU, windows, socket buffers, and direction fixed.

## 2026-07-13 - Traffic-shaped proofs close Gate A without weakening ownership

- Clean `79b41b3` reached `188 Mbit/s` receiver in the timed A-capacity window
  with zero TUN drop, actor bypass, send/flush error, pressure debt, and QUIC
  loss/congestion/blocking. The only terminal was the approved exact local
  socket boundary: one `524288B` D16 ownership release plus `27840B` already in
  the terminal smoltcp send queue.
- The same binary/profile/tunnel then transferred exactly `64 MiB` at
  `179 Mbit/s` and closed by remote EOF through `clean_queue_lifecycle`, with
  queue, reserved, leased, pending, inflight, terminal-drop, and close-egress
  bytes all zero.
- Reusable rule: do not require an abort-capable timed generator to prove
  graceful EOF. Require typed, exact, bounded, one-shot accounting at its
  terminal edge, and use a fixed-byte generator to prove natural EOF and zero
  tail. Keep both subproofs under one AND gate.
- Review found no reason to change D16 ownership, actor cadence, DrainOnly,
  EOF, MTU, pool, QUIC windows, chunk size, or self-wake. Once capacity and
  clean lifecycle pass independently on the same process, advance to repeated
  statistical parity instead of reopening architecture.

## 2026-07-13 - Bilateral capture selects Quinn transmit burst shape

- A half-closed relay timer must consult payload ownership, not elapsed time
  alone. Queued and leased/inflight D16 payload now defer timeout; reserved-only
  capacity does not. The focused test must also release ownership and prove the
  timeout becomes eligible again, or it only proves a leak.
- Same-instance forward control is essential. Sing-box completed at
  `182.856 Mbit/s` with zero socket drops, while mini_vpn completed at
  `193 Mbit/s` but added `30` TUN TX drops and `57,632,755B` of in-window QUIC
  loss. Throughput alone would have falsely called the mini_vpn sample healthy.
- Compare client egress bytes to Exit ingress bytes, not raw packet counts,
  because GRO coalesced mini_vpn packets at the Exit. Mini_vpn's bilateral UDP
  gap (`62,205,385B`) matched Quinn's loss counter (`62,280,889B`); both
  tcpdump processes had zero kernel drops.
- Timing buckets exposed a concrete mechanism: mini_vpn peaked at `261`
  packets/`334,080B` per millisecond versus control `93`/`132,738B`. This
  closely matches Quinn-proto's `256 * MTU` pacer burst clamp and makes the
  GSO/send-service seam the next reversible tracer bullet.
- Keep TUN MTU and QUIC MTU policy distinct in reports. This accepted profile
  used TUN MTU `1200` but QUIC `mtu_policy=default`; silently substituting QUIC
  `safe1200` would be a new MTU experiment, not a configuration correction.
- `black_holes_detected` is Quinn's derived suspicious-loss-burst count. It can
  rise even when MTU discovery is disabled. Here all four actual PLPMTUD probes
  succeeded, so the count describes loss shape and does not prove an MTU root.
- Sanitized discriminator evidence, including four bilateral pcaps, is under
  `/tmp/mini_vpn_h10d16_forward_discriminator_a54fb17/`. The Exit was restored
  fail-closed and all four temporary socket sysctls returned to `212992`.

## 2026-07-13 - Disabling GSO does not bound Quinn send service

- The default-enabled GSO policy seam and a real 32 MiB disabled-GSO upload
  proved reachability, exact delivery, clean EOF, and local capacity. That was
  a necessary tracer gate, not proof of burst pacing.
- In the same valid Shoes window, control reached `175.102 Mbit/s` receiver
  with a 2.47% bilateral byte gap, while disabled-GSO mini_vpn reached
  `193 Mbit/s` but retained `30` TUN drops and a 12.89% byte gap. High
  throughput must never override the loss/drop discriminator.
- Disabling GSO reduced the prior mini_vpn 1 ms peak from 261 to 163 packets,
  yet formal QUIC loss increased to `67,826,613B`. GSO aggregation affects
  syscall/packet shape but is not the sufficient root.
- Code reachability explains the result: Quinn-proto emits one datagram per
  disabled-GSO `poll_transmit`, but Quinn loops to 20 datagrams per driver
  poll, self-wakes when work remains, and its pacer can retain 256 packets of
  capacity. Bound service across driver polls, not only each UDP syscall.
- The public `AsyncUdpSocket` abstraction is the preferred reversible seam for
  a shared endpoint-level send service; it avoids vendoring Quinn and can be
  tested with a mock socket, paused time, multiple pollers, byte/datagram
  conservation, and explicit no-busy-wake invariants.
- Reusable capacity rule: `170 Mbit/s` is `21.25 MB/s`, or about `16,602`
  1280-byte datagrams/s. A proposed fixed 48-datagram/2ms service ceiling has
  `30.72 MB/s` raw capacity, so it can bound the measured microburst without
  making the target mathematically unreachable.

## 2026-07-13 - Static send-service capacity needs achieved-period evidence

- The endpoint-level `AsyncUdpSocket` seam is real: one shared adapter covered
  two pollers, counted GSO wire equivalents, preserved inner `WouldBlock`, and
  conserved accepted bytes/metadata without a queue or background task.
- Exact `32 MiB` delivery and clean EOF still reached only `94.172 Mbit/s`
  under the fixed `48 then 2ms cooldown` profile. Dividing capacity by the
  requested timer period was not sufficient because runtime wake and driver
  service sit in the actual batch period.
- Reusable rule: a timer-gated throughput design must measure scheduled versus
  actual rearm time and achieved bytes/datagrams per second before claiming
  sufficiency. When the real local capacity gate fails, stop before VPS and
  distinguish timer lateness from packetization instead of tuning the nominal
  interval.

## 2026-07-13 - Additive pacing can fail even with a perfect timer

- The instrumented fixed service delivered exact `32 MiB` and clean EOF at
  `98.311 Mbit/s`. Mean payload was `1199.953B`, so packetization was healthy.
- A 48-datagram batch averaged `4.573ms`: `2ms` intentional cooldown,
  `1.340ms` measured rearm lateness, and roughly `1.233ms` of send/service
  work. The `170 Mbit/s` limit requires at most `2.710ms` per batch.
- Reusable rule: when a transport already has an elapsed-time token-bucket
  pacer, a socket wrapper that sleeps for a full interval after each batch
  double-paces it. Subtracting timer lateness is not enough; if work plus the
  intentional delay already misses the capacity bound, reject the mechanism
  rather than tuning the timer.
- Burst-cap work must preserve the existing rate/debt model and separately
  define whether its limit is per connection or aggregate across the endpoint
  pool. Those contracts are not interchangeable.

## 2026-07-13 - Pacer caps are byte ownership, not all-packet theorems

- Quinn checks pacing before packet construction and records send/loss/pacing
  state before the runtime `AsyncUdpSocket` call. A public socket adapter that
  intentionally returns `WouldBlock` therefore delays an already-accounted
  packet and is a second pacer, even if it shares a correct endpoint token
  bucket.
- Quinn's pacer capacity is bytes. A configured `64 * mtu` maximum is best
  described as 64 paced MTU-equivalents, not 64 wire datagrams. Padded/bulk
  packets are charged before GSO coalescing, while pure ACK-only packets may
  retain zero-sized pacing accounting.
- At the measured `1.027567595` wire/application ratio, `170 Mbit/s` requires
  about `21.836 MB/s` wire and `18,197` measured datagrams/s. Reusing the failed
  cap 48 lacks mean-lateness headroom; cap 64 has about `203.4 Mbit/s`
  conservative mean-lateness application capacity while preserving Quinn's
  refill slope and elapsed-time debt.
- Reusable scope rule: two independent cap-64 connections prove at most a
  static `128 * mtu` stored paced-token ceiling. They do not prove an
  endpoint-wide sliding-window rate or fairness bound. A single-flow result is
  attributable only when one connection carries at least 95% of QUIC TX,
  current uncapped/effective capacity proves the cap active, every snapshot
  has `cwnd <= u32::MAX`, and no migration occurs.

## 2026-07-13 - Quinn pacer-cap64 passes the real local capacity gate

- The pinned `quinn-proto 0.11.16` patch passed all `274/274` upstream library
  tests. Default configuration omits the new option and retains the upstream
  `256 * mtu` capacity; the candidate changes only the stored token ceiling.
- The GSO-enabled real loopback upload delivered exactly `32 MiB` with a fixed
  `64 KiB` application chunk, zero pattern errors, clean EOF, and
  `537.106 Mbit/s` application sender throughput. The old bounded socket
  service was absent.
- The active snapshot proved the selected mechanism rather than only the env
  label: `uncapped=307200B`, `capacity=76800B=64*1200`, `tokens=42913B`,
  `cap_active=true`, `delay_events=12`, and `cwnd=34116309 <= u32::MAX`.
- Quinn does not eagerly apply elapsed refill time when existing tokens already
  cover the requested send. Characterization tests must use an empty bucket
  when proving half/full refill, otherwise they accidentally assert a behavior
  Quinn does not provide.
- `PathStats.current_mtu` may advance before the pacer receives another
  `delay()` call and refreshes its accounting MTU. Formal acceptance must log
  both `current_mtu` and `pacing_mtu`, require equality in the selected window,
  and calculate the effective cap from that matched snapshot.
- Review rule: preserve the default hot path as well as its result. Compute the
  upstream capacity once and derive the optional maximum from that value; do
  not repeat the RTT/window division on every congestion-window change merely
  to expose an uncapped diagnostic.
- Expected-failing measurement tracers must not remain as unconditional tests
  in the default product regression. Retain their deterministic mechanism
  tests and make the expensive known-negative replay explicit/ignored.
