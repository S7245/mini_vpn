# TODO

## Current Knife14 Status (2026-07-09)

### Approved next stage: H10d16 byte-owned egress

#### Latest decision (2026-07-13)

Task 12 step 4's zero-drop H10d16 successor is accepted. The complete chain is
EndpointPacingService `c55737e`, batch relay `d934f12`, bounded TUN ingress
`20a0f8c`, and the independent local TCP receive-credit service
`e20140340f7f949fa8bad9e960ce94451d2c0229`.

The repair preserves `1 MiB` smoltcp RX/TX storage but bounds H10d16 advertised
and accepted TCP credit at `368,640B`. Correct semantics use
`max_receive_extent - queued`; neither queueing nor ACK generation can move the
right edge forward before application consumption. Default/non-H10 sockets are
unchanged.

Local evidence is complete. The exact 32 MiB real-Quinn gate passed at
`302.246 Mbit/s`, exact bytes, ring/pump `15/500`, zero waits/drops, and
`recv_queue_max=39,440B`. Vendored smoltcp, Quinn-proto/Quinn, root/harness,
integration, explicit `64/256/1024`, four UDP sizes, fmt, clippy, scripts, and
diff checks passed; no unresolved P0/P1 remains.

The frozen VPS P1 passed at `191 Mbit/s` receiver with `20/20` intervals and a
`201.333/192 Mbit/s` tail average/minimum. TUN drops were `0/0`, formal QUIC
loss delta was `11,459,701B`, and flow-control blocking stayed zero. The local
receive queue reached exactly `368,640B`; the predicted packet envelope and
real pump high-water both resolved to `318`, leaving `182` slots of headroom
and zero full waits/read errors. `623,222` packets mapped exactly to `4,022`
batch/relay/poll/flush services.

Endpoint conservation and cleanup were exact:
`61,403 + 0 + 0 <= 61,440`, and
`500,763,062 - 59,480 = 500,703,582`. There was no abandoned or outstanding
byte, reconnect/migration, terminal pending/reap, TUN flush failure, residual
process, or residual TUN route. The five-member sanitized bundle is archived
under `/private/tmp/mini_vpn_local_uplink_window_e201403/` with SHA-256
`769dadd36d1b6db8ba0ff4aad3bd7efc16699b5cc01530932e81cdfb018f18d4`.

No further repair is pending for this discriminator. Resume Task 12 step 4 at
a fresh reverse P8 gate, then UDP/live-streaming, Linux fake-IP DNS, and TUN
stop/rearm. Preserve H10d16,
EndpointWindowV1 constants, MTU, kernel/FIFO/batch capacities, pool, QUIC
windows, chunk, Cubic, GSO default, Quinn sender/driver bound, and self-wake.
Do not reopen bounded sender, cap64, GSO-only, or frozen-parameter tuning.
macOS TUN remains prohibited. Source:
`docs/tech/2026-07-13-knife14h10d16-local-uplink-window-service-{architecture-spec,implementation-plan,local-gate-results,vps-results}.md`.

The older text below is chronological stage history and no longer describes
the active authorization boundary.

The confirmed post-cap64 design-preparation plan is complete. A deterministic
fake-time Pacer replay at `RTT=200us`, `cwnd=40000B`, and `MTU=1280` failed
the initial one-bucket/1ms assertion exactly at `259 > 64` datagrams. It is
now a default-enabled characterization proving the selected root: a stored
capacity ceiling does not impose a time-window service ceiling.

The proposed successor spec and implementation plan are now ready:

- `docs/tech/2026-07-13-knife14h10d16-endpoint-pacing-service-architecture-spec.md`
- `docs/tech/2026-07-13-knife14h10d16-endpoint-pacing-service-implementation-plan.md`

The candidate endpoint service is fixed at `30.72 MB/s` aggregate wire rate,
`61,440B` burst, `10,240B` control reserve, and `20,480B` bulk DRR quantum.
It provides formal connection-datagram bounds of `92,160B/1ms` and
`368,640B/10ms`, with about `239.167 Mbit/s` application capacity after the
measured QUIC overhead. The design owns pre-build reservations, short-build
refunds, socket-blocked outstanding bytes, two-connection fairness/idle
borrowing, control liveness, combined deadlines/wakers, and exact
cancel/migration/drain cleanup.

Current decision: **await explicit review/confirmation before coordinator
implementation**. No implementation, VPS, P8, macOS TUN, commit, or external
notification is authorized by the spec itself. Gate A/B and every frozen
parameter remain unchanged. Vendored Quinn-proto `276/276` plus doc `3/3`,
root fmt, and tracked/untracked diff-checks passed. A later VPS still requires
a separate explicit authorization.

The one authorized same-window pacer-cap64 forward discriminator is complete
and **FAIL**. The valid sing-box control reached `192.567/191.928 Mbit/s` with
target-only routing and zero client/Exit socket drops. The fresh frozen-profile
mini_vpn P1 reached `196/185 Mbit/s` with all `20/20` intervals nonzero, but
added `29` TUN TX drops, `50,621,275B` formal QUIC lost bytes, and `15,417`
congestion events. It therefore failed both the zero-drop and `<=16 MiB` loss
gates and stopped before P8.

Attribution was sufficient to judge the mechanism: the data pool connection
carried `99.9998%` of TX bytes and `99.9961%` of datagrams, no migration or
reconnect occurred, blocking stayed zero, every formal `cwnd` was below
`u32::MAX`, and one snapshot proved the cap active at `81920B = 64*1280`.
Nevertheless client egress peaked at `267 packets/1ms` and `1337/10ms`, versus
same-window sing-box `100/605` and prior Quinn-default mini_vpn `261/1259`.
The cap did not leave the rejected burst class.

Post-failure code review finds the design limitation: maximum stored Pacer
tokens do not bound tokens refilled and spent within a time window. Quinn's
unchanged `1.25*cwnd/rtt` refill slope can cycle multiple cap64 buckets in 1ms
on the measured sub-ms path; later formal snapshots stopped adding delay
events. Do not retry cap constants, revive the bounded socket cooldown or
GSO-only branches, or tune frozen D16/MTU/pool/window/chunk/Cubic/self-wake
values.

Await confirmation of the next modification plan: first add a deterministic
red sub-ms refill replay, then write a new reachability/capacity spec for a true
Quinn-proto endpoint pre-accounting time-window/service contract across both
pool connections. No implementation or further VPS is authorized. Gate A and
Gate B remain PASS; cleanup is complete; no macOS TUN, commit, or Slack
notification ran. See
`docs/tech/2026-07-13-knife14h10d16-pacer-cap64-forward-discriminator-results.md`.

Task 12 step 4's confirmed four-P1 repair and full local regression gate are
now **PASS**. The rejected bounded replay is explicit/ignored, Quinn derives
the optional cap from one upstream capacity result, formal stats distinguish
`current_mtu` from `pacing_mtu`, and the runner validates, mutually excludes,
propagates, reports, and independently fingerprints
`MINI_VPN_TUIC_PACING_POLICY=quinn|pacer-cap64`.

Final evidence: vendored Quinn `275/275` and doc tests `3/3`; the exact/clean
GSO-enabled `32 MiB` cap64 gate at `621.573 Mbit/s`; library `620 passed / 0
failed / 3 ignored`; normal harness `10 passed / 4 ignored`; explicit
concurrency `64/64`, `256/256`, and `1024/1024`; zero-loss UDP payload sweep;
default/harness checks, root fmt, all shell syntax/self-tests, and diff-check.
Code review has no open P0/P1. Gate A and Gate B remain PASS.

Stop locally here. No VPS or macOS TUN ran, no commit was created, and D16,
MTU, pool, QUIC windows, chunk, Cubic, and self-wake remain frozen. The spec's
single same-window forward control plus cap64 mini_vpn P1 is now locally
eligible but was not authorized or run in this turn; obtain explicit user
authorization before it. The required stage-stop Slack notification was sent.

Task 12 step 4 is now **STOPPED after local capacity PASS but before full local
regression completion**. The pinned Quinn patch passed `274/274` upstream
tests. The candidate GSO-enabled `32 MiB` upload delivered exact bytes, zero
pattern error, clean EOF, and `537.106 Mbit/s`; active stats proved the
effective `64 * 1200 = 76800B` cap, uncapped `307200B`, pacing delay activity,
and `cwnd <= u32::MAX`, with no stacked socket sender.

The default library gate then ended at `620 passed / 1 failed / 2 ignored`.
The failure was the already rejected fixed `48 then 2ms` bounded-sender replay
still hard-coded to require `>170 Mbit/s`; it reached `101.084 Mbit/s`. Per the
user's repeat-failure rule, do not edit or continue without confirmation.

Proposed repair order after confirmation:

1. mark the known-negative real bounded replay explicit/ignored while keeping
   its deterministic conservation/timer/GSO/no-busy-wake tests;
2. remove duplicate default-path pacer capacity division by deriving the cap
   from the already computed upstream capacity;
3. add `current_mtu` beside `pacing_mtu` in formal QUIC stats and tests;
4. TDD the runner's `MINI_VPN_TUIC_PACING_POLICY=quinn|pacer-cap64`, mutual
   exclusion, command/report propagation, and startup verification;
5. rerun `274/274`, the fixed 32 MiB cap gate, the full library/harness,
   64/256/1024 concurrency, UDP sweep, check/fmt/shell self-tests, diff-check,
   and code-review. Only then decide whether the architecture's single allowed
   same-window forward control + mini_vpn P1 is authorized.

No VPS or macOS TUN ran and no frozen D16, MTU, pool, QUIC-window, chunk,
Cubic, or self-wake value changed.

The post-failure Task 12 step 4 architecture/capacity gate is complete. Gate B
remains accepted and is not being rerun. Decision: **CONDITIONAL GO for local
TDD implementation only** of a pinned `quinn-proto 0.11.16` per-connection
pacer cap at `64` paced MTU-equivalents. Default remains the exact upstream
`256 * mtu` maximum. With frozen pool=2, the design proves only a static
`128 * mtu` stored paced-token ceiling; it does not claim an endpoint-wide
sliding-window rate or cross-connection fairness bound.

The public socket gate is rejected as a post-accounting second pacer, and the
driver/kernel alternatives do not provide the required portable
pre-accounting product seam. The full shared endpoint coordinator is deferred.
Before any VPS, implementation must pass the spec's default-equivalence,
rate/debt, migration, two-connection stored-cap, protocol-liveness,
observability, exact GSO-enabled `32 MiB >170 Mbit/s`, concurrency, UDP, and
code-review gates. This paragraph records the prior local-only design
authorization; the implementation checkpoint and stop above are current.
Source of truth:
`docs/tech/2026-07-13-knife14h10d16-quinn-pacer-burst-cap-architecture-spec.md`.

Task 12 step 4 is **stopped before VPS after the confirmed measurement gate
failed**. Timing/service-rate instrumentation and the runner's pool-aggregate
QUIC-loss decision passed red/green. The real GSO-enabled `32 MiB` upload
again delivered exact bytes and clean EOF but reached only `98.311 Mbit/s`.

The measured mean payload was `1199.953B`, so packetization is not the primary
branch. A 48-datagram batch averaged `4.573ms`: `2ms` intentional cooldown,
`1.340ms` mean rearm lateness, and about `1.233ms` send/service work. The
required period is at most `2.710ms`; even zero timer lateness leaves only
about `142.5 Mbit/s`. The additive cooldown wrapper is therefore rejected,
not retuned.

The architecture request from
`docs/tech/2026-07-13-knife14h10d16-bounded-udp-send-service-measurement-results.md`
is resolved by the conditional local-only decision above. No VPS or macOS TUN
test ran; all frozen knobs and unspent product regressions remain unchanged.

Task 12 step 4 is **stopped after the confirmed GSO-disabled tracer failed**.
The default-enabled Quinn policy seam, real 32 MiB disabled-GSO upload, full
library/harness gates, `64/256/1024` concurrency, and UDP sweep all passed
locally. The same-window control reached `177.303/175.102 Mbit/s` with zero
socket/capture drops. Fresh mini_vpn reached `206/193 Mbit/s` but again added
`30` TUN TX drops, `67,826,613B` formal QUIC lost bytes, and `40,887`
congestion events; final loss was `73,572,300B`.

Bilateral capture measured a 2.47% control byte gap versus 12.89% for
mini_vpn. GSO-disabled reduced the mini_vpn 1 ms peak from 261 to 163 packets
but did not remove the loss edge. Code review confirms the mechanism:
Quinn-proto limits one disabled-GSO `poll_transmit` to one datagram, while the
Quinn driver still loops to 20 datagrams, self-wakes, and retains the
256-packet pacer token capacity. GSO aggregation is rejected as a sufficient
root; bounded send service remains active.

Await confirmation of the next modification plan: first red/green the runner
so a failed standard P1 cannot exit success and target-only checks are labeled
correctly; then implement one shared timer-backed Quinn `AsyncUdpSocket`
egress service with a fixed 48-datagram-equivalent/2ms ceiling. Capacity math
is `30.72 MB/s` raw at 1280B versus `21.25 MB/s` required for 170 Mbit/s. Prove
the bound across two pollers, no busy wake, byte/datagram conservation, exact
32 MiB delivery/EOF and `>170 Mbit/s`, then repeat the full local gates and
code review before at most one new control + mini forward P1. Do not spend
another GSO-only sample. Result:
`docs/tech/2026-07-13-knife14h10d16-gso-disabled-forward-discriminator-results.md`.
P8, UDP/live-streaming, fake-IP DNS, and rearm remain unspent.

Task 12 steps 1-3 / Gate B **passed** from clean `a54fb17`. The corrected
same-window sing-box control reached `144.519/143.228 Mbit/s` with zero
client/server UDP socket drops. The three exact mini_vpn `20s` reverse-first
P1 receiver results were `192`, `188`, and `191 Mbit/s`; median `191 Mbit/s`
passes the absolute `170 Mbit/s` gate and all three exceed `150 Mbit/s`.
TUN drops, actor bypass, send/flush errors, pressure/drop debt, QUIC
loss/blocking, reconnect, and terminal pending reap were zero. The only
post-median fixed `64 MiB` A-clean completed at `179/179 Mbit/s`, delivered
exactly `67108864B`, and closed via `clean_queue_lifecycle` with every queue,
pending/inflight, terminal-drop/reap, close-egress, TUN-drop, and bypass counter
zero. Result:
`docs/tech/2026-07-13-knife14h10d16-gate-b-results.md`.

The approved next action is Task 12 step 4 product regression: sustained `60s`
reverse TCP, TCP multi-flow/concurrency, UDP/live-streaming, fake-IP DNS, and
TUN create/start/stop/rearm lifecycle. Do not claim final stable `170 Mbit/s`,
clean old paths, or change D16, MTU, pool, QUIC windows, chunk, or self-wake
until that gate passes. No macOS TUN test ran.

Operational override: evidence-backed preflight/configuration errors may be
corrected and rerun without stopping for confirmation; a preflight-only exit is
not a measurement sample. Real gate/product failures still require analysis
and a proposed modification plan before any code or profile change.

Composite Gate A **passed** from clean `79b41b3` against Shoes `v0.2.7` /
Quinn `0.11.9` on `.111:8443`. A-capacity reached `192/188 Mbit/s`
sender/receiver with zero TUN drop, actor bypass, send/flush error, pressure
debt, and QUIC loss/congestion/blocking. Its only timed terminal was the
approved exact `local_to_remote/local_socket_terminal`: one bounded `524288B`
D16 ownership release and `27840B` terminal smoltcp egress. A-clean then
completed exactly `64 MiB` at `179/179 Mbit/s` and closed through remote EOF
plus `clean_queue_lifecycle`, with every queue/pending/inflight/terminal-drop/
close-egress/TUN-drop counter at zero.

Stage 8 and Gate A are closed. Their accepted composite evidence is the source
that authorized the now-completed Gate B above; keep it as historical gate
evidence, not as the current next action.
Result:
`docs/tech/2026-07-13-knife14h10d16-shoes-composite-gate-a-results.md`.

H10d15 reached `186/185 Mbit/s` sender/receiver and proved that mini_vpn has
sing-box-class single-flow capacity when the native QUIC read side maintains a
service-sized independent pump. It did not pass clean acceptance: the tail had
`tx_dropped_delta=229`, `global_rx_paused=true`,
`close_egress_class=terminal_closed_no_send`, and
`close_egress_bytes=327272`.

The next step is not another floor, chunk, self-wake, pressure-threshold, VPS,
MTU, or QUIC-window adjustment. The approved architecture closes the code-level
contracts exposed by review:

1. acquire an RAII byte reservation before every ordered Quinn read;
2. remove the message-count TUIC payload channel before the D6 queue;
3. keep payload in one per-flow leased byte queue and use `global_rx` for
   coalesced readiness;
4. make D16 actor mode the only downlink `send_slice` owner;
5. use `Running -> DrainOnly -> Recovery`, where pressure stops read/admission
   but never stops ACK/TUN RX, poll, flush, or permit release;
6. expose remote EOF only after the owned queue and local inflight bytes drain.

Source of truth:

- `docs/tech/2026-07-09-knife14h10d16-byte-owned-egress-architecture-spec.md`
- `docs/tech/2026-07-09-knife14h10d16-byte-owned-egress-implementation-plan.md`
- `docs/tech/2026-07-09-knife14h10d16-session-handoff.md`

Current stage remains stage 8 with the capacity half passed and the clean half
failed. D16 code baseline `8496b8f` and ACK-capacity repair `f7847dd` are pushed
on `codex/knife14d-downlink-reap-open`. The reopened stages 3-7 and Task 11A local
TDD are now complete.
Ownership conservation, actor-exclusive admission, authoritative DrainOnly
recovery, bounded TUN RX service, per-flow ACK-completion isolation, and EOF
ordering pass locally. The final 64 MiB/64-packet production seam passed 50/50
capacity-qualified repeats at about `224 Mbit/s`, with a cumulative MTU-derived
24-payload-packet admission window, zero modeled drop/bypass, and zero tail
bytes.

The single authorized ACK-capacity replacement Gate A has completed and
failed at `19.2/17.9 Mbit/s` sender/receiver. It kept
`tx_dropped_delta=0`, actor bypass `0`, pressure/backlog edges `0`, and
send/flush failures `0`. Observed pending/egress/terminal-tail counters were
zero, but the data flow was still active at final snapshot, so natural
EOF/close was not established. The active failure was repeated ordered stream
read starvation: data read gaps reached `3548ms` while application polling,
local actor drain, smoltcp capacity, and QUIC loss/congestion/blocking were
healthy.

The sustained real-Quinn discriminator is complete at `1bf1f78`. Loopback tests
now cover the direct ordered reader, RAII reservation/readiness queue, and the
full `run_event_loop` TCP/smoltcp/TUN actor path. The full path delivered
`32 MiB` above `170 Mbit/s`, respected the 24-payload-packet flush bound, and
closed with zero modeled drop, actor bypass, and EOF tail in `30/30` repeats.
All local gates pass. A transient timeout was traced to a stale harness
lifecycle snapshot after a control-only dirty-relay pass; production queue,
permit, actor, EOF, and socket behavior was unchanged.

The later stream/frontier follow-up is complete through `bdaa19c`.
`4bc847b` removed the disproved service-sized application `AsyncRead` buffer
and restored transport-owned ordered Quinn chunks. In a window where mature
sing-box reached `172.167 Mbit/s`, the clean scoped mini_vpn run reached only
`38.5 Mbit/s`; D16 ownership, actor exclusivity, TUN drops, pressure, and tail
surfaces stayed clean while ordered read gaps reached `5.219s`. A bounded
unordered-frontier prototype is rejected because it deterministically fills
the `512 KiB` per-flow ownership cap before a missing offset is retransmitted.
`bdaa19c` now preserves an already armed reservation across non-pausing Running
credit changes and cancels only for pause/close/stop. Default `591/591`, harness
`600/600`, and checks pass. Its scoped VPS run remains unspent because the
fresh mandatory mature control fell to `18.873 Mbit/s` receiver with direct
`217.011 Mbit/s` and socket drop `0`. Do not run composite Gate A or Gate B
until a new control exceeds `150 Mbit/s`; then test clean `bdaa19c` once before
any further architecture change.

A subsequent clean `ce5a87c` qualification against a same-version temporary
`.77` Exit remained incapable. The role-reversed `.33` target had healthy
direct receivers of `216.801 Mbit/s` from `.27` and `212.398 Mbit/s` from
`.77`; client/Exit UDP socket drops were zero. Mature sing-box reached only
`0.192 Mbit/s`, with eighteen of twenty one-second intervals at exactly zero.
The `bdaa19c` scoped run and composite Gate A remain unspent. The next action
requires an independent Exit-service change or genuinely capable control
window, not another unchanged restart/retry or a mini_vpn parameter edit.

The attempted independent `.33:9443` service started and listened correctly,
but its mature control timed out before opening a TUIC stream. A controlled
packet-arrival A/B sent one UDP byte to `8443` and one to `9443`; `.33` captured
the `8443` packet and no `9443` packet. The alternate port is blocked outside
the host, so this was not a capability measurement. The safe next discriminator
is a user-confirmed maintenance window: temporarily replace the original
`8443` process with the minimal isolated service, run one control, and restore
the original service on every exit path. Scoped mini_vpn and Gate A remain
unspent.

The confirmed maintenance swap on the allowed `8443` port also failed the
capability floor. A minimal same-version service replaced the original process
under a restore watchdog. Direct receivers were `218.688 Mbit/s` from `.27`
and `214.285 Mbit/s` from `.33`; both UDP socket drops were zero. Mature TUIC
receiver was only `11.953 Mbit/s`, with thirteen zero-rate seconds and isolated
bursts. The original service was restored successfully. Do not repeat process
restart/full-config/minimal-config A/B or run `bdaa19c` scoped from this host
window. Reaching Gate A now requires a genuinely independent capable Exit; a
host-local network-namespace mature control is useful only if more attribution
is required before provisioning that Exit.

The new independent `.111` Exit also failed the historical mature precondition:
`.111 -> .77` direct was `212.013 Mbit/s`, control direct was `214.092 Mbit/s`,
both UDP socket drops were zero, but TUIC receiver was only `4.928 Mbit/s` with
thirteen zero-rate seconds. This rules out `.33`-specific process/host state as
a sufficient root.

Code review now requests a Gate-process correction before more VPS traffic.
The only authorization control hard-codes sing-box `BBR` and MTU `1500`, while
the exact product Gate uses mini_vpn `Cubic` and safe MTU `1200`; mini_vpn's own
TUIC source documents BBR as an experimental override that may underperform
Cubic. Add one TDD-locked, versioned `gate-aligned` mature profile (`Cubic +
MTU1200`) and keep the old historical profile as a diagnostic result rather
than the sole authorization predicate. The aligned control must still exceed
`150 Mbit/s` with both socket drops zero before clean `bdaa19c` scoped. Do not
run scoped or Gate A until this plan is confirmed and implemented.

That correction is complete at `1b83004`. The historical profile is now
diagnostic-only and the new Gate-aligned control is fixed at mature-client
`Cubic + MTU1200`, with exact profile, route, authorization, and strict-floor
self-tests. Its first clean `.111` run did not authorize scoped mini_vpn:
direct receiver was `217.220 Mbit/s`, both UDP sockets were `16 MiB` with drop
`0`, but TUIC receiver was only `3.041 Mbit/s` and fourteen of twenty intervals
were zero. The same burst/idle shape under historical and Gate-aligned profiles
rejects BBR/MTU mismatch as the active root. Keep `bdaa19c` scoped, composite
Gate A, and Gate B frozen. The next discriminator changes the mature-client
host while holding `.111`, `.77`, sing-box version, Gate-aligned profile, and
floor constant; do not change D16 or repeat `.27` against unchanged external
state. Result:
`docs/tech/2026-07-12-knife14h10d16-gate-aligned-control-results.md`.

The approved client-host discriminator is complete and also incapable. With
`.111`, `.77`, sing-box `1.13.14`, Cubic/MTU1200, and the floor unchanged, the
`.33` mature client reached only `5.347 Mbit/s` receiver despite `218.285
Mbit/s` direct, zero client/Exit UDP drops, and correct routing. Thirteen of
twenty intervals were zero, and the existing `.33` sing-box remained active.
This rejects `.27` as a sufficient root and rejects moving Gate A to `.33`.
The next proposed discriminator is raw reverse UDP over the same allowed
`.111:8443` port from both `.27` and `.33`, first at `100 Mbit/s` and then at
`200 Mbit/s` only after a low-loss first stage. If raw UDP is clean, isolate
minimal QUIC without TUIC/TUN next. Keep D16, scoped `bdaa19c`, composite Gate
A, and Gate B frozen. Result:
`docs/tech/2026-07-12-knife14h10d16-client-host-discriminator-results.md`.

The raw UDP8443 discriminator is complete. Iperf3 could not run because its
TCP8443 control was blocked before `.111`, so fixed iperf2 `2.1.9` pure UDP
reverse was installed temporarily and purged after the test. Both `.27` and
`.33` received a stable `105 Mbit/s` at the 100 Mbit/s setting with zero loss.
At the 200 Mbit/s setting both received `198 Mbit/s` overall with no zero-rate
interval, but both hit the same `.111` edge after five seconds: about `193
Mbit/s` with `7.9%` interval loss. Client UDP kernel error/buffer-drop deltas
were zero. This proves continuous raw capacity sufficient for Gate A and 170M
Gate B while rejecting raw UDP as the TUIC burst/idle root. Next add a
test-only, versioned minimal Quinn Cubic/safe1200 reverse-stream probe with a
real loopback RED/GREEN test before one `.27 -> .111` cross-host run. Do not
modify TUIC, D16, TUN, or product runtime. Result:
`docs/tech/2026-07-12-knife14h10d16-raw-udp-path-results.md`.

The minimal Quinn discriminator is complete at `8ea8925` with bounded probe
cleanup at `21dd361`. A clean, identical test binary ran Cubic/safe1200 from
`.27` against `.111` and received `489095168B` in `20.315s` at `192.597
Mbit/s`. All 21 intervals were nonzero, minimum non-empty interval throughput
was `183.934 Mbit/s`, byte-pattern errors were zero, EOF was clean, and the
client had zero Quinn loss, congestion events, and data blocking. Server loss
at the known raw-path shaping edge did not interrupt continuous delivery. This
rules out raw UDP and minimal Quinn as the TUIC burst/idle root. The next
approved design seam is a test-only direct TUIC Connect relay from `.27`
through `.111` to `.77`: force `tcp_pool=1`, use generic ordered `open_tcp`,
and bridge the loopback iperf3 control/data sockets to separate Connect
streams. This bypasses auxiliary-pool policy, TUN, smoltcp, native readers,
and D16.
RED/GREEN the loopback relay, byte accounting, half-close/EOF, and bounded
shutdown locally, then run one strict `>150 Mbit/s` cross-host discriminator.
Gate A and Gate B remain frozen.
Result:
`docs/tech/2026-07-12-knife14h10d16-minimal-quinn-path-results.md`.

The direct TUIC probe is implemented at `0f07406` and its server-CC A/B is
complete. The exact same release test binary, pool 1, generic OrderedJoin,
client Cubic/safe1200, `.111` sing-box, and `.77` target reached only `3.775
Mbit/s` with server BBR and `2.674 Mbit/s` with server Cubic. BBR caused
`14/20` zero client intervals and `3441ms` data-read gaps; Cubic made client
delivery continuous and reduced the maximum data-read gap to `225ms`, but the
`.77` sender still had `15/20` zero intervals and roughly five-second bursts.
Quinn loss/congestion/blocking surfaces stayed clean. Because this path has no
TUN, smoltcp, native reader, D16, product event loop, or auxiliary pool slot,
the active root is now sing-box TUIC server/copy/flow-control behavior or its
external QUIC-path interaction. Server BBR is rejected as a capacity root.
Next run one host-local `.111` TUIC loopback discriminator with the same Cubic
service and exact probe; Gate A and Gate B remain frozen. Result:
`docs/tech/2026-07-12-knife14h10d16-direct-tuic-server-cc-results.md`.

The host-local `.111` discriminator passed. The exact `0f07406` direct probe,
same sing-box `1.13.14` binary, Cubic/safe1200 client, Cubic server, pool 1,
and `.77` target reached `199.639 Mbit/s` receiver. All `20/20` intervals were
nonzero, the minimum interval was `169.868 Mbit/s`, both control/data relays
completed, and Quinn/socket error surfaces were zero. Target sender aggregate
was `201 Mbit/s`, versus `5.08 Mbit/s` and `15/20` zero intervals in the
external Cubic run. This proves the tested sing-box TUIC/copy path has capacity
and moves the active root to its external sender interaction with `.111 ->
.27`; it does not justify any D16/product edit. Next run one strict cross-host
server implementation/version A/B on `.111:8443`, preserving the exact probe,
profile, target, floor, and target journal. Gate A and Gate B remain frozen.
This paragraph overrides the older historical next-action text below.
Result:
`docs/tech/2026-07-12-knife14h10d16-host-local-tuic-results.md`.

The first alternate mature server did not authorize Gate A. Replacing
sing-box with official Mihomo `v1.19.28` on `.111:8443` yielded only `3.460
Mbit/s` receiver, `7/20` client zero intervals, and a `3431ms` maximum data
read gap. `.77` showed `14/20` zero sender intervals and `5.18 Mbit/s`, while
same-window direct receivers were `217.010 Mbit/s` from `.27` and `216.591
Mbit/s` from `.111`. Quinn loss/congestion/blocking and `.111` UDP drops were
zero. This removes sing-box application code as a sufficient root but not the
quic-go transport family: Mihomo and sing-box use different quic-go `0.59.x`
forks. Next review/TDD a TUIC v5 server with an independent QUIC stack and add
synchronized bilateral capture/transport evidence before one further A/B.
Gate A/B and D16 remain frozen. This paragraph supersedes older next-action
text below. Result:
`docs/tech/2026-07-12-knife14h10d16-mihomo-alternate-server-results.md`.

The independent Rust/Quinn reference server also did not authorize Gate A,
but it removed the active starvation signature. Official `tuic-server 1.0.0`
using Quinn `0.10.1` reached `112.559 Mbit/s` receiver with `20/20` nonzero
intervals, `103.805 Mbit/s` minimum interval, and `27ms` maximum data-read gap.
Direct receivers were `217.640/216.382 Mbit/s`; `.77` sender was continuous at
`119 Mbit/s`; client/server drops were zero and server CPU was not saturated.
This separates quic-go multi-second starvation from the old reference
implementation's insufficient continuous capacity. The next single server
candidate is maintained Shoes `v0.2.7`, which supports TUIC v5 and uses Quinn
`0.11.9`. First review its hot path/windows, then dry-run and SSH-banner
preflight, then run one exact `0f07406` strict A/B with corrected `-i any`,
port-only, 120-second bilateral captures. Only a clean `>150 Mbit/s` result
authorizes composite Gate A. D16 and Gate A/B remain frozen. This paragraph
supersedes older next-action text. Result:
`docs/tech/2026-07-12-knife14h10d16-rust-quinn-reference-server-results.md`.

The Shoes `v0.2.7` / Quinn `0.11.9` discriminator proved sufficient external
TUIC capacity: `192.666 Mbit/s` receiver, `20/20` nonzero intervals, and
`153.099 Mbit/s` minimum interval, with healthy direct baselines, zero pcap
kernel drops, zero Exit UDP errors, and zero client UDP buffer-error delta.
The process nevertheless exited failed because timed iperf ended one data
Connect with `Connection reset by peer`. Code review found that the direct
probe's blanket relay-error veto conflicts with the already approved composite
Gate A split between timed capacity terminal classification and fixed-byte
clean EOF. Do not rerun the 20-second Shoes discriminator or change D16. Next
RED/GREEN a pure capacity decision that retains and permits only an expected
post-result timed reset, keeps fixed-byte close strict, and replay the captured
JSON/terminal evidence. After user confirmation, redeploy the exact Shoes
release and run one clean-source composite Gate A. Gate B remains frozen.
Result:
`docs/tech/2026-07-12-knife14h10d16-shoes-modern-quinn-results.md`.

The next task is now an external same-window capability precondition, not a
production code edit. Mature sing-box reverse P1 controls fell to
`14.207 Mbit/s` and then `1.363 Mbit/s` despite correct routing, `16 MiB` client
UDP buffers, socket drop `0`, and healthy direct reverse baselines around
`217/219 Mbit/s`. Do not spend the single post-local Gate A while the mature
control is below `150 Mbit/s`. A further same-shape retry reached only
`1.182 Mbit/s` while sequential direct baselines remained `218.898/211.140
Mbit/s`. Restarting `.33` sing-box once improved MTU1200 control only to
`17.301 Mbit/s`; MTU1500 then reached only `3.146 Mbit/s`. Both UDP sockets
were `16 MiB` with drop `0`, service logs showed normal opens, and bidirectional
ICMP had `0%` loss at about `0.5ms`. Do not repeat unchanged controls, restart
again, tune VPS/CC/MTU, or spend Gate A in this window. Gate-process R1-R4 are
now complete at `ec112a9`: the real-Quinn test uses the exact safe1200 profile,
both acceptance runners are versioned and self-tested, artifacts record source/
binary/runner hashes, and a clean `.27` build/startup rehearsal verified the
D16 fingerprint, MTU, routing, zero idle TUN drop, and cleanup without iperf.
In a genuinely new external window, run the versioned historical-MTU1500
mature control exactly once. Only a receiver result `>150 Mbit/s` with both UDP
socket drops `0` authorizes exactly one `20s` reverse-first P1 Gate A. Gate B
remains frozen. Review and closure:
`docs/tech/2026-07-10-knife14h10d16-gate-process-code-review.md` and
`docs/tech/2026-07-11-knife14h10d16-gate-process-r1-r4-results.md`.

The first versioned post-R1-R4 control ran from clean `044eccb` and did not
unlock Gate A. Direct reverse baselines were `216.172 Mbit/s` from `.27` and
`213.865 Mbit/s` from `.33`, while the MTU1500 sing-box TUIC control reached
only `13.472/11.219 Mbit/s` sender/receiver. Both UDP sockets were `16 MiB` with
drop `0`, routing was correct, and the receiver intervals were burst/idle with
many zero-rate seconds. Gate A and Gate B remain unspent. Do not repeat the same
control, restart services, or change mini_vpn until an independent external
window change provides a new discriminator. Result:
`docs/tech/2026-07-11-knife14h10d16-versioned-control-results.md`.

An alternate-Exit discriminator has now removed the shared external blocker.
Bilateral packet capture and role reversal showed that `.33` itself stopped
emitting during the mature-client gaps. A temporary same-version TUIC Exit on
`.77` produced a `163.786 Mbit/s` mature receiver and unlocked the one
authorized Gate A. Clean source `5884ac0` then reached `187/183 Mbit/s`
sender/receiver under the exact safe1200 profile with TUN drops, actor bypass,
send/flush errors, QUIC loss/congestion/blocking, and terminal pending reap all
at zero.

The literal Gate A still failed close-tail: at the timed iperf boundary the
data socket moved directly from `Established` to terminal `Closed` before data
remote EOF, leaving an exact `524288B` owned reservoir and `27840B` smoltcp
send queue. The ledger bounded and released the bytes once, but the relay
summary lost the terminal reason and mislabeled the lifecycle clean. Gate B
remains frozen.

The acceptance correction is now implemented and pushed. Commit `879e904`
removed unused close-reap parameters and duplicate terminal cleanup. Commit
`7a7ca04` replaces the ambiguous leased-queue close boolean with
`Open / RemoteEof / Terminal(cause)`, preserves
`local_to_remote/local_socket_terminal` through relay teardown, reports D16
close-cause counts, and versions a fixed-byte reverse `iperf3 -n` mode. Default
library `588/588`, harness library `597/597`, integration `2/2`, harness targets
`10 passed/4 ignored`, focused formatting/checks, and both runner self-tests
pass.

The next task is one composite Gate A from a clean `.27` build and one capable
Exit window: first the established `20s` reverse-first P1 must exceed
`150 Mbit/s` with zero TUN/bypass/error and only exact bounded timed-terminal
accounting; after it becomes quiet, one `64 MiB` fixed-byte reverse flow must
close by remote EOF with `clean_queue_lifecycle` and every queue/tail counter
zero. Both subproofs are an AND gate. Do not try to drain bytes into an already
`Closed` socket, shrink the reservoir to hide the terminal edge, or restart
MTU/QUIC/pool/chunk/self-wake work. Result:
`docs/tech/2026-07-11-knife14h10d16-alternate-exit-gate-a-results.md`.

That composite Gate A has now run from clean `f1627bc` in a capable temporary
Exit window. The mature control passed at `157.650 Mbit/s` receiver and direct
reverse reached `212.607 Mbit/s`, but pool-2 mini_vpn reached only
`0.0265 Mbit/s` in A-capacity and the fixed `64 MiB` A-clean flow timed out
after about `3.68 MiB`. All D16/TUN local drop, bypass, pressure, send/flush,
and QUIC loss/blocking surfaces stayed clean; the target sender itself stopped
after a small TUIC-stream burst.

A pool-1 discriminator on the same capable server reached `115 Mbit/s` with
zero local drop/pressure and selected TUIC pool lifecycle as the next
correctness seam. Task 11C is complete at `6209910`: idle auxiliary reuse now
uses a bounded Heartbeat/ACK health probe, slot lease/open races are closed,
reconnected slots have a ready barrier, and generation/reconnect evidence is
versioned. All local gates pass without a macOS TUN test.

The scoped capable-window A/B did not authorize Gate A. Mature sing-box reached
`195.033 Mbit/s` receiver; clean pool-2 mini_vpn used auxiliary `conn=1`,
generation `1`, with no probe or reconnect, but reached `108 Mbit/s`. The
middle window sustained `188-190 Mbit/s`, while startup and tail contained
multi-second starvation. D16 ownership/release, TUN drops, actor bypass,
pressure, send/flush errors, and QUIC loss/congestion/blocking stayed clean.
Therefore stale recycle is rejected as the active capacity root. Next build a
deterministic TUIC stream-service/frontier discriminator that separates server
write starvation, ordered-frontier blockage, and reader-service delay. Gate B
remains frozen; do not change D16 ownership, actor cadence, queue size, EOF,
MTU, broad QUIC windows, chunk size, or self-wake. Results:
`docs/tech/2026-07-11-knife14h10d16-composite-gate-a-pool-lifecycle-results.md`
and `docs/tech/2026-07-11-knife14h10d16-pool-health-probe-results.md`.

Knife14fp found a mandatory high-throughput prerequisite: the exit VPS `.33`
had Linux socket buffer caps/defaults of only `212992B`, which capped both
mini_vpn and a mature sing-box client around the `20-30 Mbit/s` band. `.33` is
now persistently raised to:

```text
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.core.rmem_default = 1048576
net.core.wmem_default = 1048576
```

After restarting sing-box under those limits, the mature sing-box client reached
`185.242 Mbit/s` receiver and a later current-window mature sing-box repeat
still reached `173/173 Mbit/s`. The VPS install/optimization checklist is
captured in
`docs/tech/2026-07-08-vps-install-and-optimization-guide.md`.

Do **not** lower the target to `30 Mbit/s`. The `100+ Mbit/s` target is
reachable on the current `.27/.33/.77` topology. Final mini_vpn acceptance is
not closed because mini_vpn's reverse TUIC TCP data path is still below the
mature-client baseline.

Latest discriminator:

- Knife14fp mini_vpn safe1200 reverse-first P1 reached a reported
  `114.000 Mbit/s` receiver with high intervals averaging `189.483 Mbit/s`, but
  iperf exited by external timeout and close-tail accounting was dirty.
- Knife14fq repeated from a clean `f8765c1` clone with `IPERF_TIMEOUT_SECS=120`.
  It exited normally and cleaned close-tail accounting, but regressed to
  `35.700 Mbit/s` receiver with local pressure/headroom gating
  (`may_recv_false=13594`, `headroom_deferred_bytes=17512861`) while QUIC
  loss/blocking and TUN drops stayed `0`.
- Knife14fu restored the ordered default after rejecting unordered TUIC chunk
  reassembly, but the current branch at `6eb52e9` collapsed to a no-data
  reverse-first shape: `0.349/0.046 Mbit/s`, with local pending/headroom/TUN
  and QUIC loss/blocking surfaces clean.
- Knife14fw reran the known Knife14fp-era commit `f8765c1` from a clean
  detached worktree and got `19.900/18.700 Mbit/s` with clean close-tail and
  healthy direct baselines. This shows the current branch has a worse no-data
  regression, but the fp-era code still does not reliably reproduce `100+` in
  the current window.
- Knife14fx ran a mature sing-box `v1.13.14` client in the same current
  environment and reached `173/173 Mbit/s`. Therefore VPS config can reach
  `100+`; the remaining blocker is mini_vpn client/data-plane behavior.
- Knife14gm added local TDD and code for the Knife14gl low-byte ordered stream
  gap. Commit `9e50a32` made active payload-shaped ordered streams below the
  old `64KiB` ACK/window service gate reach both the ACK-drain due predicate
  and the relay supervisor polling gate. The focused safe1200 reverse-first P1
  retry restored data movement (`23.2/21.2 Mbit/s`) but did **not** exceed
  `30 Mbit/s`; the run stayed `low_average` with local-pressure/credit-edge
  signals and clean TUN/QUIC surfaces.
- Knife14gn added TDD and code for accepted downlink flush progress. Commit
  `97cb55e` moves downlink credit-controller feedback until after
  `send_slice`, so bytes accepted into smoltcp count as useful local egress
  progress and wake read-credit publishers. The focused safe1200 reverse-first
  P1 completed normally but regressed to `18.8/18.0 Mbit/s`, **not** `>30`.
  The important discriminator changed: `local_pressure=0`, pressure/drop debt
  and TUN drops stayed `0`, while `connection_stream_frames_pending` gaps
  remained at `3405ms`.
- Knife14go added TDD and code for active connection-RX self-wake. Commit
  `39112ca` makes pending TUIC reads self-wake for both
  `connection_stream_frames_pending` and `connection_rx_no_stream_frames`.
  Focused safe1200 reverse-first P1 improved only to `24.6/22.4 Mbit/s`, still
  **not** `>30`. The useful discriminator: data-stream polling cadence improved
  (`data_poll_gap_max_ms=43`, `self_wake_armed=11702`,
  `self_wake_fired=8726`), but ordered delivery still had multi-second gaps
  (`data_read_gap_max_ms=3872`, `data_pending_gap_max_ms=3006`) and the final
  burst reintroduced one local pressure edge (`downlink_backpressure
  pause_edges=1`, `read_credit_pause_updates=1`).
- Knife14gp added T13/T14 TDD and code for since-last-pending TUIC stream
  diagnostics plus post-flush residual pressure debt. Commit `bd0d264` removed
  the obvious false pre-flush debt shape: the focused run had
  `downlink_backpressure pause_edges=0 resume_edges=0`, clean TUN drops,
  clean close-tail accounting, and clean QUIC loss/blocking. It still regressed
  to `15.0/13.5 Mbit/s`, **not** `>30`. The new discriminator showed many
  repeated multi-second pending windows with
  `conn_rx_stream_frames_since_pending=0`, meaning older
  `connection_stream_frames_pending` evidence was often stale since the last
  successful stream read. The remaining active branch is useful read-credit
  collapse to `1200B` under residual local pressure/headroom evidence, not VPS
  capacity, iperf3, MTU, stale pool, broad QUIC windows, or simply sleeping
  stream polling.
- Knife14gq implemented the B0-B7 feature-flagged buffered downlink
  architecture. Commit `8a85ce7` promoted `SocketCtx.downlink_pending` into an
  explicit buffered layer for TUIC read-credit decisions. The B7 focused
  safe1200 reverse-first P1 did **not** pass: `18.3/17.2 Mbit/s`, below the
  `>30 Mbit/s` gate. The old read-service collapse was fixed
  (`remote_read_service_len_min=65536`,
  `remote_batch_limit_bytes_min=524288`, `read_credit_pause_updates=0`), but
  throughput stayed `low_average` with
  `attribution: local_downlink_backpressure`, one local pressure pause/resume
  edge, and multi-second ordered-stream pending gaps. Buffered read credit is
  useful but not sufficient; the next architecture target is local
  writer/egress cadence, not another credit-floor or self-wake tweak.
- Knife14gs implemented the next G2-G5 local egress service lane after the
  Knife14gr performance gate. The main loop now has an explicit bounded local
  egress window that can alternate TUN RX ACK intake, `iface.poll`, dirty
  downlink flush, and `flush_tx` until a target, no-progress, hard-pause, or
  cycle-budget stop. Local gates passed, including the code-level G5 capacity
  floor of `128KiB/5ms` active window. This is a local architecture/capacity
  gate only; VPS acceptance still must prove first `>30 Mbit/s` before any
  `100+ Mbit/s` claim.
- Knife14gs G6 ran commit `4a44a11` on `.27` from a clean `/tmp` workdir. The
  focused safe1200 reverse-first P1 improved to `28.6/27.6 Mbit/s` but still
  did **not** exceed the `>30 Mbit/s` gate. The new local egress service ran
  (`windows=6024`, `cycles=1718`) but reported `accepted_bytes=0` with dominant
  `no_progress/no_work` exits. The useful discriminator is now clear: G5
  serviced only local dirty/flush work; it did not fix the TUIC stream
  read-cadence gap (`max_remote_read_gap_ms=5029`,
  repeated `connection_stream_frames_pending`). The next slice must join TUIC
  stream read service and main-loop local injection/egress under one measured
  service contract before another VPS claim.
- Knife14gt G7 ran commit `4caf60a` on `.27` from a clean `/tmp` workdir after
  aligning relay dispatch with one local egress service window
  (`64KiB -> 128KiB`). The focused safe1200 reverse-first P1 reached
  `147/144 Mbit/s`, so the current branch has now passed the `>30 Mbit/s`
  discriminator and demonstrated `100+ Mbit/s` data movement on this topology.
  This is **not** final clean acceptance: the tail logged
  `tx_dropped_delta=783`, `global_rx_queue_used_max=1019/1024`, and
  `terminal_pending_reap_bytes=2653878`
  (`close_pending_class=terminal_closed_no_send`). Next work must preserve this
  dispatch/egress cadence while cleaning tail drop/pending, not return to VPS,
  MTU/PLPMTUD, stale pool, broad QUIC windows, or unordered reassembly.
- Knife14gu G8 tried the first close-tail cleanup slice at commit `1a3c5cb`:
  stop extra relay ready-burst reads when the relay-to-main `global_rx` queue
  is at the critical edge. The focused safe1200 reverse-first P1 did **not**
  preserve G7 throughput; it regressed to `20.2/19.2 Mbit/s`, below even the
  earlier `>30 Mbit/s` discriminator. Tail-drop surfaces improved
  (`tx_dropped_delta=0`, data relay `global_rx_queue_used_max=210/1024`), but
  the new limiter did not actually activate (`remote_batch_limited=0`). The
  active failure returned to multi-second ordered TUIC stream read gaps
  (`max_remote_read_gap_ms=3610`) and `tcp-local-egress-service
  accepted_bytes=0`. Stop condition: no more code changes from this result
  alone; next step is A/B repeat of `1a3c5cb` vs parent `4caf60a` before
  choosing another implementation direction.
- Knife14gv ran that A/B without code changes. `1a3c5cb` repeat improved to
  `41.4/39.9 Mbit/s`, so it exceeded `30M` but still did not preserve
  `100M+`; the RX-edge limiter still did not activate
  (`remote_batch_limited=0`) and the run ended with local downlink
  backpressure/pending (`pending_high=734035`, `may_recv_false=6519`).
  Parent `4caf60a` did **not** reproduce the prior G7 high-throughput result;
  it collapsed to `0.349/0.151 Mbit/s` with attribution
  `tuic_stream_read_gap+tuic_stream_read_pending+relay_remote_read_gap+target_sender_stalled+tuic_stream_starved`.
  This means G7 remains high-throughput evidence but not a stable baseline.
  The active branch is now TUIC ordered stream service stability, not
  `global_rx` queue-edge cleanup.

Current result doc:

- `docs/tech/2026-07-08-knife14gq-buffered-downlink-results.md`
- Performance gate for the next slice:
  `docs/tech/2026-07-09-knife14gr-egress-cadence-reachability.md`
- Local G2-G5 result:
  `docs/tech/2026-07-09-knife14gs-local-egress-service-g5-results.md`
- G6 VPS result:
  `docs/tech/2026-07-09-knife14gs-g6-local-egress-vps-results.md`
- G7 VPS result:
  `docs/tech/2026-07-09-knife14gt-dispatch-window-g7-results.md`
- G8 VPS result:
  `docs/tech/2026-07-09-knife14gu-rx-edge-g8-results.md`
- G8/G7 A/B repeat:
  `docs/tech/2026-07-09-knife14gv-ab-repeat-results.md`

Current follow-up queue:

1. Keep the ordered-default gate, Knife14gm low-byte ACK/window service,
   Knife14gn accepted-flush progress feedback, Knife14go active connection-RX
   self-wake, Knife14gp since-last-pending diagnostics/post-flush debt
   separation, and Knife14gq buffered read-credit isolation; unordered
   reassembly remains diagnostic only.
2. Do not continue self-wake timer work as the main branch. Knife14go proved
   frequent polling, Knife14gp proved repeated pending windows often have no
   fresh `conn_rx_stream_frames_since_pending` progress, and Knife14gq proved
   preserving a useful read-service floor alone does not clear `30 Mbit/s`.
3. Do not keep changing local pressure-credit constants blindly. Knife14gq
   removed the `1200B` credit-collapse symptom but still failed at
   `17.2 Mbit/s`.
4. Knife14gs added the first explicit local writer/egress service lane. Keep
   its bounded stop reasons and diagnostics; do not convert it into an
   unbounded pump or bypass hard pending/drop/terminal guards.
5. Do not repeat G5-style local-only egress work as the next slice.
   Knife14gs G6 proved that `tcp-local-egress-service` can run heavily while
   `accepted_bytes=0`; this does not address TUIC ordered stream read cadence.
6. Knife14gt proved the first TUIC-read/main-loop cadence slice can exceed both
   `30 Mbit/s` and `100 Mbit/s`; preserve that dispatch-window alignment.
7. Knife14gu proved that a `global_rx` critical-edge read guard alone is not
   enough to preserve the G7 cadence. Do not keep editing queue-edge guards
   without an A/B repeat.
8. Knife14gv A/B is complete. Do not treat parent `4caf60a` as a stable
   `100M+` baseline until it repeats; in the latest same-window run it produced
   only `0.151 Mbit/s` receiver.
9. The next implementation target must be TUIC ordered stream service
   stability: classify fresh/stale `connection_stream_frames_pending`, explain
   multi-second pending/read gaps while connection UDP/frame counters move, and
   bind useful remote reads plus local admission in one measured service
   contract.
10. The next focused VPS acceptance target remains a clean safe1200
   reverse-first P1 with receiver `>100 Mbit/s`, `tx_dropped_delta=0`,
   `terminal_pending_reap_bytes=0`, and no data-relay
   `terminal_closed_no_send`, but any `100M+` claim must now repeat at least
   twice before being promoted to a stable baseline. Only after that should
   longer duration, P2/P4, or concurrency stability work resume.

## Roadmap: TUN transparent proxy (Target extraction)

The TUN client must relay each intercepted connection to its real **Target**
(the destination `IP:port` the OS routed into the TUN), not a hardcoded address.
Split into two stages so the hot-path rewrite stays isolated and reversible:

- **Stage 8 — fixed-port Target extraction (stepping stone).**
  Enable smoltcp AnyIP + a default route so arbitrary destination IPs are accepted
  on a single fixed listen port. Extract the Target from `local_endpoint()` and use
  it instead of the removed hardcoded target. Deliverable is mechanism-only:
  verifiable by routing a real port-80 host into the TUN; cannot browse real sites yet.
- **Stage 9 — SYN-sniffing dynamic ports.**
  Parse inbound IP/TCP headers (etherparse) in the rx path; on a SYN to a port with
  no listener, create a smoltcp socket listening on that port; maintain a connection
  table with idle reclamation. Only after this can arbitrary ports (incl. 443) work.

### Gating dependencies for real browsing (e.g. facebook from Shenzhen)

Target extraction alone does NOT make real sites work. In order of blocking severity:

1. ~~Arbitrary ports (Stage 9)~~ — DONE.
2. ~~DNS over tunnel or fake-IP (Stage 11)~~ — DONE (fake-IP).
3. ~~UDP relay for QUIC/HTTP3 and live streaming~~ — DONE through TUIC:
   UDP now rides TUIC `Packet` over QUIC datagrams (ADR-0004), oversized packets have
   uni-stream fallback (刀3), and native+cubic high-rate soak passed (刀3.5). Re-test
   on new exits/paths, but this is no longer a known missing feature.
4. Exit IP reputation — datacenter IPs trigger target-site risk control; protocol-independent.
5. MSS clamping / MTU handling — prevents large packets from stalling through the tunnel.

### Historical architectural constraints retired by TUIC

- Yamux/TLS is no longer the data plane. TUIC Connect uses one QUIC bi-stream per TCP
  session, and the legacy server/yamux path was retired in 13d.
- Server-speaks-first protocols remain outside the current proxy trigger model (the
  local relay still opens on first local payload), but this is now a product/protocol
  requirement question, not a yamux limitation.

## Future architecture topics

These cut across multiple stages and may need their own design before being scheduled.

### Prioritized future task backlog (post-14d)

Keep this list as the canonical next-work queue after 刀14c/14d. Items are ordered by current leverage.
The 2026-06-30 US-client result moved connection-pool work behind downlink/backpressure + async-open
validation; see `docs/tech/2026-06-30-knife14b-usclient-results.md` and
`docs/tech/2026-06-30-knife14d-downlink-reap-open-spec.md`.

1. **Re-run the US-client suite after 14c+14d.** Keep the same Client/Exit/Target shape and upload the
   generated markdown/tar bundle for analysis.
2. **Use the bundle to choose the next knife.** If reverse/P2 still fails, read the TCP diagnostics and loop
   profile first; do not jump straight to connection-pool work.
3. **#3 connection-pool spike.** Only start this if post-14d measurements still prove a single TUIC/QUIC
   connection is the wall. Otherwise do not add pool complexity.
4. **Mobile/productization core seam.** Add packet I/O traits for macOS tun / iOS `NEPacketTunnelFlow` /
   Android `VpnService`, library-style config structs, and config injection for knobs such as `cc` and
   `udp_mode`.
5. **0-RTT / weak-network resume.** Revisit quinn/rustls once early exporter support is available, and pair
   it with adaptive keepalive / mobile radio-sleep behavior.
6. **DNS edge hardening.** Decide policy for IPv6 DNS, split-horizon/internal domains, exotic multi-question
   DNS, hardcoded-IP apps, and when to switch parsing to `hickory-proto`.
7. **Anti-censorship resilience beyond TCP failover.** Evaluate UDP-over-VLESS/TCP fallback only if QUIC is
   blocked and UDP service continuity matters more than latency/complexity.
8. **Scale / ops.** If multi-server or many-user operation returns, design service discovery, weighted
   upstream health, graceful drain, metrics/alerting, and multi-region steering.
9. **Longer-horizon product modes.** Multi-Hop, L3 tunnel mode, REALITY Vision flow / broader TLS cipher
   fingerprinting, and exit-IP reputation handling are product-line decisions, not current data-plane blockers.

### Data plane → TUIC protocol on QUIC (ADR-0004, supersedes self-built transport)

ADR-0003's north star (unify on QUIC) is now realized via the **TUIC v5 protocol** on quinn
(**ADR-0004**), NOT a self-designed transport (that work was reverted). Priority rule:
「用成熟方案拿到最好体验」>「复用自研引擎」. **Client-only**: the exit is a mature **sing-box TUIC
server** (interop = best experience, zero server code from us). Stage 12's UDP-over-QUIC-datagram
≈ TUIC native mode and is reused. quinn 0.10 already exposes `export_keying_material` (TUIC auth).

- **Stage 13 — TUIC client** (13a→13d complete; legacy/yamux retired):
  - ~~**13a — TCP relay over TUIC Connect**~~ — DONE (2026-06-10): Authenticate (token via
    keying-material) + per-flow Connect bi-stream behind a `ProxyUpstream` trait; **verified end-to-end
    against a real sing-box TUIC server** (curl https://1.1.1.1 → Cloudflare 301, US egress).
  - ~~**13b — UDP relay over TUIC `Packet`**~~ — DONE (2026-06-11): native QUIC datagram, one u16
    assoc-id per UDP 4-tuple (`AssocTable`), `send_udp` + a self-healing downlink datagram pump +
    periodic Heartbeat over the *same* authenticated connection as TCP; **verified end-to-end against a
    real sing-box** (`dig @1.1.1.1 example.com/facebook.com` → real A records; `curl https://1.1.1.1`
    still HTTP/2 301 = TCP non-regression). See `docs/tech/13b-tuic-udp-packet.md`. Oversized-packet
    stream fallback was completed in 刀3.
  - **13c — 0-RTT reconnect + keepalive clarification** — PARTIAL (2026-06-11). Scope narrowed (real
    migration + battery-adaptive heartbeat moved to the mobile-readiness stage; they need iOS/Android
    packet-flow backends to truly accept).
    - DONE: **on-demand TUIC Heartbeat** — fires only while UDP is recently active (`last_udp_activity`
      + `should_send_heartbeat`); pure-TCP sessions rely on the QUIC keep-alive PING. Two keepalive
      layers clarified (QUIC PING = connection liveness; TUIC Heartbeat = UDP-session liveness).
    - **0-RTT DEFERRED (ecosystem wall)**: quinn 0.10 / rustls 0.21 cannot `export_keying_material`
      during the 0-RTT (handshake-incomplete) phase, and TUIC's auth token derives from it — so TUIC
      0-RTT auth is **structurally impossible on this stack**. Verified vs sing-box (`zero_rtt_handshake`
      on): auth fails → self-healing 1-RTT fallback, traffic flows. The 0-RTT code path + the
      `MINI_VPN_TUIC_ZERO_RTT` switch are kept (default **OFF**, opt-in only).
      - **Follow-up to actually get 0-RTT**: bump quinn/rustls to a version that exposes 0-RTT
        keying-material (early-exporter) export — the version bump ADR-0004 foresaw. Best folded into
        the **mobile-readiness stage** (weak-net / radio-sleep is where 0-RTT fast resume pays off),
        alongside real connection migration + adaptive heartbeat. Re-validate against sing-box after the bump.
  - ~~**13d — retire legacy** (yamux + Stage-12 self-server QUIC datagram + self server)~~ — DONE.

#### Transport / protocol extensibility — two tiers

- **Proxy-transport trait** (the abstraction Stage 13 builds): pluggable L4/L7 proxy protocols sharing
  "intercept flow → relay to exit → exit dials target". TUIC = impl #1; **VLESS+REALITY (TCP)** is impl #2
  and is wired into health-aware failover (刀6→刀10). Trojan/SS/Hysteria could follow the same trait if
  a real product need appears.
  - **Protocol selection** is implemented as `MINI_VPN_UPSTREAM=tuic|reality|failover`; failover keeps
    UDP on TUIC and switches TCP to REALITY when QUIC/TUIC is unhealthy.
- **Data-plane "tunnel mode"** (separate future path, NOT behind the proxy trait): WireGuard / OpenVPN
  forward raw IP packets (L3), bypassing smoltcp/fake-IP/per-flow — a different mode for "general VPN
  (non-circumvention)" use, and easily GFW-blocked without obfuscation. Add only if that product need
  appears. **StealthVPN (Astrill, proprietary/closed): not doing** (no open spec).

#### Mobile (iOS/Android) readiness
- Abstract packet I/O behind a trait (current `tun` crate backend; **iOS NEPacketTunnelFlow**; **Android
  VpnService fd**) and library-ize the core (config struct in, not env/CLI) for FFI.
- Adaptive keep-alive (battery vs NAT timeout; current 5s is battery-hostile) + memory budget (iOS
  NetworkExtension ~15–50MB limit) + QUIC 0-RTT for fast resume after radio sleep.
- UDP→TCP upstream fallback where UDP/QUIC is blocked (ties into the protocol selector above).

#### Other follow-ons
- **DNS hardening**: core DoH/DoT/DoQ blocking and all plaintext `:53` hijack are done (刀4/刀5).
  Remaining work is edge-case policy: IPv6 DNS, split-horizon/internal domains, exotic multi-question DNS,
  or moving to hickory-proto when richer DNS record handling is needed.
- **Scale/ops** (only if our own server returns): session-table hardening, multi-upstream/failover,
  graceful drain, control-plane discovery + metrics (external stores belong here, not the hot path).

### Multi-Hop

Chain multiple Upstream hops (e.g. Shenzhen → HK → US) for jurisdiction layering
and exit-IP rotation. Affects relay protocol, connection-table, and TLS chaining.

### fake-IP / DNS interception

Core implemented in Stage 11 (ADR-0002): intercept DNS in the TUN, forge A responses
with `198.18.0.0/15` placeholders, map fake-IP↔domain, rewrite TCP target to DomainPort
so the Upstream resolves at the exit. Follow-ups not in the original Stage 11, with current status:

- ~~**DoH/DoT interception**: encrypted DNS (browser/system) bypasses the plaintext UDP/53
  resolver → app gets real IP → blocked IPs still fail.~~ DONE in 刀4 for known encrypted DNS endpoints.
- **Hardcoded-IP domains**: apps connecting to a literal IP never enter the fake-IP map;
  stays IpPort. No clean fix without app cooperation.
- ~~**QUIC/UDP relay**: needed for QUIC (UDP/443) and UDP services; until then apps usually
  fall back to TCP.~~ DONE via TUIC Packet + native/cubic default.
- ~~**fake-IP reclamation / LRU**: pool is never reclaimed this stage (131k addresses); add
  LRU + TTL-based eviction if it ever matters.~~ DONE in 刀2 with refcount + idle sweep.
- **Switch DNS codec to hickory-proto** when any of: parsing real upstream responses
  (compression pointers), EDNS0/DNSSEC/DoH, more record types (CNAME/HTTPS/SVCB), or
  hardening against malicious packets. Only the dns.rs codec module changes; interface stable.
- **First-SYN-to-fresh-fake-IP refused race is closed**. 刀4 acceptance confirmed knife2's same-frame
  listener creation and elastic spare listener logic eliminated the observed refused race.
- ~~**Large HTTP/2 / multiplexed streams fail mid-transfer with `bad decrypt`**~~
  RESOLVED 2026-06-04 (commit b476854): root cause was `send_slice` dropping the
  unwritten tail when the tx buffer was full; fixed with a per-handle downlink
  pending buffer that never drops bytes. Verified: `curl https://www.facebook.com/`
  downloads a full ~415KB repeatedly with no bad decrypt.

### Client-side concurrency bottlenecks (knife1/12/13 findings)

Quantified by the mock loopback harness (`src/harness.rs`, feature `harness`) and later
real-egress probes. Full data starts in `docs/tech/2026-06-12-knife1-bottleneck-findings.md`;
ADR-0013 records the 100M attribution update.

- ~~**P0 #1 — `run_event_loop` sweeps `registry.all_handles()` O(total listener slots)
  every tick.** relay/call scales linearly with `distinct_ports × pool_size` and is
  independent of active connections (0.13→0.45ms as slots 512→2048, N fixed). knife2:
  process only handles with readiness (event/dirty-set), not a full per-tick sweep.~~ DONE in 刀2.
- ~~**P0 #2 — per-port `pool_size` is a hard concurrency ceiling.** 256 conns to ONE
  port with default pool=2 complete only 2/256; hot-port bursts stall (rearm-under-churn
  doesn't drain — overlaps the first-SYN-refused race below). knife2: elastic per-port
  pool / reuse + accept backlog.~~ DONE in 刀2.
- **P1 #4 single-thread select ceiling** was re-tested by 刀12 LoopProfiler. `iface.poll` is not the 100M
  bottleneck on measured paths, so event-loop sharding is cancelled for now.
- **Cross-flow HoL from blocking TCP uplink send** was found by 刀12 and fixed by 刀13 using non-blocking
  `try_reserve`: Full leaves bytes in smoltcp rx buffer and lets TCP window apply backpressure.
- **#3 single QUIC connection / connection pool** remains unproven. Only revisit on a low RTT, genuinely
  >100M end-to-end path where single-connection-vs-pool can be measured cleanly.

### Scale & reconnection resilience (100+ servers / 5000+ users)

Stage 10 ships client-side full-jitter reconnect as the baseline. To survive
reconnect storms and scale, layered work beyond client code:

- **Architecture**
  - Multiple Upstream addresses + failover (rotate / health-aware pick); spreads
    5000 users across the server pool (~50/server).
  - Control plane / service discovery: clients pull a live healthy-server list with
    weights instead of hardcoding one address; enables dynamic eviction + steering.
  - L4 load balancer (LVS / NLB / nginx stream) in front of the pool. NOTE: a yamux
    long connection is pinned to one backend, so if that backend dies the connection
    still drops — client reconnect+jitter remains the foundation, LB does not replace it.
  - Connection epoch/generation to discard stale relays after reconnect (anti-crosstalk).
  - Application-layer heartbeat to detect half-open connections instead of waiting for
    TCP timeout.
- **Ops / deployment**
  - Rolling restart + graceful drain (stop accepting, let existing connections drain)
    — turns "5000 disconnect at once" into "a few dozen per batch"; most effective
    anti-thundering-herd measure, more so than client jitter.
  - Health checks with automatic node eviction; server-side accept rate limiting +
    connection cap + SYN cookies to avoid being overwhelmed.
  - Metrics (reconnect rate, concurrent connections, handshake failure rate) for
    observability + alerting on reconnect storms.
  - Cross-AZ / multi-region redundancy so single-point failure does not affect everyone.

## Deferred Work

These items are intentionally out of scope for the current stage, but are likely to be needed later.

### TLS / Certificates

- Support multiple server certificates selected by SNI.
- Support certificate hot reload without restarting the server.
- Support client certificate authentication (mTLS).
- Unify TLS config loading between `client-direct` and `client-tun`.
- Add explicit certificate expiry diagnostics at startup.
- Consider separating CA bundle path from leaf certificate path more strictly in default dev assets.

### Runtime / Reliability

- Add reconnect policy for `client-tun` upstream TLS/Yamux connection.
- Add upstream failover support with multiple server addresses.
- Replace remaining runtime `unwrap()` paths in TLS material loading with structured errors.
- Add retry/backoff strategy for transient upstream connection failures.

### Testing / Tooling

- Add scripted local dev certificate generation with stable output paths.
- Add an end-to-end local test recipe covering `localhost` and `example.com` SANs.
- Consider adding integration tests for TLS config loading with temporary test certificates.

### Product / Config

- Consider sharing a single top-level config model across `server`, `client-direct`, and `client-tun`.
- Add config file support in addition to environment variables.
- Evaluate whether `cert_path`, `key_path`, and `ca_path` should be documented in a single deployment guide.
