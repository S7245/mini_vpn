# HANDOFF — mini_vpn core 路线（达成 Rules.md 用户使用目标）

给后续 **逐刀接力的新 session**。每刀单独开 session（省 token），按本文件冷启动。

## Next Planned Stage — Knife16 Path-Diverse Resumable Upstream (2026-08-18)

- **Latest accepted position: Knife16 Task 4 R1–R5 foundation is accepted
  locally; R6 is next.** The crate-private `OwnedUpstream` branch preserves
  exact legacy
  `Generic`/`Native`/`NativeByteOwned`, TUIC UDP, Reality, and Failover
  behavior while allowing the real TUN/smoltcp loop to drive typed resumable
  flow ports without knowing transport-leg or replay mechanics. The public
  production entry remains legacy until a real transport and owner exist.
- One session-scoped port factory owns the aggregate uplink-byte ledger.
  Message, per-flow, and global-byte capacity are reserved before smoltcp
  extraction, and all exhaustion is backpressure rather than drop/reset.
  Reducer-minted `ReplayStored` and `ReplayAcknowledged` receipts bind replay
  storage and release. DATA/CLOSE ordering, bounded control fairness, FIN,
  terminal, stale epoch, owner loss, and uninstalled async opens are all
  capability-owned and fail closed.
- Application ACK advances only for exact positive `TcpSocket::send_slice`
  acceptance. Resumable sink traffic stays behind the unchanged D16/local-
  egress phase, credit, pressure/debt, headroom, and flush-feedback actor.
  Exact transport-leg seals prevent frames from an equally configured second
  TLS connection from borrowing another leg's authority.
- The real-smoltcp fake adapter proves pre-extraction reservation, one shared
  `65,535B` ledger, a retained `97B` saturation suffix, exact `65,632B` echo,
  one-to-one sink acceptance, unique FIN/terminal, and zero final ownership.
  Focused `43/43`, all-target harness `875 + 3 ignored`, concurrency
  `10 + 4 ignored`, typed provenance `101/101`, fake adapter `2/2` plus
  100-repeat, release, strict Clippy, rustdoc, vendored Quinn/proto, fmt, and
  diff gates pass. Four concentrated reviews report P0/P1 `0/0` for Task 3.
- Task 4 R1–R5 now add the production-shared codec/seal, exact attach
  transaction, single-owner supervisor, source/replay receipts, `TargetIo`,
  one-event checked wire scheduler, category-owned work budget, and one full
  byte-level baseline. Source extraction reserves message/byte/segment/offset
  authority first; transient reducer pressure returns the exact input; one
  pristine supervisor is the sole factory mint; Target, continuation, replay,
  quarantine, queue, and scheduler ownership all finish at zero. Independent
  foundation reviews report P0/P1 `0/0`.
- This is still local foundation evidence only. There is no standby control
  protocol, production switch controller, blackout recovery, stale-A matrix,
  real owner/transport, WAN result, or throughput result. Task 4 is NOT PASS.
  Results:
  `docs/tech/2026-08-18-knife16-owned-upstream-local-results.md` and
  `docs/tech/2026-08-18-knife16-two-leg-foundation-local-results.md`.
- **Latest accepted position: Knife15 is closed by a genuine Tier-B quality
  failure.** Exact-source `d5b8304` entered the first valid frequency schedule,
  completed eleven cycles, and failed cycle 12 `udp-reverse` at
  `13,760 / 447,313` lost packets (`3.076146%`) above the frozen `3%` limit.
  No six-hour epoch sealed; zero epoch credit does not permit a retry. Mac/Exit
  bundle SHA-256 values are `c96b342d.../567409b7...`. The once-only email,
  paired evidence finalization, status/snapshot/stop, TUN/routes, observer,
  caffeinate, and IPv6 cleanup all completed. M3 remains blocked.
- Paired packet accounting observed all `447,313` Target datagrams at the Exit,
  `436,189` outer TUIC application-sized egress datagrams, and `433,553` Mac
  receiver packets. `11,124` packets (`2.486849%`) disappeared between the
  Exit Target capture and outer TUIC egress; a further `2,636` (`0.589297%`)
  disappeared afterward. Exit capture drops and Mac TUN/D16/Endpoint/interface
  drop/backpressure evidence are zero. The dominant boundary is the Exit UDP
  socket / sing-box / QUIC send handoff; exact internal attribution remains
  unknown because live socket-overflow and server QUIC-queue counters were not
  captured.
- Do not repeat Tier B, add another equivalent VPS, relax `3%`, tune D16,
  Endpoint, MTU, pool, windows, Cubic, GSO, or switch to all-stream UDP.
  Standard single-path TUIC exhausted its bounded Knife15 decision tree.
- ADR-0015 accepts a mini_vpn-owned server-side session owner reachable through
  two independent ingress provider/ASN paths. TCP uses application byte
  offsets, ACKs, bounded replay, and dedup across replaceable legs while one
  owner keeps the Target socket. UDP uses sequence, feedback, independent
  bounded queues, dedup, deadline, and hot failover; permanent full-rate 2x
  duplication is rejected by HK capacity math. Standard TUIC remains a
  compatibility profile.
- **Only next step:** implement R6's P/A/L vertical locally: freeze
  `STANDBY_CONTROL_V1`, add five bounded leg-control records, independent
  standby HMAC, exact-leg control/liveness capabilities, and the typed
  acceptance-enqueue receipt required before recovery. Then add the
  production-shared controller's actual-close tracer; the harness must never
  read its fault oracle to switch. Do not run macOS TUN, VPS, WAN, or
  throughput acceptance. Then continue Tasks 5–10 in order; a bounded
  two-ingress qualification must pass
  before any long macOS run. Spec/plan/results:
  `docs/tech/2026-08-18-knife16-path-diverse-resumable-upstream-architecture-spec.md`,
  `docs/tech/2026-08-18-knife16-path-diverse-resumable-upstream-implementation-plan.md`,
  `docs/tech/2026-08-18-knife16-owned-upstream-local-results.md`,
  `docs/tech/2026-08-18-knife16-resumable-protocol-capacity-local-results.md`,
  and
  `docs/tech/2026-08-18-knife15-m2-frequency-first-run-udp-loss-results.md`.

### Historical Knife15 position

- **Latest accepted position: first Tier-B run has not started.** Two
  exact-source `ce164e6` attempts passed start/smoke and failed closed before
  the first epoch. Attempt 1 Mac/Exit SHA-256
  `18dd409b.../72c7fec7...` exposed the destroyed ECS key still present in
  root's `known_hosts`; the out-of-band replacement ED25519 fingerprint was
  reverified and exact root SSH now passes. Attempt 2 Mac/Exit SHA-256
  `dc5c83cd.../b066908a...` exposed `m2-frequency` missing from the workload
  PID command allowlist, making identity registration deterministically
  impossible. Both attempts sent the completion notice and cleaned TUN,
  routes, observer, caffeinate, and IPv6. They consume no epoch or ledger
  sequence.
- Local TDD now admits only the exact `m2-frequency` workload command and
  rejects suffixed variants, includes frequency in sleep and same-TUN
  isolation policies, and makes the detached controller validate the exact
  root observer identity before workload launch. Runner/controller and all
  related script/reducer/ledger/resource/observer gates pass; review has no
  unresolved P0/P1. No Rust, workload, SLI, network, or frozen value changed.
  Result:
  `docs/tech/2026-08-18-knife15-m2-frequency-preaction-control-failures-local-results.md`.

- **Current resource transaction:** the original Alibaba US candidate-1 ECS
  was destroyed after Tier A was sealed and before any Tier-B epoch started.
  The retained EIP `47.89.211.4` / `eip-rj9hj9g6dbtxwwxfqmw0t` is attached to
  replacement ECS `i-rj9c5rn1psf504mf1zo2`, `us-west-1b`, `ecs.c8i.large`,
  Ubuntu 24.04. Freeze this as Tier-B resource
  `tierb-alibaba-usw1-r1`, not as a third Tier-A candidate. Its strict host
  key is `SHA256:km4qtBz/r+jNuPv4WKoCP3LoIyw1U6nfRNMIWpW9K24`; frozen
  sing-box binary/config hashes are `4ea794fd.../0c48b683...`; key-only SSH,
  Target reachability, listener, clock, capacity, and apt isolation pass.
  The exact no-TUN TUIC handshake/auth/Connect probe from HK passed in
  `1002ms`, and the guest UDP 8443 counter advanced by `23 packets / 11,726B`,
  so real TUIC admission passes. Aegis Agent Protection was disabled and the
  `2026-08-17 14:44:14Z` reboot proves its unit absent/inactive plus zero agent
  processes and zero `AliSecGuard` modules; sing-box, hashes, listener, Target,
  IPv6, clock, and timer isolation pass. After the three extra ingress rules
  were removed, non-HK direct SSH failed while HK strict SSH and a fresh exact
  TUIC probe passed in `1001ms`; the guest counter observed `25 packets /
  11,843B` and temporary nftables ownership cleaned. Replacement Exit
  admission passes. No TUN, observer, or Tier-B epoch has started. Result:
  `docs/tech/2026-08-17-knife15-m2-frequency-usw1-replacement-exit-admission-results.md`.
- **Latest accepted position: Tier A is exhausted and Tier B is open.**
  Exact-source `27a7ca0` candidate-2 Formal-1 Mac/Exit bundle SHA-256
  `0501d259.../2453a153...` passed resource/observer/result-integrity/cleanup
  admission but the first complete `udp-reverse` result measured `6.164314%`
  loss above the frozen `3%` limit. TCP sender/receiver zero was zero.
- mini_vpn reported zero UDP drop/backpressure, zero Endpoint would-block, and
  zero interface errors. The Exit retained the full failure window with zero
  kernel drops and steady TUIC egress; gateway probes had zero loss while
  Mac-to-Exit control probes reached `33.3%`. This selects transient
  HK-to-Tokyo public-path loss after Exit kernel egress, not Target, Exit
  service, TUN, D16, Endpoint, or a fixed-capacity bottleneck.
- Strict attempt 4 is sealed as `quality_failure/udp_loss`. Ledger/evaluation
  SHA-256 `96e50cd2.../8a953089...` reject both candidates and emit
  `TIER_A_EXHAUSTED`. Do not add a third Tier-A resource, retry candidate 2,
  resize, tune, or substitute AWS. M3 remains blocked.
- **Task 8 local implementation/review passes.** Source floor `5ca79d4` adds
  a separate `m2-frequency` action admitted only by the exact
  Tier-A exhaustion hashes. It seals one to four exact six-hour epochs, keeps
  valid sealed epochs after a later failed/interrupted parent, and requires
  twelve epochs plus one uninterrupted four-epoch/24-hour process/TUN
  lifetime. Strict `m2` rejects Tier-B inputs.
- Only complete receiver-zero intervals may continue. UDP above `3%`, TCP gap
  above `16MiB`, DNS/real-client, D16/Endpoint, resource/observer, network,
  recovery, or cleanup failure remains fail-closed. Archive replay binds exact
  epoch paths, source/binary/workload, stable resource/server/route identity,
  fresh per-run direct/resource evidence, paired Exit capture, and cleanup;
  one Exit capture cannot serve multiple Mac runs.
- Full fake Mac/Exit replay, frequency/market/ledger, runner, resource, and
  observer self-tests, Python/shell syntax, diff/secret, and concentrated
  review pass with no unresolved P0/P1. No Rust, strict workload, transport,
  pool, MTU, QUIC, Endpoint, or frozen traffic value changed. Result:
  `docs/tech/2026-08-17-knife15-m2-tier-b-epoch-ledger-local-results.md`.
- **Task 9 local gates pass.** The detached controller maintains same-TTY sudo,
  sends the once-only best-effort email after the action returns, always
  attempts Mac status/snapshot/stop, and freezes/bundles a still-active Exit
  observer after an early runner failure. Unknown observer status fails
  closed. Root `713+3 ignored`, main `2`, integration `10+4 ignored`, release,
  Clippy, vendored Quinn/proto/docs, shell/self-tests, provenance/secret,
  Endpoint `240.511 Mbit/s`, D16 batch, and full-TUN gates pass. Concentrated
  review has no unresolved P0/P1. Result:
  `docs/tech/2026-08-17-knife15-m2-frequency-controller-local-results.md`.
- **Only next step:** commit/push this control-plane repair, sync/build the HK
  Mac at the new exact source, rerun script gates, take a new baseline plus
  fresh direct/resource evidence, then start the first four-epoch/24-hour run.
  M3 remains blocked until twelve valid epochs pass the immutable ledger.
- Preserve the user-requested once-only completion notice in every detached
  long-run controller: after M2/m2-frequency returns on success or failure,
  run `printf "Subject: 执行结束~" | msmtp 870941563@qq.com`. It is
  best-effort and must not replace evidence cleanup or alter the verdict. The
  current operational controller already includes it.

- **Historical superseded status:** exact-source `b4244a7` candidate-2 Formal 1
  bundles SHA-256 `5d26e42d.../d6e1dad8...` completed 17 cycles plus the
  active portion of cycle 18, then failed between the healthy end of the first
  600-second idle window and its first checkpoint row. All completed
  receiver-zero counts were zero, maximum UDP loss was `0.561407%`, the Exit
  captured `64,792,664` packets with zero kernel drops, and cleanup passed.
  This is invalid evidence, not a candidate quality failure; the ledger stays
  at sequence 3 with candidate 2 `awaiting_formal`, zero formal passes, and
  `TIER_A_PENDING`.
- Root cause is concurrent append parsing in the evidence runner. The old
  envelope ignored an incomplete `📊 数据面:` tail while replay counted it;
  incomplete replay lifecycle and active-lease EOF records had the same false
  mismatch risk. Reviewed local RED/GREEN now uses complete-record authority
  and preserves fail-closed handling once a malformed record is proved
  complete. Repaired runner SHA-256 is `8b0d0c9e...`; no Rust, binary behavior,
  workload, server, observer, resource, or frozen value changed.
- The strict ledger permits only the exact qualified/repaired runner pair with
  Mac release SHA-256 `5e946af2...` as one evidence-reader compatibility
  class. Arbitrary source/runner/binary drift still fails. Runner, resource
  profile/preflight, ledger, observer, shell, Python, diff, secret, existing
  ledger replay, and focused review pass with no unresolved P0/P1.
- Candidate Exit precheck passes exact identity, service/config hashes, UDP
  8443 listener, clean observer ownership, capacity, clock, and Target path.
  Its previous shutdown was an explicit ACPI power action, not a crash. Before
  retry: commit/push, sync/build on the HK Mac, prove exact release hash,
  disable bounded-window automatic maintenance, take fresh direct/resource
  evidence, and launch a fresh Formal 1. Result:
  `docs/tech/2026-08-17-knife15-m2-candidate2-formal1-control-failure-local-results.md`.

- **Latest accepted position:** exact-source `b4244a7` Alibaba Tokyo
  candidate-2 qualification pair SHA-256 `71a61c55.../516ff6d4...` passed
  baseline `22.809/36.695 Mbit/s`, bounded direct `11.404318 Mbit/s`, live
  resource admission, smoke, two cycles/eight phases, two DNS and two
  real-client checks, observer, safety, and cleanup. Target receiver-zero and
  sender-zero were zero; maximum TCP gap was `7,077,888B`; maximum UDP loss
  was `0.145685%`.
- Endpoint terminal ownership was `61,402/0/0B`, with zero abandon,
  socket-would-block, and interface errors. Three `Stopped(0)` writes were
  exact timed-transfer tails with D16 queued/leased/reserved `0/0/0B`. The
  Exit captured `7,517,117` packets with zero kernel drops and released all
  observer ownership. Native Quinn absorbed one PLPMTUD black-hole event;
  there was no mini_vpn path reset, Endpoint rebind, generation replacement,
  ordered-gap recovery, or gap-ACK reinforcement.
- The strict ledger seals sequence 3 as `pass/strict_pass`, candidate 2 is
  `awaiting_formal`, and Tier A remains `PENDING`. Qualification does not
  accept Tier A. Reuse the preserved exact baseline, take a fresh direct and
  resource preflight, then run Formal 1 under the reviewed runner bridge and
  frozen binary/server/workload contract. A clean Formal 1 opens Formal 2; a genuine failure
  rejects candidate 2 and exhausts Tier A. M3 remains blocked. Result:
  `docs/tech/2026-08-15-knife15-m2-strict-candidate2-qualification-results.md`.
- Three controller/setup failures before the valid qualification consumed no
  ledger slot: GNU-only `find -maxdepth`, leaked `OUT_DIR`, and an SSH
  control-link timeout. The active runbook now unsets `OUT_DIR`; remote long
  runs must be owned by a persistent Mac terminal session plus bounded
  same-TTY sudo keepalive and an independent controller log.

- **Latest accepted position:** exact-source `b4244a7` Alibaba candidate-1
  formal pair SHA-256 `0791b4e5.../8778fe15...` ran `13h04m21s`, passed 49
  complete cycles, three idle/resume boundaries, 503 phases, every resource,
  observer, safety, and cleanup gate, then failed cycle 53
  `short-forward-1` on one Target receiver-zero interval. This is the only
  receiver-zero among 228 forward results. Formal M2 failed; M3 is blocked.
- Paired failure-window evidence excludes a one-second wire blackout:
  Mac-to-Exit QUIC ingress maximum gap `144.568ms`, Exit-to-Target payload
  maximum gap `165.236ms`, Target RTT `1..5ms`, one `1,371B` retransmission,
  and sampled send queue at most `5,524B`. The first Target window carried
  `128,505B` raw TCP payload (`128,468B` test data), only `2,604B` below
  iperf3's `131,072B` application block, so
  the first application interval reported zero. The strict
  application-observed SLI remains frozen and therefore rejects the candidate.
- The immutable strict ledger sealed `attempt-002` as
  `quality_failure/receiver_zero`; candidate 1 is `rejected` with zero formal
  passes and Tier A remains `PENDING`. Evidence root:
  `/Users/liushan/knife15-evidence/candidate1-alibaba-usw1/b4244a7c3fb58efe9fbfc832cda402d6ed4e96d7`.
  Do not rerun or tune Alibaba candidate 1. Result:
  `docs/tech/2026-08-15-knife15-m2-strict-candidate1-formal-failure-results.md`.
- Cleanup is complete: Endpoint final `61,403/0/0B`, Mac routes/DNS/TUN and
  Exit observer ownership restored, sing-box active with zero restarts,
  bounded caffeinate stopped, and TUIC credential variables unset.
- **Only next step:** provision and traffic-admit final Tier-A candidate 2,
  Alibaba ECS `i-6weckus0r7voaarxz2k3`, EIP `8.211.176.98`, Tokyo
  `ap-northeast-1c`, `ecs.c8ine.large` 2-vCPU/4-GiB class, Ubuntu 24.04. The
  out-of-band ED25519 fingerprint `SHA256:r7JYHgl+...KowZZYYM` matches the
  network and strict SSH; image, instance, VPC/EIP, and IPv6 identity pass.
  The agent owns service, preflight, Mac, observer, ledger, and cleanup. Contract:
  `docs/tech/2026-08-15-knife15-m2-strict-candidate2-resource-selection.md`.
- The user explicitly substituted Alibaba Tokyo for AWS Tokyo. It differs from
  frozen `.33` Tencent/AS132203 but shares Alibaba/AS45102 with candidate 1;
  do not claim provider diversity between candidates. A genuine candidate-2
  quality failure exhausts Tier A and opens Tier B, not an AWS retry.

- **Latest candidate-1 position:** Alibaba ECS
  `i-rj9cabfprph7x3sard3z`, EIP `47.89.211.4`, `us-west-1b`, AS45102, and the
  exact sing-box service are provisioned and healthy. Host key, server hashes,
  Target reachability, key-only SSH, a real observer lifecycle, and local
  identity/capacity gates pass. Aegis/Alibaba agents/modules and apt timers are
  inactive for the dedicated window; sing-box remains active with zero
  restarts.
- Qualification is `NOT_RUN`. The narrow inbound rule is now active: guest
  capture sees HK-Mac UDP 8443, and a real TUIC handshake/auth/Target-Connect
  probe passes in `1002ms`. Keep the rule limited to the current HK public
  `/32`; do not broaden it.
- Reviewed repair `218467b` adds that no-TUN handshake/auth/Connect probe,
  makes baseline/direct/start/qualification/formal fail closed without an
  active `PreventUserIdleSystemSleep` assertion, and permits pre-ready cleanup
  only when the utun set and Target/Exit/DNS interface+gateway match their
  pre-start snapshots. Scripts/ledger bind the exact probe evidence. Local
  shell/self-tests, diff/secret, and review
  pass with no unresolved P0/P1. No Rust or frozen value changed. Strict source
  admission now requires `218467b` or a descendant. Result:
  `docs/tech/2026-08-14-knife15-m2-alibaba-candidate1-provisioning-and-preflight-results.md`.
- Real macOS GREEN: repaired `stop` finalized the pre-ready invalid run as
  SHA-256 `228533d3...`; utun and Target/Exit/DNS restoration passed,
  `cleanup_evidence=PASS`, qualification remains `not_run`, and formal
  acceptance is `NOT_RUN`. The temporary caffeinate process is stopped and
  TUIC credential variables are unset.
- A fresh 28-hour caffeinate is active as PID `85019`. The exact release probe
  revealed controlled startup diagnostics before its report; reviewed source
  preserves and hashes them while requiring one valid final report and empty
  stderr. Pull the reviewed descendant and begin a fresh baseline.

- **HK Mac is agent-operated:** use
  `ssh -i ~/.ssh/vpn xiaoou@192.168.133.109` and the clean
  `/Users/xiaoou/mini_vpn` worktree. The Desktop clone is unreadable to SSH due
  to macOS TCC and remains untouched. Exact `a16f661` release SHA-256
  `5e946af2...`, runner self-test, physical `en0` routes, and shared SSH-key
  identity pass. `sudo -n` is intentionally unavailable; privileged runner
  actions require a writable SSH TTY and interactive prompt. Never store or
  echo the password. The user no longer needs to run Mac shell commands.
  Result:
  `docs/tech/2026-08-14-knife15-m2-hk-mac-agent-access-results.md`.
- **Previous Task-6 selection:** `.111` and `.27` are ineligible before traffic:
  both are Tencent AS132203 like `.33`; historical Client/Exit roles do not
  create a different failure domain. `.111` also has an unverified changed SSH
  host key and `.27` currently closes SSH. Ordinary Tencent remains
  ineligible; Tencent AIA is a separate costly contracted-route option, not
  the default VPS. The selected candidate shape was:
  Alibaba Cloud ECS `us-west-1`, `ecs.c8i.large` 2-vCPU/4-GiB Ubuntu 24.04,
  directly attached 200-Mbit/s pay-by-data-transfer EIP. AWS Lightsail is the
  provisioning/candidate-2 fallback. The assigned address must still prove a
  non-AS132203 provider/ASN and pass exact host-key, service, Target, route,
  capacity, and immutable-profile admission. Do not start a Mac long run yet.
  Selection contract:
  `docs/tech/2026-08-14-knife15-m2-strict-candidate1-resource-selection.md`.

- **Latest implementation position:** Tasks 1–5 are complete through strict
  source-floor closure `bcd63b4`. The strict ledger validates paired bundle
  content, source/tool/binary/workload/resource/server/observer identity,
  coverage, strict SLI, safety, and cleanup; it serializes two candidates and
  requires qualification plus two formal passes. Invalid evidence remains
  decision-neutral.
- Resource comparison rejects drift from the frozen `.33` reference and `.77`
  Target. Provider/route evidence is now semantically matched, `64KiB` bounded,
  post-copy verified, and independently replayed by the root runner and ledger.
  Historical strict source admission required `9909465` or a descendant. The
  runbook also
  preserves the exact candidate baseline outside `/tmp`. Root `725+3 ignored`,
  main `2`, integration `10+4 ignored`, release, established Clippy, vendor,
  docs, shell/self-tests, provenance/secret, `240.108 Mbit/s` Endpoint, and
  review pass with no unresolved P0/P1. Rust and strict `receiver_zero == 0`
  are unchanged. Candidate provisioning has since completed; use the latest
  position above. Result:
  `docs/tech/2026-08-14-knife15-m2-strict-resource-local-results.md`.
- **Latest accepted position:** the user canceled mature-client/commercial-VPN
  comparison before C0 and selected a bounded two-tier policy. Do not ask the
  user to open a TUIC client and do not execute the old calibration runbook.
- **Tier A remains strict:** zero complete one-second TCP receiver-zero
  intervals. `.33` is the already-failed reference. Admit no more than two new
  Exit candidates that change both provider and ASN, or independently
  contracted route. Equivalent CPU/RAM/bandwidth resize, pool/window/buffer
  changes, and frozen-value tuning are not candidates.
- A candidate first passes one bounded strict qualification. It is accepted
  only after two consecutive clean 24-hour strict formal runs with identical
  source, binary, workload profile, resource, and server configuration. A
  genuine quality failure rejects it without repetition; an operator/VPS/power
  invalidation neither rejects nor passes it.
- Only after two eligible candidates are rejected does **Tier B** open. It
  permits at most three isolated one-second receiver-zero episodes in rolling
  24 hours, at most one in rolling six hours, and never two consecutive zero
  intervals. All UDP, TCP-gap, DNS, real-client, D16/Endpoint/TUN, observer,
  lifecycle, and cleanup gates stay unchanged.
- Tier B requires twelve valid six-hour evidence epochs (72 valid hours) under
  one immutable profile, including one uninterrupted four-epoch/24-hour
  process-and-TUN lifetime. A later invalid infrastructure event does not erase
  already sealed valid epochs; reducers never bridge unknown evidence.
- Next work is Task 6 candidate 1 provisioning, exact provider/route/server
  evidence, and read-only resource admission. Do not issue placeholder Mac
  exports; supply one concrete reviewed candidate first. M3 remains blocked.
  Spec and plan:
  `docs/tech/2026-08-14-knife15-m2-tiered-continuity-resource-strategy-spec.md`
  and
  `docs/tech/2026-08-14-knife15-m2-tiered-continuity-resource-strategy-implementation-plan.md`.

- **Accepted formal-failure basis:** exact-source `cdbfe36` formal artifact
  `/tmp/mini_vpn_knife15_macos_20260814_014536.tar.gz` (SHA-256
  `2c002684...`) passed baseline `32.701/56.515 Mbit/s`, bounded direct
  `16.341 Mbit/s`, smoke, every preflight, four complete cycles/36 phases,
  four DNS and real-client checks, and cleanup. Cycle 5 `tcp-forward` then
  produced five complete Target receiver-zero intervals. Formal M2 failed;
  M3 remains blocked.
- Paired Exit SHA-256 `ba378743...` captured `25,857,901` packets with zero
  kernel drops. Its exact Target socket had only `2,725B` retransmitted,
  maximum `30,350B` send queue, and a `996.276ms` maximum payload-supply gap.
  Per-second Target egress fell with TUIC ingress. This selects Mac-to-Exit
  QUIC supply, not Target, Exit-to-Target TCP, observer, or cleanup.
- The established stream retained ACK progress while its owning connection
  moved from `2,675,530B` to `24,463B` cwnd; lost bytes advanced
  `2,467,761 -> 3,795,148`, congestion events `1,552 -> 2,029`, and PLPMTUD
  black holes `4 -> 36`. Endpoint conservation ended `61,414/0/0B`, with
  zero abandon, would-block, and delay events. D16/TUN/process/routes/DNS were
  clean. No mini_vpn path reset, Endpoint rebind, or generation replacement
  occurred.
- Concentrated code review found no unresolved local P0/P1. Rebinding or
  resetting an ACK-progressing same path reopens the rejected path-reset
  branch; generation replacement and VLESS/REALITY only own future opens;
  duplicate standard TUIC streams cannot preserve one remote Target TCP
  socket. Established-stream migration requires a custom resumable Upstream
  protocol/server and conflicts with ADR-0004's accepted sing-box/client-only
  direction.
- Do not repeat formal M2 unchanged and do not tune frozen values. Freeze
  `cdbfe36`; M2 and M3 remain blocked while the accepted tiered-resource
  stage determines whether a materially different path can satisfy strict
  continuity or the low-frequency SLI must be used. Result:
  `docs/tech/2026-08-14-knife15-m2-ack-progress-native-loss-recovery-formal-failure-results.md`.

- **Previous accepted position:** exact-source `d91f205` paired qualification
  artifact `/tmp/mini_vpn_knife15_macos_20260813_095649.tar.gz` (SHA-256
  `db8297c6...`) passed baseline `19.549/52.870 Mbit/s`, bounded direct
  `9.771 Mbit/s`, smoke, every preflight, two cycles/eight phases, two DNS and
  real-client checks, and cleanup. Target receiver-zero was zero, maximum TCP
  gap was `5,767,168B`, and maximum UDP loss was `2.548546%`. Verdict is
  `PASS_NON_ACCEPTANCE`; formal M2 was not run.
- The repaired real replacement branch was reached. Generation 1 surrendered
  current `235,811B`; generation 2 retained first-turn readiness `26,338B`,
  completed five exact turns at final `427,953B` with `414,587/414,587/0B`
  sent/acked/lost, passed the live install CAS, and installed in `1,946ms`.
  No replacement failed, no `stale_cwnd` recurred, and no mini_vpn path reset
  or Endpoint rebind occurred.
- Three sender-zero intervals had zero Target receiver-zero intervals. Three
  timed-transfer `Stopped(0)` tails had D16 queued/leased/reserved `0/0/0B`
  and passed terminal replay. Endpoint final was `61,402/0/0B`, with zero
  abandon and socket would-block. Process, interface, DNS, routes, TUN, secret
  scan, and cleanup passed.
- Paired Exit SHA-256 `9dc4c864...` captured `10,442,111` packets with zero
  kernel drops. Observer state/nftables ownership is gone and sing-box is
  active with zero restarts. Result:
  `docs/tech/2026-08-13-knife15-m2-successor-install-revalidation-macos-qualification-results.md`.
- Do not repeat qualification. Pull/rebuild the pushed reviewed descendant and
  take exactly one fresh formal `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> fresh .33 observer start -> m2 ->
  status -> stop`. Reserve about 25 uninterrupted hours and sync both final
  bundles. The runner requires `cce3bf8` or a descendant, so exact `c06a9d0`
  with its older script is rejected. M3 remains blocked until formal M2 and
  cleanup pass.

- **Previous accepted position:** concentrated prequalification code review found
  and repaired three locally preventable P1 risks before another Mac run.
  Production/TDD `c06a9d0` revalidates the successor's live close state, path
  generation, and cwnd under the generation-slot install mutex; a connection
  that changes or regresses after proof cannot become current. Runner
  `cce3bf8` rejects untracked exact-source inputs and requires a fresh healthy
  matching Exit observer for qualification as well as formal M2.
- Expected REDs covered path change, cwnd contraction, close-after-proof,
  untracked `build.rs`, and missing qualification observer authority. Root
  `713+3 ignored`, main `2`, integration `10+4 ignored`, release, established
  Clippy, full-TUN/D16 batch, runner/observer self-tests, shell/fmt/diff/secret,
  and review pass with no unresolved P0/P1. Exact Endpoint capacity was
  `225.382 Mbit/s`, terminal `61,440/0/0B`, and zero socket would-block.
  Result:
  `docs/tech/2026-08-13-knife15-m2-prequalification-code-review-local-results.md`.
- The runner requires `c06a9d0` or a descendant. Pull/rebuild and take exactly
  one fresh paired `m2-ipv6-check -> baseline -> direct-discriminator -> start
  -> smoke -> fresh .33 observer start -> m2-qualification -> status -> stop
  -> observer freeze/bundle`. Reserve about 45 uninterrupted minutes. A clean
  qualification reopens one fresh formal M2 of about 25 hours; M3 remains
  blocked.

- **Formal failure basis:** exact-source `642e3ae` formal artifact
  `/tmp/mini_vpn_knife15_macos_20260813_053346.tar.gz` (SHA-256
  `6aeddcf1...`) passed baseline `23.289/52.799 Mbit/s`, bounded direct
  `11.632 Mbit/s`, smoke, every preflight, nine complete cycles, 87 phases,
  nine DNS/real-client checks, and cleanup. Cycle 10 `short-reverse-4` failed
  before creating an iperf connection; formal M2 remains failed and M3 is
  blocked.
- Paired Exit SHA-256 `8a0c0c19...` captured `57,705,606` packets with zero
  kernel drops. The auxiliary generation retained exact identity/path and
  zero black-hole movement, but admission compared current native cwnd
  `381,502B` with a historical multi-round certificate `3,605,919B`. It
  repeatedly requested `stale_cwnd` replacement; the failure-time proof lost
  one `1,409B` packet and correctly failed closed, leaving no qualified lane
  for the business open. Target counters never advanced. This is pool-service
  lifetime ownership, not Mac/VPS/network, D16, TUN, or Endpoint failure.
- Reviewed production/TDD `b7bb9a9` separates immutable first-turn successor
  readiness from transaction-only final handoff proof. Every later prepare
  overwrites its requirement with `max(readiness floor, exact current cwnd)`;
  typed install proof binds successor identity/path/readiness/final cwnd. The
  prior `17,360/24,800B` stale discriminator and `361,778/26,338B` current
  handoff remain load-bearing. No frozen constant, workload, SLI, retry,
  deadline, pool, Cubic, MTU, D16, or Endpoint behavior changed.
- Root `709+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, vendored Quinn `40+3 ignored` plus doc `1`, quinn-proto
  `330` plus docs `3`, full-TUN/D16 batch, runner/observer self-tests,
  fmt/diff/provenance/secret, and review pass with no unresolved P0/P1. Exact
  release Endpoint capacity was `240.256 Mbit/s`, terminal
  `61,440/0/0B`, and zero socket would-block. Results:
  `docs/tech/2026-08-13-knife15-m2-successor-service-floor-separation-formal-failure-results.md`
  and
  `docs/tech/2026-08-13-knife15-m2-successor-service-floor-separation-local-results.md`.
- The runner requires `b7bb9a9` or a descendant. Pull/rebuild and take exactly
  one fresh paired `m2-ipv6-check -> baseline -> direct-discriminator ->
  start -> smoke -> fresh .33 observer start -> m2-qualification -> status ->
  stop -> observer freeze/bundle`. Do not run formal M2 first and do not
  repeat/tune unchanged. A clean qualification reopens one fresh formal M2.

- **Previous accepted position:** exact-source `bec6dc8` paired qualification
  artifact `/tmp/mini_vpn_knife15_macos_20260813_030113.tar.gz` (SHA-256
  `8a65cc5b...`) completed baseline `31.445/60.511 Mbit/s`, bounded direct
  `15.716 Mbit/s`, smoke, every preflight, two cycles/eight phases, two DNS
  and real-client checks, and cleanup. Target receiver-zero was zero, maximum
  TCP gap was `7,208,960B`, and maximum UDP loss was `2.230867%`.
- The immutable archive says `failed/MISMATCH` because final D16 replay
  required a zero smoltcp send queue. Two Apple control flows had transferred
  their exact final 24-byte D16 lease through TUN before the local TCP peer
  became terminally Closed. Pending/reap/permit-drop and Endpoint ownership
  were all zero. Reviewed replay therefore classifies the run as
  `PASS_NON_ACCEPTANCE`; the archive itself is not rewritten.
- Paired Exit SHA-256 `97b7f704...` captured `11,576,962` packets with zero
  kernel drops. Observer state/nftables ownership is gone and sing-box remains
  active. A replacement also reached current/inherited floor `242,949B`,
  failed successor proof on `1,280B` loss, and safely fell back without a
  Target receiver-zero interval. No mini_vpn path reset remained.
- Reviewed `0296688` permits only an exact terminal local-close proof after
  same-handle D16 lease/local-EOF byte equality. Any pending/reap/permit-drop,
  byte mismatch, send capability, incomplete sequence, or handle reuse still
  fails. `2ada935` raises the formal source floor to `0296688` so the stale
  runner is rejected before a 25-hour run. No Rust production or frozen value
  changed.
- Complete runner self-test, exact artifact terminal replay, shell, diff,
  secret, and review gates pass with no unresolved P0/P1. Result:
  `docs/tech/2026-08-13-knife15-m2-path-reset-retirement-macos-qualification-results.md`.
- Do not repeat qualification. Pull/rebuild the pushed reviewed descendant and
  take exactly one fresh formal `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> fresh .33 observer start -> m2 ->
  status -> stop`. Reserve about 25 uninterrupted hours and sync both bundles.
  M3 remains blocked until formal M2 and cleanup pass.

- **Previous accepted position:** exact-source `fe7b809` formal artifact
  `/tmp/mini_vpn_knife15_macos_20260812_102923.tar.gz` (SHA-256
  `eb1888a1...`) passed baseline, direct, smoke, every preflight, twenty
  complete cycles, 181 phases, and cleanup before cycle 22 `tcp-forward`
  produced three complete Target receiver-zero intervals. Formal M2 remains
  failed; M3 remains blocked.
- Paired Exit SHA-256 `ce2478a...` captured `35,179,614` packets with zero
  capture/kernel drops. The exact Exit-to-Target socket kept continuous ACKs,
  about `1..8ms` RTT, negligible send queue, and no payload-supply gap above
  `320.240ms`. Mac-to-Exit QUIC supply was insufficient; Target, Exit path,
  operator, D16, TUN, Endpoint, process, routes, DNS, and cleanup were clean.
- The exact current flow kept ACK progress. At about 163 seconds, one PLPMTUD
  black-hole increment nevertheless authorized `Connection::path_changed()`
  on the unchanged path, collapsing cwnd `24,285 -> 12,000B`. Receiver zeros
  followed. Replacement later proved the inherited `24,285B` service floor,
  but the established stream correctly remained on its draining predecessor.
  This hits the stop rule and rejects connection-local path reset; do not tune
  or repeat `fe7b809`.
- Reviewed `0a3cc9c` removes `TcpPathDegraded -> ResetConnectionPath`, its
  executor/adapter/state, and all mini_vpn `path_changed()` calls. Exact
  ACK-stall/UDP Endpoint rebind, native Quinn recovery/PLPMTUD, replacement,
  current-service handoff, and predecessor drain remain unchanged.
- Root `705+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell/runner/observer self-tests, vendored Quinn
  `40+3 ignored` plus doc `1`, quinn-proto `330` plus docs `3`, root docs,
  fmt/diff/provenance/secret, exact `240.079 Mbit/s` capacity, and review pass
  with no unresolved P0/P1. Result:
  `docs/tech/2026-08-12-knife15-m2-ack-progress-path-reset-retirement-local-results.md`.
- Pull/rebuild the pushed reviewed descendant and take exactly one paired
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
  fresh .33 observer start -> m2-qualification -> status -> stop -> observer
  freeze/bundle`. Do not run formal M2 or repeat/tune unchanged. A clean
  qualification only reopens one fresh formal M2.

- **Previous accepted position:** exact-source `579fff4` paired qualification
  artifact `/tmp/mini_vpn_knife15_macos_20260812_092030.tar.gz` (SHA-256
  `c6d9c339...`) passed baseline `38.274/61.255 Mbit/s`, bounded direct at
  `19.125 Mbit/s`, smoke, every preflight, two cycles/eight phases, two DNS
  and real-client checks, and cleanup. Target receiver-zero was zero; maximum
  TCP gap was `7,733,248B`; maximum UDP loss was `1.954378%`. Verdict is
  `PASS_NON_ACCEPTANCE`; formal M2 was not run.
- Four `Stopped(0)` writes were completed timed-transfer close tails with D16
  queued/leased/reserved `0/0/0B`. Endpoint conservation stayed within
  `61,440B`, terminal live/outstanding was `0/0B`, socket would-block was
  zero, recovery evidence was safe, and the final process was dead after
  cleanup.
- No generation replacement or path reset occurred. The qualification proves
  regression cleanliness but does not independently reach the rare
  `observed_current_cwnd` handoff branch. Do not repeat or tune; formal M2 is
  the decisive branch/effectiveness gate.
- Paired Exit observer SHA-256 `95afe240...` captured `12,235,760` packets
  with zero kernel drops. It was frozen/bundled after qualification; observer
  nftables/state ownership is gone and sing-box remains active.
- Formal M2 now rejects `85d8772` and requires reviewed handoff source
  `579fff4` or a descendant. Focused source-floor RED/GREEN and the full
  runner self-test pass; no data-plane code, workload, SLI, or frozen value
  changed. Result:
  `docs/tech/2026-08-12-knife15-m2-replacement-current-service-handoff-macos-qualification-results.md`.
- Pull/rebuild the pushed reviewed descendant. Take exactly one fresh formal
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
  fresh .33 observer start -> m2 -> status -> stop` while the Mac and VPSs
  can remain uninterrupted for about 25 hours. Sync both final bundles. M3
  remains blocked until formal M2 and cleanup pass.

- **Previous accepted position:** exact-source `85d8772` formal artifact
  `/tmp/mini_vpn_knife15_macos_20260812_034701.tar.gz` (SHA-256
  `9d33b1af...`) passed baseline `17.776/64.868 Mbit/s`, direct, smoke,
  observer admission, every preflight, fifteen complete M2 cycles, and
  cleanup. Cycle 16 `short-forward-5` lost its first complete Target receiver
  interval. Formal M2 remains failed; M3 remains blocked.
- The matching `.33` v2 observer was healthy. Its independently verified
  bundle SHA-256 is `643a019a...`. The exact Exit flow supplied only
  `113,261B` during the first `1.001031s`, below one `131,072B` iperf block,
  despite continuous Target ACKs, about `1..5ms` RTT, and a maximum
  `180.701ms` supply gap. D16, TUN, Endpoint, routes, DNS, process, observer,
  and cleanup evidence was clean.
- The replaced predecessor currently owned `361,778B` cwnd, but its stored
  service floor was only `26,338B`. Direct degraded-generation replacement
  therefore installed a fresh successor at `26,338B`, which immediately
  owned the failed flow. This satisfies the prior stop rule and rejects
  path-reset-only successor inheritance as sufficient. Do not repeat or tune
  `85d8772`.
- Reviewed local implementation adds one atomic slot-owned current-service
  handoff before successor handshake/proof:
  `required=max(previous_owned_floor,current_generation_cwnd)`. The existing
  sequential ACK-owned proof reaches that exact dynamic floor within the
  unchanged five-second deadline, and install CAS rechecks it. No D16, MTU,
  pool, window, chunk, Cubic, GSO, Endpoint, self-wake, workload, or SLI
  changed.
- Root `707+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell, runner/observer self-tests, vendored Quinn
  `40+3 ignored` plus doc `1`, quinn-proto `330` plus docs `3`, root docs,
  fmt/diff/provenance/secret, capacity, and review pass with no unresolved
  P0/P1. Exact Endpoint capacity was `239.536 Mbit/s`, final
  `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-12-knife15-m2-replacement-current-service-handoff-local-results.md`.
- Pull/rebuild the pushed reviewed descendant. Start one fresh matching `.33`
  observer after smoke and take exactly one paired `m2-ipv6-check -> baseline
  -> direct-discriminator -> start -> smoke -> observer start ->
  m2-qualification -> status -> stop`. Do not run formal M2 or repeat/tune
  unchanged. A receiver-zero after proof reaches the handoff floor rejects
  this mechanism. A clean qualification only reopens formal M2.

- **Previous accepted position:** exact-source `166c390` artifact
  `/tmp/mini_vpn_knife15_macos_20260812_030150.tar.gz` (SHA-256
  `692c6286...`) passed start/smoke and the gates before observer admission,
  then formal M2 stopped before traffic with an empty
  `m2-exit-observer-status.txt`. Cleanup passed; formal M2 is `NOT_RUN`.
- The matching `.33` observer was active and healthy. Root cause was local:
  `start_runner()` persisted the Exit host and iperf port but omitted the
  parsed TUIC `server_port`; formal M2 read an empty state value and the old
  input guard returned before SSH without writing an error.
- Reviewed repair commit `1cdb17c` persists the whole start network binding
  including TUIC port. Invalid observer inputs now leave sanitized readable
  evidence, and focused RED/GREEN runner checks cover both contracts. The
  unused observer was frozen/bundled as SHA-256 `78542720...`.
- Formal M2 now rejects source older than `1cdb17c`. No Rust production code,
  workload, SLI, or frozen value changed. Result:
  `docs/tech/2026-08-12-knife15-formal-m2-observer-state-binding-repair-local-results.md`.
- Pull/rebuild the pushed reviewed descendant and take one fresh
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
  observer start -> m2 -> status -> stop`. Do not reuse the stopped run or
  frozen observer. M3 remains blocked.

- **Previous accepted position:** exact-source `0e44a939` formal artifact
  `/tmp/mini_vpn_knife15_macos_20260811_074703.tar.gz` (SHA-256
  `a0c268c1...`) completed fourteen cycles and failed cycle 15 reverse UDP at
  `3.934088%` loss versus the frozen `3%` limit. All prior UDP phases were
  `1.1968..2.3812%`; TCP Target receiver-zero remained zero and cleanup
  passed.
- Target sent all `562,748` datagrams. The Mac physical receive rate fell
  during burst loss while gateway/interface errors, local UDP drops,
  backpressure, TUN pump, D16, Endpoint, and socket would-block stayed clean.
  `.33`/`.77` did not restart. A later exact same-rate `.77 -> .33` reverse
  UDP discriminator delivered `562,214/562,214` datagrams. This selects a
  transient external UDP/QUIC path event, but the absent historical observer
  cannot split before/after `.33`; formal M2 therefore remains failed.
- The reviewed v2 observer now covers Target TCP/UDP plus encrypted TUIC,
  keeps a bounded pcap ring, records four-direction nftables packet/byte
  counters every second for up to `93,600s`, and refuses ambiguous or bypassed
  counter cleanup. Formal M2 requires a matching healthy observer no older
  than `900s`, binds its SSH IP to the recorded Exit, and automatically
  `freeze -> bundle`s it on success, failure, signal, or unexpected exit.
- Formal M2 rejects source older than reviewed observer commit `2707873`.
- Local observer and Mac runner self-tests pass. A real `.33` lifecycle probe
  counted reverse UDP, froze cleanly, left sing-box active with zero restarts,
  and produced Exit bundle SHA-256 `fff3011e...`. No Rust production code,
  workload, SLI, or frozen value changed. Result:
  `docs/tech/2026-08-11-knife15-formal-m2-udp-path-attribution-observer-local-results.md`.
- Do not rerun yet. Pull/rebuild the reviewed pushed descendant, start one
  fresh v2 observer after smoke, then take exactly one
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
  observer start -> m2 -> status -> stop`. Sync both Mac and Exit bundles.
  Do not repeat qualification or tune. M3 remains blocked.

- **Previous accepted position:** concentrated formal-M2 self-audit found and
  fixed three locally preventable 25-hour-run risks without changing Rust or
  any frozen value. Formal M2 now shares the qualification's exact recovery
  contract, rejects source older than reviewed inheritance `de4d170`, and
  refuses a stale/symlinked release binary or dirty tracked worktree before
  traffic. The runbook now points only to formal `m2`, not the completed
  qualification.
- Root `705+3 ignored`, main `2`, integration `10+4 ignored`, focused
  successor `9+1`, release full-TUN, release build, established Clippy, fmt,
  shell, runner/observer self-tests, and diff checks pass. Exact release batch
  capacity was `1,406.229 Mbit/s`, with zero ring drops/full waits. Review has
  no unresolved P0/P1; readiness to start formal M2 is `10/10`. Result:
  `docs/tech/2026-08-11-knife15-formal-m2-preflight-self-audit-local-results.md`.
- Pull/rebuild the pushed reviewed descendant and take exactly one clean
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke -> m2
  -> status -> stop` while `.33` Exit and `.77` Target can remain up for about
  25 hours. Do not repeat qualification. M3 remains blocked until formal M2
  and cleanup pass.

- **Previous accepted position:** exact-source `25ea39c` paired qualification
  artifact `/tmp/mini_vpn_knife15_macos_20260811_054300.tar.gz` (SHA-256
  `23a49673...`) passed baseline `11.858/55.917 Mbit/s`, direct at
  `5.928 Mbit/s` without zero intervals, smoke, every preflight, two exact
  cycles/eight phases, two DNS/real-client checks, and cleanup. All six TCP
  results had zero Target receiver-zero intervals; one sender interval was
  zero, maximum TCP gap was `5,636,096B`, and maximum UDP loss was
  `1.830049%`.
- The immutable bundle records `failed/MISMATCH`, but this is a runner false
  negative. Its immediate terminal sample was
  `60,031/1,409/0B` available/live/outstanding after the final real-client
  probe. The next ordinary sample about 14 seconds later was `61,414/0/0B`
  and stayed zero-owned through cleanup. Exact prefix replay is dirty before
  that record and PASS after it; the archive itself was not modified.
- Paired Exit SHA-256 `6f745e9f...` captured `3,270,562` packets with zero
  kernel drops. The cycle-2 short-forward connection containing the one Mac
  sender-zero supplied `2,817,041B/10.182s` with a maximum `263.147ms` gap,
  so Target remained continuous. Three remote-write closes were completed
  timed-transfer tails with D16 queued/leased/reserved `0/0/0B`.
- No path reset, replacement, or inherited floor occurred. The qualification
  is regression-clean but does not claim reachability of the inheritance
  branch; formal M2 is decisive. Reviewed runner fix `3a8a796` waits up to
  the existing `duration + 30` drain bound, fails persistent dirt, and
  rechecks process/watchdog/routes/DNS/network after drain. Shell, full
  self-test, artifact replay, diff, secret, and review pass with no unresolved
  P0/P1. Result:
  `docs/tech/2026-08-11-knife15-m2-successor-forward-service-inheritance-macos-qualification-results.md`.
- Do not repeat/tune qualification. Pull/rebuild the pushed reviewed
  descendant. When the Mac and `.33` services can remain uninterrupted for
  about 25 hours, take one fresh `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> m2 -> status -> stop`. A Target
  receiver-zero after a logged nonzero inherited floor rejects the mechanism.
  M3 remains blocked until formal M2 and cleanup pass.

- **Previous accepted position:** exact-source `e531b30` formal artifact
  `/tmp/mini_vpn_knife15_macos_20260811_014220.tar.gz` (SHA-256
  `2d8b4e6e...`) passed baseline `12.838/54.648 Mbit/s`, direct, smoke, every
  preflight, five complete M2 cycles, and most of cycle 6 before
  `short-forward-3` lost its first complete Target receiver interval. Sender
  admitted `10,878,976B`, Target received `4,194,304B`, TCP retransmits were
  zero, and cleanup passed. This is a formal M2 failure; M3 remains blocked.
- D16, TUN, Endpoint, path identity, ACK progress, VPS probes, resources,
  routes, DNS, and cleanup were healthy. No paired Exit observer existed, so
  no packet-capture claim is made. The exact predecessor reset at
  `41,301 -> 12,000B` cwnd, while its successor installed after one exact
  service turn at only `26,424B` and owned the failed flow. The previous
  one-turn sufficiency claim is rejected; do not repeat or tune that build.
- Reviewed implementation `de4d170` records the exact positive pre-reset cwnd
  as a monotonic generation-owned forward-service floor. A successor performs
  sequential exact ACK-owned service turns on one path, under the unchanged
  five-second whole-replacement deadline, until current cwnd reaches that
  floor. Loss, path change, close, timeout, or no progress fails closed.
  Reset publication is atomic with `path_changed()`, and install CAS rechecks
  the latest slot-owned floor.
- Root `705+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`,
  quinn-proto `330` plus docs `3`, root docs, fmt/diff/local-patch/secret, and
  review pass with no unresolved P0/P1. Exact 32MiB Endpoint capacity was
  `240.349 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-11-knife15-m2-successor-forward-service-inheritance-local-results.md`.
- Do not run another 25-hour formal M2 yet. Pull/rebuild the pushed reviewed
  descendant and take exactly one fresh paired `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> m2-qualification -> status ->
  stop`. It is about thirty minutes and remains non-acceptance. A recurrence
  after a logged nonzero inherited floor rejects the mechanism; do not repeat
  or tune unchanged. A clean qualification reopens formal M2. M3 remains
  blocked until formal M2 passes.

- **Previous accepted position:** exact-source `c6ffa3a` paired qualification
  artifact `/tmp/mini_vpn_knife15_macos_20260810_101221.tar.gz` (SHA-256
  `6121b8b8...`) passed baseline `12.791/44.901 Mbit/s`, direct at
  `6.393 Mbit/s` without zero intervals, smoke, every preflight, two exact
  cycles/eight phases, two DNS/real-client checks, and cleanup. All TCP
  sender/receiver interval counts were zero; maximum TCP gap was `5,767,168B`
  and maximum UDP loss was `1.830894%`. Formal M2 correctly remained `NOT_RUN`.
- Conn1 `gap_ack_reinforcements` advanced `0 -> 8 -> 22` during smoke and
  remained exactly `22` through both cycles and drain. The real mature-server
  path therefore reached reviewed implementation `300fb16` without a
  persistent/self-sustaining ACK stream.
- Paired Exit bundle SHA-256 `03d51370...` captured `2,971,792` packets with
  zero kernel drops. Cycle-1/2 reverse maximum supply gaps were
  `251.174/342.575ms`, no qualification flow reached `500ms`, and longest zero
  receive windows were `249.901/341.525ms`; the previous failure paused
  supply for `1,158.373ms`.
- Endpoint high/final was `61,440B / 61,414/0/0B`, with zero interface errors
  and socket would-block. Two `Stopped(0)` writes were timed-transfer close
  tails with D16 queued/leased/reserved `0/0/0B`. Process, DNS, route/TUN
  ownership, secret scan, and cleanup passed.
- Do not repeat/tune qualification. Formal M2 is now reopened on the reviewed
  `c6ffa3a` production-code tree and its docs-only pushed descendant, with the
  frozen workload/config. Take one fresh
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke -> m2
  -> status -> stop`; preserve evidence after failure. M3 remains blocked
  until formal M2 and cleanup pass. Result:
  `docs/tech/2026-08-10-knife15-m2-reverse-gap-ack-reinforcement-macos-qualification-results.md`.

- **Previous accepted position:** exact-source `673d13d` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260810_075621.tar.gz` (SHA-256
  `25847808...`) passed baseline `23.502/64.236 Mbit/s`, bounded direct,
  smoke, every preflight, cycle 1, cycle-2 forward, and
  cleanup. Cycle-2 reverse then lost exactly one complete Target receiver
  interval; formal M2 was not run.
- Conn1 generation 2 stream 2 retained a Ready service certificate with no
  replacement or migration. Its ordered reader stopped at `385,286,941B`
  behind a missing `1,386B` prefix while same-stream buffered tail grew from
  `5,876,229B` to `7,552,771B`; another `2,982` datagrams and `2,981` STREAM
  frames arrived, and the prefix recovered after `1,759ms`.
- Paired Exit artifact SHA-256 `5c26edfd...` identified the exact reverse
  socket with zero capture/kernel drops. Its TCP receive window fell from 3
  to 0, Exit supply paused `1,158.373ms`, and the window reopened immediately
  before supply resumed. D16, TUN, Endpoint, certificate admission, Target,
  routes, process, and cleanup were healthy. This selects QUIC ordered-loss
  recovery plus stream-credit exhaustion; transient WAN loss/RTT growth is a
  contributor, not a frozen-value branch.
- Reviewed pushed `300fb16` adds one bounded ACK reinforcement only after
  exact `STREAM_DATA_BLOCKED` pressure for the same open receive stream with
  an ordered assembler gap and buffered tail. Split packet-number history
  alone is ineligible. One negotiated `max_ack_delay` opportunity is replaced
  by newer ACK progress, cannot self-rearm, and disarms after range collapse.
  `gap_ack_reinforcements` exposes exact branch reachability.
- The deterministic 64KiB flow-control replay blocks the writer beyond its
  receive window, drops the sole gap ACK, and recovers the missing prefix via
  one reinforcement before the sender loss-detection/PTO deadline. Root
  `700+3 ignored`, main `2`, integration `10+4 ignored`, release, established
  Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`, quinn-proto `330`
  plus docs `3`, root docs, fmt/diff/vendor/secret, exact capacity, and review
  all pass with no unresolved P0/P1. Exact 32MiB Endpoint capacity was
  `239.815 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-10-knife15-m2-reverse-gap-ack-reinforcement-local-results.md`.
- Pull/rebuild `300fb16`. When the Mac is ready, start one fresh bounded `.33`
  observer and take exactly one `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> m2-qualification -> status ->
  stop`. Preserve failure evidence; do not run formal M2 or repeat/tune
  unchanged. A recurrence with nonzero reinforcement rejects this mechanism
  as sufficient; zero reinforcement preserves only the exact reachability
  branch for paired analysis. Formal M2 and M3 remain blocked.

- **Previous accepted position:** exact-source `80df179` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260810_055556.tar.gz` (SHA-256
  `9d291f6c...`) passed baseline `23.658/61.795 Mbit/s`, direct at
  `11.822 Mbit/s` without zero intervals, smoke, every preflight, seven mixed
  phases, and cleanup. Cycle-2 short forward then lost one complete Target
  receiver interval; formal M2 was not run.
- Paired Exit SHA-256 `6d5538c9...` saw the exact socket receive `1,506,514B`
  over `10.210s`, maximum supply gap `280.203ms`, Target ACK RTT about
  `1..6ms`, zero retransmit growth, and zero capture/kernel drops. D16, TUN,
  Endpoint, routes, process, and cleanup were healthy.
- The selected generation-2 successor had proved a `24,800B` post-turn cwnd
  floor but was later admitted at `17,360B`, about `173ms` RTT, zero black
  holes, and one congestion event. `17,360 * 7 = 121,520B < 128KiB`, while
  `24,800 * 7 = 173,600B > 128KiB`. Historical black-hole qualification did
  not express stale current service.
- Reviewed pushed implementation `3737dee` persists an exact immutable
  identity/logical-generation/path-generation/post-turn-cwnd certificate.
  Ready requires the same identity/path and current cwnd at least its proved
  floor. Stale auxiliaries reuse the existing fresh replacement/CAS/drain;
  initial or incomplete evidence stays `Unknown`, and bounded fallback is
  preserved.
- Code review found and fixed a possible replacement-decision spin when stale
  evidence lacked an exact qualification epoch. Root `700+3 ignored`, main
  `2`, integration `10+4 ignored`, release, established Clippy, shell,
  vendored Quinn `40+3 ignored` plus doc `1`, quinn-proto `326` plus docs `3`,
  root docs, fmt/diff/vendor/secret all pass with no unresolved P0/P1. Exact
  32MiB Endpoint capacity was `237.868 Mbit/s`, final `61,440/0/0B`, zero
  socket would-block. Result:
  `docs/tech/2026-08-10-knife15-m2-successor-service-certificate-local-results.md`.
- Pull/rebuild the pushed reviewed descendant. When the Mac is ready, start
  one fresh bounded `.33` observer and take exactly one `m2-ipv6-check ->
  baseline -> direct-discriminator -> start -> smoke -> m2-qualification ->
  status -> stop`. Preserve failure evidence; do not run formal M2 or
  repeat/tune unchanged. A recurrence rejects this architecture. Formal M2
  and M3 remain blocked.

- **Previous accepted position:** exact-source `b437b93` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260810_021421.tar.gz` (SHA-256
  `33dddf00...`) passed baseline `25.429/54.539 Mbit/s`, direct at
  `12.715 Mbit/s` without a complete zero interval, smoke, every preflight,
  cycle 1, cycle 2 long forward/reverse TCP and reverse UDP, and cleanup. The
  cycle-2 short forward then lost one complete Target receiver interval;
  formal M2 was not run.
- Paired Exit artifact
  `/tmp/mini_vpn_knife15_exit_target_observer_20260810_020005.tar.gz`
  (SHA-256 `2bc46085...`) saw the exact socket receive about `4.23MB` with no
  supply gap above `165.430ms`, Target ACK RTT about `1..4ms`, zero sender TCP
  retransmit growth, and zero capture/kernel drops. D16 accepted `6,296,563B`
  into Quinn and waited up to `3,820,792us`; the owning generation-3 QUIC cwnd
  grew only `12,947 -> 223,724B`. D16/TUN/Endpoint/routes/process/cleanup were
  healthy. This selects client-to-Exit congestion ownership, not a frozen
  value or downstream branch.
- Production `run_relay_writer` yields through its async Progress signal after
  every successful partial write. The Quinn worker can therefore packetize
  those accepted bytes before the next writer retry reaches Blocked. The prior
  bit was cleared on write progress, so the exact worker-turn flight lost
  ownership; the old test retried immediately and missed this interleaving.
- Reviewed `c218d8a` retains one exclusive per-stream accepted-offset
  boundary through that worker turn and clears it only after cumulative ACK or
  terminal lifecycle. Another stream, control traffic, and later same-stream
  bytes cannot borrow authority. The new RED was
  `24,000 -> 1,772,034B < 1,990,080B`; it and all isolation/lifecycle tests are
  GREEN without changing D16, MTU, pool, windows, chunk, Cubic, GSO, Endpoint,
  self-wake, retry, or workload.
- Root `689+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`,
  quinn-proto `325` plus docs `3`, root docs, fmt/diff/vendor/secret, and
  review pass with no unresolved P0/P1. Exact 32MiB Endpoint capacity was
  `239.784 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-10-knife15-m2-transport-write-demand-flight-ownership-local-results.md`.
- Pull/rebuild the pushed reviewed descendant. When the Mac is ready, start
  one fresh bounded `.33` observer and take exactly one `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> m2-qualification -> status ->
  stop`. Preserve failure evidence; do not run formal M2 or repeat/tune
  unchanged. A recurrence rejects this architecture. Formal M2 and M3 remain
  blocked.

- **Previous accepted position:** exact-source `eb2185f` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260808_092053.tar.gz` (SHA-256
  `100d3163...`) passed baseline `17.521/65.771 Mbit/s`, direct at
  `8.758 Mbit/s` without a complete zero interval, smoke, every preflight,
  long forward/reverse TCP, reverse UDP, and cleanup. The cycle-1 short
  forward then admitted `10,092,544B`, delivered `4,194,304B`, and had one
  complete Target receiver-zero interval; formal M2 was not run.
- Generation 2 installed at `24,886B` cwnd and about `165ms` RTT. Its exact
  writer accepted `6,300,119B`, remained Pending up to `2,367,062us`, and
  sampled QUIC ACKs reached only `3,849,795B`. Target bytes exceeded that
  snapshot, selecting pre-Exit transport supply. D16, TUN, Endpoint
  conservation, routes, process, and cleanup stayed healthy. No fresh paired
  Exit observer existed for this run.
- Reviewed `f9c3c23` retains per-stream write-Blocked demand across Writable
  delivery and assigns ACK growth authority only to packets that actually
  carry that stream. The code-review RED proves a cancelled/deferred writer
  cannot pollute an unrelated stream. The exact active PLPMTUD probe is also
  excluded from successor pre-start adoption while authentication loss stays
  fail-closed.
- Root `689+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`,
  quinn-proto `323` plus docs `3`, root docs, fmt/diff/vendor/secret, and
  review pass with no unresolved P0/P1. Exact 32 MiB Endpoint capacity was
  `240.079 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-08-knife15-m2-transport-write-demand-local-results.md`.
- Pull/rebuild the pushed reviewed descendant. When the Mac is ready, start a
  fresh bounded `.33` observer and take exactly one `m2-ipv6-check -> baseline
  -> direct-discriminator -> start -> smoke -> m2-qualification -> status ->
  stop`. Preserve failure evidence; do not run formal M2 or repeat/tune
  unchanged. Formal M2 and M3 remain blocked.

- **Previous accepted position:** code review of the `1d08565` descendant found
  that a TUIC Authenticate packet could already be in Quinn's Data-space sent
  map before the successor service turn started. It was then outside the
  turn's exact ACK/loss ownership. The same review found that an adopted
  PLPMTUD probe used a special loss branch that left the turn nonterminal.
- Reviewed `6aefd19` adopts every existing ACK-eliciting,
  congestion-accounted Data packet at turn start. Delivered authentication
  succeeds only after exact ACK ownership; withheld authentication and owned
  PLPMTUD loss both fail `PacketLost`. MTUD/congestion policy, turn target,
  deadline, payload, retry, Endpoint, admission, and every frozen value remain
  unchanged.
- Root `689+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`,
  quinn-proto `320` plus docs `3`, root docs, fmt/diff/vendor/secret, and
  review pass with no unresolved P0/P1. Exact 32 MiB Endpoint capacity was
  `240.300 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-08-knife15-m2-successor-authentication-flight-ownership-local-results.md`.
- The accepted Mac evidence remains exact-source `a7cd603`; no new Mac result
  is claimed. Pull/rebuild the pushed reviewed descendant. When the Mac is
  ready, start a fresh bounded `.33` observer and take exactly one
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
  m2-qualification -> status -> stop`. Preserve failure evidence; do not run
  formal M2 or repeat/tune unchanged. Formal M2 and M3 remain blocked.

- **Previous accepted position:** exact-source `a7cd603` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260808_065723.tar.gz` (SHA-256
  `6fae3611...`) passed baseline `28.490/52.003 Mbit/s`, direct at
  `13.970 Mbit/s` without gaps, smoke, every preflight, cycle-1 long
  forward/reverse TCP, reverse UDP, and cleanup. The short forward then lost
  one complete Target receiver interval; formal M2 was not run.
- The prior service-turn ACK repair was present. Conn1 generation 2 passed at
  target/sent/acked/lost `12,000/12,800/12,800/0B` and installed with
  `24,800B` cwnd. Its ordinary business writer remained Pending and
  ACK-progressing, yet the first 128KiB took about `1.21s` to reach the Exit.
- Paired Exit artifact
  `/tmp/mini_vpn_knife15_exit_target_observer_20260808_063213.tar.gz`
  (SHA-256 `ffb39a48...`) captured `2,075,057` packets with zero kernel drops.
  Target ACKed supplied bytes at about `1ms`; client-to-Exit ordinary cwnd
  growth was the limiting seam. The observer is stopped and bundled.
- Reviewed `1d08565` treats only an Endpoint Bulk reservation wait as
  transport backpressure rather than application idle. Control waits, true
  idle, partial batches, Quinn pacing/congestion, Cubic, successor tags,
  loss/path/close, Endpoint accounting, and every frozen value remain
  unchanged. The realistic pair was RED at `12,000 -> 12,000B` across 120
  waits and is GREEN at one full cwnd growth round.
- Root `689+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`,
  quinn-proto `317` plus docs `3`, root docs, fmt/diff/vendor/secret, and
  review pass with no unresolved P0/P1. Exact 32 MiB Endpoint capacity was
  `239.487 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-08-knife15-m2-endpoint-blocked-app-limited-local-results.md`.
- Pull and rebuild the pushed reviewed descendant. When the Mac operator is
  ready, start a fresh bounded `.33` observer and take exactly one
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
  m2-qualification -> status -> stop`. Preserve evidence after failure; do
  not run formal M2 or repeat/tune unchanged. Formal M2 and M3 remain blocked.

- **Previous accepted position:** exact-source `f693d0d` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260808_054225.tar.gz` (SHA-256
  `0e6d8ee6...`) passed baseline `21.003/42.391 Mbit/s`, direct at
  `10.497 Mbit/s` without gaps, smoke, every preflight, the long forward,
  reverse TCP, reverse UDP phases, and cleanup. The first ten-second short
  forward lost one complete Target receiver interval; formal M2 was not run.
- The exact owner was conn1 generation 2. Its successor service turn had
  succeeded at target/sent/acked/lost `12,000/12,800/12,800/0B`, yet the
  installed cwnd was only `13,280B` and receiving bulk did not grow the
  client-to-Exit forward window before the short flow.
- Paired Exit artifact
  `/tmp/mini_vpn_knife15_exit_target_observer_20260808_052126.tar.gz`
  (SHA-256 `4879b790...`) captured `1,569,331` packets with zero kernel drops.
  Target ACKed every Exit-supplied byte at about `1..3ms`; supply into the Exit
  socket was the limiting seam. The observer is stopped and bundled.
- Reviewed `c3c264a` preserves non-application-limited congestion ownership
  for only the existing tagged service-turn ACKs when a later empty transmit
  poll changes Quinn's global app-limited snapshot. Ordinary packets, Cubic,
  turn bytes/deadline/failure, D16, MTU, pool, windows, chunk, Endpoint, GSO,
  recovery, workload, and SLIs remain unchanged.
- The realistic `164ms` RTT replay was RED at
  `initial=12,000B, acked=13,068B, final=23,616B` and is now GREEN. Root
  `689+3 ignored`, main `2`, integration `10+4 ignored`, release, established
  Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`, quinn-proto `316`
  plus docs `3`, root docs, fmt/diff/vendor/secret, and review pass with no
  unresolved P0/P1. Exact 32 MiB Endpoint capacity was `237.701 Mbit/s`,
  final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-08-knife15-m2-successor-service-turn-app-limited-local-results.md`.
- Pull and rebuild the pushed reviewed descendant. When the Mac operator is
  ready, start a fresh bounded `.33` observer and take exactly one
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
  m2-qualification -> status -> stop`. Preserve evidence after failure; do
  not run formal M2 or repeat/tune unchanged. Formal M2 and M3 remain blocked.

- **Previous accepted position:** exact-source `b04cb65` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260808_030357.tar.gz` (SHA-256
  `e5c866f6...`) passed baseline `27.973/63.814 Mbit/s`, direct at
  `13.970 Mbit/s` without gaps, smoke, every preflight, the exact
  `524,550,144B` forward, and cleanup. Cycle 1 reverse never connected and
  hit its 330-second child timeout; formal M2 was not run.
- Conn1 predecessor black holes advanced `0 -> 5`. The successor service turn
  correctly failed closed at target/sent/acked/lost
  `12,000/13,058/10,326/1,366B`; no successor installed. The maintenance error
  was then returned to the triggering business open as
  `handshake_failed -> rearm`, which left iperf unconnected. A later
  independent open passed a fresh turn at `12,800/12,800/0B` and installed
  generation 2, rejecting sustained WAN/VPS failure.
- Paired Exit artifact `/tmp/mini_vpn_knife15_exit_target_observer_20260808_021631.tar.gz`
  (SHA-256 `21b08483...`) captured `547,358` packets with zero kernel drops.
  No new Target socket followed the completed forward. The observer is now
  stopped and bundled.
- Reviewed `68c7271` keeps service-turn failure terminal, preserves the
  predecessor, excludes the failed slot for that business open, and admits
  only another already-qualified current generation. The same open cannot
  replace a second degraded auxiliary; no safe fallback returns both exact
  causes. A later independent open retains fresh replacement authority.
- Root `689+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`,
  quinn-proto `315` plus docs `3`, root docs, fmt/diff/vendor/secret, and
  review pass with no unresolved P0/P1. Exact 32 MiB Endpoint capacity was
  `240.470 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-07-knife15-m2-replacement-failure-business-fallback-local-results.md`.
- Pull and rebuild the pushed reviewed descendant. When the Mac operator is
  ready, start a fresh bounded `.33` observer and take exactly one
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
  m2-qualification -> status -> stop`. Preserve evidence after failure; do not
  run formal M2 or repeat/tune unchanged. Formal M2 and M3 remain blocked.

- **Previous accepted position:** exact-source `7131de4` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260807_091815.tar.gz` (SHA-256
  `7c749fb9...`) passed baseline `35.336/61.978 Mbit/s`, direct at
  `17.659 Mbit/s` without gaps, start/smoke, every preflight, the long
  forward/reverse TCP/reverse UDP phases, and cleanup. The first short
  forward then lost one complete Target receiver interval; formal M2 was not
  run.
- Normalized admission correctly chose conn1 generation 2 at
  `12,000B/163.342ms`; exact D16 writer and Endpoint conservation stayed
  live, but that authenticated replacement had never served forward bulk and
  remained cold while its predecessor had reached `780,994B/164.107ms`.
  Normalized placement is retained; authentication-only successor install is
  the selected seam.
- Reviewed `541fbfb` inserts one exact current-cwnd Quinn service turn between
  TUIC authentication and the existing generation CAS. It uses Endpoint bulk
  service, succeeds only when every tagged encrypted packet byte is ACKed on
  the same path generation, and fails without retry on loss/path/close or the
  existing shared five-second replacement deadline. Failure creates no new
  owner or draining generation.
- Root `688+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`,
  quinn-proto `315` plus docs `3`, root docs, fmt/diff/vendor/secret, and
  review pass with no unresolved P0/P1. Exact 32 MiB Endpoint capacity was
  `240.291 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-07-knife15-m2-quinn-successor-service-turn-local-results.md`.
- The intended prior `.33` observer expired before this Mac run. A fresh
  observer is active at
  `/tmp/mini_vpn_knife15_exit_target_observer_20260807_102734`. Pull/rebuild
  the pushed descendant, then take exactly one
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
  m2-qualification -> status -> stop`. Preserve evidence after failure; do
  not run formal M2 or repeat/tune unchanged. Formal M2 and M3 remain blocked.

- **Previous accepted position:** exact-source `12e845f` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260807_054320.tar.gz` (SHA-256
  `916db92b...`) passed baseline `26.054/60.199 Mbit/s`, 300-second direct at
  `13.020 Mbit/s` without gaps, start/smoke, every preflight, exact transfer,
  and cleanup. Its first qualification forward had two complete sender and
  receiver zero intervals in the first six seconds; formal M2 was not run.
- The paired `.33` observer (SHA-256 `936e8a16...`) captured `515,691`
  packets with zero kernel drops. The exact Exit data socket supplied
  `486,791,898B`, never paused more than `223.987ms`, and Target ACKed around
  `1ms`. Client writer ACKs, D16, TUN, Endpoint conservation, routes, process,
  and cleanup stayed live. This rejects operator/network/Exit-to-Target and
  selects cold TCP-pool placement.
- Control selected warm conn1 from equal active `2:2` using current service
  (`871,763B/163.889ms` versus conn0 `12,000B/164.215ms`). Its reservation
  changed data ownership to `2:4`, so the old raw least-active rule selected
  cold conn0. Reviewed `6e78d23` keeps forward qualification/replacement
  first, then exactly compares `active * RTT / cwnd` only when all admitted
  candidates are busy and known. Idle/unknown/all-degraded fallbacks, current
  flows, recovery, payload, and every frozen value remain unchanged.
- Focused pool `34/34`, root `686+3 ignored`, main `2`, integration `10+4
  ignored`, release, established Clippy, shell, vendored Quinn `40+3 ignored`
  plus doc `1`, quinn-proto `311` plus docs `3`, root docs, fmt/diff/vendor/
  secret, and review pass with no unresolved P0/P1. Exact 32 MiB Endpoint
  capacity was `240.313 Mbit/s`, final `61,440/0/0B`, zero socket would-block.
  Result:
  `docs/tech/2026-08-07-knife15-m2-service-normalized-admission-local-results.md`.
- The paired observer is active on `.33` at
  `/tmp/mini_vpn_knife15_exit_target_observer_20260807_064333`. Next pull and
  rebuild the pushed descendant, then take exactly one fresh
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
  m2-qualification -> status -> stop`. Preserve `status/snapshot/stop` after
  failure. Do not run formal M2 or repeat/tune unchanged. Formal M2 and M3
  remain blocked.

- **Previous accepted position:** exact-source `f8c0639` qualification artifact
  `/tmp/mini_vpn_knife15_macos_20260807_031530.tar.gz` (SHA-256
  `60c60b7f...`) passed baseline `24.026/58.984 Mbit/s`, direct, start/smoke,
  every preflight, long forward/reverse TCP/reverse UDP, and cleanup. Cycle 1
  short forward then had two complete initial Target receiver-zero intervals;
  formal M2 was not run.
- The ordered-gap action fired six Endpoint rebinds on the preceding passing
  long forward. The same missing prefixes coexisted with multi-megabyte tail
  growth and the phase completed `119,087,605B`. The failed short stream was
  uplink-only, accepted `6,296,649B`, waited `4,493,917us`, and had no business
  read/gap signal. This rejects active ordered-gap migration as both unsafe
  and insufficient; do not tune its old `250ms` observation interval.
- Reviewed implementation `847a5d7` removes ordered gaps from recovery
  authority and adds a pure, bounded `RecoveryEvidenceObserver`. Under the
  existing TCP diagnostic switch it emits one ordered-gap observation and
  exact writer-Pending start/end ACK aggregates; it cannot mutate transport.
  Existing exact ACK-stall rebind, writer plus PLPMTUD connection reset, and
  UDP-demand recovery remain unchanged. The runner rejects any legacy
  ordered-gap action or malformed evidence.
- Focused evidence `3`, Endpoint recovery `16`, root `684+3 ignored`, main
  `2`, integration `10+4 ignored`, release, established Clippy, shell,
  vendored Quinn `40+3 ignored` plus doc `1`, quinn-proto `311` plus docs `3`,
  fmt/diff/vendor/secret, and review pass with no unresolved P0/P1. Exact
  32 MiB Endpoint capacity was `240.370 Mbit/s`, final `61,440/0/0B`, zero
  socket would-block. Result:
  `docs/tech/2026-08-07-knife15-m2-recovery-evidence-observer-local-results.md`.
- Next pull/rebuild the pushed descendant and, with the `.33` Exit observer
  active, run exactly one fresh `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> m2-qualification -> status ->
  stop`. It normally takes about 30 minutes and can only produce
  `PASS_NON_ACCEPTANCE`. Preserve `status/snapshot/stop` after failure. Do not
  run formal M2 or repeat/tune unchanged; paired ACK/Exit evidence must select
  the next architecture first. Formal M2 and M3 remain blocked.

- **Previous accepted position:** exact-source `0a71ebe` formal-M2 artifact
  `/tmp/mini_vpn_knife15_macos_20260806_104632.tar.gz` (SHA-256
  `da12448c...`) passed baseline `46.328/59.947 Mbit/s`, direct
  `23.077 Mbit/s` without gaps, start/smoke, every preflight, cycle 1, cycle
  2 forward, and cleanup. Cycle 2 reverse failed with eleven complete
  receiver-zero intervals about 25 minutes into the schedule; the remaining
  fourteen hours were evidence hold time.
- Conn1 stable id `43839895568`, stream 21 stopped its ordered read at
  `569,624,937B` for `12,630ms`. Its connection had received `111` datagrams,
  `96,732B`, and `62` STREAM frames by the first complete Pending report, but
  historical evidence cannot attribute those frames to stream 21. D16, TUN,
  Endpoint conservation, routes, process, and cleanup stayed healthy. This
  selects exact same-stream ordered-gap observation and peer-visible
  migration, not tuning.
- Reviewed implementation `4d168a1` adds a read-only Quinn receive-progress
  handle and per-generation live-reader registry. An unchanged exact
  same-stream gap must survive two observations and one full existing `250ms`
  sampler interval with active workload before one covered Endpoint rebind.
  Read progress, gap change, close, inactivity, or generation replacement
  clears authority. QUIC identity, stream, Target TCP, bytes, pool, D16, and
  old-socket lifecycle remain; ACK-stall rebind retains precedence.
- Root `684+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, shell, vendored Quinn `40+3 ignored` plus doc `1`,
  quinn-proto `311` plus docs `3`, root docs, fmt/diff/secret, and review pass
  with no unresolved P0/P1. Exact 32 MiB Endpoint capacity was
  `240.403 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-06-knife15-m2-ordered-gap-path-migration-local-results.md`.
- Next pull/rebuild the pushed descendant and run exactly one fresh
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
  m2-qualification -> status -> stop`. The qualification is two mixed cycles,
  normally about 30 minutes, and can produce only `PASS_NON_ACCEPTANCE`.
  Preserve `status/snapshot/stop` after failure. Do not run formal M2, tune,
  or repeat unchanged. Formal M2 and M3 remain blocked.

- **Previous accepted position:** exact-source `0c521fa` artifact
  `/tmp/mini_vpn_knife15_macos_20260806_101142.tar.gz` (SHA-256
  `2a45314d...`) passed baseline `34.964/8.262 Mbit/s`, direct
  `13.539 Mbit/s` without gaps, start/smoke, every qualification preflight,
  the exact `300 + 300 + 180 + 10` second cycle, DNS/real-client checks, and
  cleanup. Verdict is `PASS_NON_ACCEPTANCE`; formal M2 is `NOT_RUN`.
- Qualification TCP forward/reverse/short had zero sender/receiver intervals;
  max sender/receiver gap was `7,208,960B`. Reverse UDP loss was `0.223184%`.
  Endpoint max/final was `61,440B / 61,403/0/0B`, with zero TUN/interface
  errors and complete route/DNS/process cleanup.
- No path reset fired. Conn0 had black holes `0 -> 12` but exact writer wait
  only `274,915us`, below the unchanged minimum `2s` bound; conn1 stayed at
  zero. This is a correct healthy comparator and real-WAN false-positive
  check, not a predicate mismatch. The selected failure remains distinct at
  `7,001,335us` plus `144` black holes on the same connection.
- Two `Stopped(0)` write lines were timed-transfer close tails: the peer had
  ended, every D16 queue/lease/reservation was zero, data SLIs passed, and no
  ownership debt remained. They explain `internal_failure_scan=REVIEW` but
  are not active-transfer failures. Result:
  `docs/tech/2026-08-06-knife15-m2-connection-local-path-state-recovery-macos-qualification-results.md`.
- Next pull/rebuild the pushed descendant and run exactly one fresh formal
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke -> m2
  -> status -> stop` with every other VPN off. Preserve
  `status/snapshot/stop` after failure. Do not run another qualification or
  tune. An applied reset followed by a complete healthy-control receiver-zero
  rejects the architecture. M3 remains blocked until full M2 plus cleanup.

- **Previous accepted position:** exact-source `bfaba9e` artifact
  `/tmp/mini_vpn_knife15_macos_20260806_083422.tar.gz` (SHA-256
  `74300b2e...`) passed baseline `32.551/58.899 Mbit/s`, direct
  `16.268 Mbit/s` without gaps, start/smoke, preflights, exact `610,402,304B`
  forward completion, and cleanup. That 300-second qualification forward had
  three complete Target receiver-zero intervals around seconds 101/103/105;
  the exact writer waited up to `7,001,335us`.
- The owning conn1 advanced PLPMTUD black holes `0 -> 144`, recorded
  `2,147,556B` QUIC loss and `537` congestion events, and reached MTU `1280`;
  healthy conn0 had no comparable growth. Paired Exit observer artifact
  `/tmp/mini_vpn_knife15_exit_target_observer_20260806_065748.tar.gz`
  (SHA-256 `e95880ae...`) had zero capture/kernel drops, Target TCP RTT
  `1..9ms`, no retransmit growth, and full Target ACKs for every byte supplied
  by the Exit while QUIC application supply decayed. This selects
  client-to-Exit per-connection QUIC path-state degradation, not operator,
  physical baseline, Exit-to-Target, TUN, D16, Endpoint, or a frozen value.
- Reviewed implementation `0e94e56` adds one-shot per-stable-identity recovery:
  exact writer Pending for the existing RTT-derived bound plus same-window
  black-hole growth resets only that connection's configured Quinn
  congestion/RTT/MTUD state. Same connection, TUIC stream, Target TCP, shared
  socket, and payload remain; no replay/migration/tuning occurs. Exact
  ACK-stall rebind retains precedence and cannot overlap the reset.
- Root `679+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, Knife15/Knife14 shell, vendored Quinn `39+3 ignored`
  plus doc `1`, quinn-proto `310` plus docs `3`, root docs, fmt/diff/secret,
  and review pass with no unresolved P0/P1. Exact 32 MiB Endpoint capacity was
  `240.348 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Result:
  `docs/tech/2026-08-06-knife15-m2-connection-local-path-state-recovery-local-results.md`.
- Next pull/rebuild the pushed descendant, keep every other VPN off, and after
  the `.33` observer is active take exactly one fresh `m2-ipv6-check ->
  baseline -> direct-discriminator -> start -> smoke -> m2-qualification ->
  status -> stop`. Do not run formal `m2`. Qualification is about `13m10s`
  of scheduled traffic and can produce only `PASS_NON_ACCEPTANCE`. An applied
  reset plus another healthy-control receiver-zero interval rejects this
  architecture without tuning/repeat. Formal M2 and M3 remain blocked.

- **Previous accepted position:** exact-source `727f00b` artifact
  `/tmp/mini_vpn_knife15_macos_20260806_035626.tar.gz` (SHA-256
  `4013a05b...`) passed baseline `38.747/47.278 Mbit/s`, direct
  `19.361574 Mbit/s` without gaps, start/smoke, every preflight, cycle-1 long
  TCP/reverse TCP/reverse UDP, and cleanup. Its first 10-second short forward
  then had two complete Target receiver-zero intervals; Mac/Target bytes were
  `10,092,544/4,325,376B`.
- The data stream visibly consumed the one-turn startup service and the Exit
  ACKed QUIC data. D16, smoltcp, Endpoint conservation (`61,414/0/0B` final),
  gateway/Exit controls, routes, interfaces, process, and cleanup stayed
  healthy. The unresolved seam is after Exit QUIC receipt and before Target
  TCP delivery. Retain the bounded startup code but reject it as sufficient;
  do not tune priority, pool, MTU, windows, chunk, Cubic, GSO, pacing,
  recovery, workload, or SLIs.
- Exact-order bare Exit-to-Target control passed all four phases. Sixty fresh
  short connections then passed 600/600 receiver intervals with one total
  retransmit. This rejects a persistent/readily recurring bare path limit but
  cannot classify the historical TUIC-to-direct-TCP copy boundary.
- Reviewed `4d02355` adds a non-acceptance `m2-qualification` action for one
  exact `300 + 300 + 180 + 10` second mixed cycle and a bounded Exit observer:
  exact `.77:5201` filter, 96-byte snapshots, 340,000,000-byte ring, 250ms
  TCP_INFO samples, two-hour timeout, identity-safe stop, drop/version/secret/
  checksum evidence. Formal M2 remains blocked; qualification success is only
  `PASS_NON_ACCEPTANCE` and formal acceptance stays `NOT_RUN`.
- Root `673+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, Knife15/Knife14 shell, vendored Quinn `38+3 ignored`
  plus doc `1`, quinn-proto `310` plus docs `3`, root docs, fmt/diff/secret,
  and review pass with no unresolved P0/P1. Exact 32 MiB Endpoint capacity was
  `237.737 Mbit/s`, final `61,440/0/0B`, zero socket would-block. Final real
  observer smoke bundle SHA-256 is `b119ec7e...`. Result:
  `docs/tech/2026-08-06-knife15-exit-target-forwarding-observability-local-results.md`.
- Next pull/rebuild `4d02355` or its pushed descendant. After the agent starts
  the observer on `.33`, take exactly one fresh `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> m2-qualification -> status ->
  stop`; then stop/bundle the observer. Do not run formal `m2`. Matching Mac
  and Exit evidence must select kernel/path, mature-server copy service,
  client-to-Exit QUIC, or observer mismatch before another architecture.
  M3 remains blocked.

- **Previous accepted position:** exact-source `f570353` artifact
  `/tmp/mini_vpn_knife15_macos_20260805_111712.tar.gz` (SHA-256
  `b0d3815e...`) passed baseline `34.319/52.001 Mbit/s`, direct
  `17.151374 Mbit/s` without gaps, start/smoke, every preflight, seven mixed
  cycles, and cleanup. Cycle 8 `tcp-forward` then had one complete initial
  Target receiver-zero interval although the local sender had already
  admitted `2,228,224B`; the exact transfer later completed.
- The data/control streams used installed conn1 generation 2. Same-window
  gateway/Exit, routes, interfaces, process, TUN, D16, Endpoint conservation,
  and recovery remained healthy. This fires the auxiliary-replacement stop
  rule: retain its bounded lifecycle protection but reject it as sufficient
  first-stream service. Do not tune selectors, pool, MTU, windows, chunk,
  Cubic, GSO, Endpoint, recovery, workload, or SLIs.
- Reviewed implementation `f7260ee` gives every TUIC TCP stream relative
  Quinn priority `original + 1` through Connect and its first accepted
  nonempty business write, then restores the exact original priority under
  the same Quinn connection lock. Blocked/empty writes remain armed. All
  generic/native/D16 modes share one writer; UDP, queues, capacity, timers,
  connection admission, and frozen values are unchanged.
- Root `685+3 ignored`, main `2`, integration `10+4 ignored`, release,
  established Clippy, Knife15/Knife14 shell, vendored Quinn `38+3 ignored`,
  quinn-proto `310`, docs, fmt/diff/secret, and review pass with no unresolved
  P0/P1. Exact 32 MiB Endpoint capacity reached `240.076 Mbit/s` with final
  `61,440/0/0B` and zero socket would-block. Result:
  `docs/tech/2026-08-05-knife15-m2-quinn-new-stream-startup-service-local-results.md`.
- Next pull/rebuild, keep Clash-TUN/every other VPN off, avoid deliberate
  heavy non-test traffic, and take exactly one fresh `m2-ipv6-check ->
  baseline -> direct-discriminator -> start -> smoke -> m2 -> status ->
  stop`. Normal Apple Push/iCloud may remain. Preserve
  `status/snapshot/stop` after failure. A consumed startup service plus the
  same healthy-control installed-successor receiver zero rejects this
  architecture and opens transport first-payload ACK/failover research. M3
  remains blocked pending complete M2 plus cleanup acceptance.

- **Previous accepted position:** exact-source `a1e22ca` artifact
  `/tmp/mini_vpn_knife15_macos_20260805_080607.tar.gz` (SHA-256
  `f5f6d933...`) passed baseline `19.183/50.639 Mbit/s`, direct
  `9.584823 Mbit/s` without gaps, start/smoke, IPv6/full-tunnel/real-client,
  the bounded early stop, and cleanup. The only decisive relay was Apple Push
  `28-courier.push.apple.com:5223`; closing all user Apps was correct but
  cannot eliminate normal system traffic.
- The Apple relay triggered fifteen false endpoint rebinds in about fifty
  seconds: generic `no_rx`, `udp_active=false`, no exact writer pressure, and
  `37B` QUIC transport TX. The prior global-zero host model is rejected.
- Implementation `1debaff` keeps exact TCP writer/stream ACK-stall recovery,
  but generic no-RX now requires existing UDP application demand. M2 replays
  exact lifecycle for its three controlled Targets and drains only that
  ownership; ambient global counts remain observations. Endpoint debt,
  conservation, DNS drops, replay ambiguity, quality, resources, routes,
  cleanup, and all frozen values remain fail-closed/unchanged.
- Focused `9/9`, root `672+3 ignored`, main `2`, integration `10+4 ignored`,
  release, established Clippy, shell, vendored Quinn/proto/doc, exact 32 MiB
  `235.232 Mbit/s` with `61,440/0/0B`, fmt/diff/secret, and review pass with no
  unresolved P0/P1. Result:
  `docs/tech/2026-08-05-knife15-m2-ambient-traffic-recovery-local-results.md`.
- Next pull/rebuild, keep all other VPNs off, avoid intentional heavy traffic,
  and take one fresh `m2-ipv6-check -> baseline -> direct-discriminator ->
  start -> smoke -> m2 -> status -> stop`. Normal Apple Push/iCloud may
  remain. M3 stays blocked until full M2 plus cleanup acceptance.

- **Previous accepted position:** exact-source `798c1a5` artifact
  `/tmp/mini_vpn_knife15_macos_20260805_015011.tar.gz` (SHA-256
  `386ecafc...`) passed baseline/direct, start/smoke, every formal preflight,
  the four-hour steady-a window, 17 mixed cycles plus cycle 18 forward, and
  cleanup. Its 154 valid results had zero Target receiver gaps, max TCP gap
  `10,354,688B`, max UDP loss `0.604884%`, and Endpoint max/final
  `61,440B / 61,403/0/0B`.
- Auxiliary generation replacement ran correctly: degraded conn1 generation
  1 advanced PLPMTUD black holes `192 -> 193`; generation 2 authenticated and
  installed in `689ms`, then served new opens. The predecessor remained
  bounded drain-only for its existing Google push flow. No healthy-control
  installed-successor receiver-zero interval occurred, so the architecture
  stop rule did not fire.
- M2 failed at the first idle checkpoint only because the full tunnel still
  owned four non-test relays and fake-IP `7/19`: Apple/Google push, WeChat,
  and Cursor. The existing checkpoint correctly failed and is unchanged. The
  missing control was an early dedicated-machine quiescence prerequisite, not
  operator command misuse or a data-plane failure.
- Reviewed runner commit `4ceb2ed` requires fresh zero-ownership data-plane and
  Endpoint observations within the existing smoke timeout (normally `50s`)
  after full-tunnel/real-client preflight and before the 86,400s schedule. It
  records structured evidence and distinguishes dirty traffic, process
  health, and evidence-write failure. No Rust, workload, SLO, or frozen value
  changed. Result:
  `docs/tech/2026-08-05-knife15-m2-full-tunnel-quiescence-fail-fast-local-results.md`.
- Next pull the pushed descendant, rebuild release, quit every non-test
  network App and every VPN, and take exactly one fresh `m2-ipv6-check ->
  baseline -> direct-discriminator -> start -> smoke -> m2 -> status ->
  stop`. A dirty host must now fail before the long schedule. Preserve
  `status/snapshot/stop`; do not tune or repeat unchanged. M3 remains blocked.

- **Previous accepted position:** exact-source `fd6c34f` xiaoou bundle
  `/tmp/mini_vpn_knife15_macos_20260804_095117.tar.gz` (SHA-256
  `889cdd276c...`) passed baseline at `29.369/44.838 Mbit/s`, direct at
  `14.675 Mbit/s` with no gaps, start/smoke, IPv6/full-tunnel/real-client
  gates, the first mixed cycle, and cleanup. The first short forward then had
  one complete initial receiver-zero interval.
- The reviewed busy-epoch discriminator correctly excluded conn1 after its
  PLPMTUD black holes advanced `0 -> 16`. Both opens used qualified conn0,
  which stayed at zero black holes but had fourteen existing lease halves,
  `5,140B/175ms` service, and a `1,169,717us` writer wait. Selector-only
  isolation is therefore rejected. The preceding reverse UDP independently
  lost `3.391937%`, above the frozen `3%` SLI.
- Runner `addc54d` makes formal UDP loss fail at the phase boundary.
  Implementation `5533d15` adds bounded auxiliary generation replacement:
  the degraded predecessor becomes drain-only, an authenticated successor is
  atomically installed and receives the triggering reservation, and exact
  generation-zero closes only the predecessor. Primary conn0/UDP/health,
  eligible pool two, existing flows, D16, Endpoint, MTU, windows, chunk,
  Cubic, GSO, self-wake, workload, and SLOs remain unchanged.
- Focused `31/31`, root `671+3 ignored`, main `2`, integration `10+4 ignored`,
  release, Clippy, Knife15/Knife14 shell, vendored Quinn/proto, 32 MiB
  `239.967 Mbit/s` with `61,440/0/0B`, fmt/diff/secret, and code review pass
  with no unresolved P0/P1. Result:
  `docs/tech/2026-08-04-knife15-m2-auxiliary-generation-replacement-local-results.md`.
- Next take exactly one fresh real-Mac `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> m2 -> status -> stop` from the
  pushed reviewed descendant with a rebuilt release and every other VPN off.
  Preserve `status/snapshot/stop` after any post-start failure. Do not tune or
  repeat unchanged. An installed-successor healthy-control receiver-zero
  rejects this architecture and opens Quinn scheduling/failover research. M3
  remains blocked pending full M2 plus cleanup acceptance.

- **Previous accepted position:** exact-source `99e56b0` xiaoou Ethernet bundle
  `/tmp/mini_vpn_knife15_macos_20260803_093012.tar.gz` (SHA-256
  `804b96c5...`) passed baseline at `34.527/60.550 Mbit/s`, the 300-second
  direct discriminator at `17.264 Mbit/s` without sender/receiver gaps,
  start/smoke, IPv6/full-tunnel/real-client gates, long cycle-1 TCP, and
  reverse UDP. The first short forward then lost two complete receiver
  intervals while exact physical/Exit controls, TUN, D16, Endpoint
  (`61,412/0/0B`), resources, routes, and cleanup remained healthy.
- Both opens chose strictly less-loaded conn1 (`active_before=6`, then `8`),
  rejecting the prior equal-load tie hypothesis. Conn1 also had greater
  current `cwnd/RTT` (`12,887B/176ms`) than conn0 (`6,665B/176ms`), yet its
  writer waited `4,214,880us`. Conn1 advanced from zero to ten PLPMTUD
  black-hole detections during one nonzero ownership epoch while conn0 stayed
  at zero. The earlier Wi-Fi run is only a clean-short comparator.
- Implementation `b40aa75` adds categorical busy-epoch qualification inside
  the deep TCP admission module. It commits an idle anchor only after winning
  reservation CAS, isolates a proven degraded busy slot only when a
  qualified/unknown alternative exists, and keeps an explicit bounded
  all-degraded fallback. Existing flows, UDP, reconnect, pool, MTU, pacing,
  windows, D16, Endpoint, and all frozen values are unchanged.
- Focused `27/27`, root `678+3 ignored`, main `2`, integration `10+4 ignored`,
  release, Clippy, shell, vendored Quinn/proto, 32 MiB `240.154 Mbit/s`,
  fmt/diff/secret, and code-review gates pass with no unresolved P0/P1.
  Observer-only repair `3ad7128` is separate. Result:
  `docs/tech/2026-08-03-knife15-m2-busy-epoch-forward-qualification-local-results.md`.
- Next take exactly one fresh HK `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> m2 -> status -> stop` from the
  pushed reviewed descendant with a rebuilt release. Keep every other VPN off,
  restore IPv6 only at the runbook boundary, and do not tune or repeat
  unchanged. M3 remains blocked pending full M2 plus cleanup acceptance.

- **Previous accepted position:** exact-source `6231048` HK preflight directories
  `/tmp/mini_vpn_knife15_macos_baseline_20260803_035615` and
  `/tmp/mini_vpn_knife15_macos_direct_20260803_035733` bind the reviewed
  runner/binary and each other. Baseline receiver continuity passed at
  `12.930/49.357 Mbit/s` forward/reverse.
- The 300-second direct Target discriminator correctly reduced offered load to
  `6.462 Mbit/s` but failed with seven complete receiver-zero seconds in two
  nonterminal episodes (`62–65s`, `152–156s`). Sender zeroed at the same
  boundaries, recorded 533 retransmits, and collapsed cwnd to `1,344B`.
- This is a physical Target-path continuity preflight failure before TUN
  startup, not mini_vpn, path-service selection, D16, Endpoint, M2, or a slow
  bandwidth rejection. Do not execute `start`; use the runbook pre-start
  branch to restore IPv6 immediately. No `status/snapshot/stop` is required.
  Result:
  `docs/tech/2026-08-03-knife15-hk-m2-direct-continuity-preflight-failure-results.md`.
- Do not repeat unchanged in the same network window. After a materially later
  or repaired path, take a fresh `m2-ipv6-check -> baseline -> direct`; only a
  fresh PASS may continue immediately to `start -> smoke -> m2 -> status ->
  stop`. No code or frozen value changed. M3 remains blocked.

- **Previous reviewed position:** recovered exact-source `6f1df4a` HK bundle
  `/tmp/mini_vpn_knife15_macos_20260801_053127.tar.gz` (SHA-256
  `d4fc3bc6...`) proves the `cebf30d` kernel-reap cleanup repair worked:
  low/high/fake markers released without foreign route mutation, DNS/process
  cleanup passed, and the immutable artifact finalized.
- Formal M2 itself failed cycle 2 `short-forward-1` on a first complete
  `1.001225s` Target receiver interval of `0B`. Exit/gateway controls, routes,
  TUN, Endpoint conservation (`61,404/0/0B`), D16 closure, and authenticated
  QUIC progress were healthy. This is not an observer tail, operator error,
  outage, or frozen-value tuning branch.
- Control chose conn1 at `active_before=60`; after the two relay half-leases,
  data saw an equal busy `62:62` tie and stable index chose conn0. Current path
  state was conn0 `10,124B/163ms` versus conn1 `23,842B/163ms`; the conn0 D16
  writer waited `3,355,211us`, 4.4x the comparable passing conn1 flow. This
  selects equal-busy placement without current QUIC send-service evidence.
- The reviewed selector keeps lease count first and idle `conn0 -> conn1`, but
  breaks equal nonzero ties by exact known `cwnd/RTT`; unknown/equal stays
  stable. Sampling is nonblocking, refreshed after preparation waits, and
  affects no reconnect, pool, pacing, or payload hot path. Focused `22/22`,
  root `673+3 ignored`, main `2`, integration `10+4 ignored`, release, Clippy,
  shell, vendored Quinn/proto, >170 Mbit/s 32 MiB capacity, fmt/diff/secret,
  and review gates pass with no unresolved P0/P1. Result:
  `docs/tech/2026-08-02-knife15-m2-path-service-aware-pool-selection-local-results.md`.
- Next take exactly one fresh HK
  `m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke -> m2
  -> status -> stop` from the pushed reviewed source with a rebuilt release
  binary. Do not tune or repeat unchanged. M3 remains blocked pending full M2
  plus cleanup acceptance.

- **Previous reviewed position:** exact-source `6f1df4a` real HK M2 run
  `/tmp/mini_vpn_knife15_macos_20260801_053127` passed baseline/direct,
  start/smoke, IPv6/full-tunnel/real-client preflight, and cycle 1. Cycle 2
  failed `short-forward-1` on `receiver_zero_interval`; that remains an
  independent, unclassified bundle-review failure.
- Stop could not bundle. The 151-line diagnostic
  `/tmp/mini_vpn_knife15_cleanup_diag_20260801_053127.txt` (SHA-256
  `146eb0e1...`) proves `utun4` absent, six IPv4 probes restored to exact
  `en0/192.168.133.1`, and current/saved Ethernet DNS both `EMPTY`. Only
  low/high/fake ownership markers remained one; DNS and Exit ownership were
  already zero. macOS reaped the three interface routes with the utun and the
  runner rejected its stale markers.
- The repair releases a stale marker without route mutation only when the
  owned utun is absent and the current route exactly matches the recorded
  physical interface/gateway. Live/reused utun, foreign tunnel/interface,
  changed gateway, missing observation, DNS/IPv6 mismatch remain fail-closed.
  Result:
  `docs/tech/2026-08-02-knife15-m2-kernel-route-reap-cleanup-local-results.md`.
- Next pull the pushed repair on the test Mac and run only `stop`, then
  `bundle`, to recover the existing artifact. Do not rerun baseline/M2 or
  mutate routes/DNS manually. Replay the bundle before classifying the
  receiver-zero failure. M3 remains blocked.

- **Latest accepted position:** exact-source `19b5ceb` HK bundle
  `/tmp/mini_vpn_knife15_macos_20260801_044032.tar.gz` (SHA-256
  `651f977b...`) followed the new `Automatic -> Off` service procedure and
  passed start/smoke, but formal M2 repeated the IPv6 error before M2 evidence
  creation. M2 remained `not_run`; Endpoint and cleanup passed.
- A local exact-probe replay returned status zero while printing the known
  `route: writing to routing socket: not in table` absence. The old runner
  checked that text only for nonzero status, so it tried and failed to parse
  an interface. This is a runner observer false negative, not proof that IPv6
  remained enabled. The first runbook repair also checked a different address
  from formal M2.
- The repaired shared classifier accepts the exact `not in table` line only
  when no interface is present, independent of status; it accepts only
  `lo0`/`utun*` successful routes, rejects physical interfaces, and keeps every
  other outcome unknown/fail-closed. The public
  read-only `m2-ipv6-check` uses the exact formal probe before baseline.
  Formal M2 preserves raw status/text/class/interface in the bundle and
  publishes decisive summary/status fields. Result:
  `docs/tech/2026-08-01-knife15-m2-ipv6-route-status-observability-local-results.md`.
- Next pull the pushed repair and run only `m2-ipv6-check` after disabling the
  exact physical-service IPv6. Take no baseline until it reports
  `safe_absent` or `safe_tunnel` plus PASS. Restore IPv6 immediately on check
  failure. M3 remains blocked pending formal M2 acceptance.

- **Latest accepted position:** exact-source `753691a` HK bundle
  `/tmp/mini_vpn_knife15_macos_20260801_041142.tar.gz` (SHA-256
  `93d384a...`) passed target-only start/smoke, then formal M2 reported its
  physical-IPv6 error before any M2 route/DNS mutation. The bundle preserved
  neither the raw route output nor its status, so the original physical-route
  inference was not proven. M2 status/full-tunnel/acceptance remained
  `not_run/not_run/NOT_RUN`; this was not a smoke or data-plane failure.
- Smoke forward/reverse receivers were `63.428/49.695 Mbit/s`; pool ownership
  drained, Endpoint conservation ended `61,414/0/0B`, controls and cleanup
  passed, and the one `Stopped(0)` boundary had zero D16 ownership. The
  operator's `status/snapshot/stop` response was correct.
- Current mini_vpn M2 is IPv4-only and must not claim full-tunnel acceptance
  while physical IPv6 can bypass it. The later exact-probe reproduction
  selected an observer false negative and supersedes this bundle's earlier
  physical-route inference; the safety boundary itself remains unchanged.
- The HITL runbook now derives the exact service owning the physical Exit
  interface, permits only a recorded `Automatic -> Off` temporary change,
  verifies the global IPv6 route is absent before baseline, and restores the
  same service immediately after a pre-start failure or only after `stop` once
  start was invoked. Result:
  `docs/tech/2026-08-01-knife15-hk-m2-ipv6-precondition-results.md`.
- Superseded next action: first run the repaired public `m2-ipv6-check`; only a
  PASS can authorize fresh baseline/direct evidence. Restore IPv6 immediately
  on a pre-start failure; once `start` is invoked, restore only after `stop`.
  M3 remains blocked pending formal M2 acceptance.

- **Latest accepted position:** Knife15 M2 local implementation, gates, and
  review are complete at `4dac87c`. The public `m2` action owns a controlled
  IPv4 full tunnel and system DNS for an exact `86,400s` schedule, then
  requires two-phase cleanup before formal acceptance. No Rust data-plane or
  frozen Knife14/M1 value changed.
- The immutable schedule has six active windows, five 600-second idle/resume
  boundaries, a 600-second final drain, 93 complete DNS/real-client cycles,
  `934` TCP plus `95` UDP results, `1,029` phase results, and six
  lifecycle/resource checkpoints. Partial cycles retain phase evidence but use
  a separate contiguous complete-cycle identity for the 93 HTTPS records.
- M2 records the physical Exit path and DNS snapshot, pins Exit physical,
  routes both IPv4 halves and `198.18.0.0/15` through the owned utun, blocks a
  physical IPv6 route, and probes public egress plus real HTTPS through the
  system resolver/fake-IP path. Cleanup removes only matching owned state and
  restores the exact DNS snapshot.
- Endpoint/D16/M1 receiver, TCP-gap, UDP-loss, rebind, resource, disk, log, and
  cleanup gates remain fail-closed. Fake-IP checkpoints require zero active
  ownership and a stable one/two-entry cache because the frozen `1,800s` TTL
  intentionally exceeds the `600s` drain.
- Final root `667+3 ignored`, main `2`, integration `10+4 ignored`, release,
  Clippy, Knife15/Knife14 shell, syntax, fmt/diff/secret, vendored Quinn
  `37+3 ignored` plus doc `1`, quinn-proto `309` plus doc `3`, and code review
  pass with no unresolved P0/P1. Results:
  `docs/tech/2026-07-30-knife15-m2-24h-real-client-soak-local-results.md`.
- Next pull the pushed source, disable Clash-TUN/every other VPN from baseline
  through stop, and run exactly one fresh HK
  `baseline -> direct-discriminator -> start -> smoke -> m2 -> status -> stop`
  using
  `docs/tech/2026-07-30-knife15-m2-macos-hitl-runbook.md`. Reserve about 25
  hours. On any start/smoke/M2 failure preserve
  `status -> snapshot -> stop`; do not tune or repeat unchanged. M3 remains
  blocked pending real M2 bundle acceptance.

- **Latest accepted position:** exact-source `ee1a423` HK formal M1 bundle
  `/tmp/mini_vpn_knife15_macos_20260729_105127.tar.gz` (SHA-256
  `c263507c...`) was operated correctly and completed the entire frozen
  `28,800s` schedule: five active windows, 30 cycles/DNS checks, three
  idle/resume pairs, final drain, `302` TCP plus `30` UDP results, and four
  clean checkpoints.
- The old runner reported `acceptance SLO mismatch` only after logging
  `m1 complete`. It counted an unchanged lifetime `en0` input-error counter
  (`16 -> 16`) as 1,539 error samples and rejected two normal
  relay-task snapshots with `19,456B`/`18,944B` already leased to smoltcp.
  The same handles proved equal local-EOF queues, terminal send queues of
  zero, zero reap, and no reuse before drain.
- Focused TDD makes physical errors movement/reset-aware and replaces the
  global queue grep with a same-handle fail-closed lifecycle validator.
  Counter movement/reset, open terminal queues, queued/reserved bytes,
  unequal/missing drain, terminal ownership, and cross-epoch reuse still
  fail. No Rust data-plane or frozen value changed.
- Immutable-bundle replay reports formal M1 PASS. Target receiver zero
  intervals were `0`; maximum TCP gap was `10,485,760B`, maximum UDP loss
  `2.446087%`, Endpoint conservation max/final was
  `61,440B / 61,403/0/0B`, checkpoints were stable, and cleanup passed.
  Eighty-eight `Stopped(0)` boundaries and 22 sender-zero intervals remain
  visible `CLASSIFIED_REVIEW` evidence; neither violates receiver or ownership
  acceptance.
- Root `667+3 ignored`, main `2`, integration `10+4 ignored`, release,
  Clippy, Knife15/Knife14 shell, syntax, fmt/diff/secret, real-bundle replay,
  and review gates pass with no unresolved P0/P1. Result:
  `docs/tech/2026-07-30-knife15-hk-m1-formal-acceptance-results.md`.
- Knife15 M1 is complete; do not repeat it. M2 planning is unblocked. Next
  write the M2 24-hour real-client soak architecture spec and implementation
  plan for product-like TCP/UDP/video/DNS/idle activity, bounded logs/resources,
  route/DNS leak checks, and Shenzhen/HK path attribution. Keep M3
  single-variable recovery work sequenced after that contract.

- **Previous reviewed position:** exact-source `0b43141` HK M1 diagnostic bundle
  `/tmp/mini_vpn_knife15_macos_20260727_085340.tar.gz` (SHA-256
  `0fdfbfd4...`) was operated correctly and ran 7h34m43s before cycle 32
  reverse TCP ended with `control socket has closed unexpectedly`.
- The failure is externally attributed. Exit `43.153.32.33` changed from
  `3/3`, 0% loss at `16:28:33Z` to `0/3`, 100% loss at `16:29:04Z`, while
  the local gateway stayed `3/3`, 0% loss. Exit loss then persisted for 861
  consecutive samples through `00:04:07Z`; TUIC rebind/reconnect could not
  recover an unreachable host. `utun1024` appeared only about nine hours after
  the diagnostic failed, immediately before stop, and did not cause it.
- Before the outage, 305 completed TCP/UDP results, 27 DNS results, and all
  three idle/resume checkpoints were valid. The diagnostic ledger was empty;
  UDP loss maxed at `2.127049%`, TCP gap at `12,451,840B`, and Endpoint
  checkpoints were `61,403/0/0B`. Conservation, D16, pool idle, TUN,
  resources, and interfaces remained healthy. About 39 minutes of the frozen
  schedule remained.
- This is useful post-`44ff086` long real-Mac non-regression evidence, but it
  is neither a complete diagnostic nor formal M1 acceptance. No product,
  runner, SLI, or frozen-value change is selected. Result:
  `docs/tech/2026-07-28-knife15-hk-m1-diagnostic-exit-outage-results.md`.
- Do not repeat the diagnostic. Once the Exit VPS is confirmed continuously
  powered and its TUIC service stable, take fresh baseline/direct evidence and
  run one formal `start -> smoke -> m1 -> status -> stop`. Keep every other
  VPN/TUN off through stop. M2/M3 remain blocked pending formal M1.

- **Previous reviewed position:** exact-source `5c3cd95` HK bundle
  `/tmp/mini_vpn_knife15_macos_20260727_074752.tar.gz` (SHA-256
  `27f105d8...`) safely completed `start -> smoke -> stop` in about 49
  seconds. The repaired pool-idle barrier reached `active_leases=0`; both TCP
  directions, Endpoint/D16 conservation, controls, resources, routes, and
  cleanup passed. No half-closed-idle block occurred.
- This is post-`44ff086` smoke non-regression evidence, not an M1 diagnostic.
  M1 status/mode remained `not_run`, with no controller, results, checkpoints,
  or violation ledger. `DNS_TARGET` was disabled in start-owned state, and the
  otherwise passing direct result was already 1,875 seconds old at start,
  beyond the frozen 900-second limit. A later DNS export cannot change the
  existing run state.
- Baseline receivers were `30.885/52.530 Mbit/s` with zero gaps; direct was
  `15.412 Mbit/s` with zero receiver gaps. Smoke was `26.763/48.275 Mbit/s`;
  its four forward sender-zero rows remain the accepted cold-start REVIEW
  class. No code or frozen value changed. Result:
  `docs/tech/2026-07-27-knife15-hk-post-repair-smoke-only-results.md`.
- Next take a fresh uninterrupted HK
  `baseline -> direct-discriminator -> start -> smoke -> m1-diagnostic ->
  status -> stop` sequence. Export `DNS_TARGET=8.8.8.8` before `start` and
  enter M1 diagnostic within 900 seconds of direct completion. Do not stop
  after smoke. The diagnostic cannot accept M1 or unblock M2/M3.

- **Previous reviewed position:** exact-source `a3ebe42` HK bundle
  `/tmp/mini_vpn_knife15_macos_20260724_105822.tar.gz` (SHA-256
  `b5ce3af4...`) was operated correctly. Both smoke TCP commands and fake-IP
  DNS completed, but smoke's pool-idle barrier failed: reverse-data handle 1
  epoch 3 retained one native lease and exactly `524,288B` queued through four
  identical 10-second half-close windows. M1 diagnostic did not run.
- Endpoint conservation ended `61,414/0/0B`; three rebinds recovered,
  TUN/pump/interfaces/resources stayed healthy, and stop cleaned everything.
  The old relay timer nevertheless re-armed on ownership presence alone,
  despite roughly 19,000 subsequent egress windows with no ownership progress.
  This is a local bounded-lifecycle defect, not user operation, path loss,
  pacing, or a frozen-value tuning branch.
- Commit `44ff086` publishes monotonic queued-to-leased/permit-release progress
  from the D16 queue. The first owned half-close window is preserved; later
  windows re-arm only after real local progress. Static ownership reaches the
  existing timeout. Root `667+3 ignored`, main `2`, integration `10+4
  ignored`, release, Clippy, shell, vendored Quinn/proto, capacity,
  fmt/diff/secret, and review gates pass with no unresolved P0/P1. Result:
  `docs/tech/2026-07-26-knife15-hk-smoke-half-close-progress-results.md`.
- Next pull the pushed repair, rebuild release, and take a fresh user-run HK
  `baseline -> direct-discriminator -> start -> smoke -> m1-diagnostic ->
  status -> stop` sequence. Keep every other VPN/TUN off through stop and
  preserve status/snapshot/stop on failure. The diagnostic cannot accept M1
  or unblock M2/M3.

- **Previous reviewed position:** exact-source `f2c1484` HK pre-run bundle
  `/tmp/mini_vpn_knife15_macos_20260724_103153.tar.gz` (SHA-256
  `07f7e620...`) completed `start -> smoke -> stop`; M1 diagnostic was not
  run. Binary/runner provenance, routes, bidirectional TCP, fake-IP DNS,
  smoke pool idle, Endpoint conservation, D16 ownership, resources, and
  cleanup pass.
- Forward smoke had four top-level zero-rate intervals, but they are local
  sender evidence because smoke has no embedded server JSON. The preceding
  six real HK M1 smoke bundles had `4/4/10/8/0/4` cold-start sender zeros, so
  this is not a new regression or a useful long-run rejection gate. One
  `1/3` Exit ICMP loss sample recovered immediately; the gateway stayed
  lossless. Do not tune or add an over-broad smoke gate. Result:
  `docs/tech/2026-07-24-knife15-hk-diagnostic-prerun-smoke-results.md`.
- Because the run was stopped, next take a fresh
  `baseline -> direct-discriminator -> start -> smoke -> m1-diagnostic ->
  status -> stop` sequence. The diagnostic cannot accept M1 or unblock M2/M3.

- **Latest accepted position:** M1 diagnostic continuation is implemented
  locally at `b675540`. `m1-diagnostic` runs the exact frozen `28,800s` M1
  schedule, records valid receiver-zero intervals, reverse-UDP loss above
  `3%`, and aggregate TCP gap above `16MiB`, and continues to final drain.
  Formal `m1` is unchanged and remains the only acceptance path.
- The violation TSV is typed and source-bound. Final summary replays each row
  against its JSON and proves there are no missing observations. A second
  validator relaxes only receiver positivity, so a zero plus any malformed
  field still fails. Commands/timeouts, DNS, TUN/routes/watchdog, checkpoints,
  samples, Endpoint/D16/pump/TUN signals, resource/rebind/ownership mismatches,
  and ledger corruption remain fail-closed.
- Root `666+3 ignored`, main `2`, integration `10+4 ignored`, release, Clippy,
  shell, syntax, fmt/diff/secret, and review gates pass; no P0/P1 remains.
  Result:
  `docs/tech/2026-07-24-knife15-m1-diagnostic-continuation-local-results.md`.
- Next use a fresh user-operated HK Mac from the pushed source:
  `baseline -> direct-discriminator -> start -> smoke -> m1-diagnostic ->
  status -> stop`. Rebuild release, use fresh `M1_BASELINE_DIR` /
  `M1_DIRECT_DIR`, and keep every other VPN/TUN disabled through stop. The
  artifact is diagnostic and cannot accept M1 or unblock M2/M3; formal M1
  still needs a separate passing run.

- **Previous accepted position:** exact-source `297dee9` HK bundle
  `/tmp/mini_vpn_knife15_macos_20260724_034941.tar.gz` (SHA-256
  `182cc896...`) was operated correctly. Fresh baseline/direct and the
  repaired smoke pool-idle barrier passed. M1 completed two active windows and
  two idle checkpoints before the first `steady-b` forward phase failed about
  3h20m into the run with three complete Target receiver-zero seconds.
- The previous repair worked: both checkpoints ended at `61,403/0/0B`, and
  cycle 15 control/data split conn0/conn1 from an idle pool. The data writer
  waited `9,256,932us`; conn1 added `783` lost packets, `898,964B` loss,
  `366` congestion events, and `18` PLPMTUD black holes. Direct Exit ICMP lost
  one of three probes in the same interruption while the gateway, TUN,
  Endpoint conservation, D16 ownership, resources, and cleanup stayed healthy.
- Reverse UDP independently exceeded the frozen `3%` SLO in cycle 8
  (`3.356208%`) and cycle 12 (`3.408435%`); each aligned with direct Exit RTT
  or loss degradation, with zero mini_vpn internal UDP drops/backpressure.
- This is an external single-path M1 failure, not a local repair or tuning
  target. M2/M3 remain blocked. Do not repeat in the same window. A fresh
  later-window HK M1 remains allowed. A healthy-control TCP repeat opens
  connection isolation/failover; a healthy-control UDP repeat opens UDP/TUIC
  quality architecture. Result:
  `docs/tech/2026-07-24-knife15-m1-hk-path-quality-failure-results.md`.

- **Previous accepted position:** exact-source `f60926e` HK bundle
  `/tmp/mini_vpn_knife15_macos_20260722_110059.tar.gz` (SHA-256
  `9071ec0f...`) was operated correctly. Baseline was `24.371/30.882 Mbit/s`;
  direct passed 300 seconds at `12.181 Mbit/s`. M1's first forward phase then
  had one complete initial Target receiver-zero second.
- Smoke completed at `11:01:45Z` and M1 began at `11:01:46Z`. The old conn1
  reverse-data relay still held two native half leases. M1 control occupied
  conn0; the `2:2` tie placed M1 data on conn0 too; conn1 reaped only after the
  open. Earlier comparable starts drained first, split the pair across the
  pool, and had positive first intervals. User operation, delayed stop,
  physical continuity, TUN, pacing, conservation, and rebind are rejected.
- TUIC now publishes transition-only TCP-pool `active_leases`. Smoke waits for
  exact zero within its existing hard timeout, and formal M0/M1 independently
  require zero. Missing/malformed/nonzero evidence fails closed with TUN
  evidence preserved. There is no fixed sleep, selector change, SLI waiver, or
  frozen-value tuning.
- Root `666+3 ignored`, main `2`, Quinn `37+3 ignored + doc 1`, quinn-proto
  `309 + doc 3`, release, Clippy, shell, fmt/diff/secret, and review gates
  pass. Capacity is `240.322 Mbit/s`, final `61,440/0/0B`; no P0/P1 remains.
  Result:
  `docs/tech/2026-07-22-knife15-m1-post-smoke-pool-idle-local-results.md`.
- Next use the pushed repair for one fresh user-run HK M1 with rebuilt release
  and fresh baseline/direct. Keep every other VPN/TUN off through `stop` and
  preserve status/snapshot/stop on failure. M2/M3 remain blocked.

- **Previous accepted position:** exact-source `e479013` HK bundle
  `/tmp/mini_vpn_knife15_macos_20260722_094608.tar.gz` (SHA-256
  `7443769e...`) was operated correctly. Fresh baseline was
  `31.258/26.417 Mbit/s`; direct passed 300 seconds at `15.621 Mbit/s` with
  zero receiver gaps. M1 cycle 1 forward then had three complete Target
  receiver-zero seconds while seven `tcp_write_stall` rebinds all reported
  connection-level recovery.
- The previous Pending-only trigger was over-broad. Generations 3--7 repeatedly
  changed source port while the business writer remained blocked; current-
  socket RX proved shared connection activity, not ACK progress for the
  failing stream. TUN, physical controls, resources, Endpoint conservation,
  and cleanup stayed healthy. This is a false-rebind positive-feedback loop,
  not operator, baseline, route, or frozen-capacity failure.
- Vendored Quinn/proto now exposes monotonic per-send-stream acknowledged
  bytes. TUIC owns separate writer/stream Pending episodes and permits the
  existing rebind only when both Pending age and exact-stream ACK-stall age
  reach the unchanged RTT bound. Other-stream ACKs cannot mask a black hole;
  same-stream ACK progress prevents normal flow-control pressure from
  rebinding. One action covers every sampled writer episode.
- Root `653+3 ignored`, main `2`, Quinn `37+3 ignored + doc 1`, quinn-proto
  `309 + doc 3`, release, Clippy, shell, fmt/diff, secret, and code-review
  gates pass. The 32 MiB gate reached `240.472 Mbit/s` with final Endpoint
  conservation `61,440/0/0B`. No frozen value changed and no unresolved P0/P1
  remains. Result:
  `docs/tech/2026-07-22-knife15-m1-stream-ack-qualified-rebind-local-results.md`.
- This partial run is not M1 acceptance. Next take one fresh user-run HK M1
  from the pushed repair with rebuilt release and fresh baseline/direct. Keep
  every other VPN/TUN off through `stop`; preserve `status/snapshot/stop` on
  failure. M2/M3 remain blocked.

- **Previous accepted position:** exact-source `b33a3f6` HK M1 bundle
  `/tmp/mini_vpn_knife15_macos_20260722_075828.tar.gz` (SHA-256
  `f8b5b2ea...`) was operated correctly. Fresh baseline/direct, start/smoke,
  and cycle 1 passed; cycle 2 forward then contained one complete Target
  receiver-zero interval at `151.001053s -> 152.001047s`.
- The sender paused for most of `149s -> 167s`. Conn1 added `356` QUIC lost
  packets, `480,190B` lost bytes, and `112` congestion events; cwnd fell to
  `208,427B`. Its D16 writer blocked for `11,279,769us`. The frozen `32 MiB`
  send window represents about `12.177s` at the `22.042647 Mbit/s` offer, so
  the evidence is capacity-consistent with unacknowledged-data saturation.
  ACK/control RX continued and hid the failure from the old no-RX monitor.
- TUIC now tracks lock-free continuous `poll_write -> Pending` ownership per
  pool connection. The existing recovery monitor rebinds once when an episode
  reaches the unchanged RTT-derived bound even if RX progresses, and one
  rebind covers every then-pending episode. Ready/error/drop clears ownership;
  a cleared/new episode may rearm. Existing no-RX and authenticated set-wise
  current-generation recovery remain unchanged.
- Root `650+3 ignored`, main `2`, TUIC `100`, Quinn `36+3 ignored + doc 1`,
  quinn-proto `309 + doc 3`, release, Clippy, shell, fmt/diff, and secret gates
  pass. The 32 MiB gate reached `240.585 Mbit/s` with exact conservation. Code
  review has no unresolved P0/P1 and no frozen value changed. Result:
  `docs/tech/2026-07-22-knife15-m1-tcp-write-stall-rebind-local-results.md`.
- This partial run is not M1 acceptance. Next take one fresh user-run HK M1
  from the pushed repair with a rebuilt release and fresh baseline/direct.
  Keep every other VPN/TUN off through `stop`; on failure preserve
  `status/snapshot/stop`. M2/M3 remain blocked.

- **Previous accepted position:** exact-source `1c587ba` HK M1 bundle
  `/tmp/mini_vpn_knife15_macos_20260721_110225.tar.gz` (SHA-256
  `62280ba0...`) was operated correctly, passed fresh baseline/direct, then
  failed after about 6h43m in cycle 28 `steady-c/short-reverse-6`. The local
  receiver had eight complete zero-byte seconds; this is not a partial-tail
  observer failure.
- Conn1 business RX paused `8,301ms` while conn0 control RX paused `10,733ms`.
  Endpoint rebind triggered correctly, but old Quinn released its previous
  socket after the first pooled connection reached the current socket; the
  other connection had not yet proved migration. TUN/pump/Endpoint ownership,
  routes, controls, resources, and cleanup stayed healthy.
- Quinn now snapshots all live handles at rebind and retains one previous
  socket until every snapshot connection authenticates on the current
  generation or drains. Routed-but-unauthenticated, old-socket, stale-
  generation, and post-snapshot connection traffic cannot complete recovery.
  mini_vpn also requires every sampled pool connection generation before
  logging recovery.
- Code review found and repaired the unauthenticated-routing P1. Root
  `646+3 ignored`, main `2`, Quinn `36+3 ignored + doc 1`, quinn-proto
  `309 + doc 3`, release, Clippy, shell, fmt/diff, and secret gates pass. The
  32 MiB capacity gate reached `232.164 Mbit/s` with exact conservation. No
  frozen value changed and no P0/P1 remains. Result:
  `docs/tech/2026-07-21-knife15-m1-multi-connection-rebind-retention-local-results.md`.
- This real bundle also had two independent reverse-UDP windows above the
  `3%` SLO (`4.421980%`, `4.127822%`). The rebind repair does not waive or
  claim to fix them. Next take one fresh user-run HK M1 from the pushed repair
  with a rebuilt release and fresh baseline/direct. Keep every other VPN/TUN
  off through `stop`; on failure preserve `status/snapshot/stop`. M2/M3 remain
  blocked.
- **Previous accepted position:** the first real HK M1 bundle
  `/tmp/mini_vpn_knife15_macos_20260720_090636.tar.gz` (SHA-256
  `5fd183b1...`) used exact source `dc8cfb1` and was operated correctly. It
  completed five full mixed cycles before cycle 6 forward was falsely rejected
  as `receiver_zero_interval`.
- Cycle 6 transferred `292,945,920B` over the full 300-second command. Its
  Target receiver had `300` complete positive intervals followed by one final
  `0.163918s` zero command tail. Target remained on `utun4`, Exit on `en0`,
  Endpoint conservation stayed within `61,440B` and ended `61,403/0/0B`, and
  process/TUN/routes cleaned completely. This was not user, sudo, Clash, route,
  or mini_vpn data-plane failure.
- The local runner repair excludes only a numeric, final, sub-`0.5s` zero row
  proven at the command boundary. Complete, nonterminal, missing-timing, and
  malformed zeros still fail closed. Real artifact replay plus shell, Rust,
  release, Clippy, fmt, diff, and secret gates pass; review has no unresolved
  P0/P1. Result:
  `docs/tech/2026-07-20-knife15-m1-partial-tail-observer-repair-results.md`.
- The bundle is partial and does not pass M1. Next use the pushed repair,
  rebuild release, and take fresh M1 baseline/direct artifacts before one new
  user-run HK `start -> smoke -> m1 -> status -> stop`. Keep all other VPN/TUN
  disabled through stop. On failure use `status -> snapshot -> stop`. M2/M3
  remain blocked.
- **Previous accepted position:** Knife15 M1 local implementation is complete at
  pushed commit `2cca535`. The runner now owns the exact `28,800s` schedule,
  five active windows, three idle/resume boundaries, four fresh drain
  checkpoints, `302` TCP + `30` UDP + `30` DNS results, resource/recovery
  SLOs, child hard deadlines, and fail-closed cleanup. All shell, Rust,
  release, Clippy, fmt, diff, and secret gates pass; review has no unresolved
  P0/P1. Result:
  `docs/tech/2026-07-17-knife15-m1-eight-hour-soak-local-results.md`.
- No real M1 TUN run occurred in the implementation stage. The next action is
  one user-run HK macOS M1 from the exact reviewed source. Clash-TUN and every
  other VPN/TUN must be completely disabled before baseline and remain off
  through `stop`. Use fresh `M1_BASELINE_DIR` and `M1_DIRECT_DIR`, then run
  `start -> smoke -> m1 -> status -> stop`. On failure use
  `status -> snapshot -> stop`. M2/M3 remain blocked until bundle review.
- **Previous accepted position:** Knife15 M0 is complete. The independent rearm
  bundle `/tmp/mini_vpn_knife15_macos_20260717_063948.tar.gz` (SHA-256
  `f4e0f649...`) used source `d3f7b13` with the exact same release-binary and
  runner hashes as the accepted main run. It created fresh `utun4`, kept Exit
  on `en1`, passed forward/reverse TCP and fake-IP DNS, then stopped with the
  process dead, TUN unavailable, and Target/Exit/physical routes all on `en1`.
- Endpoint conservation passed and ended `61,277/0/0B`; interface errors, log
  compactions, abandoned bytes, and stranded ownership were zero. The one
  forward `Stopped(0)` and exact bounded reverse local-close release are the
  already accepted command-boundary REVIEW classes. No unresolved P0/P1.
- The accepted two-hour main bundle remains `...035908.tar.gz` (SHA-256
  `1ecee823...`). Together the pair closes M0. Do not repeat M0, baseline, or
  direct. Next write the explicit M1 eight-hour SLO/spec and deterministic
  runner/TDD plan from the M0 envelope before changing or executing the runner.
  Result:
  `docs/tech/2026-07-17-knife15-hk-m0-rearm-acceptance-results.md`.

- **Previous accepted position:** exact-source `8bc7b7c` HK M0 bundle
  `/tmp/mini_vpn_knife15_macos_20260717_035908.tar.gz` (SHA-256
  `1ecee823...`) passes the two-hour main run and first cleanup. The fresh
  300-second direct prerequisite passed at `18.101918 Mbit/s` with zero
  sender/Target-receiver zero intervals. M0 completed eight full mixed cycles,
  both planned forward bookends, idle/resume, final drain, `74` result files,
  and `8/8` DNS checks with zero phase/health failures and zero direction-aware
  receiver-zero intervals.
- Endpoint conservation held at or below `61,440B` and ended
  `61,403/0/0B`; RSS/FD/thread envelopes were bounded, interface errors and log
  compactions were zero, secret scan passed, and stop removed the process/TUN
  and restored physical routes. No endpoint rebind was needed.
- The bundle's `REVIEW` consists of command-boundary Quinn `Stopped(0)` events
  with zero D16 queue ownership, plus one pre-M0 smoke local-close release of
  exactly the accepted `524288B + 27840B` bounded tail. All formal receiver
  evidence stayed positive and final ownership is zero; review found no
  unresolved P0/P1.
- At that point M0 was not fully closed and M1 remained blocked until one
  independent fresh `start -> smoke -> stop` rearm bundle passed. It did not
  require another baseline, direct, or two-hour M0. Result:
  `docs/tech/2026-07-17-knife15-hk-m0-main-run-results.md`.

- **Previous accepted position:** exact-source `13faccc` HK bundle
  `/tmp/mini_vpn_knife15_macos_20260717_033923.tar.gz` (SHA-256
  `a189b848...`) started with Target, Exit, and DNS on physical `en1`, routed
  Target/DNS into `utun4`, and passed forward/reverse TCP plus fake-IP DNS
  smoke. `DNS_TARGET=8.8.8.8` was correctly captured this time.
- The forward iperf tail produced one canonical `remote_write_failed` with
  Quinn `Stopped(0)`. D16 queued/leased/reserved ownership was zero, Endpoint
  conservation passed, no rebind occurred, and reverse/DNS succeeded next.
  Existing Knife15 rules classify this as expected `REVIEW` close-tail
  evidence, not a stop gate. A manual generic grep instruction was wrong and
  caused the user to stop before M0; `m0_status=not_run`.
- The first direct attempt `...033055` was user-interrupted by SIGINT; the
  complete `...033233` repeat passed but is now stale. Rerun one fresh
  300-second direct discriminator, then start/smoke/M0 immediately. Do not
  stop for a lone boundary `Stopped(0)` with zero ownership and successful
  subsequent smoke. Do stop for a smoke command failure, nonzero receiver
  interval failure in formal evidence, nonzero stranded ownership, connection
  failure/rebind, or a non-equivalent terminal error.
- Another VPN created `utun1024` after smoke but before snapshot/stop. It did
  not cause the close tail, but it contaminated final route/counter evidence.
  Keep other VPNs off until Knife15 `stop` has completed. Result:
  `docs/tech/2026-07-17-knife15-hk-smoke-stopped0-classification-results.md`.

- **Previous accepted position:** the first three post-`fe3ec83` Shenzhen
  physical baseline attempts failed before any TUN or mini_vpn execution. Attempt
  `...134708` connected both iperf control/data sockets but produced zero
  intervals; client reported `control socket has closed unexpectedly` and
  Target reported `idle timeout for receiving data`. The Target service stayed
  active with zero restarts.
- Attempt `...135653` completed both directions but measured forward
  `2.598 Mbit/s` with two Target receiver zeros and 24 retransmits, and reverse
  `0.098 Mbit/s` with four local receiver zeros and 50 retransmits. Reverse
  already used the accepted `1KiB` observer; three consecutive zeros plus
  `1,388-2,776B` cwnd prove real physical stalls, not low-rate quantization.
- Attempt `...140906` again completed both directions. Reverse receiver had
  no zero interval at `0.566 Mbit/s`, but forward Target receiver repeated one
  complete zero second plus a zero short tail at `2.444 Mbit/s`; the client
  recorded 31 retransmits and cwnd down to `1,388B`. This rejects a tail-only
  validator bug and confirms the current Shenzhen window is discontinuous.
- Formal M0 remains blocked before direct/start. No failed directory may
  be exported as `M0_BASELINE_DIR`. Do not tune mini_vpn or relax the SLI.
  Retry in a different network window or use another Mac whose direct physical
  path passes the unchanged baseline; there is still no minimum Mbps. Result:
  `docs/tech/2026-07-16-knife15-shenzhen-post-rebind-physical-baseline-results.md`.

- **Previous accepted position:** exact source `8e9daf9` Shenzhen M0 bundle
  `/tmp/mini_vpn_knife15_macos_20260716_110535.tar.gz` (SHA-256
  `f9799711...`) was operated correctly. Smoke and the entire first M0 cycle
  passed. Cycle 2 forward failed itself after about `65s`; the user did not
  interrupt it or wait too long at sudo. Status/snapshot/stop later preserved
  and cleaned the evidence.
- The fresh physical baseline measured `31.447419 Mbit/s` forward and
  `0.487728 Mbit/s` reverse. The fresh 300-second direct gate passed at
  `15.716047 Mbit/s` receiver with no zero interval. Slow Shenzhen capacity
  remains environment evidence, not a mini_vpn bug.
- Conn0 and conn1 shared Endpoint/source port `64195`, served traffic for
  about 18 minutes, then timed out together. Connection-local reconnect reused
  the same endpoint/socket identity and timed out. TUN, endpoint pacing,
  resources, physical controls, sing-box, and iperf remained healthy. This
  selects an endpoint-wide UDP receive/service outage and rejects operation,
  pacing, D16, local pressure, or service restart as the immediate cause.
- Commit `0460886` adds one endpoint-owned, one-shot live UDP socket rebind.
  Active TX without any Endpoint RX is sampled at `250ms` and triggers before
  the frozen 15-second QUIC idle timeout using
  `clamp(8 * max_rtt, 2s, 7s)`. The same Quinn Endpoint, both QUIC
  connections, active TUIC streams, and EndpointWindowV1 conservation state
  survive the source-port change. Existing reconnect remains the fallback.
- Review found and fixed a P1: Quinn temporarily receives on the retained old
  socket, so aggregate connection RX alone could falsely prove recovery. A
  recovery now requires a known connection packet on the current socket's
  rebind generation. The runner requires that generation in positive evidence.
- Final local gates pass: focused policy `4/4`, root library `646 passed + 3
  ignored`, main `2/2`, Quinn-proto `309/309 + 3/3` doc, Quinn `29 passed + 3
  ignored + 1/1` doc, release, all-target check/Clippy, Knife15/Knife14 shell,
  fmt, and diff. Review has no unresolved P0/P1; no frozen parameter or SLI
  changed. Results:
  `docs/tech/2026-07-16-knife15-macos-m0-cycle2-endpoint-timeout-results.md`
  and
  `docs/tech/2026-07-16-knife15-endpoint-socket-rebind-recovery-local-results.md`.
- Next synchronize the pushed commit to Shenzhen and build release. Take a
  fresh physical baseline and fresh 300-second direct discriminator because
  source/runner/binary changed. Direct PASS permits start/smoke/M0. If rebind
  occurs, require one trigger and current-socket RX recovery; any receiver-zero
  interval, missing trigger, missing recovery, or repeated same-episode rebind
  rejects the architecture and does not authorize constant tuning.

- **Previous accepted position:** exact source `c50613c` clean rearm bundle
  `/tmp/mini_vpn_knife15_macos_20260716_100056.tar.gz` (SHA-256
  `baee8f2f...`) was operated correctly and exposed a deterministic local TCP
  close-lifecycle defect. Start was ready on `utun5`; the target route stayed
  on `utun5`, the Exit stayed on `en0`, and the user correctly interrupted the
  hung smoke before snapshot/stop/clean cleanup. `curl ipinfo.io` remaining in
  Shenzhen is expected because this is a target-only TUN, not a default route.
- The first smoke relay wrote exactly `37B` of iperf control, then received no
  remote stream frame for `11.045s`. D16 queue closure first installed
  `remote_eof` and called `socket.close()`, but the later
  `remote_read_failed` event immediately rearmed/aborted the same smoltcp
  socket before its FIN could pass `iface.poll + flush_tx`. Local iperf3 saw
  neither FIN nor RST and waited indefinitely. This is not Shenzhen speed,
  sudo timing, operator procedure, endpoint pacing, TUN pressure, or an old
  five-tuple permanent blackhole.
- `.33` capture contained bidirectional UDP `8443` on the fresh source port,
  including handshake/recovery bursts; later fresh connections exchanged
  heartbeats. `.33` also logged authentication timeouts, so the network path
  remains lossy and transport stability is not yet accepted, but packet
  direction no longer blocks the local lifecycle repair.
- Commit `9f68435` coalesces a same-epoch terminal relay event into an existing
  deferred close, upgrades `remote_eof` to the first non-clean cause, and lets
  the bounded drain state machine deliver local FIN/reset semantics before
  rearm. A full in-memory TUN + dual-smoltcp RED test previously ended
  `Established`; it now observes local failure within the two-second bound.
  The macOS smoke runner also hard-bounds each iperf command at
  `duration+30s`, preserves evidence, and returns `124` on timeout.
- Full local gates pass: library `640 passed + 3 ignored`, main `2 passed`,
  focused D16 close tests, release build, all-target harness check/Clippy,
  Knife15 and Knife14 shell suites, fmt, and diff checks. Review has no
  unresolved P0/P1. Frozen H10d16 constants and SLI are unchanged.
- The next action is to get commit `9f68435` onto the Shenzhen Mac and build a
  fresh release binary. Because source, runner, and binary changed, the old
  `...064427` baseline and `...071534` direct result are historical evidence,
  not formal provenance for the next M0. Run fresh baseline and direct, then
  one bounded start/smoke. A smoke PASS permits formal M0; a smoke failure now
  terminates within the hard bound and must be preserved with status/snapshot/
  stop. Do not tune frozen constants. Result:
  `docs/tech/2026-07-16-knife15-macos-smoke-close-lifecycle-results.md`.

- **Previous accepted position:** exact source `c50613c` bundle
  `/tmp/mini_vpn_knife15_macos_20260716_072536.tar.gz` (SHA-256
  `dd8a28f0...`) correctly failed M0 cycle 1 forward. Both pool connections on
  one shared endpoint timed out after the accepted baseline/direct pair; this
  motivated the clean rearm plus `.33` capture above. Result:
  `docs/tech/2026-07-16-knife15-macos-m0-shared-quic-timeout-results.md`.

- **Previous accepted position:** the synchronized Shenzhen direct directory
  `/tmp/mini_vpn_knife15_macos_direct_20260716_071534` is a formal 300-second
  direct continuity PASS. Its result SHA-256 is `810e777b...`; the Target
  receiver delivered `420,741,120B` at `11.213247 Mbit/s` for `300.174s`, with
  `300` complete positive intervals and no receiver or sender zero interval.
  The sender reported one retransmit.
- The manifest is `status=pass/reason=ok`, binds the accepted baseline's exact
  two JSON hashes, source `c50613c`, the accepted runner/binary hashes, and
  physical `en0` routes for both Target and Exit. The requested rate was the
  exact half-forward baseline rate, `11,218,349 bit/s`. Current-HK live state
  is excluded from this copied Shenzhen evidence.
- This pair was the accepted prerequisite for source `c50613c` and permitted
  its old start/smoke/M0 attempt. It is now historical evidence and cannot
  provide formal provenance for the `9f68435` repair at the top of this file.
  Result:
  `docs/tech/2026-07-15-knife15-macos-low-rate-reverse-observer-results.md`.

- **Earlier accepted position:** the exact copied Shenzhen archive
  `/tmp/mini_vpn_knife15_macos_baseline_20260716_064427.tar.gz` (SHA-256
  `5ea583c5...`) reaches the 1KiB reverse observer and passes the formal
  direction-aware baseline validator. Physical forward Target receiver was
  `22.436698 Mbit/s`, `57,147,392B`, with `21/21` positive intervals. Physical
  reverse local receiver was `0.181787 Mbit/s`, `454,656B`, with `20/20`
  positive intervals. The reverse sender still showed nine zero intervals,
  50 retransmits, about `164-170ms` RTT, and max cwnd `8,328B`; those are
  physical-path diagnostics and do not invalidate the receiver SLI.
- The archive contains only the expected two regular JSON files and its
  checksum is exact. `blksize=1024` proves the repaired observer was reached;
  baseline artifacts do not bind an exact source commit. Slow speed remains
  Shenzhen environment capacity, not a mini_vpn bug. The next action, still
  without mini_vpn/TUN, is the unchanged 300-second direct discriminator using
  `/tmp/mini_vpn_knife15_macos_baseline_20260716_064427`. Only a direct PASS
  permits user-run start/smoke/M0. Result:
  `docs/tech/2026-07-15-knife15-macos-low-rate-reverse-observer-results.md`.

- **Earlier accepted position:** the exact copied Shenzhen archive
  `/tmp/mini_vpn_knife15_macos_baseline_20260715_154130.tar.gz` (SHA-256
  `49c4b71a...`) proves the 16KiB reverse observer reached iperf. Physical
  forward was `31.798980 Mbit/s`, all Target intervals positive, with `9,420`
  sender retransmits. Physical reverse was only `0.131071 Mbit/s`, five local
  receiver zeros, 45 retransmits, `169-184ms` RTT, and max cwnd `8,328B`.
  mini_vpn/TUN was absent: slow speed and loss are environment profile, not a
  product bug.
- Every positive reverse interval remained a 16KiB multiple; useful delivery
  was also about 16KiB/s and the block exceeded cwnd. Commit `e49d83c` therefore
  uses 1KiB only for reverse baseline/M0, giving about eight blocks/s at the
  latest formal half-rate. Baseline now prints direction-aware receiver Mbit/s
  and zero counts before any verdict. Forward/direct/short-forward, rates,
  durations, UDP1160, and all frozen mini_vpn settings remain unchanged.
  Knife15/Knife14 shell, syntax, fmt/diff, and review pass with no P0/P1.
- Next rebuild final HEAD on Shenzhen and run one fresh physical-`en0`
  baseline; do not reuse `...131049` or `...154130`. Slow values are accepted
  as capacity. Positive 1KiB receiver intervals continue to direct/M0. Zeros
  with 1KiB stop formal M0 as physical continuity evidence, not a mini_vpn bug;
  a later degraded-path lane must remain separate from formal acceptance.
  Result:
  `docs/tech/2026-07-15-knife15-macos-low-rate-reverse-observer-results.md`.

- **Previous accepted position:** the user-operated source `5c127eb` M0 bundle
  `/tmp/mini_vpn_knife15_macos_20260715_104417.tar.gz` (SHA-256
  `5743b352...`) is exact and correctly operated. Its prerequisite direct run
  `/tmp/mini_vpn_knife15_macos_direct_20260715_102948` passed all 300 Target
  receiver seconds at `6.626752 Mbit/s` and was 615 seconds old at M0 start.
  Password timing, stale evidence, route/provenance mismatch, premature
  cleanup, and operator error are rejected.
- Sustained forward, sustained reverse, and reverse UDP passed. The first
  short-forward flow failed after global round-robin history inverted the pair:
  the UDP phase opened its TCP control on conn0, then short control/data opened
  on conn1/conn0. The short sender wrote `10,616,832B`, but Target received only
  `3,670,016B`; the first two receiver seconds were zero and the conn0 writer
  waited `3.864382s`. Endpoint ownership, TUN/pump, resources, Exit/gateway,
  and exit-side Connect timing remained clean. This is a deterministic pool-
  placement architecture defect, not a frozen-constant issue.
- Commit `c945a41` replaces the global cursor with stable, least-active atomic
  reservation plus a per-slot RAII preparation gate held through slot-mutex
  wait, optional probe/reconnect, and connection clone. Review caught and
  repaired the pre-clone overtaking risk in the initial counter-only design.
  Focused pool tests passed `15/15`; all-target library tests passed `640 + 3
  ignored`, main `2/2`; release, Clippy, Knife15/Knife14 shell gates, fmt, and
  diff checks pass. No unresolved P0/P1 or frozen-setting change remains.
- Next action is user-run `cargo build --release`, then a fresh baseline and
  300-second direct discriminator because the binary source changed. A direct
  PASS permits immediate start/smoke/M0. M0 must show short control/data on
  conn0/conn1 and no receiver-zero interval. If placement is corrected but the
  short SLI still fails, do not repeat or tune constants; reopen the flow-
  specific QUIC/path branch with a usable same-path mature-client control. A
  passing M0 is followed by an independent rearm; M1 remains blocked until
  both pass. Results:
  `docs/tech/2026-07-15-knife15-macos-m0-directpass-pool-parity-results.md` and
  `docs/tech/2026-07-15-knife15-macos-m0-lease-aware-pool-selection-architecture-spec.md`.

- **Previous accepted position:** exact source `5e8846f` repaired-observer short
  bundle `/tmp/mini_vpn_knife15_macos_20260715_084113.tar.gz` (SHA-256
  `85dd7a66...`) passes its scoped gate. All `5/5` physical-interface rows are
  semantically numeric, errors remain zero, RX/TX counters increase by
  `185,341,406/106,914,245B`, derived rates are meaningful, and all five Exit/
  gateway controls are complete with zero loss. TCP smoke completed both
  directions with `20/20` nonzero intervals, DNS returned `198.18.0.2`,
  endpoint conservation stayed `<=61,365B`, and process/utun/routes/ownership
  cleaned up.
- `internal_failure_scan: REVIEW` is one forward `Stopped(0)` close tail
  repeated under three log labels, not three independent failures. D16 and
  endpoint ownership ended clean and reverse rearm succeeded. This short run
  validates the observer only; it is not formal M0. Follow-up commit `2c8030a`
  now reports one canonical terminal relay plus three diagnostic log matches;
  it changes summary semantics only and keeps REVIEW visible. Next take a
  fresh direct baseline on the dedicated Shenzhen Mac, run formal M0, then
  independent rearm. M0 acceptance and M1 remain blocked.

- **Previous accepted position:** exact source `b0fcb76` M0 bundle
  `/tmp/mini_vpn_knife15_macos_20260715_072254.tar.gz` (SHA-256
  `0127e3af...`) correctly failed cycle 1 forward after five complete Target
  receiver zero-byte seconds plus one short tail. The command ran the full
  `300s`; receiver delivery was `208,142,336B / 5.547 Mbit/s` from a
  `14.425 Mbit/s` offer. Baseline/provenance and user operation were correct.
  Delayed sudo input before the later stop can only delay cleanup, not cause an
  earlier completed receiver interval to become zero.
- Internal regression discriminators remained negative: endpoint conservation
  stayed `<=61,440B` and ended `61,414/0/0B`; utun errors, pump waits/errors,
  endpoint blocking/delay, D16 ownership, terminal reap, reconnect, resource
  growth, and log compaction were absent. The bulk QUIC connection instead
  showed loss/congestion growth, cwnd contraction, and a `10.05s` writer wait.
- Exit/gateway ping controls were complete and broadly stable in the failure
  window, but the physical-counter portion was invalid. BSD physical
  `netstat` included a Link Address absent from the utun test fixture, shifting
  all counters while retaining 27 columns. The old summary's
  `network_control_evidence: PASS`, physical errors, and zero rates were false.
- Commit `524139b` parses Link rows with or without Address and requires all
  physical MTU/counter fields to be numeric before sample/error/byte/rate or
  freshness acceptance. Three RED/GREEN cycles plus a review P2 freshness
  negative test pass. Root `635+3 ignored`, main `2`, release, Knife15 and
  Knife14 shell gates, syntax, fmt/diff, and review pass; no P0/P1 or frozen
  constant change remains.
- Rearm bundle `/tmp/mini_vpn_knife15_macos_20260715_074823.tar.gz` (SHA-256
  `6d271b0c...`) passed fresh create, TCP/DNS smoke, zero ownership, route/utun
  cleanup, and process exit. Its physical rows share the old observer defect.
  M0 remains failed and blocks M1. The later exact-source short validation
  above closes the physical-observer prerequisite. Result:
  `docs/tech/2026-07-15-knife15-macos-m0-physical-counter-observer-repair-results.md`.

- **Previous accepted position:** exact source `b5c3963` M0 bundle
  `/tmp/mini_vpn_knife15_macos_20260715_032723.tar.gz` (SHA-256
  `865aa440...`) completed two mixed cycles, then cycle 3 forward produced four
  genuine Target receiver zero-byte seconds while still transferring exact
  `230,031,360B` over 300 seconds. The direction-aware receiver SLI correctly
  failed; this was not user operation or the earlier sender-evidence defect.
- Internal regression discriminators are negative: endpoint conservation
  stayed `<=61,440B` and ended `61,414/0/0B`; TUN/pump errors, lifecycle
  timeouts, terminal reap, stranded D16 ownership, FD/thread growth, and log
  compaction were zero. The failure window instead had QUIC cwnd contraction
  down to `25,174B`, about `+550` lost packets, `+772,134B` lost bytes, and
  `+237` congestion events before recovery.
- Exact external-path versus tunnel-only attribution is not proven because the
  old `network.csv` recorded only route names, despite the plan requiring
  same-window direct/control RTT, loss, and throughput. The independent rearm
  bundle `/tmp/mini_vpn_knife15_macos_20260715_051746.tar.gz` (SHA-256
  `823926e6...`) passed fresh utun, TCP/DNS smoke, zero ownership, and cleanup.
- The local runner now records Exit/gateway RTT/loss and physical-interface
  counters/rates in a 27-column `network.csv`, preserves raw `network.log`,
  verifies control completeness before M0, watches freshness and watchdog
  identity during M0, and requires one valid network row per process sample at
  final PASS. Root `635+2`, release build, Knife15/Knife14 shell gates, syntax,
  diff, and review pass with no unresolved P0/P1. No frozen constant changed.
- M0 remains failed and blocks M1. Next action is a fresh physical-route
  baseline and formal M0 on the dedicated Shenzhen Mac, followed by its own
  rearm bundle. Correlate any receiver-zero seconds with Exit/gateway controls,
  physical counters, and QUIC deltas; do not tune constants or relax the SLI.
  Result:
  `docs/tech/2026-07-15-knife15-macos-m0-network-control-discriminator-results.md`.

- **Previous accepted position:** fresh M0 bundle
  `/tmp/mini_vpn_knife15_macos_20260715_013158.tar.gz` (SHA-256 `5c04031e...`)
  completed its full first `300s` forward phase at `14.420 Mbit/s` Target
  receiver, crossed the rejected 90-second boundary, and closed both M0 D16
  relays via `clean_queue_lifecycle`. Endpoint conservation stayed
  `<=61,440B`; utun/pump/TUN failures were zero. This validates the relay
  lifecycle repair.
- The runner stopped because one client **sender** interval at `213-214s` was
  zero. The Target **receiver** still carried `640 KiB / 5.24 Mbit/s` in that
  second and had zero zero-rate rows across `301` interval/tail rows. WAN QUIC
  loss/cwnd contraction caused about `1.548s` sender backpressure; treating
  sender cadence as receiver quality was the evidence-contract defect. It was
  not user operation, endpoint pacing, or a new relay failure.
- The repaired runner requests `--get-server-output`, requires structured
  evidence from both endpoints, validates forward server receivers and reverse
  local receivers, aborts on receiver zero, and records sender zeros as final
  `REVIEW` diagnostics without ending the two-hour collection. Readiness now
  proves this JSON capability before route/TUN mutation. `.77` uses the tested
  reversible `iperf3 -s --json --forceflush` drop-in; `.27 -> .77` capability
  probe PASS. Repair commit: `4f836b9`; root tests `635/635` plus main `2/2`,
  both script self-tests, fmt/diff/syntax, release build, and review PASS.
- Current HK Target route was `utun1024` during post-run review. Before the
  next fresh baseline/M0, exit that other VPN/proxy and require physical/non-
  `utun` Target and Exit routes. No frozen data-plane or workload constant
  changed. Result:
  `docs/tech/2026-07-15-knife15-macos-m0-receiver-evidence-results.md`.

- **Previous accepted position:** the first formal HK M0 did not fail because of
  user credentials/routes or endpoint pacing. The iperf control relay was a
  healthy full-duplex Established flow with only `4B` downlink and `186B`
  uplink; the old D16 payload-idle timer killed it at `90,001ms`, which made
  the target close the data flow and the client report Broken pipe. The target
  journal matched this causal time. Conservation stayed `<=61,440B`, with no
  TUN/pump error discriminator.
- ADR-0014 now makes full-open Established lifetime socket/transport-owned.
  A shared `RelayCloseTimer` arms `90s` only for a concrete incomplete remote
  write, resets on actual partial-write or remote-read progress, disarms after
  completed flush, and retains the `10s` half-close drain plus D16 queued/
  leased-byte protection. D16 child-task failure is explicitly terminal.
- Runner evidence is now immutable after the first valid archive/checksum.
  Repeated stop prints the existing pair; snapshot/event/overwrite are
  refused. An interrupted archive-first publication may add its missing
  checksum without rewriting the archive. `start` runs a positive direct
  one-second Target transaction before any local state/TUN/route mutation and
  records it in the manifest, preventing the prior too-early rearm.
- Local focused tests, shell syntax/internal/external self-tests, formatting,
  diff checks, release build, and the all-target root suite pass (`635`
  library tests passed, `3` ignored; main binary `2/2`). No frozen data-plane
  or M0 load constant changed. Code review has no unresolved P0/P1. Relay
  lifecycle commit `a4e4549` and runner/rearm commit `89cf1e9` are complete;
  only the fresh user-run M0 and its clean rearm remain for acceptance.
- Result:
  `docs/tech/2026-07-14-knife15-macos-m0-first-run-failure-and-repair-results.md`.

- The formal M0 controller is locally PASS. A fresh same-target baseline
  derives `50%` sustained TCP/UDP and `80%` short TCP burst rates; UDP remains
  `1160B`. The frozen timeline is `6,780s` active + `300s` idle + `120s`
  final drain, with eight complete mixed cycles, 48 short flows, and eight DNS
  checks.
- Public `m0` refuses shortened profiles, wrong baseline provenance, zero
  intervals, missing byte/loss evidence, non-fake DNS, process/route health
  failure, and concurrent/stale run evidence. It tracks traffic/idle/drain
  children, fails closed if log history is compacted, stops work before
  cleanup, and leaves a failed TUN alive for evidence.
- Summary output includes M0 result/DNS/timeline integrity, RSS/FD/thread
  envelopes, utun deltas, endpoint conservation maximum, and final live/
  outstanding ownership. Local shell TDD passed; no new real TUN or two-hour
  run occurred. Next action is a user-executed M0 followed by a fresh create/
  smoke/stop rearm bundle. Result:
  `docs/tech/2026-07-14-knife15-macos-m0-controller-local-gate-results.md`.

- The first HK user-executed target-only qualification passed on exact source
  `2a85fd4`. Direct receiver rates were `9.045/26.790 Mbit/s` and TUN receiver
  rates `31.444/48.490 Mbit/s` forward/reverse, all `20/20` nonzero.
- Endpoint conservation was exact at or below `61,440B`; pump high was
  `317/500` with zero waits/errors; macOS interface errors were `0/0`; DNS,
  PID/utun/route cleanup, secret scan, and bundle checksum passed.
- One forward iperf-tail `Stopped(0)` is `REVIEW`, not a leak: D16
  queue/lease/reservation closed at zero and the slot rearmed for reverse. M0
  must preserve an idle-drain epoch and sender/receiver byte-gap evidence.
- Real-bundle TDD repaired blank macOS-awk summary counts and remote-write
  classification. High-rate per-flush permit logs moved behind full TRACE;
  aggregate/exception/lifecycle evidence remains. No data-plane constant
  changed.
- Short qualification passes, but 2-hour M0 has not run. HK may run target-
  only M0 through the user-controlled HITL script; Shenzhen remains preferred
  for M1/M2/recovery. Add the mixed-workload and idle-drain controller before
  M0. Result:
  `docs/tech/2026-07-14-knife15-macos-hitl-short-qualification-results.md`.

- Knife14 is complete. Knife15 will prove hours-long bounded operation,
  recovery, and operational evidence rather than reopen peak-throughput
  tuning.
- Preserve two distinct lanes: the capable Linux/VPS topology owns H10d16
  architecture and peak-throughput regression; macOS owns real-client utun,
  resource stability, mixed TCP/UDP/DNS traffic, idle/resume, and network-
  lifecycle evidence. HK may run the first user-controlled target-only
  qualification; Shenzhen remains the preferred long-duration host.
- HK/Shenzhen-to-US bandwidth below `200 Mbit/s` is expected and is not an
  architecture failure. Establish direct/control bandwidth `B`, use roughly
  `40-60% of B` sustained and `75-85% of B` bursts, and judge internal
  invariants, resource trends, recovery, and relative path behavior.
- The H10d16-aware target-only HITL runner and its shell TDD fixture are now
  implemented at `scripts/knife15-macos-soak.sh` and
  `scripts/knife15-macos-soak-self-test.sh`. Local syntax/self-tests pass. It
  proves exact source/binary provenance, refuses Exit-route recursion, owns
  only explicit host routes, has signal/watchdog cleanup, bounds logs, gathers
  process/utun/network/event evidence, and scans bundles for secrets.
- Planned gates are `2h -> 8h -> 24h`, followed by independent Wi-Fi,
  sleep/wake, path-change, client-restart, and authorized Exit-restart recovery
  windows. A shorter failure blocks the longer run until diagnosed.
- The HK development Mac is authorized only through the reviewed HITL script:
  the user must explicitly execute every `sudo` command. Agent-started TUN and
  ad-hoc route mutation remain prohibited. A read-only preflight found the
  current Exit/target path on `utun1024`; the user must exit that VPN/proxy
  before qualification. No real macOS TUN or soak has run yet.
- Keep H10d16, EndpointWindowV1, MTU1200, `1160B` UDP shape, pool, QUIC
  windows, chunk, Cubic, GSO default, queue/FIFO/batch bounds, driver bound,
  and self-wake frozen. Do not reopen bounded sender, cap64, or GSO-only.
- Plan:
  `docs/tech/2026-07-14-knife15-long-duration-release-readiness-plan.md`.
- User runbook:
  `docs/tech/2026-07-14-knife15-macos-hitl-m0-runbook.md`.

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
