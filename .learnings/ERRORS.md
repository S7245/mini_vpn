# Errors

## 2026-07-22 - Pending-only recovery caused a false-rebind feedback loop

- Exact source `e479013` passed fresh baseline/direct, then M1 cycle 1 forward
  had three complete receiver-zero seconds while the Endpoint performed seven
  successful `tcp_write_stall` rebinds. Generations 3--7 repeatedly changed
  source port during the blocked business flow.
- The trigger treated any two-second Pending episode as path failure. Normal
  per-stream ACK progress was not observed, and connection-level recovery
  repeatedly declared success while the owning business writer remained
  Pending. Rebind churn amplified congestion instead of recovering it.
- Correct behavior: require both continuous Pending and no ACK advancement on
  the exact send stream for the unchanged bound. Do not repair this class by
  increasing thresholds or tuning frozen windows/pacing values.

## 2026-07-22 - Gate invocations must preserve dependency and test provenance

- Cargo accepts one positional test filter; attempts to pass multiple test
  names were command errors, not test failures. Use one common substring or
  separate invocations.
- Standalone vendored Quinn again selected registry quinn-proto until the
  absolute local `patch.crates-io.quinn-proto.path` was supplied. An `--exact`
  filter without the module-qualified test name also executed zero tests.
- Correct behavior: use the documented absolute patch and verify a nonzero
  executed-test count before accepting a standalone result. Remove the
  generated ignored vendored `Cargo.lock` after the gate.

## 2026-07-22 - ACK traffic hid a saturated business-stream send window

- The exact HK M1 reached cycle 2 forward before a complete Target receiver
  second went empty. Its sender paused for most of 18 seconds and the matching
  D16 writer waited `11.280s`, while QUIC loss/congestion rose sharply.
- Endpoint-wide receive counters still advanced through ACK/control traffic,
  so the accepted TX-without-RX detector remained disarmed and never requested
  socket recovery. Shared transport liveness was incorrectly treated as proof
  of business-stream write progress.
- The repair observes continuous per-connection writer `Pending` ownership as
  a separate bounded episode and reuses the existing socket rebind/recovery
  path. Reusable rule: aggregate liveness cannot discharge a narrower blocked
  ownership invariant.

## 2026-07-21 - Endpoint routing was initially mistaken for authenticated recovery

- The first multi-connection repair removed a pending handle as soon as the
  Endpoint routed a datagram to that connection. Quinn routing precedes packet-
  protection authentication, so corrupt or spoofed traffic could falsely
  release the previous socket.
- Code review moved recovery proof behind connection event handling and a real
  authenticated-packet-count advance. Each connection reports at most once per
  generation; stale and old-socket events cannot advance it.
- A focused negative test and the full Quinn/root/capacity gates pass after the
  repair. Reusable rule: identity routing selects the consumer, but only
  protocol authentication proves path recovery.

## 2026-07-21 - Standalone Quinn tests require the explicit local proto patch

- Directly invoking the vendored Quinn manifest selected registry
  `quinn-proto` and failed on missing Endpoint pacing and authenticated-packet
  APIs. This was dependency-provenance failure, not a product regression.
- The correct gate passes an absolute
  `patch.crates-io.quinn-proto.path`, verifies `cargo tree`, and requires a
  nonzero executed test count. This rule already existed in project memory and
  must be applied before interpreting standalone compiler output.

## 2026-07-21 - One pooled connection cannot complete Endpoint-wide rebind

- The six-hour HK M1 had simultaneous conn0/conn1 receive gaps. The Endpoint
  rebound, then the old lifecycle released its previous socket on the first
  current-socket connection packet while the other connection remained
  stalled for eight complete receiver seconds.
- Treating Endpoint-level first RX as full recovery produced a false-positive
  recovery event and selected the wrong socket lifetime. The repair snapshots
  every live handle and waits for per-connection authenticated recovery or
  drain without changing any threshold or workload value.

## 2026-07-20 - A direct fixture changed only one endpoint's duration

- The new direct-discriminator tail regression initially changed the iperf
  client duration to `300s` but left the embedded server duration at the
  compressed `2s` M1 fixture value. The correct classifier rejected the row
  because it was not near the server command boundary.
- This was a test-fixture construction error, not a repair or product failure.
  Work stopped at the unexpected RED; the confirmed repair aligned client and
  server durations, after which the intended artifact passed.
- Reusable rule: when direction-aware evidence embeds both iperf endpoints,
  fixture time scaling must update the authoritative sender and receiver
  command contracts together.

## 2026-07-17 - Resolve documented gate names before final execution

- The initial M1 implementation plan named
  `scripts/knife14-h10d16-gate.sh`, which does not exist. The actual preserved
  control gate is `scripts/knife14h10d16-singbox-control.sh --self-test`.
- The nonexistent command failed once during closeout; the plan was corrected
  and the real gate passed. This was a documentation/command-resolution error,
  not a product or regression failure.
- Reusable rule: resolve every plan command against `rg --files` before the
  final gate batch. Do not infer historical script names from stage prose.

## 2026-07-17 - A negative fixture accidentally changed two invariants

- The first sender-zero mutation was direction-agnostic and stored a backup in
  the formal M1 result directory. Depending on the selected result, it could
  modify the receiver side and always changed evidence multiplicity.
- The fixture was repaired to choose the direction-aware sender and keep its
  backup outside the counted directory. The intended sender-only REVIEW then
  passed without weakening receiver or result-count gates.
- Reusable rule: preserve file multiplicity and traffic direction when testing
  result classification; one mutation should select one discriminator.

## 2026-07-17 - Do not promote generic remote-write grep to an M0 stop gate

- A manual pre-M0 instruction incorrectly treated any
  `remote_write_failed` line as an internal smoke failure. Two HK bundles
  instead reproduced the already accepted iperf forward close tail: one
  canonical failure, Quinn `Stopped(0)`, zero D16 queued/leased/reserved
  ownership, a live Endpoint, and successful subsequent reverse/DNS smoke.
- `internal_failure_scan: REVIEW` is intentionally fail-visible and requires
  phase-aware review; it is not equivalent to an automatic stop. Count
  canonical relay-close events, inspect the peer stop code and ownership, and
  apply the receiver SLI to formal M0 artifacts. Do not block M0 solely with a
  broad grep over raw/derived log labels.
- Another VPN may be enabled only after Knife15 `stop` finishes. Enabling it
  before snapshot/stop changes the final physical-route sample to `utun*` and
  contaminates cleanup/network evidence even when smoke preceded that change.

## 2026-07-16 - Post-rebind acceptance was blocked by the physical lane

- The first Shenzhen baseline established iperf control/data sockets but sent
  no data interval. The second completed both directions but recorded two
  forward and four reverse receiver-zero intervals before mini_vpn or TUN ran.
  A third completed with continuous reverse delivery but repeated a complete
  forward zero second, 31 retransmits, and cwnd down to `1,388B`.
- Target iperf remained active with zero restarts. The accepted `1KiB` reverse
  observer, repeated retransmission, and one-to-two-segment cwnd reject service
  restart, operation, minimum speed, and coarse observation as causes.
- Three immediate attempts exhaust the same-window discriminator. Do not loop
  direct/start/M0 after a failed baseline and do not export the
  failed directory. Move to another network window or physical Mac. Any
  degraded-path soak must be an explicitly separate non-acceptance lane.

## 2026-07-16 - M0 outlived one healthy UDP endpoint identity

- The user correctly ran smoke and one full M0 cycle. Cycle 2 forward then
  failed by itself after about `65s`; it was not an early interruption, late
  sudo password, missing client, or low Shenzhen speed.
- Both QUIC pool connections shared source port `64195`, timed out together,
  and failed reconnect on the unchanged Endpoint. Physical/direct continuity,
  TUN, pacing conservation, resources, sing-box, and iperf were healthy.
- Do not repeat the unchanged M0 or tune frozen constants. A shared socket
  identity that stops serving every child requires bounded endpoint socket
  replacement and explicit current-socket recovery evidence.

## 2026-07-16 - A loopback server lifetime looked like rebind failure

- The first live two-connection rebind test let the server task drop its Quinn
  Endpoint immediately after calling `SendStream::finish`. The client could
  lose the response before observing it, even though live rebind was correct.
- Waiting for `SendStream::stopped` keeps the server Endpoint alive until the
  peer acknowledges the response. The repaired integration then proves both
  established connections survive the source-port change.
- Reusable rule: a transport integration fixture must own its Endpoint until
  the exact peer-visible completion boundary under test, not merely until the
  local send API accepts `finish`.

## 2026-07-16 - An invented strict Clippy gate obscured the accepted gate

- A final command added `-D warnings` to the repository's current Clippy gate.
  It promoted 17 established lints to errors and made the stage look broken,
  although tests, check, and release were green.
- The command also exposed one new tuple-complexity warning in the rebind
  helper. That new warning was repaired with a named socket result structure;
  the unchanged project Clippy command then passed with only established
  warnings.
- Reusable rule: do not silently strengthen a stage gate while reporting its
  result as the accepted gate. Repair new warnings in changed code, but track
  repository-wide lint ratcheting as a separate authorized cleanup.

## 2026-07-16 - A later relay error erased the queued local TCP close

- The clean Shenzhen rearm wrote `37B` of iperf control, then D16 reported
  queue closure followed by `remote_read_failed`. The first event called
  `socket.close()`, but the second immediately called rearm/abort while the
  socket was still `FinWait1`.
- The local client received neither FIN nor RST and the old smoke command had
  no hard deadline, so it appeared frozen until manual interruption. Route,
  sudo timing, public IP, Shenzhen speed, and user operation were not causes.
- Future close handlers must first check for an existing same-epoch deferred
  close. Coalesce and refine its cause; do not independently abort the socket
  before the local protocol signal drains. Every external smoke command must
  also have its own evidence-preserving timeout.

## 2026-07-16 - macOS Bash 3.2 does not provide BASHPID

- The first portable timeout-helper self-test failed under the system Bash
  with `BASHPID: unbound variable`. `BASHPID` is not available in macOS Bash
  3.2 and `set -u` turned the portability mistake into a hard failure.
- The helper only needs to prove that the owning shell still exists, so `$$`
  is sufficient and is stable in Bash subshells. The repaired self-test kills
  a 30-second sleep at one second and requires status `124`.
- Reusable rule: new macOS runbook helpers must be exercised with the actual
  `/bin/bash` version and must not rely on post-3.2 variables or syntax.

## 2026-07-16 - M0 lost every QUIC connection on one shared endpoint

- The accepted baseline and 300-second physical TCP direct control passed,
  but M0 cycle 1 forward lost both pool connections within its opening window.
  The client then spent most of 300 seconds at zero and failed its final
  control message with `Broken pipe`.
- Reconnects on the same endpoint repeatedly timed out even though sing-box,
  iperf, local resources, endpoint conservation, and ICMP controls remained
  available. ICMP and direct TCP do not prove UDP five-tuple continuity.
- Do not retry M0 or tune pacing, MTU, pool, windows, chunking, Cubic, or GSO
  from this evidence. Capture UDP ingress/egress at the Exit and test a fresh
  endpoint/source port to distinguish a path mapping blackhole from client
  receive/service failure.

## 2026-07-16 - A zsh field-splitting probe printed false PASS labels

- A one-off artifact validator used `set -- $spec` under zsh. Default zsh did
  not split the scalar as expected, so `jq --argjson` received an empty value.
  The command group also lacked fail-fast handling and printed PASS labels
  after jq had failed.
- The labels were rejected immediately and the exact validation was rerun with
  explicit file/reverse arguments under `set -euo pipefail`; both files then
  passed legitimately.
- Reusable rule: never derive an acceptance label from an unchecked prior
  command. Use explicit function arguments for zsh probes and fail fast before
  printing PASS.

## 2026-07-15 - A copied remote artifact was joined to the local host state

- The Shenzhen baseline directory was copied into the current Mac's `/tmp`.
  Inspection correctly read its iperf JSON, but a live route/process probe ran
  on the current Mac and was initially attributed to Shenzhen. The resulting
  Clash/`utun1024` conclusion was invalid and was withdrawn after user
  correction.
- Filesystem location is not host provenance. Never join a copied artifact
  with live route, process, clock, or interface state unless an artifact field
  binds that observation to the originating host and time.
- The portable evidence was the JSON itself: reverse rate, interval bytes,
  direction, Target, and iperf block size. The accepted Shenzhen route evidence
  is the user's physical-`en0` statement; current-HK state is excluded.

## 2026-07-15 - Oversized iperf observers fabricated low-rate zero seconds

- Reverse averaged `0.524183 Mbit/s`, but 13/20 receiver intervals were zero
  and every positive interval was exactly `131,072B` or a multiple. The old
  strict validator correctly rejected the artifact but could not distinguish
  observer quantization from a real stall.
- At M0's 50% rate, a default iperf block would take about four seconds. Do not
  relax receiver continuity or blame the network from that evidence. Reduce
  the reverse-only iperf observation length, preserve forward shape, and use a
  fresh run as the discriminator.
- A first reduction to 16KiB was still equal to the next run's entire useful
  bytes per second and exceeded its `8,328B` cwnd. Five zeros remained, with
  every positive receiver interval still a 16KiB multiple. Do not declare a
  fixed observer sufficient from one prior rate sample; replay its capacity
  math against the next evidence.
- The accepted follow-up is 1KiB, about eight observer blocks/s at the latest
  half-rate. If zeros remain at that resolution, preserve them as physical
  continuity evidence while still refusing to call low speed a mini_vpn bug.

## 2026-07-15 - Global pool parity inverted a healthy control/data pairing

- The user correctly ran an M0 with a passing fresh direct gate. Sustained TCP
  and reverse UDP passed, but reverse UDP opened only one TCP control flow and
  shifted the global cursor. The next short flow placed control on conn1 and
  data on conn0; Target then saw two initial receiver-zero seconds.
- The connection was alive and the exit opened the Target immediately. Timeout,
  stale-probe, pool-size, pacing, workload-rate, and operator changes would not
  repair a placement decision based on irrelevant history.
- The first local repair draft reserved active count before awaiting the slot
  mutex, but review found a same-slot overtaking path around idle-exclusive
  reconnect/clone. Per-slot preparation ownership and wakeup were required
  before the repair could pass review.
- A post-stop direct-short control was invalid because another local VPN/proxy
  auto-restored the Target route through `utun1024`. Recheck the actual route
  after every cleanup before classifying a command as physical direct evidence.

## 2026-07-15 - A zsh pattern error did not stop a guarded commit command

- The code commit's staged diff check passed, but a complex secret-scan regex
  inside an `if` triggered zsh `bad pattern`; despite `set -e`, the following
  commit still ran. A separate unambiguous post-commit scan found no secret.
- Do not combine shell-sensitive quote classes in a single inline regex gate.
  Use multiple `rg -e` expressions, verify their command status explicitly,
  and keep the exact staged file set small enough to audit independently.

## 2026-07-15 - The baseline claimed continuity from the wrong duration and endpoint

- Formal M0 failed on three complete Target receiver zero seconds during a
  300-second forward phase, but the prerequisite baseline covered only 20
  seconds and validated client-root forward intervals. It could set the offer
  rate but could not prove the physical path met the same receiver SLI.
- A manifest field saying `duration_secs=300` is not proof that a result ran
  for 300 seconds. Code review found that an early positive result could pass
  unless requested duration, receiver elapsed time, and 300 complete interval
  records were checked in the JSON itself.
- The repair requires structured direction-aware baseline evidence and a
  separate non-TUN direct gate whose result proves 300 requested seconds,
  299-310 receiver seconds, and at least 300 complete positive Target receiver
  intervals. Short, stale, changed, or mismatched evidence fails closed.
- Future long-run prerequisites must distinguish capacity sampling from
  continuity qualification and validate every acceptance claim from the
  artifact, not only from intended command arguments or manifest metadata.

## 2026-07-15 - A physical link Address shifted every network counter

- The network-v2 parser was tested only with a utun-shaped `<Link#>` row whose
  Address column was empty. A physical `en*` row included a link Address, so
  the fixed `$4..$10` extraction shifted all counters and omitted collisions.
- The 27-column output still passed schema width, sample correspondence, and
  freshness. It falsely reported every packet sample as an interface error,
  byte/rate deltas as zero, and `network_control_evidence: PASS`.
- The repair detects the optional Address field, requires numeric MTU and all
  seven counters, and refuses errors, bytes, rates, freshness, and final PASS
  when physical semantics are invalid. A real physical-row fixture and a
  deliberately shifted 27-column fixture lock both failure modes.
- Future platform observers must include real physical and virtual interface
  row shapes in their fixtures before a long run. A syntactically complete
  evidence row is not necessarily a semantically valid control.

## 2026-07-15 - A regression gate used stale invented script names

- After Rust and release gates passed, a fail-fast command stopped with exit
  `127` because it invoked nonexistent `knife14-*-self-test.sh` names instead
  of the repository's actual `knife14b-*.sh --self-test` interfaces.
- The failure was command selection, not a product regression. `rg --files`
  and each script's self-test dispatch identified the correct commands; the
  low-RTT, US-client-suite, and sing-box-control gates then passed.
- Future handoff text that names a gate family is not a shell command source.
  Discover the checked-in executable path first, then use fail-fast execution.

## 2026-07-15 - The M0 network collector recorded routes but not controls

- The accepted readiness plan required same-window direct/control RTT, loss,
  and throughput, but the implemented `network.csv` contained only
  `target_if` and `exit_if`. The omission was not caught by the earlier shell
  self-test because it asserted route parsing rather than the evidence schema.
- The next M0 then produced real receiver interruptions alongside QUIC
  congestion. Clean TUN, endpoint, lifecycle, and resource evidence rejected
  an internal regression, but the missing control prevented exact
  external-path versus tunnel-only attribution.
- The repair locks BSD ping parsing, total-loss semantics, gateway and physical
  interface parsing, composed 27-column rows, raw logs, counter-derived rates,
  freshness, watchdog identity, per-process correspondence, and fail-closed
  partial-summary behavior.
- Future formal evidence plans must have a schema-level self-test for every
  promised evidence family. Do not equate route stability with path health,
  and do not spend a long soak when its independent control is already missing.

## 2026-07-15 - M0 validated forward sender cadence as receiver quality

- A full 300-second forward phase delivered `541,065,216B` to the Target with
  no receiver-zero interval, but one client sender interval was zero during a
  WAN congestion/backpressure episode. The generic no-zero validator inspected
  the client root and incorrectly emitted `invalid_iperf_result`.
- Correct behavior is direction-aware: request structured server output,
  validate forward at the server receiver and reverse at the local receiver,
  and keep sender zeros as separate review evidence. Missing either endpoint
  is a capability/schema failure.
- BSD `mktemp` requires the replacement `XXXXXX` at the end of the template.
  The readiness path's `.XXXXXX.json` form created one literal fixed path;
  ending the template at `XXXXXX` restores unique, concurrency-safe files.
- The first summary's three remote-write matches described one earlier smoke
  close-tail event in three log forms. Always correlate epoch and phase before
  treating an aggregate grep count as the active failure cause.

## 2026-07-14 - A non-fail-fast commit command crossed a failed diff check

- `git diff --cached --check` correctly found two trailing spaces in a newly
  staged ADR, but the following `git commit` still ran because the multi-line
  shell command did not enable fail-fast behavior.
- The defect was documentation-only and did not change the tested Rust code,
  but the commit must not be amended or hidden. Remove the whitespace in a
  follow-up and use `set -e` for any command group where a failed pre-commit
  gate must prevent the commit.
- Untracked files are absent from an ordinary `git diff --check`; stage the
  exact intended files first and make `git diff --cached --check` a hard gate.

## 2026-07-14 - A payload-idle timer killed M0 control and polluted rearm

- At `90,001ms`, D16 closed an active Established iperf control relay with
  `reason=idle_timeout`; the target then logged an unexpected client close,
  ended the data flow, and the client reported Broken pipe. The control relay
  was legitimately quiet, so raising the timeout or adding keepalive traffic
  would only hide the invalid lifecycle inference.
- The first archive's reported SHA later changed because repeated `stop` and
  `snapshot` commands were allowed to mutate and rebuild an already-published
  evidence path. Treat the valid archive/checksum pair as immutable; never
  ask users to snapshot after stop.
- The immediate rearm smoke failed while the target iperf server still owned
  the broken prior session. Port/routing preflight was insufficient. Require a
  complete positive direct transaction before new route/TUN mutation.
- The user's intended baseline/start/smoke/m0 operation was not the root
  error. Script wording and missing guards permitted the evidence/rearm
  mistakes, so the repair belongs in code and runbook rather than operator
  blame.

## 2026-07-14 - M0 review found false-complete and cleanup gaps before real TUN

- The first local controller draft could retain `m0_status: complete` after an
  expected iperf JSON or DNS artifact disappeared, because the summary counted
  events and files independently. It now requires exact phase/file and
  DNS/file correspondence plus zero invalid files.
- Idle and final drain initially called `sleep` outside the tracked-child seam.
  A signal or `stop` could therefore terminate the controller without proving
  the pause child ended. All scheduled children now use the same monitored,
  TERM-then-KILL lifecycle.
- Log compaction initially preserved disk safety while silently deleting early
  conservation evidence. It now fails the running workload and forces summary
  review. A live workload PID identity mismatch also blocks user-requested
  process/route cleanup instead of signaling an unrelated PID or claiming a
  clean stop.
- These failures were caught by shell RED tests and code review before a real
  two-hour TUN run; they do not authorize any H10d16 constant change.

## 2026-07-14 - The first macOS HITL summary hid evidence and overproduced logs

- BSD `awk` rejected `NR>0?NR-1:0`, leaving process and event counts blank even
  though their CSV/TSV files were intact. Use the portable spaced/parenthesized
  form and lock it with the runner's macOS self-test.
- The summary emitted `NO_KNOWN_INTERNAL_FAILURE_SIGNAL` despite a real
  `remote_write_failed` close-tail event. Count both error/lifecycle forms,
  promote any match or nonzero TUN interface error to `REVIEW`, and keep the
  summary explicitly non-authoritative.
- Per-flush permit-release diagnostics produced `8,909` lines in a short run
  and would force repeated compaction during M0. Preserve their aggregate byte
  accounting while gating individual batches behind full TRACE; keep bounded
  log and free-disk fail-closed checks for the remaining evidence.
- These were observer failures, not permission to alter H10d16, pacing, MTU,
  pool, window, chunk, Cubic, GSO, queue, or self-wake parameters.

## 2026-07-14 - Final VPS regression exposed topology, packet-shape, and runner traps

- A reverse P8 against `.33` was initially treated as a client architecture
  failure even though that external sing-box/quic-go sender is independently
  known to remain in the low single-digit Mbit/s class on this topology. The
  capable Shoes/Quinn `.111:8443` rerun reached `188 Mbit/s` receiver with all
  formal client invariants clean. Require a direction-capable peer before
  interpreting a throughput failure.
- The first UDP run used `-l 1200` with TUN MTU1200. IPv4/UDP headers made the
  actual IP packet `1228B`, caused fragmentation, and produced a misleading
  `55%` forward loss result. Test payload must satisfy
  `payload + IP/transport headers <= TUN MTU`; the corrected `1160B` run is
  the only accepted UDP result.
- A standalone vendored Quinn test again omitted the explicit local
  quinn-proto patch and failed on missing EndpointPacing APIs. The root build
  and quinn-proto tests were already green; the same Quinn manifest passed
  `29+1` once given the absolute patch path. A compiler failure against the
  wrong dependency graph is provenance failure, not product regression.
- `.27` reports NOPASSWD command rules, but the suite's bare `sudo -v`
  preflight can still select a password-requiring rule. `sudo -n true` proved
  noninteractive command authority; running the unchanged suite through
  `sudo -n -E` avoided putting a password in commands, logs, or artifacts.
- A long multiline paste into the remote PTY lost commands after the first
  line. Send stateful credential/profile exports one line at a time and verify
  only redacted lengths before starting a suite.
- The first transient Shoes copy was partial and failed its SHA/status check;
  an exact re-copy fixed it. A root-owned FIFO glob also required the glob to
  expand inside `sudo sh -c`, and its feeder writers had to be terminated after
  the service loaded. Hash the final archive and binary before traffic, and
  keep a fail-closed restoration timer until manual cleanup is verified.
- Cleanup succeeded: the temporary service/timer/binary/runtime were removed,
  UDP8443 closed, and all four `.111` socket-buffer values returned to
  `212992`.

## 2026-07-13 - Reverse P8 exposed lost recovery evidence, not missing capacity

- Source `55792b3` opened eight data flows across both healthy pool
  connections but timed out at `0.103 Mbit/s`; each flow delivered only
  `117,412-134,760B`. The client nevertheless received about `114 MiB` of QUIC
  wire data, and peer flow-control blocking appeared only after application
  stream consumption stopped.
- The final `DrainOnly=8`, `Recovery=1`, zero current pending, later
  `send_queue=0`, clean `6/6` backlog pause/resume, zero TUN drops, pump
  `177/500`, and zero QUIC loss reject capacity tuning, reader wakers, pool,
  and path loss as the next repair.
- The exact bug was clearing `d16_tun_rx_ack_barrier` on a zero snapshot before
  checking whether hard pressure/debt would suppress Recovery. Preserve the
  barrier through the suppressor and consume it on the first eligible clean
  zero snapshot; do not weaken DrainOnly or count arbitrary zero-byte cycles as
  progress.
- Strict Rust `1.95.0` all-target Clippy also failed on `19` pre-existing lint
  sites outside this repair. The all-target rerun allowing only the five known
  baseline classes passed. Do not turn a throughput repair into an unrelated
  whole-codebase lint migration; keep the baseline failure explicit until a
  separate cleanup stage owns it.

## 2026-07-13 - Secret-free VPS source exports need explicit provenance and portable inspection

- The isolated source export intentionally omitted `.git`, so the runner
  reported `source_commit: unknown` even though the archive came from the exact
  `e2014034` Git object. Record the source object in the deployment manifest and
  prove it with local/remote archive, binary, runner, and critical-file hashes;
  do not depend on runtime Git metadata in a secret-free export.
- One remote checksum command failed because an `awk` field reference crossed
  nested shell quoting, and a later report query assumed `rg` was installed on
  `.27`. Use `sha256sum -c` for remote verification, probe tool availability,
  and retrieve sanitized evidence for local `rg` inspection when the host has
  only baseline POSIX tools.
- Neither operational miss changed the build, formal profile, or test window.
  The corrected source hashes matched before rehearsal/P1, and the final
  five-member evidence archive passed local path and secret scans.

## 2026-07-13 - Formal-profile and TCP-window mistakes can create false local safety

- The previous exact 32 MiB tracer configured H10d16 pacing and queues but
  left the SUT's smoltcp RX/TX storage at the `65,535B` test default. VPS used
  the frozen `1 MiB` buffers, so the local gate silently pre-limited the sender
  and missed the real admitted burst. Every architecture discriminator must
  assert all formal profile inputs inside the SUT before measuring capacity.
- The first receive-window implementation used an additive
  `min(physical_free, limit)` formula. With `100,000B` unconsumed, it still
  advertised `368,640B`; the right edge moved forward by `100,000B`. The
  correct formula is `min(capacity, limit) - queued`, with saturation at zero.
  Test advertisement and segment acceptability together after queueing data.
- `--all-features --offline` on the standalone vendored Quinn manifests tried
  to select uncached fuzz/extra runtime dependencies and failed before tests.
  Use the accepted default upstream suites, pass Quinn an explicit absolute
  local quinn-proto patch, and verify the executed counts and provenance.
- Rust 1.95 strict all-target Clippy also reports established repository-wide
  lints unrelated to this stage. Compare against the baseline and rerun with
  explicit allowances for only those known lint classes; do not expand a
  throughput repair into an unrelated whole-codebase rewrite.

## 2026-07-13 - The local ingress gate modeled average capacity but missed the VPS startup burst

- The exact local 32 MiB gate passed above `293 Mbit/s` with ring `38/500`,
  pump `56/500`, and zero waits/drops, but the frozen VPS P1 filled the pump,
  produced `347` full waits, and dropped `419` TUN packets while still reaching
  `194 Mbit/s` receiver.
- The old capacity proof used average packet service and a generator whose
  burst shape did not match the sub-millisecond VPS TCP startup. It therefore
  proved sustained service but not the required jitter envelope.
- Do not respond by enlarging FIFO, kernel queue, or 48/240 drain bounds. Add a
  deterministic startup-burst replay at the real actor seam, map each service
  gap to the frozen 500+500 packet capacity, and reject the next architecture
  locally if either bounded layer fills.
- The runner correctly stopped after the single P1. Cleanup restored the target
  route and removed the client; the five-member bundle passed secret scans.

## 2026-07-13 - Ingress implementation gates exposed lifecycle and test-provenance traps

- The existing multi-thread profiler test could run both 32-flow phases to
  their 10-second timeout and still compare fractions without checking
  completion. Exact `d934f12` reproduced the same 20-second behavior. Isolate
  profiler calibration to one completed flow, require completion before metric
  assertions, and keep concurrency in the dedicated 64/256/1024 sweeps.
- A terminal pump channel initially returned `BrokenPipe` forever while
  `run_event_loop` ignored TUN wait errors, creating a potential busy select
  loop. Terminal TUN failure must log once and end the event loop; test the
  consumer lifecycle, not only producer closure.
- A standalone vendored Quinn command selected registry `quinn-proto 0.11.16`
  and failed on missing endpoint APIs. Pass an explicit absolute
  `patch.crates-io.quinn-proto.path` and verify build provenance before treating
  compiler errors as regressions.
- One focused command again used `--exact` without the full module path and ran
  zero tests. A successful exit is invalid unless the summary reports the
  expected nonzero test count.
- The first VPS source copy was partial until `rsync --partial` completed and
  the SHA-256 matched. The first macOS tar also carried Apple xattr/`._`
  metadata; clear xattrs and use `COPYFILE_DISABLE=1` plus `--no-xattrs`, then
  inspect members before deployment.
- An initial UDP `8443` preflight read the wrong `ss` column, and a broad process
  match could include the checker itself. Filter the exact source port and
  require `comm == mini_vpn` for client process cleanup.

## 2026-07-13 - TUN batch tracer failures exposed reachability and fixture-fidelity gaps

- The first batch RED compiled only after replacing an invalid
  `BytesMut::from(Vec<u8>)` conversion with `Bytes::from(packet).into()`. A later
  Quinn tracer failed because its inherent `RecvStream::read` returns
  `Option<usize>`; the test must handle `Some(n)` and clean `None` EOF explicitly.
- One command supplied two Cargo test filters, and another omitted
  `--features harness`; both exited without exercising the intended tracer.
  Require the test summary to show the expected nonzero executed count, not only
  a successful exit status.
- The first full-path GREEN delivered exact bytes but reported zero batches and
  about 30,000 relay calls. The actor's direct-ready branch still used the
  one-packet adapter and bypassed `drain_ready_tun_rx`. Fix product-path
  reachability before accepting a helper-level GREEN.
- An early 10-second forward fixture sent only `28,297,696B` because it copied
  the reverse ACK-starvation fixture's two ingress packets per poll. Restore the
  normal generator behavior for a faithful forward-capacity test; do not change
  product constants to compensate for an artificial fixture throttle.

## 2026-07-13 - Prefetched TUN packets must not be overwritten by the next wait

- A two-packet RED showed that the bounded drain probe could leave packet one in
  `rx_buffer`, after which `wait_for_rx` overwrote it with packet two.
- Correct behavior is to return ready immediately when the caller-provided slot
  is already populated. Apply the contract to both the real virtual TUN device
  and deterministic harness device, then retain the exact-delivery tests.

## 2026-07-13 - Parallel localhost rate gates can manufacture throughput regressions

- `cargo test --features harness --lib` initially failed several rate thresholds
  even though the same tests passed alone and all exact/lifecycle assertions
  remained clean. The failures came from concurrent rate-heavy tests competing
  for the same local CPU and socket capacity.
- Use one test-only shared capacity guard for localhost Mbps gates and rerun the
  complete suite. Never weaken a product threshold or tune pacing/queue constants
  in response to host-resource contention created by the test runner.

## 2026-07-13 - Endpoint integration gates need crate-local commands and exact test selection

- Root `cargo test -p quinn --lib` and `cargo test -p quinn-proto` failed
  because mini_vpn is not a Cargo workspace containing those packages. Test
  quinn-proto from its vendored manifest and test Quinn from an isolated exact
  source copy with the local proto patch explicitly configured.
- The first isolated Quinn attempt silently selected registry
  `quinn-proto 0.11.15` from its lockfile, so the new API was missing. Require
  `cargo tree`/build output to show the intended local `0.11.16` path and pass
  `patch.crates-io.quinn-proto.path` explicitly.
- A focused command using `--exact` without the full module path ran zero
  tests. Use the listed fully qualified name or an unambiguous substring and
  require the summary to report at least one executed test.
- Reusable rule: package selection, dependency provenance, and executed test
  count are separate gates; exit code zero alone proves none of them.

## 2026-07-13 - Endpoint repair attempts exposed three avoidable local hazards

- Removing the test-only-looking `ReservationCharge::None` variant broke
  generic RAII matches in non-test code, and converting off-path path challenge
  handling to `Result<Option<_>, _>` initially left two stale `return None`
  branches. Make warning cleanups only after tracing every cfg and return type;
  rerun the entire vendored suite, not just the new module.
- A proposed control-vs-bulk liveness RED was invalid because a real socket
  outcome is allowed to wake the parked peer before the control waiter
  re-registers. Do not change scheduling to satisfy a false ordering
  assumption; state the permitted wake partial order, then test continued
  service and conservation.
- A relative `rsync` source was resolved from `/tmp` and did not refresh the
  isolated Quinn copy. Use an absolute repository source and verify a changed
  file or build path before trusting the isolated result.

## 2026-07-13 - Vendored test artifacts must be ignored before staging

- Crate-local tests created about `1.9 GiB` of nested `target/`, plus local
  `Cargo.lock` and `.cargo-ok` files. Root `/target` ignore rules did not cover
  them, so an unqualified `git add third_party` could have staged generated
  artifacts.
- Add exact `/third_party/*/{target,Cargo.lock,.cargo-ok}` ignore rules, use an
  external `CARGO_TARGET_DIR` for repeated vendor gates, and inspect
  `git status --ignored` before staging a vendored dependency.

## 2026-07-13 - `FxHashMap` does not re-export the standard `Entry` API

- Symptom: the first keyed endpoint-outstanding implementation failed to
  compile because it imported `rustc_hash::hash_map::Entry`.
- Cause: `rustc_hash` exposes the `FxHashMap` alias, while the entry enum still
  comes from `std::collections::hash_map::Entry`.
- Correct behavior: import `FxHashMap` from `rustc_hash` and `Entry` from the
  standard library. Keep dependency-alias APIs distinct from their underlying
  standard collection APIs.

## 2026-07-13 - `status` is a read-only zsh parameter

- Symptom: an untracked-file whitespace-check loop stopped at assignment to
  `status` even though root fmt and the preceding tracked diff-check passed.
- Cause: zsh reserves `status` as a read-only special parameter for the last
  command exit code.
- Correct behavior: use an ordinary name such as `diff_exit` when preserving
  `git diff --no-index --check`'s expected `1` result for a nonempty new file.
  The corrected loop passed all new spec, plan, Pacer, and patch-manifest
  files.

## 2026-07-13 - Stored-token cap was mistaken for a temporal burst contract

- Symptom: cap64 bound the real data connection at `64*1280` in one formal
  snapshot, yet the P1 still peaked at `267 packets/ms`, dropped `29` TUN TX
  packets, and added `50,621,275B` formal QUIC loss.
- Cause: the patch intentionally preserved Quinn's refill slope. On a sub-ms
  path, `1.25*cwnd/rtt` can replenish multiple buckets inside one millisecond;
  limiting stored tokens alone cannot bound a wall-clock window.
- Correct behavior: reject cap-value retries. RED the sub-ms refill case and
  require any successor design to prove an explicit time-window/service
  contract before implementation or VPS use.

## 2026-07-13 - Unconnected Quinn sockets defeat peer-address ss sampling

- Symptom: the 12-sample active socket artifact captured both Shoes endpoint
  sockets at `drop=0` on every Exit sample but produced no client socket row.
- Cause: the sampler searches `ss` output for the Exit peer, while Quinn owns
  an unconnected UDP socket whose row does not contain that peer address.
- Correct behavior: attribute the client UDP socket from the mini_vpn PID/FD
  and local port, then snapshot that row. Do not report an empty peer-match as
  zero client socket drops.

## 2026-07-13 - Large transfer staging must be polled and secret-denied

- The first source archive included tracked development private keys. It was
  detected and deleted before upload; the replacement archive excluded PEM,
  key, environment, service-account, target, and git material and passed an
  explicit path scan.
- Two initially launched SCP sessions were not polled to completion and left
  partial `510 KiB` remote files; hash verification caught them before use.
- Correct behavior: apply a denylist/path scan before transfer, then run and
  poll one large transfer to completion and require a full SHA-256 match before
  extraction or execution. Parallel fire-and-forget copy completion is not an
  integrity proof.

## 2026-07-13 - Whole-vendor rustfmt is not a valid patch gate

- Symptom: `cargo fmt --manifest-path third_party/quinn-proto-0.11.16/Cargo.toml
  -- --check` proposed broad import/order rewrites across the byte-pinned
  crates.io source, far beyond mini_vpn's six-file Quinn patch manifest.
- A later sequential gate repeated this invalid whole-vendor check without
  fail-fast; the following successful test command made the compound shell
  return zero even though formatting had failed.
- Correct behavior: keep root `cargo fmt --all -- --check`, run the vendored
  Quinn tests/doc tests, compare the vendored tree against the exact crates.io
  source, format/check new standalone patch files directly, and review only
  the manifest-listed differences. Use `set -e` or `&&` for sequential gates
  so a later PASS cannot mask an earlier failure. Do not reformat the entire
  pinned dependency with a different local rustfmt version.

## 2026-07-13 - A rejected throughput tracer remained a mandatory library test

- Symptom: after cap64 passed exact delivery, clean EOF, active attribution,
  and `>170 Mbit/s`, the full library gate failed only because the previously
  rejected bounded `48 then 2ms` replay still asserted `>170 Mbit/s` by
  default.
- Correct behavior: preserve known-negative real replays as named, ignored
  measurement tests and run them only explicitly. Keep deterministic tests for
  their conservation and scheduling contracts active; do not let a rejected
  architecture's host-timing threshold veto a different accepted candidate.

## 2026-07-13 - Forward-first product regression poisoned connection state and unsafe close-tail

- Symptom: a fresh P1 forward window completed at `195/183 Mbit/s` but added
  `54` TUN TX drops, about `61.3 MB` of QUIC lost bytes, and `20,983`
  congestion events. The following reverse inherited that state, averaged
  `136 Mbit/s`, and later reached `half_closed_idle_timeout` with `524288B`
  still in the D16 queue plus `27736B` of active send-capable egress.
- This is not the repaired O(active²) scheduler: local `1024/1024` and fresh
  `60s` reverse both pass. It is not yet proven whether the forward QUIC loss
  is mini_vpn-specific or shared by the external path/server.
- Correct behavior: stop before spending P8/UDP/DNS/rearm, preserve per-window
  start deltas, TDD-inhibit timeout reaping while useful ownership/drain
  remains, and run one same-window sing-box versus mini_vpn forward
  discriminator before selecting a QUIC change. Do not tune frozen parameters.

## 2026-07-13 - reverse-first suite mode is always P1

- Symptom: setting `PARALLEL_SET=8` together with
  `RUN_REVERSE_FIRST_P1=1` still launched `-P 1`; that branch passes a literal
  `1` to the low-RTT probe. The clean `193/191 Mbit/s` result was an extra P1,
  not a concurrency sample.
- Correct behavior: inspect the suite branch as well as environment output.
  Use the `full` branch for `PARALLEL_SET`, or an explicitly bounded direct
  probe behind a lifecycle-safe launcher. Require the report's
  `parallel_set` and actual iperf argv to agree before counting a sample.

## 2026-07-13 - Task 12 product gate exposed O(n squared) relay scheduling

- Symptom: the clean `a54fb17` ignored concurrency sweep passed `64/64` and
  `256/256` but failed `N=1024` at `733/1024` after `120s`; the relay segment
  consumed `101.271s`, all `1024` upstream opens occurred, and remaining flows
  fell into the `90s` idle timeout.
- Root cause: `process_dirty_relay` calls `downlink_pressure_stats` once per
  handle. That helper scans the dirty set and all sockets, restoring
  O(active²) work. For frozen D16/non-buffered operation, the resulting global
  pending value is unused by the downstream credit decision.
- Correct behavior: put a scan-count/cost invariant under TDD, skip aggregate
  work when buffered credit is disabled, and compute retained buffered-path
  aggregates once per pass or incrementally. Re-run the explicit 1024 gate
  before any VPS product regression. Do not mask the failure with a longer
  timeout, smaller concurrency, larger pool, or a product knob change.
- Resolution: the confirmed repair passes `1024/1024` and the local UDP/full
  gates; the current stop is the separate forward-first failure above.

## 2026-07-13 - Gate runner preflight must lock paths, auth env, and noninteractive sudo

- Several repeat-3 attempts exited before mini_vpn or iperf started: the suite
  filename is `knife14b-usclient-tunnel-suite.sh`, the executable is
  `target/release/mini_vpn` with `client-tun` as its subcommand, a fresh SSH
  shell must load the remote `.env`, and Exit evidence uses `EXIT_SSH_KEY`
  rather than a generic `SSH_KEY`.
- `.27` has NOPASSWD command authorization, but mixed sudoers entries make
  `sudo -v` prompt for a credential. A temporary runner changed only that
  preflight to `sudo -n true`; its binary, probe, measurement logic, and frozen
  profile remained identical.
- Correct behavior: before a limited gate, assert the exact suite/binary/probe
  hashes, source the auth environment without printing it, validate both
  dedicated SSH-key variables, use a noninteractive sudo capability check, and
  prove there is no process/TUN/artifact before starting the sample. A
  preflight-only exit is an orchestration failure, not a throughput sample.

## 2026-07-13 - Reconstructed Shoes config omitted the Gate ALPN

- Symptom: the one Task 12 Gate-aligned control had a healthy `214.494
  Mbit/s` direct receiver, correct MTU/routes, and zero-drop full UDP sockets,
  but iperf produced zero intervals. Both endpoints reported
  `peer doesn't support any known protocol` during the cryptographic handshake.
- Root cause: the ephemeral Shoes config omitted
  `quic_settings.alpn_protocols: ["h3"]`. The client and accepted product
  profiles require `h3`; the service was alive but its QUIC/TLS contract was
  incomplete.
- Correct behavior: validate explicit ALPN in the rendered in-memory config
  before feeding FIFO-backed config/cert/key, and treat a pre-throughput
  handshake failure as invalid setup rather than a low control. Preserve the
  failure and do not proceed to mini_vpn repeats until the config is valid.
  Under the user's current standing override, an evidenced configuration-only
  fault may be corrected and rerun without another confirmation.

## 2026-07-13 - Multi-file TLS FIFOs must be written concurrently

- Symptom: a sequential Shoes config/cert/key feed blocked on the certificate
  writer while the service was free to open the key FIFO first.
- Correct behavior: start all one-shot FIFO writers concurrently, wait for all
  of them, and require `Loaded 2 certs/keys`, `Starting 1 server`, and the UDP
  listener before any traffic. Sequential writers are not safe when the
  consumer controls file-open order.

## 2026-07-12 - Timed capacity probes must not impersonate clean-close gates

- The Shoes run produced complete iperf JSON at `192.666 Mbit/s` with every
  interval nonzero, but the direct probe exited failed because one data Connect
  reset at the timed boundary. Its blanket relay-error veto conflicts with the
  already approved composite Gate A separation.
- Correct behavior: parse and preserve terminal causes. A timed capacity gate
  may accept only an expected post-result bounded reset; a fixed-byte EOF gate
  must still reject every reset and nonzero tail. TDD the decision and replay
  captured evidence before another VPS gate.
- Zsh scalar variables do not split into command argv by default, and a glob
  under a root-only directory expands before `sudo`. Use a shell function or
  array for reusable SSH argv, and pass explicit FIFO paths (or expand inside a
  privileged shell) when permissions block caller-side globbing.

## 2026-07-12 - Capture lifetime must begin at the measured window

- Both 55-second bilateral pcaps were empty because capture started before two
  direct baselines and orchestration delays, then expired before the official
  flow. They cannot be used as packet evidence.
- A one-byte probe proved `tcpdump -i any` sees the translated `.111:8443`
  packet on `eth0` with zero kernel drops. Future performance captures must use
  `-i any`, a port-first filter, at least 120 seconds, explicit readiness, and a
  remaining-time check immediately before traffic; baselines run first.
- Reference-server info logs include the TUIC user UUID. Redact UUID patterns
  before packaging and require a silent pattern scan to pass; never include raw
  service journals in artifacts or summaries.
- Tcpdump-owned pcap files require privileged cleanup even when they live in
  `/tmp`; archive first, then remove them with `sudo`.

## 2026-07-12 - Compatibility clients need platform-socket preflight

- The reference client failed before TUIC traffic when `dual_stack:false` was
  applied to an IPv4 SOCKS socket. Omitting the optional dual-stack field let
  the bounded SSH-banner preflight pass.
- `.27` no longer had the sing-box binary/service promised by older environment
  memory. An attempted large streamed copy remained partial and was explicitly
  terminated and deleted after failing SHA. Prefer the small official reference
  client for config compatibility, and always require full binary SHA before
  execution.
- In zsh, `path` is a special variable tied to `PATH`; using it as a loop
  variable made later `curl`/`rg` commands disappear. Use a non-special name
  such as `file_name` in orchestration loops.

## 2026-07-12 - Ephemeral server setup needs mount and safe-path preflight

- `.111` mounts `/run` with `noexec`, so systemd rejected launcher/watchdog
  scripts stored there even though their modes were executable. Use an
  executable mount or invoke the script through an executable interpreter;
  keep only data FIFOs under `/run`.
- Mihomo `SAFE_PATHS` rejected certificate/key paths under `/proc/self/fd`.
  Root-only, one-shot FIFOs below the configured Mihomo home passed validation
  without persisting TLS bytes.
- A one-second run of the strict capacity harness proved auth/Connect
  compatibility but failed relay accounting at the forced timed close. Do not
  repurpose a duration-based capacity harness as an auth-only test; give the
  next server a separate bounded compatibility seam.
- `git bundle create <file> <abbreviated-commit>` can be rejected as an empty
  bundle because the argument is not a ref. Bundle a real branch/ref and
  explicitly checkout and verify the intended commit in the isolated clone.

## 2026-07-12 - A server leaf certificate is not the client trust anchor

- Symptom: the first host-local TLS preflight failed with `UnknownIssuer`
  before opening any TUIC Connect stream or starting iperf traffic.
- Cause: the server leaf certificate was streamed into the client CA FIFO.
- Correct behavior: keep the service leaf/key FIFOs distinct from the client
  trust-anchor FIFO, source the configured CA specifically, and require TLS/
  TUIC preflight success before measured traffic. Preserve the failed
  preflight as setup evidence, not as a capacity run.

## 2026-07-12 - Preserve pipeline exit status and exact test-binary build path

- The first remote direct-TUIC command piped the ignored test through `tee`
  without `pipefail`; the test failed but SSH returned status zero from `tee`.
  Future remote test pipelines must set `set -o pipefail` or avoid the pipe.
- Rebuilding the same source in a different clean-clone path changed the test
  binary SHA because another compiled test module embeds
  `CARGO_MANIFEST_DIR`. For strict A/B, rebuild at the same absolute path and
  require the previous SHA before traffic, or preserve the exact binary.
- A long binary copy can outlive the tool's visible output wait. Never start a
  second writer; poll the existing process/file to completion or explicitly
  terminate it and remove the partial file before switching to one compressed
  stream.

## 2026-07-12 - Unbounded Quinn probe shutdown obscured completed transfer time

- Symptom: the server transfer and completion ACK finished in about `20.3s`,
  but the ignored test process remained alive for about another 45 seconds in
  `Endpoint::wait_idle` and emitted Cargo's over-60-second warning.
- Impact: throughput, integrity, EOF, and ACK evidence were already complete;
  the delay affected test orchestration only.
- Correct behavior: after the explicit completion barrier, close the
  connection and endpoint and bound `wait_idle` to two seconds. Report
  sub-millisecond RTT in microseconds rather than truncating it to `0ms`.

## 2026-07-10 - ACK-capacity Gate A still failed on ordered stream service

- Stage: H10d16 ACK-capacity replacement Gate A.
- Failed bundle:
  `/tmp/mini_vpn_knife14h10d16_ack_capacity_gatea_local/mvpn_knife14h10d16_ack_capacity_gatea_usclient_suite_20260710_233419.tar.gz`.
- Symptom: the clean `f7847dd` build completed reverse-first P1 at
  `19.2/17.9 Mbit/s`, with burst/idle intervals and data read gaps up to
  `3548ms`, below the `>150 Mbit/s` Gate A requirement.
- Rejected roots in this run: TUN drop, actor bypass, local pressure/backlog,
  smoltcp send capacity, send-slice/flush errors, main-loop CPU saturation,
  and client-observed QUIC loss/congestion/blocking.
- Evidence limit: six pending samples had fresh connection-level STREAM-frame
  progress, but connection-global counters cannot prove those frames completed
  the missing ordered offset on the data stream. Do not label this a Quinn
  wake bug without a same-stream RED.
- Lifecycle limit: pending/egress/terminal-tail counters were zero, but the
  data handle was still active/dirty at shutdown and produced no natural close
  event. Do not record the close gate as passed.
- Correct behavior: build a sustained real-Quinn same-stream test through the
  exact direct adapter, reservation owner, TUIC ordered reader, and D16 actor;
  compare it with direct RecvStream consumption; repair only the first seam
  that reproduces the burst/idle gap. Do not tune VPS, MTU/PLPMTUD, broad QUIC
  windows, pool, chunk size, self-wake, or the clean local 24/48 budgets.

## 2026-07-08 - Use `env` rather than long `export ... bash script` suite launches

- Stage: Knife14go focused safe1200 reverse-first P1 acceptance.
- Symptom: the first suite launch failed immediately with
  `bash: line 1: export: 'scripts/knife14b-usclient-tunnel-suite.sh': not a valid identifier`.
- Cause: the command used `export VAR=... bash scripts/...`; shell `export`
  treated `bash` and the script path as identifiers instead of executing the
  suite.
- Correct behavior: after sourcing `.env`, launch one-shot suite environment
  overrides as `env VAR=... bash scripts/knife14b-usclient-tunnel-suite.sh`.
  This keeps secrets in sourced environment variables and avoids malformed
  export syntax.

## 2026-07-08 - Codex exec must allocate a writable PTY for sudo suites

- Stage: Knife14gn focused safe1200 reverse-first P1 acceptance.
- Symptom: the first suite launch used remote `ssh -tt`, reached
  `[sudo] password for ubuntu:`, but `write_stdin` failed with `stdin is
  closed for this session`.
- Cause: the local Codex `exec_command` call did not set `tty=true`; remote
  `ssh -tt` alone was not enough to keep a writable stdin in this tool session.
- Correct behavior: for any suite that may run `sudo -v`, set both remote
  `ssh -tt` and local tool `tty=true` from the initial `exec_command`. If this
  mistake happens, kill only the hung ssh process, then rerun with a writable
  TTY. Never place sudo passwords in commands, scripts, logs, docs, or
  summaries.

## 2026-07-08 - Older Knife14 suite env flags are easy to mis-set

- Stage: Knife14fv/fx acceptance discriminators.
- Symptom: the first `f8765c1` clean-worktree run failed before tunnel startup
  because `EXIT_TO_TARGET_IPERF_CHECK=1` was set without the expected exit SSH
  host/key env. A second run unintentionally executed the normal forward-first
  suite because only the stop-after flag was set, not
  `RUN_REVERSE_FIRST_P1=1`.
- Cause: the older `knife14b-usclient-tunnel-suite.sh` env contract is strict
  and does not infer reverse-first mode from `STOP_AFTER_REVERSE_FIRST_P1=1`.
- Correct behavior: when exit-to-target evidence is enabled, set the required
  exit SSH env explicitly. For a clean reverse-first P1 run, set both
  `RUN_REVERSE_FIRST_P1=1` and `STOP_AFTER_REVERSE_FIRST_P1=1`.

## 2026-07-08 - sing-box v1.13 TUN config uses `address`, not `inet4_address`

- Stage: Knife14fx mature sing-box current A/B.
- Symptom: the temporary sing-box client config check failed when using the old
  TUN field `inet4_address`.
- Cause: sing-box `v1.13.14` uses the current TUN schema with `address`, for
  example `address = ["172.19.0.1/30"]`.
- Correct behavior: for future mature-client A/B configs, use the v1.13
  `address` field and run `sing-box check` before starting the temporary TUN
  client.

## 2026-07-08 - Avoid interactive heredoc helpers when sudo may prompt

- Stage: Knife14fx mature sing-box current A/B.
- Symptom: an interactive SSH heredoc helper got stuck around the sudo prompt
  and had to be interrupted.
- Cause: combining `ssh -tt`, heredoc-fed shell scripts, and sudo prompts makes
  terminal state and input echo hard to reason about.
- Correct behavior: copy or create a non-secret temporary helper on the remote
  host, mark it executable, then run it through a true TTY. Type sudo passwords
  only at prompts, never in commands, scripts, docs, logs, or summaries.

## 2026-07-08 - Knife14fq timeout120 repeat regressed throughput

- Stage: Knife14fq high-buffer clean-tail repeat.
- Failed bundle:
  `/tmp/mini_vpn/knife14fq_socketbuf_timeout120_p1_30/mvpn_knife14fq_socketbuf_timeout120_p1_30_usclient_suite_20260708_094207.tar.gz`
- Symptom: extending `IPERF_TIMEOUT_SECS` from the previous `50s` to `120s`
  made iperf exit normally and cleaned close-tail metrics, but reverse-first P1
  reached only `37.0/35.7 Mbit/s` with `throughput_shape=low_average`.
- Important discriminator: the clean clone at `/home/ubuntu/mini_vpn_accept`
  used `f8765c1`, and key source/script hashes matched the existing dirty
  `/home/ubuntu/mini_vpn` working tree. This was not a different-code
  artifact.
- Clean surfaces: `pending_at_close=0`, `terminal_pending_reap=0`,
  `egress_at_close=0`, `tun_tx_dropped_delta=0`, QUIC
  loss/congestion/blocking/rx_blocked deltas `0`, no send-slice errors, and no
  TUN flush failures.
- Remaining failure: local pressure/headroom gating returned during the data
  window: `downlink_backpressure pause_edges=1 resume_edges=0`,
  `may_recv_false=13594`, `headroom_deferred_bytes=17512861`,
  `pressure_credit_blocked_bytes=923353`, and
  `hard_edge_guard_deferred_bytes=10524`.
- Correct behavior: stop claiming final `100+ Mbit/s` completion from the
  Knife14fp timeout-killed high-throughput run. Run a focused repeat/A-B before
  code changes; if the low shape repeats, fix the local pressure edge rather
  than changing sing-box, iperf3, stale pools, MTU/PLPMTUD, receive windows, or
  broad pool settings.

## 2026-07-06 - Knife14cq remote sync must target repository subdirectories

- Stage: Knife14cq `.27` sync before VPS acceptance.
- Symptom: an initial `rsync` sent `src/client_tun.rs` and docs to
  `/home/ubuntu/mini_vpn/` instead of `src/` and `docs/tech/`.
- Resolution: the misplaced files created by this sync were removed, then
  `src/client_tun.rs` and docs were copied to their exact target directories.
- Correct behavior: when syncing scoped source to `.27`, use explicit remote
  paths such as `/home/ubuntu/mini_vpn/src/client_tun.rs` and
  `/home/ubuntu/mini_vpn/docs/tech/`, or use a tested `--relative` pattern.
  Verify the remote source hash before building.

## 2026-07-06 - Knife14cq clippy caught a nested if in the opt-in gate

- Stage: Knife14cq local gates.
- Symptom: `cargo clippy --all-targets --features harness -- -D warnings`
  failed with `clippy::collapsible-if` in the recent-active timer deadline
  gate.
- Resolution: collapse the condition to `has_downlink_work && let
  Some(duration) = ...`, then rerun focused tests, clippy, full tests, release
  build, harness, and `git diff --check`.
- Correct behavior: keep clippy in the Knife14 gate set after code changes that
  touch runtime control flow; small style failures are cheap to fix before VPS.

## 2026-07-06 - Knife14cp VPS regressed after recent-active timer ACK drain engaged

- Stage: Knife14cp recent-active timer ACK drain VPS acceptance.
- Failed bundle:
  `/tmp/mini_vpn/knife14cp_recent_active_timer_retry_20260706_035508/mvpn_knife14cp_recent_active_timer_retry_usclient_suite_20260707_035508.tar.gz`
- Symptom: reverse-first P1 reached only `19.2/18.0 Mbit/s`, worse than
  Knife14co's `25.5/24.3 Mbit/s`, while still `low_average`.
- Important discriminator: the new code path definitely engaged:
  `timer_active_flow_attempts=1321`, `tun_rx_drain attempts=10633`,
  `would_block=10542`, and `errors=0`.
- Clean surfaces: `local_pressure=0`, `downlink_backpressure pause_edges=0`,
  `tun_tx_dropped_delta=0`, runtime `drop_delta_total=0`,
  `pending_at_close=0`, `egress_at_close=0`, `terminal_pending_reap=0`,
  `terminal_late_remote_payload=0`, no QUIC loss/congestion/blocking deltas,
  no send-slice or TUN flush errors, and no current `.33` TUIC `fail auth`.
- Rejected next moves: do not keep extending recent-active timer drain windows,
  increasing below-pressure ACK budgets, or adding more timer ACK-drain
  variants from this evidence.
- Correct behavior: remove or disable this path as a default, keep the evidence
  as an A/B rejection, and continue with the no-pressure burst/idle branch at
  the TUIC stream read/wake or remote-to-local scheduling boundary.

## 2026-07-06 - Knife14cp suite setup needs explicit exit SSH when server evidence is off

- Stage: first Knife14cp VPS suite attempt.
- Failed bundle:
  `/tmp/mini_vpn/knife14cp_recent_active_timer_20260706_035349/mvpn_knife14cp_recent_active_timer_usclient_suite_20260707_035349.tar.gz`
- Symptom: the suite failed before P1 because `SERVER_EVIDENCE_CHECK=0` skipped
  default SSH host setup, but `EXIT_TO_TARGET_IPERF_CHECK=1` still required
  exit-side SSH variables.
- Correct behavior: when disabling server evidence to avoid `.77:22` tail
  pollution, explicitly set `EXIT_SSH_HOST=ubuntu@43.153.32.33` and
  `EXIT_SSH_KEY=/home/ubuntu/.ssh/vpn` if exit-side baseline checks remain on.

## 2026-07-06 - Knife14co VPS still failed after active-flow drain removed local pressure

- Stage: Knife14co active-flow ACK drain VPS acceptance.
- Failed bundle:
  `/tmp/mini_vpn/knife14co_active_flow_ack_20260706_034114/mvpn_knife14co_active_flow_ack_usclient_suite_20260707_034114.tar.gz`
- Symptom: reverse-first P1 improved to `25.5/24.3 Mbit/s` but remained
  `low_average`.
- Important discriminator: the failure was no longer local pressure during the
  probe window. The parser reported `local_pressure=0`,
  `downlink_backpressure pause_edges=0`, `tun_tx_dropped_delta=0`,
  runtime `drop_delta_total=0`, and `send_queue_max=427496`.
- Active-flow ACK drain was engaged and bounded:
  `tun_rx_drain attempts=7506 packets=24055 tcp=24055 budget_exhausted=15
  would_block=7491 errors=0`.
- Clean surfaces: direct `.27/.33 <-> .77` baselines were healthy, current
  `.33` logs had no `fail auth`, QUIC loss/congestion/blocking deltas were
  zero, pending/close/reap accounting stayed clean, and there were no
  send-slice or TUN flush errors.
- Remaining root direction: do not keep adding pressure/drop debt or ACK budget
  size. The next repair should test a recent-active timer drain below pressure
  so ACK/window updates generated after a burst can be drained even when no new
  remote payload arrives to trigger event-driven drain.

## 2026-07-06 - Knife14co server evidence target SSH polluted post-probe client log tail

- Stage: Knife14co VPS acceptance evidence collection.
- Bundle:
  `/tmp/mini_vpn/knife14co_active_flow_ack_20260706_034114/mvpn_knife14co_active_flow_ack_usclient_suite_20260707_034114.tar.gz`
- Symptom: after the P1 attribution summary, the client log showed a new
  `tuic-open-tcp target=43.130.32.77:22` flow and later pressure/drop lines.
- Root cause: the suite collects target `.77` SSH evidence while the
  `43.130.32.77/32` route is still installed through `tun0`, so the evidence
  SSH session itself enters mini_vpn and perturbs the client log tail.
- Correct behavior: for Knife14 attribution, trust the probe-window summary and
  treat target-evidence tail pressure as test noise unless the same signals
  appear inside the P1 window. Future suite improvements should remove the
  target route before target SSH evidence or skip target SSH evidence for
  reverse-first stop runs.

## 2026-07-06 - cargo fmt check is not a safe Knife14 gate in the current worktree

- Stage: Knife14co local gates.
- Symptom: an extra `cargo fmt --check` failed with broad formatting diffs in
  many pre-existing files, far beyond the Knife14co patch.
- Correct behavior: do not run `cargo fmt` during Knife14 unless a dedicated
  formatting task is requested. Keep using `git diff --check`, focused tests,
  full tests, release build, harness, and clippy as the stage gates.

## 2026-07-06 - Knife14cl VPS failed after local-close deferral exposed unpaid drop debt

- Stage: Knife14cl local uplink close pending deferral VPS acceptance.
- Failed bundle:
  `/tmp/mini_vpn/knife14cl_local_close_pending_20260706_1852/mvpn_knife14cl_local_close_pending_usclient_suite_20260707_025232.tar.gz`
- Symptom: reverse-first P1 improved to `32.9/31.9 Mbit/s` but remained
  `low_average local_pressure=1`.
- Important discriminator: the code change worked. The data flow produced
  `tcp-deferred-close-pending ... direction=local_to_remote
  reason=uplink_channel_closed pending=528364`, while parser close accounting
  reported `pending_at_close=0` and `egress_at_close=0`.
- Remaining local loss/backlog: TUN egress drops returned
  (`tun_tx_dropped_delta=4604`, runtime `drop_delta_total=4334`), pending stayed
  dirty at `528364`, and feedback paused without resume.
- Debt dead-end: final downlink diagnostics had
  `drop_credit_debt_bytes=167956 drop_credit_debt_paid_bytes=0` and
  `pressure_credit_debt_bytes=28652 pressure_credit_debt_paid_bytes=0`, while
  `send_queue_max=892928`, `may_recv_false=8333`, and
  `headroom_limited=8444`.
- Clean surfaces: no current `.33` `fail auth`, no QUIC loss/congestion/blocking
  deltas, no send-slice zero/errors, no TUN flush failure, no terminal pending
  reap, and no terminal late remote payload.
- Rejected next moves: do not continue close-accounting edits, ACK drain budget
  increases, stale pool work, sing-box auth/time/config work, iperf3 tuning,
  receive-window expansion, or static threshold widening from this evidence.
- Correct behavior: repair pressure debt recovery so observed local egress
  drain can retire drop/pressure debt and release bounded flush credit for
  dirty send-capable pending bytes.

## 2026-07-06 - Knife14ck VPS failed after ACK-sized drain reached would_block

- Stage: Knife14ck ACK-sized pressure drain budget VPS acceptance.
- Failed bundle:
  `/tmp/mini_vpn/knife14ck_ack_sized_drain_20260707_0240/mvpn_knife14ck_ack_sized_drain_usclient_suite_20260707_024026.tar.gz`
- Symptom: reverse-first P1 reached only `16.5/15.7 Mbit/s`, with
  `throughput_shape=low_average local_pressure=1`, despite healthy direct
  `.27 -> .77`, `.27 <- .77`, `.33 -> .77`, and `.33 <- .77` baselines.
- Important discriminator: ACK-sized pressure drain was no longer starved by
  budget. The final summary showed `tun_rx_drain attempts=764`,
  `budget_exhausted=1`, and `would_block=763`.
- Remaining local loss/backlog: runtime TUN egress feedback saw
  `drop_events=1 drop_delta_total=273`, and final close still had active
  send-capable backlog with `pending=524906`, `send_queue=892928`,
  `close_egress_drain_candidate=true`, `tcp_state=CloseWait`,
  `can_send=true`, and `may_send=true`.
- Clean surfaces: no QUIC loss/congestion/blocking deltas, no send-slice
  zero/errors, no TUN flush failure, no terminal pending reap, no terminal late
  remote payload, and no current `.33` `fail auth` evidence.
- Rejected next moves: do not keep raising the TUN RX drain budget or tuning
  static ACK-drain values. Do not redirect this result to iperf3, sing-box
  config/time/auth, stale pool slots, QUIC congestion, or receive-window
  expansion.
- Correct behavior: inspect and repair dirty-relay egress retention and
  close-drain lifecycle so active send-capable queued bytes keep receiving
  bounded flush opportunities before close/reap.

## 2026-07-06 - Knife14ci VPS failed because ACK drain runs after the burst

- Stage: Knife14ci adaptive TUN RX ACK drain VPS acceptance.
- Failed/diagnostic bundles:
  `/tmp/mini_vpn/knife14ci_adaptive_ack_drain_20260707_0208/mvpn_knife14ci_adaptive_ack_drain_usclient_suite_20260707_020847.tar.gz`,
  `/tmp/mini_vpn/knife14ci_adaptive_ack_drain_retry_20260707_0212/mvpn_knife14ci_adaptive_ack_drain_retry_usclient_suite_20260707_021233.tar.gz`
- Symptom: the retry P1 stayed `low_average` at `18.8/17.9 Mbit/s` despite
  healthy direct `.27 -> .77` and `.33 -> .77` baselines.
- Important discriminator: `tcp-tun-rx-drain attempts=5 packets=105 tcp=105
  budget_exhausted=5`, so ACK/window packets were ready and the new code path
  engaged. The failure is not "no ACK drain"; it is "ACK drain happens too late
  and only on remote-payload events."
- Local loss/backlog: `tun_tx_dropped_delta=82`, `send_queue_max=892928`,
  `hard_edge_guard_limited=51`, and close-tail active send-capable backlog
  (`pending=525514`, `close_egress_bytes=892928`).
- Clean surfaces: no QUIC loss/congestion/blocking deltas, no send-slice
  zero/errors, no TUN flush failure, no terminal pending reap, no terminal late
  remote payload.
- Correct behavior: add pressure-gated pre-payload and maintenance TUN RX drain
  before more remote bytes are accepted/flushed. Keep it bounded and off below
  the existing egress credit edge.

## 2026-07-06 - Knife14ci startup auth failure was transient, not config mismatch

- Stage: first Knife14ci VPS suite attempt.
- Failed bundle:
  `/tmp/mini_vpn/knife14ci_adaptive_ack_drain_20260707_0208/mvpn_knife14ci_adaptive_ack_drain_usclient_suite_20260707_020847.tar.gz`
- Symptom: client-tun exited during startup with
  `tuic auth finish: sending stopped by peer: error 0`.
- Checks: `.33` sing-box was active with `NRestarts=0`, config check passed,
  `.27` and `.33` time skew was `0s`, NTP was synchronized, and no-secret
  comparison showed UUID/password/ALPN/SNI all matched exactly.
- Resolution: a 20s client-tun smoke immediately after the failure connected
  successfully, and the retry suite reached P1. Treat this as a transient
  TUIC/QUIC entry failure unless it repeats three times in a row.
- Correct behavior: when this appears, run no-secret config/time checks and a
  short startup smoke before restarting sing-box or changing mini_vpn code.

## 2026-07-06 - .27 non-login shell lacks cargo and rg

- Stage: Knife14ci `.27` verification.
- Symptom: `ssh ... 'cargo test ...'` failed with `cargo: command not found`;
  `rg` was also unavailable on `.27`.
- Correct behavior: use `source ~/.cargo/env && cd /home/ubuntu/mini_vpn` for
  remote cargo commands, and use `grep` on `.27` unless `rg` is installed.

## 2026-07-06 - Knife14ch VPS failed before pressure debt could prove throughput

- Stage: Knife14ch adaptive pressure credit debt VPS acceptance.
- Failed bundles:
  `/tmp/mini_vpn/knife14ch_adaptive_pressure_20260707_0142/mvpn_knife14ch_adaptive_pressure_usclient_suite_20260707_014241.tar.gz`,
  `/tmp/mini_vpn/knife14ch_adaptive_pressure_repeat_20260707_0149/mvpn_knife14ch_adaptive_pressure_repeat_usclient_suite_20260707_014902.tar.gz`
- Symptom A: the first run was `no_data` (`0.245/0.020 Mbit/s`) with
  `pressure_credit_debt_bytes=0`, no local pressure, no TUN drops, and no
  current TUIC `fail auth`. It did not exercise the code path being tested.
- Symptom B: the repeat, with `.33 -> .77` baseline forced on, returned to
  `low_average` (`16.0/15.5 Mbit/s`) while direct baselines stayed healthy:
  `.27 -> .77` `282/298 Mbit/s`, `.33 -> .77` `266/297 Mbit/s`.
- Important discriminator: the repeat had clean QUIC loss/congestion/blocking
  deltas, no TUN drops, no global RX queue pressure, and no send-slice errors,
  but still reached `send_queue_max=892928`, `hard_edge_guard_limited=47`, and
  `terminal_late_remote_payload_bytes=1834980`.
- Root cause direction: adaptive pressure debt was not rejected directly; it
  was not installed. The remaining local branch is ACK/window/TUN-RX drain
  cadence under sustained remote payload, plus the close lifecycle consequence
  when the socket reaches terminal no-send while the relay still has tail data.
- Rejected next moves: do not attribute this run to sing-box auth/time/config,
  iperf3, stale pool, QUIC congestion/loss, bounded receive-window growth, or
  another static pressure-debt variant.
- Correct behavior: add a focused, pressure-triggered TUN RX drain path that
  activates only near the egress credit edge and keeps explicit
  `MINI_VPN_TUN_RX_DRAIN_BUDGET` as an override/test knob.

## 2026-07-06 - Knife14cg VPS failed because receive decoupling inflated local backlog

- Stage: Knife14cg bounded global RX receive-window VPS acceptance.
- Failed bundle:
  `/tmp/mini_vpn/knife14cg_global_rx_receive_20260707_0126/mvpn_knife14cg_global_rx_receive_usclient_suite_20260707_012632.tar.gz`
- Symptom: reverse-first P1 reached only `20.0/18.7 Mbit/s` with
  `throughput_shape=low_average`, despite healthy direct `.27 -> .77` and
  `.33 -> .77` baselines.
- Important discriminator: `global_rx_receive` paused at the intended receive
  bound (`receive_high=2097152`, `max_pending_bytes=2123091`), proving the A/B
  path was active. The result was larger app-owned pending, not higher
  throughput.
- Local loss/backlog: final lifecycle showed active send-capable pending
  (`pending=2123091`) and close egress backlog (`close_egress_bytes=892928`),
  with final TUN egress drops totaling `4051`.
- Rejected next moves: do not keep raising receive windows, split receive
  thresholds, pool size, iperf3, sing-box auth/time/config, QUIC congestion, or
  stale pool logic from this evidence.
- Correct behavior: gate bounded global receive decoupling behind
  `MINI_VPN_BOUNDED_GLOBAL_RX_RECEIVE_WINDOW=1` and keep the product default on
  the safe receive gate. The next behavior patch must target local egress
  drain/cadence and prove that queued bytes are consumed rather than buffered
  into larger pending.

## 2026-07-06 - Knife14cf VPS failed after proactive pressure credit reduced drops

- Stage: Knife14cf proactive egress credit gate VPS acceptance.
- Failed bundle:
  `/tmp/mini_vpn/knife14cf_pressure_credit_20260707_0112/mvpn_knife14cf_pressure_credit_usclient_suite_20260707_011213.tar.gz`
- Symptom: reverse-first P1 reached only `20.5/19.5 Mbit/s`, with many
  zero-throughput iperf intervals, even though direct `.27 -> .77` and
  `.33 -> .77` baselines were healthy.
- Important discriminator: proactive pressure debt engaged and reduced probe
  TUN drops (`tun_tx_dropped_delta=539`, down from Knife14ce's `2813`), and the
  feedback gate recovered during the probe (`pause_edges=1 resume_edges=1`).
- Remaining root for this stage: local burst/stall cadence still reached the
  hard smoltcp tx-queue edge (`send_queue_max=892928`) and closed with active
  send-capable backlog (`pending=574203`, `close_egress_bytes=892928`).
- Rejected next moves: do not keep adding static pressure/drop debt, stale pool
  changes, iperf3 tuning, sing-box auth/time/config work, QUIC congestion work,
  TUN queue length tuning, or close/reap hiding.
- Correct behavior: re-evaluate the receive-path architecture. The next patch
  should test bounded decoupling between TUIC stream reads/global receive
  progress and local TUN egress pressure, while keeping per-flow pending and
  close accounting bounded and observable.

## 2026-07-06 - Knife14cf rsync must preserve repository paths and SSH key

- Stage: Knife14cf local-to-`.27` sync.
- Symptom A: a first `rsync` attempt without `-i ~/.ssh/vpn` failed with SSH
  `Permission denied`.
- Correct behavior A: use `rsync -e 'ssh -i ~/.ssh/vpn ...'` for `.27`, `.33`,
  and `.77` syncs from the Mac mini.
- Symptom B: a second sync without `-R` copied selected files into
  `/home/ubuntu/mini_vpn/` root instead of their repository subdirectories.
- Correct behavior B: for focused file syncs, use `rsync -avR` from the repo
  root or explicit destination directories. Remove any accidental root-level
  copies before running VPS tests so the remote worktree stays understandable.

## 2026-07-06 - Knife14ce VPS failed because drop-debt feedback arrived after the hot burst

- Stage: Knife14ce drop-aware egress credit VPS acceptance.
- Failed bundle:
  `/tmp/mini_vpn/knife14ce_drop_credit_20260707_0049/mvpn_knife14ce_drop_credit_usclient_suite_20260707_005007.tar.gz`
- Symptom: reverse-first P1 reached only `23.6/21.5 Mbit/s` with
  `tun_tx_dropped_delta=2813` in the probe and final `drop_delta_total=5405`.
- Important discriminator: `tcp-tun-egress-feedback` installed
  `drop_credit_debt_bytes=196608`, but the hot `tcp-downlink-flush` and final
  lifecycle summaries kept `drop_credit_debt_bytes=0`,
  `drop_credit_debt_paid_bytes=0`, and `drop_credit_blocked_bytes=0`.
- Root cause for this stage: sysfs TUN drop feedback was sampled too late to
  control the burst that had already filled local egress pressure; it became
  close-tail evidence rather than hot-path control.
- Rejected next moves: do not treat this as stale pool, iperf3, sing-box auth,
  time skew, server config, QUIC loss/congestion, terminal pending, or
  close/reap loss.
- Correct behavior: keep the debt invariant, but add a proactive local
  egress-pressure credit gate before another VPS run. Also verify feedback can
  resume from raw low pressure instead of remaining stuck after drops.

## 2026-07-06 - Knife14ce remote commands need login shell and explicit env source

- Stage: Knife14ce `.27` sync and VPS acceptance setup.
- Symptom A: a direct non-login SSH command on `.27` failed with
  `cargo: command not found`.
- Correct behavior A: use `bash -lc 'cd /home/ubuntu/mini_vpn && cargo ...'`
  for remote Rust commands so the VPS toolchain environment is loaded.
- Symptom B: the first suite attempt stopped before the business test because
  required TUIC environment variables were not present in the shell even though
  `.env` existed on `.27`.
- Correct behavior B: in the same true TTY command that runs the suite, source
  the VPS-local `.env` with `set -a; . ./.env; set +a` before invoking the
  script. Do not print or store any secret values.

## 2026-07-06 - Knife14bw VPS acceptance failed with tx-queue pressure oscillation

- Stage: Knife14bw reverse starvation diagnostics acceptance for commit
  `4a12b18`.
- Failed bundle:
  `/tmp/mini_vpn/knife14bw_starvation_diag_20260706/mvpn_knife14bw_starvation_diag_usclient_suite_20260706_212704.tar.gz`
- Symptom: reverse-first P1 reached only `19.8/18.9 Mbit/s`, with burst/idle
  intervals rather than stable high throughput.
- Important discriminator: data stream delivery was not tiny
  (`tuic_tcp_stream data_rx_bytes_max=72662685`), and live
  `tcp_reverse_window` samples showed `send_capacity=1048576`, `pending=0`,
  `active=true`, `can_send=true`, and mostly `may_recv=true`.
- Active limiter: `downlink_backpressure` toggled `pause_edges=51` and
  `resume_edges=51` on smoltcp tx-queue pressure
  (`max_tx_queue_bytes=588901`) while app-owned pending stayed `0`.
- Rejected next moves: do not treat this run as stale pool, iperf3, sing-box,
  TUIC auth, TUN drop, QUIC congestion, terminal pending, or close-drain
  evidence.
- Correct behavior: before the next behavior patch, propose and confirm a
  tx-queue pressure cadence change with focused TDD. The patch must reduce
  pause/resume oscillation without allowing unbounded read-ahead.

## 2026-07-06 - Knife14bw clippy caught wide diagnostic formatter arguments

- Stage: Knife14bw reverse-window diagnostic local gate.
- Symptom: `cargo clippy --all-targets --features harness -- -D warnings`
  failed on `format_tcp_reverse_window_diag` with
  `clippy::too_many_arguments` after the first implementation used eight
  parameters.
- Root cause: diagnostic-only helper functions can still trip repo quality
  gates when they mirror log fields directly as positional parameters.
- Correct behavior: collect diagnostic log fields into a small purpose-specific
  struct and keep the formatter interface narrow. This preserves log output,
  makes call sites clearer, and avoids adding local `allow` attributes for a
  simple design issue.

## 2026-07-06 - Cargo test accepts one test filter before harness args

- Stage: Knife14bw focused local test rerun.
- Symptom: `cargo test --lib test_a test_b test_c` failed with
  `unexpected argument` because Cargo accepts only one test name/filter before
  `--`.
- Correct behavior: use one broad filter such as
  `cargo test --lib client_tun::tests::reverse_window_diag`, or run separate
  filtered commands when exact test names are needed.

## 2026-07-05 - Knife14bg VPS acceptance failed after TUN RX drain patch

- Stage: Knife14bg bounded TUN RX drain cadence acceptance for commit
  `356e2d2`.
- Failed bundle:
  `/tmp/mini_vpn/mvpn_knife14bg_tun_rx_drain_usclient_suite_20260705_085637.tar.gz`
- Symptom: clean reverse-first P1 reached only `0.315/0.025 Mbit/s` even
  though `.27 -> .77` and `.33 -> .77` direct reverse preflights were healthy.
- Important discriminator: `tun_rx_drain` processed `25` TCP packets with
  zero errors, while `downlink_backpressure`, terminal pending, close-time
  pending, TUN drops, TUN flush failures, `send_slice` zero/errors, and
  clean-window QUIC loss/congestion all stayed zero.
- Active signal: only about `92 KiB` reached mini_vpn on the reverse data
  stream, and the clean attribution was
  `tuic_stream_read_gap+relay_remote_read_gap`.
- Rejected next moves: do not tune TUN RX drain budget, TUN queue length,
  close/reap grace, terminal pending accounting, downlink egress pacing, stale
  TCP pool, iperf3, sing-box availability, or connection pool from this failed
  clean window.
- Correct behavior: after this failure, stop before behavior edits. First add
  behavior-neutral stream-gap instrumentation and a scoped reverse-only A/B that
  can compare drain enabled versus disabled without later forward-window QUIC
  congestion polluting the result.

## 2026-07-04 - Knife14az reverse_sender_backpressured needs exit-target baseline and stream-gap evidence

- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14az_tcp_policy_usclient_suite_20260704_224324.tar.gz`
- Symptom: clean reverse-first P1 on commit `f21e782` produced only
  `0.245/0.009 Mbit/s` while local downlink pressure, terminal pending, TUN
  drops, and QUIC loss/congestion were all absent.
- Rejected assumption: a tiny reverse receiver result after the local TCP socket
  policy patch still implies local smoltcp tx-queue drain is the active limiter.
  In this run `send_queue_max=1`, pending was `0`, and the parser attributed the
  probe to `reverse_sender_backpressured`.
- Follow-up check: `.33 -> .77` direct forward/reverse was healthy
  (`231/232 Mbit/s` receiver), so the suite result was not explained by a
  simple exit-target iperf path bottleneck.
- Correct behavior: when `reverse_sender_backpressured` appears, collect or
  enable exit-target preflight in the suite, then add behavior-neutral TUIC TCP
  stream first-byte/read-gap diagnostics before changing lifecycle, pacing,
  socket buffer, queue, or close/reap behavior again.

## 2026-07-04 - Knife14ay VPS acceptance failed but identified pre-terminal Closed edge

- Stage: Knife14ay local TCP Closed transition diagnostics acceptance for
  commit `67a8c46`.
- Failed bundle:
  `/tmp/mini_vpn/mvpn_knife14ay_tcp_lifecycle_usclient_suite_20260704_222042.tar.gz`
- Symptom: clean reverse-first P1 reached only `9.15/8.18 Mbit/s` while the
  direct `.27 -> .77` reverse preflight receiver was about `276 Mbit/s`.
- Important discriminator: the new lifecycle line showed
  `prev_state=Established state=Closed source=dirty_relay pending=0
  terminal_candidate=false`, followed by close-tail terminal pending
  `552440` bytes. Terminal pending was therefore a post-Closed accounting
  outcome, not the hidden pre-close loss point.
- Rejected next moves: do not change close/reap behavior, stale pool, iperf3,
  sing-box, TUN queue length, connection pool, or egress pacer for this clean
  reverse-first failure without new evidence.
- Future behavior: before the next behavior patch, confirm a plan that targets
  local smoltcp TCP send policy and tx-queue/receive-window drain. The first
  candidates are focused tests plus local-link socket policy evaluation
  (`TCP_NODELAY` via `set_nagle_enabled(false)` and, if justified,
  `set_ack_delay(None)`).

## 2026-07-04 - Broad cargo fmt check exposed pre-existing repo formatting drift

- Stage: Knife14ay local regression.
- Symptom: `cargo fmt --all -- --check` produced large diffs in many files
  outside the Knife14ay change set, including unrelated modules such as
  `src/main.rs` and `src/reality_upstream.rs`.
- Root cause: repository-wide rustfmt drift predates this diagnostic patch.
  Applying broad formatting here would mix unrelated churn into a lifecycle
  observability commit.
- Correct behavior: do not treat this broad diff as a Knife14ay code
  regression. Keep the current stage scoped, use `git diff --check` for
  whitespace safety, and reserve full-repo rustfmt cleanup for a separate
  coherent task.

## 2026-07-04 - Knife14ax VPS acceptance failed after tx-queue backpressure patch

- Stage: Knife14ax tx-queue-aware downlink backpressure acceptance for commit
  `58f847d`.
- Failed bundle:
  `/tmp/mini_vpn/mvpn_knife14ax_tx_queue_backpressure_usclient_suite_20260704_215053.tar.gz`
- Symptom: clean reverse-first P1 failed with `iperf3: unable to receive
  results` instead of producing a low but complete `10-20 Mbit/s` result.
- Important discriminator: the new tx-queue pressure metrics did not trigger:
  `downlink_backpressure pause_edges=0`, `max_tx_queue_bytes=0`, and
  `max_pressure_bytes=0`. The clean window also had `global_rx_pressure=0`,
  `tun_tx_dropped_delta=0`, TUN flush failures zero, and QUIC loss/congestion
  delta zero.
- Close-boundary signal: the reverse data socket had only about `221884`
  remote-to-local bytes before `dead_slot_reap` observed
  `tcp_state=Closed active=false can_send=false`, with `41208` bytes classified
  as terminal pending.
- Rejected next moves: do not keep tuning tx-queue backpressure, stale pool,
  iperf3, sing-box, egress pacing, TUN queue length, or QUIC congestion for the
  clean reverse-first root without new evidence.
- Future behavior: after a failed repair run like this, stop before behavior
  edits. First add lifecycle/state-transition diagnostics and focused tests that
  explain why the local TCP socket becomes `Closed` while remote downlink volume
  is still tiny.

## 2026-07-04 - Local cargo test QUIC bind checks fail inside restricted sandbox

- Stage: Knife14ax local regression.
- Symptom: `cargo test` and `cargo test --features harness` failed only for
  `quic::tests::client_endpoint_binds` and
  `quic::tests::client_endpoint_binds_with_each_cc` when run inside the managed
  sandbox. The same filtered tests passed immediately outside the sandbox.
- Root cause: the tests create local QUIC endpoints and require local bind
  permissions not available in the restricted sandbox.
- Correct behavior: when these exact tests fail under sandboxing, rerun the
  cargo test command outside the sandbox before treating it as a code
  regression. Do not change QUIC code or test assertions for this failure mode.

## 2026-07-04 - Knife14aw diagnostics passed but reverse-first throughput stayed low

- Stage: Knife14aw send-window/global_rx diagnostics acceptance for commit
  `ef7364c`.
- Failed bundle:
  `/tmp/mini_vpn/mvpn_knife14aw_send_window_globalrx_usclient_suite_20260704_204329.tar.gz`
- Symptom: clean reverse-first P1 reached only `16.4/15.0 Mbit/s`, while
  post-suite direct baselines on `.27 <-> .77` and `.33 <-> .77` were about
  `196-198 Mbit/s`.
- Important discriminator: the new diagnostics showed `send_queue_max=1048576`
  with `send_capacity_min=max=1048576`, `pending_total=0`,
  `global_rx_queue_used_max=258/1024`, no clean-window QUIC loss/congestion,
  and no `send_slice` zero/error. The low throughput is not explained by a
  missing parser tail, relay channel saturation, or a shrunken smoltcp capacity.
- Rejected next moves: do not return to stale pool, iperf3, sing-box, egress
  pacer, TUN queue length, or close/reap tuning without new evidence.
- Future behavior: make downlink backpressure aware of smoltcp tx queue
  occupancy, because app-owned `downlink_pending` can be zero while the socket
  tx queue is saturated and TUN egress drops.

## 2026-07-04 - Knife14av post-iperf reporting passed but VPS throughput failed

- Stage: Knife14av post-iperf close-tail reporting acceptance for commit
  `b7e10b1`.
- Failed bundle:
  `/tmp/mini_vpn/mvpn_knife14av_post_iperf_close_tail_usclient_suite_20260704_202011.tar.gz`
- Symptom: reverse-first P1 reached only `11.4/10.1 Mbit/s`, and later reverse
  windows stayed low (`18.1/17.0 Mbit/s`, `24.1/23.3 Mbit/s`).
- Important discriminator: the parser fix worked. The clean reverse-first
  `tcp-handle-close ... pending=69769 ... terminal_pending_reap_bytes=69769`
  line appeared in the same probe summary as `terminal_pending_reap` and
  `pending_at_close`.
- Rejected next moves: do not treat this as a stale-pool, iperf3, sing-box,
  egress-pacer, TUN queue, or clean-window QUIC loss/congestion issue without
  new evidence.
- Future behavior: add send-window and relay queue diagnostics before changing
  close/reap behavior. `global_rx_pressure=0` and `no_send_capacity=N` are not
  detailed enough to distinguish channel backlog, smoltcp tx-buffer fullness,
  terminal `may_send=false`, or receive-window behavior.

## 2026-07-04 - Knife14at first VPS attempt missed local TUIC environment

- Failed report:
  `/tmp/conn/mvpn_knife14at_tun_egress_feedback_usclient_suite_20260704_185107.md`
- Failed bundle:
  `/tmp/conn/mvpn_knife14at_tun_egress_feedback_usclient_suite_20260704_185107.tar.gz`
- Symptom: the first `.27` Knife14at suite attempt stopped before sudo, build,
  tunnel startup, or iperf traffic because the non-login SSH shell had not
  loaded required TUIC environment variables.
- Rejected assumption: if `.env` exists on `.27`, a direct remote suite command
  will automatically load it.
- Correct behavior: before running the suite, check that the required variable
  names exist without printing values, then explicitly source the VPS-local
  `.env` in the same TTY command:
  `set -a && . ./.env && set +a && ...`.
- Security rule: keep `.env` local to the VPS/user environment. Do not print,
  commit, copy, summarize, or store secret values in reports, docs, learning
  memory, commands, or final messages.
- Future debugging rule: if a suite fails at TUIC Env Checks, treat it as an
  invocation/environment failure. Do not analyze mini_vpn, sing-box, iperf, TUN,
  or QUIC behavior until the environment is loaded and the tunnel actually
  starts.

## 2026-07-04 - Non-TTY sudo preflight failure produced no data-plane evidence

- Log bundle:
  `/tmp/conn/mvpn_knife14as_terminal_pending_reversefirst_usclient_suite_20260704_181027.tar.gz`
- Symptom: the first `.27` Knife14as suite attempt stopped at `sudo -v` before
  build, tunnel startup, or iperf traffic. There was no mini_vpn behavior to
  analyze.
- Rejected assumption: a suite that may invoke `sudo -v` can be rerun through a
  non-interactive SSH command and still produce useful acceptance evidence.
- Correct behavior: start `.27` suites that need sudo in a real writable TTY and
  enter the sudo password only at the prompt.
- Security rule: do not store sudo passwords in `.env`, commands, scripts,
  repository files, docs, learning memory, reports, or summaries.
- Future debugging rule: if a suite fails before build or service preflight,
  treat it as a harness/operator failure, fix the invocation mode, and do not
  spend analysis budget on TUIC, QUIC, TUN, smoltcp, or iperf.

## 2026-07-04 — Knife14ar close-safe egress pacing failed VPS acceptance

- Stage: Knife14ar close-safe downlink egress pacing acceptance for commit
  `3baf476`.
- Failed bundle:
  `/tmp/mini_vpn/mvpn_knife14ar_close_safe_default_usclient_suite_20260704_164651.tar.gz`
- Symptom: clean reverse-first P1 reached only `17.2/16.2 Mbit/s`, still below
  the Knife14ap clean reverse-first receiver result of `22.0 Mbit/s`.
- Important discriminator: the intended defaults and code path were active
  (`MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES=16777216`,
  `tun_flush_deferred=0`). Direct reverse baselines were healthy
  (`.27 -> .77` receiver `271 Mbit/s`, `.33 -> .77` receiver
  `285 Mbit/s`). The clean reverse window had no QUIC loss/congestion delta, no
  TUN drops, no `send_slice` zero/error, and no TUN flush syscall failure.
- Close-boundary signal: after the clean reverse window, a relay still hit
  `reason=dead_slot_reap` with `pending=224765`, `pending_high=583624`,
  `tcp_state=Closed`, `active=false`, `can_send=false`, and
  `tun_flush_deferred=0`.
- Future behavior: do not keep tuning downlink egress pacing for this failure.
  The next code plan must add deterministic close/reap tests and explicit
  accounting for terminal pending bytes before another VPS throughput run.

## 2026-07-04 — Knife14aq egress pacing default failed VPS acceptance

- Stage: Knife14aq default downlink egress pacing acceptance for commit
  `212ce26`.
- Failed bundle:
  `/tmp/mini_vpn/mvpn_knife14aq_egress_pacing_default_usclient_suite_20260704_160317.tar.gz`
- Symptom: clean reverse-first P1 reached only `16.3/13.8 Mbit/s` with
  `MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES=65536`, worse than the previous
  Knife14ap clean reverse-first receiver result of `22.0 Mbit/s`.
- Important discriminator: direct `.27 -> .77` and `.33 -> .77` reverse
  baselines were healthy (`295 Mbit/s` and `283 Mbit/s` receiver). The clean
  reverse window had no QUIC loss/congestion delta, no `send_slice` zero/error,
  and no TUN flush syscall failure. The pacer did engage
  (`tun_flush_deferred=1057`) and reduced TUN drops (`366 -> 65`), but
  backpressure remained and throughput fell.
- Close-boundary signal: the same run reaped a relay with
  `reason=dead_slot_reap`, `pending=576827`, `tcp_state=Closed`,
  `can_send=false`, and `can_recv=false`.
- Future behavior: do not continue tuning this default as if it were accepted.
  Before further VPS runs, change the design so remote-payload egress pacing is
  close-safe or default-off, and add tests for pending downlink bytes across
  close/reap boundaries.

## 2026-07-04 — Codex interactive sudo over SSH needs a writable PTY session

- Stage: Knife14ap VPS acceptance.
- Failed/non-useful attempts:
  - `ssh -tt ...` without `tty=true` reached `.27` `sudo -v`, but the Codex
    session did not keep writable stdin for the password prompt.
  - Non-TTY suite invocation failed at `sudo -v` even after an outer
    `sudo -n true` check, because the script uses a naked `sudo -v` and sudo
    needed a terminal/password for that invocation.
- Working behavior: run the suite over SSH with a true writable PTY
  (`exec_command` with `tty=true`) and enter the sudo password only into the
  prompt. Do not place the password in scripts, repository files, reports, or
  final summaries.
- Future behavior: for `.27` acceptance runs where sudo may be cold, start the
  VPS suite in a writable PTY from the beginning. A plain `ssh -tt` subprocess
  can still leave Codex unable to answer the prompt.

## 2026-07-04 — Knife14ao default-watermark acceptance did not reproduce the high A/B result

- Stage: Knife14ao default downlink-watermark acceptance for commit `0d0765f`.
- Failed bundle:
  `/tmp/mini_vpn/mvpn_knife14ao_default_bp512_128_flush256_tty_usclient_suite_20260704_144755.tar.gz`
- Symptom: `.27` ran the current branch with the new product defaults active
  (`high=524288B`, `low=131072B`, `flush=262144B`), but clean reverse-first P1
  reached only `22.5/21.4 Mbit/s`. The earlier no-code 512/128 KiB A/B had
  reached `158/157 Mbit/s`.
- Important discriminator: direct `.27 -> .77` and `.33 -> .77` iperf baselines
  were healthy, sing-box and iperf3 services were active, and the clean
  reverse-first tunnel window had no QUIC loss/congestion or inherited
  congestion. The remaining signals were local:
  `local_tun_egress_drop+local_downlink_backpressure`,
  `max_pending_bytes=589698`, and `tun_tx_dropped_delta=496`.
- Future behavior: do not treat 512/128 KiB watermarks as a final throughput
  fix. Before further tuning or scheduler changes, add downlink flush progress
  observability so the next bundle can distinguish `can_send=false`, zero/short
  `send_slice`, budget clipping, and TUN/qdisc loss after successful smoltcp
  acceptance.

## 2026-07-04 — Manual `.27` suite invocations must explicitly source `.env`

- Stage: Knife14ao downlink-watermark A/B.
- Failed bundle:
  `/tmp/conn/mvpn_knife14ao_bp256_64_flush256_defaultqlen_tty_usclient_suite_20260704_142904.tar.gz`
- Symptom: the suite stopped before startup with all TUIC environment variables
  reported missing.
- Important discriminator: `/home/ubuntu/mini_vpn/.env` existed, had mode 600,
  and `set -a; . ./.env; set +a` exported the required TUIC variables. The file
  uses `export KEY=...`, so a naive `grep '^KEY='` check is not a valid
  presence test.
- Correct behavior: when invoking the suite manually over SSH, run it from
  `/home/ubuntu/mini_vpn` after sourcing `.env` in the same shell. Keep secrets
  out of command output, scripts, repository files, reports, and learning
  memory.

## 2026-07-04 — Knife14ao A/B #2 failed before TUIC app-level logging

- Stage: Knife14ao downlink-watermark A/B with
  `MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=262144`,
  `MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=65536`, and
  `MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES=262144`.
- Failed bundle:
  `/tmp/conn/mvpn_knife14ao_bp256_64_flush256_defaultqlen_tty_usclient_suite_20260704_143108.tar.gz`
- Symptom: `.27` loaded the TUIC env and direct baselines were healthy
  (`.27 -> .77` reverse 290 Mbit/s receiver, `.33 -> .77` reverse
  287 Mbit/s receiver), but `client-tun` exited during startup with
  `tuic auth finish: sending stopped by peer: error 0`.
- Important discriminator: `.33` `sing-box` was active, had not restarted, and
  UDP `:8443` was owned by the `sing-box` process, but
  `/var/log/sing-box.log` mtime remained at `2026-07-04 14:27:09 +0800`.
  The failed 14:31 startup did not produce a TUIC inbound log line.
- Future behavior: do not interpret this failure as a throughput result or a
  watermark regression. Treat it as a startup/QUIC-handshake boundary issue
  until a rerun reaches the iperf probe window or server-side logs show an
  app-level TUIC connection.

## 2026-07-04 — `.27` suite needs a TTY when sudo timestamp is cold

- Stage: Knife14an VPS acceptance.
- Failed bundle:
  `/tmp/conn/mvpn_knife14an_flush256_defaultqlen_usclient_suite_20260704_140940.tar.gz`
- Symptom: the suite stopped at `sudo -v` with
  "a terminal is required to read the password" before building or starting
  `client-tun`.
- Important discriminator: rerunning the same suite through `ssh -tt` and
  entering the `.27` sudo password succeeded. The successful bundle was
  `/tmp/conn/mvpn_knife14an_flush256_defaultqlen_tty_usclient_suite_20260704_141008.tar.gz`.
- Future behavior: when `.27` sudo timestamp may be cold, use an interactive
  TTY for the suite instead of non-TTY SSH. Do not put the sudo password into
  scripts, repo files, reports, or learning memory.

## 2026-07-04 — Use `.27` HTTPS origin or known_hosts before GitHub SSH pull

- Stage: Knife14an VPS checkout sync.
- Command failed on `.27`: one-shot `git pull --ff-only` from
  `git@github.com:S7245/mini_vpn.git`.
- Error: `Host key verification failed`.
- Working fallback: `.27` already had
  `origin=https://github.com/S7245/mini_vpn.git`, and
  `git pull --ff-only origin codex/knife14d-downlink-reap-open` succeeded.
- Future behavior: do not use GitHub SSH from `.27` until its known_hosts entry
  is deliberately initialized. HTTPS origin is fine for fetch/pull; local Mac
  still needs SSH for push.

## 2026-07-04 — Full `cargo fmt --check` reports broad pre-existing formatting drift

- Stage: Knife14an local verification after adding the downlink flush budget.
- Command failed locally: `cargo fmt --check`.
- Symptom: rustfmt wanted to rewrite large portions of `src/client_tun.rs`,
  `src/reality_upstream.rs`, and `src/main.rs`, including many lines outside
  the Knife14an diff.
- Important discriminator: focused behavior checks passed:
  `cargo test --lib client_tun`, `bash -n scripts/knife14b-usclient-tunnel-suite.sh`,
  `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`, and
  `git diff --check`.
- Correct behavior: do not run whole-repo `cargo fmt` inside a scoped
  throughput fix because it would create unrelated churn and make review harder.
  Use `git diff --check` for patch whitespace, and make formatting cleanup a
  separate explicit task if the project decides to normalize rustfmt output.

## 2026-07-04 — Knife14al cleanup regex killed the active suite shell

- Failed bundle:
  `/tmp/mini_vpn/mvpn_knife14al_samewindow_defaultqlen_usclient_suite_20260704_132521.tar.gz`
- Symptom: the default-queue same-window diagnostic stopped during
  `Stop Old Tunnel`, before any tunnel iperf connection reached `.77`.
- Root cause: `pgrep/pkill -f '[m]ini_vpn.*client-tun'` matched the active
  shell/SSH command line because it included the `/home/ubuntu/mini_vpn` working
  path and the `knife14b-usclient-tunnel-suite.sh` script name. The cleanup
  therefore killed the runner instead of only stale `mini_vpn client-tun`
  processes.
- Fix: commit `1ab58e3` matches `comm == mini_vpn` plus an independent
  `client-tun` argv token, and adds `--self-test` false-positive coverage.
- Future behavior: before adding `pkill -f` to a harness, write or run a
  self-test with realistic SSH/sudo/script command lines. Prefer PID lists from
  `ps` parsing over broad command-line regexes for destructive cleanup.

## 2026-07-04 — Knife14al attribution missed inherited QUIC congestion

- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14al_samewindow_defaultqlen2_usclient_suite_20260704_133130.tar.gz`
- Symptom: the standard P1 reverse probe after a high-throughput forward probe
  reported `attribution: no_pressure_signal` while throughput was only
  11.5 Mbit/s sender and 10.7 Mbit/s receiver.
- Important discriminator: the reverse window did not accumulate new QUIC
  loss/congestion deltas, but it started with a damaged connection state:
  `cwnd=5808`, `lost_bytes=500976100`, and `congestion_events=93571` inherited
  from the preceding forward burst.
- Correct behavior: do not interpret this post-forward reverse sample as a
  clean no-pressure branch. Prefer reverse-first/fresh-connection samples for
  directional diagnosis, or reset the TUIC connection before the comparison.
- Future behavior: update low-RTT attribution to flag absolute low cwnd and
  preexisting high loss/congestion counters, not only per-window deltas.

## 2026-07-04 — Knife14ak qlen=5000 sample hit reverse sender backpressure, not TUN drops

- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14ak_tunqlen5000_usclient_suite_20260704_131103.tar.gz`
- Tested commit: `b31b234`
- Setup: `.27` sourced `/home/ubuntu/mini_vpn/.evn` quietly, used
  `TUN_TX_QUEUE_LEN=5000`, `MINI_VPN_TUIC_CC=bbr`, `MINI_VPN_TUIC_TCP_POOL=1`,
  and `RUN_REVERSE_FIRST_P1=1`.
- The qlen setup itself worked: report showed `tun_tx_queue_len_actual=5000`.
- Symptom: reverse tunnel P1 collapsed to 0.314 Mbit/s sender and 0.057 Mbit/s
  receiver, while direct client-target and exit-target reverse preflights were
  healthy.
- Important discriminator: `tun_tx_dropped_delta=0`, downlink backpressure was
  zero, local/global pressure was zero, QUIC loss/congestion and blocked-frame
  deltas were zero, and the close tail had `pending=0`.
- Exit-side clue: sing-box logged the TUIC inbound/direct outbound opens and
  later `connection download closed: stream 4 canceled by remote with error
  code 0` for the probe window.
- Correct behavior: do not interpret this as proof that qlen=5000 fixes or
  breaks mini_vpn throughput. The run switched to the
  `reverse_sender_backpressured` branch. Before product changes, add or run
  paired diagnostics that capture target sender, exit sing-box, and mini_vpn
  TUIC receive evidence during the same reverse probe.

## 2026-07-04 — Use portable find commands on macOS

- Command failed locally after extracting the Knife14ak bundle:
  `find /tmp/mini_vpn/knife14ak_tunqlen5000_131103 -maxdepth 1 -type f -printf '%f\n'`.
- Error: macOS/BSD `find` does not support GNU `-printf`.
- Working fallback: `find <dir> -maxdepth 1 -type f -exec basename {} \; | sort`.
- Future behavior: avoid GNU-only `find -printf` in this Mac workspace unless
  GNU find is explicitly installed.

## 2026-07-04 — HTTPS origin push failed; SSH one-shot push worked

- Command failed: `git push` against `origin=https://github.com/S7245/mini_vpn.git`.
- Error: `fatal: could not read Username for 'https://github.com': Device not configured`.
- Working fallback: `git push git@github.com:S7245/mini_vpn.git codex/knife14d-downlink-reap-open`.
- Future behavior: do not assume HTTPS origin can push from this session. If
  SSH GitHub access is available, use a one-shot SSH URL or ask before changing
  `origin`.

## 2026-07-04 — Source `.evn` with output redirected; it can dump secrets

- First VPS suite invocation failed because TUIC env vars were not loaded:
  `/tmp/mini_vpn/mvpn_knife14aj_tun_drop_usclient_suite_20260704_111955.tar.gz`.
- The `.27` project root had `.evn`, not `.env`, and it was missing explicit
  `MINI_VPN_TUIC_SNI`/`MINI_VPN_TUIC_ALPN`; repo convention fills those as
  `example.com` and `h3`.
- Pitfall: sourcing `.evn` printed exported environment lines to stdout in the
  SSH command stream, including sensitive values. The generated suite bundle
  still redacted secrets, but the command stream did not.
- Future behavior: never source `.evn` directly in a command whose stdout is
  captured. Use a quiet wrapper such as `set -a; source ./.evn >/dev/null
  2>&1; set +a`, then explicitly export non-secret defaults.

## 2026-07-04 — Knife14aj initial plan mistook Closed pending for drainable pressure

- Initial design branch: add a progress-sensitive grace for inactive pending
  even when `can_send=false`, based on the post-restart close tail
  `dead_slot_reap ... pending=2111595 ... tcp_state=Closed active=false
  can_send=false`.
- Re-grounding result: in smoltcp `0.10.0`, `can_send()` is false for `Closed`
  because `may_send()` only permits `Established` and `CloseWait`. A grace in
  this branch cannot flush userspace pending into the local TCP socket.
- Correct behavior: do not weaken knife14u's immediate reap for
  `Closed && !can_send` pending. Attribute earlier pressure instead, especially
  TUN/qdisc TX drops that were visible in the VPS report but absent from the
  per-probe summary.
- Command pitfall encountered: `cargo test` accepts one filter; running
  `cargo test --lib test_a test_b` fails with "unexpected argument". Use a
  shared substring such as `cargo test --lib reap_predicate` for grouped tests.

## 2026-07-04 — Knife14ai post-restart reverse P1 still reaps closed relay with pending downlink

- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14ai_pool4_after_singbox_restart_usclient_suite_20260704_091224.tar.gz`
- Tested commit: `34d5cb7`
- Precondition: pool=4 startup-only smoke failed before restarting `.33`
  sing-box, then passed immediately after `.33` restart at 09:11:13 CST.
- Symptom: reverse-first P1 completed, but throughput remained bursty and low
  relative to direct baselines: iperf sender 24.8 Mbit/s, receiver 23.6 Mbit/s.
- Important discriminator: attribution reported `local_downlink_backpressure`
  with two pause/resume edges and max pending 2,149,256 bytes. QUIC had no
  loss/congestion/blocked deltas; local write and global_rx pressure counters
  were zero.
- Close-tail signal: handle 1 closed by `dead_slot_reap` with
  `state=Relaying pending=2111595`, `tcp_state=Closed active=false
  can_send=false can_recv=false`, no send-slice errors, and no TUN flush
  failures. Standard P1/full sweep were skipped because no fresh quiet metrics
  tick arrived within the short gate.
- Correct behavior: do not continue stale TCP pool slot diagnosis and do not
  change iperf3. The next repair branch should target downlink pending lifecycle
  and TUN local delivery/backpressure for inactive or closed TCP handles.

## 2026-07-03 — Knife14ai pool=4 failed before tunnel iperf at TUIC auth finish

- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14ai_pool4_usclient_suite_20260703_234304.tar.gz`
- Tested commit: `34d5cb7`
- Symptom: the pool=4 experiment failed during `client-tun` startup with
  `tuic auth finish: sending stopped by peer: error 0`. The suite never reached
  tunnel iperf.
- Important discriminator: `.77` iperf3 stayed active and completed the
  direct/exit-target preflights at roughly 285-309 Mbit/s during the same run.
  The preceding pool=1 control also completed tunnel iperf, so credentials and
  the basic TUIC path were not globally broken.
- Exit-side signal: `.33` sing-box was systemd-active. Its log window showed
  pool=1 TUIC inbound/direct outbound opens at 23:42:07 CST and one stream
  cancel at 23:42:38 CST, but no corresponding pool=4 TUIC inbound line around
  the 23:43 startup failure.
- Correct behavior: do not adjust iperf3 or change Rust data-plane throughput
  code from this failure. First validate `MINI_VPN_TUIC_TCP_POOL>1` with a
  startup-only smoke and collect `.33` TUIC logs; if auth failure persists,
  treat sing-box service state or multi-connection startup behavior as the
  next hypothesis before running another expensive throughput suite.

## 2026-07-03 — Knife14ah reverse-first backpressured target sender through TUIC

- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14ah_usclient_suite_20260703_231523.tar.gz`
- Tested commit: `4282682`
- Symptom: reverse-first P1 over the tunnel exited 0 but delivered only
  88.2 KBytes / 24.1 Kbit/s to the receiver. The suite skipped standard P1 and
  full sweep because one relay stayed active after the quiet wait.
- Important discriminator: client-side relay diagnostics showed no local write
  pressure, no global_rx pressure, no downlink backpressure, no QUIC
  loss/congestion or blocked-frame deltas, and no stale-pool reconnects.
  The relay accepted 186,936 remote bytes total into smoltcp with `pending=0`
  and zero TUN flush failures.
- Late signal: 90,312 bytes arrived before local `Finish`; another 96,624
  bytes arrived after local `Finish`, then the relay closed by
  `half_closed_idle_timeout`.
- Server-side signal: `.77` iperf3 journal showed the reverse sender only sent
  3.00 MBytes in the first second, then 29 seconds of 0 bytes with cwnd about
  432 KBytes. `.33` sing-box INFO logs showed TUIC inbound and direct outbound
  opens to `.77:5201` at the run time, but no close/error detail.
- Correct behavior: do not tune client local downlink queues, socket buffers,
  stale slot handling, or congestion control from this result. First add or
  collect diagnostics that distinguish sing-box/TUIC server-side stream
  flow-control/write pressure, target TCP receive-window behavior, and TUIC
  Connect half-close semantics.

## 2026-07-03 — Full `cargo test` can fail on local QUIC endpoint bind

- Stage: Knife14ah local verification.
- Symptom: full `cargo test` passed 251 tests but failed
  `quic::tests::client_endpoint_binds` and
  `quic::tests::client_endpoint_binds_with_each_cc`.
- Important discriminator: `src/quic.rs` had no diff in this stage, and focused
  relay/parser tests passed. The failing tests exercise `client_endpoint`, which
  binds `0.0.0.0:0` and creates a Quinn endpoint through the local async runtime.
- Correct behavior: treat these two tests as local endpoint-bind environment
  checks, not evidence about relay late-remote diagnostics. For scoped data-plane
  observability stages, report the full-test residual risk and rely on focused
  Rust tests plus VPS acceptance for QUIC path behavior unless the stage touches
  `src/quic.rs`.

## 2026-07-03 — Knife14ag reverse-first failed with no pressure signal

- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14ag_usclient_suite_20260703_225103.tar.gz`
- Tested commit: `fbe030c`
- Symptom: reverse-first P1 over the tunnel exited 0 but delivered
  0 receiver bytes. The suite then skipped standard P1/full sweep because the
  tunnel still had one active relay after the quiet wait.
- Important discriminator: the attribution summary reported no local write
  pressure, no global_rx pressure, no downlink backpressure, no QUIC
  loss/congestion delta, no blocked-frame delta, and no stale reconnects.
  During the iperf window the data relay had `remote_to_global_rx_bytes=0`.
- Late signal: after iperf finished and `tcp-relay-write-half-closed
  reason=local_finish` fired, the same relay later read 43,772 remote bytes and
  closed by `half_closed_idle_timeout`.
- Correct behavior: do not continue stale-slot diagnosis and do not tune local
  pressure/backpressure or QUIC congestion parameters from this result. First
  add/collect diagnostics that distinguish TUIC control vs data stream timing,
  server-side target connect/write timing, and late remote bytes after local
  half-close.

## 2026-07-03 — Source `.evn` silently, never echo secret exports

- Stage: Knife14ag VPS suite launch after the user created `.evn` in `.27`'s
  project root.
- Symptom: sourcing `.evn` in a noninteractive command produced export output,
  which exposed secret values in command output.
- Correct behavior: when loading project-local env files on VPS hosts, use
  `set -a && . ./.evn >/dev/null && set +a` and only run secret-safe
  presence/length checks. Do not print env-file output, TUIC UUIDs, passwords,
  private keys, or raw env dumps into reports, learnings, or final summaries.

## 2026-07-03 — Run `.27` suite as root when `sudo -v` cannot prompt

- Stage: Knife14ag VPS suite launch from noninteractive SSH.
- Symptom: invoking the suite as `ubuntu` failed early at `sudo -v` because
  the session had no TTY/password prompt, even though `sudo -n true` was
  available.
- Correct behavior: for noninteractive agent-launched `.27` suites, load the
  env file silently and invoke the suite itself with
  `sudo -n -E env ... bash scripts/knife14b-usclient-tunnel-suite.sh`. That
  makes the suite's internal `sudo -v` run as root and keeps env propagation
  explicit.

## 2026-07-03 — Noninteractive Client VPS SSH does not carry TUIC env

- Stage: Knife14ag VPS acceptance attempt after pushing `4c62d74`.
- Preflight result: `.33` sing-box and `.77` iperf3 were active; `.27` was
  fast-forwarded to `4c62d74`; `bash scripts/knife14b-lowrtt-probe.sh
  --self-test` passed on `.27`.
- Blocker: noninteractive SSH, `bash -lc`, and `sudo -E` on `.27` all reported
  `MINI_VPN_TUIC_SERVER`, `MINI_VPN_TUIC_UUID`, `MINI_VPN_TUIC_PASSWORD`,
  `MINI_VPN_TUIC_SNI`, `MINI_VPN_TUIC_CA_PATH`, and `MINI_VPN_TUIC_ALPN` as
  missing. A file-name-only search found no common `.env`/`*env*` candidate.
- Correct behavior: before launching an expensive VPS suite from a fresh SSH
  session, verify TUIC env presence without printing values. If missing, ask for
  an env file path or have the user run/export the credentials from a shell that
  already has them; do not inspect shell history or reports for secrets.

## 2026-07-03 — Shell parser tests must cover Bash and BSD awk strictness

- Stage: Knife14ag local verification for low-RTT probe attribution summaries.
- Symptoms:
  - `bash -n scripts/knife14b-lowrtt-probe.sh` failed when self-test used one
    multi-line `case` pattern with adjacent `*pattern*` fragments.
  - `bash scripts/knife14b-lowrtt-probe.sh --self-test` failed on macOS awk with
    `illegal primary in regular expression +local_write_pressure+`.
  - A later self-test printed success but exited non-zero because an `EXIT` trap
    referenced a local `tmpdir` after the function returned under `set -u`.
- Root causes:
  - Bash `case` patterns cannot be composed that way across lines.
  - awk string `!~` and `split(..., "+")` treat the right side/separator as a
    regex; bare or leading `+` is not portable.
  - `EXIT` traps run after local variables leave scope.
- Correct behavior: use explicit `assert_contains` checks, compare de-duplicated
  values with split/string loops rather than regex membership, use `[+]` for a
  literal plus separator, and either expand temp paths into traps or clear traps
  before returning.

## 2026-07-03 — TUN discovery awk regex must not double-escape `/`

- Symptom: `knife14af2` printed `awk: syntax error` while discovering `tun0`,
  but continued through the fallback path and completed the suite.
- Root cause: an awk program inside single quotes used `\\//`; awk received an
  extra backslash before `/`.
- Correct behavior: use `\//` inside the single-quoted awk regex, matching the
  earlier stale-TUN cleanup code.

## 2026-07-03 — VPS `/tmp` mode can break acceptance before code runs

- Symptom: the first `knife14af` attempt on `.27` failed before build/suite with
  `mkdir: cannot create directory '/tmp': Permission denied`.
- Root cause: `.27` had `/tmp` as `drwx------ root root` instead of the standard
  sticky world-writable mode.
- Correct behavior: repair the host with `sudo chmod 1777 /tmp`, then create and
  chown the suite output directory before rerunning.

## 2026-07-03 — Use cargo fmt for file-level Rust formatting in this 2024-edition repo

- Symptom: running bare `rustfmt src/tuic.rs` failed with Rust 2015 parsing
  errors (`async fn` not permitted, let chains require Rust 2024).
- Root cause: bare `rustfmt` did not pick up the crate's `edition = "2024"`.
- Correct behavior: use `cargo fmt -- <path>` for targeted formatting, or expect
  `cargo fmt --check` to report historical repository-wide formatting noise.

## 2026-07-03 — A TCP pool slot can be stale before `close_reason` is observed

- Log bundle: `/tmp/mini_vpn/mvpn_knife14ae3_usclient_suite_20260703_212412.tar.gz`
- Tested commit: `ebd3567`
- Symptom: the final full reverse P1 opened TUIC TCP on `conn=1`, then the
  connection immediately reported `closed=TimedOut`; iperf reverse transferred
  0 bytes and exited after the 50s wrapper timeout.
- Rejected assumption: `close_reason().is_none()` before `open_bi` is enough to
  prove a pooled QUIC connection is safe for a new TCP relay.
- Correct behavior: track TCP pool slot freshness and reconnect stale slots
  before opening a new TUIC Connect stream, so a relay does not inherit a
  connection that only reveals timeout after the stream is handed to the pump.

## 2026-07-03 — Stale pool reconnect must be idle-only

- Symptom avoided during code review: a simple stale timeout on a shared QUIC
  pool slot would close the whole connection even if another TCP relay on that
  slot was still active, breaking long-lived or concurrent TCP flows.
- Correct behavior: only reconnect a stale non-primary pool slot when its active
  or opening relay count is zero; keep a CAS-reserved lease from slot selection
  through returned stream drop, and let only the exclusive idle owner reconnect.

## 2026-07-03 — Active sing-box can still fail TUIC auth until restarted

- Log bundle: `/tmp/mini_vpn/mvpn_knife14ae2_usclient_suite_20260703_212154.tar.gz`
- Tested commit: `ebd3567`
- Symptom: `.33` sing-box was `active`, config credentials matched the client,
  and direct `.33 <-> .77` iperf was healthy, but mini_vpn startup failed at
  `tuic auth finish: sending stopped by peer: error 0`.
- Observed fix: restarting `sing-box` on `.33` made the next run authenticate
  and proceed into tunnel throughput testing.
- Correct behavior: when TUIC auth fails despite matching config, treat VPS
  service state as a first-class branch; collect `.33` logs/status and restart
  the service before changing mini_vpn protocol code.

## 2026-07-03 — Exit SSH preflight must not depend on root known_hosts

- Log bundle: `/tmp/mini_vpn/mvpn_knife14ae_usclient_suite_20260703_211833.tar.gz`
- Tested commit: `70e59a0`
- Symptom: `EXIT_TO_TARGET_IPERF_CHECK=1` failed before tunnel testing with
  `Host key verification failed` while SSHing from `.27` to `.33`.
- Root cause: the suite runs under `sudo -E`, so the SSH command used root's
  host-key store rather than `ubuntu`'s interactive `known_hosts`; with
  `BatchMode=yes`, first-use host-key confirmation cannot be answered.
- Correct behavior: Exit SSH preflight should be explicitly noninteractive by
  default, using a suite-local known-hosts file and `StrictHostKeyChecking`
  policy that accepts new hosts while still failing on changed host keys.

## 2026-07-03 — Client↔Target direct baseline is insufficient for tunnel reverse

- Log bundle: `/tmp/mini_vpn/mvpn_knife14ad_usclient_suite_20260703_183503.tar.gz`
- Tested commit: `33659c9`
- Symptom: direct `.77 -> .27` reverse reached 268 Mbit/s, but tunnel reverse
  stayed around 15-22 Mbit/s. The new diagnostics showed reverse data did reach
  the client (`remote_to_global_rx_bytes` grew to tens of MB), so the failure was
  no longer the old "no server bytes at all" branch.
- Rejected assumption: a healthy Client↔Target direct reverse baseline proves
  the reverse path used by the tunnel is healthy.
- Correct behavior: also measure Exit↔Target from `.33`, especially `.77 -> .33`
  via `iperf3 -R`, because tunnel reverse is Target→Exit→TUIC→Client. Without
  this baseline, server/path attribution and client data-plane attribution are
  mixed.

## 2026-07-03 — `cargo fmt --check` is noisy on the current historical tree

- Stage: Knife14ad local verification after adding stream-level diagnostics.
- Symptom: `cargo fmt --check` failed with a huge rustfmt diff spanning many
  pre-existing files and hunks outside the current change. No files were changed
  because it was a check-only command.
- Rejected assumption: a failing full-repo fmt check necessarily means the
  current small patch should run `cargo fmt` and accept the resulting churn.
- Correct behavior: for scoped acceptance/debugging stages, use
  `git diff --check`, focused tests, full `cargo test`, and `cargo clippy
  --all-targets -- -D warnings`. Only run/apply broad formatting when the stage
  explicitly owns formatting cleanup.

## 2026-07-03 — Knife14ab disproved socket buffer as sufficient reverse fix

- Log bundle: `/tmp/mini_vpn/mvpn_knife14ab_usclient_suite_20260703_172934.tar.gz`
- Tested commit: `3af4f7c`
- Symptom: 1MiB smoltcp TCP socket buffers were confirmed in the startup log and
  forward throughput improved, but tunnel reverse remained only 17-32 Mbit/s
  while direct reverse was 281 Mbit/s.
- Important discriminator: client-side `send_slice_zero`/`send_slice_errors`
  stayed zero, and reverse throughput was bursty with long zero-throughput
  windows. This is not the same as the pre-knife14ab 64KiB local socket-window
  hypothesis.
- Rejected assumption: after raising smoltcp socket buffers, any remaining
  reverse failure should be fixed by increasing the same buffers again.
- Correct behavior: run a fresh reverse-only P1 probe before any forward pressure
  and test with an explicit TUIC TCP connection pool. If fresh reverse is still
  low, collect `.33`/sing-box or path-level evidence for server-side/downlink
  QUIC behavior.

## 2026-07-03 — Knife14aa showed healthy direct reverse but tiny tunnel reverse

- Log bundle: `/tmp/mini_vpn/mvpn_knife14aa_usclient_suite_20260703_162932.tar.gz`
- Tested commit: `7621bc6`
- Symptom: direct `.77:5201 -R` reached 299 Mbit/s receiver, but tunnel reverse
  stayed at 8.25 Mbit/s standalone P1 and 18-27 Mbit/s in the full sweep.
- Important discriminator: `.77` iperf service and the direct reverse route were
  healthy; QUIC `tx_blocked` stayed zero; client logs showed
  `tcp-downlink-backpressure` and pending downlink at close.
- Rejected assumption: reverse tunnel collapse can still be blamed on `.77`
  service health after direct `iperf3 -R` passes.
- Correct behavior: test whether the 65,535-byte smoltcp TCP tx buffer is the
  local downlink window bottleneck by making socket rx/tx buffers configurable
  and running the high-throughput suite with larger explicit values.
- Future debugging rule: for reverse/downlink bottlenecks, always compare direct
  reverse baseline with tunnel reverse and inspect local socket/window sizing
  before changing QUIC CC or relay FIN logic again.

## 2026-07-03 — Knife14z reverse results lacked a direct reverse baseline

- Log bundle: `/tmp/mvpn_knife14z_usclient_suite_20260703_154429.tar.gz`
- Tested commit: `1b6f2d8`
- Symptom: Cubic and BBR both showed Kbit/s-scale reverse tunnel results, while
  the suite only proved direct forward `.77:5201` iperf was healthy.
- Important discriminator: BBR dramatically improved forward P1/P2/P4, so the
  remaining reverse failure cannot be explained by "Cubic only" or by the CC
  sweep wrapper itself.
- Rejected assumption: a healthy direct forward iperf preflight is enough to
  attribute reverse tunnel collapse to relay/TUIC code.
- Correct behavior: measure direct `iperf3 -R` before the target route is moved
  into the TUN. If the command fails, stop or explicitly mark reverse tunnel
  results as degraded evidence.
- Future debugging rule: every reverse tunnel acceptance bundle needs a direct
  reverse baseline recorded before the tunnel starts.

## 2026-07-03 — Local CC sweep smoke stops at the host guard on macOS

- Command pattern:
  `CC_SWEEP="cubic bbr" OUT_DIR=/tmp/mvpn_cc_sweep_smoke* SUITE_TAG=knife14z_smoke CHECK_VPS_SERVICES=0 BUILD_RELEASE=0 bash scripts/knife14b-usclient-tunnel-suite.sh`
- Symptom: the parent wrapper correctly dispatched the `cubic` child suite, but
  the child failed at `此脚本面向 Ubuntu/Linux Client VPS。当前内核: Darwin`.
- Rejected assumption: a developer-machine smoke run can validate the later
  Ubuntu-only TUIC env checks.
- Correct behavior: on macOS, use the smoke only to validate `CC_SWEEP`
  dispatch, child artifact naming, and parent bundle creation. The missing-env,
  sudo, build, VPS service, and tunnel startup checks must be trusted to the
  Ubuntu/VPS run.

## 2026-07-03 — ee2e83a proved client window tuning alone is insufficient

- Log bundle: `/tmp/mvpn_knife14x_usclient_suite_20260703_120013.tar.gz`
- Tested commit: `ee2e83a`
- Symptom: direct `.77` iperf receiver was healthy at about 285 Mbit/s, but
  tunnel forward remained Kbit/s-to-low-Mbit/s and reverse remained
  Kbit/s-scale or timed out.
- Important discriminator: the startup log confirmed the new client windows
  (`stream_rx=8388608B`, `conn_rx=33554432B`, `send=33554432B`), yet
  `tcp-local-write-pressure` still appeared 104 times with max wait about
  37.8s. `global_rx_pressure_events` stayed zero.
- Rejected assumption: making the client QUIC windows explicit is enough to
  resolve the long `write_all` stalls.
- Correct behavior: add QUIC connection stats around the stall so the next run
  can attribute it to peer flow-control, congestion/path loss, or server/egress
  behavior.
- Future debugging rule: after a client-window change is confirmed in the live
  log, do not repeat that same tuning loop. Instrument blocked frames and path
  stats before deciding between server config, BBR/Cubic A/B, or VPS path tests.

## 2026-07-03 — 95bfe47 exposed long TUIC stream write waits after coalescing

- Log bundle: `/tmp/mvpn_knife14w2_usclient_suite_20260703_112033.tar.gz`
- Tested commit: `95bfe47`
- Symptom: direct `.77` iperf receiver was healthy at about 281 Mbit/s, but
  tunnel forward P1 was 2.83 Mbit/s receiver and reverse P1 was 314 Kbit/s
  receiver. Reverse P2/P4/P8 later timed out.
- Important discriminator: `global_rx_pressure_events=0`; the main loop was
  mostly parked; forward writer payloads were often 64KiB-class; and
  `tcp-local-write-pressure` appeared 110 times with waits up to multi-second
  and tens-of-seconds ranges.
- Rejected assumption: after writer-side coalescing, remaining low throughput is
  still caused by too many MSS-sized `write_all` calls.
- Correct behavior: make client QUIC stream/data/send windows explicit and log
  them. If pressure remains with larger client windows, investigate peer/server
  flow-control, congestion controller choice, and path loss.
- Future debugging rule: when coalescing proves batching is active, do not keep
  tuning relay batching blindly. Follow the pressure signal into QUIC transport
  windows and congestion-control evidence.

## 2026-07-03 — 1144fc2 suite failed because root PATH missed cargo

- Log bundle: `/tmp/mvpn_knife14w_usclient_suite_20260703_111144.tar.gz`
- Tested commit: `1144fc2`
- Symptom: suite stopped at `BUILD_RELEASE=1 但 cargo 不可用。` before VPS
  service preflight, tunnel startup, or iperf traffic.
- Important discriminator: the archive contained only the markdown report; there
  was no accept log evidence to inspect for data-plane behavior.
- Rejected assumption: because the checkout is clean and `BUILD_RELEASE=1` is
  set, root can necessarily run `cargo` through `PATH`.
- Correct behavior: resolve cargo from `CARGO`, `PATH`, `$HOME/.cargo/bin`,
  `/home/ubuntu/.cargo/bin`, or `/root/.cargo/bin`, then record the chosen path
  and version in the report.
- Future debugging rule: if a suite fails before `.33/.77` preflight, fix the
  harness/environment first and do not spend analysis budget on TUIC, QUIC,
  smoltcp, MTU, or concurrency branches.

## 2026-07-03 — c90471c exposed relay-writer small-write amplification

- Log bundle: `/tmp/mvpn_knife14v_usclient_suite_20260703_103350.tar.gz`
- Tested commit: `c90471c`
- Symptom: reverse TCP was healthy at roughly 153-183 Mbit/s receiver, but
  forward TCP stayed near 1-3 Mbit/s receiver with long zero-bps gaps.
- Important discriminator: `remote_write_timeout` was absent; forward P1 close
  diagnostics showed roughly 10.5 MB upstream across 8529 writer calls, so the
  path was dominated by many 1160-byte class `write_all` operations.
- Rejected assumption: once established-uplink reads are batched in the main
  loop, forward throughput is no longer limited by per-payload scheduling.
- Correct behavior: coalesce already queued relay `Data` commands into bounded
  writer batches, preserve `Finish` order, and log local write wait pressure on
  relay close.
- Future debugging rule: when forward throughput shows burst-then-silence
  windows without write timeouts, compare `uplink_bytes` with `uplink_writes`
  before changing TUIC pool, MTU, congestion control, or VPS service settings.

## 2026-07-02 — a57873a exposed premature local Finish on reverse traffic

- Log bundle: `/tmp/mvpn_knife14c_usclient_suite_20260702_223847.tar.gz`
- Tested commit: `a57873a`
- Symptom: forward P1 recovered, but reverse P1 stayed at Kbit/s scale and
  reverse P2/P4/P8 timed out or transferred zero.
- Important discriminator: the only `dead_slot_reap pending>0` was
  `tcp_state=Closed active=false can_send=false`, so the knife14u pending-reap
  branch was not the reverse bottleneck.
- Rejected assumption: once smoltcp reports local `CloseWait`, immediately
  sending upstream `Finish` is harmless for reverse traffic.
- Correct behavior: defer `Finish` while remote-to-local data is making progress;
  send it only after a short remote-quiet window, then let the existing
  half-closed idle timeout bound cleanup.
- Future debugging rule: when reverse traffic collapses after a tiny burst, look
  for early local FIN/half-close propagation before changing QUIC flow-control or
  buffer sizes.

## 2026-07-02 — Knife14t showed generic pending grace can preserve dead local tails

- Log bundle: `/tmp/mvpn_knife14t_usclient_suite_20260702_215901.tar.gz`
- Tested commit: `51072c8`
- Symptom: VPS preflight was healthy, but tunnel throughput regressed badly and
  reverse tests showed Kbit/s-scale windows/timeouts. `dead_slot_reap pending>0`
  remained, with smaller pending values than knife14s.
- Rejected assumption: if inactive `downlink_pending` had recent progress, it is
  always worth preserving for the full grace window.
- Correct behavior: split inactive pending by local send capability. If
  `TcpSocket::can_send()` is false while the socket is inactive, the pending
  bytes are no longer deliverable and should be reaped immediately; if it is
  true, keep the existing bounded progress-sensitive grace.
- Future debugging rule: pending-byte lifecycle logs must include enough local
  socket state (`tcp_state`, `active`, `can_send`) to tell whether a close
  dropped useful tail bytes or correctly discarded undeliverable bytes.

## 2026-07-02 — Full-repo cargo fmt creates unrelated churn in this workspace

- Symptom: running `cargo fmt` during knife14u rewrote many unrelated source
  files and inflated the diff far beyond the data-plane change.
- Correct behavior: avoid full-repo formatting unless the stage explicitly owns
  that cleanup. For scoped fixes in this repository, keep existing local style
  and rely on tests, clippy, and `git diff --check` unless formatting is needed
  for the touched hunk.
- Future debugging rule: inspect `git diff --stat` after any formatting command
  before continuing; revert accidental churn immediately.

## 2026-07-02 — Knife14s exposed a non-deferred pending downlink reap hole

- Log bundle: `/tmp/mvpn_knife14s_usclient_suite_20260702_180337.tar.gz`
- Tested commit reported by the suite: `6ff49cf`
- Symptom: client diagnostics still showed `tcp-handle-close ... reason=dead_slot_reap ...
  pending>0`, including `state=Relaying pending=1348494`,
  `state=Relaying pending=1307798`, `state=Relaying pending=1258063`, and
  smaller `state=Closing pending>0` cases.
- Important discriminator: those lines had `send_slice_errors=0` and
  `tun_flush_tx_failures=0`; the backlog was not being cleared by a local send
  failure path.
- Rejected assumption: only `pending_relay_close` needs progress-sensitive grace.
- Correct behavior: non-empty `downlink_pending` should have generic progress
  metadata and should be reapable only after no observation/accepted-byte
  progress for the grace window when the socket is inactive.
- Future debugging rule: whenever logs show `pending>0` at close, split the
  branch by lifecycle state (`Relaying`, `Closing`, `CloseWait`, `Closed`) before
  assuming one terminal-event guard covers them all.

## 2026-07-02 — Knife14r exposed one-payload-per-tick uplink throttling

- Log bundle: `/tmp/mvpn_knife14r_usclient_suite_20260702_171341.tar.gz`
- Symptom: forward P1/P4/P8 stayed around low Mbit/s with long zero-bps gaps,
  and P2 timed out. `remote_write_timeout` was absent, so this was not the old
  QUIC write-deadline failure.
- Root cause in code: established uplink drained at most one smoltcp payload per
  dirty pass. Without continuous inbound wakeups, the 5ms timer became the
  effective throughput limiter.
- Correct behavior: drain a bounded batch per dirty pass, but reserve relay mpsc
  capacity before reading each payload so channel fullness still applies TCP
  backpressure.
- Future debugging rule: when measured throughput is suspiciously close to
  `MSS * timer frequency`, inspect event-loop batching before touching QUIC,
  congestion control, or VPS tuning.

## 2026-07-02 — Knife14r report could not prove the exact binary commit

- Log bundle: `/tmp/mvpn_knife14r_usclient_suite_20260702_171341.tar.gz`
- Symptom: the report showed `SUITE_TAG=knife14r`, but did not include
  `git rev-parse` output or binary checksum. The script also only built the
  release binary if it was missing.
- Correct behavior: test suites should record git commit, worktree status,
  binary path, and checksum; `BUILD_RELEASE=1` should rebuild before running.
- Future debugging rule: never analyze a VPS performance run as definitive until
  the report proves the binary came from the intended commit.

## 2026-07-02 — Knife14q showed fixed deferred-close grace still drops useful tail bytes

- Log bundle: `/tmp/mvpn_knife14q_usclient_suite_20260702_160853.tar.gz`
- Symptom: uplink `remote_write_timeout` was gone, but client diagnostics still
  showed `tcp-handle-close ... reason=dead_slot_reap state=Closing pending>0`
  with no corresponding send-slice or TUN flush failure.
- Rejected assumption: "relay close plus pending downlink only needs a fixed
  grace window from close time."
- Correct behavior: if pending downlink is still decreasing, refresh the reap
  deadline; reap only after no drain progress for the grace window.
- Future debugging rule: for pending-buffer lifecycle bugs, track progress
  counters and last-progress time, not just absolute age.

## 2026-07-02 — Knife14p VPS run exposed a wrong per-write timeout assumption

- Log bundle: `/tmp/mvpn_knife14p_usclient_suite_20260702_143215.tar.gz`
- Tested commit: `480c3e8`
- Symptom: throughput collapsed/timeouts while client logs contained
  `remote_write_timeout attempted_bytes=1160 timeout=5s`.
- Rejected assumption: a TUIC/QUIC stream write pending for 5s means the relay is
  broken.
- Correct behavior: pending writes are normal backpressure; keep the writer
  awaitable and let bounded relay-level idle/cleanup paths terminate true stalls.
- Future debugging rule: when a stage adds backpressure, verify which direction
  the evidence points to. `global_rx_pressure_events=0` meant knife14p's
  downlink mechanism was not firing; the next fix belonged in the uplink writer.

## 2026-07-04 — Knife14au repeated the full-repo cargo fmt churn risk

- Symptom: running `cargo fmt` at the repository root reformatted many unrelated
  Rust files and expanded the diff beyond the pending-at-close observability
  slice.
- Fix: reverted the formatter-only churn before committing, then reapplied only
  the `src/client_tun.rs` behavior-neutral logging/test changes.
- Future debugging rule: for scoped Knife14 fixes, do not run full-repo
  formatting unless the stage explicitly owns that cleanup. Prefer existing
  local style plus `cargo test`, clippy, script self-tests, shell syntax checks,
  and `git diff --check`.

## 2026-07-04 — Knife14au per-probe summary missed a post-iperf close-tail

- Log bundle: `/tmp/mini_vpn/mvpn_knife14au_pending_close_taxonomy_usclient_suite_20260704_193706.tar.gz`
- Symptom: full reverse raw log contained `tcp-handle-close pending>0` with
  `close_pending_class=terminal_closed_no_send`, but that probe's attribution
  summary reported `pending_at_close=0`.
- Root cause: `scripts/knife14b-lowrtt-probe.sh` sampled TUN counters and wrote
  metrics/attribution immediately after `iperf3` exited. The close-tail log
  arrived shortly afterward and only appeared in the final post-run metric tail.
- Correct behavior: wait a short, bounded post-iperf settle window before
  per-probe metrics and attribution summaries. Sample final TUN counters after
  that window so tail flush/drop effects stay in the same probe.
- Future debugging rule: if a raw log and a per-probe summary disagree, inspect
  the report timing before changing Rust lifecycle, pacing, TUIC pool, iperf3,
  or sing-box behavior.

## 2026-07-04 — Knife14ba repeated scoped-change cargo fmt churn

- Symptom: running root `cargo fmt` during the scoped diagnostics stage
  reformatted many unrelated Rust files, including REALITY and failover modules,
  and expanded the diff far beyond the intended `client_tun`, `tuic`, and parser
  changes.
- Fix: restored the unrelated formatter-only churn and reapplied the
  `client_tun.rs` relay timing patch as a minimal diff.
- Future debugging rule: for Knife14 scoped changes, do not run root
  `cargo fmt`. If formatting is necessary, format only the touched hunks or
  accept existing local style, then use `git diff --stat` and `git diff --check`
  to catch whitespace issues.

## 2026-07-04 — Knife14ba stream-gap attribution counted idle control streams

- Bundle: `/tmp/mini_vpn/mvpn_knife14ba_stream_timing_usclient_suite_20260704_231537.tar.gz`
- Symptom: clean reverse-first attribution included `tuic_stream_read_gap` and
  `relay_remote_read_gap`, but the >30s gap came from a tiny iperf control
  stream (`rx_bytes=343`). The data stream had `rx_bytes=110790062` and
  `max_read_gap_ms=3763`.
- Root cause: the parser grouped all TUIC TCP streams and relay remote-read
  streams together, so an idle control stream could label the probe as a data
  read-gap failure.
- Correct behavior: split or filter small control streams before assigning
  read-gap attribution. Data-stream read-gap labels should require enough
  received bytes to represent the throughput stream.
- Future debugging rule: if stream timing diagnostics show a large gap, always
  pair the gap with stream byte volume before treating it as the performance
  root.

## 2026-07-04 — Knife14bb fixed control-stream read-gap over-attribution

- Fix: `scripts/knife14b-lowrtt-probe.sh` now keeps all-stream timing maxima in
  the summary but uses data-bearing streams/handles (`>=65536` received bytes)
  for reverse TCP read-gap attribution.
- Regression test: the parser self-test now includes the Knife14ba shape where
  a `343B` control stream has `30147ms` gap and a `110MB` data stream has
  `3763ms` gap. The expected attribution excludes `tuic_stream_read_gap` and
  `relay_remote_read_gap`.
- Future debugging rule: when adding parser labels, include a negative fixture
  for the nearest known false-positive shape before trusting new attribution in
  VPS reports.

## 2026-07-04 — Knife14bc left a parser helper as production dead code

- Symptom: after `from_env` switched to the tx-buffer-aware backpressure parser,
  full `cargo test` emitted a `dead_code` warning for the old fixed-default
  `parse_downlink_backpressure_config` helper. `cargo clippy -D warnings` would
  have failed if this had been left in place.
- Fix: mark the fixed-default helper `#[cfg(test)]`; production now uses the
  tx-buffer-aware parser while legacy default behavior remains covered by
  tests.
- Future debugging rule: after replacing a production config path, check whether
  any helper became test-only and gate it before running clippy.

## 2026-07-04 — Knife14bc VPS run would have been masked by suite env defaults

- Symptom: before running VPS acceptance, inspection showed
  `scripts/knife14b-usclient-tunnel-suite.sh` exported fixed
  `MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=524288` and
  `MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=131072`, which would override the
  Knife14bc adaptive binary defaults.
- Fix: Knife14bd changed the suite defaults to empty `<auto>` values while
  preserving explicit operator overrides.
- Future debugging rule: after a config-default patch, inspect the acceptance
  script's exported env vars before running VPS. A script-level default can
  silently invalidate the code path under test.

## 2026-07-04 — .27 VPS does not have ripgrep installed

- Symptom: `ssh ... 'rg ...'` on `.27` failed with `bash: line 1: rg: command
  not found`.
- Correct behavior: use portable `grep`/`find` commands on VPS hosts unless
  `rg` availability has been checked.

## 2026-07-04 — sudo VPS suites need a real tool TTY from the start

- Symptom: the first Knife14bd suite attempt used `ssh -tt`, but the local
  command was not started with a writable tool TTY. `sudo -v` prompted for a
  password, stdin closed, and the resulting bundle was incomplete.
- Correct behavior: when a `.27` suite may need sudo, start the command with a
  true TTY session at the tool level from the beginning, then enter the password
  only at the sudo prompt. Do not treat any non-TTY sudo-failed bundle as
  acceptance evidence.

## 2026-07-04 — 1MiB adaptive downlink backpressure did not fix Knife14 throughput

- Symptom: Knife14bd clean reverse-first with auto watermarks
  `high=1048576B low=262144B` still achieved only `27.8/26.2 Mbit/s` and ended
  with `terminal_pending_reap=1058416B`.
- Rejected interpretation: the remaining clean root is not the legacy 512KiB
  suite override alone; that was fixed and the binary startup confirmed the
  1MiB watermarks.
- Correct behavior: analyze the failed run before further code changes. The
  next patch/run must distinguish tx-buffer/receive-window capacity from local
  TCP/TUN drain cadence; do not keep changing close-drain, pool, sing-box,
  iperf3, TUN queue length, or egress pacing without new evidence.

## 2026-07-05 — Knife14be repeated the missing tool-level TTY mistake

- Symptom: the first Knife14be suite attempt used `ssh -tt`, but the local tool
  command omitted `tty=true`. The run blocked at `sudo -v`, stdin was closed,
  and the partial bundle
  `/tmp/conn/mvpn_knife14be_tx4m_auto_usclient_suite_20260705_065544.tar.gz`
  is invalid.
- Correct behavior: any `.27` suite that may need sudo must start with both
  remote `ssh -tt` and tool-level `tty=true`. If this is missed, kill the remote
  suite process and discard the partial bundle.

## 2026-07-05 — 4MiB receive-window A/B introduced clean TUN egress drops

- Symptom: Knife14be clean reverse-first with `tx=4194304B` and auto
  backpressure `high=4194304B low=1048576B` achieved only `20.3/18.9 Mbit/s`
  and reported `tun_tx_dropped_delta=27530`.
- Rejected interpretation: larger local TCP tx buffers are not the Knife14 fix.
  The 4MiB run worsened clean throughput and moved the failure to TUN/qdisc
  loss while QUIC remained clean.
- Correct behavior: stop increasing tx buffers. The next repair must first
  analyze and plan local TCP/TUN drain cadence or TUN-drop-aware feedback, then
  add deterministic accounting/parser tests before another VPS run.

## 2026-07-05 — Knife14bf repeated root cargo fmt churn

- Symptom: running root `cargo fmt` during the Knife14bf scoped patch
  reformatted many unrelated Rust files and expanded the diff well beyond the
  intended `client_tun`/parser/docs task.
- Fix: restored the unrelated files and then restored/reapplied
  `src/client_tun.rs` as a minimal hand patch. No root `cargo fmt` output was
  kept in the final diff.
- Correct behavior: do not run root `cargo fmt` in Knife14 scoped stages. If a
  patch needs formatting, keep the local style or format only the precise
  touched hunk, then rely on `cargo test`, script self-tests, and
  `git diff --check`.

## 2026-07-05 — Knife14bf initial VPS run failed at TUIC auth finish

- Symptom: the first `.27` Knife14bf suite on `e661613` failed before data
  plane acceptance with `tuic auth finish: sending stopped by peer: error 0`.
- Follow-up: `.33` sing-box was active and client/server credential hashes
  matched without exposing secrets. Restarting sing-box cleared the startup
  failure; the rerun connected and produced a valid bundle.
- Correct behavior: when TUIC startup fails with `auth finish: sending stopped
  by peer` while service health and credential hashes match, restart sing-box
  once and rerun before invalidating the client code. Do not record the failed
  startup bundle as throughput evidence.

## 2026-07-05 — .27 lacks GitHub deploy-key fetch access

- Symptom: syncing `.27` directly from GitHub over SSH failed because the VPS
  key was not accepted for the repository.
- Workaround: created a local git bundle for the Mac commit range, copied it to
  `.27`, then used `git fetch /tmp/... HEAD` plus `git merge --ff-only
  FETCH_HEAD`.
- Correct behavior: use the bundle sync path for `.27` until repository SSH
  access is deliberately configured on that host. Do not keep retrying
  interactive GitHub authentication from the VPS.

## 2026-07-05 — Knife14bg TUN RX drain default can starve reverse downlink

- Symptom: Knife14bh A/B on `7e33d44` showed
  `MINI_VPN_TUN_RX_DRAIN_BUDGET=8` collapsed clean reverse-first P1 to
  `0.245/0.035 Mbit/s`; data stream first useful RX was delayed about
  `24.5s`, and `tun_rx_drain` consumed `53` TCP packets in the window.
- Rejected interpretation: the bg drain path is not merely a harmless fairness
  helper. It changes the clean reverse-first failure mode and can make the data
  stream appear TUIC-starved.
- Correct behavior: flip the product/suite default drain budget to `0` before
  further lifecycle or receive-window work. Keep the drain path only as an
  explicit env-gated A/B tool until a safer scheduling design exists.

## 2026-07-05 — Root cargo fmt --check is not a Knife14 scoped gate

- Symptom: `cargo fmt --check` reported thousands of repo-wide formatting diffs
  including unrelated files, matching the earlier Knife14bf fmt-churn hazard.
- Correct behavior: do not run or apply root `cargo fmt` in Knife14 scoped
  stages. Use `git diff --check`, focused tests, clippy, and hand-format the
  touched hunks only.

## 2026-07-05 — Knife14bi default VPS run failed before throughput at TUIC startup

- Symptom: the `.27` Knife14bi default suite on `76af8dc` failed before the
  reverse-first probe with
  `tuic auth finish: sending stopped by peer: error 0`.
- Evidence: `.33` sing-box was active, had `NRestarts=0`, passed config check,
  listened on UDP `8443`, used a valid server certificate, and a no-secret exact
  comparison reported matching UUID/password/SNI/ALPN between `.27` env and
  `.33` config.
- Correct behavior: do not treat this bundle as throughput evidence and do not
  change mini_vpn lifecycle/backpressure code for this failure. Restart
  `.33` sing-box once and rerun the identical scoped suite; if it fails again,
  stop for TUIC startup compatibility analysis.

## 2026-07-05 — Nested SSH config comparison must not expose TUIC secrets

- Symptom: a first attempt to summarize `.33` config with remote Python failed
  due escaped newline syntax, and a second nested SSH `jq` comparison failed
  because the remote shell interpreted the unquoted jq filter's pipes.
- Safety catch: an attempted grep that would have printed `uuid` and
  `password` lines was rejected. That rejection was correct.
- Correct behavior: when exact credential comparison is needed, feed a short
  script to remote `python3 -` over SSH stdin and print only boolean
  match/mismatch fields. Never print TUIC credentials or hashes in logs,
  reports, learnings, or summaries.

## 2026-07-05 — Knife14bi default drain-off did not reproduce bh drain0 throughput

- Symptom: after restarting `.33`, the Knife14bi default rerun started
  successfully with `MINI_VPN_TUN_RX_DRAIN_BUDGET=0`, but reverse-first P1
  collapsed to `0.280/0.017 Mbit/s`.
- Rejected interpretation: this is not the old bg TUN RX drain regression and
  not a close-drain/terminal-pending loss point. The run had
  `tun_rx_drain attempts=0`, no downlink backpressure, no TUN drops, no
  terminal pending reap, no send-slice errors, and no QUIC loss/blocking.
- Correct behavior: stop treating drain-off as sufficient proof. Before the
  next VPS run, add or inspect instrumentation that explains why the TUIC data
  stream receives only about `86KB` despite healthy direct baselines and clean
  local egress counters.

## 2026-07-05 — Knife14bj clippy caught a widening formatter argument list

- Symptom: `cargo clippy --lib -- -D warnings` failed after adding TUIC stream
  poll-cadence fields because `format_tuic_tcp_stream_close_line` grew to nine
  positional arguments and triggered `clippy::too_many_arguments`.
- Fix: pass the existing `TuicTcpStreamCloseSnapshot` into the close-line
  formatter instead of extending the positional parameter list.
- Correct behavior: when adding multiple diagnostic counters to an existing
  formatter, prefer a snapshot/struct parameter over more positional arguments,
  then run clippy before committing.

## 2026-07-05 — Sandbox blocks UDP bind for QUIC endpoint tests

- Symptom: sandboxed `cargo test --lib` failed only
  `quic::tests::client_endpoint_binds` and
  `quic::tests::client_endpoint_binds_with_each_cc`; a direct sandboxed
  `python3` UDP bind to `0.0.0.0:0` also failed with `Operation not permitted`.
- Evidence: rerunning `cargo test --lib` outside the sandbox passed all
  `282` lib tests.
- Correct behavior: if QUIC endpoint bind tests fail under the managed sandbox,
  verify with a sandbox-external `cargo test` before changing QUIC endpoint or
  transport code. Treat the sandbox failure as test-environment evidence, not
  a product regression.

## 2026-07-05 — .27 non-login SSH shell does not expose cargo

- Symptom: `ssh ... 'cd /home/ubuntu/mini_vpn && cargo build --release'`
  failed on `.27` with `cargo: command not found`.
- Fix: rerun through a login shell:
  `bash -lc "cd /home/ubuntu/mini_vpn && cargo build --release"`.
- Correct behavior: for `.27` remote builds, use `bash -lc` or explicitly
  source the Rust environment before invoking cargo. Do not treat a non-login
  shell `cargo` miss as a Rust build failure.

## 2026-07-05 — Knife14bj poll diagnostics did not fix reverse starvation

- Symptom: the `4dc79af` scoped reverse-first VPS suite measured only
  `0.210/0.019 Mbit/s` even though `.27 -> .77` and `.33 -> .77` baselines
  were healthy and `.33` reported no sampled `fail auth`.
- Rejected interpretation: this bundle is not evidence for stale pool, TUN RX
  drain, local TUN egress drops, global_rx pressure, downlink backpressure,
  egress pacing, or clean-window QUIC loss/congestion. The data stream was
  periodically polled (`data_poll_gap_max_ms=5000`) while its pending/read gaps
  reached about `20.5s`.
- Correct behavior: before any behavior code patch, add or collect
  server-side/send-side diagnostics that prove whether `.77` sent little data,
  sing-box stopped forwarding, or mini_vpn/quinn did not receive stream-ready
  data. Do not keep patching local lifecycle paths without that evidence.

## 2026-07-05 — Knife14bk acceptance still low, but failure shape changed

- Symptom: the `804eec1` server-evidence suite completed but reverse-first P1
  was still only `18.8/17.0 Mbit/s`.
- Evidence: `.77` journal showed its reverse sender also finished at
  `67.1 MBytes / 18.8 Mbit/s`; `.33` showed TUIC inbound/direct outbound opens
  and no current `fail auth`; mini_vpn showed `tun_tx_dropped_delta=6070`,
  `downlink_backpressure pause_edges=10`, and `active_no_send` close pending.
- Rejected interpretation: this run is not proof that `.77` sent hundreds of
  Mbit/s and mini_vpn silently lost the bytes, and it is not a terminal
  pending-reap loss point.
- Correct behavior: before changing data-plane behavior, propose a small local
  TUN egress/backpressure plan that targets smoltcp send-queue saturation and
  TUN qdisc drops. Also tighten `.33` server-evidence log bounding because the
  current tail-based capture includes unrelated older VLESS noise.

## 2026-07-05 — Root cargo fmt creates unrelated repository-wide churn

- Symptom: running `cargo fmt` at the repository root during Knife14bl rewrote
  many unrelated Rust files, producing a large diff outside the scoped
  downlink/TUN egress task.
- Fix: reverse only the agent-created formatting churn and reapply the scoped
  Knife14bl patch manually.
- Correct behavior: on this branch, do not use root `cargo fmt` as a default
  gate for narrow data-plane patches. Prefer `git diff --check`, focused tests,
  and manual/targeted formatting unless the intended task is repository
  formatting.

## 2026-07-05 — ssh -tt alone is not enough for sudo prompt input through exec

- Symptom: the first Knife14bl suite launch used remote `ssh -tt`, but the local
  exec session did not set `tty=true`; stdin was closed when sudo prompted for
  the `.27` password.
- Fix: terminate that hung SSH process and rerun the identical command with
  `tty=true`, then enter the password only at the sudo prompt.
- Correct behavior: whenever a `.27` suite may need `sudo -v`, set both remote
  `ssh -tt` and local exec `tty=true` from the beginning. Do not rely on
  `ssh -tt` alone.

## 2026-07-05 — Knife14bm failed in a non-comparable no-data-stream shape

- Symptom: the `3d06bea` recent-pressure VPS run measured only
  `1.19/0.00 Mbit/s`; `.77` sender stopped after `4.25 MiB`, mini_vpn data
  stream first RX arrived after about `37.6s`, and the clean window had no TUN
  drops, downlink backpressure, QUIC loss, or terminal pending.
- Rejected interpretation: this is not evidence that the recent-pressure latch
  made TUN egress worse, and it is not a valid replay of Knife14bl's
  `130 Mbit/s` tail-collapse with sampled TUN drops.
- Correct behavior: after a repair run changes failure shape this sharply,
  stop before new behavior-code edits. Record the result, compare same-window
  behavior or tighten attribution, then proceed only after confirming the next
  plan.

## 2026-07-05 — raw SHA bundle creation can produce an empty bundle

- Symptom: `git bundle create /tmp/... 09bb67c` and the same command for
  `3d06bea` failed with `fatal: Refusing to create empty bundle`.
- Cause: a raw commit SHA is not a bundle ref by itself for this usage.
- Correct behavior: create a bundle from a real ref such as `HEAD` when the
  target commits are reachable, then fetch that bundle on `.27` and switch to
  the desired commit SHA locally.

## 2026-07-05 — .27 suite commands must source .env explicitly

- Symptom: the first Knife14bn `09bb67c` suite failed before throughput because
  `MINI_VPN_TUIC_*` env vars were missing.
- Cause: the remote command did not source `/home/ubuntu/mini_vpn/.env`; an SSH
  login shell did not export those variables automatically.
- Correct behavior: when running the suite manually from `.27`, start from the
  repo root and run `set -a; . ./.env; set +a` before invoking the suite. Treat
  missing TUIC env bundles as invalid pre-throughput artifacts.

## 2026-07-05 — cargo fmt --check is still a noisy gate on this branch

- Symptom: Knife14bo `cargo fmt --check` failed with repository-wide formatting
  diffs in unrelated Rust files, including files outside the scoped
  close-drain change.
- Cause: the current branch still contains historical non-rustfmt formatting,
  so even check-only formatting is not a useful narrow-stage gate.
- Correct behavior: keep using `git diff --check`, focused tests, parser
  self-tests, and sandbox-external `cargo test --lib` for this Knife14 branch.
  Do not run or apply root formatting unless repository formatting is the
  explicit task.

## 2026-07-05 — SERVER_EVIDENCE_CHECK needs SSH host envs

- Symptom: the Knife14bo suite was launched with `SERVER_EVIDENCE_CHECK=1`, but
  its server-evidence artifact skipped both `.33` sing-box and `.77` iperf3
  collection because `EXIT_SSH_HOST` and `TARGET_SSH_HOST` were unset.
- Fix: manually collected `.33` and `.77` evidence for the probe time window
  after the run.
- Correct behavior: when requesting server evidence on the known VPS topology,
  also pass `EXIT_SSH_HOST=ubuntu@43.153.32.33`,
  `TARGET_SSH_HOST=ubuntu@43.130.32.77`, and
  `EXIT_SSH_KEY=/home/ubuntu/.ssh/vpn` / `TARGET_SSH_KEY=/home/ubuntu/.ssh/vpn`
  from `.27`, or teach the suite to default these values for the Knife14
  acceptance hosts.

## 2026-07-05 — Knife14bp evidence run changed to no-data shape

- Symptom: the `d5d8542` evidence-defaults suite had healthy direct baselines
  but reverse-first P1 over mini_vpn measured only `280 Kbit/s` sender and
  `2.64 Kbit/s` receiver.
- Evidence: server evidence was complete. `.33` showed current TUIC
  inbound/direct outbound with no `fail auth`; `.77` iperf3 journal showed the
  target sender itself at `1.00 MBytes / 280 Kbit/s`. mini_vpn had no TUN
  drops, no downlink backpressure, no QUIC loss/congestion, no pending-at-close,
  and no terminal pending reap. Terminal-late payload was visible and bounded:
  `28544B` across `2` events.
- Correct behavior: do not treat this run as a clean tx-queue-only
  receive-window branch and do not make a behavior patch from it alone. First
  repeat in the same evidence mode or add evidence-only stream/window
  instrumentation to distinguish VPS run variance from a deterministic
  stream-readiness or local TCP ACK/window issue.

## 2026-07-06 — Knife14bq repeat changed from no-data to tail-collapse pressure

- Symptom: the same `d5d8542` reverse-first P1 repeat with server evidence and
  `.33 <-> .77` path checks averaged `150/149 Mbit/s`, but the final six
  seconds collapsed to about `15.7-16.8 Mbit/s`.
- Evidence: `.27 <-> .77` and `.33 <-> .77` baselines were healthy, `.33`
  showed current TUIC inbound/direct outbound to `.77:5201` and no current TUIC
  `fail auth`, `.77` sender matched `537 MBytes / 150 Mbit/s`, QUIC
  loss/congestion stayed `0/0`, while mini_vpn recorded
  `tun_tx_dropped_delta=14804`, `downlink_backpressure=693/693`, and
  `terminal_late_remote_payload=1474528B`.
- Correct behavior: do not continue the no-data branch and do not treat the
  average `149 Mbit/s` as final acceptance. The next patch must be
  evidence/TDD-first around local TUN egress pressure, downlink pause/resume
  timing, and close-tail terminal-late correlation before changing behavior.

## 2026-07-06 — throughput-shape tests must match probe direction

- Symptom: during Knife14br TDD, a new `no_data` assertion was first attached to
  an existing late-remote fixture that was not a reverse TCP iperf sample, so the
  parser correctly returned `throughput_shape: shape=unknown`.
- Cause: the new shape classifier is intentionally scoped to reverse TCP probes;
  forward and UDP samples keep attribution labels but do not receive reverse
  throughput-shape semantics.
- Correct behavior: put `no_data`, `tail_collapse`, and `stable_high`
  throughput-shape assertions on reverse TCP fixtures only. Use forward fixtures
  for relay/late-remote behavior, not reverse-throughput acceptance shape.

## 2026-07-06 — Stale downlink backpressure env can bypass auto defaults

- Symptom: Knife14bq was expected to exercise tx-buffer-scaled downlink
  backpressure, but the report and startup log showed
  `MINI_VPN_TCP_TX_BUFFER_BYTES=1048576` with high `524288` and low `131072`.
  The run then produced `downlink_backpressure=693/693`, TUN drops, and tail
  collapse.
- Cause: inherited `.env` or shell variables explicitly set the old
  `524288/131072` pair, so `client_tun.rs` correctly honored explicit config
  instead of applying its `<auto>` scaling.
- Correct behavior: Knife14 acceptance suites must normalize only this legacy
  pair back to `<auto>` under a larger tx buffer unless
  `KNIFE14_KEEP_EXPLICIT_DOWNLINK_BACKPRESSURE=1` is set. Future result
  analysis must check the actual startup high/low line before blaming
  receive-window code.

## 2026-07-06 — TUIC auth finish can fail once despite matching config

- Symptom: the first Knife14bs VPS run on `dd9c6ad` failed during mini_vpn
  startup with `tuic auth finish: sending stopped by peer: error 0`.
- Evidence: `.33` sing-box was active with UDP `:8443` listening, `.27/.33`
  time skew was `0s`, sing-box config check passed, and a manual no-secret
  comparison showed `uuid_match=1`, `password_match=1`, `sni_match=1`, and
  `alpn_match=1`. A no-build retry immediately afterward connected and reached
  the reverse-first probe.
- Correct behavior: if this exact startup failure appears once while no-secret
  config/time/service checks pass, treat it as a startup transient and retry
  once before changing code or restarting sing-box. If repeated, then stop and
  inspect `.33` service/runtime logs before more acceptance runs.

## 2026-07-06 — No-secret TUIC compare error is inconclusive by itself

- Symptom: the first Knife14bt VPS run on `b5752c4` failed with the same
  `tuic auth finish: sending stopped by peer: error 0`, and the generated
  no-secret compare script printed `exit_config_compare_error=1` with
  `CalledProcessError`.
- Evidence: service diagnostics still showed `.27/.33` time skew `0s`, `.33`
  sing-box active, UDP `:8443` listening, and sing-box config check passing. A
  manual rerun of the no-secret compare with explicit SSH env immediately
  returned `uuid_match=1`, `password_match=1`, `sni_match=1`, and
  `alpn_match=1`; the no-build retry then connected.
- Correct behavior: treat `exit_config_compare_error=1` as an inconclusive
  diagnostic failure, not as an auth mismatch. Rerun the no-secret compare with
  explicit `EXIT_SSH_*` env and only blame sing-box/config if the match booleans
  fail or the startup failure repeats after one retry.

## 2026-07-06 — Tx-buffer-scaled high watermark can regress reverse throughput

- Symptom: with suite normalization forcing binary auto defaults,
  `high=1048576B low=262144B`, reverse-first P1 fell to `18.8 Mbit/s` receiver
  with periodic zero-throughput intervals.
- Evidence: direct and exit-target baselines stayed healthy; QUIC
  loss/congestion/blocking stayed zero; global_rx pressure stayed zero; TUN
  drops reduced to `586`, but smoltcp `send_queue` hit `1048576`, stream read
  gaps reached `3782ms`, and target sender cwnd collapsed in the same stop/go
  pattern.
- Correct behavior: do not increase receive-window/high watermark as a default
  throughput fix without TUN/qdisc-capacity evidence. The next patch must make
  local egress pressure durable across poll/flush or explicitly tune high/low
  against measured TUN egress capacity.

## 2026-07-06 - Knife14bu pressure hold failed VPS acceptance

- Stage: Knife14bu durable egress pressure acceptance for commit `fd3f2ed`.
- Failed bundle:
  `/tmp/mini_vpn/knife14bu_durable_20260706/mvpn_knife14bu_durable_usclient_suite_20260706_190244.tar.gz`
- Symptom: reverse-first P1 reached only `17.3/15.3 Mbit/s` with the same
  low-average stop/go profile.
- Important discriminator: the pressure hold eliminated TUN tx drops
  (`tun_tx_dropped_delta=0`) and terminal late payload
  (`terminal_late_remote_payload=0`), but throughput worsened, pause/resume
  churn increased to `63/62`, and `tun_flush_deferred` rose to `62`.
- Close-boundary signal: the data handle closed with `tcp_state=CloseWait`,
  `may_recv=false`, `can_send=true`, `may_send=true`, `send_queue=524288`,
  `pending=0`, and `terminal_pending_reap_bytes=0`.
- Rejected next moves: do not keep extending the pressure hold, changing
  scripts, tuning stale pool, iperf3, sing-box, TUN qlen, or QUIC from this
  evidence.
- Correct behavior: stop before the next behavior edit. First clean the release
  warning, add raw/effective pressure and close-time egress observability, then
  TDD a bounded close/egress-drain rule for send-capable `CloseWait` sockets.

## 2026-07-06 - Release build warnings must be cleaned before VPS evidence

- Symptom: the `.27` release build for Knife14bu succeeded but printed
  `warning: method observe_pressure is never used`.
- Cause: the compatibility wrapper is used only by tests after production code
  moved to the timestamped `observe_pressure_at` path.
- Correct behavior: future VPS-bound commits should leave release builds
  warning-clean. Remove unused wrappers or gate test-only helpers with
  `#[cfg(test)]` before running acceptance, so warning noise does not blur
  operational evidence.

## 2026-07-06 - Repo-wide rustfmt check is not a Knife14 gate

- Symptom: `cargo fmt --check` failed before the Knife14bv-b commit by printing
  a repo-wide formatting diff across existing Rust files, including files
  outside the close-egress lifecycle patch.
- Cause: the repository has pre-existing global rustfmt drift; applying
  `cargo fmt` would create a large unrelated formatting change and obscure the
  small lifecycle patch.
- Correct behavior: do not use repo-wide `cargo fmt --check` as a blocking
  Knife14 gate until formatting is normalized in a dedicated task. For these
  lifecycle patches, keep edits narrow and use `cargo test/build`,
  script self-tests/syntax checks, and `git diff --check`.

## 2026-07-06 - Knife14bv no-data run was not a close-egress candidate

- Symptom: Knife14bv acceptance for `8f68a89` regressed reverse-first P1 to
  `0.315/0.113 Mbit/s` with `throughput_shape=no_data`.
- Evidence: `downlink_backpressure=0/0`, TUN drops `0`, send-slice errors `0`,
  QUIC client-side loss/congestion/blocking `0`, and no
  `tcp-deferred-close-egress` line. The only close-egress accounting was a
  terminal closed/no-send `14824B`, while TUIC stream 4 showed
  `max_read_gap_ms=20567` and only `723424B` total rx.
- Rejected next move: do not extend the close-egress drain grace, tune
  close/reap thresholds, or chase sing-box auth from this evidence. `.33`
  current-window TUIC auth was clean and the bottleneck appears before the
  close boundary.
- Correct behavior: stop before behavior changes. First add diagnostics/TDD
  that distinguish TUIC server-to-client stream starvation from mini_vpn local
  TCP ACK/receive-window collapse, then run one scoped acceptance before
  choosing a behavior patch.

## 2026-07-06 - Soft tx_queue pressure must still feed TUN drop attribution

- Symptom: during Knife14bx local gates, after splitting tx_queue-only
  backpressure to use a hard cap, the focused
  `tun_egress_feedback_pauses_on_drop_delta_with_recent_high_pressure` test
  failed.
- Cause: the first implementation reused the new hard-cap predicate for both
  the 25ms remote-read pressure hold and the recent-pressure sample used by
  TUN drop feedback. That made a real TUN `tx_dropped` sample after soft-high
  pressure look pressure-free.
- Correct behavior: keep these two roles separate. Record recent pressure for
  drop attribution whenever raw pressure reaches the conservative soft high,
  but install the 25ms egress hold only when app pending reaches high or
  tx_queue-only pressure reaches its derived hard cap.

## 2026-07-06 - Knife14bx tx_queue headroom still left flush deferral at soft high

- Symptom: Knife14bx VPS acceptance improved reverse-first P1 to
  `27.4/26.4 Mbit/s`, but it still failed with `throughput_shape=low_average`.
- Evidence: app pending stayed `0`, TUN drops stayed `0`, QUIC
  loss/congestion/blocking deltas stayed `0`, send-slice zero/errors stayed
  `0`, and terminal pending reap stayed `0`. The new tx_queue hard cap was
  active (`tx_queue_pause_high=917504`, `max_tx_queue_bytes=980698`), but
  `tun_flush_deferred` reached `446` while `send_queue_max=917482`.
- Cause: the remote-read backpressure threshold moved to the tx_queue hard cap,
  but immediate downlink flush deferral still used the soft high watermark.
  This left a second local cadence gate at the old `524288` threshold.
- Correct behavior: do not treat this as sing-box, iperf3, stale pool, QUIC, or
  close/reap loss. Before the next behavior edit, propose a TDD patch that keeps
  app pending strict but aligns tx_queue-only egress flush deferral with the
  tx_queue hard cap.

## 2026-07-06 - Hard-cap no-pending flush headroom is too permissive

- Symptom: Knife14by reduced `tun_flush_deferred` from `446` to `25`, but
  reverse-first P1 only improved to `30.0/29.0 Mbit/s` and still failed as
  `throughput_shape=low_average`.
- Evidence: direct `.27/.33/.77` baselines were healthy, `.33` current-window
  TUIC auth was clean, QUIC loss/congestion/blocking deltas were zero, app
  pending and pending-at-close were zero, and terminal pending reap stayed
  zero. The new signal was `tun_tx_dropped_delta=37` while tx_queue pressure
  reached `975399B` against `tx_queue_pause_high=917504B`.
- Cause: aligning no-pending flush deferral with the tx_queue hard cap removed
  one cadence bottleneck but allowed the local TUN/qdisc side to be pushed past
  its clean capacity.
- Correct behavior: do not simply widen immediate flush headroom again and do
  not revert to external-service hypotheses. Before the next behavior edit,
  propose a TDD patch that uses a middle or drop-aware no-pending flush guard:
  above the old soft high, below the hard cap under saturation evidence, while
  preserving strict app-pending semantics and the existing tx_queue read
  headroom.

## 2026-07-06 - Static midpoint no-pending flush threshold was not enough

- Symptom: Knife14bz bounded no-pending flush at `tx_queue_flush_high=720896B`,
  but reverse-first P1 regressed to `20.7/19.5 Mbit/s`.
- Evidence: `tun_flush_deferred=162`, `tun_tx_dropped_delta=340`,
  `max_tx_queue_bytes=983022`, `send_queue_max=917489`, and
  `throughput_shape=low_average`. App pending stayed `0`, pending-at-close and
  terminal pending reap stayed `0`, QUIC loss/congestion/blocking stayed `0`,
  and `.33` current-window TUIC auth was clean.
- Cause: the threshold took effect, but the system can still accept enough
  remote payload in bursts to overshoot the clean local TUN egress capacity
  before the remote-read pause/flush cadence catches up.
- Correct behavior: stop threshold-only tuning. The next behavior patch should
  add TDD for bounded remote payload acceptance by remaining egress headroom,
  with explicit counters for headroom-limited accepts, before another VPS run.

## 2026-07-06 - .27 cannot rely on one-shot SSH git pull

- Symptom: syncing `.27` with `git pull git@github.com:S7245/mini_vpn.git`
  failed with `Permission denied (publickey)` while the local Mac one-shot SSH
  push path worked.
- Cause: `.27` did not have GitHub SSH authentication for that pull path.
- Correct behavior: on `.27`, use its existing HTTPS fetch/tracking state and
  fast-forward from `origin/codex/knife14d-downlink-reap-open`, or configure
  deploy-key access deliberately outside the test path. Do not change the repo
  origin or retry interactive HTTPS pushes during Knife14 acceptance.

## 2026-07-06 - Cargo test accepts one filter per invocation

- Symptom: while running Knife14ca focused tests, commands such as
  `cargo test --lib test_a test_b` failed with
  `unexpected argument 'test_b' found`.
- Cause: `cargo test` accepts at most one test filter before `--`; additional
  positional filters are interpreted as invalid arguments.
- Correct behavior: run exact test filters in separate commands, or use one
  broader substring filter that matches the desired group. Do not combine
  multiple exact test names in one `cargo test` invocation.

## 2026-07-06 - Fixed pre-send headroom caps can starve receive progress

- Symptom: Knife14ca commit `82003b8` capped `send_queue_max` at `720896B`,
  but VPS reverse-first P1 regressed to `16.1/14.8 Mbit/s`.
- Evidence: final close showed `pending=566509`, `may_recv_false=11897`,
  `headroom_limited_calls=12973`, and
  `headroom_deferred_bytes=3317103134` with `tcp_state=CloseWait`,
  `can_send=true`, and `send_queue=720896`.
- Cause: a fixed queue-occupancy cap can become sticky under saturation. It
  prevents overshoot above the cap, but it does not prove the local TUN path is
  actually draining; remote accept becomes threshold-clocked rather than
  egress-progress-clocked.
- Correct behavior: do not repair this by only moving cap values. Before the
  next behavior patch, propose and test an algorithm that resumes remote
  acceptance based on observed local egress progress plus a bounded per-loop
  quantum.

## 2026-07-06 - Low-RTT summary can miss final lifecycle/drop lines

- Symptom: the Knife14ca low-RTT attribution summary reported
  `pending_at_close=0` and `tun_drops=0`, but suite-level lines after that
  summary showed `pending=566509` and TUN egress feedback
  `drop_delta_total=2691`.
- Cause: the parser summary was generated before the final post-P1
  close/drop snapshots were emitted.
- Correct behavior: parse the whole returned bundle, not just the low-RTT
  attribution block, before declaring pending, close, reap, or TUN-drop
  accounting clean. Add parser/self-test coverage for final post-summary
  lifecycle and egress feedback lines before the next expensive VPS run.

## 2026-07-06 - Test-only helpers must be cfg(test) before clippy gates

- Symptom: during Knife14cb local gates, `cargo clippy --all-targets
  --features harness -- -D warnings` failed because
  `bounded_downlink_flush_limit_for_window` became production-dead after the
  egress-clock implementation replaced its runtime use.
- Cause: the helper was still compiled into the library even though only tests
  used it.
- Correct behavior: when a behavior patch replaces a runtime helper but keeps
  it for regression tests, mark that helper `#[cfg(test)]` before running
  clippy with `-D warnings`.

## 2026-07-06 - High throughput can still hide hard-edge TUN drops

- Symptom: Knife14cb commit `5bf60d9` restored reverse-first P1 to
  `152/152 Mbit/s`, but the same bundle still showed
  `tun_tx_dropped_delta=712`, final TUN egress feedback
  `drop_delta_total=712`, and a two-second iperf zero-throughput gap.
- Evidence: close/reap accounting was clean (`pending=0`,
  `final_pending_at_close=0`, `terminal_pending_reap_bytes=0`), QUIC
  loss/congestion/blocking was zero, and current-window `.33` TUIC evidence
  had no `fail auth`.
- Cause: progress-clocked accept fixed receive-window starvation, but credit
  spending can still drive local pressure to the hard tx_queue pause threshold
  (`917504B`) and trigger kernel/TUN egress drops.
- Correct behavior: do not declare Knife14 acceptance from average throughput
  alone. Require final TUN egress/drop summaries to stay clean, and repair this
  as a credit-spend/drop-feedback algorithm issue instead of another static
  threshold tune.

## 2026-07-06 - Knife14cc pool=1 no-data was stream starvation, not hard-edge credit

- Stage: Knife14cc VPS acceptance and repeat for commit `7d1f47f`.
- Failed bundles:
  `/tmp/mini_vpn/knife14cc_hard_edge_guard_20260706_1606/mvpn_knife14cc_hard_edge_guard_usclient_suite_20260707_000531.tar.gz`
  and
  `/tmp/mini_vpn/knife14cc_repeat_20260706_1611/mvpn_knife14cc_repeat_usclient_suite_20260707_001035.tar.gz`.
- Symptom: reverse-first P1 collapsed twice with `throughput_shape=no_data`
  (`0.033 Mbit/s` and `0.034 Mbit/s` receiver) even though direct
  `.27/.33 -> .77` baselines were healthy.
- Important discriminator: TUN drops, QUIC loss/congestion/blocking,
  send-slice errors, pending-at-close, terminal pending reap, and hard-edge
  guard activity were all clean or zero. The repeat run's data stream had
  `first_rx_ms=17880` and only about `258 KiB` received.
- Cause: concurrent TUIC TCP streams on the same QUIC connection can starve the
  reverse data stream under this acceptance shape. Pool=2 A/B changed the
  stream set to `conns=0,1` and restored immediate data delivery.
- Correct behavior: make pool=2 the default isolation layer and keep pool=1 as
  an explicit A/B/regression setting. Do not keep patching local egress, close,
  or reap logic from pool=1 no-data evidence.

## 2026-07-06 - Relay writer flush is not a TUIC causality claim on quinn 0.10

- Stage: Knife14cd relay writer semantic TDD.
- Symptom: a generic `AsyncWrite` test proved a flush-gated stream can withhold
  remote response until `flush()` is called after `write_all`.
- Transport check: `quinn 0.10.2` implements `SendStream::poll_flush` as
  immediate `Poll::Ready(Ok(()))` for both `futures_io` and Tokio
  `AsyncWrite`.
- Correct behavior: keep `writer.flush().await` as a generic AsyncWrite
  semantic safeguard, but do not attribute TUIC VPS throughput changes to it.
  For current TUIC acceptance, use TCP pool isolation and stream metrics as the
  causal evidence.

## 2026-07-06 - Default pool=2 can restore throughput while worsening TUN drops

- Stage: Knife14cd default pool=2 VPS acceptance for commit `f219044`.
- Bundle:
  `/tmp/mini_vpn/knife14cd_default_pool2_20260707_0028/mvpn_knife14cd_default_pool2_usclient_suite_20260707_002800.tar.gz`
- Symptom: reverse-first P1 reached `180/179 Mbit/s` and
  `throughput_shape=stable_high`, but final egress was not clean:
  `tun_tx_dropped_delta=6754`, `drop_events=8`, `max_delta=1723`,
  `final_egress_at_close=892928`, and final send-capable pending `7680B`.
- Important discriminator: data stream starvation was gone (`conns=0,1`,
  `data_first_rx_max_ms=3`, `data_rx_bytes_max=674859752`), QUIC loss and
  congestion were zero, and terminal pending reap stayed zero.
- Cause: restoring data volume exposes the remaining local downlink egress
  limiter. Drain credit and hard-edge guard still allow repeated pressure at
  `892928B`, followed by TUN drop feedback.
- Correct behavior: do not call Knife14 done from throughput alone, and do not
  increase pool size. The next repair should add TDD for drop-aware credit debt
  or credit freeze after TUN egress drops, preserving bounded pending and final
  lifecycle visibility.

## 2026-07-06 - .27 VPS image does not have ripgrep

- Stage: Knife14cd `.27` preflight after deploying `f219044`.
- Symptom: a remote preflight command failed with `bash: rg: command not found`
  after the useful environment checks had already printed.
- Correct behavior: on `.27`, use POSIX tools such as `grep`, `find`, and
  `sed` for remote smoke/preflight checks unless `rg` is deliberately
  installed. Keep using local `rg` on the Mac workspace.

## 2026-07-06 - Exit-to-target preflight needs explicit SSH env when server evidence is off

- Stage: Knife14cj first VPS run.
- Failed bundle:
  `/tmp/mini_vpn/knife14cj_pre_payload_ack_drain_20260707_0229/mvpn_knife14cj_pre_payload_ack_drain_usclient_suite_20260707_022926.tar.gz`
- Symptom: the suite failed before tunnel P1 with
  `EXIT_TO_TARGET_IPERF_CHECK=1 需要设置 EXIT_SSH_HOST`.
- Cause: `EXIT_TO_TARGET_IPERF_CHECK=1` was enabled while `EXIT_SSH_HOST` and
  `TARGET_SSH_HOST` were not passed explicitly; the script did not infer them
  in that configuration.
- Correct behavior: when running `.27` suites with Exit↔Target preflight,
  always pass `EXIT_SSH_HOST=ubuntu@43.153.32.33`,
  `EXIT_SSH_KEY=/home/ubuntu/.ssh/vpn`,
  `TARGET_SSH_HOST=ubuntu@43.130.32.77`, and
  `TARGET_SSH_KEY=/home/ubuntu/.ssh/vpn`.

## 2026-07-06 - Do not use repo-wide cargo fmt as a Knife14 gate

- Stage: Knife14cj local review.
- Symptom: `cargo fmt --check` reported broad rustfmt changes across
  unrelated files, including files not touched by the stage.
- Cause: the current repository is not rustfmt-clean as a whole.
- Correct behavior: avoid running `cargo fmt` for Knife14 hot-path patches
  because it creates unrelated churn. Use `git diff --check`, clippy, focused
  tests, and small manual formatting for touched hunks unless a dedicated
  formatting-only stage is opened.

## 2026-07-06 - MTU-derived pressure ACK drain budget can be too small

- Stage: Knife14cj VPS retry.
- Bundle:
  `/tmp/mini_vpn/knife14cj_pre_payload_ack_drain_retry_20260707_0231/mvpn_knife14cj_pre_payload_ack_drain_retry_usclient_suite_20260707_023111.tar.gz`
- Symptom: pre-payload and maintenance TUN RX drains engaged, but every drain
  exhausted its budget (`attempts=252`, `packets=5292`,
  `budget_exhausted=252`, `would_block=0`) while local TUN egress drops still
  reached `7794`.
- Cause: `guard_bytes / tun_mtu` estimates data-sized packets, but pressure
  TUN RX work is mostly small TCP ACK/window packets.
- Correct behavior: derive pressure-drain packet budget from ACK-sized packets
  with a hard cap, and prove it can reach `would_block` under pressure without
  becoming unconditional TUN RX polling.

## 2026-07-06 - rsync mtime preservation can leave stale cargo artifacts on .27

- Stage: Knife14cm A/B and restore on `.27`.
- Symptom: after restoring the current `src/client_tun.rs`, the remote file
  hash and source content were correct, but `cargo test pressure_credit --lib`
  initially ran the old focused test set because cargo reused prior build
  artifacts.
- Cause: the deployment copy preserved mtimes, so cargo did not see the source
  file as newer than the artifact.
- Correct behavior: when swapping Rust source files on `.27` for A/B or VPS
  validation, force rebuild freshness with `touch src/client_tun.rs`, avoid
  mtime-preserving sync for edited core files, or clean the affected target
  artifact before trusting test counts or release binaries.

## 2026-07-06 - Pressure debt without receive gating is too late to recover

- Stage: Knife14cm valid VPS acceptance.
- Bundle:
  `/tmp/mini_vpn/knife14cm_pressure_credit_edge_valid_20260706_1916/mvpn_knife14cm_pressure_credit_edge_valid_usclient_suite_20260707_031619.tar.gz`
- Symptom: `pressure_credit_edge` debt installed at `pending=9074`, but remote
  payload continued after debt was active, `accepted_bytes=0` accumulated,
  pending grew to `541802`, TUN drops reached `4056`, and
  `pressure_credit_debt_paid_bytes=0`.
- Cause: credit debt limited future extra write credit, but it did not feed
  the downlink receive-window/backpressure decision. The remote reader could
  continue adding useful bytes while local egress was already pinned.
- Correct behavior: the next repair should make active drop/pressure debt part
  of receive gating. Do not keep moving thresholds or only changing debt
  sizing without proving receive pause/resume and debt repayment.

## 2026-07-06 - .27 VPS suite must source repo .env explicitly

- Stage: Knife14cn first VPS launch.
- Failed bundle:
  `/tmp/mini_vpn/knife14cn_debt_receive_gate_20260706_1927/mvpn_knife14c_usclient_suite_20260707_032726.tar.gz`
- Symptom: the suite exited before tunnel startup with missing TUIC variables:
  `MINI_VPN_TUIC_SERVER`, `MINI_VPN_TUIC_UUID`,
  `MINI_VPN_TUIC_PASSWORD`, `MINI_VPN_TUIC_SNI`,
  `MINI_VPN_TUIC_CA_PATH`, and `MINI_VPN_TUIC_ALPN`.
- Cause: the SSH command launched the suite without loading
  `/home/ubuntu/mini_vpn/.env`; the file exists and uses `export KEY=...`
  lines.
- Correct behavior: run `.27` acceptance commands from the repo root with
  `. ./.env` before invoking the suite. Continue redacting values; only inspect
  key names or lengths when diagnosing env loading.

## 2026-07-06 - Debt-coupled receive gate does not restore throughput by itself

- Stage: Knife14cn valid VPS acceptance.
- Bundle:
  `/tmp/mini_vpn/knife14cn_debt_receive_gate_env_20260706_1930/mvpn_knife14c_usclient_suite_20260707_032833.tar.gz`
- Symptom: active debt paused receive at `pending=2035`, but reverse-first P1
  still stayed low at `20.5/19.2 Mbit/s` and TUN egress still dropped at the
  end.
- Cause: the previous pending-growth failure is repaired, but most of the
  30-second window still has long remote-read gaps with pending near zero and
  little TUN RX drain before pressure. The remaining bottleneck is likely
  progress cadence/ACK-window feedback before the credit edge, not more debt
  accounting.
- Correct behavior: the next repair should add a bounded, observable
  active-flow ACK/window drain path before pressure. Do not lower static
  thresholds or keep installing more debt without proving sender progress.

## 2026-07-07 - Single high pool=1 run was not stable evidence

- Stage: Knife14dk/dl/dm discriminator sequence.
- Symptom: one pool=1 run reached `166/165 Mbit/s`, but the same current code
  and default pool=1 later fell to `23.4/22.4 Mbit/s` and then near no-data.
- Cause: the single high run was a transient acceptance sample, not a stable
  causal fix. Pool size alone did not explain the remaining failure.
- Correct behavior: do not change product defaults or acceptance status from a
  single high VPS run. Require repeat evidence and clean parsed surfaces before
  treating a pool/config discriminator as causal.

## 2026-07-07 - Sing-box restart did not restore stable high throughput

- Stage: Knife14dp/dq after `.33` sing-box restart.
- Symptom: `.33` restart succeeded and direct `.33 <-> .77` baselines stayed
  healthy, but d85 reached only `20.8/19.9 Mbit/s` and current code only
  `30.9/29.9 Mbit/s`.
- Cause: the failure is not just stale sing-box service state, target iperf3,
  `.33 -> .77` TCP path, time sync, or TUIC auth. Current evidence points to
  TUIC stream read cadence on the tunnel path.
- Correct behavior: after restart, still parse the mini_vpn bundle before
  editing. If pending/close/reap/TUN/QUIC surfaces are clean and stream gaps
  remain second-scale, move to relay/TUIC read-wakeup tests instead of more
  service restarts.

## 2026-07-07 - No-op waker polling is a risky ready-drain pattern

- Stage: Knife14dq code review after stream-gap evidence.
- Symptom: current code contains `drain_ready_remote_reads`, which polls the
  split relay reader with a no-op waker to pull extra ready chunks after a real
  async read. The failing bundle still shows `tuic_stream_pending` and
  `tuic-tcp-stream-read-gap` in the `1.7-3.4s` range.
- Cause: if a manual ready-drain poll reaches `Pending`, async IO
  implementations may store that no-op waker as the current wake target. The
  relay task can then wait for a timer tick or unrelated event before polling
  again, producing bursty reads even with no local pressure.
- Correct behavior: before the next patch, add a deterministic waker-safety
  test and then either replace the ready-drain helper with a real-waker-safe
  pattern or remove the speculative ready-drain path.

## 2026-07-07 - Relay batch cap without egress credit still overdrives TUN

- Stage: Knife14dx reverse-first VPS acceptance.
- Bundle:
  `/tmp/mini_vpn/knife14dx_relay_credit_p1_30/mvpn_knife14dx_relay_credit_p1_30_usclient_suite_20260707_125810.tar.gz`
- Symptom: reverse-first P1 stayed low at `25.7/24.8 Mbit/s`, with repeated
  zero-throughput intervals.
- Rejected explanations: close/reap/pending accounting was clean, QUIC
  loss/congestion/blocking was clean, `.33` logs showed no current `fail auth`,
  and the relay -> main-loop channel never approached capacity.
- Cause: the relay read-side batch cap was only tied to `mpsc` channel
  occupancy. The actual pressure was downstream in smoltcp/TUN egress:
  `tun_tx_dropped_delta=591`, `send_queue_max=892928`, and
  `headroom_deferred_bytes=28883630`.
- Correct behavior: read credit must be fed from per-flow egress/send-queue
  state back to the relay reader. Do not treat a clean `global_rx_queue` as
  proof that it is safe to keep pulling full 512KiB ready batches.

## 2026-07-07 - Post-accept read credit is one batch late

- Stage: Knife14dy reverse-first VPS acceptance.
- Bundle:
  `/tmp/mini_vpn/knife14dy_read_credit_p1_30/mvpn_knife14dy_read_credit_p1_30_usclient_suite_20260707_131037.tar.gz`
- Symptom: the data relay showed read-credit updates and 64KiB minimum batch
  limits, but reverse-first P1 still failed at `22.8/21.6 Mbit/s`.
- Important discriminator: read-credit paused only at the tail
  (`read_credit_pause_updates=1`) after local pressure already hit
  `send_queue_max=892928` and TUN egress drops appeared.
- Cause: credit was computed from actual `send_queue`/`pending` after the
  current remote payload had entered the main loop. The current batch could
  still spend stale drain-credit before the relay reader saw the pressure.
- Correct behavior: compute read-credit and pressure debt from projected local
  pressure (`send_queue + pending + incoming`) before accepting/flushing the
  payload. Do not treat a working credit channel as sufficient if its policy is
  one payload behind.

## 2026-07-07 - Pressure debt must not hard-pause QUIC stream reads

- Stage: Knife14dz projected-credit VPS acceptance.
- Bundle:
  `/tmp/mini_vpn/mvpn_knife14dz_projected_credit_p1_30_usclient_suite_20260707_132432.tar.gz`
- Symptom: reverse-first P1 regressed to `6.05 Mbit/s` receiver and timed out
  while `read_credit_pause_updates=2` and `max_rx_blocked_stream_delta=1`
  appeared.
- Cause: projected local pressure was installed correctly, but active pressure
  debt was fed into the relay-read hard pause. This starved QUIC stream receive
  progress instead of merely bounding smoltcp/TUN flush.
- Correct behavior: use pressure debt to constrain local flush/close credit,
  not to stop relay reads. Relay reads should drain QUIC into bounded staging
  until receive-window high water or TUN drop feedback requires a real pause.

## 2026-07-07 - Do not kill remote mini_vpn with broad pkill patterns

- Stage: Knife14ea remote cleanup around `.27`.
- Symptom: a broad cleanup command matching `mini_vpn client-tun` could match
  its own SSH command line and disrupt the control session.
- Cause: process-name cleanup was expressed as a loose full-command pattern.
- Correct behavior: prefer suite-managed cleanup or narrow `pgrep`/`pkill`
  patterns that cannot match the SSH wrapper command. Verify with a non-killing
  process list before using `pkill` on VPS hosts.

## 2026-07-07 - Sing-box auth transient was service state, not config/time

- Stage: Knife14ea startup after Knife14dz.
- Symptom: `client-tun` startup failed at `tuic auth finish: sending stopped by
  peer: error 0`, while `.33` had no matching current TUIC inbound log.
- Checks: `.27` env and `.33` sing-box config matched by key length and fields,
  all hosts had synchronized time, and no TUIC `fail auth` appeared in the
  current `.33` window.
- Cause: likely transient sing-box/TUIC service state. Restarting sing-box
  restored primary startup. The auxiliary TUIC pool slot still can fail first
  auth once and recover through retry.
- Correct behavior: when this exact auth-finish/no-server-log pattern appears,
  verify env/config/time once, then inspect or restart sing-box before
  changing mini_vpn auth code.

## 2026-07-07 - Patch the intended ACK drain path, not the adjacent one

- Stage: Knife14eb local patch.
- Symptom: the first attempt to make relay read gaps use a larger ACK drain
  budget changed deferred remote-payload ACK drain instead. Focused
  `relay_gap_hint` and `tun_rx_drain_budget` tests caught the mismatch.
- Cause: similarly named budget helpers sit next to each other:
  `tun_rx_drain_budget_for_deferred_ack_drain` and
  `tun_rx_drain_budget_for_relay_gap_hint`.
- Correct behavior: for cadence fixes, add or update focused tests that name
  the exact source (`relay_gap_hint`, `remote_payload_deferred`, etc.) before
  accepting a budget change.

## 2026-07-07 - Relay-gap follow-up did not trigger in VPS

- Stage: Knife14ec reverse-first VPS acceptance.
- Bundle:
  `/tmp/mini_vpn/mvpn_knife14ec_gap_followup_p1_30_usclient_suite_20260707_135859.tar.gz`
- Symptom: the relay-gap follow-up implementation passed local tests but the
  VPS run fell to `23.2/21.5 Mbit/s` and recorded
  `relay_gap_hint_followup_attempts=0`.
- Cause: the exhaustion evidence was aggregate. The drains that exhausted
  budget were not relay-gap drains; the active/deferred remote-payload drain
  path still hit `budget_exhausted=210` with
  `remote_payload_deferred_attempts=812`.
- Correct behavior: after aggregate drain exhaustion, add source-specific
  diagnostics or attach escalation to the source that actually exhausts. The
  next patch should make deferred ACK drain escalate from active-flow budget to
  pressure budget only after it really exhausts.

## 2026-07-07 - Remote cargo needs explicit environment in non-login SSH

- Stage: Knife14ec `.27` focused tests.
- Symptom: `ssh ... cargo test` failed with `cargo: command not found`; a
  second attempt accidentally expanded `$HOME` on the Mac and looked for
  `/Users/liushan/.cargo/env` on `.27`.
- Cause: non-login SSH shells do not load `.cargo/env`, and double-quoted
  local commands expand `$HOME` before SSH.
- Correct behavior: run remote cargo commands with single-quoted SSH payloads
  and source the remote cargo environment inside the remote shell:
  `. "$HOME/.cargo/env"`.

## 2026-07-07 - Cargo accepts only one positional test filter

- Stage: Knife14en local gates.
- Symptom: `cargo test --lib <test1> <test2> ...` failed with
  `unexpected argument`.
- Cause: Cargo accepts one positional `TESTNAME` filter before `--`; multiple
  independent filters must be run as separate commands or replaced with a
  broader shared substring.
- Correct behavior: use a shared filter such as `mtu_policy` /
  `tuic_tcp_stream_diag`, run separate focused commands, or run
  `cargo test --lib --quiet`.

## 2026-07-07 - Do not test quinn MTU config through opaque Debug output

- Stage: Knife14en MTU policy TDD.
- Symptom: the first `safe1200` test failed because `TransportConfig` Debug
  output did not include `initial_mtu`, `min_mtu`, or
  `mtu_discovery_config`.
- Cause: quinn intentionally formats parts of `TransportConfig` opaquely, so
  Debug output is not a stable public observation point for MTU behavior.
- Correct behavior: keep mini_vpn-owned MTU policy as a pure profile function
  and test that profile directly; use the quinn config build only as a smoke
  check that shared VPN flow-control settings are still installed.

## 2026-07-07 - rsync to VPS must carry the project SSH key

- Stage: Knife14en `.27` sync.
- Symptom: the first `rsync` to `.27` failed with
  `Permission denied (publickey,password)`.
- Cause: the command omitted the required SSH identity from AGENTS.md.
- Correct behavior: use `rsync -e "ssh -i ~/.ssh/vpn ..."` and keep excluding
  `.env`, `.git`, and `target` when syncing the working tree to
  `/home/ubuntu/mini_vpn`.

## 2026-07-07 - TUN-MTU-derived controller tests need production-scale pressure config

- Stage: Knife14eo TDD.
- Symptom: the first `downlink_credit_controller_grows_credit_from_observed_egress_progress`
  test still failed after the controller allowed progress-based growth.
- Cause: the test used a tiny synthetic `DownlinkBackpressureConfig`
  (`high_bytes=100`) while the pressure floor is derived from TUN MTU
  (`1200 * RELAY_REMOTE_READ_PRESSURE_MIN_PACKETS`). The staging limit was
  therefore smaller than the pressure floor, so the test asserted credit growth
  in an impossible configuration.
- Correct behavior: controller tests that involve TUN-MTU-derived floors should
  either use production-scale/default backpressure config or explicitly assert
  the staging-limit clamp. Do not infer controller failure from a synthetic
  config whose watermarks are below the minimum ACK/window drain floor.

## 2026-07-07 - Hot-path projected credit helper may need a narrow clippy allow

- Stage: Knife14 downlink credit controller follow-up.
- Symptom: `cargo clippy ... -D warnings` can fail with
  `clippy::too_many_arguments` on
  `publish_projected_relay_read_credit_for_payload`.
- Cause: this helper sits on the `client_tun.rs` main-loop hot path and passes
  several already-owned local state references to avoid broad restructuring or
  allocation while iterating on the downlink credit algorithm. This file
  already has several narrow `#[allow(clippy::too_many_arguments)]` annotations
  for equivalent hot-path helpers and diagnostic formatters.
- Correct behavior: if the helper remains a local hot-path helper and the
  focused tests prove the behavior, add a narrow
  `#[allow(clippy::too_many_arguments)]` directly on that function rather than
  performing a cosmetic argument-object refactor during the performance fix.
  Revisit the signature only after the control-loop design stabilizes.

## 2026-07-07 - mini_vpn does not support `--help` as a safe preflight

- Stage: Knife14eo 97% `.27` preflight.
- Symptom: a lightweight remote preflight using `target/release/mini_vpn --help`
  exited `101` because `main.rs` treats unknown modes as a panic and supports
  only `client-tun` and `reality-probe`.
- Cause: the binary does not implement a help mode yet; the suite also records
  this panic as a non-blocking binary snapshot.
- Correct behavior: for acceptance preflight, check binary existence with
  `test -x target/release/mini_vpn` or use a real supported mode when a runtime
  smoke is required. Do not treat the existing `--help` panic snapshot as a
  tunnel failure unless it starts blocking the suite.

## 2026-07-07 - Knife14eo controller still reacts after the TUN drop edge

- Stage: Knife14eo 97% VPS reverse-first P1.
- Symptom: the scoped safe1200 P1 completed at only `32.3/30.8 Mbit/s` and
  had `tun_tx_dropped_delta=117`, despite clean QUIC loss/congestion/blocking
  and clean close lifecycle counters.
- Cause: the per-flow credit controller reduced the huge headroom-debt spiral
  but still allowed projected payload debt to push local pressure beyond the
  egress pause edge (`send_queue_max=553848`, `pending_high=576836`) before
  the hard clamp took effect.
- Correct behavior: the next patch must add a focused TDD case for projected
  payload overrun and make pressure/drop debt predictive for the next control
  epoch. Do not rerun full VPS acceptance on the same controller without a code
  change that caps projected payload credit before `tx_queue_pause_high`.

## 2026-07-07 - Static one-MTU ACK drain floor over-throttles clean egress

- Stage: Knife14ep VPS reverse-first P1.
- Symptom: the predictive drop-edge controller removed TUN drops
  (`tun_tx_dropped_delta=0`) but throughput stayed low at `30.3/28.7 Mbit/s`,
  with repeated zero-throughput intervals, `may_recv_false=8335`, and data
  stream read/pending gaps around `3.47s`.
- Cause: shrinking relay read credit below the old pressure floor was
  necessary near the drop edge, but a static one-packet ACK/window floor is too
  small once egress is clean and making progress. It protects the edge while
  starving reverse TCP receive cadence.
- Correct behavior: do not continue by shrinking the floor further or by
  re-running the same fixed-floor controller. Keep the predictive edge cap, but
  add adaptive growth from observed clean egress progress, with bounded
  multi-MTU non-paused drain credit and fast shrink only near high water or
  no-progress pressure.

## 2026-07-07 - Exit-to-target iperf preflight needs explicit exit SSH settings

- Stage: Knife14eq first VPS suite attempt.
- Symptom: the suite failed before tunnel P1 when
  `EXIT_TO_TARGET_IPERF_CHECK=1` was enabled.
- Cause: `SERVER_EVIDENCE_CHECK=0` meant the expected exit SSH defaults were
  not populated, and the command omitted `EXIT_SSH_HOST` / `EXIT_SSH_KEY`.
- Correct behavior: whenever enabling `EXIT_TO_TARGET_IPERF_CHECK=1`, set the
  matching exit SSH host and key explicitly, along with the target SSH host and
  key. Do not assume server-evidence defaults are active when
  `SERVER_EVIDENCE_CHECK=0`.

## 2026-07-07 - No-pressure reverse stalls are not pressure-controller evidence

- Stage: Knife14eq VPS reverse-first P1.
- Symptom: the scoped safe1200 P1 failed at `0.699/0.030 Mbit/s`, but local
  pressure and pending stayed near zero and read credit never fell below
  `65536` bytes.
- Cause: the failed window was a no-data / late-remote-after-local-finish /
  TUIC stream pending-read-gap branch, not an active adaptive ACK drain pressure
  clamp.
- Correct behavior: before attributing a low-throughput reverse-first run to
  read-credit pressure logic, check `throughput_shape`, `max_pressure_bytes`,
  `pending_max`, `headroom_deferred_bytes`, and `read_credit_limit_bytes_min`.
  If pressure is zero and read credit is still at the old floor, shift to local
  FIN ordering and stream wakeup diagnostics instead of tuning controller
  thresholds.

## 2026-07-07 - Repeating exit-to-target checks still needs explicit SSH env

- Stage: Knife14et first VPS suite attempt.
- Symptom: the suite failed before tunnel P1 with
  `EXIT_TO_TARGET_IPERF_CHECK=1` because `EXIT_SSH_HOST` was unset.
- Cause: the rerun command omitted explicit exit SSH settings even though
  `SERVER_EVIDENCE_CHECK=0` was used, so the suite had no source for exit SSH
  defaults.
- Correct behavior: always pass `EXIT_SSH_HOST`, `EXIT_SSH_KEY`,
  `EXIT_SSH_STRICT_HOST_KEY_CHECKING`, and `EXIT_SSH_KNOWN_HOSTS_FILE` when
  enabling `EXIT_TO_TARGET_IPERF_CHECK=1`. Do not rely on remembered defaults
  across handoffs or compactions.

## 2026-07-07 - Do not continue headroom tuning after pressure-free no-data

- Stage: Knife14et valid VPS reverse-first P1.
- Symptom: the scoped safe1200 P1 failed at `0.769/0.042 Mbit/s` while local
  pressure was gone: `pause_edges=0`, `pending_total_max=0`,
  `headroom_deferred_bytes=0`, and `read_credit_limit_bytes_min=65536`.
- Cause: the run was a reverse ACK/window cadence or stream wakeup starvation
  problem, not a local egress pressure problem. The data stream only received
  `210912B`, with `data_read_gap_max_ms=20500` and
  `data_pending_gap_max_ms=20383`.
- Correct behavior: stop the threshold-tuning loop when this shape appears.
  Analyze ACK propagation, local FIN ordering, and TUIC stream pending/read
  wakeups, or split local egress pressure control from ACK/window cadence
  control before another VPS acceptance attempt.

## 2026-07-07 - Do not implement FIN deferral when gaps are pre-FIN

- Stage: Knife14eu VPS reverse-first P1.
- Symptom: the scoped safe1200 P1 failed at `20.8/19.8 Mbit/s` with a
  `7002ms` data-stream read gap, but the new FIN-boundary diagnostics reported
  `local_finish_events=0`, `remote_after_local_finish_bytes=0`, and
  `data_max_read_gap_after_finish_ms=0`.
- Cause: the slow window was not caused by payload arriving after local FIN.
  The stall happened before local finish while local pressure and TUIC stream
  pending/read gaps coexisted.
- Correct behavior: do not add bounded FIN deferral as the next fix for this
  evidence shape. Move to a split controller: payload egress pressure remains
  bounded by pending/send_queue, while ACK/window cadence gets a separate
  bounded drain path driven by clean QUIC stream pending gaps and observed
  egress progress.

## 2026-07-07 - Do not repeat short-lived ACK cadence boost after it is active

- Stage: Knife14ev VPS reverse-first P1.
- Symptom: the scoped safe1200 P1 failed at `16.0/14.9 Mbit/s` even though the
  new relay gap-hint hook fired (`relay_gap_hints events=59`,
  `max_cadence_floor=19200`). QUIC loss/congestion/blocking, TUN drops, and
  terminal pending remained clean.
- Cause: the boost was wired but too episodic and still coupled to the same
  local pressure edge. The effective relay read-credit floor still collapsed
  to `1200`, then repeated `projected_payload_credit_edge` debt pushed
  pressure past the credit edge and paused read credit.
- Correct behavior: do not raise `cadence_floor`, TUN RX drain budget, or MTU
  knobs for this shape. The next fix must separate a continuously serviced
  ACK/window lane from payload staging and grant payload read credit only from
  measured TUN egress progress below a safe target.

## 2026-07-07 - Do not keep tuning payload credit after pressure-free stream starvation

- Stage: Knife14ew VPS reverse-first P1.
- Symptom: the scoped safe1200 P1 failed at `0.979/0.046 Mbit/s` even though
  the new egress-earned reservoir was active (`egress_payload_credit_bytes=122727`)
  and local pressure/backpressure was clean (`pending_total_max=0`,
  `headroom_deferred_bytes=0`, `pressure_credit_debt_bytes=0`,
  `tun_tx_dropped_delta=0`).
- Cause: this was not a payload credit shortage. The active data stream kept
  `read_credit_limit_bytes_min=65536`, yet `tuic_stream_pending` reached
  `23247ms` on the data stream and relay data read gaps reached `23974ms`.
  Connection-level UDP receive and stream-frame counters advanced while the
  active stream read remained pending.
- Correct behavior: pause code changes after this shape and design the next
  branch around TUIC stream pending/read wakeup and reverse ACK/window uplink
  service diagnostics. Do not raise payload-token caps, cadence floor, egress
  pacer, TUN queue length, MTU/PLPMTUD, stale-pool, iperf3, or sing-box knobs
  for this evidence.

## 2026-07-07 - Direct rustfmt needs the repository edition

- Stage: Knife14ew local gates.
- Symptom: `rustfmt src/client_tun.rs` failed with Rust 2015 parsing errors
  (`async fn` and let-chains rejected) because direct rustfmt did not read the
  Cargo manifest edition.
- Cause: this repository uses edition `2024`, and direct file-level rustfmt
  defaults were insufficient.
- Correct behavior: when formatting only one Rust file to avoid unrelated
  `cargo fmt --check` churn, run `rustfmt --edition 2024 <file>` and verify
  with `rustfmt --edition 2024 --check <file>`.

## 2026-07-07 - Do not widen ACK/window drain after Knife14ex

- Stage: Knife14ex VPS reverse-first P1.
- Symptom: the scoped safe1200 P1 failed at `17.8/16.7 Mbit/s` even though the
  default active-flow ACK/window service lane was enabled and clearly active:
  `timer_active_flow_attempts=598`, `tun_rx_drain attempts=11770`, and
  `tun_rx_drain packets=14090`.
- Cause: ACK servicing helped but was not the final limiter. The new
  `tuic_stream_pending_causes` discriminator reported
  `connection_stream_frames_pending=18` and `no_connection_rx=0`, while local
  egress/headroom pressure returned (`send_queue_max=557386`,
  `may_recv_false=13443`, `headroom_deferred_bytes=16754655`). QUIC
  loss/congestion/blocking and TUN drops remained zero.
- Correct behavior: do not increase active-flow timer duration, TUN RX drain
  budget, cadence floor, payload-token caps, TUN queue length, MTU/PLPMTUD,
  stale-pool logic, iperf3, or sing-box settings for this evidence. The next
  repair must first design/test relay or TUIC stream read-service stability and
  an egress target/headroom loop that does not park send_queue at the credit
  edge.

## 2026-07-08 - Treat all-VPS SSH pre-banner closes as environment blocked

- Stage: Knife14ey remote focused gate attempt.
- Symptom: `.27`, `.33`, and `.77` all accepted TCP/22 but closed before
  sending an SSH banner. SSH failed with
  `kex_exchange_identification: Connection closed by remote host` before
  authentication; `nc` confirmed the port was reachable but no banner was
  returned.
- Cause: this is not a mini_vpn code failure, not sudo, not `.env`, and not
  host-key or public-key authentication. It happens before authentication on
  every acceptance host, which points to a transient SSH service, source-IP
  policy, network middlebox, or provider-side limit.
- Correct behavior: do not rerun suites or rsync in a loop while this shape is
  present. First wait or fix SSH/banner availability, then resume with focused
  gates and the safe1200 reverse-first P1 suite.

## 2026-07-08 - Do not widen ordinary ACK/target knobs after Knife14ey

- Stage: Knife14ey VPS reverse-first P1.
- Symptom: the scoped safe1200 P1 failed at `25.5/24.2 Mbit/s` even though
  active-transfer pressure improved: before the close tail,
  `send_queue_max=447679`, `may_recv_false=0`, and
  `headroom_deferred_bytes=72259`. QUIC loss/congestion/blocking, TUN drops,
  send-slice errors, and global RX pressure stayed clean.
- Cause: the failure returned after the iperf close tail. Local TCP entered a
  `may_recv=false` shape with pending downlink, and terminal close-drain pushed
  the flow back to the old credit edge (`send_queue_max=557386`,
  `may_recv_false=11961`, `headroom_deferred_bytes=14717970`).
- Correct behavior: do not keep raising ACK cadence, TUN RX budgets,
  payload-credit caps, MTU/PLPMTUD, or ordinary egress target constants for
  this evidence. The next repair must make terminal/CloseWait close-drain
  target-aware and only expand payload drain credit when measured local egress
  progress creates room.

## 2026-07-08 - Do not run bidirectional iperf3 baselines concurrently

- Stage: Knife14fi-fo direct `.33 <-> .77` discriminator.
- Symptom: the first attempt to run forward and reverse direct iperf3 baselines
  at the same time failed one side with `iperf3: server is busy running a test`.
- Cause: the `.77` iperf3 service accepts only one active test in this mode.
- Correct behavior: run `.33 -> .77` and `.77 -> .33` direct baselines
  sequentially when using the shared iperf3 service.

## 2026-07-08 - Treat isolated TUIC auth close as retryable before changing code

- Stage: Knife14fl safe1200 receive-window shrink.
- Symptom: the first FL suite failed during startup with
  `tuic auth finish: sending stopped by peer: error 0`, while the `.33`
  service was active and a scoped minimal startup succeeded afterward.
- Cause: the evidence matched a transient peer close or environment hiccup, not
  a deterministic local code regression.
- Correct behavior: for a single startup auth close with healthy services,
  retry or run a minimal startup probe before redesigning the transport branch.

## 2026-07-08 - Avoid ad-hoc inbound iperf on `.27` as throughput evidence

- Stage: Knife14fi-fo path discriminator.
- Symptom: a `.33 -> .27` ad-hoc iperf test to a temporary listener timed out
  despite the listener being started.
- Cause: the result is likely firewall or security-group related and does not
  isolate mini_vpn's TUIC data path.
- Correct behavior: do not use `.33 -> .27` temporary inbound iperf as a
  blocker or root-cause signal unless the network policy is explicitly verified.

## 2026-07-08 - Use precise process cleanup for one-shot remote helpers

- Stage: Knife14fi-fo path discriminator cleanup.
- Symptom: `pkill -f "iperf3 -s -1 -p 5209"` can match and kill the shell that
  issued it, producing confusing remote command termination.
- Cause: broad `pkill -f` patterns can match their own command line on the
  remote host.
- Correct behavior: prefer a captured PID or a narrower process-selection
  method for temporary remote helper cleanup.

## 2026-07-08 - Include SSH identity when syncing to `.27`

- Stage: Knife14fi-fo remote focused gates.
- Symptom: `rsync` to `.27` without an explicit SSH command failed because it
  did not use the required identity.
- Cause: the default SSH identity was not the VPS key for this environment.
- Correct behavior: use `rsync -e 'ssh -i ~/.ssh/vpn' ...` for `.27` syncs.

## 2026-07-08 - Avoid large cross-VPS scp for sing-box A/B binaries

- Stage: mature sing-box client A/B.
- Symptom: copying `/usr/bin/sing-box` from `.33` through local scp was slow and
  appeared to leave a partial local file before the process exited.
- Cause: cross-VPS scp through the local machine was unnecessary and introduced
  noisy transfer state.
- Correct behavior: for this A/B, download the official sing-box release
  directly on `.27`, verify `sing-box version`, and keep the binary in a remote
  temp directory outside the repository.

## 2026-07-08 - Do not use fragile remote f-strings inside nested SSH heredocs

- Stage: mature sing-box client A/B summary parsing.
- Symptom: the iperf run completed, but the remote summary step failed with
  Python `NameError` after shell quoting stripped intended string literals in
  f-string expressions.
- Cause: nested SSH single quotes, heredocs, and Python f-strings with quoted
  dictionary keys are easy to mangle.
- Correct behavior: pull iperf JSON/log artifacts locally and parse them there,
  or use a single correctly quoted heredoc with no nested shell interpolation.

## 2026-07-08 - Check exit-side socket buffers before lowering throughput target

- Stage: Knife14fp server-side A/B.
- Symptom: both mini_vpn and a mature sing-box client stayed around
  `20-30 Mbit/s` even though `.33 -> .77` direct reverse was above
  `200 Mbit/s`, QUIC loss/congestion was clean, and `.33` sing-box was active
  with TUIC `congestion_control=bbr`.
- Cause: `.33` Linux socket buffer caps/defaults were only `212992B`. After
  raising `net.core.rmem_max` and `net.core.wmem_max` to `16777216`, and
  `net.core.rmem_default` and `net.core.wmem_default` to `1048576`, then
  restarting sing-box, the mature client reached `185.242 Mbit/s` and mini_vpn
  reached a reported `114.000 Mbit/s`.
- Correct behavior: before lowering the target or declaring TUIC single-stream
  architecture blocked, check and fix exit-side Linux socket buffers, then
  restart sing-box. Treat this as a mandatory high-throughput preflight.

## 2026-07-08 - Do not treat `tokio::io::join` as the remaining stream starvation root

- Stage: Knife14fs explicit TUIC relay stream wrapper.
- Symptom: commit `f25c952` replaced `tokio::io::join(recv, send)` with an
  explicit `TuicTcpRelayStream { recv, send }`, but the focused safe1200 P1
  regressed to `1.29/0.132 Mbit/s` and remained a no-data shape.
- Cause: the failure persisted below the wrapper layer. Local pressure was
  clean (`pending_total_max=0`, `may_recv_false=0`,
  `headroom_deferred_bytes=0`) and QUIC loss/blocking/rx-blocked stayed zero,
  while the active stream still showed `connection_stream_frames_pending` and
  multi-second read gaps.
- Correct behavior: do not continue join-vs-wrapper edits or local credit
  tuning for this shape. Revert or isolate `f25c952` before the next code
  branch, then design a deeper discriminator around quinn per-stream readiness,
  ordered stream offsets, server-side sending cadence, or a custom-exit data
  channel.

## 2026-07-08 - Do not keep unordered TUIC reassembly enabled by default

- Stage: Knife14ft unordered TUIC chunk reassembly.
- Symptom: commit `a4bfbe6` consumed `RecvStream::read_chunk(..., false)` and
  locally reassembled by stream offset, but the focused safe1200 P1 collapsed to
  `0.280/0.004 Mbit/s`, worse than the prior ordered stream path.
- Cause: unordered reads exposed real stream offset gaps, but local staging did
  not turn later-offset chunks into useful ordered TCP payload. The run showed
  `tuic-tcp-unordered-staging` with `gap_bytes=47972`, `max_buffered=29702B`,
  and `cap_hits=0`, while the active stream still had
  `connection_stream_frames_pending=34`, `data_pending_gap_max_ms=23033`, and
  clean local pending/headroom/TUN/QUIC blocking surfaces.
- Correct behavior: before the next VPS acceptance, revert `a4bfbe6` or gate it
  behind an explicit diagnostic env flag with the default path restored to the
  ordered stream reader. Use unordered staging only to collect offset-gap
  evidence, not as the production data path.

## 2026-07-08 - Do not run full `cargo fmt` in a narrow Knife14 stage

- Stage: Knife14fy ordered read-service restore.
- Symptom: running full `cargo fmt` reformatted many unrelated files
  (`src/device.rs`, `src/failover.rs`, `src/reality_*`, and others), creating
  noisy churn outside the two intended files.
- Cause: the repository has pre-existing non-target files that rustfmt would
  rewrite. A narrow diagnostic/data-plane stage should not accept full-repo
  formatting changes.
- Correct behavior: for scoped stages, run targeted formatting/checks such as
  `rustfmt --edition 2024 --check src/tuic.rs src/client_tun.rs`, and avoid
  full `cargo fmt` unless the stage explicitly includes full-repo formatting.

## 2026-07-08 - Do not treat a startup-auth failure as throughput evidence

- Stage: Knife14fy ordered read-service restore acceptance.
- Symptom: the focused reverse-first suite on `.27` commit `653d62bf` failed
  before iperf because `client-tun` exited with
  `tuic auth finish: sending stopped by peer: error 0`.
- Cause: not proven. The immediate evidence showed healthy direct baselines,
  active `.33` `sing-box`, expected `.33` socket buffers, valid no-secret config
  matches, and no contemporaneous TUIC inbound record in the inspected `.33`
  log tail.
- Correct behavior: do not tune read-service, local pressure-credit, MTU,
  iperf3, stale pool, or VPS settings from this result. First run a bounded
  startup-only probe on the same commit, then isolate `MINI_VPN_TUIC_TCP_POOL=1`
  vs `2`; compare with clean `f8765c1` startup only if the failure repeats.

Follow-up: the recommended startup-only probes on `653d62bf` with pool `1` and
pool `2` both succeeded, and the retry acceptance entered the data plane. This
confirms the first startup-auth close should not be carried forward as the
active root unless it repeats.

## 2026-07-08 - `.27` may not have GitHub SSH fetch credentials

- Stage: Knife14fy remote clean worktree setup.
- Symptom: `git fetch` from `.27` using the repository's SSH remote failed with
  public-key authentication, while HTTPS fetch of the same branch succeeded.
- Cause: the `.27` machine did not have a usable GitHub SSH identity for that
  fetch path.
- Correct behavior: for clean remote acceptance worktrees, use HTTPS fetch or
  an explicit local-to-remote sync path when `.27` lacks GitHub SSH access. Do
  not change the repository origin or keep retrying an interactive credential
  path during acceptance setup.

## 2026-07-08 - Soft target-edge alone does not close Knife14 throughput

- Stage: Knife14fz soft pressure-credit acceptance.
- Symptom: commit `52f2bae0` restored clean reverse-first data movement, but
  the focused safe1200 P1 still reached only `24.6/22.9 Mbit/s` against a
  healthy direct reverse baseline around `278 Mbit/s`.
- Cause: target-edge debt was no longer the only limiter. The run showed
  clean TUN drop, close-tail, and QUIC loss/blocking surfaces, while local
  downlink still had one pause/resume edge and the data TUIC stream repeatedly
  reported `connection_stream_frames_pending` with read gaps up to `3522ms`.
- Correct behavior: do not keep iterating only pressure-credit constants after
  this result. The next repair must add tests and code for egress-progress
  feedback into TUIC stream read service/self-wake, while preserving bounded
  pending and hard drop/pause safety.

## 2026-07-08 - Keep-read-armed alone does not cover low-byte ordered stream gaps

- Stage: Knife14gl keep relay reads armed across credit updates.
- Symptom: commit `18ed16ef` fixed the local TDD regression and restored the
  data stream's first useful read to `first_rx_ms=3`, but the focused safe1200
  reverse-first P1 still failed at `280 Kbit/s` sender and `16.2 Kbit/s`
  receiver.
- Cause: the run stalled after only `remote_to_global_rx_bytes=60704`, below
  `RELAY_ACK_DRAIN_HINT_MIN_DATA_BYTES=64KiB`. With
  `pending_cause=connection_stream_frames_pending` and no local pressure,
  `ack_drain_hint_due=0`/`ack_drain_hint_sent=0` left the ordered stream in a
  low-byte early-gap shape that the local TDD suite did not cover.
- Correct behavior: do not claim the `20M -> 100M+` path is closed merely
  because relay-reader cancellation and pressure-credit tests pass. Before the
  next VPS run, add a focused test for low-byte ordered-stream gap ACK/window
  service while preserving no-read-while-paused and bounded pending invariants.

## 2026-07-08 - Knife14gm first VPS attempt failed before the data plane

- Stage: Knife14gm focused safe1200 reverse-first P1 acceptance.
- Symptom: the first suite attempt
  `knife14gm_lowbyte_gap_safe1200_p1_30` exited before routing/probing because
  client startup failed at `tuic auth finish: sending stopped by peer: error 0`.
  sing-box and iperf3 were active, and no code/data-plane evidence was produced.
- Cause: not proven; the same command shape retried immediately as
  `knife14gm_lowbyte_gap_safe1200_p1_30_retry1` started TUIC successfully and
  completed the reverse-first P1.
- Correct behavior: treat this as a pre-data-plane startup failure, not as
  throughput evidence. Do read-only diagnostics, avoid restarting or retuning
  VPS services by default, then retry once with the same command before
  attributing it to code.

## 2026-07-08 - Knife14gp T14 failed below 30M after post-flush debt

- Stage: Knife14gp post-flush projected pressure debt acceptance.
- Symptom: commit `bd0d264` passed local and remote focused gates, and the
  focused safe1200 reverse-first P1 completed with healthy direct baselines,
  but throughput regressed to `15.0/13.5 Mbit/s` instead of exceeding
  `30 Mbit/s`.
- Cause: post-flush residual-pressure debt removed the obvious
  `downlink_backpressure` pause/resume edge (`0/0`), but read service still
  collapsed to `1200B` under residual pressure/headroom evidence while TUN
  drops, send failures, close-tail accounting, and QUIC loss/blocking stayed
  clean. The new since-last-pending diagnostics also showed repeated
  multi-second pending windows with `conn_rx_stream_frames_since_pending=0`,
  so prior `connection_stream_frames_pending` evidence could be stale since
  the last successful stream read.
- Correct behavior: do not repeat T14 as a completion fix, do not chase
  pause-edge counters alone, and do not broaden VPS/MTU/window/pool work from
  this result. The next fix must be TDD-first around stale repeated
  STREAM-pending classification plus a useful read-service floor during
  accepted egress progress when hard drop/failure signals are absent.

## 2026-07-08 - Knife14gq B7 failed after buffered read-credit isolation

- Stage: Knife14gq buffered downlink architecture B7.
- Symptom: commit `8a85ce7` passed local/remote gates and enabled buffered
  downlink, but focused safe1200 reverse-first P1 reached only
  `18.3/17.2 Mbit/s`, below the `>30 Mbit/s` B7 gate.
- Cause: the original `1200B` read-credit collapse was no longer present:
  `remote_read_service_len_min=65536`,
  `remote_batch_limit_bytes_min=524288`, and `read_credit_pause_updates=0`.
  The remaining low-average shape was attributed to local downlink
  writer/egress cadence (`local_downlink_backpressure`) with clean TUN drops,
  send errors, close-tail accounting, and QUIC loss/blocking.
- Correct behavior: stop at B7 and do not continue to B8/B9 without a new
  confirmed design. Do not keep tuning credit floors, self-wake cadence,
  VPS services, MTU/PLPMTUD, stale pool, or broad QUIC windows from this
  result. Any next attempt must be TDD-first around local writer/TUN egress
  cadence and must include diagnostics that show whether smoltcp/TUN drain is
  continuous or burst/idle.

## 2026-07-08 - Buffered credit diagnostics may not log when the open decision is unchanged

- Stage: Knife14gq B7 parsing.
- Symptom: startup confirmed buffered mode and relay metrics showed useful
  read service, but the expected `tcp-buffered-downlink-credit` lines did not
  appear in the B7 log because the open buffered decision did not require a
  later credit change.
- Cause: the diagnostic is change-oriented. If the controller starts and stays
  at the open default, the run still has evidence in `tcp-relay-live`, but it
  lacks a direct per-flow buffered-credit attribution line.
- Correct behavior: before relying on buffered-credit diagnostics in another
  acceptance gate, make the controller emit an initial per-flow decision when a
  relay enters buffered mode, or update the gate parser to treat startup mode
  plus relay-live read-credit fields as the explicit evidence.

## 2026-07-09 - Loop profiler self-test must not require extra loop-active headroom after egress service

- Stage: Knife14gs local egress service G5.
- Symptom: after adding the bounded local egress service lane, the harness test
  `loop_profiler_detects_on_loop_cpu_saturation` failed because the no-burn
  baseline loop-active fraction was already high (`0.980`) and the synthetic
  burn only moved it to `0.992`, below the old `+0.05` assertion.
- Cause: the new service lane legitimately makes the baseline main loop more
  active in the harness. The profiler was still detecting synthetic work: the
  burn's stable signal, `poll_fraction`, increased strongly.
- Correct behavior: for profiler self-tests after local egress scheduler
  changes, keep loop-active boundedness/iteration checks, but use
  `poll_fraction` as the stable synthetic burn attribution signal when the
  no-burn baseline is already near active saturation.

## 2026-07-09 - Local-only egress service capacity was over-predicted for G6

- Stage: Knife14gs G6 VPS acceptance.
- Symptom: commit `4a44a11` passed local and remote focused gates, but the
  focused safe1200 reverse-first P1 reached only `28.6/27.6 Mbit/s`, below
  the `>30 Mbit/s` gate.
- Cause: the G5 code-level capacity gate counted the new main-loop local
  egress lane as sufficient for throughput, but that lane only services dirty
  local state already visible to the main loop. It does not actively pull new
  remote bytes from the TUIC stream. The VPS run showed
  `tcp-local-egress-service accepted_bytes=0` while useful throughput remained
  limited by multi-second TUIC stream read gaps.
- Correct behavior: do not use local-only egress capacity math to predict
  `>30 Mbit/s` or `100+ Mbit/s`. A performance gate must require remote-read
  bytes and local accepted bytes to make progress in the same measured active
  window before another VPS acceptance claim.

## 2026-07-09 - High Mbps is not clean acceptance when tail drops or terminal pending remain

- Stage: Knife14gt G7 VPS acceptance.
- Symptom: commit `4caf60a` reached `147/144 Mbit/s` on focused safe1200
  reverse-first P1, but the tail still logged `tx_dropped_delta=783`,
  `global_rx_queue_used_max=1019/1024`, and
  `terminal_pending_reap_bytes=2653878`.
- Cause: the dispatch-window alignment restored high data movement, but the
  local egress/feedback path still allowed a late queue edge and terminal
  close with unsent pending data.
- Correct behavior: do not call a `100+ Mbit/s` run final unless throughput and
  close-tail are both clean. The next acceptance must preserve `>100 Mbit/s`
  while proving `tx_dropped_delta=0`, `terminal_pending_reap_bytes=0`, and no
  data-relay `terminal_closed_no_send`.

## 2026-07-09 - RX-edge tail cleanup overfit the visible G7 queue edge

- Stage: Knife14gu G8 RX-edge cleanup.
- Symptom: commit `1a3c5cb` passed local/remote gates but focused safe1200
  reverse-first P1 regressed to `20.2/19.2 Mbit/s` after G7's
  `147/144 Mbit/s`.
- Cause: the cleanup guarded extra relay ready-burst reads only when
  `global_rx` was critically near full. In the failed run the data relay queue
  stayed at `210/1024`, the limiter did not activate
  (`remote_batch_limited=0`), and throughput instead failed with repeated
  multi-second TUIC ordered stream read gaps plus
  `tcp-local-egress-service accepted_bytes=0`.
- Correct behavior: do not make another code change from a single dirty-tail
  hypothesis after a high-throughput run. First repeat the candidate commit,
  then A/B the parent high-throughput commit under the same suite shape. Only
  code after the A/B says whether this was run variance, an indirect scheduling
  regression, or the broader TUIC stream-service/local-admission contract.

## 2026-07-09 - G7 high-throughput parent did not reproduce in A/B

- Stage: Knife14gv A/B repeat after G8.
- Symptom: parent commit `4caf60a`, previously observed at `147/144 Mbit/s`,
  reached only `0.349/0.151 Mbit/s` in the same safe1200 reverse-first P1
  suite shape. The direct baselines were healthy, and local/TUN pressure
  surfaces were clean.
- Cause: not a proven external VPS bottleneck and not `global_rx` pressure.
  The run was dominated by TUIC ordered stream starvation:
  `max_remote_read_gap_ms=10457`,
  `pending_cause=connection_stream_frames_pending`, and tunnel attribution
  included `tuic_stream_starved`.
- Correct behavior: do not use G7 as a stable parent baseline or design solely
  around its tail counters. Require repeatability before cleanup work. The next
  design must target TUIC ordered stream service stability and distinguish
  fresh versus stale stream-frame pending evidence before another code change.

## 2026-07-09 - Suite attribution parser missed Knife14gw fresh/stale pending causes

- Stage: Knife14gw stream-service diagnostics.
- Symptom: raw `tuic-tcp-stream-pending` lines correctly logged
  `connection_fresh_stream_frames_pending` and
  `connection_stale_stream_frames_pending`, but the suite attribution summary
  still reported only the legacy `connection_stream_frames_pending` field and
  showed it as `0`.
- Cause: code-side pending cause names changed, but the suite attribution
  parser has not yet been taught the new fresh/stale names.
- Correct behavior: until the parser is updated, do not trust the summary line
  `tuic_stream_pending_causes` for Knife14gw+ freshness counts. Inspect raw
  `tuic-tcp-stream-pending` lines, or update the parser and self-test before
  using the summary for decisions.

## 2026-07-09 - Thin relay staging alone did not restore throughput cadence

- Stage: Knife14gx thin TCP relay staging first threshold.
- Symptom: commit `1bda549` enabled `MINI_VPN_THIN_TCP_RELAY=1` and passed
  local gates, but focused reverse-first P1 reached only `17.6/15.2 Mbit/s`,
  below the `30 Mbit/s` threshold.
- Cause: the design removed relay-reader waiting on `global_rx.reserve()` but
  did not address the remaining ordered TUIC stream-service/local-admission
  coupling. The run had `global_rx_pressure=0`, TUN drops `0`, and clean QUIC
  loss/blocking, yet still showed `data_read_gap_max_ms=4038`,
  `data_pending_gap_max_ms=4006`, fresh/stale pending causes, and attribution
  `local_pressure_credit`.
- Correct behavior: do not spend another VPS run on relay staging, global_rx
  admission, or dispatcher shape alone. Require a design and TDD gate that
  proves sustained TUIC ordered-stream polling and local admission progress
  remain coupled under local pressure before claiming a path to `>30M` or
  `100+M`.

## 2026-07-09 - Split-poll credit decoupling did not clear local pressure

- Stage: Knife14gy ordered stream polling versus local pressure-credit/headroom.
- Symptom: commit `372d6e3` passed local and remote focused gates, and the data
  stream kept `remote_read_service_len_min=max=65536` even when
  `read_credit_limit_bytes_min=6686`, but focused reverse-first P1 reached only
  `24.1/22.7 Mbit/s`.
- Cause: the split-poll fix removed one direct read-length clamp, but the
  local admission/headroom path still installed pressure debt and stopped
  useful admission at the tail:
  `may_recv_false=5190`, `headroom_limited=5185`,
  `pressure_credit_debt_bytes=122727`, `send_queue_max=557386`, and
  `tcp-local-egress-service accepted_bytes=0`.
- Correct behavior: do not claim throughput recovery from stream-poll cadence
  alone. The next implementation must directly change and test local
  admission/headroom egress progress under pressure, with an acceptance signal
  requiring both remote read progress and local accepted/flush progress in the
  same active window.

## 2026-07-09 - TUN TX drain-progress hypothesis was not sufficient

- Stage: Knife14hz local egress drain progress H1/H2/H3.
- Symptom: commit `8fc0cdb` passed local and remote focused gates, but focused
  reverse-first P1 reached only `15.3/14.3 Mbit/s`, below the `30 Mbit/s`
  first threshold.
- Cause: the code-level contract was valid, but the VPS run showed it was not
  the active throughput root. `tcp-local-egress-service` reported
  `egress_drain_bytes=0` and `accepted_bytes=0`, while the main downlink flush
  had already accepted `52039183` bytes with no pressure debt or headroom
  limiting. The remaining bad signal was ordered TUIC stream read/pending gaps
  up to `5086ms` with fresh/stale pending evidence.
- Correct behavior: stop extending this branch through local egress-drain,
  headroom, pressure-credit, VPS service, iperf3, MTU/PLPMTUD, stale-pool, or
  broad QUIC-window changes. The next design must instrument and fix the
  ordered TUIC stream `read().await` / pending self-wake path, with acceptance
  requiring active data-read gaps below `500ms` before another `100+ Mbit/s`
  claim.

## 2026-07-09 - Ordered chunk adapter alone did not break the throughput band

- Stage: Knife14h4 ordered TUIC chunk adapter.
- Symptom: the experimental H4 code replaced default ordered TCP reads with a
  Quinn `read_chunk(max, true)` adapter and passed local/remote gates, but the
  focused reverse-first P1 reached only `17.1/15.7 Mbit/s`.
- Cause: the seam was real but not sufficient. The run proved
  `relay_mode=ordered_chunk` was active and slightly reduced data read gap
  (`3685ms` versus Knife14hz `5086ms`), yet the same bursty/idle iperf shape
  remained with clean TUN drops, clean global-rx/local-write pressure, clean
  pressure/headroom debt, clean close-tail, and clean QUIC loss/blocking.
- Correct behavior: do not repeat H4 by tuning ordered chunk sizes,
  `read_chunk(true)` polling shape, or self-wake timers as standalone fixes.
  Before another VPS run, either revert/gate the H4 adapter or use it only as a
  diagnostic seam in a design that explains bursty remote delivery under clean
  local and QUIC counters.

## 2026-07-09 - Knife14h8 VPS smoke was blocked before throughput by sudo TTY

- Stage: Knife14h8 continuous pump product gate remote smoke.
- Symptom: the first VPS suite attempt on `.27` received
  `MINI_VPN_CONTINUOUS_TCP_RELAY=1` and passed early environment checks, but
  failed at the suite's `sudo -v` preflight with:
  `sudo: a terminal is required to read the password`.
- Artifact:
  `/tmp/conn/mvpn_knife14h8_continuous_pump_p1_usclient_suite_20260709_173738.tar.gz`
- Cause: the suite was launched through non-TTY SSH. A later `ssh -tt` sudo
  probe reached an interactive password prompt and was interrupted without
  entering or storing a password.
- Correct behavior: do not treat this artifact as throughput evidence. For
  future `.27` suites, start from a real TTY and complete `sudo -v` at the
  prompt, or explicitly design a non-interactive sudo preflight. Never put the
  sudo password in commands, scripts, docs, logs, learning memory, or summaries.

## 2026-07-09 - Remote `.27` shell differs from the local dev shell

- Stage: Knife14h8 remote preparation.
- Symptom: `.27` did not have `rg` in PATH, and non-login SSH did not have
  `cargo` in PATH. A single `cargo test` invocation with multiple bare filters
  also failed because Cargo accepts only one positional test filter.
- Cause: VPS command environment differs from the Mac development shell, and
  the Cargo CLI filter syntax was misused.
- Correct behavior: use `grep`/`find` on `.27` unless `rg` is confirmed
  installed; run Rust commands through `bash -lc`; run multiple focused Cargo
  filters as separate commands or use one broader filter such as
  `cargo test -q continuous_relay_reader`.

## 2026-07-09 - Continuous relay pump did not clear the first throughput gate

- Stage: Knife14h8 continuous relay pump VPS smoke.
- Symptom: with `MINI_VPN_CONTINUOUS_TCP_RELAY=1` active and
  `engine=continuous_pump` confirmed, reverse-first P1 reached only
  `30.7/29.6 Mbit/s`. The receiver did not clear the `>30 Mbit/s` first gate,
  and the run remained far from `100+ Mbit/s`.
- Artifact:
  `/tmp/conn/mvpn_knife14h8_continuous_pump_p1_usclient_suite_20260709_174648.tar.gz`
- Local copy:
  `/tmp/mini_vpn_knife14h8_continuous_pump/mvpn_knife14h8_continuous_pump_p1_usclient_suite_20260709_174648.tar.gz`
- Cause: the H8 change removed normal local `RelayReadCredit` as the remote
  read clock, but that was not the main remaining bottleneck. The active data
  stream still had `data_read_gap_max_ms=3823`,
  `data_pending_gap_max_ms=3408`, and `data_poll_gap_max_ms=3406` while
  `continuous_queue_wait_events=0`, QUIC loss/congestion/blocking were clean,
  TUN drops were `0`, global-rx/local-write pressure were `0`, and close-tail
  pending was `0`.
- Correct behavior: do not keep tuning the continuous queue, relay-reader
  read-credit decoupling, chunk size, self-wake timers, broad QUIC windows, VPS
  buffers, MTU/PLPMTUD, stale pool, sing-box liveness, or iperf3 for this
  branch. The next design must explain why TUIC reports
  `connection_stream_frames_pending` and multi-second active data poll/read
  gaps while the relay byte queue is not full and local/QUIC counters are
  mostly clean; focus on the seam between `tuic.rs` ordered stream readiness
  and `client_tun.rs` local downlink backpressure/`flush_downlink` pressure
  transitions.

## 2026-07-09 - D2.1c joined-flow VPS gate still failed throughput

- Stage: Knife14h9/D2.1c canonical bridge gate.
- Symptom: real VPS logs successfully joined TUIC `conn/id/stream` to local
  `handle/epoch`, but reverse-first P1 reached only `1.500/0.615 Mbit/s`
  sender/receiver.
- Artifact:
  `/tmp/conn/mvpn_knife14h9_d2_bridge_p1_usclient_suite_20260709_190629.tar.gz`
- Local copy:
  `/tmp/mini_vpn_knife14h9_d2_bridge/mvpn_knife14h9_d2_bridge_p1_usclient_suite_20260709_190629.tar.gz`
- Cause status: not fully solved. This run falsified "the relay task is not
  polling the data stream" because `data_poll_gap_max_ms=4`, but
  `data_read_gap_max_ms=6837` and `data_pending_gap_max_ms=6010` remained.
  Local pressure, TUN drops, QUIC loss/congestion/blocking, pressure credit
  debt, headroom limiting, TUN flush failures, and close-tail pending were
  clean in the summary.
- Correct behavior: do not respond by tuning chunk size, self-wake, read
  batching, VPS buffers, MTU/PLPMTUD, stale pool, or broad QUIC windows. The
  next probe/design must distinguish whether connection-level stream frames
  are for the joined data stream, and whether local egress service is actually
  draining TUN/smoltcp progress or only accepting bytes into the local TCP send
  queue.

## 2026-07-09 - D2.2a local egress drain metric had a snapshot-order blind spot

- Stage: Knife14h9/D2.2b local egress discriminator.
- Symptom: D2.2a re-summary reported
  `local_egress_drain_bytes_max=0` and
  `local_egress_service.egress_drain_bytes_max=0`, which was tempting to read
  as proof that no local smoltcp/TUN egress drain happened.
- Cause: `service_local_egress_until` measured pressure after
  `drain_ready_tun_rx`, but `drain_ready_tun_rx` itself can call
  `iface.poll`, `flush_tx`, and dirty relay processing. ACK/TUN RX can reduce
  smoltcp send-queue or queued TUN TX pressure before the old before/after
  snapshot window starts.
- Correct behavior: do not treat old `egress_drain_bytes=0` as proof of no
  local drain unless the snapshot covers the full ACK/TUN RX plus poll/flush
  service cycle. The D2.2b `LocalEgressPressureSnapshot` test family is the
  local guard for this metric.

## 2026-07-09 - Unordered TUIC reassembly diagnostic is evidence, not an accepted fix

- Stage: Knife14h9/D2.2b VPS unordered discriminator.
- Symptom: with `MINI_VPN_TUIC_TCP_UNORDERED_REASSEMBLY=1` and
  `relay_mode=unordered_reassembly_diag`, reverse-first P1 still reached only
  `18.400/17.500 Mbit/s` and kept multi-second read/pending gaps.
- Artifact:
  `/tmp/conn/mvpn_knife14c_usclient_suite_20260709_193528.tar.gz`
- Local copy:
  `/tmp/mini_vpn_knife14h9_d2_2b_unordered_diag/mvpn_knife14c_usclient_suite_20260709_193528.tar.gz`
- Cause status: not fully solved. The diagnostic proved same-stream
  out-of-order chunks (`max_gap_bytes=1169774`,
  `out_of_order_chunks=30287`, `cap_hits=0`), but the current implementation
  is still an in-`poll_read` staging adapter, not a full per-flow copy contract
  with downstream permit lifetime and local egress feedback.
- Correct behavior: do not flip unordered reassembly on as the product answer,
  and do not keep tuning chunk size/self-wake/read batching from this result.
  The next change must be a bounded per-flow reassembly/copy contract with TDD
  and a falsifiable VPS `>30 Mbit/s` first gate.

## 2026-07-09 - Tooling and VPS startup failures during D2.3

- `rustfmt src/client_tun.rs src/tcp_downlink_pump.rs` failed because direct
  `rustfmt` did not inherit the crate edition and parsed the files as Rust
  2015. Use `cargo fmt -- ...` or `cargo fmt --check` from the repo root for
  this project.
- One hand-written nested SSH/Python here-doc for TUIC config comparison broke
  in shell quoting and ran partially in the local shell. For no-secret remote
  comparison scripts, send Python over SSH stdin instead of embedding nested
  here-docs inside a quoted command.
- The first D2.3 suite attempt failed during TUIC startup with
  `tuic auth finish: sending stopped by peer: error 0`, but immediate baseline
  and D2.3 smoke starts succeeded and no-secret UUID/password/SNI/ALPN
  comparison matched. Treat a single startup auth failure as a retry/smoke
  condition before changing code or VPS config.

## 2026-07-09 - D2.4 high-port byte-source probe was an invalid capacity signal

- Failed run: a temporary byte source on `.77:5297` produced a
  `tuic_tcp_sink_probe` result of `0` bytes and EOF after about `5s`.
- Correct interpretation: sing-box logs showed it attempted `.77:5297` but
  failed with a dial timeout. The target byte source had not accepted the
  connection, so this was a target-port reachability/security-group issue, not
  mini_vpn TUIC read capacity.
- Fix used for the valid discriminator: temporarily stop `.77` iperf3, bind the
  byte source to the already-open `.77:5201`, run the probe, then restart and
  confirm iperf3 active/listening.
- Future behavior: do not use arbitrary high target ports for VPS capacity
  probes unless direct reachability from the exit side has been proven first.
  A zero-byte sink result must be checked against exit-side sing-box logs and
  target-side accept logs before being treated as a data-plane result.

## 2026-07-09 - rsync without relative paths copied H10 files to the remote repo root

- Stage: Knife14h10/H10d remote preparation.
- Symptom: the first `.27` sync command sent `src/client_tun.rs`,
  `src/tcp_egress.rs`, `src/tcp_downlink_pump.rs`, `src/lib.rs`, and the suite
  script to `/home/ubuntu/mini_vpn/` by basename instead of preserving
  `src/` and `scripts/`.
- Fix used: reran sync with `rsync -R` for the intended paths and removed the
  mistaken root-level copies.
- Correct behavior: for partial repo syncs to `.27`, use `rsync -R` from the
  repo root or explicit destination paths. After syncing, run remote
  `git status --short -- <expected paths> <known accidental basenames>` before
  building.

## 2026-07-09 - H10d high-throughput actor run is not clean acceptance

- Stage: Knife14h10/H10d D3 egress actor VPS first gate.
- Symptom: with `MINI_VPN_D3_EGRESS_ACTOR=1`, reverse-first P1 reached
  `iperf_receiver_mbps=102.000` and active interval averages above `170M`, but
  the iperf command exited by timeout (`exit=124`).
- Artifact:
  `/tmp/conn/mvpn_knife14h10c_d3_actor_p1_usclient_suite_20260709_215654.tar.gz`
- Local copy:
  `/tmp/mini_vpn_knife14h10c_d3_actor/mvpn_knife14h10c_d3_actor_p1_usclient_suite_20260709_215654.tar.gz`
- Acceptance blockers: `tun_tx_dropped_delta=97`,
  `pending_at_close=421577`, and `terminal_pending_reap=421577`.
- Correct behavior: treat H10d as architecture-capacity proof, not an accepted
  fix. The next stage must analyze D3 actor close-tail/TUN egress-drop cleanup
  before another "final" repeat; do not return to TUIC chunk size, read-credit,
  VPS buffers, MTU/PLPMTUD, stale pool, or broad QUIC window tuning.

## 2026-07-09 - Pure clean-headroom actor pacing is too conservative

- Stage: Knife14h10/H10d2 actor clean-headroom local pacing.
- Symptom: the focused reverse-first P1 with `MINI_VPN_D3_EGRESS_ACTOR=1`
  regressed to `sender=11.6 Mbit/s` and `receiver=9.72 Mbit/s`.
- Artifact:
  `/tmp/conn/mvpn_knife14h10d2_actor_clean_headroom_usclient_suite_20260709_221940.tar.gz`
- Local copy:
  `/tmp/mini_vpn_knife14h10d2_actor_clean_headroom/mvpn_knife14h10d2_actor_clean_headroom_usclient_suite_20260709_221940.tar.gz`
- What the failed run proved: clean-headroom-only eliminated the H10d tail
  blockers (`tun_tx_dropped_delta=0`, `pending_at_close=0`,
  `terminal_pending_reap=0`), but it also forced
  `drain_credit_planned_bytes=0`, `send_queue_max=449999`,
  `headroom_limited_calls=13881`, and low throughput.
- Correct behavior: do not accept or keep extending pure clean-headroom-only
  pacing as the product fix. The next design must preserve H10d's bounded
  elastic credit above clean headroom while making that credit drop-aware and
  recent-drain-backed.

## 2026-07-09 - Target-edge actor hybrid credit is still below the 100M capacity gate

- Stage: Knife14h10/H10d3 actor hybrid target-edge credit.
- Symptom: reverse-first P1 improved to `receiver=29.8 Mbit/s` but stayed below
  `100M+`.
- Artifact:
  `/tmp/conn/mvpn_knife14h10d3_actor_hybrid_target_usclient_suite_20260709_224720.tar.gz`
- Local copy:
  `/tmp/mini_vpn_knife14h10d3_actor_hybrid_target/mvpn_knife14h10d3_actor_hybrid_target_usclient_suite_20260709_224720.tar.gz`
- What the failed run proved: target-edge credit preserved the H10d2 safety
  wins (`tun_tx_dropped_delta=0`, `pending_at_close=0`,
  `terminal_pending_reap=0`) but remained locally pressure-limited
  (`send_queue_max=503692`, `pending_total_max=454619`,
  `hard_edge_guard_limited=18275`, `pressure_credit_debt_bytes=122727`).
- Correct behavior: do not accept target-edge actor hybrid as the final H10 fix.
  The next design must allow a higher adaptive elastic edge between target and
  credit edge while retaining drop-debt clean-headroom backoff.

## 2026-07-09 - Actor adaptive credit edge did not address the active Gate A bottleneck

- Stage: Knife14h10/H10d4 actor adaptive credit edge.
- Symptom: reverse-first P1 regressed to `receiver=20.6 Mbit/s`, below the
  `>30M` Gate A target.
- Artifact:
  `/tmp/conn/mvpn_knife14h10d4_actor_adaptive_credit_usclient_suite_20260709_231822.tar.gz`
- Local copy:
  `/tmp/mini_vpn_knife14h10d4_actor_adaptive_credit/mvpn_knife14h10d4_actor_adaptive_credit_usclient_suite_20260709_231822.tar.gz`
- What the failed run proved: target-to-credit elasticity was not the active
  limiter. `send_queue_max=390896` stayed below `tx_queue_flush_high=449999`,
  `headroom_limited=0`, `pressure_credit_debt_bytes=0`, and
  `hard_edge_guard_limited=0`.
- Correct behavior: do not continue with actor credit threshold variants as the
  next fix. The next discriminator must explain TUIC data read/pending/poll
  gaps (`3849/3848/3394ms`) while local egress pressure, TUN drops, close-tail,
  global_rx pressure, and QUIC loss/blocking are clean.

## 2026-07-09 - SSH TTY must be tool-writable before sudo suites

- Stage: Knife14h10/H10d15 Gate A launch.
- Symptom: running `ssh -tt ... sudo -v ...` without tool-level `tty=true`
  reached the sudo prompt, but `write_stdin` reported stdin was closed and
  could not enter the password.
- Impact: the first suite launch had to be interrupted with Ctrl-C and rerun.
- Correct behavior: for any suite that may invoke `sudo -v`, start the
  `exec_command` with both remote `ssh -tt` and tool `tty=true` from the
  beginning. Never put sudo passwords in commands, files, docs, logs, or
  summaries.

## 2026-07-09 - H10d15 passes 100M capacity but is not clean final acceptance

- Stage: Knife14h10/H10d15 native permit read floor Gate A.
- Artifact:
  `/tmp/conn/mvpn_knife14h10d15_native_read_floor_gatea_usclient_suite_20260710_070433.tar.gz`
- Local copy:
  `/tmp/mini_vpn_knife14h10d15_native_read_floor/mvpn_knife14h10d15_native_read_floor_gatea_usclient_suite_20260710_070433.tar.gz`
- Positive result: reverse-first P1 reached `sender=186 Mbit/s` and
  `receiver=185 Mbit/s`, proving the native read floor plus D6 bounded permit
  queue can exceed `100M+`.
- Acceptance blocker: after the high-throughput window, TUN egress reported
  `tx_dropped_delta=229`, `global_rx_paused=true`, and the data handle closed
  through `dead_slot_reap` with `close_egress_class=terminal_closed_no_send`,
  `close_egress_bytes=327272`, and `send_queue=327272`. QUIC also showed
  `rx_blocked(stream=1)` near the tail, though loss/congestion stayed zero.
- Correct behavior: do not call H10d15 the final accepted fix yet. Next stage
  must make the high-throughput path drop-aware/close-clean and then repeat
  reverse-first `>100M` at least twice with `tx_dropped_delta=0`,
  no dead-slot close-tail blocker, and clean close accounting.

## 2026-07-09 - Pre-D16 staged tree is not self-contained on HEAD dependencies

- Stage: H10d16 first-commit baseline verification.
- Symptom: the current mixed working tree passed `518` lib tests, but an
  archive of only the approved staged TCP/TUN files failed to compile at two
  TUIC `SendStream::finish` call sites.
- Root cause: the existing pre-D16 `src/tuic.rs` diff targets Quinn `0.11`,
  while HEAD still declares Quinn `0.10`. The uncommitted dependency upgrade
  also changes rustls and overlaps separate QUIC/REALITY work.
- Correct behavior: verify the staged tree independently before committing a
  dirty baseline. Do not widen a TCP/TUN commit to Cargo/QUIC/REALITY merely to
  make it compile. Preserve the working tree and continue uncommitted only
  after explicit user approval.

## 2026-07-10 - D16 terminal publication preceded pending-read refund

- Stage: H10d16 Task 10 lifecycle review.
- Symptom: the deterministic writer-error test observed the owned queue closed
  while `reserved_bytes=65536`; the supervisor sent terminal events before it
  cancelled the reader task holding that reservation.
- Risk: terminal close could race a second reader-produced event, and local
  lifecycle code could observe a closed relay before the byte ledger was closed.
- Correct behavior: on supervisor-owned terminal paths, stop and join the Quinn
  read owner first, verify RAII reservation refund, then publish readiness/close
  in order. Await Tokio `JoinHandle`s through `&mut` when later structured
  cleanup still references the handle; consuming the handle creates a Rust
  ownership error even when a runtime boolean makes the later branch unreachable.

## 2026-07-10 - D16 Gate A stalled before the reverse data stream

- Stage: H10d16 Task 11 single Gate A.
- Symptom: iperf emitted no interval samples and timed out. The first remote
  byte arrived in `3ms`, was actor-admitted and TUN-flushed, but the stream then
  had a `39995ms` read gap and closed with only `2` received bytes.
- Rejected roots: direct path capacity (`277 Mbit/s` reverse baseline), TUIC
  auth/connect, QUIC loss/congestion/blocking, actor bypass, send-slice/flush
  errors, queue pressure, and TUN qdisc drops were clean.
- Correct behavior: do not tune VPS, MTU, QUIC windows, pool, chunk size, or
  self-wake. First add a deterministic bidirectional seam proving that a D16
  response flushed to TUN is followed by local ACK/control ingestion and relay
  writer progress.
- Operational failure: server-evidence collection disconnected the SSH session
  while reading the target clock. The suite cleanup succeeded and the bundle
  was preserved; future collectors must bound each cross-host evidence command
  so evidence failure cannot terminate the parent suite session.
- Local confirmation: the two-smoltcp red test reproduced the stall when the
  post-flush async TUN wait edge was suppressed. Reusing the existing bounded
  active-flow TUN RX follow-up made the second control message round-trip in
  about `20ms`; future D16 composition changes must keep this test green.

## 2026-07-10 - D16 real Quinn read stayed pending while stream frames accumulated

- Stage: H10d16 explicitly authorized replacement Gate A.
- Symptom: the data connection accumulated thousands of Quinn stream frames
  and filled its receive window while the D16 ordered reader showed a
  `20001ms` poll gap and no first chunk until `20004ms`. Iperf receiver stayed
  at zero and close left both pending and smoltcp egress bytes.
- Rejected roots: target/exit service state, direct path capacity, TUIC
  connect/auth, QUIC loss/congestion, TUN qdisc drops, actor bypass, and byte
  ledger leaks. The final queue was closed and exactly empty.
- Correct behavior: add a real Quinn delayed-readability red test at the direct
  adapter, then a reservation/feedback test around `run_d16_native_reader`.
  Preserve one owner, bounded ordered read, cancellation refund, and
  readiness-only queueing; do not mask the defect with a periodic self-wake.
- Confirmed root: the real Quinn adapter test passed. Running feedback had
  subtracted the pending read's own full-capacity reservation, published pause,
  cancelled/refunded the read, and then had no readiness event to publish
  resume. Running feedback must include its own reservation as an already-armed
  opportunity while leaving the ownership ledger unchanged.

## 2026-07-10 - Target evidence SSH followed the active target TUN route

- Stage: H10d16 replacement Gate A evidence collection.
- Symptom: after the probe, the suite's SSH to the target host used the same
  target `/32` route that was intentionally installed on `tun0`. It opened
  extra D16 port-22 flows, the target clock command hung, and the parent SSH
  eventually exited `255` even though cleanup restored the route and stopped
  client-tun.
- Correct behavior: never collect target management evidence directly through
  the data-plane route under test. Use a bounded out-of-band path such as an
  Exit-host ProxyJump, or defer target collection until the target route has
  been restored. Evidence failure must remain isolated from suite cleanup and
  bundle creation.
- Local repair: known-topology Target evidence now uses an explicit bounded
  Exit proxy carrying the Exit SSH identity and host-key options; every raw
  evidence SSH command also has a `20s` outer timeout. Keep this suite self-test
  green before another remote acceptance request.

## 2026-07-10 - D16 Gate A froze in DrainOnly after global drop debt missed drain

- Stage: H10d16 explicitly authorized credit-rearm Gate A.
- Symptom: the data reader started in 3ms and the actor admitted 60,928,613
  bytes, but a single `tx_dropped_delta=2029` event installed 122,727 bytes of
  global drop debt. Feedback subsequently resumed at zero pressure, while both
  D16 flows remained DrainOnly for 8,283 cycles and receiver throughput ended
  at 12.2 Mbit/s over the timeout window.
- Root cause: aggregate pressure fell from 994,674 bytes to zero between TUN
  feedback samples. The only debt-payment path was a per-flow send-limit clock,
  which did not observe that decrease. DrainOnly prohibited new admission, so
  it also prohibited creation of a future queue decrease that could pay debt.
- Secondary defect: the production adapter treats `Some(0)` completed drain as
  `drain_progress=true`; this violates the positive-progress Recovery contract,
  although active debt masked it in this run.
- Correct behavior: add an exact red composition test, pay debt once from
  measured drop-episode aggregate drain, propagate the same clean-drain edge to
  eligible D16 flows, and require positive bytes for normal actor-cycle
  recovery. Do not bypass debt, tune capacity/cadence parameters, rerun Gate A,
  or start Gate B before local red/green and review are complete.

## 2026-07-10 - Reverse-only TUN RX harness never opened the remote relay

- Stage: H10d16 Task 11A bounded kernel-to-userspace TUN RX starvation harness.
- Symptom: the new scenario timed out with `tcp_opens=0`, `received_bytes=0`,
  zero modeled drops, and a one-packet ring high-water mark. Increasing runtime
  scheduling interleave did not change the result.
- Root cause: the TUN TCP path opens `open_tcp_relay` only after
  `process_listener_activity` extracts the first local application payload;
  SYN and handshake completion alone do not open the upstream. A generator
  that waits for reverse data before sending any payload therefore cannot
  reach the D16 reader/actor/TUN egress path.
- Correct behavior: every deterministic reverse-first harness must send a
  minimal bootstrap application payload, confirm one upstream open, and have
  the mock consume that bootstrap without echoing it before measuring the
  reverse payload and bounded TUN RX ring. Treat `tcp_opens=0` as a fixture
  failure, never as a valid drop/starvation RED.

## 2026-07-10 - TUN RX guard GREEN lacked proof of its recovery edges

- Stage: H10d16 Task 11A bounded TUN RX starvation RED/GREEN.
- Evidence: the calibrated 64 MiB, 64-packet stress reached ring capacity in
  every five-run RED sample and recorded modeled drops in four runs. The first
  backlog-latch implementation then completed the payload with zero modeled
  drops in an observed failure, but still reached a high-water mark equal to
  capacity; a five-run repeat was not uniformly green under the test's stricter
  `high_water < capacity` assertion.
- Diagnostic distinction: a ring becoming exactly full is risk evidence but
  is not itself the Linux `tx_dropped` condition; the kernel increments the
  counter when producing into the already-full `ptr_ring` fails. Removing the
  high-water assertion alone would nevertheless create false confidence unless
  the harness proves budget exhaustion latched the device-global pause and a
  clean `WouldBlock` edge released it.
- Correct behavior: expose backlog pause/resume edges, budget exhaustion, and
  actor-bypass accounting in the integrated report. Require zero modeled drop,
  complete delivery, zero actor bypass, at least one pause edge, and at least
  one clean resume edge across repeats. Keep high-water diagnostic; if any
  modeled drop remains, re-evaluate the seam instead of tuning ring size or
  timing.

## 2026-07-10 - Initial/final guard booleans lost a transient backlog episode

- Symptom: an enlarged bounded drain could prove backlog and then reach clean
  WouldBlock in the same call. Comparing only `active_before` and
  `active_after` saw false→false, skipped the pause action, or left a flow in
  DrainOnly without a recoverable edge.
- Root cause: the guard already carried monotonic pause/resume generations, but
  orchestration discarded them and classified only the final boolean state.
- Superseded intermediate behavior: classifying the edge pair as an immediate
  ForceDrainOnlyThenRecover action still allowed admission to reopen before
  the flow's already-admitted bytes completed their ACK feedback.
- Correct behavior: preserve the pause generation, but let the first clean
  probe only arm recovery. Complete one admission-free ControlOnly poll/flush
  epoch, then require a later independent clean probe. Recovery must still pass
  each flow's own ACK-completion barrier.

## 2026-07-10 - Per-call burst caps did not bound outstanding ACK feedback

- Stage: H10d16 Task 11A stability closure after the first 20-run green.
- Evidence: a nominal 48-payload-packet per-flush cap still failed the bounded
  64-packet production seam (`run 4/50`, modeled drop `88`). Adding a per-flow
  ACK barrier without a cumulative window could stall around `590 KiB` in
  DrainOnly, and a 48-packet window that deducted existing `send_queue` still
  produced a one-packet modeled drop in a repeat.
- Root cause: a per-call cap is not an outstanding-byte cap; prior smoltcp
  `send_queue` bytes survive into the next actor edge. The ring model can also
  produce both an ACK and a window update for one payload packet, so the
  original one-feedback-packet assumption had no safety margin.
- Correct behavior: calculate available admission as an MTU-derived
  24-payload-packet maximum minus the current per-flow smoltcp `send_queue`.
  Keep the read reservoir and actor service target unchanged, and use up to
  eight bounded cycles to reach the service target. Lock the production seam
  to `max_tcp_payload_packets_per_flush <= 24` and require 50/50 complete,
  zero-drop repeats before another VPS run.
- Rejected alternatives: do not enlarge the modeled/kernel ring, tune MTU,
  change Quinn windows, add self-wake, or impose a global all-flows-zero barrier
  to hide this ownership error.

## 2026-07-10 - Clean 50/50 repeats hid a 56 Mbit/s architecture ceiling

- Symptom: the bounded 64 MiB production seam passed 50/50 for ownership,
  zero drop, actor exclusivity, and EOF, but a release measurement still took
  `9.51s` (`56.4 Mbit/s`), far below the `170 Mbit/s` target.
- Root cause: the test had a 15-second completion timeout but no rate
  assertion. The 16-packet local TUN RX budget also treated every normal
  feedback batch from one safe 24-payload window as backlog, producing `1915`
  pause/resume episodes and making the two-epoch circuit breaker a 5ms pacer.
- Correct behavior: add a capacity value to the same production report and
  require `>=170 Mbit/s`. Consume the modeled 48-packet feedback allowance
  before declaring backlog; only a further ready packet may trip DrainOnly.
  Keep the 24-payload cap and verify partial `send_queue` segments consume a
  full packet slot.

## 2026-07-10 - Dropping a split duplex WriteHalf did not publish mock EOF

- Symptom: the full reverse payload arrived with zero drop, but the D16 queue
  retained exactly one 128 KiB pending-read reservation and local EOF never
  became observable before timeout.
- Root cause: the mock retained the duplex ReadHalf; dropping only the split
  WriteHalf did not explicitly shut down the write direction, so the real D16
  reader correctly remained pending instead of seeing EOF.
- Correct behavior: deterministic EOF fixtures must call AsyncWrite shutdown
  after their final byte. Then assert reservation refund, queue close/empty,
  pending/inflight zero, local EOF, and terminal/close-tail zero through the
  production actor seam.

## 2026-07-10 - Full-path timeout reported a stale lifecycle snapshot

- Symptom: the real Quinn full TCP/TUN test passed three times, then timed out
  with only `Elapsed(())`. After diagnostic state was added, a repeat showed all
  `32 MiB` received, generator `CloseWait`, empty TUN rings, zero drop, and clean
  QUIC, while the harness snapshot still reported `34507B` owned/inflight and
  no local EOF.
- Root cause: D16 harness lifecycle observations were refreshed only by
  `record_local_egress_service_window`. A later control-only
  `process_dirty_relay` pass could complete the EOF transition without another
  egress-service window, leaving the test sink stale.
- Correct behavior: on timeout capture endpoint, queue, actor, task, and QUIC
  state before cleanup. Refresh test-only lifecycle observations after every
  dirty-relay pass; do not change queue/permit/EOF production behavior to make
  a stale metric green.

## 2026-07-10 - The harness integration target is concurrency_harness

- Symptom: `cargo test --features harness --test integration` failed because
  the repository has no `integration` test target.
- Correct behavior: use
  `cargo test --features harness --test concurrency_harness`; current expected
  result is `10 passed` with `4` existing ignored tests.

## 2026-07-10 - Parallel direct baselines collided on the iperf3 service

- Symptom: `.27 -> .77` and `.33 -> .77` direct reverse baselines were started
  concurrently. The first completed, while the second JSON had no
  `sum_received` field because the target iperf3 service was already occupied.
- Correct behavior: run acceptance baselines sequentially when the target has a
  single iperf3 server instance, and fail explicitly on the JSON `error` field
  before reading throughput values.

## 2026-07-10 - Clean D16 commit did not include the D16 acceptance runner

- Symptom: the planned isolated deployment pointed at `1bf1f78`, but that
  commit's suite script has no H10d16 option/export. The working runner only
  gains D3-D6/D11/D16 flags and current parsers from uncommitted script diffs.
- Correct behavior: version the binary source and acceptance runner together,
  record their hashes, and fail before traffic unless the D16 startup/profile
  fingerprint is present.

## 2026-07-10 - Full real-Quinn gate used the default runtime config

- Symptom: the test named as the complete D16 TCP/TUN path directly supplied a
  `NativeByteOwned` relay but used `TunRuntimeConfig::from_sources`, whose TUN
  MTU is `1500` and H10d16 flag is false. Gate A uses safe1200.
- Correct behavior: provide one test-only exact Gate A config constructor and
  assert its fingerprint before running the existing real-Quinn tracer bullet.
  Keep the relay boundary real; do not compensate with shallow flag tests.

## 2026-07-11 - Full formatting is red outside the D16 stage

- Symptom: `cargo fmt --all -- --check` in the clean R4 worktree reported large
  diffs in previously committed Reality, DNS, failover, metrics, and main files.
- Root cause: those non-D16 files predate the R1-R4 commits and are not formatted
  under the current toolchain. The primary worktree also contains overlapping
  user edits, so a bulk formatter would mix unrelated changes into this stage.
- Correct behavior: keep the full-format result visible, run focused D16
  `rustfmt --check --edition 2024 --config skip_children=true`, and schedule the
  independent formatting debt only with an explicit commit strategy.
- Command correction: direct `rustfmt` must use this crate's Rust 2024 edition;
  forcing edition 2021 produces false let-chain parse errors.

## 2026-07-11 - Post-R1-R4 mature control remained externally incapable

- Symptom: the versioned historical-MTU1500 control returned receiver
  `11.219 Mbit/s` despite `216.172/213.865 Mbit/s` direct reverse baselines.
  Its one-second profile contained repeated zero-rate intervals.
- Discriminators: target/Exit routes were correct, both UDP sockets reported
  `rb=tb=16777216` and drop `0`, sing-box accepted the TUIC flows and opened the
  target, and cleanup restored `.27` sysctls/TUN/routes.
- Correct behavior: treat the window as shared TUIC incapability, keep Gate A/B
  unspent, and do not repeat the unchanged control until independent external
  state has changed.

## 2026-07-11 - Gate A mixed a timed peer reset with a zero-tail EOF assertion

- Symptom: the alternate-Exit Gate A passed capacity at `183 Mbit/s` receiver
  with zero TUN/QUIC/actor errors, but reported `524288B` terminal owned bytes
  and `27840B` terminal smoltcp egress.
- Root cause: the iperf data socket moved directly from `Established` to
  `Closed` at its 20-second time boundary before the data TUIC stream observed
  remote EOF. The peer abort made both remaining reservoirs undeliverable; a
  later wait cannot drain an already terminal TCP socket.
- Correct behavior: do not repair this by changing MTU, read chunk, queue cap,
  pool, QUIC windows, or egress pacing. First preserve the local terminal cause
  and its exact accounting. Then prove capacity with the timed flow and clean
  EOF with an EOF-terminated flow, or replace the strict gate's generator with
  one that guarantees EOF.

## 2026-07-11 - Sudo producer did not make outer FIFO redirection privileged

- Symptom: a transient Exit A/B failed before startup because an unprivileged
  shell could not redirect configuration into a root-owned mode-0600 FIFO,
  even though the producer command itself used `sudo`.
- Root cause: the calling shell opens `>` before executing the elevated
  producer; privilege does not apply retroactively to the redirection.
- Correct behavior: create the FIFO with the writer's owner and mode `0600`, or
  perform the redirection inside an explicitly elevated shell. Keep all
  cleanup idempotent and verify the original service is restored after failed
  transient starts.

## 2026-07-11 - Current clippy adds baseline warnings outside the focused fix

- Symptom: `cargo clippy -q --lib -- -D warnings` failed at 13 existing sites:
  enum postfix naming, pre-existing functions with more than seven arguments,
  and old min/max clamp forms. The terminal-state diff did not introduce a new
  warning site.
- Correct behavior: keep focused compile/tests/fmt/diff gates authoritative for
  the D16 lifecycle fix, record strict clippy as repository/toolchain debt, and
  do not mix a broad mechanical cleanup into overlapping user changes. Schedule
  that cleanup as a separate coherent commit.

## 2026-07-11 - Fixed idle age recycled a healthy auxiliary connection

- Symptom correction: pool-2 contained a destructive fixed-idle reconnect and
  a later A-clean flow exercised it, but the failed capacity flow itself used
  auxiliary `conn=1` without reconnect. Pool 1 on persistent primary reached
  `115 Mbit/s` with zero local pressure.
- Root cause status: the fixed `10s` reconnect is a confirmed policy defect,
  but the clean no-reconnect A/B later rejected it as the active capacity root.
  A prior auxiliary run reached `183 Mbit/s`, so auxiliary connections are not
  intrinsically incapable.
- Correct behavior: never infer transport death from idle time alone. Preserve
  a healthy authenticated slot until Quinn close state or a bounded transport
  operation supplies failure evidence; prove the selected slot/generation in
  the acceptance artifact.

## 2026-07-11 - Minimal temporary TUIC config omitted a required outbound tag

- Symptom: the first minimal temporary Exit failed startup because a route
  referenced the direct outbound but the generated source outbound had no tag.
- Correct behavior: when reducing a mature server config for a discriminator,
  validate every route reference and run a config check before replacing the
  live temporary process. A reduced config must also pass the mature-client
  capability floor before it can qualify Gate A.

## 2026-07-11 - Temporary full Exit config required privileged listeners

- Symptom: the first temporary `.77` Exit start failed because the streamed
  full sing-box configuration also declared an existing privileged TCP port;
  running the temporary process as the unprivileged user could not bind it.
- Correct behavior: validate the exact inherited configuration and listener
  privileges before replacing a temporary service. If elevated execution is
  required, keep credentials in mode-0600 FIFOs, use idempotent cleanup, and
  restore socket defaults before socket maxima.

## 2026-07-11 - macOS sandbox blocked Quinn UDP binds

- Symptom: a full local Rust test run failed every real Quinn UDP bind with
  `EPERM` inside the command sandbox.
- Correct behavior: treat this as host policy, not TUN or transport failure;
  rerun only the Quinn loopback test outside that sandbox. Do not run real TUN
  acceptance on macOS. Use `.27` for TUN and full-path VPS validation.

## 2026-07-12 - Clean worktree runtime and alternate-Exit controls

- Symptom: the first diagnostic command sourced `./.env` inside a clean `/tmp`
  worktree, where the intentionally untracked file does not exist. A second
  attempt omitted the suite's `TARGET` variable and correctly tripped recursive
  routing protection because Exit and Target both resolved to `.77`.
- Correct behavior: source `/home/ubuntu/mini_vpn/.env` without printing it,
  and set both `MINI_VPN_TUIC_SERVER` and the independent suite `TARGET` when
  roles are reversed. Verify that Exit stays on `eth0` before traffic.

## 2026-07-12 - Unordered frontier exhausted the D16 ownership cap

- Symptom: the reservation-owned unordered prototype delivered about `2 MiB`
  in Quinn loopback, then a missing offset allowed later chunks to consume the
  full `524288B` per-flow ledger before retransmission arrived.
- Correct behavior: do not promote the old unordered diagnostic or enlarge the
  D16 reservoir as a throughput shortcut. Keep direct ordered transport
  ownership and use unordered offsets only as a bounded discriminator.

## 2026-07-12 - Fresh temporary Exit did not guarantee a capable window

- Symptom: mature controls against rebuilt temporary `.77` Exit instances
  varied from `172.167` to `18.873 Mbit/s` while direct receivers stayed above
  `217 Mbit/s` and both UDP socket drops stayed zero.
- Correct behavior: restart is not an acceptance precondition. Require the
  versioned mature control to pass immediately before every scoped mini_vpn
  run, and leave Gate A unspent when it does not.

## 2026-07-12 - Long transfer sessions must be polled to completion

- Symptom: repeated rsync calls were started after the command returned a live
  session identifier, so multiple writers modified the same binary and caused
  truncation, checksum changes, and `Text file busy`.
- Correct behavior: when a long transfer returns a session identifier, poll
  that one session until it exits before verifying or starting another writer.
  Treat exact size and SHA-256 equality as deployment prerequisites.

## 2026-07-12 - Nested SSH stdin roles cannot share one `-n` policy

- Symptom: nested SSH first consumed the rest of a heredoc; applying `-n` to
  every nested SSH then made pipeline sinks read `/dev/null`, producing an
  empty config FIFO with successful writer status.
- Correct behavior: use `ssh -n` for control/source commands that must not
  consume the orchestration script. A pipeline sink that must receive bytes on
  stdin must omit `-n` and receive only the explicit pipe.

## 2026-07-12 - TLS config checks consume FIFO-backed key material

- Symptom: a standalone sing-box config check blocked after consuming its
  config FIFO because certificate/key FIFOs had no compatible one-shot reader
  lifecycle.
- Correct behavior: do not assume a static check only parses JSON. When TLS
  material is intentionally non-persistent, let one fail-closed transient run
  consume all three FIFOs and require an active unit plus the expected UDP
  listener before capability traffic.

## 2026-07-12 - A listening alternate port was blocked before the VPS

- Symptom: the independent `.33:9443` sing-box unit was active with a healthy
  UDP socket, but the mature control timed out before server accept and did not
  produce a throughput result.
- Discriminator: a simultaneous capture saw the probe sent to allowed port
  `8443` and did not see the probe sent to `9443`.
- Correct behavior: add a host-arrival probe before an alternate-port control.
  Do not diagnose authentication, TLS, QUIC, or client throughput until the
  server host has observed the packet.

## 2026-07-12 - Minimal same-port Exit remained burst/idle

- Symptom: replacing the original `.33:8443` process with a minimal
  same-version service preserved the low mature-client result (`11.953
  Mbit/s`) and thirteen zero-rate seconds despite healthy direct paths and zero
  socket drops.
- Correct behavior: do not repeat service restarts, full/minimal config swaps,
  or mini_vpn scoped runs on this host window. Treat the `.33` host or external
  QUIC path as the blocker until a host-local discriminator or a genuinely
  independent Exit changes the evidence.

## 2026-07-12 - Independent Exit did not rescue the historical control

- Symptom: a new `.111` Exit with healthy direct capacity, verified UDP
  arrival, full socket buffers, and zero drops still produced only `4.928
  Mbit/s` under the fixed historical BBR/MTU1500 mature control.
- Review finding: the sole precondition profile does not match the authorized
  product path, which uses Cubic/safe1200; project source already warns that
  BBR may underperform Cubic on affected paths.
- Correct behavior: do not repeat the historical control on more Exits or run
  mini_vpn without qualification. TDD a fixed gate-aligned mature profile,
  preserve the historical profile as diagnostic evidence, and keep the same
  `>150 Mbit/s` plus zero-drop floor.

## 2026-07-12 - Cargo name filters can succeed after running zero tests

- Symptom: a clean `.27` command used a remembered D16 test name with
  `--exact`; Cargo exited successfully while every target reported `0 passed`
  and all tests filtered out.
- Root cause: Cargo treats a filter with no matches as a successful test run.
  The requested behavior therefore had no evidence even though the command
  status was zero.
- Correct behavior: resolve the current test name from source first and verify
  that the output reports at least one executed test. For focused acceptance,
  a zero-match Cargo result is a failed evidence gate, not a pass.

## 2026-07-12 - SSH identity use does not imply agent forwarding

- Symptom: the Mac connected to `.33` with `ssh -i`, but an `-A` session could
  not authenticate `.33 -> .111`; the identity file had never been loaded into
  the forwarded agent.
- Correct behavior: for nested ephemeral access, start a temporary local
  ssh-agent, add the existing identity in memory, forward that agent, and place
  only its public key selector on the intermediate host. Never copy the private
  key to a VPS.

## 2026-07-12 - Tcpdump filter counters are not an arrival assertion

- Symptom: the first `.33 -> .111:8443` probe timed out with `0 packets
  captured` but `1 packet received by filter`; treating the latter as success
  would have bypassed the host-arrival gate.
- Correct behavior: require an actual ingress capture line and exit status
  zero. Bind to the ingress interface/direction, wait for capture readiness,
  and send several bounded probes before classifying an upstream ACL.

## 2026-07-12 - Iperf3 UDP still depends on a TCP control path

- Symptom: an iperf3 server listened on `.111:8443`, but `.27` and `.33`
  clients timed out before UDP measurement. An ingress capture saw no TCP SYN
  even though UDP8443 probes reached the host.
- Root cause: iperf3's UDP data mode still establishes its control session over
  TCP. The cloud policy exposed only the TUIC UDP port.
- Correct behavior: preflight both protocols required by a benchmark. For a
  UDP-only allowed port, use a fixed mature tool whose reverse mode stays on
  one UDP socket; do not interpret control-channel failure as UDP incapacity.

## 2026-07-12 - Offered 200M crossed a shared raw UDP shaping edge

- Symptom: both clients received five seconds near `210 Mbit/s` with zero
  loss, then settled near `193 Mbit/s` with `7.9%` interval loss; client
  `UdpRcvbufErrors` remained zero.
- Correct behavior: classify this as shared sender/egress/provider headroom,
  not a client buffer bug or multi-second service stall. Preserve it as a
  ceiling signal and test QUIC below/through the edge with congestion metrics.

## 2026-07-13 - Gate verdict must apply the latest architecture amendment

- Symptom: the first manual reading called the composite run a tail failure
  because A-capacity ended with nonzero terminal ownership and smoltcp egress.
- Root cause: that reading applied the older single-window zero-tail rule
  before re-grounding in the later architecture amendment. The approved AND
  gate permits one exact bounded `local_socket_terminal` in timed A-capacity
  and assigns strict zero-tail proof to fixed-byte A-clean.
- Correct behavior: before declaring any versioned gate PASS or FAIL, quote the
  latest source-of-truth decision table and classify each evidence window
  separately. A later amendment supersedes historical failure wording.

## 2026-07-13 - Remote evidence commands cannot assume ripgrep

- Symptom: the first read-only report parser on `.27` produced no evidence
  because `rg` was not installed.
- Correct behavior: use `command -v rg` and fall back to POSIX `grep`/`sed` in
  VPS evidence commands. A missing convenience tool must not be mistaken for
  an empty report.

## 2026-07-13 - Temporary Shoes/TLS preflight must match both clients

- Symptom: the first Shoes config omitted the required `h3` ALPN and both QUIC
  clients failed negotiation. A later self-signed CA certificate was also used
  directly as the server leaf; sing-box accepted it, but rustls rejected it as
  `CaUsedAsEndEntity`.
- Correct behavior: assert the server ALPN before launch and use a temporary CA
  only to sign a distinct `CA:false`, server-auth leaf with the required SAN.
  Run both client handshakes before counting a control. These were preflight
  configuration failures and did not consume measurement samples.

## 2026-07-13 - Noninteractive sudo must be explicit in remote runners

- Symptom: `sudo -v` prompted even though the required commands had NOPASSWD
  authorization, preventing an otherwise unattended suite from starting.
- Correct behavior: when the host policy supports it, validate with `sudo -n
  true` and fail before mini_vpn/iperf if it is unavailable. Never place a sudo
  password in a command, log, report, or repository file.

## 2026-07-13 - Active socket evidence cannot be sampled after cleanup

- Symptom: the manual mini_vpn client `ss` snapshot ran after the suite had
  already stopped the process, leaving an empty evidence file.
- Correct behavior: have the versioned runner take client and Exit UDP socket
  snapshots inside the active iperf interval. A post-run snapshot is useful
  only for proving cleanup, not socket error deltas. In this run, bilateral
  pcap bytes plus Quinn TX counters and zero tcpdump drops still located the
  loss boundary, but the missing active client `ss` evidence must not recur.

## 2026-07-13 - Cleanup process checks must not match their own shell

- Symptom: a cleanup guard using `pgrep -f` matched the remote shell command
  text containing the mini_vpn path and aborted before removing temporary
  directories, even though no mini_vpn process remained.
- Correct behavior: use an executable-name query such as `ps -C mini_vpn` or a
  PID file, then separately assert TUN, route, and directory absence. Rerun the
  idempotent cleanup and verify every final condition.

## 2026-07-13 - A successful suite exit can still contain a failed gate

- Symptom: the disabled-GSO forward sample completed iperf and produced a
  bundle with exit status zero despite `30` TUN TX drops and tens of megabytes
  of QUIC loss.
- Root cause: the outer suite invokes standard P1 with `|| true`, and the
  low-RTT probe reports counters but does not apply the product discriminator.
- Correct behavior: explicit discriminator modes must propagate probe failure
  and evaluate their own TUN/loss contract before a successful exit. Manual
  adjudication remains authoritative until that red/green runner repair lands.

## 2026-07-13 - Full-tunnel gold checks are invalid under target-only routing

- Symptom: a target-only acceptance report said curl must show the Exit IP and
  DNS must return fake-IP, then recorded the client IP and public DNS answers.
- Root cause: only the target `/32` was routed into TUN; curl and DNS were
  deliberately outside the measured tunnel.
- Correct behavior: parameterize the probe's routing mode. Under target-only
  routing, assert only target-through-TUN and Exit-bypass; label curl/fake-DNS
  checks not applicable rather than presenting expected direct results as a
  failed gold check.

## 2026-07-13 - Large pcaps should be summarized where they were captured

- Symptom: sequentially copying four 50-80 MiB pcaps over a slow control link
  left a partial local file and delayed cleanup.
- Correct behavior: first verify source hashes and tcpdump drop counters, then
  run allowlisted byte/timing aggregation on the capture host. Retain the
  sanitized report bundle and exact summaries locally; copy full pcaps only
  when packet-level offline inspection is still required, and never accept a
  partial destination without matching the source hash.

## 2026-07-13 - Nominal 48/2ms math overstated achieved send-service capacity

- Symptom: deterministic bounded-service tests passed and the real GSO-enabled
  upload delivered exact `32 MiB` with clean EOF, but throughput was only
  `94.172 Mbit/s` instead of the required `>170 Mbit/s`.
- Root cause class: the implementation starts a full 2ms cooldown after each
  exhausted batch; the capacity proof assumed a 2ms achieved batch period but
  did not measure timer wake lateness or driver scheduling overhead.
- Correct behavior: fail the local sufficiency gate, do not run VPS, and add
  actual deadline/rearm/service-rate discriminators before selecting a new
  mechanism. Do not adjust D16, MTU, pool, windows, chunk, CC, self-wake, or
  the nominal service constants from this result alone.

## 2026-07-13 - A full post-batch cooldown double-paced Quinn

- Symptom: after adding actual timing counters, the same exact/clean `32 MiB`
  upload reached only `98.311 Mbit/s`; service rate was `10,506` datagrams/s
  versus about `17,709` required.
- Root cause: the public socket wrapper starts a fresh `now + 2ms` cooldown
  after batch work, on top of Quinn-proto's existing token-bucket pacing.
  Actual batches took `4.573ms`; even removing the measured `1.340ms` timer
  lateness leaves only about `142.5 Mbit/s` of capacity.
- Correct behavior: stop before VPS, do not retry constants, and move the next
  design gate to the pacing-rate/debt owner. Any dependency-level Quinn cap
  must preserve upstream defaults and prove whether the bound is per
  connection or aggregate across the pool.

## 2026-07-13 - Do not promote a per-connection cap to an endpoint guarantee

- Failure risk: describing `64 * mtu` per connection as a strict 64-wire-packet
  or pool-wide 128-packet sliding-window bound hides ACK-only pacing semantics,
  independent refills, auxiliary connection traffic, and migration reset.
- Correct behavior: use paced MTU-equivalent units; require cap-active metrics,
  `cwnd <= u32::MAX`, at least 95% data-connection attribution, and no
  formal-window migration for the single-flow tracer. Treat P8/product
  concurrency as necessary-only. If aggregate burst/fairness still fails,
  stop and design a shared coordinator inside Quinn-proto before send
  accounting; never stack another socket gate.

## 2026-07-13 - A retired negative tracer broke the default library gate

- Symptom: after the new pacer-cap64 gate passed at `537.106 Mbit/s`, full
  `cargo test --lib` ended at `620 passed / 1 failed / 2 ignored`. The failure
  was `bounded_send_service_real_loopback_upload_delivers_fixed_bytes_and_clean_eof`
  at `101.084 Mbit/s` because it still unconditionally required `>170`.
- Root cause: the old fixed `48 then 2ms` socket wrapper had already been
  measured and rejected as mathematically insufficient, but its known-negative
  real replay remained in the default regression suite as a product gate.
- Correct behavior: stop before broader gates/VPS, review first, then make the
  rejected replay explicit/ignored while keeping byte conservation, GSO
  accounting, timer, no-busy-wake, and exact lifecycle tests green.
- Review also found three pre-VPS blockers: the patched default pacer path
  currently repeats capacity math, the runner does not export or verify
  `MINI_VPN_TUIC_PACING_POLICY`, and periodic stats omit `current_mtu` needed
  to compare against `pacing_mtu`.

## 2026-07-13 - Pacer observations have deferred debt and MTU epochs

- Symptom: an early rate test expected an already-sendable pacer to eagerly
  refill to full; a stats test expected `current_mtu == pacing_mtu` immediately
  after PLPMTUD advanced the path.
- Root cause: Quinn preserves elapsed refill as deferred debt when current
  tokens already cover the send, and the pacer updates its MTU only on its next
  `delay()` accounting call.
- Correct behavior: test refill from an empty bucket, expose both MTU values,
  and require equality only in a formal active snapshot where the cap theorem
  is claimed.

## 2026-07-13 - Vendored crate targets are not covered by root /target ignore

- Symptom: running the vendored Quinn suite created hundreds of MiB under
  `third_party/quinn-proto-0.11.16/target`, which the root `/target` ignore does
  not match.
- Correct behavior: run `cargo clean --manifest-path` for the vendored crate
  before handoff and verify no nested build output is left in the untracked
  vendor tree.
