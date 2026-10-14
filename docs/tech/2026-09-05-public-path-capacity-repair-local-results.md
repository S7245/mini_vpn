# Public-path repairs: accepted local closure

Date: 2026-09-05.

Status: **PUBLIC-PATH LOCAL CLOSURE ACCEPTED; ORIGINAL REVIEWER APPROVE 0/0/0.**

The user explicitly authorized the next attempt under the independently accepted
[TUIC retirement-capacity amendment](2026-09-04-tuic-udp-stream-retirement-capacity-amendment.md),
the same Reviewer's code closure, and public-path governance/provenance closure.
The coordinating task relayed this authorization on 2026-09-05. It satisfies
the prior failed-review confirmation stop; it does not relax the design, tests,
review, or subsequent-failure stop rule.

This attempt reached coherent public-path local closure after the separately
authorized format repair. The historical stops below remain unchanged evidence.
The two Quinn client-root repairs are frozen. P2T remains with the coordinating
task; no P2T Writer, parent, frozen block or migration leaf is modified here.
There is no commit, push, macOS TUN, VPS, WAN, throughput or M2 authorization.

## Final accepted public scope and evidence

The combined public-path repair is now locally accepted. Fake-IP exhaustion
rejects allocation without replacing a live route and the DNS caller returns
SERVFAIL. IPv4 UDP construction rejects invalid size without panicking;
reassembly maintains exact bounded fragment ownership. The TUIC stream fallback
settles in one public poll and keeps all unretired stream episodes within one
256-slot Authority across generations. Original Popper closed the last sender
P2 at P0/P1/P2 `0/0/0`, Approve, and confirmed the final format-only delta at the
same verdict. The earlier portion reviews remain recorded in the
[preceding public result](2026-09-04-public-path-review-repair-local-results.md).

| Final check | Exact command / selection | Outcome |
|---|---|---|
| Standalone governance build | `rustc -D warnings --edition=2024 --test tests/code_growth_governance.rs -o /tmp/mini-vpn-public-capacity-20260905-v2pmtb9g/governance-final` | exit 0 |
| Standalone governance | `/tmp/mini-vpn-public-capacity-20260905-v2pmtb9g/governance-final --test-threads=1` | 6/6 |
| All-feature governance | `cargo test --offline --all-features --test code_growth_governance` | 6/6 |
| Knife16 / repository provenance | `cargo test --offline --test repository_test_module_provenance --test knife16_owned_leg_provenance` | 159/159 and 13/13 |
| Fake-IP | `cargo test --offline --lib fake_ip::tests::` | 10/10 |
| DNS | `cargo test --offline --lib dns::tests::` | 6/6 |
| IPv4 UDP builder | `cargo test --offline --lib udp_relay::tests::` | 6/6 |
| DNS caller characterization | `cargo test --offline --lib client_tun::tests::dns_characterization::` | 5/5 |
| Real full-pool DNS caller | `cargo test --offline --lib client_tun::dns_hijack::tests::` | 1/1 |
| DNS injection counters | `cargo test --offline --lib client_tun::tests::handle_dns_hijack_counts_forge_and_drop -- --exact` | 1/1 |
| Current UDP sender/reassembly | `cargo test --offline --lib tuic::udp_ -- --nocapture` | 22/22 |
| Current isolated Quinn | `cargo test --offline --manifest-path /tmp/mini_vpn_quinn_capacity_mokb8_gx/quinn-0.11.11/Cargo.toml -- --nocapture` | 49 unit + 1 doc passed; 3 unit + 1 integration ignored |
| Scoped repository format | `rustfmt --check --edition 2024 --config skip_children=true` on the 20 explicit paths in the gate plan | exit 0 |
| Scoped Quinn format | Identical four-file command recorded below; after the authorized export move | exit 0 |
| Whole diff | `git diff --check` | exit 0 |
| Final all-feature library | `cargo check --offline --all-features --lib` | exit 0 |

Commands ran serially. The earlier 17-test checkpoint was not reused as the
current UDP result. The successful behavior suites were not rerun solely for
the final export-order change: original Popper verified by inverse movement
that it reproduces the exact previously reviewed source hash, with no API,
visibility, attribute or semantic change. Existing dependency warnings remain;
this is not a claim of whole-project Clippy cleanliness or full-library testing.

Final gate logs and the original failed format log are retained in
`/tmp/mini-vpn-public-capacity-20260905-v2pmtb9g/`. Exact command arrays, exit codes,
counts and individual log paths are in `final-gate-plan.json`,
`final-gate-results.json` (including the historical stop), and
`authorized-final-tail-results.json`. The authorized format GREEN is
`/tmp/mini_vpn_udp_capacity_quinn_fmt_authorized_green.log`. The final tail logs
are `authorized-final-diff-check.log` and `authorized-final-library-check.log`.

All four public shrink entries are now promoted at their final measured sizes:
`client_tun.rs 16561`, `tuic.rs 7991`, client-TUN test index `14230`, and TUIC
test index `5390`. No other inventory entry, allowance, P2T parent or root
changed. Governance SHA-256:
`631cbd210d37876cb2812ee8841d06517a6553bdd28dcd49fc167f42e0d271ee`.

The final public source manifest is
`/tmp/mini-vpn-public-capacity-20260905-v2pmtb9g/public-closure-source-manifest.json`,
SHA-256 `2db46c5715d022d5f0287b3eac0e499748cdefc8a1db6680ff2abb6934a48827`.
It records the unchanged HEAD, final Rust source counts/hashes and the frozen
root/P2T/provenance parents. The dedicated final sender/Quinn manifests are:

- Nine-source `post-export-format-owned-source-lines-sha256.txt`, SHA-256
  `13e40afdeecdd67c3a92576b6e73dabe2f3107a21a9858e51326e3412f890509`.
- 91-source `post-export-format-source-sha256.txt`, SHA-256
  `29339e738c8db6f96e70579ae4cc0b96759dca4acd8a1efc1a99cb3b60232e93`.

Both are under `/tmp/mini_vpn_quinn_capacity_mokb8_gx/`. The only post-behavior-run
source delta is the complete two-line export-group move in Quinn `lib.rs`,
remaining 155 lines with SHA-256
`59d348dec65e4cef30866a2dc146998ba7eab6189226e39e8cfde6b8a7a1367b`.
The other eight owned sources and all proto sources are unchanged. The previous
`final-*` manifests remain immutable provenance for the actual behavior runs.
Original Popper verified both new manifests and the isolated copy; Herschel
explicitly stopped writing after this final delta. Independent read-only
closure audit by Zeno confirmed the final 29-source manifest, 9/91-source
manifests/copy, all recorded test counts, preserved failed logs and consistent
compact state. The Integrator completed the exact inventory, final gates and
these records, then stopped repository writes for handoff to the coordinating
task. Artifact/evidence fingerprints are retained in the Integrator snapshot;
no further Writer or Integrator source/document work is pending here.

The public path still reaches `LegacyOwnedUpstream`. The 16.5 MiB bound applies
to outstanding submitted Packet bytes, plus bounded episode metadata; it is not
a process-wide QUIC memory ceiling or throughput acceptance. Both client-root
combination repairs remain frozen, and no R7/P2T Rust was changed. Public closure
removes the coordinating task's shared-governance dependency; that task still
owns P2T's source revalidation, pre-Move baselines, migration and acceptance.
No Case C implementation, public TUN cutover, external run or commit/push was
performed. The remaining sections preserve the exact attempts and their stops.

## Baseline and ownership

- Shared starting HEAD: `a7c9a95438386739e467061ac961fecf3bbe9d60`.
  The user saved the workspace before this attempt. A clean worktree is not
  acceptance evidence.
- Original sole TUIC Writer restored: `/root/udp_repair_design` (Herschel).
- Original independent Reviewer restored: `/root/review_public_repairs`
  (Popper). The agent confirmed its identity and original RESET P2 plus the
  `046263ff...653f061` design review history. R7 Reviewer Heisenberg is not
  substituted.
- Accepted amendment at entry SHA-256:
  `6a17305109ec1625041954b6aa449142ac5d3559f55fc6d68e7007b59a1a5be6`.
- Writer snapshot:
  `/var/folders/l8/np2lxkgd3wg_dk5mbm9cpry40000gn/T/mini_vpn_udp_capacity_rgurf5yc`.
- Integrator snapshot and exact manifest:
  `/tmp/mini-vpn-public-capacity-20260905-v2pmtb9g/manifest.json`.

Allowed Writer changes: `src/tuic.rs` capacity storage/constructor wiring,
`src/tuic/udp_send.rs` and its private external tests; vendored Quinn
`connection.rs`, `send_stream.rs` only if construction requires it, the `lib.rs`
export seam, and corresponding private external tests. No `quinn-proto`
mutation, handshake rewrite, unrelated Rust or inventory update is allowed.

The Integrator owns later exact governance changes, applicable provenance gates,
the public result and compact state. The four existing public-path shrink
entries remained untouched until sender implementation and same-Reviewer
closure, then were lowered and validated.

## Frozen causal contract

One TUIC Authority holds 256 retirement slots across all connection generations,
examines at most four rotating slots per call, and reserves before opening.
Atomic 1-RTT admission rejects handshaking before consuming a stream ID. The
weak-only ticket releases only for exact send-map absence or a Release/Acquire
token published after State/shared destruction. FIN, RESET, STOP, byte ACK,
close, driver exit and Weak failure alone are insufficient.

The first causal tracer grants more than 256 uni streams with zero/partial
stream credit, then sends 257 public UDP packets without yielding to ACK
processing. The next actual `Connection::open_uni` stream ID independently
counts admissions. The old one-shot implementation must expose 257; the bounded
implementation must stop at 256. Keep the existing 16 UDP tests and add actual
ACK retirement, cross-generation/other-TCP-handle retention, destructor-window,
handshaking, interruption and poison tests incrementally.

Each unexpected compile/test failure or new Review Request Changes stops this
attempt. Report the observed causal branch and minimum next plan; do not apply
a second repair or weaken a tracer. Expected initial causal RED is diagnosis.

## Frozen non-target fingerprints

| File | Lines | SHA-256 at entry |
|---|---:|---|
| `src/owned_upstream/quinn_leg_actor/tests/mod.rs` | 16288 | `14ff8c18a200345c0519d0a703950a4bfbb4bfeaffec9213bf20c76adee34333` |
| `tests/knife16_owned_leg_provenance.rs` | 2988 | `b988c0655eca940d884f925fa95c59aa50def818df3701114937e8357ec8d7aa` |
| `src/owned_upstream/quinn_client_root.rs` | 6468 | `46010f019660309adcfc4fce246387d22ca15168e796693e5d209ba3e47fa80f` |

## Historical E0505 stop: tracer borrow outlives its fixture

This section preserves the first stop's evidence. The later confirmation below
supersedes its execution hold, not its diagnostic result.

The original 16-test UDP baseline passed. The first capacity tracer was added
only to `src/tuic/udp_send/tests/uplink.rs`, changing it from 301 to 327 lines.
It did not reach a behavior RED: compilation failed with `E0505`.

At line 312, `pin!(pair.connection.open_uni())` creates an implicit pinned
temporary borrowing `pair.connection` until the surrounding loop body ends.
At line 325, `pair.close(self)` attempts to move the whole fixture before that
temporary is dropped. The compiler diagnoses exactly this borrow/move conflict
in `/tmp/mini_vpn_udp_capacity_red.log`. Production sender and Quinn code have
not been changed; no next fixture repair was performed.

The failure tree is:

1. **Confirmed:** the tracer's borrow scope prevents compilation.
2. **Not yet tested:** the old sender admits 257 unretired streams where the
   new bound must allow 256. The assertion has not executed.
3. **Not started:** fixed-slot admission, exact retirement, 1-RTT and guard
   implementation; there is no implementation GREEN or closure verdict.

The next minimum proposal is limited to the tracer:

- Put the pinned `open_uni` and single poll in an inner block, returning its
  `Poll<Result<SendStream, _>>`. The borrow ends before consuming `pair`.
- Save the observed stream index and drop count, reset/drop the probe, drop
  the upstream and explicitly close the fixture before the final capacity
  assertions. The expected behavior RED must also clean its real endpoints.
- Preserve 257 sends, the exact 256 admission assertion, 1024 peer stream
  credit, both byte windows `[0, 16]`, current-thread runtime and first-poll
  completion. Do not clone the connection merely to extend its lifetime,
  weaken assertions or change pressure/timing parameters.

At that stop the proposal was not implemented. The coordination task received
the exact plan, and the Writer waited for the separately recorded confirmation
below before repairing the fixture or starting production work.

Original Reviewer Popper independently confirmed the borrow diagnosis and this
fixture-only scope/cleanup plan. It preserves the accepted architecture and
does not constitute an implementation approval. The Reviewer remained read-only
and ran no Cargo commands.

## Subsequent explicit confirmation on 2026-09-05

The user confirmed exactly the fixture-only scope and cleanup proposal through
the same coordinating task. The original Writer Herschel has resumed. Only the
full-qualified exact tracer is rerun after the fixture repair:

```text
cargo test --offline --lib tuic::udp_send::tests::uplink::public_udp_reset_pressure_bounds_physical_stream_admission -- --exact --nocapture
```

A precise expected 257-versus-256 capacity behavior RED permits continuing the
already approved capacity implementation, its vertical TDD, original Reviewer
Popper's closure, and public governance/provenance closure without another
confirmation at that expected RED. Any unexpected compile/test red or new
Request Changes still stops the next repair. The original E0505 log remains
unchanged. All Writer files, fork scope and frozen non-goals remain unchanged.

The authorized fixture repair completed and the exact command above produced
the predicted behavior RED: `window=0`, physical admission `left: 257`,
`right: 256`, one test failed. Its real probe/upstream/endpoints were explicitly
cleaned before that assertion. Log:
`/tmp/mini_vpn_udp_capacity_fixture_authorized_red.log`.
The original production sender was still unchanged at this RED. It is valid
causal evidence for the capacity branch, and the Writer has now started the
existing approved implementation.

The first implementation passed the same exact capacity tracer (`1/1`, both
zero and partial byte windows). Log:
`/tmp/mini_vpn_udp_capacity_first_green.log`. The subsequent
`cargo test --offline --lib tuic::udp_ -- --nocapture` passed `17/17` in
`/tmp/mini_vpn_udp_capacity_first_regression.log`. This proves the initial
admission bound and preserves the existing UDP tests, but does not close
retirement, cross-generation, destruction, handshaking, interruption, code
review or governance. Those remain required before promotion.

## Historical E0433 stop: missing test-only Waker import

The Writer next passed one actual RESET-ACK retirement/reuse tracer in
`/tmp/mini_vpn_udp_capacity_reset_ack.log` and one cross-generation/other-TCP
reference retirement tracer in `/tmp/mini_vpn_udp_capacity_generations.log`;
each selected exactly one test and passed. The production implementation was still at the first GREEN version at that checkpoint.

The subsequent interruption test did not compile. Command:

```text
cargo test --offline --lib tuic::udp_send::tests::uplink::public_udp_interruption_resets_and_keeps_poisoned_custody_occupied -- --exact --nocapture
```

Log: `/tmp/mini_vpn_udp_capacity_interruption.log`. Compiler `E0433` identifies
three `Waker::noop()` references in the private test leaf at lines 375, 454 and
463. That leaf imports `crate::tuic::*` and `std::future::Future`, but not
`std::task::Waker`. A prior test's lock-held polling was also made synchronous
before this compile, so the old 17-test and RESET-ACK results do not prove the
whole current test tree is green. No interruption behavior assertion ran.

Confirmed cause: missing test-only name import. Unproven branches: interruption
and poison behavior, STOP/partial-ACK non-retirement, 0-RTT/handshaking and the
post-field destructor-window tracer. No production runtime failure is inferred
from this compile error, and no implementation closure is claimed.

The single minimum proposal at that stop was to add only
`use std::task::Waker;` to this leaf and rerun the unchanged exact command above.
All inputs, assertions, pressure and physical transport paths remain unchanged.
The Writer stopped before making that repair, formatting or running another
test. The coordination task received the stop and proposal; the next section records
the required confirmation and its exact outcome.

The E0433 stopped-tree line counts were `tuic.rs 7991`, `udp_send.rs 177`,
`udp_send/tests/uplink.rs 498`, vendored Quinn `connection.rs 2339` and
`lib.rs 155`. `send_stream.rs` is unchanged. Exact stopped-tree hashes are in
`/tmp/mini-vpn-public-capacity-20260905-v2pmtb9g/waker-stop-manifest.json`.
P2T's two parents, the Quinn client root, both provenance sources and governance
remain unchanged against the entry manifest. No shrink entry was lowered.
At that checkpoint no new Quinn private tracer had been written or run, and
no same-Reviewer implementation verdict had been requested.

## Explicit confirmation after the E0433 stop

The user replied “继续” after reviewing the remaining pre-P2T gates. The
coordinating task authorized original Writer Herschel to add only
`use std::task::Waker;` and first rerun the unchanged full-qualified exact
interruption command. No assertion, input, timing or production change belongs
to that fixture repair. The old E0505 and E0433 logs remain intact.

If the exact interruption test passes, this same authorization continues the
already reviewed capacity plan's remaining vertical lifecycle tracers, current
complete regressions, original Reviewer Popper's code review, and public
Integrator closure. It does not require another confirmation at that GREEN.
Any unexpected new red or Request Changes still stops; no second Writer, root,
R7/P2T, commit/push or external action is authorized. The previous 17-test
checkpoint cannot substitute for tests of the final current source.

The Writer verified all five stopped-tree source hashes before adding that
single import. The original exact interruption command passed `1/1` in
`/tmp/mini_vpn_udp_capacity_interruption_authorized_green.log`. Its injected
panic is intentionally caught by the tracer; the test verdict is GREEN.
No other source change was part of that repair. Remaining private Quinn and
lifecycle tracers now continue under the existing scope and stop rules.

## Current lifecycle evidence after the authorized import repair

The original Writer added and passed each remaining lifecycle tracer through
real loopback QUIC endpoints. The coordinating task subsequently confirmed
resumption from these completed steps without rerunning them merely because
execution was interrupted.

| Focused tracer | Passed | Log |
|---|---:|---|
| RESET ACK frees custody; native bypasses the sender lock | 1 | `/tmp/mini_vpn_udp_capacity_reset_ack.log` |
| Cross-generation custody survives close and unrelated TCP handles | 1 | `/tmp/mini_vpn_udp_capacity_generations.log` |
| Interruption RESET, cancellation, sticky poison and native bypass | 1 | `/tmp/mini_vpn_udp_capacity_interruption_authorized_green.log` |
| Data ACK retains custody until actual FIN ACK / map removal | 1 | `/tmp/mini_vpn_udp_capacity_fin_ack.log` |
| STOP_SENDING and RESET publication retain custody until RESET ACK | 1 | `/tmp/mini_vpn_udp_capacity_stop_sending.log` |
| Real 0-RTT handshake rejects 257 admissions without consuming an ID | 1 | `/tmp/mini_vpn_udp_capacity_zero_rtt.log` |
| Real final-Arc destructor window retains custody until the final token | 1 | `/tmp/mini_vpn_udp_capacity_destructor.log` |

The destructor tracer pauses actual `ConnectionInner` destruction after its
State/shared fields and before the sentinel. In that window Weak upgrade is
unavailable, the token is false and the ticket remains unretired. Releasing the
barrier publishes the token and permits retirement. The barrier is test-only;
it is not a scalar replacement for protocol destruction.

The current full UDP selection passed `22/22` in
`/tmp/mini_vpn_udp_capacity_current_udp_regression.log`, including exact slot
occupancy and the four-inspection cursor assertion. The original Writer then
froze all implementation sources and stopped writing.
At the freeze boundary, original Popper received that exact implementation,
including the two untracked private Quinn test files, for independent review.
The later verdict is recorded below; freeze alone did not imply acceptance.

## Frozen implementation and exact isolated Quinn evidence

Final current commands:

```text
cargo test --offline --lib tuic::udp_ -- --nocapture
cargo test --offline --manifest-path /tmp/mini_vpn_quinn_capacity_mokb8_gx/quinn-0.11.11/Cargo.toml -- --nocapture
```

The former passed `22/22`; the latter passed 49 unit tests with 3 ignored,
0 integration tests with 1 ignored, and 1 doc test. There were no failures.
The four ignored stress/many-connection tests were not executed. The full Quinn run used the pre-export-order frozen-source formatting. The
later two-line export move is covered by its separate format and delta/hash
audit, without attributing the earlier behavior run to a later source hash. Logs:
`/tmp/mini_vpn_udp_capacity_current_udp_regression.log` and
`/tmp/mini_vpn_udp_capacity_quinn_full_regression.log`.

The isolated build copies the current vendored Quinn and exact local proto.
Its only manifest change is a local `[patch.crates-io]` pointing to the adjacent
proto copy. Shared manifests/locks and proto sources are unchanged; no shared
Quinn lock was created. Complete 91-file source/copy fingerprints are in
`/tmp/mini_vpn_quinn_capacity_mokb8_gx/final-source-sha256.txt`, SHA-256
`5f9e0114b03203e852bccd2bd4f8f9c9fb6af841108877599d6e933aba860323`.
The copy manifest SHA-256 is
`fddbeaa061935723a0d362d396b7da9df9e62a3a50c7869d5195bdf1aba05355`.
Earlier `source-sha256.txt` and `destructor-tracer-source-sha256.txt` are
checkpoint records; this final manifest identifies the full regression source.

The Writer's final nine-file manifest is
`/tmp/mini_vpn_quinn_capacity_mokb8_gx/final-owned-source-lines-sha256.txt`:

| Source | Lines | SHA-256 |
|---|---:|---|
| `src/tuic.rs` | 7991 | `75973fe3bd18df24a54bdea32899350883cea6cd7a0873b1e4dca1ea7fe8fad3` |
| `src/tuic/udp_send.rs` | 178 | `6027657247c75590a1a98446b40ad9625789ecc3166454c0680908fa7e5e7846` |
| `src/tuic/udp_send/tests/mod.rs` | 1 | `eb07a594cb8b3962af0e62067a590dbb978d48feee5528c0ac4dcb9d6de13c0c` |
| `src/tuic/udp_send/tests/uplink.rs` | 650 | `60ba8ae2476424aa570d15d3ce34bfee43039f742f4bf60bb95ea196c88ea2e6` |
| `third_party/quinn-0.11.11/src/connection.rs` | 2345 | `a1476ce6fc65eaa6346ef98fafd963b772224cbcd9a8c8776542f84bb2dade03` |
| `third_party/quinn-0.11.11/src/send_stream.rs` | 494 | `abbb3943f612757929b6fdb0f5ee2df3e96d421cf11c83288d6e7eca84de282b` |
| `third_party/quinn-0.11.11/src/lib.rs` | 155 | `e105b391166e73192b5287445b1c1af0e775821dc4d41b59af036588b28a0c61` |
| `third_party/quinn-0.11.11/src/connection/tests/mod.rs` | 17 | `59ba085b9a9c19114217a2a6a05c485e6d682b3bc2089618aa91c8ac4fa9be6c` |
| `third_party/quinn-0.11.11/src/connection/tests/retirement.rs` | 179 | `26fc7cabe168a8ea06aa6b7ddabe20b37dcd640b489f2fd8ec73a3254e989fe1` |

Independent source audit by Zeno confirmed all nine owned-source fingerprints,
all 91 source-manifest entries, no source omissions, and the single isolated
manifest delta. The four current parent sizes and all frozen P2T/root,
governance and repository-provenance entries also matched their snapshots.
This is source provenance evidence, not a substitute for Popper's review.

During final review Popper identified a non-blocking wording correction:
64 calls cover all slots only when every call inspects all four occupied slots
and rejects. Success may return early, so a general full rotation takes at most
256 calls. The amendment now states these conditions. This changes no slot,
scan, admission or retirement behavior and requires no source repair.

The sole Writer Herschel explicitly confirmed freeze and stopped all source
writes. No second Writer was used. Original Popper owns the implementation
verdict below; its prior design approval was not substituted for that review.

## Independent implementation closure and Integrator handoff

Original Reviewer Popper (`/root/review_public_repairs`) returned
**P0/P1/P2 `0/0/0`, Approve**, explicitly closing the original RESET-capacity P2.
The review covered production admission, custody, exact retirement, weak
reference/ref-count behavior, guard/poison settlement and real tracer causality.
Popper independently checked the full 91-file isolated source record and all
nine frozen Writer files; it ran no Cargo and made no code changes. The reviewed
nine-source manifest SHA-256 is
`daaac037dcda164621955b43e7400e853b8813897dff874560f54e2ea21d35b4`.
The scan-coverage wording clarification is non-blocking and is not a Request
Changes. This verdict makes no throughput or all-transport-zero claim.

Only after this implementation closure, the Integrator lowered the four exact
public-path grandfathered entries to the measured current source sizes:

| File | Previous inventory | Final exact inventory |
|---|---:|---:|
| `src/client_tun.rs` | 16613 | 16561 |
| `src/tuic.rs` | 8172 | 7991 |
| `src/client_tun/tests/mod.rs` | 14301 | 14230 |
| `src/tuic/tests/mod.rs` | 5499 | 5390 |

`tuic.rs` naturally adds two storage/constructor lines to its prior 7989-line
public extraction; its final 7991 remains a net shrink from the unpromoted 8172.
No formatting compression or new grandfathered path was used. All other growth
and argument-allowance entries remain unchanged. The serial final gate plan is
`/tmp/mini-vpn-public-capacity-20260905-v2pmtb9g/final-gate-plan.json`; execution
stops at the first unexpected command/count failure. The execution results and new format stop follow below.

## Historical final-gate stop: Quinn export-order formatting

This section records the pre-authorization stop. Final closure is recorded above.

The final serial run completed 11 commands successfully and stopped at its
12th command. No later gate or automatic repair ran. The exact command arrays,
exit codes, counts and log paths are in
`/tmp/mini-vpn-public-capacity-20260905-v2pmtb9g/final-gate-results.json`.

| Gate | Actual outcome | Log filename in the Integrator snapshot |
|---|---|---|
| `governance-build` | exit 0 | `final-governance-build.log` |
| `governance-standalone` | 6 passed | `final-governance-standalone.log` |
| `governance-cargo` | 6 passed | `final-governance-cargo.log` |
| `provenance` | 159 / 13 passed | `final-provenance.log` |
| `fake-ip` | 10 passed | `final-fake-ip.log` |
| `dns` | 6 passed | `final-dns.log` |
| `udp-builder` | 6 passed | `final-udp-builder.log` |
| `dns-characterization` | 5 passed | `final-dns-characterization.log` |
| `dns-capacity` | 1 passed | `final-dns-capacity.log` |
| `dns-injection` | 1 passed | `final-dns-injection.log` |
| `repository-fmt` | exit 0 | `final-repository-fmt.log` |
| `quinn-fmt` | exit 1 | `final-quinn-fmt.log` |

The format failure is limited to the touched vendored `lib.rs` export ordering.
The command was:

```text
rustfmt --check --edition 2021 --config skip_children=true third_party/quinn-0.11.11/src/connection.rs third_party/quinn-0.11.11/src/lib.rs third_party/quinn-0.11.11/src/connection/tests/mod.rs third_party/quinn-0.11.11/src/connection/tests/retirement.rs
```

`rustfmt` wants the complete two-line `#[doc(hidden)]` /
`pub use crate::connection::{SendStreamRetirement, SuccessorServiceTurnError};`
group after the ordinary multiline `crate::connection` export group. No other
file or diff hunk is reported. Its `--check` invocation did not change source.

Failure tree:

1. **Confirmed:** the current hidden export group is out of rustfmt order.
2. **No new behavior failure:** current UDP `22/22`, isolated Quinn 49 unit plus
   1 doc test, and all current public behavior/provenance selections are green.
   Original Popper's `0/0/0`, Approve remains a scoped implementation verdict.
3. **Unverified at that stop:** the final whole-diff check and
   `cargo check --offline --all-features --lib` had not executed because the
   runner stopped before them. Public closure was therefore incomplete.

The single minimum next proposal is for original Writer Herschel to move only
that two-line attribute/export group to the location required by rustfmt, keeping
all names, visibility, types, all other source and the 155-line count unchanged.
First rerun the identical four-file format command above. On GREEN, synchronize
that same pure-format delta into the isolated Quinn copy and update the frozen
source manifests; ask original Popper to verify the new fingerprint/no semantic
change, then run the two remaining diff/check gates and finish current documents.
Existing successful behavior tests need not be repeated solely for this export
ordering change. Any new unexpected red still stops; no second repair is implied.

Original Popper independently confirmed that this complete-group move changes
no visibility, public API, attribute attachment or runtime semantics. Its
read-only diagnosis requires only the exact format rerun, source-delta and
fingerprint audit, then remaining diff/check gates; no behavior rerun is needed.
This diagnostic confirmation is not user authorization and caused no source edit.

At this historical stop, the coordinating task received the proposal for user
confirmation under its any-unexpected-red rule. No format repair followed until
the explicit authorization below. The four lowered entries remained unpromoted
at that point and could not authorize starting P2T before public closure.

## Explicit authorization after the final format stop

The user explicitly replied “授权” through the coordinating task. This permits
only original Writer Herschel to perform the exact two-line complete-group
move above while preserving 155 lines, API, visibility, attribute attachment,
semantics and all other production sources. First rerun the identical four-file
Quinn format command. On GREEN, synchronize the same pure-format delta into the
isolated copy, append new final fingerprints without replacing the original
behavior-run provenance, and obtain original Popper's read-only delta/hash
confirmation. Then the Integrator may finish the remaining diff/check gates
and current documents. Existing behavior suites need not be rerun for this
format-only delta.

The original format-failure log remains immutable. Any new unexpected red or
Request Changes still stops without a second repair. The four exact shrink
entries remain unchanged; root, R7/P2T, commit/push and external work are still
excluded. Authorization itself did not constitute closure; the completed
checks and final verdict are recorded in the final evidence table above.

## Verification ledger and unchanged scope at the E0505 stop

- Original UDP baseline: `16/16` green, no production changes at that point.
  Command: `cargo test --offline --lib tuic::udp_ -- --nocapture`;
  output: `/tmp/mini_vpn_udp_capacity_baseline.log`.
- First capacity tracer: compiler `E0505`; zero capacity assertions executed.
  Command: `cargo test --offline --lib
  public_udp_reset_pressure_bounds_physical_stream_admission -- --nocapture`;
  output: `/tmp/mini_vpn_udp_capacity_red.log`.
- Original Reviewer Popper remains assigned. There is no new implementation
  approval to replace its outstanding sender P2.
- Both frozen P2T parents and `quinn_client_root.rs` match the complete hashes
  above. This also preserves the P2T frozen blocks and routes.
- `tuic.rs`, `client_tun.rs`, both oversized test indexes, governance and
  repository provenance match the Integrator's entry manifest exactly.
- `src/tuic/udp_send.rs` and vendored Quinn `connection.rs`, `send_stream.rs`
  and `lib.rs` are byte-identical to the starting HEAD.
- No new Quinn test module, capacity field or retirement type exists.
- Four public shrink ratchets remain unchanged. No fresh post-closure
  governance/provenance/format/check sequence was run because closure was not
  reached. The coordinating task's known `4/6` gate remains unresolved.

Under the existing authorization, after implementation and same-Reviewer
closure the Integrator records final exact line counts and lowers
only the four owned shrink entries, and serially runs standalone governance,
Cargo all-features governance, Knife16/repository provenance, required
focused/lifecycle tests, format/diff/check. The old
[public repair result](2026-09-04-public-path-review-repair-local-results.md)
retains the preceding causal and portion-level evidence. P2T remains with the
coordinating task after actual public closure.
