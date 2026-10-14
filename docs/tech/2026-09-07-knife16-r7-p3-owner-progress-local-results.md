# P3 — owner command, ready scheduling and Target-open progress

Status: **ACTIVE FIXTURE, IMPLEMENTATION MOVES AND COMMAND-VISIT EXTRACTION ACCEPTED; BOUND SERVICE OPEN; FULL P3 INCOMPLETE**.

## 2026-09-17 accepted diagnostic — channel lifetime premise, aggregate open

Whole-scope SOURCE retains aggregateP1, selecting fixed-source conservative
requested-backing proofs at original owners; it does not approve a new Target
port, whole aggregate or full P3. See existing51O for the consolidated exit.
This stage changed only a temporary Tokio1.52.0 copy and current documentation,
not project Rust, dependencies, governance or external environments.

Original public baseline2/2 passed before instrumentation. The final copied
library adds cfg-only counters/stops/accessors, not Block fields or queue logic.
Source549-file aggregate matches the original pinned copy; current project449
Rust hashes have no drift and offline package versions match the project lock.
Local rustc1.95.0/59807616e, aarch64-apple-darwin, full/test-util, debug/release.
profile.json owns hashes/cfg. New diagnostic missing-doc warnings from the first
probe build were eliminated with local comments; final22 commands have none.

Normal debug/release each8/8: public cycles/permit closure, observed cycles,
reserve/cancel/close, four simultaneous growth candidates, tail-CAS/release
pause, sequential near-wrap and zero-based adversarial interleaving. C1 retains
two blocks, C65 peaks at four; the forced C4 candidate race holds one linked
block plus four unpublished candidates, then five linked blocks. Losing growth
candidates are appended, not immediately freed. All normal final allocations
equal frees. Actual private Block<usize> Layout is288/8 on this profile, not a
generic product backing formula.

The isolated C2 adversarial case starts at aligned fresh base=usize::MAX-63.
X/Y creates a genuinely lagging tail; A pauses after old tail load and before
its first Block reference, B pauses after old block release and pointer advance.
One actual poll_recv enters reclaim twice. With rx=MAX/tx=1, the original live
chain reuses P from start=MAX-63 to0 while A/B remain stopped; allocation/free
deltas are both0. Debug and release each10 repeats match the complete evidence
marker. The process exits after flushing; suspended pointers are NOT resumed.
These are20 defensive observations, not20 clean-shutdown tests, natural MAX
lifetime execution, a proven product vulnerability or full capacity proof.
The zero-based exact interleaving retains P and cleans up normally.

Current checked Bound mailbox routes serialize complete sends through a fresh
private pair and &mut Owner. Its cloneable general API is a different scope.
Same Heisenberg diagnostic closure is SOURCE0/0/0; original aggregateP1 stays
open. Independent read-only review verified source/hash/stops/current-chain-only
inspection/records; Reviewer ran no tests. Bounded Mencius follow-up closes TCP5
and Actor3 complete pushes as serial in the actual production graph, including
DELIVERED Held: no independently escaping sender operation, only synchronous
exclusive composition commits, including same-owner replacement installation.
No generic API guarantee or lifetime formula is inferred. The next selected gate
is original serial-push proof with receiver/close/reuse/wrap, not more generic
pressure runs or a Target port. No unchecked upstream constant is promoted.
/tmp/mini-vpn-p3-channel.C4qwtI/{profile,causal-first-observation,final-runs,
review,producer-map}.json owns actual records. No network access, product Cargo
run, commit or push. Skills kept diagnosis, producer reachability and ownership
claims separate; they did not introduce a new product abstraction.

## 2026-09-17 latest accepted implementation — production Root cold aggregate

Same Heisenberg final P0/P1/P2=0/0/0 Approve only the first51O cold phase;
Module/Clean/System/DDIA9/9/9/9. Matrix4/C/physical/runtime peak/call charges and
all five51B/P3 remain open. Source review and final evidence review were read-only;
Main executed all commands. No separate wording/status approval is required.

The actual Root::cold entry now composes concrete generic Root header, N actual
CommandResident slots, Ready-owned backing and one boxed OwnedNotified before
its first allocation. Ready's local value calculation covers real slots, four
tree word arrays plus WORD_BITS directory capacity each, Arc<ReadyShared> and
Arc<Notify> using its original pinned-toolchain Arc helper. It excludes the
embedded ReadyIndex header; standalone Ready preparation adds that header itself.
Notification clone and preparation->Root moves do not duplicate backing.
Thirteen nonzero cold heap requests are charged, not allocator metadata or RSS.
No future resident pointee/backing or active whole-root peak is inferred.

Resident, Ready slots and tree words/directories use the already accepted exact
allocator primitive, outside the separate effect fault domain. All initialization
uses capacity-proven single pushes. The plan review identified resize_with as
insufficient for this no-extra-growth proof; that correction was included before
implementation. Flow readiness also uses the same tree constructor and passed
its unchanged tests. Vec refusal returns Reserve; Arc/Box keep allocator abort.
No new unsafe, public Interface, ownership holder, callback or hot-path audit.

TDD: one real Root tracer initially failed compilation only for missing
cold_layout/cold_backing_layout helpers (three E0599 diagnostics), then passed.
Its bounded pure search finds a checked process plan with individually valid
resident and Ready Layouts but invalid aggregate. A test-only one-shot directly
before the first resident allocation prevents huge allocation even on a wiring
regression. Actual cold returns Layout with the flag untouched; the next small
call returns injected Reserve and the full original plan; the next succeeds and
start transfers the same resident backing and Notify allocation. This is not
natural OOM. Three native tests independently compose actual metadata shapes,
inspect exact capacities/dormant fields at word/level crossings, preserve
InvalidResident precedence and prove the separate effect fault remains armed.
First native compile hit E0624 by calling a sibling-private method. The sole
repair replaced that test call with direct original metadata assertions;
production scope and privacy stayed unchanged. All failures remain in sequence.

Final six-path scope: owner_ready.rs1167, flow.rs1130 (only arc_layout qualifier),
server_root/construction.rs300, process_plan.rs494, Ready tests/mod.rs127 and new
cold_storage.rs109.449 source hashes, six changed paths,443 unchanged; post-gate
drift/missing/trailing-whitespace/final-newline errors all empty. Governance751
unchanged; no oversized growth/shrink or inventory edit.

22 final commands exit0: default/all Ready28/Root26/C68/CurrentOnly13/Executor185,
Knife220/repository20/growth7/cargo check, scoped six-file rustfmt, git diff check
and strict rustc standalone governance7.40 whole-module repeats, Ready28 and
Root26 each10 per mode, total1080 actual test executions, all green. Warning
heading hashes match the previous accepted baselines: existing smoltcp22/lib7/
libtest1/Knife151. No globalfmt/full-library-suite/external/commit/push claim.

/tmp/mini-vpn-p3-root-peak.iroUDm/{baseline,plan-review,source,sequence,gates,
repeats,review}.json owns exact evidence. Pre-write strict governance7 and original
Ready25/Root25 passed. Current source matches independent review's449 hashes.

Next bounded read-only mapping has exposed a real composition Interface gap:
TargetIo currently returns logical capacity/inventory only, while original
Executor maps and channel/task allocations need their own physical proof. C's
first new allocation is grace; binding scratch/Ready/mailbox coexist with its
already-owned Executor; physical preflight later adds Sleep. Do not subtract
embedded headers from aligned layouts or reinterpret logical bytes as backing.
Consolidate one finite whole-scope proof/representation decision in existing51O
before further behavior; this mapping is not an implementation or waived gate.

## 2026-09-17 preceding accepted — original progress cold ordered arena

Same Heisenberg final P0/P1/P2=0/0/0, Approve only51N; local Module/Clean/System/
DDIA9/9/9/9. Full matrix4/Root aggregate/call charges/five51B/P3 stay open.

Implementation: actual OwnerReverseProgress owns exactly2(F+Z) concrete slots,
one AVL with parent links and a vacant chain. Cold initialization allocates once;
observation admission, rebase, ACK, retirement and reset allocate no backing.
Pure count witness and count-overflow precedence remain unchanged. Concrete
Layout and corruption are separate errors. C prepares progress after original
grace and before Ready installation; build rejection returns original clock,
Executor, plan, reservation and grace. Legacy returns its original inputs too.
Cold audit traverses actual tree/free partition with independent depth/visit
bounds, verifies key/parent/height/count/capacity/coverage and propagates C outer
Err. Scan remains key/revision-based O(logN+B), B32, classification before cursor
advance. Both retirement directions preflight before removal; original revision
priorities remain. Reset keeps backing and visits only occupied entries.

Real checked-C tracer: test-plan Executor produces original Replay before binding;
actual checked Ready binding and C observation settlement consume that page.
Real RESET emits one original local-direction terminal, then output/lifecycle
and original timer expire/retire it with stable backing. The original returned
Replay page remains test-owned; expiry output owns its return before settlement.
This is BoundInactive consumption, not Active selected-read/transport/root-zero.
Seven native tests cover zero/full/reuse, sparse rotations/deletion against an
ordered reference, bounded scans/revision reset, two-direction rejection,
revision MAX, malformed partitions and concrete original header+array Layout.

Sequence preserves non-accepted evidence: first compile RED mixed two missing
imports with missing measurement; import repair isolated expected E0599. First
runtime reached RESET but the fixture wrongly expected two directions; source
proved one original local-direction observation, and only that test was repaired.
All initial gates/repeats then green. Independent SOURCE found P1: array Layout
alone omitted original owner header. Two-file repair added actual combined
extend/pad before allocation. New missing-helper RED was compilation, not a
runtime counterexample. One test-only E0382 fixed validity sampling before Result
consumption. Final actual geometry header72/Slot152 has no combined-only rejection
at reachable even-N boundaries on this platform; no huge allocation or natural
C rejection was fabricated. Actual allocator/handle_alloc_error use array Layout,
not an extra allocation for the already embedded header. Review closes that P1.

Final scope:448 source hashes,12 changed paths (six production, two existing test
files, three new test files, Integrator-only governance),436 unchanged. Exact lines:
reverse2768/progress877/C2956/construction230/capacity295/cold399; reverse test2393,
native320/index1/Ccapacity492/Cchild179. Integrator alone lowered reverse2770->2768
and reverse test2395->2393; governance751, SHAee6ad5fe...970593df. Final review and
post-gate checks match all448 source hashes; no drift/missing.

Post-repair20 final gates all exit0: default/all reverse41/C68/OwnerTarget185/
capacity25/Knife220/repository20/growth7/check, scoped12-filefmt, diff and strict
standalone compile/run7. Forty whole-module repeats (each10 in each mode) total
2180 test executions, all green. Warning delta empty; existing smoltcp22/lib7/
libtest1/Knife151 retained. Reviewer independently read source/hash/gate records;
Main ran tests. No globalfmt/full-library-suite/external/Git claim.
Pre-repair20 gates/2160 repeats remain valid only for their old snapshot. Two
truncated JSON gate records were recaptured with bounded output, not discarded
or promoted as final evidence; no Rust change for capture/parser repairs.

/tmp/mini-vpn-p3-progress.nYWfZO/arena-{source,sequence,first-review,integrator,
gates,repeats}, arena-repair-{source,sequence,source-review,gates,repeats} and
arena-review.json own exact evidence. Next: consolidate actual phased Root peak
and call charges at real construction/runtime owners using existing section51.
No new design sidecar or repeated preparation Move is needed for this local closure.

## 2026-09-17 preceding accepted — progress capacity and Legacy cold-construction Moves

Same Heisenberg final SOURCE P0/P1/P2=0/0/0 Approve, local9/9/9/9, solely these
behavior-preserving Moves. Original OwnerProgressCapacity impl18 lines is in
existing progress.rs with pub(crate) methods unchanged. Complete cfg(test)
Legacy Core impl51 lines is in existing Controller construction.rs; only its
two methods become pub(super), preserving effective C scope. Necessary imports
remain cfg(test); two compact construction docs now name test-only construction.
Nominal aliases/errors/fields, checked production constructor, callers, test
bodies, BTreeMap representation, count/order/error semantics remain unchanged.

Exact lines: reverse2770/progress472/C2956/construction207/governance751.
Before/after reverse34 and C67 pass in both modes; scopedfmt passes. Expected
standalone6/7 named only reverse2789->2770 and C3007->2956. Original Integrator
Mencius independently verified and updated precisely those two entries; Main
never edited governance. Its strict standalone/default/all-features each7 green.

Four whole-file inverses, exact capacity body and two-qualifier-normalized
Legacy body all match. Main's initial construction inverse failed to remove
one inserted cfg-import separator; Integrator's separately assumed an extra
blank after the EOF impl. Both were read-only checker false-negatives, corrected
without touching Rust. The construction inverse is exactly5739 bytes with SHA
0a5501cd94c5106cb3baee0a803cbb71dfa799f51b60c41a8eed19807c76d101. Initial
false results and the Main report correction remain in the artifacts/review.
Final445 hashes/five paths changed/440 unchanged; governance inverse exact,
no drift, deleted or unrecorded new Rust path.

Main final20 commands all exit0: both reverse34/OwnerTarget185/Controller67/
capacity25/Knife220/repository20/growth7/check, four-file scopedfmt/diff and
strict standalone compile/run7. Reverse34 and C67 each repeat10 times in each
mode:40 runs,2020 executions, all green. Complete warning-heading comparison
delta empty; smoltcp22/lib7/libtest1/Knife151 existing warnings remain.
Reviewer independently checked source/scope/inverses/inventory and Main/Integrator
records, without running Cargo/tests or writing. No external/Git operations.

/tmp/mini-vpn-p3-progress.nYWfZO/construction-move-{baseline,source,
inverse-correction,sequence,source-review,integrator,final-source,gates,repeats,
review}.json owns exact evidence. Next is the selected cold arena behavior,
not another speculative constructor: exact scope and first real checked-C
construction/original observations tracer are under bounded same-Reviewer
review. Include original cold audit, preserve pure count arithmetic and
pre-install exact-return; no new hot-path allocation. Progress backing,
aggregate/call charges and all matrix4/five51B/P3/Case C/R7 remain incomplete.

## 2026-09-17 preceding accepted — complete progress Implementation Move

Same Heisenberg final SOURCE0/0/0 Approve, local Module/Clean/System/DDIA9/9/9/9,
only behavior-preserving migration. Original442-line OwnerReverseProgress impl
moved to private unconditional reverse_switch/progress.rs453. Parent3231->2789.
Four original parent-private operations (revision, scan_reverse_stall,
exact_reverse_stall_candidate, exact_entry) are child pub(super), preserving
effective scope. All other helper/crate method scopes remain. No wrapper,
re-export, caller/test-body/storage/count/ordering change. Nominal anchors stay
parent. Normalized body SHA b55cdd07cae555afeeae97810d89dadd765c7a0d431a622aff7db45608d50b78
and complete parent inverse are exact.

Before Move: strict governance7 and reverse-switch34 in each mode green.
First scopedfmt found only the extra deletion-boundary blank; one-line repair
then fmt and both34 green. Expected standalone6/7 named only3231->2789.
Original Integrator Mencius independently verified and applied precisely that
ratchet; governance751, inverse restores original SHA, both Cargo/strict7 green.
Main never edited inventory. Final445 hashes: parent/child/governance changed,
442 unchanged, no removals, drift or unrecorded new Rust path.

Main final20 commands all exit0: both reverse34/OwnerTarget185/Controller67/
capacity25/Knife220/repository20/growth7/check, scopedfmt/diff/strict compile/run7.
Ten reverse34 repeats EACH mode execute680 tests, all green; warning delta empty.
Existing smoltcp22/lib7/libtest1/Knife151 warnings remain. Reviewer independently
checked source/inverses/full inventory/evidence, ran no tests or writes.
/tmp/mini-vpn-p3-progress.nYWfZO/move-{baseline,plan-review,source,sequence,
source-review,integrator,final-source,gates,repeats,review}.json owns exact evidence.

No physical storage behavior is accepted here. Selected next direction is cold
N=2(F+Z) exact ordered arena, no allocator in postcommit progress observations,
concrete Layout separated from pure count arithmetic, original checked-C exact
constructor rejection before install, bounded O(logN+32) scans and unchanged
generation/revision/retire/reset contracts. Remaining preparatory construction
Moves are under bounded review. Original progress backing, phased Root aggregate,
call charges and all five51B/P3/Case C/R7 remain incomplete. No external/Git work.

## 2026-09-17 preceding accepted — Target requested-backing peak

Same Heisenberg final SOURCE P0/P1/P2=0/0/0, Approve only this local peak;
Module/Clean/System/DDIA9/9/9/9.444 Rust hashes, six changed paths/438 unchanged,
no removals/governance edit. Exact lines: target1405, memory_io372, flow_io1311,
readiness550, existing node tests435/new storage_peak237. Independent Reviewer
recomputed hashes and checked the source and recorded gates; no tests/writes.

Checked configuration now composes one MemoryTarget header, actual F+Z nodes,
readiness backing,2FM retained byte backing,FD directives, output pending plus
preparation<=min(G,FM), and one byte-or-directive replacement branch. Compare
complete aligned Layouts, M=min(P,G); withZ recomputes. Exact readiness slots,
directory and words are prepared through the existing allocator, pushed to
length==capacity before boxing, without a new unsafe or Effect fault domain.
The existing unrecoverable readiness allocator policy is unchanged.

Original OPEN keeps duplicate/stale/full/counter/readiness ordering, then checks
cached peak plus owned/incoming/borrowed address capacities BEFORE allocating a
node. Commit publishes original node/address and capacity mirror together.
Join keeps address charge; retire checks subtraction before consuming joined
custody and publishes accounting before waking. Cold independently sums actual
String capacities, validates real buffer/directive/pending bounds and rejects
mirror mismatch without normalization. No new scan per DATA. The old node-only
extreme test explicitly distinguishes valid node Layout from a rejected stronger
whole configuration; all other native assertions remain.

Five new tests:
- Real F1,M=G=isize::MAX/4+1 configuration: old Ok was exact runtime RED;
  checked combined Layout now rejects before allocation.
- Real binary-searched near-limit configuration: OPEN originally reached armed
  NODE Reserve before rejecting address overlap. Minimal ordering GREEN leaves
  that fault for the next IP OPEN, then same-ID retry/cleanup succeeds.
- Real F1/64/65/257 lifecycles, long raw900-byte domains with caller capacity4096,
  retained buffers/directives and caller outputs prove original address charge
  remains through join and clears on retirement. Readiness leaves Effect fault
  untouched; logical zero still retains free-node/buffer backing.
- Explicit defensive mirror/address/buffer/directive/pending corruption is
  rejected by cold audit without normalizing state.
- Explicit defensive retirement mirror underflow preserves original joined
  String/phase and emits zero real wakes; restored invariant retires/wakes once.

Prewrite governance7; two runtime RED->GREEN pairs; focused5/5. Final20 commands
pass: default/all-feature Target36/OwnerTarget185/Supervisor54/Controller67/
Knife220/repository20/growth7/libcheck, six-file scopedfmt/diff, strict standalone
compile/run7. Ten full Target-module repetitions EACH mode execute720 tests,
all green. No warning delta: existing vendored22/lib7/libtest1/Knife151 remain.
No unexpected Rust repair. Final444-hash comparison has zero drift.

/tmp/mini-vpn-p3-target.z6G5fA/peak-{source,sequence,gates,repeats,review}.json owns
exact commands, complete warning headings, result counts, output sizes/hashes
and review. No globalfmt/full-library-suite/external/commit/push claim.
This conservative requested-backing bound is not RSS or proof all maxima are
simultaneously attainable. Allocator metadata, opaque input/Waker pointees and
transferred outputs are not fabricated as Target-owned backing. Their real
supplying/subsequent owners, progress ordered storage, phased Root aggregate,
actual call charges and all five51B remain mandatory. Full P3 is incomplete.

## 2026-09-17 preceding accepted — native Target node transaction

Same Heisenberg final SOURCE P0/P1/P2=0/0/0, Approve only this local transaction;
Module/Clean/System/DDIA9/9/9/9.443 Rust hashes, seven paths changed,436 unchanged,
no deletions/governance edit. target1401, memory_io371, flow_io1168, readiness534,
old test leaves798/215, new node_storage430. Original complete TargetIo owns
native Live/Joined/Vacant nodes, two AVL roots and retained free chain. Actual
F+Z Layout and preparation precede OPEN publication; address refusal preserves
free custody, join moves the original Box/String, retire commits before wake.
Cold validates shape/phase/sort/balance/counts/disjoint keys/free/readiness;
node walks are linear, whole cold still includes existing readiness lookups.

Eight tests cover Reserve/retry/reuse and terminal pressure,127-node rotations/
two-child removal with all original identities, exact Layout/addition boundaries,
candidate/vacancy address refusal, real join Waker Drop/retire wake interruption,
defensive corruption and8192-node real lifecycle/128KiB-stack destruction.
First E0599 only missing fault seam is compile feedback, not runtime RED.
No unexpected Rust repair. Prewrite governance7/Target23; final20 commands:
both Target31/OwnerTarget185/Supervisor54/Controller67/Knife220/repository20/
growth7/libcheck, scopedfmt/diff and strict standalone compile/run7, all green.
Two modes each10 repeats of8 tests execute160, all green. Preliminary20 gates
also passed before Reviewer requested the long-chain test. Source review ran
no tests/writes. Library warning headings match Move; Knife retains151 warnings.
Additional log audit hit duplicate quiet CLI flags before tests; removed the
ambiguity and captured both actual Knife220 runs in bounded memory. No source
change; original truncated logs and command failure remain in evidence.

/tmp/mini-vpn-p3-target.z6G5fA/native-*.json owns baseline, source, sequence,
gates, repeats, independent review and evidence-only follow-up. No full suite/
globalfmt/external/commit/push claim. Full Target address/buffer/output/readiness
aggregate peak, progress and phased Root plus all five51B remain mandatory.

## Goal and authority

The user asked to commit P2B first, then complete the whole P3 node, with
autonomous causal repair for fixable in-scope failures. On 2026-09-07 the user
also confirmed the required public-prerequisite commit before the P2 chain.
This exception replaces the repeated-confirmation repair loop only within P3.
It does not waive independent review, source-growth ratchets, architectural
hard stops, or authorization for material scope expansion and external runs.

The [accepted conditional plan](2026-09-04-knife16-r7-case-c-owner-server-root-implementation-plan.md)
Section 10 defines P3. The newer
[Target-open/ready amendment](2026-09-04-knife16-r7-target-open-ready-scheduler-amendment.md)
defines its progress, ownership and budget contracts. Historical authorization
statements in those documents remain historical; this user instruction opens
P3, not P4–P7 or deferred final integration. Case C/R7/Task 4 remain incomplete.

Main is the sole overlapping root Writer. Heisenberg
(`01a06bfa-4552-7ea3-b5ce-9d6b65f88992`) remains the independent Reviewer;
Mencius (`01a070b9-40c9-7ee0-8cd1-26cfa530f968`) owns any exact inventory
promotion. Parallel work is read-only unless a disjoint scope is frozen first.

## Completed commit prerequisite

Two local commits, with no push:

1. `5fca1ed` — `fix(tuic): bound UDP stream retirement custody`.
   Fifteen paths contain only the accepted public capacity closure, its
   tests/evidence, four exact public ratchets and consistent checkpoint docs.
2. `532d766` — `refactor(knife16): converge P2 bootstrap ownership modules`.
   Sixteen paths contain the accepted P2T/P2A/P2B dependency chain and its four
   governance changes, not another public implementation change.

The first commit could not safely omit the public prerequisite: HEAD's TUIC
source was 7989 lines, the approved closure 7991, and the prior ratchet 8172.
Staging the final 7991 ratchet without its source would be inconsistent;
inventing an intermediate 7989 ratchet would forbid the already-reviewed
7991 closure. The user explicitly authorized separate prerequisite submission.
This is a snapshot/governance dependency, not a bootstrap runtime dependency
on the TUIC sender API. No unrelated work was discarded or reformatted.

Fresh commit-boundary evidence is under
`/tmp/mini-vpn-commit-boundary-A1DP9p/`:

| Check | Evidence | Result |
|---|---|---|
| Public staged source | Compare index with accepted public 29-source manifest | 29/29 exact |
| Public staged governance | Exact prior approved SHA `631cbd…271ee` | Four public changes only |
| Isolated public snapshot | `public-tree/`, exported from index with `checkout-index` | No P2 files |
| Public standalone governance | Build with `rustc -D warnings --edition=2024 --test`, run `public-governance --test-threads=1` in that snapshot | 6/6 |
| Public UDP | `cargo test --offline --lib tuic::udp_ -- --test-threads=1` in that snapshot; `public-udp.log` | 22/22 |
| P2 source-preservation audit | Existing `source-audit.mjs` from P2B binding repair | 9/9 |
| P2 all-feature governance | `cargo test --offline --all-features --test code_growth_governance -- --test-threads=1`; `p2-governance.log` | 6/6 |
| P2 bootstrap | `cargo test --offline --lib 'owned_upstream::quinn_leg_actor::tests::bootstrap_custody::' -- --test-threads=1`; `p2-bootstrap.log` | 6/6 |
| P2 provenance | `cargo test --offline --test knife16_owned_leg_provenance --test repository_test_module_provenance -- --test-threads=1`; `p2-provenance.log` | 162/162 and 13/13 |
| P2 formatting | `cargo fmt --all -- --check`; `p2-fmt.log` | PASS |
| Both staged groups | `git diff --cached --check` | PASS |

The P2 index exactly equalled the working tree at these checks. Heisenberg's
two bounded staged reviews each returned findings none, P0/P1/P2 0/0/0,
Approve. Public review checked 29 source hashes and 83 index-resolved document
targets/references; P2 review checked eight source/governance hashes, the nine
source checks and 28 links. Those reviews ran no Cargo/project tests and do
not replace the earlier Popper public or Heisenberg implementation acceptances.
The clean post-commit working tree was verified before P3 entry documentation.

## Full P3 acceptance ledger

- [x] Establish one private, compiling, behavior-bearing
  `owner -> server_root -> scheduler` route with owner.rs net-zero/shrinking.
- [x] Close the mailbox prerequisite: bounded owning prefetch, checked
  per-lane progress and one persistent observer, including cancellation,
  close/observed end-of-stream, normal destruction, exhaustion and wake panic.
  That mailbox-only prerequisite did not wire the root to its observer.
- [x] Retain the actual capacity wake registration across cancelled borrowing
  waits, not merely its observer lease/epoch pair. The later five-test packet
  below repairs that distinction; it does not establish global ready/park.
- [x] Connect the existing single Control completion to the persistent
  observer: matching rearm, exact Busy/Full/Closed custody, cancellation and
  wake unwind. This is not acceptance of the two-lane item below.
- [ ] Root-own exact pending commands in the two bounded lanes; success once,
  exact Full/Closed returns, sticky unknown, observer cancellation/re-entry.
  Full waits on actual lane-capacity/close progress, not repeated polling.
  B accepts the conditional matched-Owner Driver primitive and local lifecycle
  tracers below. Persistent production association/exclusive caller routing
  remains unproved; this full-ledger item is therefore still unchecked.
- [ ] Generation-safe, deduplicated process and local ready leases, persistent
  cursors, dirty publication, clear/recheck and root park; stale wake isolation.
- [ ] Implement and exercise all four dedicated class budgets and the checked
  service-delay formulas, including positive bounds, overflow/max boundaries,
  nested source competition and settlement routing without stolen budgets.
  The process-only value plan is accepted below; actual class scheduling,
  nested/physical envelopes and executable fairness remain unproved.
- [ ] Characterize/Move coherent Target implementation first wherever required
  by growth/locality; delete the synchronous open route at the actual cutover.
- [ ] One static-dispatch begin/poll/finish/open_inventory port, MemoryTarget
  exercising Pending/Ready/cancel/fatal, and executor-owned exact progress and
  reducer-blocked completion without head-of-line blocking or duplicate install.
- [ ] Audit supported-platform/dependency primitives before TCP/DNS code;
  implement direct nonblocking numeric and bounded domain opens without tasks,
  workers, blocking resolver, dynamic futures or optimistic success.
- [ ] Prove pressure, rejection, refusal/timeout, cancellation/late socket,
  fatal/quarantine, shutdown and independently audited open/live inventory.
  Keep command/socket/ready leases and Z distinct; no false session/root zero.
- [ ] Close each applicable amendment counterexample with deterministic
  tracers, repeats where justified, direct regressions, both provenance routes,
  fmt/diff/check, both governance routes and same independent review.
- [ ] Review the complete P3 scope and reconcile result/status/handoff/TODO
  and learning memory. Do not substitute one green foothold for P3 completion.

P4 bootstrap/table, P5 fused adoption, P6 private integration, P7 custody review,
and deferred whole-server zero/facade/binary remain distinct plan nodes. Exact
bootstrap/seed migration integration tracers must use their genuine owning
phases when those nodes exist; a scalar scheduler fixture cannot prove them.
No P3 claim closes Case C or authorizes public TUN, VPS, WAN, throughput or M2.

## Entry observations and next bounded packet

Current sources: owner.rs 2062; session.rs 769; target.rs 2017;
owner_target.rs 11110. The Target and owner executor still use synchronous
`open`; the latter immediately maps its result in PeerOpenRequested. Source
reads are symbol-first and bounded; no archived memory was loaded.

The mailbox already exact-returns commands from nonblocking try-send, but has
no capacity/close epoch notification port. Its current async send owns the
command in the observer future. ClientDriver is a separate sealed input;
no new constructor, unwrap/re-wrap bypass or visibility change is presumed.
The missing notification is known P3 work, not a newly observed regression.

The first frozen packet has only the private owner route, new root/scheduler
and private test index/leaf. It must execute real owned work in the first
compiling boundary. No empty root, child-first scaffold, synthetic command
inventory or busy-polling Pending is acceptable. Bounded Reviewer/Integrator
source audits preceded the exact freeze below. The first missing-seam RED
and local GREEN are recorded below; they are not full P3 acceptance.

In-scope feedback-loop failures will be diagnosed and causally repaired one at
a time, with exact reruns and independent re-review. A demonstrated missing
Authority/Interface, forbidden scope growth, or contract contradiction still
stops behavior work for architectural convergence; it is not waived by the
autonomous-repair instruction.

## First vertical tracer freeze

Heisenberg's bounded feasibility review permits a synchronous, real command
transfer as the first route behavior, provided its input is narrowed to an
existing supported event, all reachable failures retain custody, and no async
Pending is offered without real notification. Mencius independently confirmed
that the later notification can stay in session.rs, without changing the
sealed ClientDriver route or moving existing inline tests. These are feasibility
findings, not implementation approval. Initial scoped design scores were
Clean Architecture 9/10 and System Design/ownership 8/10; missing concrete input,
failure states and executable proof must be supplied, not waived.

Exact first Writer files:

- `src/owned_upstream/quinn_leg_composition/owner.rs`: private server_root route
  and its adjacent module-doc separator only; stdin rustfmt probe proves exact
  2062 lines, unchanged layout beyond those two route-local lines.
- `.../owner/server_root.rs`: private OwnerServerRoot, one root-held
  PeerOpenResolved command, nonoptional send phase and synchronous one-unit
  transfer observation. No generic command admission or facade.
- `.../owner/server_root/scheduler.rs`: private root-local ready claim and
  bounded one-unit selection for this immutable first command, no payload.
- `.../owner/server_root/tests/mod.rs` and `tests/ready_scheduler.rs`: private
  test route and one real SessionModel/SessionOwnerMailbox tracer.

First input is exactly `PeerOpenRequest + OpenResultCode`, represented by the
existing fixed-size `SessionEvent::PeerOpenResolved`. No socket is opened or
success fabricated by this root. The fixture supplies a refusal outcome for
a request genuinely minted by the owner reducer; inbox receipt and reducer
application must preserve that exact request/outcome. The command is not cloned.
This closes only one control completion's initial synchronous handoff; configured
multi-session pools, both lanes and dynamic generations remain subsequent P3
work, not implicitly completed by the first immutable claim.

Expected RED: the exact new test
`owned_upstream::quinn_leg_composition::owner::server_root::tests::ready_scheduler::ready_open_completion_enters_real_mailbox_exactly_once`
must compile far enough to identify the missing OwnerServerRoot/drive observation
implementation. Route/test declarations at that RED are not an accepted scaffold;
the first promoted compiling boundary must include root, scheduler and real
behavior together. No child-first promotion or empty production placeholder.

Minimum GREEN retains the event root-first, selects its actual ready claim,
publishes TransferringUnknown before nonblocking try_send_event, and records
mailbox ownership infallibly on Ok. Full/Closed retain the exact returned boxed
command and block scheduling; unknown retains its unreleased nonzero claim and
cannot retry. A second drive cannot duplicate transfer. There is no wait,
capacity poll loop, task, callback, clone, Option owning hole or joined-zero
conversion. The successful test alone cannot promote all reachable branches;
subsequent distinct tracers cover them before accepting this command sub-slice.

Failure predictions: missing-seam compiler RED is valid; fixture/privacy errors
are feedback-loop failures and are repaired in test scope first. Duplicate
delivery falsifies root publication; changed request/outcome falsifies exact
transfer; repeated Full or any clean-zero claim with retained/unknown state
falsifies custody. Needing a mailbox/Target/physical-root edit or unrelated
owner compression stops this first packet, not the overall P3 authorization.

## First command evidence — limited route accepted

Pre-write standalone governance passed 6/6 (`p3-prewrite-governance`). The exact
first tracer compiled its real fixture and failed only with E0433 for the
missing OwnerServerRoot / OwnerReadyDrive (`p3-route-red.log`). The minimum
root/scheduler implementation then passed 1/1 (`p3-route-green.log`).

Distinct characterization checks directly passed against that implementation:

- Full: 1/1; exact returned command and its allocation remain root-owned across
  ten subsequent drives, even after actual inbox capacity becomes free.
- Closed: 1/1; exact returned command remains root-owned across ten drives.
- Unknown transfer: 1/1; an actual registered receiver Waker panics after the
  real Tokio channel has enqueued the command. The root retains unknown
  disposition, stays quarantined on re-entry and never sends a duplicate.
  Inbox receipt proves physical mailbox custody; it cannot clean the root's
  unknown disposition. No production failpoint or synthetic sender was used.
- Claim mismatch: 1/1; retiring the ready claim while the concrete event is
  still held causes sticky quarantine without discarding or transferring it.

Logs use the `p3-full-characterization.log`, `p3-closed-characterization.log`,
`p3-unknown-characterization.log`, and `p3-invariant-characterization.log`
names under the same commit-boundary artifact directory. Unknown wake panic
is expected/caught. Dependency audit used locked Tokio 1.52.0:
`sync/mpsc/bounded.rs:924` calls `chan.send` before Ok; `chan.rs:528` pushes
then wakes; `sync/task/atomic_waker.rs:303` permits that user wake to unwind.
These checks report only the historical vendored smoltcp 22 warnings.

Only the frozen five Rust paths changed. owner.rs remains 2062; no mailbox,
Target, physical root, scanner, provenance or governance source was edited.
Full/Closed are deliberately blocked, not a claimed wait/progress API. Both
lanes, notifications, dynamic ready sets, service budgets, Target work and
full P3 review remain unchecked. Only the first route item is accepted.

Final local gate results:

| Gate | Result | Artifact suffix |
|---|---|---|
| Five route branches after test-only formatting | 5/5 | `p3-first-matrix.log` |
| Serial repeats of all five | Ten rounds, each 5/5 | `p3-first-repeat-1.log` through `-10.log` |
| Existing mailbox | 8/8 | `p3-mailbox-regression.log` |
| Existing non-Clone DriverData ownership | 1/1 | `p3-nonclone-regression.log` |
| Knife16 / repository provenance | 162/162 and 13/13 | `p3-first-provenance.log` |
| fmt / tracked diff / library check | PASS, exit 0 each | `p3-first-fmt-green.log`, `p3-first-diff.log`, `p3-first-check.log` |
| Standalone governance compile/run | `-D warnings` PASS, 6/6 | `p3-first-governance-compile.log`, `p3-first-governance.log` |
| Cargo offline all-features governance | 6/6 | `p3-first-cargo-governance.log` |
| Source, untracked whitespace, local Markdown links | PASS | `p3-first-source-audit.json` |

The first formatter check reported only the new test leaf's import/wrapping
layout. User repair authority covered that pure-format repair; no owner or
other source changed. The final test arms its panic only after registering
the real Pending receive. All five were rerun afterward. Library checks have
the historical vendored smoltcp 22 warnings; Knife16's embedded source inspection
additionally has its historical 142 warnings. No warning allowance was added.
Main individually checked all final gate exit codes.

Heisenberg's independent closure returned findings none, P0/P1/P2 0/0/0,
Approve for this first route only. Scoped Architecture/Module Depth/Authority
Locality are 9/9/10; necessary optimization findings none. The Reviewer audited
source, inverse-diff/hash, exact counts and gate excerpts, not a separate Cargo
rerun. Main's source audit covers changed tracked/untracked scope, final newline
and trailing whitespace, Markdown targets, exact owner inverse restoration and
unchanged governance. Counts: owner.rs 2062; root 125; scheduler 61; test index 1;
test leaf 268. No ratchet promotion is needed. P3 is not committed or pushed.

At the first route's acceptance, mailbox work was pre-write. Notification can
wake user code that unwinds: an actual dequeued command must occupy its owner's
bounded prefetch cell before that wake. Accepted C_prefetch=N*2 reserves these
positions; a copied event or dropped local cannot substitute. Subsequent
mailbox work is recorded below; Target remains unstarted. Full P3 is the goal.

## Mailbox progress packet — approved direction, staged implementation freeze

Independent Mencius and Heisenberg audits confirm this is within authorized
P3, not a new provider or an architecture amendment. The first route stays
accepted. Local Tokio 1.52.0 has two additional relevant details: chan.rs:424
try_recv can park its thread on Busy; both try_recv and poll_recv return a
semaphore permit before returning the popped command. An async sender Waker
could therefore unwind while the command is still inside the dependency.
Adding prefetch only after recv would not close that window.

Repository call audit proves awaited send_event is used only at session.rs:750
by an already-Closed Debug test; send_driver has no caller; send_command and
SessionMailboxSendError have no independent caller. The four raw channel
fields are private to session.rs, without reserve/permit/raw-channel exits.
Deleting the full awaited chain, including in test builds, excludes semaphore
sender waiters. This is a specific closed-port proof, not a claim that Tokio
poll_recv is universally infallible, wait-free or real-time bounded.

Only two additional Rust paths are frozen for this packet: session.rs (769 at
entry, normal production limit 1500), and new session/tests/progress.rs (test
limit 800). The latter is a private `mod progress;` inside the existing tests
Module, preserving all eight legacy inline test identities and bodies except
the one mechanical Closed-error call/Debug expectation. No new inline test body.
First-root paths, Target, physical roots, dependencies, vendors, provenance,
governance and unrelated helpers remain unchanged by this packet.

Sequence, never a combined rewrite:

1. Existing eight mailbox tests are the green characterization. Remove only
   the three awaited-send methods and their now-unused error implementation;
   mechanically change the Closed test to try_send_event and its exact redacted
   TrySendError expectation. Rerun all eight plus the real non-Clone caller and
   five accepted root tests. This is Interface contraction, not new notification.
2. Add a direct receive-Pending/close/drain tracer predicting the missing
   private poll_next seam. Replace only the receive selection Implementation
   with at most two borrowed poll_recv operations per poll, retaining recv_next
   signature, control burst 8, ordered DATA/CLOSE FIFO and half-closed draining.
   No try_recv, outer select as fairness, notify or hidden prefetch yet.
3. A separate real capacity-wake panic tracer precedes adding per-lane owning
   prefetch and checked capacity/close epochs. Publish command/lane/streak and
   epoch state before wake; nonempty prefetch prevents a second pop. Close
   publishes both logical lane-closed states before its first user wake.
4. Add distinct notification register/recheck, observer cancellation, Closed,
   actual non-Clone Driver DATA, epoch exhaustion and repeat/no-duplicate
   checks. Full may rearm only on actual lane capacity/close change. The exact
   notification Interface will be frozen before step 3, not guessed here.

Existing first-route unknown-send quarantine remains necessary: removing
async sender waiters does not remove receiver Waker callbacks inside try_send.
Missing raw-port closure, a command left local across wake, a source-size breach
or a new Authority/Interface beyond this existing Module stops that step.
Skills require separate characterization/contraction/replacement/progress
steps; user P3 authority permits causal in-scope repairs, not random follow-ups.
Reviewer candidate scores Clean Architecture/System Design/fault consistency
are 9/8/8; executable wake/custody evidence is required before improving them.

### Contraction and direct receive — preparatory seam accepted

Step 1 baseline eight passed. Deleting the unused awaited chain and mechanically
adapting its Closed Debug assertion preserved eight mailbox, five first-root
and one real non-Clone command test, all green after formatting. Logs:
`p3-mailbox-pre-contraction.log`, `p3-mailbox-contraction.log`, and
`p3-contraction-{mailbox,root,nonclone}.log` in the same artifact directory.

Step 2 added only the private progress test route and
`direct_mailbox_poll_preserves_priority_and_half_closed_drain`. Its compiler
RED was exactly six missing-poll_next E0599 diagnostics, no route/privacy or
fixture errors (`p3-mailbox-poll-red.log`). The replacement uses a fixed two-lane
order and at most two poll_recv calls; the existing recv_next borrows it through
poll_fn, with no explicit park or select-based fairness. Exact GREEN 1/1 and
complete mailbox 9/9 are in `p3-mailbox-poll-green.log` and
`p3-mailbox-poll-matrix.log`. The final preparatory closure is recorded below.
No capacity notification, prefetch, epoch, generic root admission or Target
implementation was mixed into these two preparatory steps.

Final preparatory gates all exited 0. Under the `p3-mailbox-prep-` artifact
prefix: mailbox 9/9, root 5/5, nonclone 1/1; repeat-1 through repeat-10 each
9/9; provenance 162/162 and 13/13; fmt/diff/check PASS; standalone governance
compile with -D warnings PASS and run 6/6; Cargo all-features governance 6/6.
The initial formatter check had only the new two-lane array layout and new
test line wrapping; the two-file pure format repair preceded all final gates.
Historical warnings remain the previously recorded 22 and provenance 142.

Heisenberg independently approved Step 1–2 only, P0/P1/P2 0/0/0, with
Architecture/ModuleDepth/AuthorityLocality 9/9/10 and no necessary revision.
It confirmed exact old-test suffix preservation except the authorized child
declaration and Closed call/Debug expectation, no awaited/raw-channel bypass,
bounded two-poll selection, coop Pending not mistaken for closure and surviving
lane draining. This was a source/log review, not a separate Cargo rerun.

Main's `p3-mailbox-prep-audit.mjs/json` also checks that exact test suffix,
13 changed-path scope, all tracked/untracked whitespace, 22 Markdown targets,
five accepted root source hashes unchanged, old awaited/try_recv routes absent
and governance unchanged. session.rs is now 682 lines (from normal-limit 769),
its new test leaf 33; no grandfather entry or ratchet change is needed. This is
an accepted preparation, not accepted mailbox notification or full P3.

The final status review found stale HANDOFF wording that accepted only the
first route. That doc-only phrase was synchronized with the accepted mailbox
preparation; this ledger heading was made neutral. No implementation scope or
full-P3 checkbox changed. All source and document audits were rerun afterward.
The same Reviewer then closed that P2: 0/0/0, Approve for the status flip only;
implementation acceptance stayed unchanged and full P3 stayed incomplete.

### Preparation checkpoint — superseded continuation position

Continue the existing P3 goal with the same repair authority. The next step is
the two-file packet's step 3: freeze the notification/epoch Interface, then one
real capacity-wake panic RED/minimum GREEN proving actual prefetch custody.
The prior eight mailbox tests and accepted first-root tests must remain visible;
send-side unknown remains quarantined. The primitive audit is specific to locked
Tokio and does not turn queue Pending into a full scheduler fairness proof.
Observe/notify integration must then close both lanes, clear/recheck, cancellation,
close and exhaustion before the later root/generation/fairness/Target steps.

The original goal record still reports `blocked`, inherited from the resolved
commit-scope permission stop; this is not a new engineering blocker. Its API
cannot resume it. Do not create a replacement objective or mark P3 complete;
the user must restore the existing continuous goal for automatic continuation.
Both requested commits are done, no push occurred. No P3 commit, Task 4/Case C
completion, external run, public cutover or new task has been authorized by
this checkpoint. The full unchecked ledger above remains authoritative.

## Owning prefetch / notification — accepted packet

Step 3 entry passed fresh standalone governance 6/6, mailbox 9/9 and the
preparation's unchanged-source audit. Artifacts retain the same directory and
use the `p3-notify-` prefix. Only session.rs and its private progress test leaf
are Rust Writer paths. The accepted five first-root sources remain frozen.

The initial two-observer candidate was rejected before production writes:
locked Tokio Notify unlinks waiters before waking them; WakeList unwind drops,
but does not wake, the remaining Wakers. Repeating notify cannot recover them.
The persistent metadata also needed a normal Inbox Drop terminal protocol.
Heisenberg reported 0/2/0 Request Changes; Mencius independently confirmed the
dependency counterexample. No fallback task or custom callback was added.

The corrected Interface freeze is independently approved 0/0/0, contract only:

- Exactly one non-Clone capacity observation per mailbox, acquired before
  trying sends. It owns an Arc of metadata and the last reported **pair** of
  independent lane epochs, not a channel, command or capacity reservation.
  Duplicate acquisition is a typed Busy before command transfer.
- `changed(&mut self)` borrows that persistent observation. One Notify waiter
  is enabled before terminal/epoch recheck; cancel removes only registration.
  Results are Control/OrderedSource/Both/Closed/EpochExhausted. The saved pair
  must be exactly the pair read for that result, never a later refresh.
- Each lane reserves one owning prefetch. A real popped command, selection
  streak and checked lane epoch are published before wake; retained prefetch
  drains before either lane is popped again. This preserves C_prefetch=N*2.
- Explicit close, epoch exhaustion and normal Inbox Drop publish monotonic
  admission termination before closing both raw channels and notifying the
  sole observer. No async semaphore or raw closed-waiter port is exposed.
  A concurrent send already past its precheck settles by its actual outcome.
  Closed is not drained/zero; panic propagates, and double-panic abort is not
  claimed recoverable. Case C root destruction is still outside this packet.

First tracer `capacity_wake_unwind_retains_exact_prefetch_before_another_dequeue`
failed only with E0599 for missing observe_capacity, including after the
single-observer signature contraction. These are compiler missing-seam REDs,
not a claim of a prior runtime loss. The test registers an actual panic Waker,
then requires both exact commands in order after real wake interruption.
Separate characterization gates will cover progress/registration races,
cancellation/lease release, close/normal Drop, non-Clone DATA and exhaustion.

### Notification implementation evidence — first review boundary

The first minimum GREEN passed 1/1 with the real wake panic caught. Seven
additional, separately run characterizations directly passed against it:
Both before registration; cancelled wait/exclusive lease release; both-lane
close before wake panic with queues retained; normal destruction and ordinary
caller unwind terminating a surviving observer; exact MAX then sticky epoch
exhaustion including wake panic; actual Full progressing before/after waiter
registration without repeated sends; and real non-Clone Driver DATA retaining
its byte permits, payload identity and one reducer handoff. These were direct
GREENs, not invented REDs. No production change was needed after the first
GREEN; only the authorized formatter layout changed those two Rust sources.

Final evidence, individually checked exit 0 under `p3-notify-final-`:

| Gate | Result | Log suffix |
|---|---|---|
| Mailbox / accepted first root / existing non-Clone regression | 17/17; 5/5; 1/1 | mailbox, root, nonclone |
| Mailbox serial repeats | Ten rounds, each 17/17 | repeat-1 through repeat-10 |
| Knife16 / repository provenance | 162/162; 13/13 | provenance |
| fmt / tracked diff / library check | PASS | fmt, diff, check |
| Standalone governance | -D warnings compile PASS; 6/6 | governance-compile, governance |
| Offline all-features Cargo governance | 6/6 | cargo-governance |

Individual tracer logs use `p3-notify-{prefetch-green,both,cancel,close,drop,
exhaustion,full-race,driver}.log`. `p3-notify-fmt-check.log` contains only
wrapping in these two files; scoped rustfmt preceded every final gate. Only
the historical smoltcp 22 and embedded-provenance 142 warnings were reported.
`p3-notify-audit.mjs` / `p3-notify-source-audit.json` verify 13 changed paths,
local links, tracked/untracked whitespace, all eight legacy test bodies with
their previously accepted exceptions, frozen first-root hashes and untouched
governance. Exact formatted lines: session.rs 852; progress.rs 434. Neither
needs a ratchet. This packet does not integrate the new progress observer into
the root, complete P3's two send cells/scheduler/Target, or close Case C/R7.

### Drained closure repair — independently accepted

The first implementation review returned 0/1/0 Request Changes. Main's
additional last-ingress audit and the Reviewer identified a narrower real
inconsistency: poll_next could observe **both** channel polls as Ready(None)
yet leave progress metadata Open while that inbox remained alive. This could
leave the surviving observation Pending indefinitely. Instant notification
on last Sender Drop is not required; observed dual-lane termination is.

One new tracer drains actual queued control/DATA after the last ingress exits,
registers a real panic Waker and keeps the inbox alive. It compiled and failed
only because end-of-stream did not notify (`p3-notify-drained-red.log`). Adding
only `self.close()` in the existing all_closed branch made it pass with the
actual wake panic caught, Closed observable and repeated None stable
(`p3-notify-drained-green.log`). Neither a single closed lane nor Pending takes
this branch. No new sender lifetime wrapper, callback or Drop rescue was used.

Post-format gates all exited 0, with the `p3-notify-closure-` prefix: exact
1/1, mailbox 18/18, first-root 5/5, old nonclone 1/1, ten repeat rounds each
18/18, provenance 162/162 and 13/13, fmt/diff/check, -D warnings standalone
compile plus 6/6, and Cargo all-features governance 6/6. Historical warnings
are unchanged. The refreshed audit.log verifies the same 13-path boundary,
old test bodies, first-root hashes and unchanged governance. Final counts are
session.rs 853 and progress.rs 480; both below their normal limits. This
supersedes the pre-review 852/434 snapshot without erasing its stop evidence.

The same Heisenberg re-reviewed the complete two-file notification packet and
the repair: P0/P1/P2 0/0/0, Approve, prior P1 closed, necessary optimization
none. Scoped Architecture/ModuleDepth/AuthorityLocality are 9/9/10. Independent
inverse SHA checks confirmed the one-line production repair and unchanged
prior 434 test lines; the read-only audit and selected final log excerpts were
checked. No independent Cargo rerun or complete-log reading is claimed.

### Mailbox packet continuation (superseded by the root packet below)

The mailbox packet is locally accepted; current paths are session.rs 853 and
its private progress.rs 480. First-root sources and governance are unchanged.
Next freeze the root's actual two-lane command-custody/rearm Interface, then
connect Full to this real observer with one causal tracer. The root must own
the observer and exact commands; no awaited send, sealed ClientDriver bypass,
busy retry, duplicated ownership or new facade is authorized implicitly.
Dynamic ready generations, clear/recheck, four budgets and Target progress
remain later portions of the same full P3 goal. Do not check their boxes based
on mailbox tests. Case C/R7/Task 4, public cutover and external gates remain
open. No new commit or push occurred in this packet.

Final status-flip review also closed with the same Reviewer at 0/0/0, Approve:
six current documents agree and eight source/governance fingerprints match.
That review ran no Cargo or tests and does not expand implementation acceptance.

## Root Control completion rearm — entry freeze

The next vertical tracer connects only the existing Control completion to the
accepted mailbox observer. OrderedSource command admission remains a separate
tracer: do not broaden the generic sender or sealed ClientDriver Interface to
make this first rearm compile. The two-lane ledger above remains unchecked.

Rust write scope is the existing private `owner/server_root.rs`, its
`scheduler.rs`, and `tests/ready_scheduler.rs`. The oversized owner route,
mailbox implementation/tests and governance are frozen. Main remains the sole
Writer; the same Reviewer checks the proposed Interface before implementation.
Entry checks: prior mailbox source audit PASS, standalone governance 6/6 and
the existing five root tests 5/5 (`p3-root-rearm-*` in the artifact directory).

The proposed constructor acquires the exclusive observer from its own ingress
before root publication. Busy returns the original ingress/request/result;
there is no arbitrary observer parameter or new reservation. A borrowed rearm
future retains command and observer in the root. Only a matching Control/Both
progress report republishes a blocked Full command. Unrelated lane progress
does not send or rearm. Closed retains the exact returned command; exhaustion
or unknown disposition remains sticky/nonzero. Rearm itself never sends.

Failure tree and falsifiable checks:

- Missing seam: the first focused tracer must fail because root rearm is
  absent, not a fixture/protocol/privacy failure.
- Lost registration race: real capacity released before and after wait
  registration must rearm once, preserving the returned Box until transfer.
- Busy retry: ordinary service visits without explicit rearm stay blocked;
  capacity stolen by another producer may return Full again, but cannot create
  another retry without new progress.
- Wrong lane: OrderedSource-only progress must not rearm Control. The future
  dual-cell owner must distribute both epoch changes, not discard either bit.
- Cancellation/unwind: dropping a borrowed wait retains observer and command;
  unknown send can never be converted back into a retryable command.
- Admission/termination: observer Busy exact-returns inputs; Closed and
  exhaustion do not prove drain or whole-root zero.

Acceptance requires a causal focused tracer, targeted branch tests, repeated
root/mailbox regressions, provenance, format/diff/check and both governance
routes, then the same independent implementation and status review. In-scope
causal repairs remain authorized; an architectural/Authority expansion stops.
No dynamic generations, four-budget proof, Target progress, P4, commit/push or
external acceptance is included in this bounded packet.

### Reviewed implementation and evidence

The same Heisenberg approved the pre-write freeze at P0/P1/P2 0/0/0, explicitly
as a single Control increment, not two-lane or full P3 acceptance. Implementation
now has one persistent observer acquired before publication, exact-returning
Busy inputs, a borrowing wait and synchronous post-observation state commit.
Full/Blocked becomes Full/Published only on Control/Both. The Box is unchanged
through that rearm interval; another actual send may rebox the command on Full.
Wrong-lane progress does not loop; Closed retains its Box and exhaustion
quarantines. Unknown/mismatched state is rejected before polling the observer.

One production send transaction handles both initial Held and rearmed Full.
It publishes unknown before moving into `try_send_event`; success, Full and
Closed retain the original settlement behavior. No new public Interface,
sender, task, command copy or second command position was introduced. Capacity
is still one root command plus one metadata-only observer for this foothold;
this does not implement the final `N*2` send-cell envelope or byte sizing.

The first exact compile RED was E0599 (missing rearm) plus E0433 (its missing
result type), then exact 1/1 GREEN (`p3-root-rearm-red.log` / `-green.log`).
Subsequent tests directly characterized the implemented branches. The one
unexpected compile RED was E0559: the wrong-lane fixture supplied flow-id to
LocalClose, which requires LocalFlow. The bounded test-only repair retained
the already-minted LocalFlow from PeerOpenRequested in the existing helper's
return tuple; old callers ignore that additional fixture value. No capability
constructor/getter or production repair was added. The same test then passed
(`-lanes.log` / `-lanes-repair.log`). Its queued close tests lane routing, not
protocol-level acceptance of a close before Target open.

Fresh logs use prefix `p3-root-rearm-` in the existing artifact directory:

| Evidence | Result |
|---|---|
| Initial exact RED→GREEN | Missing root seam → 1/1 |
| Cancel before/after wake; persistent exclusive observer | `cancel.log`, 1/1 |
| OrderedSource-only exclusion and real Both progress | `lanes-repair.log`, 1/1 |
| Competing producer steals capacity; exact second Full without busy retry | `stolen.log`, 1/1 |
| Busy exact-return, then original mailbox successful retry | `busy.log`, 1/1 |
| Real close retains the original Box and separately drains inbox | `close.log`, 1/1 |
| Root exhaustion outcome mapping, not a cross-Module epoch injection | `exhaustion.log`, 1/1 |
| Actual capacity Waker panic preserves root command + inbox prefetch | `wake.log`, 1/1 |
| Registered wait publishes rearm before returning Ready | `registered.log`, 1/1 |
| Full root set, including five prior identities and unknown/mismatch guards | `root.log`, 14/14 |
| Ten serial root repeat rounds | `repeat-1.log` through `repeat-10.log`, each 14/14 |
| Unchanged mailbox progress regressions | `mailbox.log`, 18/18 |
| Knife16 / repository provenance | `provenance.log`, 162/162 and 13/13 |
| Formatting, tracked whitespace, offline library check | `fmt.log`, `diff.log`, `check.log`, PASS |
| Standalone warnings-denied compile and governance | `governance-compile.log`, `governance.log`, 6/6 |
| Offline all-feature Cargo governance | `cargo-governance.log`, 6/6 |
| Tracked/untracked scope, whitespace, source fingerprints and local links | `audit.log`, PASS |

Only the preexisting 22 vendored smoltcp and 142 embedded Knife16 provenance
warnings remain. Final accepted lines are root 224, scheduler 70, test leaf
674. Owner.rs 2062, session.rs 853, progress.rs 480, test index and governance
are unchanged; no ratchet applies. The source audit compares against the saved
pre-write manifest and verifies the unchanged Git index. All gates above ran
serially. The same Heisenberg closed implementation/evidence review with
P0/P1/P2 0/0/0, Approve; necessary optimization none. Scoped Architecture /
Depth / Locality / System Design are 9/9/10/9. The Reviewer independently
checked 8/8 source/governance fingerprints and selected result excerpts; no
independent Cargo rerun or complete-log reading is claimed. Mencius separately
confirmed exact scope and no inventory change is needed.

### Historical continuation before lane receive acceptance

Superseded by **Owner lane receive prerequisite — accepted** below. Pending
claims and line counts in this subsection describe the earlier checkpoint.

The Control rearm, Owner source characterization and two-method driver
receive Move packets are accepted. Next freeze the lane-specific poll plus
shared two-cell transaction. Existing mixed
`try_recv_next` cannot select a lane before dequeue and its dependency Busy
path can park. Requiring both send cells empty would also block RESET behind
OrderedSource pressure. Use the selected-lane vacancy before a bounded poll;
do not add a generic pull callback or uncounted held command. The prospective
new owner path must not mix lane-poll and legacy receive on the same driver.
The new lane Interface is a design direction, not an accepted behavior;
its exact scope and dependency proof need independent plan review. Continue to
reuse existing Event/Driver ports without opening generic sender or sealed
ClientDriver. Preserve both lane changes in the same root before a yield; the
current one-cell consumption of Both cannot stand in for that transaction.
Keep actual byte/segment permits with Driver DATA, and do not turn request or
flow-id correlation into proof of physical ownership. No production source
cutover or copied actor-arrival provenance is implicit in that next slice.

Root test placement is now accepted at 87-line parent, 599-line Control and
126-line OrderedSource children (see below). Parent owner.rs stays 2062. Dynamic ready
generations, clear/recheck, four budgets and concrete Target progress are still
open portions of P3. No Case C/R7/Task 4 completion, P4 work, public cutover,
commit/push or external gate is authorized by this packet's acceptance.

Final doc-only status-flip review also closed with Heisenberg at P0/P1/P2
0/0/0, Approve: six documents agree, 8/8 source/governance fingerprints remain
stable, the index is empty and 22 links/whitespace checks pass. That review ran
no Cargo/tests and did not expand the single-Control acceptance boundary.

## Control characterization test Move — accepted

The private default-path `ready_scheduler/control.rs` child now owns all 14
existing Control tests and their two local helpers. Its 593-line moved body is
byte-identical to the original, SHA-256
`2ce7a6aaf9ea9712301c9e4e597b205e5090b698ddfaa895d6d5761f441f9705`.
The fully qualified test routes gain `control::`; the parent imports and
`owner_request()` are unchanged. Parent/child are 82/599 lines. No production,
fixture behavior, visibility, callback or governance inventory changed; no
oversized file shrank.

Evidence in `/tmp/mini-vpn-commit-boundary-A1DP9p/`:
`p3-two-lane-baseline.log` and `p3-two-lane-control-move.log` each pass 14/14;
`p3-control-test-move-{fmt,diff,provenance,governance,cargo-governance,check,audit}.log`
are green, including Knife16 162/162, repository provenance 13/13 and both
governance runs 6/6. The audit inverse-reconstructs the original 674-line source,
checks all test identities, frozen source hashes, 22 links and the empty index.
Only previously documented warnings remain.

Same independent Reviewer Heisenberg accepted this pure Move at P0/P1/P2
0/0/0, Approve, independently checking the inverse reconstruction and evidence
without running Cargo or writing. This is test layout acceptance only. Owner
DATA fixture preparation and the two-lane production Interface need separate
characterization/design checks; none is implicitly approved by this Move.

## Pristine Owner source characterization — accepted

Pure fixture preparation first extracted `owner_session()` returning the
original pristine model and same committed leg. The old request helper still
executes the original OPEN and effect extraction; the parent went 82→86.
Before/after Control tests passed 14/14, with fmt/diff and both governance
runs green. Heisenberg independently accepted that checkpoint at 0/0/0.

The next test-only step adds one private OrderedSource child: final parent,
Control and OrderedSource sizes are 87/599/126. A real Owner supervisor mints
its factory before OPEN, then supplies non-Clone TargetToClient DATA. Two
four-byte extents precede CLOSE at final offset 8. ACK 2 retains both extents
(8 bytes/2 segments), ACK 4 releases exactly one (4/1), and ACK 8 releases the
last (0/0), checked at flow probe, session factory and supervisor replay.
These are permit observations, not whole-flow/session settlement.

The first characterization failed only because it incorrectly required the
source payload pointer to survive replay normalization. `OwnedPayload::new`
in `src/resumable/tcp.rs` copies into bounded backing; `bind_replay` validates
flow, range, content and permits, not pointer identity across that boundary.
The test-only repair removed that invalid constraint, preserving the exact
contract observations. No production RED/GREEN or behavior change is claimed.

Evidence in the same local artifact directory: `p3-owner-fixture-*` preserves
the pure checkpoint; `p3-owner-source-characterization.log` records the initial
failure and `p3-owner-source-characterization-repaired.log` passes exact 1/1.
`p3-owner-source-{root,fmt,diff,provenance,check,governance,cargo-governance}.log`
pass root 15/15, Knife16 162/162, repository 13/13, both governance 6/6 and the
remaining checks. `p3-owner-source-audit.mjs/json` verifies mechanical fixture
extraction, unchanged Control/production/governance, whitespace, 22 links and
empty index. Only previously recorded warnings remain.

Same Reviewer Heisenberg accepted the final fixture/characterization packet
at P0/P1/P2 0/0/0, Approve, inspecting source, fingerprints and result fragments
without running tests. It does not prove source poll safety, RESET service
under Ordered Full, two-cell rearm, or any remaining full-P3 ledger item.

## Driver receive Implementation Move — accepted

Only the existing `try_recv_next` and `recv_next` methods and docs moved into
private default-path `tcp/driver_receive.rs`. Their 40/53-line bodies are
unchanged. The sealed client wrapper, its docs, visibility and mint mapping
remain at the parent, along with driver storage and all other behavior. This
preserves the inline client ACK provenance gate without widening scope into
owner_target.rs. No new Interface, forwarding layer, callback or receive
behavior was added. Final parent/child are 2883/106 lines (parent net -93).

Body SHA-256 values:

- `try_recv_next`: `15e87a9d589267f7774ca54a1661b331f68ae68fc4eabdbf9f1e6d52d324b4ed`
- `recv_next`: `dffe84c7024a4427dc032421885d8b0c3de9d0e0510ca276e5f4911a1626a9f2`

The final audit inverse-reconstructs the original parent SHA-256
`2a83667b9f4e1e21f2cfbf82a64592d839774a1b8ef923197a7c7749f2be6b19`.
Integrator alone lowered the unique production ratchet 2976→2883; governance
stays 699 lines, with all other bytes unchanged. Its final SHA-256 is
`171ea7c3283b28a1644e36bf728bae358f7a5dcaf1a114ad8ea13c549c70eaa8`.

`p3-driver-receive-move-*` under the existing artifact directory records TCP
68/68, exact client ACK source gate 1/1, client progress 3/3, Owner tests 15/15,
Knife16 162/162, repository 13/13 and fmt/diff/all-feature check green.
Standalone first returned only the expected shrink (5/6). Integrator's fresh
`-D warnings` standalone and Cargo offline/all-feature governance then pass
6/6; logs live in `/tmp/mini-vpn-p3-driver-receive-governance.khmorj/`.
`p3-driver-receive-move-final-audit.json` verifies exact movement, unchanged
client/other Rust, the single inventory delta, whitespace and empty index.

Same Reviewer Heisenberg accepted the final Move and ratchet at P0/P1/P2
0/0/0, Approve; scoped structure/Depth/Locality scores are 9/9/10, with no
required optimization. The review did not rerun tests or write. This acceptance
does not prove bounded poll, two-lane custody or full P3 completion.

## Owner lane receive prerequisite — accepted

The reviewed two-file behavior packet adds only `poll_owner_control` and
`poll_owner_ordered_source` in `tcp/driver_receive.rs`, plus real Owner
fixtures/tracers in the existing OrderedSource test leaf. Final sizes are
148/474; tcp.rs remains 2883, root/scheduler 224/70, Control tests 599 and
governance 699. No ratchet change was needed. Both original mixed receive
methods retain the hashes above; the sealed client wrapper is unchanged.

Control polls RESET once and returns immediately on Some; otherwise it polls
the completion queue once. Pending RESET cannot hide a ready completion. Only
two actual Ready(None) results end Control. OrderedSource polls only its
DATA/CLOSE FIFO once and directly moves the existing command. Neither port
adds a queue, prefetch, allocation, callback, streak mutation or other-lane
pull. Visibility stays internal to owned_upstream; parent storage and existing
producer/settlement interfaces are untouched.

The receive proof is bounded to the audited Tokio 1.52.0 and closed try-only
producer graph. Cooperative checks and receiver registration precede dequeue;
the successful pop/permit-return/plain-move path has no registered sender
waiter to invoke. This is not wait-free, producer wake-unwind atomicity, or
enforcement that one Driver cannot mix old and new receive routes. RESET's
existing queue publication precedes its admission-bit store. Root integration
must separately enforce exclusive lane use, store commands before publication
or yield, and close the shared two-cell transaction.

TDD and falsifiable branches:

- Extract the pristine Owner fixture without dequeuing; existing root 15/15
  stays green before the first new tracer.
- Isolation produces the predicted compile RED: two missing methods, three
  E0599 diagnostics, no runtime test. Minimum production GREEN is 1/1.
- Seven later characterizations directly green: sink completion with RESET
  Pending; retained completion after source close; DATA/CLOSE drainage with
  returned DATA still owning 4 bytes/1 segment; Control and OrderedSource
  cancellation/re-entry with real wakes; RESET priority; producer-close wake
  and actual EOF. The old DATA/CLOSE/application-ACK assertions are retained.
- No sleep, spawned task, manual wake, fake ACK or session-zero inference.
  Full/Closed/unknown send custody and cross-lane fairness remain later work.

Evidence is in `/tmp/mini-vpn-commit-boundary-A1DP9p/`:

| Gate / artifact | Result |
|---|---|
| `p3-lane-poll-{fixture-characterization,red,green}.log` | 15/15 → expected E0599 → 1/1 |
| `p3-lane-poll-root.log` | Owner root 23/23 |
| `p3-lane-poll-repeat-1.log` through `-10.log` | Owner source leaf 9/9 each round |
| `p3-lane-poll-{tcp,client-provenance,client-progress,mailbox}.log` | 68/68, 1/1, 3/3, 18/18 |
| `p3-lane-poll-provenance.log` | Knife16 162/162, repository 13/13 |
| `p3-lane-poll-{fmt,diff,check}.log` | fmt/diff/all-feature lib check PASS |
| `p3-lane-poll-governance-compile.log` and both `*-governance-serial.log` | rustc -D warnings PASS; standalone/Cargo all-feature 6/6 each |
| `p3-lane-poll-final-source-audit.json` | exact old methods, two-file Rust scope, frozen inventory/docs, whitespace/links, empty index |

One rustfmt failure affected only test-leaf call wrapping; the scoped repair
and subsequent global check passed. An earlier governance pair briefly
overlapped; the final `-serial` pair was rerun in sequence and is the acceptance
evidence. Existing smoltcp 22 and provenance-embedded 142 warnings remain.
No new non-vendored implementation warning was introduced.

Same Reviewer Heisenberg accepted source and final gates at P0/P1/P2 0/0/0,
Approve, Clean/System/Ownership 9/9/10; no required fix or optimization.
Review checked fingerprints, fixture inverse reconstruction, whitespace and
selected log excerpts, not a second Cargo run. Full-P3 two-cell custody,
dynamic ready leases/fairness/budgets, Target progress and integration remain
unchecked. No P3 commit/push or external run occurred.

## Two-cell representation Phase A — accepted

This green-to-green representation convergence is separate from new two-lane
behavior. Only server_root.rs, scheduler.rs and the existing Control test leaf
changed; final lines are 348/51/604. Driver receive/source tests, session,
owner.rs, client routes and governance remain byte-identical to the accepted
entry manifest. No oversized shrink or inventory edit occurred.

The root owns two fixed SendCell positions and the existing sole capacity
observer. Readiness now belongs to each owning phase; scheduler stores only
the persistent lane cursor. OpenCompletionCustody and independent ReadyClaim
are deleted. The constructor still admits only one real OPEN completion.

| Prior representation | Owning-phase representation |
|---|---|
| Held / Published | Held(Fresh, Ready) |
| FullReturned / Blocked | Held(Returned, CapacityBlocked) |
| FullReturned / Published | Held(Returned, Ready) |
| MailboxOwned / Retired | Vacant root position; no mailbox/session-zero claim |
| TransferringUnknown | Unknown with correlation, no fabricated payload |
| ClosedReturned | ClosedReturned with the actual Fresh/Returned storage |

Unknown, invalid correlation/lane and sticky quarantine block every progress
entry. Send publishes Unknown before invoking the mailbox. Full returns the
actual Box and blocks that phase; matching capacity rearms it. Both applies
all applicable changes before return; Closed moves every known Held storage
to ClosedReturned. The A constructor cannot populate OrderedSource, so those
two-live-cell behaviors require B tracers, not inference from this code shape.

All 14 Control test identities remain, with one intentional rename: the old
independent ready-claim mismatch is now unrepresentable; its replacement swaps
the actual completion into the wrong lane and proves exact retention, no send
and sticky quarantine. There remain 124 assertion macros. Counts support the
equivalence review but do not substitute for it or runtime evidence.

An initial mechanical test migration incorrectly spanned nested let-else
blocks and failed compilation with an unclosed delimiter; no test ran. All
three owned changes were reverted to the exact saved baseline and root 23/23
was re-established. The repaired indentation-bound migration verified all
14 test names and 124 assertions before writing, then passed Control 14/14
and root 23/23. This was a test-feedback repair, not a behavior RED or license
to remove assertions. A subsequent single-match simplification and scoped
rustfmt each retained green characterization.

Final evidence under `/tmp/mini-vpn-commit-boundary-A1DP9p/`:

| Artifact | Result |
|---|---|
| `p3-two-cell-before.log`, `p3-two-cell-rollback-root.log` | 23/23 each |
| `p3-two-cell-prep-root.log` | Post-format root 23/23 |
| `p3-two-cell-prep-repeat-1.log` through `-10.log` | Control 14/14 each |
| `p3-two-cell-prep-{mailbox,tcp,client-provenance,client-progress}.log` | 18/18, 68/68, 1/1, 3/3 |
| `p3-two-cell-prep-provenance.log` | Knife16 162/162, repository 13/13 |
| `p3-two-cell-prep-{fmt,check}.log`, diff/source audit | PASS |
| `p3-two-cell-governance-compile.log`, `p3-two-cell-prep-{governance,cargo-governance}.log` | -D warnings compile; serial standalone/Cargo 6/6 each |
| `p3-two-cell-prep-final-source-audit.json` | Three-file scope, frozen other sources/inventory/docs, test inventory, whitespace/22 links, empty index |

Same independent Reviewer Heisenberg accepted the implementation and final
gates at P0/P1/P2 0/0/0, Approve, independently checking source equivalence,
18 manifest fingerprints and final result excerpts without another Cargo run.
Only the existing smoltcp 22 and provenance-embedded 142 warnings remain.
No new Driver admission, source cutover, Target work, commit/push or external
run occurred. Full P3 and its two-lane acceptance item remain unchecked.

### Historical Phase B freeze before its first behavior tracer

The reviewed A-to-B plan next permits root-local admission from the existing
Owner lane polls. The selected cell must be Vacant; immediately store a
returned command before validation, publication or yield. Other-lane emptiness
must not bypass root Unknown/quarantine. Driver commands use the existing
try_send_driver; LocalClose is OrderedSource despite its Control enum variant.
No generic by-value admission, getter, ClientDriver bypass or extra queue.

Only root/scheduler/Control, the OrderedSource parent's private child route,
and a new private ordered_source/send_cells.rs test leaf are allowed. Existing
OrderedSource fixtures/tests stay unchanged. First tracer: real DATA fills the
ordered mailbox; the next DATA becomes exact Full, CLOSE stays unpulled,
RESET still crosses Control, cancelled capacity observation retains DATA,
and actual ordered capacity permits exactly one retry. Later distinct tracers
cover Both, stolen capacity, Fresh/Returned close, send-wake Unknown and
corruption. No behavior test is claimed executed yet.

Borrowing a Driver is a conditional per-call primitive, not proof of matching
Owner/ingress or persistent exclusion of legacy receive callers. Production
binding/cutover, dynamic ready leases, budgets and Target progress remain on
the full P3 ledger. Do not invent a lifecycle wrapper to hide that obligation.

## Conditional two-cell behavior Phase B — accepted

The reviewed A-to-B plan executed within three Rust paths: server_root.rs
405 lines; OrderedSource parent 476 (only private `mod send_cells;` added);
new ordered_source/send_cells.rs 771. Parent fixtures and nine existing tests
are unchanged. Scheduler51, Control604, Driver148/tcp2883, session853 and
governance699 are frozen. No ratchet or public/client Interface changed.

The private root lane poll checks whole-root Unknown/quarantine, then only the
selected cell's vacancy. It performs one existing bounded lane poll, stores
Some into actual Fresh custody before any validation/publication/yield, then
checks Driver category and actual lane classification. Occupied is an explicit
outcome, not a Pending without registration. Actual receiver Pending/None
maps to Pending/Drained; neither EOF nor an empty root position settles DATA.
LocalClose remains OrderedSource even though its Driver variant is Control.

Private storage dispatch now sends Event or Driver through the existing ports,
after root Unknown publication. Actual Full/Closed returns retain their real
command/Box; unsupported storage is returned intact to sticky quarantine.
No ClientDriver conversion, generic by-value admission, getter, extra queue,
callback or production mixed-receive change was introduced.

The same-Owner fixture mints a second increasing OPEN (id2), refuses it through
the existing completion constructor and applies that receipt in its original
supervisor. Its established flow1 supplies real DATA, RESET and sink completion
commands; no new constructor or synthetic command authority is required.

One-at-a-time evidence, using prefix `p3-two-cell-behavior-` under the existing
`/tmp/mini-vpn-commit-boundary-A1DP9p/` artifact directory:

| Log | Exact observation |
|---|---|
| `red.log` → `green.log` | Only missing poll_driver_lane / OwnerLaneAdmission (E0599/E0433, ten diagnostics) → exact 1/1 |
| `both.log` | Both epochs make both cells ready before the observer's Ready return/drop; 1/1 |
| `stolen.log` | Competing actual commands refill both lanes; both exact Full returns wait for new progress; 1/1 |
| `close.log` | Real close observes C-Full/O-Fresh and O-Full/C-Fresh, retaining actual storage without retry; 1/1 |
| `unknown.log` | Real enqueue Waker panic on either lane, with other cell Vacant or Held; actual receipt, no retry/dequeue, survivor retained; 1/1 (four caught panics) |
| `corruption.log` | Swapped real returned commands retain both Boxes and permits; all entries quarantine; 1/1 |
| `poll-cancel.log` | Cancel/re-enter either actual lane poll, real wake, root custody before Ready, both EOF while returned DATA still owns permits; 1/1 |

The first tracer additionally proves DATA2 exact Full while DATA1 fills the
ordered mailbox, unpulled CLOSE, independently admitted/sent Control RESET,
cancelled/replaced capacity wait, actual capacity wake and one successful retry.
RESET stays unreduced while DATA ownership is checked. Box identity is asserted
only within a held/rearm interval, never across a real retry that may rebox.
The six subsequent characterizations directly green against that minimum
implementation and the accepted A phase model; no artificial behavior RED is
claimed. The close matrix is close-observation evidence, not a newly executed
Driver direct-send-Closed tracer or whole-session settlement.

Final gates are root30/30, ten serial root rounds30/30, mailbox-full18/18,
TCP68/68, client1/1+3/3, Knife16 162/162, repository13/13, fmt/diff/all-feature
lib check and serial standalone/Cargo governance6/6 each. Logs are `root.log`,
`repeat-1.log` through `repeat-10.log`, `mailbox-full.log`, `tcp.log`,
`client-provenance.log`, `client-progress.log`, `provenance.log`, `fmt.log`,
`check.log`, `governance.log` and `cargo-governance.log`. Source audit
`p3-two-cell-behavior-final-source-audit.json` binds 19 dirty paths, exact parent
inverse reconstruction, frozen other sources/inventory/docs, limits, whitespace,
22 links and the unchanged HEAD/empty index.

One gate-runner mismatch stopped the sequence: `mailbox.log` selected only the
progress child and passed10/10, not the required18. Bounded comparison with the
accepted module inventory identified the filter error. No source changed;
the full-module rerun18/18 is the acceptance evidence. No implementation or
fixture repair was needed in B. Existing warnings remain smoltcp22 plus142
provenance-embedded warnings; no new non-vendored implementation warning.

Heisenberg's independent final source/test/evidence review is P0/P1/P2 0/0/0,
Approve; necessary optimization none. It verified the 19 source/doc fingerprints
and result excerpts without another Cargo run. Approval is conditional on a
matching Owner Driver and excludes persistent production association, exclusive
caller routing, public cutover, full P3 and Case C/R7/Task4 completion.
No commit/push or external run occurred.

## Persistent capacity registration prerequisite — accepted

The next ready/park audit found an actual registration gap: changed() owned a
temporary Notified, so cancelling that borrowing future removed its Waker even
though the root retained its observer lease, epochs and Full command. The new
root tracer first passed receipt/Full-Box/Busy/NoRunnable assertions and failed
only at the real wake count:0 instead of1. This was expected runtime RED, not
a missing-symbol or fixture failure. Pre-write standalone governance passed6/6.

The four-file implementation is session.rs896, session/tests/progress.rs480,
server_root/tests/ready_scheduler.rs88 (one private route only), and its new
capacity_registration.rs344. The root405, scheduler51, Driver148, prior B
leaf771, other source files and governance699 remain byte-for-byte unchanged.
No oversized shrink or inventory edit, dependency change or visibility growth.

The observer now owns a concrete Pin<Box<OwnedNotified>> and a non-Clone RAII
lease. A shared Arc<Notify> is allocated per mailbox; one cached Box per
successfully admitted observer is reused, never allocated per poll/reset.
No dynamic future, task, channel or command-capacity reservation was added.
The plan review rejected allocate-before-CAS: k concurrent losing callers could
retain k allocations. Revised CAS→RAII lease→registration allocation→seen read→
cache reset→publication closed that P1 before implementation, at0/0/0 Approve.
Locals unwind in reverse declaration order and final fields drop in declaration
order: registration is removed before the lease allows a replacement observer.

The unchanged async Interface borrows a private poll implementation. Pending
poll installs its Waker in the owned cache; dropping only the outer borrow does
not unlink it. Logical Ready resets first, then commits seen, preserving epoch
re-entry if Waker destruction unwinds. Inner notification Ready resets before
rechecking. A no-change path polls its existing cache without reconstruction.
Tokio1.52.0's notified_owned captures broadcast count at creation; its poll
detects a broadcast racing the state check, so enable is unnecessary here.
Only one mutable Inbox writer publishes after epoch/terminal commit, using
notify_waiters only. Given a baseline no earlier than seen, at most one late
already-observed broadcast can remain: ≤2 cached polls and ≤3 state checks.
This is a closed-graph call-count proof, not wait-free dependency latency or a
global park proof. Adding notify_one, another writer or an untracked publisher
requires a new proof. No arbitrary loop limit/self-wake/fake Pending is used.

Exact artifacts use `p3-capacity-registration-` under
`/tmp/mini-vpn-commit-boundary-A1DP9p/`:

| Log | Observed result |
|---|---|
| `red.log` → `green.log` | Same root tracer: only final wake0-vs1 RED →1/1 GREEN |
| `epoch-contract.log` | Existing exact test1/1; intentional wake totals0/1→1/2, all epoch/Busy/reacquisition assertions retained |
| `replacement.log` | Two cancelled Pending borrows, real dequeue wakes only replacement Waker; same Full Box and eventual receipt;1/1 |
| `close.log` | Real close and Inbox destruction each wake the cancelled borrower, preserve root ClosedReturned Box and exact new-send Closed return;1/1 |
| `drop-order.log` | External references released with probe count0; owning root Drop causes count1/Busy, then replacement succeeds;1/1 |
| `wake-unwind.log` | Real dequeue calls panicking Waker (caught); root Box and actual mailbox receipt survive, no duplicate observed epoch;1/1 |

Four subsequent lifecycle characterizations directly GREENed against the
minimum implementation. Drop tests describe registration destruction, not
whole-root command/session settlement. Mailbox private fields remain closed;
root tests use real receipts and the existing capacity observation Interface.
The old progress test's only edits are two expected counts and their diagnostic;
the inverse audit reconstructs its exact accepted prior hash.

Final post-format gates: root35/35, ten serial rounds35/35 each, mailbox18/18,
TCP68/68, client provenance1/1 and progress3/3, Knife16 162/162, repository13/13,
fmt/diff/all-feature library check green, rustc -D warnings governance compile,
standalone and Cargo all-feature governance6/6 each. The two `*-gate-evidence.json`
manifests record exact argv, exit and actual counts; the runner stops on any
exit/count mismatch. Source audit `p3-capacity-registration-final-source-audit.json`
binds20 dirty paths, inverse route/progress checks, frozen other sources and
inventory, normal size limits, all-path whitespace and unchanged HEAD/empty
index. Warning inventories match prior B exactly: smoltcp22 and embedded
provenance142; no new implementation warning.

Same independent Reviewer Heisenberg accepted implementation and gates at
P0/P1/P2 0/0/0, Approve. Scoped Architecture/Clean Architecture/Depth/System
Design scores are9/9/9/9; Authority Locality10, necessary optimization none.
It independently checked all20 fingerprints and inverse/command evidence and
related log excerpts, not another Cargo/test run or full log read. Integrator
Mencius separately verified constructor/Drop/bounded-loop proof and9 Rust
fingerprints; no ratchet action was needed. The same Reviewer approved factual
six-document status promotion at0/0/0; implementation stayed frozen.

No production binding, full scheduler/budget, Target, P4+, commit/push or external
work occurred. Full P3 and Case C/R7/Task4 remain incomplete.

### SessionCommands naming preparation — implementation accepted

The symbol-first audit distinguished three scopes: each ResumableTcpDriver
belongs to one TargetReadSource under OwnerTargetExecutor; the current command
prototype owns one session's two send cells; the future process scheduler must
select across sessions. Neither a borrowed Driver nor a renamed type supplies
production binding. The accepted plan keeps P3 command scheduling separate
from P4 physical/bootstrap storage and P6 composition/caller cutover.

The pure Rename changed only OwnerServerRoot to SessionCommands and
OwnerServerRootBuildFailure to SessionCommandsBuildFailure in four existing
files: server_root.rs405, control.rs604, capacity_registration.rs344 and
ordered_source/send_cells.rs771. Inverse replacement restores all four prior
SHA-256 values. Fields, bodies, visibility, module/test FQ routes and assertions
are unchanged; no alias, wrapper, empty replacement root or new Interface.
The private derived Debug failure-type label changes with the identifier;
diagnostic strings are not claimed byte-identical.

Artifacts use `p3-session-commands-` under the same temporary evidence directory.
Prewrite standalone governance6/6 and before/after root35/35 passed. Final
regressions: root35/35 plus ten serial rounds35/35, mailbox18/18, TCP68/68,
client1/1+3/3, Knife16 162/162, repository13/13, fmt/diff/all-feature check,
standalone/Cargo governance6/6 each. Both gate-evidence manifests retain exact
commands, exits and counts. The final-source audit freezes all20 dirty paths,
checks four inverse hashes, whitespace/links and unchanged HEAD/empty index;
no oversized shrink or governance edit. Same Heisenberg independently accepted
the Rename implementation at0/0/0 Approve, scoped structure9/10, necessary
optimization none. That review read hashes/log excerpts, not another Cargo run.
The same Reviewer approved the final gates and factual document promotion at
P0/P1/P2 0/0/0; implementation remained frozen.

Read-only capacity audit permits a staged ReadyProcessPlan for N, four budgets,
4N+1 cells and checked T_cell/Q_cell. It is not the full ready envelope:
later composition must supply real O/F/L_actor/L_root and check all remaining
domains before their allocation/bind. A positive scalar is not a physical
position witness. Capacity counts fitting usize do not imply service products
fit; this is the next focused counterexample, before metadata allocation.
No ready lease, global dispatch/park, production binding, Target, P4+, commit,
push or external gate was implemented by the Rename.

### Checked process arithmetic — implementation and gates accepted

This separate three-file packet adds root-private ReadyProcessPlan, ReadyClass
and precise configuration errors. It checks N>0, each original budget>0,
4N+1 process cells, N+1 Settlement cells, N cells for each other class,
B_total and every T_cell/Q_cell in the frozen order. Safe quotient/remainder
ceil avoids numerator overflow; no budget truncation, saturation or fallback.
All loops are fixed-size value work, without allocation, task, callback,
resource admission, publication or new visible Interface. This is not a full
physical ready envelope; O/F/L_actor/L_root, layout and remaining domain checks
must precede their future allocation/bind. It establishes formulas, not actual
dedicated slices, cursor fairness or root park.

Prewrite standalone governance6/6 preceded the first exact tracer. Its only
RED was three E0433 errors for the frozen missing plan/error/class types; the
minimum checked constructor then passed1/1. This is missing-seam TDD, not a
claim that an existing runtime scheduler failed. Eight subsequent independent
characterizations each directly GREENed without further production behavior
changes. The nine tests cover small capacities with service-product overflow;
the normal N3/budgets1,2,7,1 vector; zero N; each zero budget; capacity-before-
budget-sum overflow; original-budget-sum overflow; MAX-1; exact MAX followed
by one more budget; and each later class's product overflow while earlier
classes fit. The before-allocation claim is constructor-source evidence, not
an allocator observation made by the test name.

Final exact lines: server_root.rs504, ready_scheduler.rs89, new private
ready_scheduler/process_plan.rs115. Removing only the added plan/docs and test
route restores both existing source hashes. SessionCommands, scheduler51,
prior lifecycle leaves, Driver148, session896/progress480, governance699 and
all other sources remain unchanged; no oversized shrink or ratchet action.

Artifacts use `p3-process-plan-` under the same evidence directory: red.log,
nine individually named exact logs, both gate-evidence manifests, and
final-source-audit.json. Post-format gates passed root44/44, mailbox18/18,
TCP68/68, client1/1+3/3, Knife162/162, repository13/13, fmt/diff/all-feature
library check and standalone/Cargo governance6/6 each. No repeat rounds are
claimed for this pure arithmetic packet. The audit binds21 paths, exact
inverse scope, whitespace/links and unchanged HEAD/empty index. Warning
headers match the Rename baseline; only the existing smoltcp22 and embedded
provenance142 warnings remain.

Same Heisenberg approved implementation and final gates at P0/P1/P2 0/0/0;
structure/Depth/Locality9/9/10, necessary optimization none. It independently
checked fingerprints, arithmetic/tests and relevant result/log excerpts, not
another Cargo run or every log line. The same Reviewer approved factual
six-document status promotion at P0/P1/P2 0/0/0; implementation remained frozen.
No commit/push/external gate occurred.

### Selected-send Extract Method — independently accepted

One-file pure movement separates lane selection from the selected transfer.
`drive_one_unit` retains whole-custody validation, Ready filtering and cursor
advancement unchanged; `transfer_held_lane` contains the complete original
Unknown/send/Ok/Full/Closed/restore/quarantine transaction. It is not a new
self-validating entry: its sole current caller proves validated Held+Ready.
Future ready callers must prove those conditions without another selection.

Root504 became508 through exactly four inserted lines. Removing them restores
the accepted504 SHA-256; the other20 dirty paths, HEAD532d766 and empty index
are frozen. Before/after root44/44 and final root44, mailbox18, TCP68,
client1+3, Knife162, repository13, fmt/diff/all-feature library check and both
governance6/6 pass. No repeats are claimed for this pure movement. Evidence
prefix is `p3-selected-send-` under the same /tmp evidence directory: before/
after logs, regressions/closure gate manifests and final-source-audit.json.
The latter binds all21 paths and inverse scope; governance remains699.
No oversized shrink, new Rust path, behavior, ratchet or external action.

Same Heisenberg independently approved this implementation and frozen gates
at P0/P1/P2 0/0/0, structure/Depth/Locality9/9/10. It checked fingerprints,
manifests and relevant results, without a separate Cargo run. This approval
does not accept the following ready design or any actual scheduling behavior.

### Current continuation: real dispatch, ready ownership and service bounds

The first actual-dispatch proposal is at
`/tmp/mini-vpn-commit-boundary-A1DP9p/p3-ready-dispatch-proposed-plan.md`.
Before behavior Rust, independent initial review returned two
P1s and Request Changes: (1) rearm Ready(UnrelatedLane) can reset the observer
while another Full remains, requiring another registration unit before park;
(2) per-slot guards do not serialize shared ancestor words, so clearing and
coalesced epoch acknowledgement need explicit acquiring RMW order. The revised
proposal retained the same four-file scope. Same Heisenberg closed both P1s
at0/0/0 and approved the design (Architecture/Clean/System/Ownership9/9/9/9).
It specifies one rearm poll per visit, remaining-Full registration publication,
AcqRel ancestor clear/Acquire child recheck/Release restore, and one checked
AcqRel epoch CAS with bounded Rescan on failure. These are pre-code contract
corrections, not a reproduced runtime bug. The subsequent implementation is
reported separately below. The factual Extract six-document promotion also
passed independent review0/0/0; that is not ready implementation approval.

Mencius's independent fixture audit confirmed the existing real APIs suffice:
distinct authenticated A/B models, two B flow requests, one B dequeue inside
A's actual receiver Wake, and retained real receipts. The test must finish B
rearm/send before dequeuing A, whose extra capacity wake could mask a loss.
Use a synchronous manual-poll fixture, no cooperative-budget retry loop,
separate root/B Wakers and no fixture-lock capture in those notifications.
This audit is not an architecture or runtime verdict.

That design froze the amendment's process/local generations, source/aggregate
ready leases, persistent cursors, clear/recheck/park and dedicated class bounds.
The behavior must actually dispatch SessionCommands selected by metadata,
not add another empty root or counter-only substitute. Use distinct actual
PeerOpenRequest session IDs and independent inbox receipts; calling the fixed-
ID fixture twice is insufficient. Preserve stable session correlation after
commands transfer: current SessionCommands cannot recover identity from vacant
cells. Its exclusive observer proves mailbox exclusivity, not session binding.
Select a real bounded owning seam and one falsifiable tracer before writing;
do not invent P4 table/bootstrap custody or a wrapper standing in for production
binding. Full-P3 association/exclusive routing, the reserved exact filters and
all remaining ledger items still require evidence. Target progress follows its
own characterization/Move and static-port sequence. A 771-line lifecycle leaf
is not permission to exceed800 or compress tests to conceal growth.

### First actual ready-dispatch tracer — independently accepted

The four-file candidate introduces a private OwnerServerRoot containing real
SessionCommands residents and stable IDs derived from their sealed initial
OPEN requests. Initial Ordered cells must be vacant; the packet does not
invent Driver association or an existing P4 admission/physical table. Cold
validation/reservation errors return the original plan and input vector.
All command moves occur after fallible preparation; allocation abort is not
claimed recoverable. Existing SessionCommands and SendCursor bodies inverse-
match their accepted hashes. No other Rust source or governance changed.

ReadyIndex owns preallocated process/local cells, persistent circular cursors
and a hierarchical summary. It is the sole clearer/selector. Each publisher
holds an immutable generation tuple and Weak metadata, never a command; the
slot guard spans revision/local-bit/ancestor publication. No callback, source
poll or DATA operation runs under that guard. Shared-ancestor clearing uses
AcqRel RMW, Acquire child recheck and Release restoration. One root-owned
cached notification uses a coalescing epoch: Release dirty publication, one
checked AcqRel acknowledgement CAS, actual waiter poll, summary rescan and
epoch reread. History-Ready returns bounded Rescan, not fabricated Pending.
Slot reuse, migration and actual non-Session classes remain inactive.

Root retains the selected visit before source work. A visit executes the
chosen lane's complete transfer or one capacity-rearm poll; it cannot select
again or conflate a source Pending with process-wide Pending. Every rearm
Ready that leaves serviceable Full commands republishes registration work.
Only an actual registered Pending may clear that obligation. Quarantine takes
precedence; retaining command/ready custody never proves whole-root zero.
The implementation of this first handoff is independently accepted. Remaining
dedicated tracers are still required; one test does not close the full contract.

The reserved exact tracer uses two authenticated SessionModels with distinct
IDs and two real B flow requests. B starts with an exact Full command and
registers its capacity wait. A's actual send wakes a fixture that dequeues B's
prefill into retained custody. The process/local index must then rearm and
send B, without another notification and before A is dequeued. All three real
receipts are reduced by their matching models. A historical notification
produces one bounded root Rescan, then a real registered Pending.

RED compiled only far enough to identify the missing Root/Step/Unit seam:
nine E0433 diagnostics, no fixture/privacy/protocol error. First GREEN attempt
compiled and passed all dispatch/receipt/park assertions, then failed at
teardown. A bounded backtrace proved Root Drop -> last Sender Drop -> A's
unused Ordered receiver Waker -> a second empty B dequeue. The single test-only
repair replaces A's two receiver registrations with noop AFTER the proof,
releases its trigger Waker, requires Arc::try_unwrap and closes owned B outside
the mutex. No production change or extra wake rescued the handoff interval.
This does not claim arbitrary user-Waker/root-destruction settlement.

At the first handoff checkpoint: root823, scheduler598, route90, leaf261. Exact
test1/1 and ten serial post-format repeats pass; root45, mailbox18, TCP68,
client1+3, Knife162, repository13, fmt/diff/all-feature library check and both
governance6/6 pass. Only the existing smoltcp22/embedded provenance142 warning
headers remain. The `p3-ready-dispatch-` evidence prefix contains red,
green-attempt, failure-backtrace and green logs; repeat/regressions/closure
manifests; and final-source-audit.json. The audit binds22 paths,22 links, four-
file scope, exact prior-body inverses, whitespace, HEAD532d766 and empty index.
No ratchet edit, second behavior, commit/push or external gate occurred.

Same Heisenberg approved implementation and gates at P0/P1/P2 0/0/0;
structure/Depth/Locality/System9/9/10/9, necessary optimization none. It checked
all four files,16 Rust/governance fingerprints, inverses,22 links, manifests and
relevant log excerpts without another Cargo run. One status-only P2 identified
an unqualified historical "no new ready behavior" sentence; it is now scoped
to the earlier Extract stage. This does not reopen the accepted source.

Mencius's bounded independent audit found no critical-interval rescue, callback
cycle or allocation after final command migration. Explicit new backing counts
are peak16+m and retained13+m for m input residents, excluding inputs and their
existing mailboxes/observers. These are backing-object counts, not allocator
bytes: no complete cold-peak TOTALBYTE witness is proved. capacity_waker is
cold construction only; ordinary clones share its Arc allocation. Full P3,
production binding, remaining generations/clear/park/fairness/lifecycle, Target,
slot reuse and Case C/R7/Task4 remain incomplete. No second behavior had started
at that checkpoint.

Final same-Reviewer status closure is P0/P1/P2 0/0/0, Approve. The remaining
historical/current-gate wording conflicts are closed;16 Rust/governance hashes,
22 links and HEAD/index remain verified. This receipt changes no source or
acceptance scope. That checkpoint prioritized remaining-Full registration;
the following whole-contract audit found a causal §7 repair to close first.

### Scheduler claim/finish fault classification — independently accepted

Re-reading the accepted scheduler amendment's §7 found an implemented P1,
not just a missing future tracer: claim/finish errors called resident-only
quarantine, which cleared its ready metadata and let other sessions continue.
§7 requires scheduler corruption/exhaustion to be root-fatal. The first handoff
review/proposal did not waive this section; its narrow approval stays valid.
Same Heisenberg confirmed the mismatch at0/1/0, Request Changes.

Frozen scope was only `OwnerServerRoot::poll_ready_unit` in `server_root.rs`
and one new test in `tests/ready_scheduler/ready_dispatch.rs`. No scheduler
hook, getter, visibility, source transaction, production caller, admission or
settlement change was authorized. Pre-write standalone governance passed6/6.
The failure tree predicted a real metadata Running/root Idle mismatch would
make poll1 quarantine A and poll2 send B, while correct behavior would remain
root-terminal and keep both original Held completions. Compilation/fixture
errors would be unrelated feedback-loop failures, not the intended RED.

`scheduler_claim_corruption_stops_all_residents_without_discarding_commands`
creates two authenticated models, real requests/mailboxes and a root. Calling
the existing private ready.claim(0) without a matching root visit deliberately
creates the mismatch; it adds no production failpoint or synthetic command.
Exact RED compiled and ran one test: first Quarantined passed; second returned
Visited(B, Control, Transfer(MailboxOwned)) instead of Quarantined, exit101.

Minimum GREEN maps claim/finish errors to first-fault root terminal rather
than resident quarantine. Terminal is checked before in-flight recovery, so
a failed finish preserves Running and the source's actual disposition, even
if the command already entered the mailbox. Source unknown/correlation failure
still isolates its resident. This is classification by origin, not by shared
ReadyFault::Corrupt spelling. No dubious metadata is normalized on this path.
The test now observes sticky terminal, repeated Quarantined, both exact Fresh
completion events/origins still Held+Ready, vacant Ordered cells, and empty
real inboxes. Models/commands remain owned fixtures, not whole-root settlement.

Evidence under `/tmp/mini-vpn-commit-boundary-A1DP9p/`:

| Gate | Result / artifact |
|---|---|
| Causal RED | `p3-ready-fatal-claim-RED.log`, actual second-session send |
| Exact GREEN | `p3-ready-dispatch-scheduler_claim_corruption_stops_all_residents_without_discarding_commands.log`, 1/1 |
| Post-format repeats | `p3-ready-fatal-repeat-gate-evidence.json`, ten serial1/1 |
| Regressions | `p3-ready-fatal-regressions-gate-evidence.json`, root46/mailbox18/TCP68/client1+3 |
| Closure | `p3-ready-fatal-closure-gate-evidence.json`, Knife162/repository13, fmt/diff/all-feature lib check, standalone-Dwarnings and Cargo governance6/6 |
| Mutation audit | `p3-ready-fatal-source-audit.json`, root825/leaf333; exact inverse root and unchanged first261 test lines |

The audit binds22 paths, only two changed Rust files, fourteen other Rust/
governance fingerprints, whitespace, local links, HEAD532d766 and empty index.
Only existing smoltcp22/embedded provenance142 warning headers remain. No
oversized shrink/ratchet edit, commit/push or external action occurred.

Same Heisenberg reviewed code, inverse/fingerprints, manifests and relevant
logs, without another Cargo run: P0/P1/P2=0/0/0, Approve for this repair;
Architecture/Clean/System/DDIA9/9/9/9, necessary local optimization none.
Finish-error preservation is source-proved, not newly runtime-traced. Stronger
end-to-end confidence still requires the separate publisher-fatal propagation,
generation/reuse, finish failure, inventory and proven settlement tracers.
Those and full P3/CaseC/R7/Task4 remain open. The STATUS current-action section
owns the next tracer sequence; this local repair is not full fatal acceptance.

Final same-Reviewer six-document status closure: P0/P1/P2=0/0/0, Approve;
16 Rust/governance fingerprints,22 paths/links, HEAD/index and whitespace stay
verified. Mencius's separate bounded evidence audit also passed, confirming
the 72-line test append, exact RED/counts and warning/proof boundaries without
rerunning tests. Neither receipt expands implementation or full-P3 acceptance.

### Remaining-Full registration after UnrelatedLane — independently accepted

Frozen scope: append exactly one test to the existing private ready_dispatch
leaf, preserving its first333 lines. No production, other test, governance,
interface or dependency changes. The already accepted implementation was
expected to directly GREEN; no artificial failing production change was made.
Possible failures were missing re-registration, a lost subsequent Control
wake, or invalid fixture setup. Pre-write standalone governance passed6/6.

`unrelated_lane_rearm_registers_remaining_full_before_root_parks` mints three
monotonic real flow requests within F4. Ancillary OPEN success and seven valid
equal-frontier ACK0 receipts warm the real inbox to its Ordered turn. Every ACK
is reduced and its session/flow/generation/direction/frontiers/zero release/
nonfinal state checked. The sole observer is acquired only after this warm-up.
Control is then filled with a second real completion and Ordered with an empty
Close; the root retains the first request's actual Full command.

The exact chain is Waiting → actual root Pending → Ordered dequeue/wake →
UnrelatedLane → new Waiting registration unit → historical Rescan → actual
root Pending → only Control dequeue/wake → Rearmed → MailboxOwned. The original
Full Box identity remains unchanged through the unrelated rearm/registration.
No close, extra Ordered wake, manual publication/rearm or enqueue rescues the
critical interval. Both completion receipts are reduced, then the retained
Close receipt yields its exact half-close capability; consuming it emits the
expected final ACK. This is a reducer direction, not real Target or whole-root
shutdown. A final bounded Rescan/Pending precedes fixture close.

First exact compilation reported six E0599 diagnostics: ByteOffset has no ZERO
constant. Its inspected API is the infallible const constructor new(0). The
only repair substituted those six test expressions; no production or timing
change. The next exact reached runtime and directly passed1/1. This is test-
first characterization with feedback-loop repair, not a runtime bug RED/GREEN.

Final leaf528 (+195), root825/scheduler598/route90/governance699 unchanged.
All existing fifteen other Rust/governance fingerprints and the original test
prefix are frozen by `p3-unrelated-registration-source-audit.json`. Evidence
under `/tmp/mini-vpn-commit-boundary-A1DP9p/`:

- `p3-unrelated-registration-fixture-compile-RED.log`: six E0599, no tests run.
- `p3-ready-dispatch-unrelated_lane_rearm_registers_remaining_full_before_root_parks.log`: exact1/1 runtime GREEN.
- `p3-unrelated-registration-repeat-gate-evidence.json`: ten serial1/1 after format.
- `p3-unrelated-registration-regressions-gate-evidence.json`: root47/mailbox18/TCP68/client1+3.
- `p3-unrelated-registration-closure-gate-evidence.json`: Knife162/repository13,
  fmt/diff/all-feature lib check, standalone-Dwarnings and Cargo governance6/6.

Only existing smoltcp22/embedded provenance142 warning headers remain. No
oversized shrink, inventory edit, commit/push or external gate. Independent
review approved this characterization; no broader readiness/lifecycle/fatal/fairness or full-P3
acceptance is claimed. STATUS owns the next action; publisher-fatal planning
is read-only and has not begun another implementation slice.

Same Heisenberg reviewed the test and six evidence documents together:
P0/P1/P2=0/0/0, Approve. It independently ran the read-only audit and checked
manifests/log excerpts, confirming the old333 lines/other15 Rust fingerprints,
22 paths/links and HEAD/index. It verified actual Pending, sole later Control
progress, Box identity, receipt/capability consumption and the compile-versus-
behavior distinction. No Cargo rerun or source edit. This final factual verdict
transcription and pending→accepted promotion need no repeated review.

Next-stage read-only audit separately found publisher faults remain in their
slot until selection, so a healthy resident may dispatch first. This is the
second scheduler-fatal propagation/ownership seam: structural convergence and
fault-authority design review precede further Rust. Required planning questions
are unique first-fault ownership, publication before any notifying callback,
bounded lock scopes, and fatal-publication versus unit-admission ordering.
Already admitted work may need to finish with exact disposition; no new unit
may be admitted after the fatal boundary. This is a design requirement/freeze,
not an approved implementation or completed runtime proof. STATUS owns the
next action; full P3 and all later nodes remain open.

### Publisher-failure Authority convergence — first runtime tracer accepted

The [bounded design](2026-09-07-knife16-r7-p3-ready-failure-authority-plan.md)
converges first-failure recording and ordinary-unit admission in the existing
scheduler Module. Initial same-Reviewer0/1/0 found quarantine swallowing poison;
the repaired plan requires metadata preflight before resident quarantine or
claim release. Same Heisenberg re-review0/0/0 Approve, scoped Module/Clean/System/
DDIA9/9/9/9. This is design evidence, not all failure branches at runtime.

Stage A moved the existing three dispatch tests and fixtures byte-for-byte
(528 lines) from `tests/ready_scheduler/ready_dispatch.rs` to
`scheduler/tests/ready_dispatch.rs` under server_root. New private index15,
scheduler598→601, old test parent90→89; all other Rust/governance unchanged.
Before/after focused3, full root47, provenance162+13, fmt/diff/check and both
governance6/6 passed. Same Reviewer independently verified hash/routes/scope and
evidence0/0/0 Approve. Full filter is now `server_root::`, not `server_root::tests::`
(the latter omits the moved three). No public getter or fixture copy was added.

Stage B adds one exact test:
`publisher_revision_exhaustion_closes_admission_before_another_resident_send`.
B's real second OPEN fills its mailbox; its original completion gets actual
Full via the existing command unit before root construction. Root [B,A] first
visits B Waiting. Only B's owning source revision is set to MAX, then its real
prefill dequeue executes the installed capacity Waker and actual overflow.
The first runtime RED passed these preconditions but the next root poll sent A
(Visited/Transfer/MailboxOwned), instead of Quarantined. No compiler/fixture RED.

Minimum GREEN installs one private AtomicU8 first-failure record in ReadyShared.
Strong Open→Open admission RMW orders against Open→fault; the existing claim
owns already-admitted work. Publisher failure commits before Notify, and root
checks before unwind classification. Root.terminal and ReadySlot.fault are
deleted; session-local source fault remains separate. Missing/poisoned metadata
quarantine returns failure and retains the root claim instead of using
into_inner. Finish records an admitted unit's actual outcome despite another
slot's fatal event; its own error retains the unfinished claim. Park observes
the gate as well as its existing cached registration/epoch. These additional
ordering/poison/finish branches are source proof here, not dedicated runtime
tracers or full fatal/lifecycle acceptance.

The new test passes exact1 plus ten serial repeats. It also checks subsequent
actual admission rejection with RevisionExhausted, both concrete unsent events,
B's identical Full Box, vacant Ordered cells, empty inboxes, no resident-only
normalization and the consumed real prefill receipt. It claims no clean root.
Final root825/scheduler687/index15/moved leaf530/new child107, governance699.
The root's exact inverse recovers its previous hash, proving existing source
transactions unchanged; old leaf inverse recovers all528 original lines.

Evidence under `/tmp/mini-vpn-commit-boundary-A1DP9p/`:

- `p3-publisher-fatal-move-before-evidence.json`: governance6, old leaf3.
- `p3-publisher-fatal-move-evidence.json` and `-move-closure-evidence.json`:
  moved3, root47 and complete scoped closure gates.
- `p3-publisher-fatal-source-revision-RED-focused-1.log`: real unexpected A send,
  exact0/1 exit101; the runner's stop exception is not another code failure.
- `p3-publisher-fatal-source-revision-GREEN-evidence.json` and
  `-source-revision-repeat-evidence.json`: exact1, ten serial1/1.
- `p3-publisher-fatal-source-revision-regressions-evidence.json`: root48,
  mailbox18, TCP68, client1+3.
- `p3-publisher-fatal-source-revision-closure-evidence.json`: Knife162/repo13,
  fmt/diff/all-features check, standalone-Dwarnings and Cargo governance6/6.
- `p3-publisher-fatal-behavior-source-audit.json` and audit script: allowed scope,
  exact root/old-leaf inverses, other Rust/inventory frozen, HEAD532d766/index.

First behavior review returned0/1/0: quarantine checked the gate before taking
the slot lock, so a same-slot publisher could commit fatal while it waited.
The repair moves only that check after successful lock acquisition, before
mutation. All source line counts remain unchanged; exact inverse verifies the
one-expression relocation and every other Rust fingerprint. A bounded
`p3-quarantine-lock-order-check.mjs` first failed (check before lock) then
passed; `p3-quarantine-lock-order-RED.log`/`-GREEN.log` are source-order evidence,
not a runtime interleaving or a new runtime RED. The actual first publisher
RED remains the earlier B-fatal/A-send test.

Post-repair `p3-publisher-fatal-lock-repair-repeat-evidence.json`,
`-lock-repair-regressions-evidence.json` and `-lock-repair-closure-evidence.json`
repeat the ten focused tests and all gates above with identical passing counts.
`p3-publisher-fatal-pre-lock-repair-source-audit.json` preserves the prior
snapshot; the current behavior-source audit records the proved relocation.
Same Heisenberg code/document re-review closed the P1: P0/P1/P2 0/0/0,
Approve for this first publisher branch and repair; scoped Module/Clean/System/
DDIA9/9/9/9. It independently checked the one-check relocation inverse, all17
other Rust fingerprints,25 links and post-repair evidence records; no new
Cargo/test run. It permits this factual pending→accepted transcription without
another review. `p3-publisher-fatal-final-closure-audit.json` binds the final
source/document snapshot. All gates pass. At that checkpoint the remaining tests were
separate process-overflow, poison/quarantine, first-cause, admitted-work/Notify
unwind and park tracers from the frozen plan. Full P3, Target, Driver binding,
physical capacity, lifecycle/fairness, P4, Case C and R7 remain open. No debt
ratchet, commit/push or external operation occurred.

### Process revision publication — characterization accepted

The next frozen change appends one test to the same private publisher_fatal
leaf; its original107 lines and all17 other Rust/governance fingerprints remain
unchanged. Pre-write standalone-Dwarnings governance6/6 and the original source
revision test1/1 passed. No production correction or artificial RED was needed.

`process_revision_exhaustion_is_published_before_root_selection` uses real
authenticated models, completions, mailboxes and the existing Full/Waiting
path. It changes only the process revision to MAX. The actual prefill dequeue
then successfully publishes the source (revision1→2, Published, local bit1)
before process publication exhausts (MAX, Quarantined). The root gate reports
exact RevisionExhausted before any new root selection or claim. Eight root
polls and another admission remain stopped, A stays unsent, B keeps its exact
Full Box, both command events/origins remain intact, and no resident-only
quarantine is fabricated. Real inboxes remain empty; the actual prefill receipt
is reduced before fixture close. This is not whole-root settlement.

The exact test directly GREENed and then passed ten serial repeats. Final
leaf215 (+108); root825/scheduler687/index15/parent530/governance699 are unchanged.
Evidence under `/tmp/mini-vpn-commit-boundary-A1DP9p/`:

- `p3-process-revision-before-evidence.json`: baseline1 and governance6.
- `p3-process-revision-exact-evidence.json` / `-repeat-evidence.json`: exact1,
  ten serial1/1; no compiler or runtime failure in this characterization.
- `p3-process-revision-regressions-evidence.json`: root49/mailbox18/TCP68/client1+3.
- `p3-process-revision-closure-evidence.json`: Knife162/repo13, fmt/diff/
  all-features lib check, standalone-Dwarnings and Cargo governance6/6.
- `p3-process-revision-source-audit.json`: single test append, old107-line
  prefix, other17 Rust/governance fingerprints,25 paths, HEAD532d766/empty index.

Warning headers match the preceding accepted slice exactly: provenance166 and
lib check23, no added or removed header. Only standalone governance uses
`-D warnings`; the library is not claimed warning-free. Same Heisenberg test and
five-document review: P0/P1/P2 0/0/0, Approve for this characterization;
Module/Clean/System/DDIA9/9/9/9. It verified source/aggregate observations,
exact prefix/other17 fingerprints, real custody/receipt and all gate records;
no new test/Cargo run or source edit. Factual acceptance transcription needs
no further review. `p3-process-revision-final-closure-audit.json` binds the
promoted snapshot. Poison/quarantine, first-cause competition, admitted
work/Notify unwind, park, full ready lifecycle/fairness, Target and other full-P3
items remain open. Before a third long publisher fixture, freeze a private
test-only extraction; do not add production hooks/getters or copy another
command setup/ownership-assertion body. No inventory edit, commit/push or
external action occurred.

### Private publisher fixture extraction — independently accepted

Heisenberg approved the single-leaf two-step Extract Method plan0/0/0,
structure9/10. Only `publisher_fatal.rs` changed215→209. Private
`root_with_full_b` owns the unchanged Full→Box identity→checked root construct;
`assert_open_completion_identity` only borrows the same packet. Models, inboxes,
first Waiting, each branch's distinct gate/claim/poll ordering and real receipt/
close/drop ordering stay in the callers. No fixture struct, production hook,
visibility change or new tracer was added. Assertion locations/backtraces may
change; diagnostic output is not claimed byte-identical.

Evidence under `/tmp/mini-vpn-commit-boundary-A1DP9p/`:

- `p3-publisher-fixture-before-evidence.json`: standalone6 and both exact1.
- `p3-publisher-fixture-construct-evidence.json` and `-identity-evidence.json`:
  both unchanged test identities pass1/1 after each separate extraction.
- `p3-publisher-fixture-source-audit.json`: exact forward transformation and
  inverse whole-leaf SHA `79e136…ff`, all17 other Rust/governance hashes frozen.
- `p3-publisher-fatal-fixture-regressions-evidence.json`: root49/mailbox18/TCP68/
  client1+3; `-fixture-closure-evidence.json`: Knife162/repo13, fmt/diff/check,
  standalone-Dwarnings6 and Cargo all-features governance6.

Same Heisenberg closure: P0/P1/P2 0/0/0, Approve, structure9/10. It independently
confirmed both source proofs, custody/order, all gate records and five docs;
25 links and warning headers166/23 are unchanged. Factual acceptance transcription
needs no repeat review. `p3-publisher-fixture-final-closure-audit.json` binds it.
No oversized shrink/inventory edit, production change, poison tracer, P3
commit/push or external action occurred. Full P3 and all previously listed
remaining fault/lifecycle/Target obligations stay open.

### Actual slot-poison publication — characterization accepted

The frozen single test `slot_poison_is_published_before_root_selection` appends
79 lines to the private leaf209→288. It uses the accepted two helpers and real
B Full/A Ready root. After B's actual Waiting registration, an intentional
panic while holding B's real slot guard poisons the mutex. The exact panic is
caught and the root gate must still be Open: no publisher has run yet. Only
the real prefill dequeue/capacity callback then publishes exact Poisoned,
observed before root selection. Eight polls and another A claim are denied;
B retains its original Box, both exact commands remain Held, inboxes stay empty,
and the actual prefill receipt is reduced before close. No clear_poison,
synthetic notification, local-only quarantine or clean-zero claim is used.

The test directly GREENed, without production change or artificial RED.
Artifacts under `/tmp/mini-vpn-commit-boundary-A1DP9p/`:

- `p3-slot-poison-before-evidence.json`: standalone6 and prior leaf2.
- `p3-slot-poison-exact-evidence.json` / `-repeat-evidence.json`: exact1+ten1/1.
- `p3-slot-poison-regressions-evidence.json`: root50/mailbox18/TCP68/client1+3.
- `p3-slot-poison-closure-evidence.json`: Knife162/repo13, fmt/diff/check,
  standalone-Dwarnings6 and Cargo all-features6; warning headers166/23 unchanged.
- `p3-slot-poison-source-audit.json`: original209-line prefix and all17 other
  Rust/governance fingerprints unchanged, same25 paths, HEAD532d766/empty index.

Same Heisenberg closure: P0/P1/P2 0/0/0, Approve, Module/Clean/System/DDIA9/9/9/9.
It independently confirmed the two causal moments, guard/custody/receipt,
prefix/other17 hashes,25 links and gate records; no Cargo/test run or source edit.
Factual acceptance transcription needs no re-review;
`p3-slot-poison-final-closure-audit.json` binds the promoted snapshot.
This covers publisher lock poison, not the
distinct poisoned-metadata source-unwind quarantine branch, admitted-work or
Notify unwind, first-cause, park, full ready lifecycle/fairness or Target.
No production/inventory/error-log edit, P3 commit/push or external action occurred.

### Poisoned source-unwind recovery — characterization accepted

Mencius's bounded read-only freeze identified a test-only causal route. The
private `PoisonOnFirstReceiveWake` holds only Weak ready metadata and a call
counter. A real A send enqueues before its registered receiver Waker obtains
slot0 with try_lock and panics; no guard is held by the production send. The
outer test catches the exact panic and verifies one wake, actual poison,
Running claim and Unknown source while the root gate is still Open.

`poisoned_metadata_source_unwind_retains_claim_and_mailbox_disposition` then
drives actual root source-unwind recovery. The poisoned quarantine lock commits
Poisoned rather than retiring local metadata. Eight recovery/re-entry polls
retain the same claim address, key/lane/revisions, source/process RunningClean,
local bit/mask/cursor and B's exact unsent command. Test-only poisoned-guard
readback is immutable; no poison is cleared. B admission remains denied.
Only afterward is A's actual queued completion received and reduced once;
empty inbox polls replace cached receiver Wakers, and the trigger has one
remaining Arc before explicit Drop. Unknown remains correlation, not a second
payload owner or Vacant. This does not turn ordinary source Unknown root-fatal;
the genuine metadata poison discovered during recovery is the cause.

Direct GREEN with no production repair. Leaf288→446; original288-line prefix
and all17 other Rust/governance fingerprints remain exact. Evidence under
`/tmp/mini-vpn-commit-boundary-A1DP9p/`:

- `p3-quarantine-poison-before-evidence.json`: standalone6/prior leaf3.
- `p3-quarantine-poison-exact-evidence.json` / `-repeat-evidence.json`: exact1+ten1/1.
- `p3-quarantine-poison-regressions-evidence.json`: root51/mailbox18/TCP68/client1+3.
- `p3-quarantine-poison-closure-evidence.json`: Knife162/repo13, fmt/diff/check,
  standalone-Dwarnings6/Cargo all-features6; warnings166/23 unchanged.
- `p3-quarantine-poison-source-audit.json`: single-leaf append,25 paths,
  other17 Rust/governance unchanged, HEAD532d766/empty index.

Same Heisenberg closure: P0/P1/P2 0/0/0, Approve, Module/Clean/System/DDIA9/9/9/9.
It independently confirmed the recovery-only cause, claim/metadata/physical
disposition, readback/teardown and all hashes/evidence/docs. Factual acceptance
transcription needs no re-review; `p3-quarantine-poison-final-closure-audit.json`
binds the promoted snapshot. Ordered first-cause retention is next; concurrent
interleaving, admitted-work/Notify unwind, park, full ready lifecycle/fairness,
Target and full P3 remain open.
No production/inventory/error-log edit, P3 commit/push or external action occurred.

### Ordered first-cause retention — characterization accepted

Mencius froze the distinct single test
`first_published_poison_survives_later_process_revision_exhaustion`. Both genuine
mailboxes have OPEN2 prefills, both OPEN1 completions actually return Full, and
the root installs B then A Waiting registrations. A's process revision alone
is set to MAX; B's real mutex is poisoned by a caught guard unwind. Gate remains
Open until actual B dequeue publishes Poisoned. Actual A dequeue then advances
source revision1→2/Published and process MAX/Idle→Quarantined, local bit0→1;
this independently proves the second publisher executed despite closed admission.
The first root cause stays Poisoned, not RevisionExhausted. No root selection,
claim, close or synthetic wake occurs between these two actual publications.

Eight later polls remain Quarantined. Both original Full Box pointers, exact
command origins/events and CapacityBlocked dispositions survive; no resident
fault is fabricated, Ordered cells stay Vacant and in-flight remains Idle.
Both actual prefill receipts are reduced against their own models before close.
No new helper, callback, production hook or visibility was required. This is
ordered two-fault evidence, not a concurrent-interleaving claim.

Direct GREEN; leaf446→575, prior446-line prefix and all17 other Rust/governance
hashes unchanged. Under `/tmp/mini-vpn-commit-boundary-A1DP9p/`:

- `p3-first-cause-before-evidence.json`: standalone6/prior leaf4.
- `p3-first-cause-exact-evidence.json` / `-repeat-evidence.json`: exact1+ten1/1.
- `p3-first-cause-regressions-evidence.json`: root52/mailbox18/TCP68/client1+3.
- `p3-first-cause-closure-evidence.json`: Knife162/repo13, fmt/diff/check,
  standalone-Dwarnings6/Cargo all-features6; warning headers166/23 unchanged.
- `p3-first-cause-source-audit.json`: same25 paths, prior prefix and other17
  fingerprints, HEAD532d766/empty index.

Same Heisenberg closure: P0/P1/P2 0/0/0, Approve, Module/Clean/System/DDIA9/9/9/9.
It confirmed second-publisher execution, first-cause/dual-Box/receipt retention,
prefix/other17 hashes,25 links and gate records. No necessary optimization was
identified; no tests/Cargo were rerun by review. Factual acceptance transcription
needs no re-review; `p3-first-cause-final-closure-audit.json` binds this snapshot.
The next read-only freeze reuses HandoffState/DequeueDuringSend unchanged for
`publisher_fault_during_send_preserves_admitted_completion`, with only one new
test proposed and the800-line cap preserved. No implementation has begun.
Concurrent interleaving, admitted-work/Notify unwind, park, full ready
lifecycle/fairness, Target and full P3 remain open.
No production/inventory/error-log edit, P3 commit/push or external action occurred.

### Admitted completion during publisher failure — characterization accepted

The frozen single test `publisher_fault_during_send_preserves_admitted_completion`
reuses session/open/root_with_full_b plus the unchanged ancestor
HandoffState/DequeueDuringSend. Real B Full first reaches root Waiting; its source
revision alone is set to MAX. A's actual receiver registration then invokes that
callback inside A's admitted send: A enqueue→B prefill dequeue→B publisher
RevisionExhausted→normal callback return→A send and finish. There is no panic,
catch_unwind, added helper, callback, production hook or visibility change.

Before draining either receipt, the test requires A/Control/MailboxOwned and
the committed root fault, callback calls1/receiptSome and actual B overflow.
A is Vacant, the root in-flight claim is Idle, A source/process are Idle with
unchanged key/revisions and zero local/mask bits. Read-only first_at verifies
A aggregate cleared while B's fault remains indexed. Eight later polls stop;
B's same Full Box, exact command and CapacityBlocked disposition remain, with
no fabricated resident-only quarantine. Both actual receipts are reduced once.
Only then noop polling replaces A's cached Wakers, Arc::try_unwrap and
Mutex::into_inner recover the fixture, and empty inboxes close outside guards.
This proves synchronous mid-unit completion, not Notify unwind or concurrency.

Direct GREEN, then ten serial repeats. Leaf575→743; original575-line prefix,
all17 other Rust/governance hashes and existing helpers remain unchanged.
Evidence under `/tmp/mini-vpn-commit-boundary-A1DP9p/`:

- `p3-admitted-send-before-evidence.json`: standalone6/prior leaf5.
- `p3-admitted-send-exact-evidence.json` / `-repeat-evidence.json`: exact1+ten1/1.
- `p3-admitted-send-regressions-evidence.json`: root53/mailbox18/TCP68/client1+3.
- `p3-admitted-send-closure-evidence.json`: Knife162/repo13, fmt/diff/check,
  standalone-Dwarnings6/Cargo all-features6; warning headers166/23 unchanged.
- `p3-admitted-send-source-audit.json`: single test append, same25 paths,
  prefix/other17 fingerprints, HEAD532d766/empty index.

Same Heisenberg closure: P0/P1/P2 0/0/0, Approve, Module/Clean/System/DDIA9/9/9/9.
It confirmed the admitted send/finish, full accounting, retained B custody and
post-proof receipt/fixture cleanup, all fingerprints/links/gate records. No
necessary optimization was identified and no tests/Cargo were rerun by review.
Factual acceptance transcription needs no re-review;
`p3-admitted-send-final-closure-audit.json` binds the promoted snapshot.
Notify/Waker unwind, park/fatal ordering,
source-unknown isolation, concurrency, full ready lifecycle/fairness, Target
and full P3 remain open. No production/inventory/error-log edit, P3 commit/push
or external action occurred.

### Fault-before-Notify unwind — characterization accepted

Same Heisenberg approved the bounded two-file placement before writing, at
P0/P1/P2 0/0/0; this was plan approval, not runtime acceptance. The parent adds
only private unconditional default-path `mod notify_unwind;` plus a separator,
743→745 lines. Removing those two lines restores the exact prior parent. The
new175-line child owns compact docs, one Weak/counter Waker and the single test
`notify_unwind::committed_publisher_fault_survives_root_notify_unwind`. No old
test moved, no sibling helper opened and no production Interface changed.

Actual B Full reaches Waiting and A sends normally; both real receipts remain
undrained. Only B source revision is set to MAX. The same root then polls to real
Pending with the custom Waker, gate Open and count0. Actual B dequeue stores its
prefetch before publishing capacity. Inside the root Notify callback, the fixture
observes RevisionExhausted, successfully try-locks the original slot, checks
source MAX/Quarantined, drops the guard, then raises the unique literal panic.
The outer catch surrounds only that real receive; exact payload/count1 and
unpoisoned metadata distinguish the intended unwind from an assertion failure.

Eight ordinary polls reuse the same root/context, with no separate registration
reset: all return Quarantined, preserve B's same Full Box/exact command and Idle
in-flight, and do not fabricate resident-local faults. Only then does ordinary
receive recover B's actual prefetched event once; a second receive is Pending.
A's actual event is also received/reduced once. Both empty inboxes close after
the proof; count remains1, gate remains faulted and metadata stays unpoisoned.
This is one-waiter, living-root evidence, not multiwaiter fan-out, concurrent
admission, root destruction, full park correctness or clean whole-root zero.

Direct GREEN, no repair. Evidence under
`/tmp/mini-vpn-commit-boundary-A1DP9p/`:

- `p3-notify-unwind-before-evidence.json`: standalone6/prior publisher subtree6.
- `p3-notify-unwind-exact-evidence.json` / `-repeat-evidence.json`: exact1+ten1/1.
- `p3-notify-unwind-regressions-evidence.json`: root54/mailbox18/TCP68/client1+3.
- `p3-notify-unwind-closure-evidence.json`: Knife162/repo13, fmt/diff/check,
  standalone-Dwarnings6/Cargo all-features6. Warning headers166/23 are identical
  to admitted-send closure, not a warning-free library claim.
- `p3-notify-unwind-source-audit.json`: parent route/inverse, single new child,
  all17 other Rust/governance fingerprints frozen,26 dirty paths/19 Rust-governance
  sources,25 local Markdown targets, HEAD532d766/empty index.

Same Heisenberg runtime/five-document closure: P0/P1/P2 0/0/0, Approve;
Module/Clean/System/DDIA9/9/9/9, no necessary optimization. It independently
checked the causal chain, parent inverse, other17 fingerprints, ERRORS, links
and gate evidence; no tests/Cargo rerun. Factual acceptance transcription needs
no re-review. `p3-notify-unwind-docs-audit.json` and
`p3-notify-unwind-final-closure-audit.json` bind the promoted snapshot. ERRORS
remains frozen because no unexpected compiler/test failure occurred. The same
Reviewer separately approved the bounded two-file healthy source-unwind test
plan at0/0/0, not its implementation. Park/fatal ordering, concurrency, full
ready lifecycle/fairness, Target and full P3 remain open. No production/inventory
edit, P3 commit/push or external action occurred.

### Healthy source-unknown isolation — characterization accepted

Mencius froze the source-local negative control, then the same Heisenberg
approved this two-file test-only plan at P0/P1/P2 0/0/0. Parent745→746 adds only
private unconditional default-path `mod source_unknown;`; its deletion restores
the accepted parent exactly. New `source_unknown.rs`255 owns compact docs,
one Weak/counter receiver Waker and the single test
`healthy_source_unwind_quarantines_only_its_resident`. No old implementation
moved; no helper visibility, metadata injection or production change occurred.

Two distinct real sessions each contribute one OPEN completion to empty1/1
mailboxes in [A,B] order. A's actual recv_next polls Pending. The first ordinary
root send publishes Unknown then enqueues; its real receiver Waker observes gate
Open and try-locks RunningClean source/process metadata. It releases that guard
before its unique literal panic, leaving both slot mutexes healthy. The catch
surrounds only the root poll and requires exact panic/count1.

Before recovery or any receipt drain, the test borrows the actual Running claim
and checks A's key/revisions/lane, Unknown correlation, both resident fault flags,
and B's exact Fresh/Held/Ready command plus empty inbox. One ordinary recovery
poll returns Quarantined while the gate stays Open: the claim becomes Idle,
only A receives local Corrupt/quarantine, its active metadata becomes Quarantined
without altering Dormant cells or correlation/revisions, local/mask clear, and
the aggregate now names only B. B remains unchanged. The next ordinary root poll
must actually return B/Control/MailboxOwned, finish its metadata Idle and clear
the aggregate. A's Unknown and local fault remain; no synthetic marker is used
as proof of B progress.

Only after that isolation proof are the two real receipts reduced once against
their matching models. Second receives are Pending. A remains Unknown/local-Q
and the root remains Open; noop empty polls replace the cached receiver Wakers
before external Waker Drop and fixture Arc count1/Drop. Both inboxes explicitly
close outside guards; metadata remains healthy. This retires a local scheduling
claim, not the unresolved sending authority; no whole-root zero, destruction,
park or full P3 claim follows.

Direct GREEN without a repair. Evidence under
`/tmp/mini-vpn-commit-boundary-A1DP9p/`:

- `p3-source-unknown-before-evidence.json`: standalone6/prior publisher subtree7.
- `p3-source-unknown-exact-evidence.json` / `-repeat-evidence.json`: exact1+ten1/1.
- `p3-source-unknown-regressions-evidence.json`: root55/mailbox18/TCP68/client1+3.
- `p3-source-unknown-closure-evidence.json`: Knife162/repo13, fmt/diff/check,
  standalone-Dwarnings6/Cargo all-features6; warning headers166/23 identical to
  Notify-unwind closure, not a warning-free library claim.
- `p3-source-unknown-source-audit.json`: parent inverse, single new child,
  all18 other Rust/governance fingerprints frozen,27 dirty paths/20 Rust-governance
  sources,25 local Markdown targets, HEAD532d766/empty index.

Same Heisenberg source/five-document closure: P0/P1/P2 0/0/0, Approve;
Module/Clean/System/DDIA9/9/9/9, no necessary optimization. It independently
checked actual A unwind/local recovery and B send/finish-before-receipts, parent
inverse, other18 fingerprints, ERRORS, links and gate evidence; no tests/Cargo
rerun. Factual acceptance transcription needs no re-review.
`p3-source-unknown-docs-audit.json` and `p3-source-unknown-final-closure-audit.json`
bind the promoted snapshot. ERRORS is frozen because there was no unexpected
test/compiler failure. Park/fatal ordering is the next read-only seam freeze,
separate from full park/lifecycle/fairness, concurrency, Target and full P3.
No production/inventory edit, P3 commit/push or external action occurred.

### Fatal during cached-Waker replacement — characterization accepted

Mencius identified the existing safe re-park seam; Heisenberg approved the
bounded two-file plan at0/0/0, scoped Module/Clean/System/DDIA9/9/9/9. Local
Tokio1.52 `notify.rs:1299–1309` replaces the Waker, unlocks, drops the old Waker,
then returns Pending. In the current root this occurs after `park_epoch` and
before the final park recheck. No RawWaker, unsafe or production hook is needed.
Parent746→747 adds only private unconditional default-path `mod park_fatal;`;
deleting that line restores the accepted parent. New217-line child owns the
single test `fatal_during_cached_waker_replacement_prevents_repark`, compact docs,
one real dequeue-on-Drop fixture and a pure-count replacement Wake. Existing
HandoffState owns B's inbox and the sole receipt location, not another root.

Actual [B Full,A Ready] proceeds B Waiting then A MailboxOwned; only B's source
revision is set to MAX. Actual root Pending installs the old Waker. Dropping all
external old references leaves Drop count0/oldWake0/receiptNone/gateOpen and an
even epoch. The next ordinary poll, with a distinct counting Waker, samples
park_epoch then replaces the cache: old Waker Drop actually receives B's prefill,
publishes RevisionExhausted/aggregate/dirty epoch and wakes the new Waker. The
replacement Wake does not reenter the fixture mutex. The same poll must return
exact Rescan, with Drop1/oldWake0/newWake1 and the actual receipt retained.

Eight later ordinary polls remain Quarantined, B's original Full Box and exact
CapacityBlocked command stay resident, A stays Vacant with its actual command
undrained, in-flight is Idle and no resident-local fault is fabricated. A direct
call to the existing park_epoch primitive after fault must return that fault
without changing the actual odd epoch; this is primitive refusal evidence,
not a claim that root entry reaches this method after fault. Only afterward are
the B prefill/A real receipts reduced once; Arc::try_unwrap/Mutex::into_inner
recover the sole handoff, second receives are Pending and inboxes close outside
guards. B is still retained, so neither fixture Drop nor drained receipts imply
whole-root settlement.

This is one deterministic re-registration window. Nonempty aggregate can
short-circuit final epoch/gate checks, so it does not independently prove those
predicates, first registration or epoch CAS concurrency. Returning Pending in
this branch would violate the final recheck contract; by itself that result
would not prove a permanently lost wake. Full P3 remains incomplete.

Direct GREEN with no repair. Evidence under
`/tmp/mini-vpn-commit-boundary-A1DP9p/`:

- `p3-park-entry-revalidation.json`: all27 accepted source/docs hashes unchanged.
- `p3-park-fatal-before-evidence.json`: standalone6/prior publisher subtree8.
- `p3-park-fatal-exact-evidence.json` / `-repeat-evidence.json`: exact1+ten1/1.
- `p3-park-fatal-regressions-evidence.json`: root56/mailbox18/TCP68/client1+3.
- `p3-park-fatal-closure-evidence.json`: Knife162/repo13, fmt/diff/check,
  standalone-Dwarnings6/Cargo all-features6; warning headers166/23 identical to
  source-unknown closure, not a warning-free library claim.
- `p3-park-fatal-source-audit.json`: parent inverse, single new child, all19 other
  Rust/governance fingerprints frozen,28 paths/21 Rust-governance sources,
  25 local Markdown targets, HEAD532d766/empty index.

Same Heisenberg implementation/five-document closure: P0/P1/P2 0/0/0, Approve;
Module/Clean/System/DDIA9/9/9/9, no necessary optimization. It independently
checked the actual replacement Drop/publication/Rescan, custody and receipt
sequence, parent inverse, other19 fingerprints, ERRORS, links and gate evidence;
no tests/Cargo rerun. Factual acceptance transcription needs no re-review.
`p3-park-fatal-docs-audit.json` and `p3-park-fatal-final-closure-audit.json` bind
this snapshot. ERRORS stays frozen because no unexpected test/compiler failure
occurred. A separate read-only full-P3 alignment audit is selecting the next
substantive tranche, not waiving unproved park/lifecycle/fairness, physical
capacity, Driver binding or Target obligations. No production/inventory edit,
P3 commit/push or external action occurred.

### Real Owner read preparation — pure Move accepted

The full-P3 alignment audit selected actual Executor source service, retaining
the existing Controller/output owning chain. Mencius and Heisenberg agree that
per-session two-cell custody can be transitively root-owned under amendment
sections2/8; this is not approval of a concrete binding Interface. Repeated
borrows, matching SessionId or an exclusive mailbox observer cannot establish
persistent Owner association. No bare Executor resident, extra pair per flow,
reverse Main dependency or second continuation/failure Authority is permitted.

The [two-file preparation freeze](2026-09-07-knife16-r7-p3-owner-source-move-plan.md)
was approved by the same Heisenberg at P0/P1/P2 0/0/0. Existing methods
`try_read_target` and `try_read_target_observed` moved from owner_target.rs to
its private unconditional default-path read_source child. Both pub(crate)
signatures and all bodies are identical except exactly four equivalent
TargetReadCompletion path rebasings. Parent storage, shared RESET/failure and
completion helpers, observation mint/consume, tests and all callers are unchanged.
This moves the complete existing read transaction, not the entire future
source/RESET settlement Implementation. It introduces no new behavior or RED.

The146-line block inverse SHA is
`367f818e14f2339aa31bf8bf95c5a8d4c1cd2d5e5838ff4bdab4452c11145445`.
Removing one doubled separator is the exact format-only correction predicted
by both read-only audits. Parent11110→10965 has SHA
`801a88613f8c0bb92653bfdeb532769e77d5ae159895e226756630666b3fc3a5`;
child164 has SHA
`5fd5d5848021781b0fd27f2ac663deb1df3181e154a78359191b437cd87db832`.
Whole-parent inverse restores `6b7bbb53996dac210d902224857f9d45772ad326dbcd723f50d20a92d1bf1510`.
The sole expected standalone5/6 stop requested11110→10965. Integrator Mencius
changed only that ratchet, preserving the699-line governance file and all prior
dirty ratchets. Its SHA is
`d89615dead665686e3c3f3092a3e3126a735ac2498b0f4114a32c4ef22713525`;
inverse replacement matches the immutable entry manifest exactly.

Exact command inventories/logs reside under
`/tmp/mini-vpn-commit-boundary-A1DP9p/`:

- `p3-owner-source-move-entry.json`: immutable original/block and30 entry
  fingerprints, including the previously clean oversized parent.
- `p3-owner-source-move-test-inventory.json`: actual pre-Move Cargo listing.
- `p3-owner-source-move-before-evidence.json`: standalone6; three exact1/1;
  Owner74, Controller44 and continuation16.
- `p3-owner-source-move-after-evidence.json`: the same exact3 plus Owner74,
  Controller44, continuation16, root56, mailbox18, TCP68 and client3.
- `p3-owner-source-move-ratchet-evidence.json`: expected sole shrink failure,
  not a behavior regression. Integrator's fresh standalone/Cargo6+6 logs are
  under `p3-owner-source-move-integrator-KD6YEH/`.
- `p3-owner-source-move-closure-evidence.json`: Knife162/repository13,
  fmt/diff/check, standalone-Dwarnings6 and Cargo all-features6.
  Warning headers166/23 match the prior packet, not warning-free-library proof.
- `p3-owner-source-move-audit.json`:31 scoped paths, exact inverse blocks,
  definitions only in child,20 other Rust fingerprints unchanged; HEAD532d766
  and empty index. Docs/final audits have separate names.

During integration, comparing the reversed governance file against the mutable
current audit initially reported false. The immutable entry hash proves the
single authorized change; no source repair or additional test was needed for
that diagnostic. Preserve entry/current/closure manifests as distinct artifacts.
An earlier multi-file patch request failed context verification before any
write; correcting the temporary audit-script patch context changed no Rust.

All planned local gates are green. Same Heisenberg implementation/document
closure: P0/P1/P2 0/0/0, Approve; Module/Clean/System/DDIA9/9/9/9, no necessary
optimization. Reviewer independently checked source/hash/scope, docs and gate
logs, without rerunning tests/Cargo or writing. Factual acceptance transcription
needs no re-review. `p3-owner-source-move-docs-audit.json` and the separately
named `p3-owner-source-move-final-audit.json` bind pre/post-transcription snapshots.
Full P3, the actual two-cell association/intake,
remaining readiness/fairness/capacity, Target progress and P4–P7 remain open.
Next freeze must define the actual cold mailbox pair, same-Owner DATA/RESET/
CLOSE service, observed reducer/output retention and shutdown enumeration;
this Move cannot stand in for those behaviors. No P3 commit/push or external run.

### Actual Owner source-service selection — doc-only decision accepted

The next [decision record](2026-09-07-knife16-r7-p3-owner-source-service-decision.md)
selects Controller-centered cold pairing, the moved existing two-cell
Implementation under the actual Executor and the existing complete output
transaction. Three read-only Interface designs compared placement and caller
Depth; none was independent acceptance. Mencius supplied constructor and
source-credit facts. Heisenberg independently confirmed that refinement of
the existing inward TargetIo readiness contract belongs to P3, not P6
whole-root adoption; no concrete Interface or Rust implementation was approved.

Main then found that source-credit readiness is a separate prerequisite.
The actual factory cursor retains identity/epoch, not the borrowed watch wait
registration. Failed source reservation publishes rollback progress and can
self-sustain retries; source FIFO dequeue does not itself publish that factory
edge while actual DATA permits remain owned. ACK/terminal permit return is a
different later edge. A bounded blocked-Flow index alone fixes neither issue;
resetting a baseline can also absorb an intervening real release. Saturating
factory epochs are not checked ready-generation exhaustion evidence.

The decision intentionally does not pretend these are already solved. Next:
freeze causal source-credit/Target readiness, then complete typed cold inputs,
quantum mapping/per-Flow bounds, physical Layout/cold peak and exact symbols.
Only then may a reviewed Move/RED/GREEN packet open Rust. This is implementation
incompleteness, not a rollback of any accepted local test or a P4/P6 expansion.

Before writing, all31 prior accepted fingerprints matched. New immutable
`/tmp/mini-vpn-p3-source-service-v3Owhx/entry.json` covers1143 repository files
including336 Rust sources. `draft.json` proves the first single-file decision
write and unchanged Rust; later doc/final manifests are distinct. Only the
decision, STATUS/HANDOFF/TODO, this result and LEARNINGS are in scope.
No Rust/tests/Cargo/governance/ERRORS/prior accepted architecture edits or
commit/push/external run. First independent decision review returned0/0/1,
Request Changes: explicitly record FIFO-only missing publication, aggregate
baseline races and notification under the admission lock. The single-file
doc repair adds those facts and next-gate obligations, without an algorithm,
Interface or Rust repair. Same Heisenberg decision/document re-review is
P0/P1/P2=0/0/0, Approve; scoped Module/Clean/System/DDIA8/8/6/7. It accepts
only placement and remaining obligations, not full binding or implementation.
Reviewer independently checked336 unchanged Rust sources, six scoped document
fingerprints and26 valid local links. `repair-review.json` binds that packet;
the separate `final.json` records factual promotion. The Reviewer explicitly
allowed this transcription without another review. Credit-readiness is next,
P3 stays unchecked/active, and no new Rust write scope is opened here.

### Owner-local source-credit protocol — protocol and documents accepted

The [credit rearm contract](2026-09-07-knife16-r7-p3-owner-credit-rearm-contract.md)
narrowly replaces the preceding decision's mandatory external credit
registration mechanism. Current actual Owner Flow/Driver/factory and replay
permits remain private under one Executor; Target receives no source ports,
and output Frames carry no source permits. Generic Client/TUN producers do
not have this exclusivity, so their rollback publication must stay unchanged.

The selected protocol observes actual source FIFO dequeue locally, committing
the command before rearm; separately, release-only owning turns compare exact
factory source byte/segment aggregates on both Ok and recoverable Err paths.
A true decrease requests a budgeted blocked-Flow pass; pressure/rollback alone
does not request an Owner retry. The same entered-turn Authority quarantines
unfinalized unwind, rather than disguising a legitimate recoverable release
as fatal. Pass cursor/dirty state persists across borrowed cancellation.
No generic factory observer, new task/provider or per-record full scan is added.

Mencius's independent source audit confirmed current effect/resume/recovery/
retry/cleanup does not acquire fresh source permits when Target-read production
is excluded; DATA binding moves permits. No normal private Owner DATA escape
was found in those current paths. The old combined read and future unbuilt
binding are not covered. That complete entry/guard proof remains a Rust gate.

Same Heisenberg contract review: P0/P1/P2=0/0/0, Approve; scoped Module/Clean/
System/DDIA8/9/7/8, no necessary optimization. This accepts only the conditional
local protocol, not binding/Rust/full P3. Target socket registration, cold-pair
inputs, concrete categories/K, physical Layout/cold peak and causal tracers
remain unclosed. The explicit old-decision amendment and compact docs also
passed same-Reviewer closure0/0/0; no implementation is authorized by this status.

Entry verification matched the previous accepted documents and336 Rust files.
`/tmp/mini-vpn-p3-owner-credit-VsDY1G/entry.json` freezes1144 repository files;
`draft.json` and `awaiting-review.json` bind the first single-file contract
and unchanged baseline. Only this contract, the explicit decision amendment,
STATUS/HANDOFF/TODO, this result and LEARNINGS are allowed to change. No Rust,
tests/Cargo, governance, ERRORS, prior scheduler specification, commit/push or
external run. Final Reviewer independently inverse-matched contract sections1–9,
verified336 unchanged Rust files and HEAD/index, seven scoped document hashes
and34 valid links. `docs-review.json` binds reviewed docs and `final.json`
records factual promotion, explicitly permitted without another review.
Full P3 remains active and unchecked; prior local code stays accepted.

### Target readiness protocol — protocol and documents accepted

The [Target readiness contract](2026-09-07-knife16-r7-p3-target-readiness-contract.md)
deepens the same static TargetIo with demanded per-Flow polling and a bounded
shared-notification operation in OwnerContinuation. It adds no provider,
task/dynamic future or ready lease. Source-credit's accepted local release
protocol is unchanged; Target buffer/tombstone capacity is a distinct domain.

Source evidence changed the next action: Memory's one-shot WouldBlock needs
an actual write to consume it; diagnostic joinable omits Z pressure; external
take_written and several retirement paths lack complete Owner-local rearm.
The current drain_pending_target_joins scans all Flows into a Vec and cannot
remain reachable under a claimed bounded poll. Shared release must publish
bounded work; eventual delivery to F candidates still costs O(F) total turns.

Pinned Tokio1.52.0 preserves separate directional Wakers and clears observed
readiness after WouldBlock. Its cooperative pre-registration branch can
instead defer a Waker in a runtime Vec. The proposed stack-only unconstrained
wrapper is limited to one finite public readiness primitive; root poll/yield
and all existing class/source budgets remain mandatory implementation gates.
No executable registration, allocation or runtime fairness proof is claimed.

Same Heisenberg first review:0/0/1 Request Changes, solely the source anchor
5086 versus actual5202 for drain_pending_target_joins; corrected without a
protocol change. No P0/P1 or other necessary optimization. Scoped Module/Clean/
System/DDIA8/9/7/8. Final protocol and six-document closure passed0/0/0. The
first review deliberately did not accept the concurrent LEARNINGS update;
complete current packet included it explicitly and passed final review.

`/tmp/mini-vpn-p3-target-readiness-ptZukx/entry.json` freezes1145 repository
files, matching prior accepted docs and336 Rust. `primitive-audit.json` binds
six local Tokio sources and identifies the next constructor/Adapter entry
inventory; `review-packet.json` binds the first reviewed draft. Only the new
contract, STATUS/HANDOFF/TODO, LEARNINGS and this result changed. No Rust,
tests/Cargo, governance, ERRORS, old accepted contracts, commit/push or external
run. Full P3 remains active/unchecked. Next is the actual coupled caller,
release guard, generation/backing, K/Layout and complete Move/symbol freeze.

Same Reviewer independently inverse-matched the protocol repair, checked all
six documents/21 local links and unchanged336 Rust/HEAD/index. A later §4
build-variant clarification also passed0/0/0 against docs-review-v2.json:
unconstrained does not bypass optional unstable Linux taskdump trace capture.
Actual resolved-feature/build qualification must exclude or separately close
that deferred-registration path. source-integration.json retains the three
additional source fingerprints and the independent pure-Move preview; neither
is executable evidence. Final factual promotion is recorded in final.json,
explicitly permitted by the Reviewer without another review. No new Rust scope.

### MemoryTarget trait Implementation — pure Move accepted

The [exact Move plan](2026-09-07-knife16-r7-p3-target-move-plan.md) is the
separate preparatory extraction required before changing the oversized Target.
Same Heisenberg plan review0/0/0 approved this two-file Move, not readiness or
binding behavior. Main was sole source Writer; Mencius owned only the ratchet.

The complete `impl TargetIo for MemoryTarget` moved from parent939–1511 to
the private unconditional default-path memory_io child. Its573 lines/22495
bytes retain SHA `1b8ae959c7e1cf96883a9b3f0448e9f9ab6c0c98da9d4123921f673d1d6d4780`.
Parent2017→1445; child582 includes only nine prefix lines (compact docs/import/
separators). Full inverse restores the original parent, not just chosen methods.
All types/storage/trait/constructors/feed/drain/terminalize/helpers/inline tests
remain in parent. No visibility, caller, allocation, output or behavior changes.

The first patch-transport response was truncated before JSON decoding, so no
write occurred. Passing the original block once then assembling the patch
locally resolved that tooling issue; there was no Rust/test RED or second
semantic repair. Exact source hashes were rechecked before the actual patch.

Fresh evidence under `/tmp/mini-vpn-p3-target-move-tA6GgI/`:

| Gate | Evidence | Result |
|---|---|---|
| Entry and original bodies | entry.json, inventory.json |1146 repository files;336 baseline Rust; exact nine Target test identities|
| Pre-Move | before-evidence.json |standalone6; Target9, Owner74, Controller44, continuation16|
| Post-Move | after-evidence.json |same four sets plus root56, mailbox18, TCP68, client-progress3; fmt/diff green|
| Exact shrink promotion | governance-evidence.json |Integrator removed only target2017 production entry; standalone-D warnings compile/6 and Cargo all-features6|
| Remaining gates | closure-evidence.json |Knife16/repository162/13; all-feature cargo check green|
| Complete inverse/scope | audit-implementation.json |parent1445/child582/governance698;334 other baseline Rust unchanged|

Library baseline/post logs each retain23 warning headers; provenance166 and
check23 match the prior scoped build pattern. These include Cargo summary
headers; they are not counts of newly introduced warnings. No warning-free
build or unrelated warning repair is claimed.

Governance SHA is now `1bc9ab7c6cc25348793eb12afed8ac71714f08ede6992d9b0fd00eaf4a7b8013`;
its exact inverse restores the prior699-line file. The target allowance1 and
inline-test allowlist remain; new child has no grandfather entry. No tests or
source-provenance assertions changed. Existing known actor ordered-wait debt
was not included or repaired. No commit/push or external operation occurred.

Independent implementation/document closure passed0/0/0. The prior readiness
contract's line anchors describe its pre-Move source snapshot; use the exact
inverse and this new child route for current code navigation. No prior protocol
is silently revised. Next is remaining inline test Adapter preparation and
the actual coupled caller/guard/generation/K/Layout behavior freeze; full P3
and P4–P7 remain incomplete, with the goal active and the ledger unchecked.

Same Heisenberg independently verified full parent inverse, exact child prefix/
body, unique private route, sole governance deletion,334 frozen baseline Rust,
nine path fingerprints,21 links and HEAD/index. Pure implementation verdict
was0/0/0 Approve, structure9/10 with no necessary code optimization. One
document P2 corrected the plan's historical2017 wording to Pre-Move baseline;
same-Reviewer full-packet closure then passed0/0/0. No independent rerun was
claimed. audit-review-packet.json and audit-doc-repair.json bind those reviews;
audit-final.json records only the explicitly permitted factual promotion.

### Complete Target test Adapter Move — accepted

The [Adapter Move plan](2026-09-07-knife16-r7-p3-target-adapter-move-plan.md)
keeps both fixtures with every caller, including the previously separate
nested-abort test. Same Heisenberg found no source/Authority problem; the sole
plan P2 was the evidence script allowing final to accept pre-Move sources or
an old ratchet. Explicit stage checks repaired it; narrow re-review passed
0/0/0 Approve. Actual negative audit-final calls reject both original sources
and moved sources with old ratchet, without creating a final snapshot.

Main applied only the frozen three-source recipe. Owner parent10965→10206;
existing r7_nested_abort98→726 owns FailOnceTarget plus all seven tests; new
target_overacceptance133 owns OverAcceptingTarget plus its only test. Both
fixtures are private, no helper exports or new Interface. Only one lexical
TargetExpireCompletion path rebase, one indentation level and rustfmt layout
change; complete formatted leaves equal the frozen candidates. Full retained
parent comparison preserves the entire production prefix. The original
nested test body/FQ is exact; only seven parent tests change their FQ prefix.

Artifacts under `/tmp/mini-vpn-p3-target-adapters-yCzYFb/`:

- entry.json:1148 pre-stage repository files/337 Rust; raw source/block hashes,
  exact candidate recipe and HEAD/index baseline. New artifacts never overwrite it.
- before-evidence.json/inventory.json: fresh standalone6, complete library list,
  Owner74/Target9/Controller44/continuation16 serial before-source baselines.
- after-evidence.json/fq.json: two leaves7/1; exact complete library FQ
  set with only seven mapped substitutions; same four baselines plus root56,
  mailbox18/TCP68/client3; fmt/diff green.
- audit-moved.json: three sources exactly match candidates;335 other baseline
  Rust remain unchanged before Integrator ratchet.
- final-rejects-prewrite.json/final-rejects-old-ratchet.json: both forbidden
  final states rejected; neither invocation emitted audit-final.json.

Integrator changed only owner_target10965→10206; governance stays698 lines,
SHA `d9f787156376c4e3e918c173aecb58caa97853eb119dc2a7a3a2c7ede4c83550`.
The exact inverse matches entry's `1bc9ab7c6cc25348793eb12afed8ac71714f08ede6992d9b0fd00eaf4a7b8013`.
governance-evidence.json records fresh standalone-D warnings compile/6 and
offline all-features Cargo6; closure-evidence.json records Knife16/repository
162/13 and all-feature library check. source-proof.json confirms the full
candidate, unchanged production prefix/nested body and334 other baseline Rust.
Library/gov/check logs retain23 warning headers and provenance166, matching
the prior scoped baseline pattern and including Cargo summaries; no claim of
a warning-free build or exact count of new warnings. No new Rust/test RED.

Final same-Reviewer implementation/document closure passed0/0/0 Approve;
audit-review-packet.json binds reviewed source/governance/metadata. No behavior, new tests,
commit/push or external run; full P3 and
Case C/R7 remain incomplete. The next coupled freeze must account for actual
checked Controller callers, cold pair/return authority, release guards, Target
generation/backing and exact service/K/Layout, not just a new trait signature.
Mencius's independent read-only next-cold-caller-audit.md inventories actual
callers and missing inputs, not a design approval. In particular, existing
checked_root_with_pending_recovery is not pristine, and SessionCommands'
from_open_completion is not an empty cold constructor. These must be addressed
explicitly in the next design rather than silently defaulting or bypassing them.

Heisenberg independently checked three approved source candidates,334 other
baseline Rust, exact governance inverse, all11 path fingerprints/33 local
links, HEAD/index and whitespace. Gate results/log excerpts match; no
independent Cargo rerun was claimed. Scoped structure8/10, no necessary
optimization. The only allowed follow-up was factual pending→accepted and
true review evidence transcription, without another review. audit-final.json
binds that final state and requires moved sources plus the new ratchet.

## Actual binding preflight — source audit accepted

The next artifact is
`docs/tech/2026-09-07-knife16-r7-p3-binding-preflight.md`. It is a grounded
preflight, not the completed coupled Interface, a Rust grant or full P3.
Main inspected actual checked construction, command/intake and output guard;
Mencius independently inspected publisher/registration/MemoryTarget lifetime.
Same Heisenberg reviewed the source conclusions and honest stage boundary.

New exact entry `/tmp/mini-vpn-p3-binding-czzlml/entry.json` verifies the last
accepted packet and captures1150 repository files/338 Rust, HEAD/index.
The prior Target test Adapter Move and all its gates remain accepted. This
stage runs no Rust/Cargo/test/format gate and makes no Rust/governance change.

Concrete findings: checked_root_with_pending_recovery is not pristine;
the plan lacks independent mailbox/cold-backing inputs; from_open_completion
is not an empty constructor; both actual mixed intake closures must move;
the existing ordinary output guard is actor-oriented and excludes a retained
page. Existing completion/join full searches cannot hide in bounded turns.
F live positions do not cap old-generation Arc backing; real unregister cannot
retract Tokio's lock-external WakeList references. No current reuse pool exists.

Next work must prove reclamation, retained pressure and release-edge liveness,
then integrate it with cold binding, guard, exact K/Layout and scope. The
owner-ticket candidate's audit supports only a conditional one-backing-per-
occupied-slot proof with real uniqueness and no ticket Weak. It found a
separate notification hazard: old Waker Drop occurs under Tokio waiter locks
and WakeList unwind. Direct raw-drop Notify can reenter or double-panic; an
atomic bit alone cannot wake a parked root. No candidate is selected, approved
or implemented; unsafe RawWaker scope is not granted. The next mechanism must
close those contexts, not repeat the same unsafe publication shortcut.
No new runtime failure, commit/push or external run is claimed. Full P3 and
Case C/R7/Task4 remain incomplete; no full-goal completion or impasse declared.

Final source-audit/seven-document review: P0/P1/P2=0/0/0, Approve;
Module/Clean/System/DDIA8/9/7/8. Reviewer checked seven fingerprints/44 links,
all338 unchanged Rust and governance/HEAD/index against audit-candidate.json.
No runtime or Cargo rerun was claimed. Only factual source-audit acceptance
and real review evidence transcription was permitted without another review;
audit-final.json binds that promotion. Coupled binding is still NOT FROZEN,
no Rust scope is granted, full P3 is incomplete. Safe destructor-context wake
is the next design gate, not direct raw-drop Notify or another pure Move.

## Controlled-wake protocol — conditional document closure accepted

The conditionally accepted protocol is
`docs/tech/2026-09-07-knife16-r7-p3-controlled-wake-contract.md`.
Mencius's closed-holder audit found no normal silent final Drop in the
restricted direct TCP wake list when callbacks return normally. It did find
a source-permitted Driver cooperative-Defer/Context-drop counterexample,
conditional on that cache being the last outside reference after retirement.
The actual Retiring implementation does not exist; no runtime RED is claimed.
Main also inspected the relevant primitive bodies and Driver send/commit
admission locks. MemoryTarget readiness remains an implementation obligation.

The accepted conditional direction keeps standard Wake and inert reference-only Drop;
checks real uniqueness after unregister and actual consuming reference release;
extends finite primitive unconstrained/trace prerequisites to bound Owner
receives; and delays local Driver/mailbox outer signalling until Owner exits
internal locks. Its explicit notification fault branch freezes one cached
Root Notify registration: inner list cleanup precedes guard failure recording,
which precedes the remaining outer same-root Waker cleanup. It does not claim
arbitrary user panic never propagates, that foreign/local/Target wake lists are
closed by analogy, or that static-open/session/root holders are already proved.

Artifacts `/tmp/mini-vpn-p3-controlled-wake-86Ev3O/`: entry.json verifies prior
final state and snapshots1151 repository files/338 Rust/HEAD/index;
primitives.json fingerprints12 local Tokio/std sources. The read-only lookup
initially named absent session/progress.rs, corrected by rg --files without
changing any file. No Rust/Cargo/test/fmt/governance or external execution.
Heisenberg's first review returned0/1/0 Request Changes: missing single-waiter
cardinality made the inner/outer cleanup ordering claim invalid. Main repaired
only the protocol document and corrected its actual source symbol anchor.
Same-Reviewer re-review passed0/0/0 Approve, scoped Module/Clean/System/DDIA
8/9/7/8, against audit-repair-anchor.json (contract253 lines at that checkpoint,
six companion docs/338 Rust/governance/HEAD/index unchanged). Factual acceptance/
status/review-evidence promotion was authorized without another review;
audit-final.json binds the seven-document promotion and final fingerprints.
The next gate remains actual holder/cold/guard/K/Layout/symbol freeze. No Rust
grant, commit/push, coupled binding or full-P3 acceptance is claimed.

## Construction/feedback direction — direction/document evidence accepted

See `docs/tech/2026-09-07-knife16-r7-p3-binding-construction-phase-decision.md`.
Mencius independently traced actual cold callers, shared ready-domain ordering,
exact constructor returns and retained backing. Main checked the relevant
source bodies and proposed the continuation/RESET wait graph; auditor confirmed
no current legal mailbox-completion-under-continuation operation exists.
This is a future-cutover counterexample, not a current runtime deadlock.

The accepted direction keeps the old Copy replay/session plan, adds a same-
domain reservation carrying service limits, prepares metadata before Controller
publication, returns all original inputs on modeled rejection and preserves
valid quiescent live/recovery callers. Flow-local completion custody, not an
input-excluding continuation wait, must own delayed RESET feedback. Full phase/
guard/identity/K/Layout and symbol scope are not frozen; no Rust grant.

Entry `/tmp/mini-vpn-p3-binding-construction-EzJiOD/entry.json` verifies the
previous accepted packet and1152 files/338 Rust/HEAD/index. Draft/packet audits
cover only this decision plus six compact/result/learning documents. Independent
Heisenberg review passed P0/P1/P2=0/0/0, Approve; scoped Module/Clean/System/DDIA
8/9/7/8. He checked seven fingerprints/42 links and338 unchanged Rust/governance/
HEAD/index. He permits factual status/evidence promotion without another review;
audit-final.json binds the promoted packet. Only direction/document evidence
is accepted. Full phase/guard, feedback admission and K/Layout are still the
next joint freeze, not another file split. No Rust/Cargo/tests/fmt/governance,
external run, commit/push or full-goal promotion occurred.

## Flow completion / terminal protocol — accepted, not bound Rust

See `docs/tech/2026-09-07-knife16-r7-p3-flow-completion-contract.md`.
Independent source audit found a second proposed feedback cycle in the old
blanket pending-completion input predicate, read failure with reverse pending
write, non-carrying RetiredFlow after peer RESET, premature Executor record
removal, and bulk Driver terminal drain. These concern the proposed asynchronous
cutover; the current synchronous path is not claimed newly broken.

The protocol puts original work and external feedback under the existing Flow
Authority, defers sink work without a duplicate queue, and uses the actual
FlowFinished/LocalFlow correlation for late command disposition. Owner-only
source revocation preserves queued work for budgeted lane/cell/inbox settlement;
actual in-transit command accounting cannot substitute for physical custody.
Staging DATA release differs from existing Supervisor replay-terminal release.
Cold migration preserves existing carrying completions without sending again.

Same Heisenberg first returned0/1/0 Request Changes: unconditional lane/join
requirements excluded valid failed Open with no retained source or Target.
Main's single-file doc repair distinguishes checked domain absence from actual
drain/join, preserves AlreadyTerminal's Target obligation and adds source-producer
and zero-byte-message premises. Re-review passed0/0/0 Approve, scoped
Module/Clean/System/DDIA8/9/7/8, against audit-repair.json (302-line contract).
Only phase/protocol accepted; carrier/guard/domain/K/Layout/Rust stay unapproved.

Artifacts `/tmp/mini-vpn-p3-flow-completion-cIJRri/`: entry.json verifies the
previous accepted packet and1153 repository files/338 Rust/HEAD/index;
audit-draft.json and audit-repair.json bind the single-file proposal/repair.
Reviewer allowed factual seven-document status/evidence/learning synchronization,
followed by a final consistency review. Source, governance and HEAD/index stay
unchanged; no Rust/Cargo/test/fmt/governance/external or commit/push execution.
The coupled binding freeze remains the next gate, not a full-P3 completion.
Final same-Reviewer seven-document consistency review passed0/0/0 Approve;
audit-packet.json binds the307-line contract,36 links and unchanged338 Rust,
governance/README/HEAD/index. No protocol or authority change was found.
Reviewer permits this final §8/result evidence transcription without another
review; audit-final.json records it. The current-stage five-file status matrix
also passed. The sealed-carrier source audit for the next joint freeze is
artifact-only and does not expand this protocol approval or open Rust scope.

## Closed carrier / owning turns / category selection — protocol accepted

See `docs/tech/2026-09-07-knife16-r7-p3-binding-turn-contract.md`.
The existing carrier is selected under a closed actual-Driver/full-LocalFlow
graph, not variant-label trust or a new Owner Box. The ordinary output,
continuation and deepened service transaction remain one root's Authority.
Thirteen categories include previously omitted output/lifecycle and recovery;
E pages, multiple Target operations and configured F/segment scans are charged
separately from category visits. K/formulas remain conditional on actual ports,
expiry and full work/Layout witnesses, not executable fairness acceptance.

Heisenberg reviewed the289-line proposal at SHA
`eb51db440427842796ff4bc471dc05b047010426bd0f9c074282d6bf56bad7e0`:
P0/P1/P2=0/0/0, Approve selection/protocol only; scoped Module/Clean/System/DDIA
8/9/7/8, no necessary structural optimization. Six links/338 Rust and companion
docs/governance/HEAD/index were independently verified unchanged. Ports/expiry
must precede executable guard/catalogue acceptance; changed charging reopens K.

Mencius's bounded physical-port audit found shared Controller/output authority
but only cfg(test) B-adapter service callers; CurrentOnly has no output/recovery
service or physical lifecycle slots. Real actor/FIFO critical-section ports,
live/stopped lifecycle transaction, A-to-B installation and session expiry
proof/caller must enter the joint freeze; no fictitious parent or P6 claim.
Main checked the named source bodies before recording this next boundary.

Artifacts `/tmp/mini-vpn-p3-turn-contract-tuBbJz/`: entry.json verifies the prior
accepted packet and1154 files/338 Rust/HEAD/index; audit-draft.json binds the
proposal. operation-source-audit.md and physical-port-source-audit.md preserve
independent source findings. Seven-document factual promotion passed final
same-Reviewer consistency review0/0/0 Approve. No Rust/Cargo/tests/fmt/governance/external or commit/push
execution; all Rust, governance and Git state remain unchanged. Full P3 remains
active/incomplete; actual domain/cold/ports/guard/expiry/work/Layout freeze is
next, not another pure Move or a full-goal completion/impasse claim.
Reviewer independently verified audit-packet.json (315-line contract,35 links,
338 Rust/governance/HEAD/index unchanged), exact inverse proposal SHA and the
physical-port source facts. No expanded protocol or P6/full-P3 claim was found.
Only this actual review evidence and artifact transcription is allowed without
another review; audit-final.json seals the final factual promotion.

## Joint working freeze — expiry choice reviewed, full joint still open

`docs/tech/2026-09-07-knife16-r7-p3-binding-joint-contract.md` now concentrates
the remaining coupled rows. Same Heisenberg reviewed its246-line proposal
(`ce4973afd8f5b94fd6166822ff4c109b15562eadaaa0880904593dd7b573fe70`):
P0/P1/P2=0/0/0, Approve only sections2–6 expiry choice and section7's actual
actor-guard ordering; scoped Module/Clean/System/DDIA8/9/7/8. No necessary
structural optimization. The original expiry wrapper stays in the Controller
phase, cleanup does not wait on its own input-excluding continuation, late
commands use actual paired full-Flow disposition, and final output is produced
once. Timer due, guarded consumer, ports/masks/work/Layout remain unapproved.

Mencius's next source audit independently confirms the complete metadata
closure, outer Layout check dependencies, and56 tests:44 outer tests plus12
metadata-descendant tests; the latter have two-sided private access.
Cold input backing is not bounded by Vec length, tree levels reserve
WORD_BITS entries, and root/mailbox caches are distinct allocations. It also
confirms new OPEN currently commits on model live-F before source factory
admission; that gate reserves no physical Retiring position. A failed open
still owns a record/completion, while duplicates must not request a new F slot.
This is a future-cutover counterexample, not a current Retiring runtime failure.
Main checked receive_open/finish_flow and actual source/Controller bodies.

Artifact `/tmp/mini-vpn-p3-joint-binding-m6KfFp/`: entry verifies the previous
accepted packet and1155 files/338 Rust/HEAD/index; audit-expiry-draft binds the
first row review. Source audit summaries retain precise next-step dependencies.
STATUS current action is compacted without deleting the result's evidence;
six companion/result/learning documents are synchronized for consistency review.
No Rust/Cargo/tests/fmt/governance/external or commit/push execution. Full P3
stays active/incomplete; no objective narrowing or genuine impasse is claimed.

Final consistency review found P0/P1/P2=0/0/1: the result incorrectly attributed
two-sided private access to all56 tests. Main corrected the sentence to44 outer
plus12 metadata descendants, with the latter having dual access. Same
Heisenberg confirmed the sole P2 closed, P0/P1/P2=0/0/0, Approve only this repair
and seven-document consistency. The inverse sentence restores the prior result
SHA; other six documents,338 Rust/governance/HEAD/index and38 links verified.
`audit-p2-repair.json` binds that review; only factual joint/result transcription
followed, as permitted by Reviewer. `audit-final.json` seals the packet.
No approval of the full joint, guarded consumer/ports/masks/work/Layout,
Rust/P6/fullP3 is implied; no project tests or external actions were run.

## Joint OPEN admission selection — resident custody, not binding acceptance

The same working joint packet now selects the actual Executor F resident as
the owner of precommit reservation and later pending/open/live/retiring state.
One shared model classification holds an exclusive prepared borrow. New OPEN
reserves F/O and attempt storage before request admission; duplicates reserve
neither. Target installation/PeerOpenResolved retain their distinct final edge.
Modeled rejection returns original input/reservations; postcommit failure keeps
the same candidate/resident, not an independent reservation-transfer ledger.

Main traced actual actor input55/3232, Supervisor clone1295/rebox1241 and root
continuation691: a carrying command value is not original Box identity, and
every actor Controller Err currently becomes root failure. Original input and
arrival, actual bounded R_cmd site, internal settlement while waiting, release
notification and legal masks therefore remain joint obligations, not existing
seams. Current model live-F rejection is not silently converted to held retry.
Mencius's OPEN(B)->RESET/finalACK(A) counterexample vetoes that blanket policy;
it does not directly veto the narrower model-terminal Retiring/O selection.
No complete real-caller liveness or physical Layout proof is claimed.

Same Heisenberg first reported P0/P1/P2=0/0/1 for source anchor1288 instead of
1295. That single-token correction preserved all protocol content; inverse
repair restores the draft SHA. Final selection review0/0/0 Approve, scoped
Module/Clean/System/DDIA8/9/7/8. Reviewed joint479-line SHA
`b51237d0ed5a176b2d3cb6cefdc406e5f7f2ede7aa802d38c6f53b7acb9a101e`.
Artifact `/tmp/mini-vpn-p3-open-admission-sBPpNl/` entry verifies previous final
packet and1156 files/338 Rust/HEAD/index; draft and anchor-repair audits bind
review evidence. source-audit.md and liveness-audit.md preserve exact findings.
Compact docs are synchronized for final consistency review only. No Rust,
tests/Cargo/fmt/governance, HEAD/index mutation or external run. Full P3 stays active;
joint Interface/ports/caller/masks/work/Layout and Rust/P6 are still unaccepted.

Final same Heisenberg consistency review0/0/0 Approve: seven fingerprints,
38 links,338 Rust/governance/HEAD/index verified; prior evidence and sole P2
history preserved. No protocol expansion, old model-capacity retry relaxation
or allocator-abort scope change. Reviewer permitted factual joint/result
transcription without recursive review; `audit-final.json` seals that update.
This is another concrete design selection toward the original full-P3 goal,
not full joint/implementation/P6 acceptance or goal completion.

## Joint metadata/test locality — selection accepted, Rust still frozen

The existing joint packet sections15–18 select complete inward owner_ready
metadata plus canonical checked capacity geometry. ReadyClass/ReadyProcessPlan
budget and service policy stay outer. All12 real Root scenarios stay outer;
nine require metadata-private recipes and three use existing ready operations.
This finite audit supersedes the earlier coarse attribution of dual private
access to all12 descendants. No behavioral assertion or test is removed.

The proposed private test fixtures retain source/process exhaustion, actual
mutex poison, phases/key/revisions/word/mask/cursor/tree and epoch observations,
three native metadata-aware Wake bodies and exact original claim comparisons.
Outer command Box identity, Unknown/Full custody, mailbox operations, real
receipts and teardown remain outer. Inferred private fixture paths are not
production getters, public test Modules or a generic mutation Interface.
Exact finite methods and assertion/lifetime mapping remain to freeze.

Mencius independently confirmed ReadyIndex::new's sole caller at root629.
Its successful prefix already checks CommandResident Layout at601 and reserves
actual Vec<Waker> at615–618, dominating the two inner foreign Layout checks.
Removing only those checks is a separate characterized preparation; moving
Waker Layout earlier could change Reserve to Layout and is not equivalent.
Geometry only replaces original N/process/class arithmetic at the existing
plan stage. N=usize::MAX/4 with budgets=[usize::MAX,1,1,1] currently yields
TotalBudgetOverflow; moving inner N*5 ahead would change that error. Those
remaining allocation-stage checks stay in place. Only two of nine process-plan
tests contain four capacity-value reads requiring mechanical migration.

Same Heisenberg selection review: P0/P1/P2=0/0/0, Approve only locality/test
closure; Module/Clean/System/DDIA9/9/8/8, no necessary structural optimization.
Reviewed joint651 SHA
`be44ece9c23059ec7956775811dffa6416ad903a001e6f001cf115468efe9f31`.
All12 symbols,9 links, six unchanged companions,338 Rust/governance/HEAD/index
independently verified. Artifact `/tmp/mini-vpn-p3-ready-locality-lXGH09/`
contains entry/audit-draft and test-closure-audit.md; compact synchronization
awaits same-Reviewer consistency review. No Rust/tests/Cargo/fmt/governance,
commit/push or external action. Same-domain cold reservation/callers, actual
actor input/ports/expiry/masks/work/Layout and full joint/P6/P3 remain open.

Final same Heisenberg consistency review: P0/P1/P2=0/0/0, Approve only the
seven-document packet. Seven fingerprints,38 links,338 Rust/governance and
HEAD/index independently verified; inverse recovery matches the reviewed651
draft, prior result prefix unchanged,12=9+3 and Layout/error-order attribution
accurate. Reviewer allowed only factual joint/result transcription, sealed by
`audit-final.json` in the same artifact directory; no recursive review needed.
Full joint, exact Interface/cold binding, Rust, P6/fullP3 remain unaccepted.

## Joint finite test-operation and callback-lifetime contract

Joint sections19–21 refine the nine metadata-private recipes into finite
arrangement/observation points, retaining all12 outer scenarios. Exactly two
tests short-borrow the actual Running ReadyClaim: poison recovery in each
original round, healthy source unwind before recovery only. Expected key/
revision/epoch values stay independent; no Root borrow crosses a mutable poll,
no claim is reconstructed and no extra owning wrapper/reference is introduced.
Original outer command identity, mailbox operations, receipts and close remain.

The three native Wake implementations and lifetimes remain distinct. Poison
Wake keeps custom wake_by_ref and guarded panic; the two wake-only fixtures
retain the standard default's temporary Arc clone. Notify-unwind has no
explicit drop or strong_count1 assertion and keeps its original scopes;
the other two retain noop polls, explicit Waker/trigger release and exact count
checks. Outer dequeue-on-Drop/receipt fixtures cannot retain an extra clone or
use a common cleanup wrapper. No counter getter, mutation language or arbitrary
callback is introduced. Rust signatures are still uncompiled and ungranted.

Mencius's finite audit and separate matrix-transcription check found no
discrepancies. Main read original assertion/callback/teardown blocks and the
local std Wake implementation. Same Heisenberg verdict0/0/0 Approve only this
finite semantic/lifetime contract, Module/Clean/System/DDIA9/9/8/8; no required
structural optimization. Reviewed joint763 SHA
`23b33cb6fc43a0025ba793408e2480bff757e091b4bad4a5286b0d1aa02e6121`;
original668-line prefix,13 links, six companions,338 Rust/governance/HEAD/index
independently verified. Artifact `/tmp/mini-vpn-p3-fixture-contract-SLDwsd/`
contains entry/audit-draft/source-evidence.md. Compact synchronization awaits
same-Reviewer consistency closure. No new Rust tests/Cargo/fmt/governance or
commit/push/external actions; the current error log has no new failure to add.
Next resolve canonical geometry/Interface and actual same-domain cold returns,
then remaining actor-input/ports/expiry/masks/work/Layout. Full joint/P6/P3 and
fixture compilation/implementation remain unaccepted; the full goal stays active.

Final same Heisenberg consistency review0/0/0 Approve: six files changed,
ERRORS unchanged, seven fingerprints,32 changed-document links plus10 frozen
ERRORS links verified. Inverse recovery restores reviewed joint763 SHA;
historical result prefix,338 Rust/governance/HEAD/index unchanged. Scoped
acceptance and next geometry/Interface/same-domain cold-return gate are accurate.
Only factual joint/result transcription followed, as allowed by Reviewer;
immutable `audit-final.json` seals the packet. Full joint/Rust/P6/P3 remain open.

## Joint ReadyGeometry Interface and actual cold-caller inventory

Sections22–24 select private inward ReadyGeometry: checked NonZeroUsize N,
4*N+1 then N+1, with only immutable N/process/class-capacity value access.
The outer plan owns that one geometry; budgets/T/Q and class policy stay outer.
The existing metadata constructor borrows it instead of ReadyProcessPlan.
Old zero-N/budget/capacity/budget-sum/service error ordering stays intact;
N*5 and other physical checks remain at allocation. The MAX/4 budget-sum
characterization is a future pre-refactor gate, not a test executed here.

Independent source inventory: one checked constructor definition/alias,
18 invocation expressions in14 functions across six cfg(test) files; no
additional renamed/function/Core caller found. Source counts are not production
reachability. Live Target has actually completed OPEN with live_flows1 before
construction; TargetIoColdInventory::is_quiescent would wrongly reject it.
Replacement/acceptance already exist at the recovery caller, but its actual
carrier enters Controller only after new_checked and before output-root new.
No pre-failed/pre-expired caller was found; this does not prove an existing
constructor rejection rule for those phases.

BuildFailure currently has one construction site, exact clock/Executor/plan/
Capacity error and one external tuple-destructuring/retry test. The missing
fourth reservation must join every modeled return, not a guessed default.
Output-root has four calls and returns whole Controller; B/CurrentOnly preserve
their actual standby or actor/handoff seed. A test-only map_err discarding the
Controller on failure is not production settlement. Capacity witnesses are
not factory identity and SessionSnapshot lacks SessionId; no raw identity
getter or borrowed client-only gate is selected to fill that missing seam.

Same Heisenberg independently reviewed joint919 SHA
`13f9745018d319a360a2380332c7ea68922e476d75b1803e918a6aa29bbe24d1`:
P0/P1/P2=0/0/0, Approve only geometry value Interface and caller/return facts,
Module/Clean/System/DDIA9/9/8/8; no necessary structural optimization. Original
776-line prefix,19 links, six companions,338 Rust/governance/HEAD/index verified.
Artifact `/tmp/mini-vpn-p3-cold-geometry-pCRcse/` holds entry/draft/identity-wording
audits and caller-evidence.md. Final same Heisenberg consistency review is
P0/P1/P2=0/0/0, Approve only document consistency: six changed documents,
ERRORS unchanged, seven fingerprints,38 changed links/48 total links verified;
inverse recovery matches reviewed919 lines and retains the historical result
prefix. All338 Rust/governance/HEAD/index remain frozen. Reviewer permits only
this factual joint/result transcription and immutable audit-final sealing,
without recursive review of the transcription.
No Rust/tests/Cargo/fmt/governance, external or commit/push execution;
no new runtime failure is added to ERRORS. The next decisive obligation is
actual reservation lifecycle/Owner correlation/installation and exact returns,
then input/ports/expiry/masks/work/Layout. Full joint/cold binding/Rust/P6/P3
remain unaccepted; full-P3 goal remains active, not narrowed to geometry.

## Installed Owner correlation selection and actual cold-slot boundary

2026-09-08. Joint sections25–26 select a borrowed cold validation chain at
Executor/Supervisor/TCP Authority: private actual reducer SessionId is compared
inward without being exported; factory origin and the actual admission gate
must both match. The gate is also checked unpoisoned/open/bound to that session
and Owner direction. No remint, source/Target operation, allocation, notification,
or reusable identity certificate is introduced by the validation selection.
Capacity precedence, Active/Legless live/recovery preservation and original
four-input rejection returns remain controlling. Precise Rust representation
and the coupled preparation/installation transaction remain unimplemented.

Source discriminators: one actual Supervisor birth route; Executor accepts
config, not an external factory. Normal factory Clone shares backing; the
existing test fork preserves origin but replaces gate/ledgers/progress. Checking
gate identity detects this fork, not arbitrary independent ledger corruption;
the whole source proof also requires closed construction and private storage.
Existing recovery/physical identity/FIFO checks are not replaced.

The second bounded audit confirms no current single-slot ready reserve/bind/
release/reuse operation. Key generations stay1; Weak metadata supplies domain
association, not the key. Runtime ReadyClaim and capacity_waker are not cold
reservation; quarantine retains backing. Actual mailbox construction and
exclusive observer are reusable but unbound. Inbox/last-sender Drop may notify
and discard retained records, not prove settlement. These facts constrain the
next lifecycle design; no hypothetical reservation Drop was declared safe.

Same Heisenberg selection review: P0/P1/P2=0/0/0, Approve only sections25–26;
Module/Clean/System/DDIA9/9/8/8, no necessary optimization. Reviewed joint1056
SHA `8fcda1a29e5f0a6213030a0755077af5c639f6c56e46f8a53df9a2cf27713cbf`;
original931-line prefix, six companions,19 links and338 Rust/governance/HEAD/
index verified. Artifact `/tmp/mini-vpn-p3-owner-correlation-gG66iN/` contains
entry and audit-draft. Final same Heisenberg consistency review is0/0/0 Approve:
seven fingerprints, six changed documents,38 changed links/48 total verified;
inverse recovery matches reviewed1056 lines, original931/2683 joint/result
prefixes preserved,338 Rust/governance/HEAD/index frozen. ERRORS stayed unchanged.
Reviewer allowed this factual joint/result transcription and immutable
audit-final sealing without recursive review; no semantic expansion followed.
No Rust, tests/Cargo/fmt/governance, external or commit/push execution; ERRORS
unchanged because no new runtime failure occurred. Next: actual same-domain
slot lifecycle and coupled pair/return transaction, then remaining input/
ports/expiry/masks/work/Layout. Full joint/P6/P3 remain open; goal stays active.

## Cold-slot registration, association and scoped cancellation selection

2026-09-08. Joint sections27–29 select one ReadyShared metadata occupancy
ledger, distinct from physical parent payload. Each cell's registration stays
charged while Idle/Pending/quarantined; runtime ReadyClaim is not another
capacity lease. One session occupancy reserves its four class destinations;
activation requires real sources, and migration returns each class separately.
Metadata reservation uses the same domain, immutable key and checked bounds;
its cold N-record uniqueness/free-position work is explicitly not O(1) or an
already-charged DATA/service operation. No new N-wide side registry is selected.

Precommit constructor rejection keeps the original four inputs. Pure-cold
cancellation returns only never-associated/unpublished metadata after exact
validation and checked generation return, not SessionJoinedZero. Associated/
bound/retiring physical custody cannot take that cancellation path. P4/P5
table, bootstrap and whole-root publication remain separately scoped.

First same Heisenberg review on1246-line draft: Request Changes, P0/P1/P2
0/2/0, Module/Clean/System/DDIA8/9/6/7. Two causal document defects:
parent association had no explicit pre-Controller state; freely escaping cold
handle Drop could fault a parked Root without waking it. Main repaired only
the joint document within existing authority; no Rust/test-feedback repair.

AssociatedPreparing now permanently loses cold-cancel right but retains one
unused bind right, preserves actual class charges/association on rejection,
and consumes that right once on bind. AwaitingIdentity cannot bind or use a
forged ID. Cold handles/returned cold failures retain an exclusive ReadyIndex
borrow and stay only in unpublished startup or an actively executing synchronous
Root operation. The private holder/exit graph, not the borrow alone, forbids
escape across park/await/worker publication. Atomic fault+epoch dirty Drop
stays nonzero; active finalization checks it before Pending or success. Real
out-of-turn publisher faults retain their required safe notification.

Same Reviewer closure on1310-line SHA
`e8c2e42319e7378636d302d09e5e50f55b91baee32c2bd91bc54fcfee40aee41`:
0/0/0 Approve only revised selection; both P1s explicitly closed. Scoped
Module/Clean/System/DDIA9/9/8/8, no necessary optimization. Original1067 joint
prefix, six companions,19 links and338 Rust/governance/HEAD/index verified.
Artifact `/tmp/mini-vpn-p3-slot-lifecycle-om1Kjv/` holds immutable entry,
audit-draft, audit-repair and audit-packet. Final same Heisenberg consistency
review:0/0/0 Approve only document consistency. Seven changed fingerprints,
48 local links and338 Rust/governance/HEAD/index verified; inverse reconstruction
matches reviewed1310 lines and original1067 joint/2726 result prefixes remain.
Reviewer permits this factual joint/result transcription and immutable
audit-final sealing without recursive review; no semantic expansion followed.
This is not compiled borrow/exit or runtime evidence. Actual constructor/
return/cancel Interface, activation/eligibility, input/ports/expiry/masks/work/
Layout must still close before Rust. No tests/Cargo/fmt/governance, external,
commit/push or P4/P5 implementation; full-P3 goal remains active and incomplete.

## Opaque constructor ownership forms and actual Root terminal failure

2026-09-08. Joint sections30–33 select one private-field borrowed reservation
for both Cold and Associated construction attempts. Associated custody at rest
and successful binding are lifetime-free owning forms; Controller never stores
the borrowed wrapper. Original clock/Executor/plan/reservation return on every
modeled constructor rejection. Scoped constructor/failure visibility changes
together, with no raw ReadyIndex/enum-field exposure or new Debug requirement.
The contained Drop-bearing custody moves whole; wrapper/enum/failure themselves
have no Drop, avoiding E0509 without unsafe, Option hole or replacement token.

Same-Reviewer first verdict on1575-line draft:0/1/0 Request Changes, scores
Module/Clean/System/DDIA8/9/7/7. Cancel generation exhaustion/metadata fault
left a short borrow with neither bind nor associated-return available. Main
causally repaired only the joint document: the cancel failure's cold-only
terminal conversion retains the original whole metadata custody/cancel cause
and ends the borrow after first-fault/epoch publication, with four charges
retained and no Free/Associated/Bound reconstruction.

The real Root's single execution state gains an inline ColdQuarantined<T>
alternative retaining original Executor/clock/plan/constructor cause/frozen
metadata and cancel cause. Startup's owning build-failure alternative instead
holds that entry with the actual prepared ReadyIndex and remaining inputs.
Only one such fault destination is possible after process admission closes;
no N-wide session/bootstrap table, extra lease/task/allocator or P4 parent is
introduced. Internal static Target type migration is explicit and unimplemented.
This is modeled-failure custody, not arbitrary Target panic or physical shutdown.

Re-review on1648-line draft closed P1 but found0/0/1: the old disarm falsifier
omitted association and terminal quarantine. Main synchronized that exact
sentence and review history. Same Heisenberg final selection review on1654
SHA `74caaae1d055c0695bc8a0138365bb1423d4cf48afc74d25b66ed9700ac61859`:
0/0/0 Approve architecture selection only, scores9/9/8/8, no necessary
optimization. Both findings closed. Reversible repair, original1322 joint
prefix, six companions,19 links and338 Rust/governance/HEAD/index verified.

Independent Mencius source audits and Main's bounded reads found15 Root exits,
one outer Pending, the strong-gate admitted-work/finish rule, one actual
ReadyIndex construction, and18 checked calls in six private test files. A cold
scope must finish before any ordinary source claim and before final park;
later unrelated fault cannot erase an already-admitted MailboxOwned result.
Current source does not yet implement these new scopes, forms or pair binding.

Next-source audit separately confirms current SessionCommands includes ingress,
sole observer, two cells, selector and quarantine; real checked C holds only
capacity, not inbox or a bound service mode. Existing [1,blocked] initialization
requires a real Held OpenCompletion and cannot represent arbitrary live/recovery
Executor state. Full single-pair fields/nested Drop/initial registration, then
actor-input retry/ports/expiry/masks/work/Layout remain open before Rust.

Artifact `/tmp/mini-vpn-p3-binding-interface-Ec0aMh/` holds immutable entry,
audit-draft, audit-repair, audit-falsifier and audit-packet. Final same Heisenberg
consistency verdict:0/0/0 Approve, document consistency only. Seven fingerprints,
48 local links,338 Rust/governance/HEAD/index and inverse1654-line selection
verified; original1322 joint/2776 result prefixes preserved. Reviewer permits
only this factual joint/result transcription and immutable audit-final sealing
without recursive review; no semantic expansion followed. No Rust/compile
probe/tests/Cargo/fmt/governance, external,
commit/push or P4/P5 implementation occurred. Full P3 goal remains active.

## Actual single-pair fields and indirect legacy-route isolation

2026-09-08, architecture documents only. Joint sections34–35 select one checked
inbox and Executor-owned complete Bound SessionCommands/Flow backing/original
ready binding. One consuming construction operation creates its own fresh pair
and returns exact original inputs on modeled failure; final RMW precedes only
owned moves. No endpoint supplied independently, extra ingress/observer,
post-publication setter, legacy default binding or fake OpenCompletion.

Main and independent Mencius audited actual nested destruction: observer
registration precedes its lease; last sender can wake receiver; inbox Drop
closes/notifies before dropping actual queued/prefetched commands. The selected
field order is not a shutdown/terminal proof. Existing live checked fixture
opens a real Target/source before construction; recovery barrier/acceptance
exist before checked construction but the carrier enters Controller afterward.
The OPEN-only fixture's absent pending write/join/completion is not a generic
constructor guarantee. Client-local unresolved OPEN is not Owner source state.

The generic Controller impl still reaches shared effect handling. Read failure,
write failure and HalfClose failure all require the selected Bound Flow-local
feedback path; checked-specific methods alone cannot remove either legacy
mixed dequeue. Positive SinkAccepted/SinkHalfClosed retain their real observed
reduction semantics. Initial registration/activation and actual owning ports
remain a joint obligation, not a default mask or new P4/P5 publication claim.

Same Heisenberg first selection review:0/0/0 Approve only sections34–35,
Module/Clean/System/DDIA9/9/8/8, necessary optimization none. Reviewed1849-line
SHA `f003b5ac55defd257656cc4b82b2cb3727dafcb537995fe87c6c7bd42ec47f9d`;
original1664-line prefix, six companions,19 local links and338 Rust/governance/
HEAD/index independently verified. Immutable audit directory:
`/tmp/mini-vpn-p3-mailbox-pair-kghwmH/` (entry, audit-draft).
One status-patch attempt had an incorrect context line and failed verification
without writing; Main read the exact tail and applied the same factual update.
This was not a code/test failure or semantic repair. No Rust/compile probe,
tests/Cargo/fmt/governance execution, external operation, commit or push.
Full joint/P3 remain incomplete; next freeze registration and actual ports,
then original-input retry/expiry/work/Layout and exact implementation sequence.
Final same Heisenberg document-consistency review:0/0/0 Approve. Seven
fingerprints/48 links and338 Rust/governance/HEAD/index verified; inverse
reconstruction matches reviewed1849 lines and original1664 joint/2836 result
prefixes remain unchanged. Only this factual joint/result transcription and
immutable audit-final sealing were allowed without recursive review. The
initial-registration/ports row and full-P3 goal remain open; no semantic grant.

## Initial-work publication, selected-lane polling and real-port boundary

2026-09-08, architecture documents only. Joint sections36–38 select closed
Owner-local work publication rather than invented receiver readiness. Fresh
internal inboxes start without fake work; imported live sources owe both lane
probes. Inner enqueue success survives an outer recoverable Err, close has its
own drain edge, and a coalesced request is not a queue count. Unknown physical
send remains uncertain owning custody, never a retry or synthetic zero.

Mencius's two read-only audits traced the complete producer graph and pinned
Tokio lane/registration behavior. Generic TCP retained delivery guards really
can enqueue Control on Drop, but current Owner does not use those delivery
ports. Future bound provenance must preserve that exclusion. Selected inbox
polling cannot consume another lane's prefetch or EOF; receipt is retained
before Notify. One raw poll can make two list-pop attempts, not constant CPU
work. Each single-raw-poll unconstrained wrapper needs exact build/scope proof;
taskdump remains a separate early-deferral predicate. Registration racing a
wake requires registration OR retained real publication, not a cached-Waker
assertion. Actual try-only senders remain necessary for pre-receipt safety.

Same Heisenberg first reviewed the finite owning-operation feasibility relation
read-only at0/0/0, conditional only. A private P3 fixture may assemble real
actor/output/FIFO/lifecycle objects and invoke a complete production-shared
operation. It cannot implement shared selection/budgets/claim/Running,
execution/result/notification/finish/recheck/park itself. Existing lifecycle
slots retain actions without a clock/deadline or normal due consumer; stop
discard is not expiry. The exact shared Interface, real guard/consumer pairing
and global activation remain open; actual table/adoption/lookup stays P4–P6.

Same Heisenberg then reviewed the written2081-line draft at SHA
`90d31217ac65696592986afbbc21a0d5b6096fc0e957fc494f4f7d8a3bb3885f`:
P0/P1/P2=0/0/0, Approve only sections36–38's conditional protocol/boundary;
Module/Clean/System/DDIA9/9/8/8, necessary optimization none. Original1863-line
prefix, six unchanged companions,19 local links and338 Rust/governance/HEAD/
index independently verified. Immutable entry/audit-draft artifacts:
`/tmp/mini-vpn-p3-initial-work-AxdFlF/`. No Rust writes, compiler probes,
tests/Cargo/fmt/governance execution, external operations or commit/push.
Next freeze actual shared owning service and guarded consumers, then remaining
input/retry/masks/work/Layout and exact implementation gates. Full joint/P3
remain incomplete; no architecture prerequisite is treated as runtime proof.
Final same Heisenberg document-consistency review:0/0/0 Approve. Seven
fingerprints/48 links and338 Rust/governance/HEAD/index independently verified;
inverse recovery matches the reviewed2081-line draft, and original1863 joint/
2880 result prefixes remain intact. Only this factual joint/result verdict
transcription and immutable audit-final sealing are allowed without recursive
review. Conditional protocol acceptance does not close actual shared service,
consumer/build/exit/activation/lookup or full joint/P3.

## Real owning-port audit — phase/expiry hypotheses rejected

2026-09-08, architecture/source audit only. Same Heisenberg returned0/2/0
against two proposed assumptions: Running actor cannot gate every Controller-
local unit (Legless/no successor still owes actual expiry/Target settlement);
loosening controller_failed, ungating or rebuilding output root cannot migrate
Applied's original discarded/stamp/high-water and already Stopped/Fatal output.
Those design P1s remain open; no existing implementation was changed or failed.
Limited feasibility Module/Clean/System/DDIA8/9/7/8. The compatible direction
is complete Controller-local and actor-bound operations at the existing deep
Authority, preceded by exact phase/guard/owning migration amendment. Existing
B Continue guard remains unchanged. No optional ports, fake actor, extracted
Controller, fresh Awaiting or zero inventory can supply missing custody.

Mencius separately traced real timers. Ordinary ResumeGraceStarted carries
only generation. Initial/recovery loss consumes that effect and retains the
original expiry event in Executor-correlated high-water, without time. Terminal
grace retains the actual LocalFlow/tombstone. Physical lifecycle slots have
exact installation/restoration but no deadline registration/normal due consumer;
Client timer arbitration is not an Owner implementation. Recovery supersedes
high-water; session expiry invalidates Z, so stale timer disposition must use
the actual committed transition, not UnknownTerminalGrace as blanket permission.
Work/retained backing include Z installation scans, F completion selection and
actual expiry cleanup. Target-open timer capacity is not an Owner grace budget.

Joint sections39–40 record these facts and the next coupled phase/expiry gate.
The record does not close either P1 or grant a callable Interface/Rust. Main
read actual bodies and retains the sole Writer role; both agents were read-only.
Entry `/tmp/mini-vpn-p3-owning-port-Qx4WEj/entry.json` revalidated the prior sealed
packet and1156 files/338 Rust/HEAD/index. A few source lookups used nonexistent
names/glob paths, returned no such file, and were corrected through rg inventory;
these non-mutating navigation failures were not compiler or test RED evidence.
No Rust/compiler/tests/Cargo/fmt/governance, external operation or commit/push.
Full P3 remains active/incomplete; actual table/adoption/lookup stays P4–P6.
Same Heisenberg factual-record consistency verdict:0/0/0 Approve, distinct
from the still-open design findings0/2/0. Seven fingerprints/48 links,338 Rust/
governance/HEAD/index and original2096 joint/2928 result prefixes verified.
Only this actual verdict transcription and immutable audit-final sealing are
permitted without recursive review; no phase repair, Rust or full-P3 grant.

## Post-join whole-custody entry — entry/retention selection accepted

2026-09-08, architecture-only. Main followed the complete initial terminal
root, Controller binder, physical FIFO cleanup, joined stamp correlation and
consuming LeglessRootParts transfer. Mencius independently confirmed that this
branch preserves the original Controller/high-water and joined stamp, without
an old output root or Fatal output history. Its canonical first-failure time
is still available; missing time in high-water alone is not total loss of time
evidence. Initial and recovery-B Applied cannot be treated as one phase.

Same Heisenberg read-only feasibility verdict0/0/0 permits a doc-only first
whole-Parts output adoption in the existing deep Authority, not a new parent
or old Fatal reconstruction. Scoped Clean Architecture9/10; exact private
owning transfer, phase/expiry exits and build remain open. It does not require
moving the entire initial Authority, but rejects getter/tuple/re-export or
fixture-side finalization. The reviewer also identifies the decisive remaining
counterexample: join has no completion bound relative to grace. The finite
entry covers already-joined inputs only; it cannot own all pre-join expiry.
Both general design P1s remain open, not closed by this feasibility verdict.

Joint41–42 records the selected entry's original-owner retention, exact
whole-input construction rejection/pressure, first output final-fallible
ordering and non-serviceable unknown/failure history. It keeps the real grace
start/policy/due operation and replacement arbitration as the next coupled
gate. In particular Controller recovery=0 does not exclude an actor-held
pending replacement; no test-only no-B assumption can close the due consumer.
Pre-join, Active→Legless/no-ACK, stale Z and full-P3 service remain open.

Entry `/tmp/mini-vpn-p3-joined-legless-O22rae/entry.json` validates the prior
seal and1156 files/338 Rust/HEAD/index. Main is the sole seven-doc Writer; both
agents read-only. One lookup guessed a nonexistent plan filename, returned a
navigation error and was corrected via rg --files to the real capacity Module;
no compiler/test RED is claimed. Written amendment/compact review passed0/0/0.
No Rust/compiler/tests/Cargo/fmt/governance, external operation or commit/push.

Same Heisenberg independently approved the actual entry/retention selection,
not merely its record: P0/P1/P2=0/0/0, Module/Clean/System/DDIA9/9/8/8, necessary
optimization none. Reviewed draft joint2312 SHA
`606d411aa709071c0bfb1f6138c39f2fa179ac12126abb1f707c71102544aa22`;
seven fingerprints/48 links,2196/2968 prefixes and338 Rust/governance/HEAD/index
verified. Private transfer/unwind holder/callable expiry remain later gates;
both broad P1s remain0/2/0. The reviewer permits only faithful limited verdict/
status/evidence transcription and immutable audit-final, no recursive review
or protocol expansion. Full P3 remains active and incomplete.

## Semantic grace and arrival policy — finite written selection accepted

2026-09-08, architecture-only. Main selected actual semantic LegLost start S,
explicit checked G and original D=S+G, carried with the same-clock expiry
capability. Mencius disproved the earlier actor-first-failure preference using
the existing direct reporter/bootstrap caller, and demonstrated unbounded
pre-D FIFO pressure before full authentication. Current high-water has no S;
neither retained physical stamp nor adoption-time now can retrofit it.

Same Heisenberg's feasibility verdict is0/0/0, Module/Clean/System/DDIA9/9/7/8,
limited to start/eligibility/winner policy. Joint43–44 makes D an eligibility
cutoff, not a settlement/retention bound, and Installed the actual recovery
commit before acceptance enqueue. Real early-input stage/arrival/error custody
must survive. Actual bounded pressure resolution and candidate-set/no-successor
Authority are hard prerequisites of callable expiry. Both general phase P1s,
pre-join and full P3 remain open. Written seven-doc closure is accepted below.

Entry `/tmp/mini-vpn-p3-resume-grace-VzNLBg/entry.json` validates the preceding
seal and1156 files/338 Rust/HEAD/index. Only the same seven docs are writable;
Main is sole Writer and both agents read-only. A no-match shell glob and
incorrect patch contexts caused navigation/edit-precondition failures; corrected
with exact paths/context, without Rust/compiler/test execution or scope change.
No Rust, compiler/tests/Cargo/fmt/governance, external operation or commit/push.

Written review returned0/0/1 Request Changes: Resynchronize's table wording
incorrectly said the status carrier owned the consumed request. Source at
leg.rs480 consumes the request; PendingOwnerAttachStatusPublication at
owner_target.rs1667 owns its generated response/provenance/arm instead. Main
corrected only that policy sentence, preserving real output settlement and
explicitly forbidding return/retry of the consumed request. All other policy
clauses and hard stops remain unchanged; same-Reviewer repair review passed.

Final same Heisenberg verdict0/0/0 Approve, Module/Clean/System/DDIA9/9/7/8,
no additional optimization required. Joint2433 SHA
`a1d87a722162efefa0c76430c64b5262fd635765964b8d6422e6117400663bde`
is the reviewed repair; single-line inverse, seven fingerprints/48 links,
historical prefixes and338 Rust/governance/HEAD/index independently verified.
Reviewer permits only faithful finite verdict/status/evidence transcription
and immutable audit-final without recursive review. Both broad P1s, actual
pressure bound and candidate-set Authority remain open. No callable expiry,
complete joint, Rust or full P3 acceptance. No new test evidence is claimed.

## Whole-holder admission and normal no-ACK — finite selection accepted

2026-09-08, architecture-only. The preceding goal turn made progress by sealing
the finite grace policy. This turn audited a concrete normal pressure exit and
the owning candidate set, rather than rerunning Rust or relabeling a timeout.
Mencius found reporter after child/inbound drain and counters.wait_zero, before
endpoint drain/join. Existing Held shutdown discard consumes physical permits
without ACK while returning the actual decoded value. Waiting for reporter
with Held still owned would self-deadlock. The real pre-join terminal/FIFO/slot
primitives can settle LocalCancelled; old hard-terminal policy cannot.

Mencius also traced all relevant producers: Supervisor's borrowed authentication
does not count candidates; slot capacity is per instance; adopt_registered has
six callers, all actor tests, and checks actor/FIFO rather than C origin.
The strongest actual association is consuming initial accepted parts into
QuinnOwnerInitialAuthenticatedStandby, which also owns A/handoff inside seed.
Its consuming authentication/aggregate transfer currently have no outer callers.
NoB cannot follow from slot=None, especially during an entered ATTACH gate.

Same Heisenberg's direction and placement feasibility reviews each returned
0/2/0 freeze blockers, not new runtime regressions: A needs explicit normal
policy/complete ownership; Held continuation B has neither a release nor time
proof; C/B-only placement forgets A; normal cancellation does not fit the old
hard-terminal guard. The reviewer permits a concrete existing-initial-holder
to output-subroot migration without a P4 table, but not an Interface freeze.

Main selected direction A and wrote joint45–47: original initial owner retains
A/current-port, original C plus complete B association move whole into its
output subroot; no duplicate seed/C or new long-lived parent. Authentication,
registration, retry and cancellation remain inside the association. Actual
phase determines carrying failure after enqueue/consumption. The normal path
is entered disposition→no-ACK input consumption→real reporter→exact FIFO/slot/
stamp→direct join; no existing spawn/abort path is imported. Existing non-initial
cold inputs remain genuine inputs, not synthetic A seeds. This is a substantive
selection accepted by written review below, not a complete pressure/callable gate.

Entry `/tmp/mini-vpn-p3-pressure-resolution-0eNfQc/entry.json` verifies the prior
seal and1156 files/338 Rust/HEAD/index. Main alone may write the same seven docs;
agents remain read-only. An incorrect ADR filename lookup was corrected through
rg --files; no compiler/test failure is claimed. No Rust/compiler/tests/Cargo/
fmt/governance, external, commit or push. Full P3 and both broad P1s remain open.

Written selection review returned0/0/1: the old adapter's stop disposition for
UnexpectedLocalCancellation is Cancelled at physical_root.rs663, not Fatal;
other rejections call fail_output. Main verified the source and corrected only
that distinction, leaving the old guard and all selected normal policy intact.
The reviewer found no new P1 in whole-holder/current-phase/no-ACK selections;
same-Reviewer repair review passed. No runtime failure/test is claimed.

Final same Heisenberg verdict0/0/0 Approve, Module/Clean/System/DDIA9/9/7/8,
limited to actual holder migration and normal no-ACK ordering. Reviewed
joint2595 SHA
`0735984c13aed9c7c1736e5eb594ef778994ab7dcbf7ed3823f7b5e6162006e1`;
exact inverse, result-only append, seven fingerprints/48 links,2444/3055
historical prefixes and338 Rust/governance/HEAD/index independently verified.
Only faithful finite verdict/status/evidence transcription and immutable
audit-final are permitted without recursive review. Complete phase, candidate
fence, charged work bound, callable Interface/Rust and both broad P1s remain
open. Full P3 remains active, not replaced by this prerequisite.

## 2026-09-08 — Exact initial-placement preparatory plan accepted

Entry `/tmp/mini-vpn-p3-initial-placement-mDjE2h/entry.json` verifies the prior
holder/order seal,1156 repository files,338 Rust, HEAD532d766 and unchanged index.
This stage remains seven-document-only, with Main as sole Writer, Mencius as
source auditor and the same Heisenberg as independent read-only Reviewer.

Mencius first found complete inward initial placement viable but a2067-line
whole-file relocation invalid. Widening continuation would break its existing
private declaration/constructor/service contract. Main selected a more precise
closure: all declarations/storage remain initial, all15 accepted-current impls
move into one private child. Independent source audit confirms all21 private
method callers move together;45 pub(crate) operations retain their scope and
the seed operation plus five later parent anchors retain composition scope.
The child introduces no type export or forwarding Interface.

Main independently read the complete selected operations and the two actual
source tests. Immutable closure.json records15 complete brace-delimited impl
hashes,1072 lines and995 remaining original lines. Mencius's grouped1082/985
includes ten separator lines, not a different closure. The original43-line
structural test is also hashed. A local read-only manifest guard initially
matched only `impl `, rejecting generic `impl<...>` starts; correcting that
predicate made the audit pass. No Rust candidate, compiler probe or test ran.

Same Heisenberg feasibility review returned0/2/0 Request Changes: explicitly
amend implementation-plan468–470's owner parent freeze for one route, and
include both real provenance read paths. It confirmed net-zero is legal; no
artificial shrink helper is required. Main wrote joint48–50 with that narrow
proposed exception, exact symbol/visibility closure and three future green
boundaries: sole structural-test Move; complete implementation extraction;
then namespace relocation. Source checks follow actual parent and child,
never concatenated fake locality. Integrator alone handles shrink inventory.

At draft handoff, written review was pending. All current Rust remains frozen;
no Cargo, rustfmt,
governance execution, external operation, commit or push. Plan acceptance will
not itself grant execution or close either broad P1. After this finite plan,
the remaining coupled owning phases/candidate fence/work/due design remains
the next no-Rust action, not a substitute definition of full P3.

Same Heisenberg final verdict0/0/0 Approve, scoped Module/Clean/System/DDIA
9/9/8/8, no necessary additional optimization. Reviewed joint2772 SHA
`d8feb5267de486d74af3aedf74789bc256aa2e21508fa80e011aacaac683a496`.
It closes only the two finite plan findings: parent route exception and both
complete provenance routes. Independent15-impl/test hashes,21/45/six scope
counts, seven fingerprints/48 links, original2606/3115 prefixes and338 Rust/
governance/HEAD/index pass. Faithful finite status/evidence transcription and
immutable audit-final are permitted without recursive review. Actual format,
compilation, inverse reconstruction and runtime gates are future evidence;
there is no Rust execution grant or broad phase/P1/P3 acceptance. Next remains
coupled owning-phase/candidate-fence design, with this preparatory plan available
for a separately authorized future execution.

## 2026-09-08 — User-directed finite design exit and full-P3 consolidation

At the accepted placement boundary the user explicitly authorized stable
AGENTS.md rules and minimal current-plan/status synchronization: classify the
remaining full goal, identify actual first-path blockers, then use compilation
and lifecycle evidence. No full-P3 reduction, safety/growth/review waiver,
external/commit/push authority or unconditional Rust scope was added.

Main integrated these norms into Work rhythm and Structural convergence;
AGENTS318→356. STATUS's270-line current-action chronology was condensed into
the current decision/checklist and links to this existing evidence, not erased
from the authoritative result history. Same eight authorized docs only; no new
repository sidecar. The existing audit tool is reused with an explicit AGENTS
scope option; script changes are not functional progress.

Mencius's bounded source audit confirms the earliest real chain: Active,
genuinely opened Flow→original Target/Driver→inward two cells/paired inbox→
observed Controller reduction→existing output guard→actual actor FIFO/action.
Root poll_ready_unit currently lacks that connection. Existing physical fixture
operations supply real endpoints; they cannot supply missing production service
logic or prove future candidate-set/parent adoption. New OPEN and Legless/B
expiry are later required P3 verticals, not all mandatory before Active DATA.

Main wrote one consolidated joint51 A/B/C checklist, exact design exit and
first full-path test candidate. It explicitly narrows the old global expiry/
before-Rust sequence only for a separately frozen Active implementation packet;
actual binding, current-phase error/stop custody, real readiness, checked live
backing/cold Layout, scope and independent review remain mandatory. Initial
placement remains an accepted available plan, not an automatic prerequisite.
No success stubs, fake ports, fixture-owned business logic or zero shortcuts.

Production changes zero; Rust test changes zero; Cargo/lifecycle tests executed
zero. A patch-context mismatch was corrected without touching Rust. Scope and
whitespace checks are documentation checks only. Substantive same-Reviewer
review of the workflow/scope amendment was requested. Next is one concrete bound
Active implementation packet, not another sequence of design-only approvals;
if its first-path contract cannot close, expose the genuine architecture choice
instead of expanding the prerequisite list again. Full P3 remains incomplete.

Final same-Reviewer0/0/0 Approve covers workflow, execution order and eight-doc
consistency only; scores9/9/8/8, no necessary new document stage or Module.
Reviewed joint2909 SHA
a2854e8728b456c52f92f1508dd28b02e3a5c96c9f90fd3297910a0ab218a3a3.
A1–A4 remain the concrete next implementation gate; reachable RESET,
FlowFinished, error, stop and phase-change custody cannot be deferred. All
eight fingerprints/48 links, original3168 result prefix and338 Rust/governance/
HEAD/index independently pass. Reviewer permits faithful status/evidence and
final audit without recursive review. No compile/test, Rust scope or overall
P3 completion is claimed.

## 2026-09-08 — Authorized CurrentOnly residency and real Active fixture

The user explicitly confirmed the independently reviewed scope choice:
P3's actual ready resident may own the complete already-built CurrentOnly and
call its shared service. An opaque owning representation may preserve private
fields/phases while exposing only necessary owner-scoped operations/values.
The original constructor remains private and unchanged; P4 table/bootstrap,
P5 PreparingParts/fused seed publication and P6 caller remain separate. The
same Reviewer accepted the scope at0/0/0, not as implementation evidence.

Main then implemented only two Rust test paths. In current_only.rs,
prepare_current_only_fixture returns the real actor/Executor/handoff/plan,
client/listener and exact AcceptedInitialAttach. Synchronous finish_current_only
retains the original checked-construction order; the old wrapper keeps a named
accepted-client binding alive through finish. The original five test bodies
and FQ routes are unchanged. Same Reviewer accepted this extraction0/0/0.

The new private current_only/active_source.rs uses the existing real client
bootstrap, sends actual reducer-generated OPEN and OPEN_RESULT through both
actor FIFOs, matches the original Flow, retires both ordered waits and feeds
five bytes into MemoryTarget. It asserts a real live Target and no pending
executor work/source ownership, then enters unchanged CurrentOnly preflight.
Structural joins report zero actor messages/bytes but retain Controller,
Executor, handoff and output each1, explicitly not whole-zero. No Bound service
or old mixed Target-read call is used. read_calls=0 is observed before finish;
it is not an independent post-construction side-effect test.

Feedback: the extraction's only formatting adjustment expanded its return
tuple signature. The new fixture's first compile produced E0432 because
TargetAddr was imported from resumable; the exact existing shared re-export
repaired that test import, without production/assertion changes. It then ran
GREEN. This compile defect is not the future bound-route expected RED.

Fresh local evidence (all exit0; exact filters fully qualified below):

| Check | Result |
|---|---|
| Pre-write standalone governance, rustc -D warnings --edition=2024 --test tests/code_growth_governance.rs | 6/6 |
| CurrentOnly original Module before/after private fixture extraction | 5/5 each |
| New exact preopened_target_data_enters_current_only_without_being_read | 1/1 |
| Same built unit-test binary, new exact, ten separate serial invocations | 10/10 |
| cargo test --offline --lib owned_upstream::quinn_leg_composition::owner::physical_session_root::tests::current_only:: -- --nocapture --test-threads=1 | 6/6 |
| cargo test --offline --test knife16_owned_leg_provenance --test repository_test_module_provenance -- --test-threads=1 | 162/162 and13/13 |
| Post-change standalone governance and cargo test --offline --all-features --test code_growth_governance -- --test-threads=1 | 6/6 each |
| cargo fmt --all -- --check; git diff --check; new-leaf whitespace | PASS |
| cargo check --offline --all-features --lib | PASS |

The new exact filter is
owned_upstream::quinn_leg_composition::owner::physical_session_root::tests::current_only::active_source::preopened_target_data_enters_current_only_without_being_read.
Repeat binary: target/debug/deps/mini_vpn-ec6d9947ca43a682, with --exact
--nocapture --test-threads=1. Standalone executable:
/tmp/mini-vpn-p3-active-NBQyq2/code-growth-governance. Library runs retained the
existing22 vendored warnings; provenance also emits its existing fixture
unused/dead-code warnings. No warnings suppression was introduced.

Final test fingerprints:

- current_only.rs744: 67e10c65256c47d464059ea84cc30578ae8077949191a48a8406fd9d0e1a0dfa.
- current_only/active_source.rs219: 4c082292d3959d2fcdd424ff259f20ee3b76fdc44d73a2f5af1e6ab8d808c6c9.

Both satisfy800; no oversized file shrank and no governance inventory changed.
Same Heisenberg final fixture review0/0/0 Approve found no necessary repair;
it explicitly limits this evidence to the real pre-opened constructor input.
The source refactor is a scoped9/10 preparatory improvement, not a production
readiness score. Main's snapshot comparison confirms only the existing test
parent plus its new child changed among Rust: production/governance/HEAD/index
unchanged. No commit/push/external operation or full-P3 ledger promotion.

Next: complete the concrete A1–A4 shared-code binding/receipt/ready/owner write
set and run bound_active_owner_source_reaches_real_output_once against this
real fixture. Residency authorization is already granted; do not ask for it
again or add prerequisite sidecars. Actual Root/Executor/C wiring, readiness,
pressure/cancel/error/stop, remaining full P3 and Case C/R7/Task4 remain open.

## 2026-09-08 — Checked construction and Target completion pure Moves

Same Heisenberg approved the six-file pure-Move scope independently of the
unfinished A1–A4 behavior packet, P0/P1/P2=0/0/0. Main was sole feature Writer;
Mencius handled each exact governance ratchet after its focused green boundary.
No new architecture sidecar, constructor parameter, service state, caller change,
runtime test body/FQ change or behavior repair was introduced.

| File / ownership | Before → after |
|---|---|
| owner_session_controller.rs | 3620→3565; private construction route |
| owner_session_controller/construction.rs | new71; complete checked impl, three original pub(crate) methods |
| owner_session_controller/tests/capacity.rs | 431→434; source anchor follows child, forbidden seams checked in both sources |
| owner_target.rs | 10206→10061; private completion route |
| owner_target/completion.rs | new164; complete failure, submit and retry methods |
| owner_target/read_source.rs | 164 unchanged; module-doc ownership correction only |
| tests/code_growth_governance.rs | 698 unchanged; Integrator-only3565/10061 ratchets |

Construction method bodies and signatures remain verbatim. The three completion
declarations become pub(super) inside the private child, restoring exactly the
original owner_target-plus-descendants access, not owned_upstream visibility.
Storage/callees stay at the original Authority; no forwarding method/re-export.
RESET mixed dequeue/reduction remains unchanged and is still a later cutover
obligation. Knife aborted-page ledger/retirement targets stay in their parents.

Fresh evidence, all exit0:

- Before either Move: capacity7/7, nested-abort7/7, owner-ACK8/8, full C44/44,
  Target74/74, CurrentOnly6/6, continuation16/16, initial acceptance8/8,
  physical attach9/9, owner recovery2/2; standalone6/6 and fmt/diff pass.
- First Move: cargo test --offline --lib
  owned_upstream::owner_session_controller::tests::capacity:: -- --nocapture
  --test-threads=1:7/7; full C44/44, fmt/diff pass. After its ratchet,
  standalone and Cargo all-feature governance each6/6.
- Second Move: equivalent Cargo lib filter
  owned_upstream::owner_target::tests::r7_nested_abort:: returned7/7; fmt/diff pass.
- Final rebuilt unit-test binary target/debug/deps/mini_vpn-ec6d9947ca43a682,
  --test-threads=1, the seven prefixes above (C/Target/CurrentOnly/continuation/
  initial/physical/recovery):44+74+6+16+8+9+2=159/159.
- The exact preopened_target_data_enters_current_only_without_being_read
  filter from the preceding entry, --exact --nocapture --test-threads=1,
  ten separate final-binary invocations:10/10.
- cargo test --offline --test knife16_owned_leg_provenance --test
  repository_test_module_provenance -- --test-threads=1:162/162 and13/13.
- After the second ratchet, freshly compiled standalone rustc -D warnings
  --edition=2024 governance:6/6; cargo test --offline --all-features --test
  code_growth_governance -- --test-threads=1:6/6.
- cargo fmt --all -- --check, git diff --check, both new-child whitespace,
  and cargo check --offline --all-features --lib pass. Existing22 vendored
  warnings remain; provenance also has existing fixture unused/dead-code warnings.

Inverse reconstruction matches the full pre-Move parents exactly:
C5b16343d1220a64221d9debf00c746202c36430fb49882f07a7ba2141322ec2c;
Target5e0aa12ed52a8546236034fec0f5b49e801003a3873040a9f08f2fa0a482936b.
Final child SHA256 values: construction
b6440085a6206f5a30cc333c90ceda76d9dc5ff7e73296479608d5f735243c6b;
completion a916dc4791c7819041a2078a0e95a38eb8c776e8a019b6660fa64eeba67cd2e6.
Governance SHA689eb823c64b2ed88ad5a36f18b61735c5085ddaa861f7e0fe1cfa2bcd551e4e.
Snapshot comparison includes only this six-file scope/two ratchets and the
preceding two-file Active fixture; other Rust, HEAD and index are unchanged.

Same independent Heisenberg final source closure:0/0/0 Approve, scoped Move9/10,
no necessary additional optimization. He independently verified both original
blocks and complete inverse parents, source guards/effective scopes and the
unchanged read-source methods. Reviewer did not run Cargo; the executed gates
above are Main evidence. Faithful result transcription needs no wording-only
re-review. No commit/push or external acceptance occurred.

The separate cfg(test)-only opaque Prototype resident representation was also
approved at choice level0/0/0, Module/Clean/System/DDIA9/9/8/8. It preserves old
actual commands-only scenarios with one inward command owner and the same
Root scheduler loop, never as the true binding proof. It is not implemented.
Next remains the coupled same-domain construction, finite test-seam migration,
actual source/inbox/observed-reduction/output operation and readiness/retained
lifecycle A1–A4 packet. The eighteen checked callers remain unchanged. No
initial-Authority Move, caller-test Move, metadata/command Move or bound-service
GREEN is claimed. Full P3 ledger, Case C, R7 and Task4 remain incomplete.

## 2026-09-08 — Complete command visit extracted from Root dispatch

Finite preparatory scope was independently approved before writing. The only
Rust change is server_root.rs825→857, below1500; tests, metadata, constructors,
other Rust and governance inventory were not edited. No namespace/Prototype/
Bound implementation or new runtime behavior is claimed. This is an actual
dependency of moving the sole commands inward without moving Root policy.

SessionCommands::visit_selected_lane owns original pre/post command validation,
one transfer or one borrowed rearm poll and the exact runnable/blocked masks.
Its private SessionCommandVisit returns only existing value work and masks.
Full and every Rearm outcome, including UnrelatedLane, preserve runnable|=blocked.
Root first short-circuits resident fault, supplies the same short-borrowed
capacity Waker and retains claim, budget, quarantine ordering, finish and park.
No command, returned Box, observer, Context or claim is exported. Unwind keeps
the original Running claim and TransferringUnknown; Err(()) replaces only the
original cause-free invalid-custody branch. No new post-source root-fault check
skips the finish of work that was already admitted.

Actual evidence, all commands exit0:

- Before and after extraction: cargo test --offline --lib
  owned_upstream::quinn_leg_composition::owner::server_root:: -- --nocapture
  --test-threads=1,56/56 at unchanged FQs. No test assertion/body changed.
- Rebuilt target/debug/deps/mini_vpn-ec6d9947ca43a682, --exact --test-threads=1:
  poisoned_metadata_source_unwind_retains_claim_and_mailbox_disposition,
  healthy_source_unwind_quarantines_only_its_resident,
  unrelated_lane_rearm_registers_remaining_full_before_root_parks,
  committed_publisher_fault_survives_root_notify_unwind and
  fatal_during_cached_waker_replacement_prevents_repark each ran10 times:50/50.
  Their exact paths remain below server_root::scheduler::tests::ready_dispatch.
- cargo test --offline --all-features --test knife16_owned_leg_provenance
  --test repository_test_module_provenance --test code_growth_governance
  -- --test-threads=1:162/162,13/13,6/6. Governance-only repeat also6/6.
- Fresh standalone rustc -D warnings --edition=2024 governance, before and
  after:6/6. cargo fmt --all -- --check, git diff --check and
  cargo check --offline --all-features --lib pass. Existing22 vendored warnings
  and provenance fixture warnings remain; no new non-vendored warning.

Final server_root SHA8a49b88b48b5b6fbb4d1c136545b69c139a37c649b9a7d6cc3b8a2344cd9bd84.
Removing the new helper/result and restoring the original743–784 caller block
reconstructs the entire original825-line file byte-for-byte, SHA
edcfcd18886b0d2ea601d5958a1776cfadd9b25548cd04c6cec327c1384c18a6.
Same Heisenberg independently verified that inverse and the actual final
source: P0/P1/P2=0/0/0 Approve, Module/Clean/System/DDIA9/9/8/8; no necessary
optimization. Reviewer did not run tests; execution evidence above is Main's.
Factual result transcription requires no wording-only re-review.

Mencius also supplied the finite candidate Prototype assertion/35 inward plus
12 outer test mapping. That candidate remains unimplemented and is not this
review's grant. Next remains joint51's concrete A1–A4 same-domain construction,
source/inbox/output/readiness/lifecycle packet using the real Active fixture.
No full-P3 ledger checkbox, P4–P6, commit/push or external acceptance changed.

## 2026-09-08 — One A1–A4 executable candidate, three review repairs pending closure

This step changes documents only: joint51 contains one concrete preparation/
whole-resident, pair/receipt/source/output, Target registration/backing and
finite file/symbol packet. It consumes the real Active fixture and accepted
command/implementation Moves; it does not replace the original full P3 ledger.
No Rust was written, and no Rust/Cargo/build/runtime test was run in this step.
Tracked git diff --check and candidate whitespace/final-newline checks passed;
those checks are not runtime evidence. No new sidecar or duplicate audit script.

Mencius's finite source audits supplied: the exact18 checked-caller lifetime/
return map; actual Target named/common registration requirements and physical
F_owner versus F_target backing; real Actor writer/DELIVERED/close producers.
Main read the relevant actual implementations and accepted contracts. The
endpoint-boundary decision was separately reviewed0/0/0: first real FIFO
admission may be checked with actual peer content observation, but that tail
does not implement pressure rearm or defer its P3 obligation to P6.

Same Heisenberg's first complete packet review was0/3/0 Request Changes,
Module/Clean/System/DDIA8/8/6/6. Concrete blockers were:

1. Bound-but-inactive producer due and final activation handoff were missing.
2. Publication interval lacked typed admission/finish failures and distinct
   source-unwind versus shared Root Notify-unwind settlement.
3. Output pressure wake was explicitly required while actual Actor writing
   scope remained excluded.

The repaired candidate now selects inactive retained requests and one locked
activation edge; a single atomic publication word outside the metadata Mutex,
typed NotEntered versus completed-failure retention and the original one-waiter
notification-fault guard; and an Actor-owned registered one-frame flush using
actual permit-release/DELIVERED/early-close edges. It does not notify from
TrackedPermit Drop or from transient acquisition rollback. Exact Actor
Readiness/output Move/implementation/test scope and physical backing are
included. Original architecture ownership and all earlier accepted Moves stay
intact. On 2026-09-09 the second review returned0/1/0 Request Changes,
Module/Clean/System/DDIA8/8/7/7: inactive activation and publication failure/
unwind closed; normal output release and child failure were sufficient, but
the whole outer actor-root exit still lacked the new Output wake.

Main repaired that omission in the same packet. The selected Actor operation
registers its existing sole-root JoinSet with the same controlled Output
publisher, retains the exact nested completion in original observed_root and
shares that result with later join. No new task/handle/result owner or Output
Notify in ActorRootExitSignal Drop. The private JoinError variant holds the
actual Tokio error before formatting. One primitive-local unconstrained poll,
the resolved trace exclusion, exact cached/in-flight references and real
registered outer-exit tests are included. Final same Heisenberg review0/0/0
Approve, Module/Clean/System/DDIA9/9/8/8, confirms the remaining P1 closed and
permits this finite packet's frozen Rust sequence under existing authorization.
It ran no tests and does not accept implemented binding or the full P3 ledger.

Mencius independently audited pinned Tokio1.52.0, Main checked the relevant
actor and JoinSet/harness paths. Completion is committed after future disposal
and invokes the list's outer Waker after unlocking; controlled clone/drop can
occur inside that lock. Last-result removal leaves a possible cached Waker;
empty/noop polling is not retirement. Task completion catching a notification
panic does not establish Root liveness. Runtime shutdown cannot promise further
Root execution. These are source findings and candidate constraints, NOT
executed runtime tests or implementation acceptance. No additional sidecar.

Next executable gate is the packet's actual baseline/Moves/compile RED/GREEN
and lifecycle tests. The reviewed scope permits those actions; full A/P3/
Case C/R7/Task4 completion does not follow. No governance inventory, commit/push, public TUN
or external operation changed.

## 2026-09-09 — Cold construction pure Moves: reviewed and accepted

The accepted joint51 packet now has actual Rust movement, not another design
prerequisite. Main moved complete existing implementations within their original
Authorities; original fields, nominal anchors, callers, error order and resource
ownership remain. Private child visibility preserves each former effective
scope. No ready binding, paired inbox or selected service behavior was added.

| Original Authority | Complete moved implementation | Parent before/after | Child lines |
|---|---|---|---|
| Controller | BuildFailure source/into_parts/Debug into existing construction.rs |3565→3536|102|
| Target executor | Five initial/ordinary/legless constructor methods into construction.rs |10061→9877|203|
| Supervisor | Three sole factory mint methods into source_factory.rs |5772→5682|106|
| TCP factory | new_bound/new_internal into factory_construction.rs |2883→2828|71|

Four original parent files reconstruct byte-for-byte after inverse movement,
removal of the new private routes and normalization of only moved visibility.
Original complete bodies are present in each child. Pre-Move SHA256:

- Controller: a7f4e3ad992a9ea43a0e8b20f42ef78ba3a32e4e51ada8f962dd0f0315675f32.
- Target: 381f53e667308e535009eb9d1b10d4ae31c0f9647398f913b94b7e8394b8948b.
- Supervisor: 796abd2146365f783c2ceb07aa385dcf7883de126f576a386689ee898410d3b7.
- TCP: 9568b8cd6f0e6c6f894cf621cc52e01da989bc01eeaff55c295443dae0d94e60.

Pre-Move all-feature characterizations: Controller44, Target74, Supervisor48,
TCP68 =234/234; provenance162/13 and standalone governance6/6. After movement,
Controller44/44 passed first. Target initially73/74: the existing initial-
bootstrap source guard scanned only parent and found0 instead of1. The sole
test repair adds the actual construction child to that source domain while
retaining all four exact1 mint/decomposition counts and original Supervisor/
legacy guards; exact repair1/1 then Target74/74. Test leaf548→550. One new TCP
child import-wrap fmt difference was repaired without behavior or line growth.

Final executed matrices, separately default and all-features, each275/275:
Controller44 + Target74 + Supervisor48 + TCP68 + CurrentOnly6 + continuation16
+ initial acceptance8 + physical attach9 + physical recovery2. The initial
misnamed r7_initial_physical_root filter ran0, was rejected as evidence and
replaced after source inspection with the actual r7_initial_acceptance_settlement
Module; only its8/8 counts above. Known unrelated real-loopback fixture debt
was not run or declared repaired. No test name/assertion/lifecycle was removed.

Standalone governance reached exactly the four expected shrink findings.
Integrator Mencius changed only those four ratchets, independently verified
their exact file counts and inverse inventory, and rebuilt standalone with
rustc -D warnings --edition=2024:6/6. Governance remains698, SHA
7af97a6605f98bb48125169fbdc221887b6000523b304e9db2e2cbca6d8ff049.
Main's final all-feature Cargo governance6/6, Knife16 provenance162/162,
repository provenance13/13, fmt/diff/all-feature cargo check all pass.
Tracked diff and all four construction child no-index whitespace checks pass;
exit1 with no output is expected for untracked files differing from /dev/null.
Only existing22 vendored smoltcp warnings and provenance fixture warnings.

Same Heisenberg code review0/0/0 Approve independently confirms all four full-
parent inverses, the unchanged original checked-construction56-line block,
effective scopes, source guard and exact governance inverse. Scoped Move9/10,
Module/Clean/System/DDIA9/9/8/8; no necessary additional optimization. Reviewer
read the Integrator's standalone6/6 log but did not run Cargo/tests; executed
matrix evidence above is Main's. Rust/governance Writers are stopped.
Next is the remaining necessary moves/geometry and actual bound-source
RED/GREEN in the same finite packet. No new sidecar,
commit/push/external operation; full A/P3/Case C/R7/Task4 remain incomplete.

## 2026-09-09 — Actor Move baseline: blocked-write cancellation fails before edits

No actor movement or Rust/governance write occurred in this diagnostic slice.
Fresh standalone governance passed6/6. Eight all-feature exact actor baselines
each passed1/1: readiness wait/fatal cancellation, out-of-order publication,
unpublished reserved epoch, abort before first root poll, aborted outer-root
progress wake, exit between progress check/subscription, fixed actor pressure
retry and submitted-FIFO rejection until ordered wait retirement.

The ninth baseline was:

```text
cargo test --offline --all-features --lib 'owned_upstream::quinn_leg_actor::tests::data_writer_cancel_interrupts_an_actually_pending_ordered_quic_write' -- --exact --test-threads=1
```

It ran1 test and failed at tests/mod.rs:13581 with
ResidualOwnership{messages:1,bytes:0}. A diagnostic repeat adding --nocapture
ran1 test and failed identically. Both exit101,1690 filtered; only existing
vendored warnings. No tenth baseline, arrival/FIFO group, Move, post-Move gate
or new binding behavior ran after the first failure.

Ranked hypotheses were retained FIFO wait, unreleased background resource,
and incorrect join inventory. Source inspection resolves the ownership:
Submitted removes the queued frame but installs an undelivered ordered wait;
the real QUIC write is demonstrably Pending and its receipt is not Delivered.
Cancellation drains children, clears pending delivery and waits for transport
counters zero before reporting the real LocalCancelled terminal (actor root
5948–5980). Join then independently adds the FIFO wait to its inventory
(3573–3609), correctly producing1/0. This is not evidence of a new production
regression or the separately excluded already-delivered real-loopback debt.

The existing try_transact_reported_terminal_with_parts and
begin_owner_terminal_cleanup validate runtime/LegIo/terminal/sole-FIFO identity
and can retire this wait without manufacturing DELIVERED. A proposed repair
must consume/assert the real LocalCancelled terminal and preserve/correlate
its original stamp with the later joined root stamp. Simply copying the
peer-close helper and keeping joined.terminal_reason would create another
failure: that test accessor falls back to FatalIo once its terminal is consumed.

The exact current joint51 scope only permits a new route in the oversized
actor test index, not modification of this old test. Same Heisenberg completed
the bounded read-only review at P0/P1/P2=0/1/0: the P1 is this test Adapter's
missing settlement, not a production join flaw. Source hashes match; Reviewer
relies on Main's two runtime failures and did not run tests or write files.
Review submission01a08473-dea7-7b30-8fa0-fe11f32ce16a completed1788929121.

Preferred proposed scope, awaiting explicit authority:

- quinn_leg_actor/tests/mod.rs: one private ordered_write_cancel route; replace
  only the old cancel/join block with one call returning (joined,actual_terminal).
  Point the original LocalCancelled assertion at that real terminal. Preserve
  Pending proof, no-DELIVERED receipts, all counter assertions and peer cleanup.
- New quinn_leg_actor/tests/ordered_write_cancel.rs: one parent-scoped test
  entry owns the complete lifecycle. Cancel and first bounded join must retain
  the exact ResidualOwnership{1,0}; this observes the root before synchronously
  taking its real reported terminal. Validate zero queued/summed/stored bytes
  and one wait through the existing terminal/FIFO transaction, commit once,
  retain the actual terminal and handoff stamp, then second bounded join must
  succeed and its original root stamp must match that handoff stamp. No
  synthetic terminal, reject-after-commit, DELIVERED wait or new polling loop.
- Integrator alone lowers any exact actor-index shrink; parent cannot grow
  and the new child stays at most800. No new wrapper type, production interface
  or Knife source-inventory edit: the original test/FQ and count110 remain.

After scope authorization: exact test/repeats, relevant FIFO/terminal/readiness
regressions, provenance/governance/fmt/diff/check and same-Reviewer code closure;
only then resume the frozen actor implementation Move. This diagnosis does
not itself authorize the two-file repair or change joint51. Production join,
first failure, counters and delivery semantics stay frozen. No new architecture
sidecar or test exclusion is introduced.

Current actor6769 SHA824d7f8f08feab5523d249edc0834d92c8a10011551b3b5c32c2ce9c477c98a0;
test index15818 SHA30bd586f2fa9008b94c9c3316eb509c2290870e8b198a22c911642594b476550;
governance698 remains unchanged. HEAD remains532d7660e3b862849aa7650589b182fd491df212.
Only current diagnosis/navigation/learning records changed; P3 remains active
and incomplete, with no commit/push or external operation.

## 2026-09-09 — Independent continuation test co-Move: accepted

The preceding actor diagnosis changed the next action, but its two-file repair
still lacks authority. Main checked the already-accepted joint51 dependency
order instead of repeating the failure or bypassing its gate. Same Heisenberg
confirmed0/0/0 that the three enumerated test co-Moves have independent
green-to-green boundaries; the actor stop does not forbid this disjoint Move.
Order review01a0847e-4284-7810-a3aa-b793693026bc, completed1788929459.

Main changed only continuation/tests/mod.rs728→330 and added its private
transactions.rs407. The original399-line block of16 complete tests is byte-
identical; eight prefix lines add compact module docs and use super::* only.
Helpers/imports/fields stay at the original Authority with no visibility,
production, signature, assertion, fault, time or resource-order change. FQ
names now add ::transactions:: beneath continuation::tests; names and
attributes/order are unchanged. No Knife/source guard needed modification.

Exact proofs:

- Original block SHA256: be42b7d8ec04905df876524082971a4cea0a60da50b0b531f10fbd80bfcdfbc9.
- Inverse full parent SHA256: 3d326389bdb370ee0be5a29bdcf3d0d8f35278a6e86da56bea029320739b5a89.
- Current parent SHA256: 4e77a325ff079628e00150223b529a16348aba6f625e56782c2583c155b0e678.
- Current child SHA256: 481f69e1d224e7b4fa9e743252e02a1385f9113b59664cb9fe55be0b95683020.

Actually executed by Main: fresh standalone baseline6/6; default and
all-feature continuation baselines each16/16, and their post-Move
::transactions:: runs each16/16. No zero-test or renamed-test substitution.
Whole fmt, tracked diff and new-child whitespace checks passed. The expected
standalone5/6 shrink stop named only728→330. Integrator Mencius lowered that
one entry, kept it because330 still exceeds the300 index limit, verified the
full inverse inventory and freshly compiled standalone with-D warnings:6/6.
Integrator submission01a08483-2b12-75e0-b9a3-1257b51be34b, completed1788929714.
Governance remains698, SHA7080f665718785fbed11974349a0861f6628dea9d2cdc074fed44d16674e900b;
its sole inverse restores prior7af97a6605f98bb48125169fbdc221887b6000523b304e9db2e2cbca6d8ff049.

Final Main command ran all-feature Cargo governance6/6, Knife162/162 and
repository provenance13/13; fmt/diff and offline all-feature lib check passed.
Existing22 vendored warnings and Knife source-fixture warnings remain. New-
file no-index whitespace exit1 with no output denotes addition, not a failure.
No production Rust, actor/test-index source, allowances or other inventory
changed. The preceding actor/index hashes and HEAD remain exact.

Same Heisenberg independently verified body/full-parent inverses, privacy,
unchanged helpers/test identity and sole ratchet:0/0/0 Approve, structure9/10,
no required additional optimization. Final evidence closure submission
01a08485-fc78-7ad2-87c7-d4de893c12a7 completed1788929785. Reviewer did not rerun
tests; runtime counts above are Main/Integrator evidence. No wording-only
review or new design sidecar was added.

This creates test growth room for the frozen real checked-caller migration;
it does not implement that migration or Bound source service. The remaining
two test co-Moves stay in joint51. Actor P1/Move remains paused pending its
separate narrow grant; no known or newly failing gate was waived. Full P3/
Case C/R7/Task4 remain open; no commit/push or external action occurred.

## 2026-09-09 — Independent initial-acceptance test co-Move: accepted

Main executed the second already-frozen joint51 test co-Move, without entering
the still-unauthorized actor cleanup repair. Exact feature writes:

- r7_initial_acceptance_settlement.rs1168→603: replace the complete41-line
  bind_real_initial_terminal helper with a private initial_composition route,
  and remove the four complete tests at original601–1124 (524 lines).
- New r7_initial_acceptance_settlement/initial_composition.rs574: compact
  eight-line prefix plus unchanged helper, separator and unchanged tests.
  Its helper's complete two-call closure moves with it; no pub/re-export.
- owner_session_controller/tests/capacity.rs434→438: only initial_physical's
  source route changes to concat!(actual parent,actual child). All original
  negative checks and other source domains remain identical. This permitted
  source-routing preservation was necessary even though absence-only checks
  could otherwise remain green while silently losing the moved source.

Four tests remain in the parent, including its existing production-source
guard. The four moved test FQs add ::initial_composition:: and preserve names,
attributes, assertions, order and runtime/terminal/lifetime behavior. No
production behavior, helper signature or actor/index file changes occurred.

Inverse proofs, independently checked by Main and the same Reviewer:

- Helper41 SHA: e47144e69ae2e48fc217c69949458edd4542dae68055cc9119f496bdc9f39ae6.
- Tests524 SHA: 5ff30d26e8afeaee708eb579fa5f9acbeee39be17fc7da9cd0c367bac1c6a9a4.
- Full original parent SHA: a04cdf301fe28b4aa875f27cecdff37f0ac73e3e5deaa91f925cce8f69fccb1a.
- Capacity routing inverse SHA: c8d6d885cb8861216c107505e86768bd0d4ae445753dca6426ea8e9b5ef4e1d5.

Actual tests: fresh standalone baseline6/6; initial-acceptance default and
all-feature baselines each8/8; explicit source guard baseline1/1. After Move,
initial8/8 and Controller44/44 pass separately in both modes (52/52 each).
All eight initial tests are counted, not merely the four in the new child.
No test body changed and no behavioral RED/GREEN or new capability is claimed.

The expected standalone5/6 stop required deleting the old1168 grandfather
entry because603 is below800. Integrator Mencius removed only its four lines,
verified the full inventory inverse and freshly compiled with-D warnings:
standalone6/6. Governance698→694, SHA
210949227cb98c2b69879c1666af0c9702e4976282618dbaf8e397910ecdcb85;
restoring the sole entry reproduces prior7080f665718785fbed11974349a0861f6628dea9d2cdc074fed44d16674e900b.
Integrator submission01a0848c-a88c-70e0-8867-3de03518c1cb completed1788930327.

Final Main gates: Cargo all-feature governance6/6, Knife162/162, repository
provenance13/13, fmt/diff and offline all-feature lib check green. New-child
no-index whitespace check had no output (exit1 for addition). Existing22
vendored warnings and Knife source-fixture warnings remain; no new warning
repair or broader full-library acceptance is claimed.

Same Heisenberg independently verified bodies, helper closure, FQ mapping,
source-domain inverse and final sole inventory removal. Final0/0/0 Approve,
structure9/10, no necessary optimization; closure submission
01a0848f-6136-78e0-8940-3b649319aed0 completed1788930418. Runtime tests were
Main/Integrator executions, not Reviewer reruns. No extra wording review.

Remaining co-Move is physical-attach's nine tests into its private scenarios
leaf, before checked-caller changes. Actor P1/Move still awaits its explicit
test-only scope grant; no failing gate was removed or relaxed. Bound source
service, full P3/Case C/R7/Task4 remain open. No commit/push/external action.

## 2026-09-09 — Independent physical-attach test co-Move: accepted

Third and final enumerated joint51 test co-Move, not actor cleanup or Bound
behavior. Feature writes only: r7_physical_attach_permit.rs1407→668 adds private
mod scenarios and removes the complete739-line block; scenarios.rs747 has an
eight-line compact-doc/import prefix and the unchanged block. All nine tests
(eight Tokio, one synchronous) retain names, attributes, bodies, ordering and
exact cleanup. Their FQs gain ::scenarios::. Fixtures and cleanup.rs stay put;
no helper visibility, production interface or behavior changes.

capacity.rs438→441 changes only attach_physical to concatenate the actual
parent and child. Its negative assertions and all other source domains remain
unchanged. Source-domain preservation is required even if absence checks alone
would stay green. Independent inverse proofs reproduce:

- Original parent SHA546985640dc3ac5eade4eac5ddfafd2f8c4427631b1d820b9e82c69b21d4642c.
- Original739-line block SHAc8ea3c52c6166ac035812b17ac6e71c362a8def96534b0443499856b9b031b3b.
- Prior capacity SHA950c6c49b5445f03a5d91122e9b5ea774b6200f8f1a38198a96f1429b98fd10d.

Actual evidence: fresh pre-Move standalone6/6; physical-attach before and after
each9/9 in both default/all-feature modes; original source guard before1/1;
Controller after44/44 each mode, so post related53/53 each. No test body changed
and no new behavioral RED/GREEN is claimed. The pre-ratchet standalone then
returned the sole expected5/6 failure: parent668 reached the normal800 limit,
requiring removal of its grandfather entry, not a lower exception.

Integrator Mencius alone removed that four-line1407 entry: governance694→690,
SHAffd6bb51d5a43c493ab8dfab1f37c2a42e0cf8a9c216e9f0f697467406d394b7.
Full inventory inverse reproduces prior210949227cb98c2b69879c1666af0c9702e4976282618dbaf8e397910ecdcb85.
Fresh rustc-D warnings compile and standalone6/6 pass; no stale pre-ratchet
binary is used as final evidence. Integrator submission
01a08498-d77d-7ef1-8666-14c3b967c099 completed1788931080.

Main final gates: all-feature Cargo governance6/6, Knife162/162, repository
provenance13/13, fmt/diff and offline all-feature lib check green. New-child
no-index whitespace check had no output; exit1 denotes the addition. Only
existing vendored22 and Knife source-fixture warnings remain. These focused
results are not a full-library green claim; actor cancellation P1 is unexcluded.

Same Heisenberg independently checked body/full-parent inverses, private
boundaries, source domains and sole inventory deletion: P0/P1/P2=0/0/0,
Approve, structure9/10. Closure submission01a08498-d792-75d1-bacf-5597db4a5afa
completed1788931151. Tests were Main/Integrator executions, not Reviewer
reruns. No necessary optimization or further wording-only review is required.

All three finite test co-Moves are accepted; checked-caller migration and Bound
source service have not begun. Actor production6769/index15818 retain their
frozen SHA824d7f8f08feab5523d249edc0834d92c8a10011551b3b5c32c2ce9c477c98a0 /
30bd586f2fa9008b94c9c3316eb509c2290870e8b198a22c911642594b476550.
The actor baseline repair still needs its narrow scope grant; the failing gate
is neither repaired nor waived. Full P3/Case C/R7/Task4 remain open. No
commit/push/external action occurred.

## 2026-09-09 — Canonical geometry preparation: accepted

This is the independent value-only preparation already frozen by joint22/51,
not the paused actor repair or complete inward metadata Move. Same Reviewer
confirmed the combined contract allows its prescribed new characterization
alongside the four existing field-read migrations: submission
01a0849e-8020-78e1-8ed0-58f72b26f033,0/0/0, completed1788931466.

Main changed exactly five Rust files: owned_upstream/mod.rs92→93 declares one
private owner_ready; owner_ready.rs55 owns ReadyGeometry/Error, checked
NonZeroUsize construction and three immutable capacity reads; server_root.rs
857→848 owns that one value instead of three capacity fields; scheduler.rs
687→690 borrows it instead of importing the outer ReadyProcessPlan; existing
process_plan.rs115→123 adds one characterization and migrates four field reads.
The effective new value scope is owned_upstream, with private fields and no
Clone/Copy, domain, reservation, publisher, allocator or mutable metadata.

Error order stays zero N, zero class budgets, checked4*N+1 thenN+1, total
budget, each class'sT thenQ. The real outer-plan N=MAX/4,budgets[MAX,1,1,1]
test directly GREENed1/1 before production edits with TotalBudgetOverflow;
4*N+1=MAX-2 fits, and later N*5 overflow must not preempt it. All original nine
plan cases remain. This is characterization, not a behavioral RED/GREEN.
Both inner foreign Layout checks remain unchanged; no allocation, custody,
failure return, callback lifetime, selector or notification order changed.

Full inverse hashes reproduce all four original files: module d2cfed1cdf49bb63d14bb4977faff2189722c3dd34e3f5b3c4217caef0b53319;
Root8a49b88b48b5b6fbb4d1c136545b69c139a37c649b9a7d6cc3b8a2344cd9bd84;
scheduler dba528a269d26c71b987dfe7a66ce2f6f46f61060cbb8c49f8a297b48306b975;
original tests3a45fd620b511ee69cd3a0d34665ebf583fa93d3aa2b38dc3f4ce336cb639f1a.
New geometry SHA06b9938480977826c12ce595cd5983205fa460bffda1cde7dd80139b4613d594.

Actual tests: fresh pre-write standalone6/6; before Root56/56 separately in
default/all-feature modes; post plan10/10, Root57/57 each mode, plus all-feature
Root57/57 over ten serial rounds. First fmt reported only two current-file
wrapping hunks; the exact format-only in-scope repair passed. Final fresh
rustc-D warnings standalone6/6, Cargo governance6/6, Knife162/162, repository
provenance13/13, fmt/diff and offline all-feature lib check pass. New-file
no-index whitespace check is silent; exit1 is its addition. Existing22 vendored
and Knife source-fixture warnings remain. No full-library green is claimed.

No oversized file changed, so Integrator inventory action was unnecessary:
governance remains690/SHAffd6bb51d5a43c493ab8dfab1f37c2a42e0cf8a9c216e9f0f697467406d394b7.
Same Heisenberg independently verified code, inverse hashes and boundaries;
final closure01a084a6-1581-7650-878e-30a6e759cabc completed1788931883:
P0/P1/P2=0/0/0, Approve; Module/Clean/System/DDIA9/9/8/8. Runtime evidence is
Main's execution, not a Reviewer rerun. No wording-only review is required.

Next permitted metadata preparation is separate characterization/removal of
the two dominated foreign Layout checks, then complete inward placement and
its original test closure. Actor baseline P1 still awaits its narrow grant;
actor6769/index15818 remain unchanged. Bound service, full P3/Case C/R7/Task4
remain open. No commit/push/external action occurred.

## 2026-09-09 — Layout coverage stop; first three metadata recipes accepted

Read-only Layout audit changed the next action: current six from_commands
test call sites expect success; the ten plan cases do not exercise its cold
OwnerServerBuildFailure return. The actual call graph still proves domination:
outer CommandResident Layout and successful same-N Waker reserve precede the
sole ReadyIndex::new call. This does not replace section17.4's required
validation/error-order and exact-input-return characterization.

Same Reviewer scope verdict0/1/0, submission01a084aa-dc71-7f23-8a8f-5ee2702d2605
completed1788932311: existing listed paths are not an unrestricted new test
axis. Smallest pending exception is adding real cold rejection/return cases
to existing process_plan.rs while retaining its ten cases. No exception or
Layout deletion occurred. Mencius read-only feasibility audit completed
1788932368, submission01a084aa-dc83-7420-99da-8de5a11652cf. It identifies real
InvalidResident-before-Layout and Layout-before-Reserve witnesses using legal
checked N above the CommandResident Layout limit and a small actual command
Vec. Verify actual request/Full Box, Vec allocation/order, sole observer and
all plan values on return; resume real command/receipt handling afterward.
This needs no large allocation or production hook and cannot prove legal-
layout allocator-OOM coverage. Fixture placement follows the eventual scope
grant, not an unauthorized cross-module helper or copied Root.

Independently authorized section19 preparation did execute. Main changed only
scheduler/tests/mod.rs15→85 and its ready_dispatch/publisher_fatal.rs747→708.
The first three existing scenarios now call eight fixed test-only ReadyIndex
methods: source-counter arm/after-dequeue assertion; process-counter arm/
after-dequeue assertion; and pre-poison, actual guarded panic, post-poison and
post-close observations. Methods take only &self and return unit; their common
test-caller visibility is confined by the private cfg(test) tree. No metadata,
guard, pointer, new Arc, claim, fixture ownership or arbitrary setter escapes.

All original statements move at the same causal points. The guarded panic is
inside the original catch_unwind and keeps the original payload; real poison
persists. Dequeue/poll/gate checks, request/Full pointers, receipt reduction,
repeat counts and close stay outer. Every native Wake/Weak/Arc implementation
and the remaining six scenarios are byte-identical after the untouched suffix
starting PoisonOnFirstReceiveWake. No test name/FQ/count changed.

Main and independent review inverse-check the full publisher file to original
SHAba2c87b51f779042e495b5640bf29d8c8032334f4a38c678a094ef0581b32b6d;
the original index SHAefce50b9d470ea4342da16007f157c9c75305661761bfb2032de9725bba466cc.
Current hashes are cb335177ef019490436681730860e8199b8bd58fcffb403b080453a80d1a2c18
and3da5c7d70f8548ee2e458bfbc1599808040a415f5a18f75b76b6a5171b8eb1ac respectively.
One preparation checker stopped before writes because an assertion also occurs
in a later scenario; exact function-prefix scoping resolved the check. There
was no Rust/compiler/test RED and no behavior repair disguised as a Move.

Actual evidence: fresh pre-write standalone6/6; before and after both-mode
Root57/57; focused publisher9/9; all-feature Root57/57 over ten serial rounds.
Final fresh rustc-D warnings standalone6/6, Cargo governance6/6, Knife162/162,
repository provenance13/13, fmt/diff and offline all-feature lib check pass.
Both untracked changed files pass whitespace/newline and expected-source
checks. Only existing warnings remain; no whole-library green is claimed.
No oversized file/inventory changed. All production hashes, actor6769/index
15818 and governance690 remain frozen; no commit/push/external operation.

Same Heisenberg final closure01a084b6-49c0-7650-9cdc-81fd3986c1e8 completed
1788932941: P0/P1/P2=0/0/0, Approve; Module/Clean/System/DDIA9/9/8/8. Reviewer
independently inspected source/inverses/lifetimes, not runtime reruns. No
wording-only review needed. Six private recipes, complete metadata placement,
Layout test-scope exception, actor repair authority, Bound and full P3 remain
open. This local acceptance does not close either separate scope blocker.

## 2026-09-09 — Two authorized cold-return characterizations accepted

The user confirmed the latest narrow question: add exactly two real cold
constructor rejection/input-return cases to existing process_plan.rs while
retaining its ten cases. This did not authorize actor repair or inner Layout
deletion in this slice. Main is the sole Writer; same Heisenberg is read-only.

Only Rust change: server_root/tests/ready_scheduler/process_plan.rs123→448,
under800. Four private leaf helpers assemble actual OPEN requests, check all
plan values, borrow the original Full Box address/content and receive/reduce
actual completions. No production, other test, metadata recipe, route,
visibility, dependency or governance inventory changed. Original ten test
bodies reconstruct byte-for-byte to SHA
1cd5f8a0e1606e2912e1cbd789fc9244e06c68cac40279905518f9baa416ce64.
Current leaf SHA7440bad47fb709638cf7c5184bdc985fe7acf9f60234d5ffa03b828fa314695a.

The incremental characterizations each directly GREEN on unchanged production:

- cold_root_rejects_duplicate_residents_before_layout_and_returns_exact_inputs:
  same real Model, three distinct OPENs, Fresh and Full residents sharing the
  actual SessionId. An already-invalid CommandResident Layout must lose to
  InvalidResident; the original Vec allocation/order, Fresh origin/event and
  exact Full Box/content return unchanged.
- cold_root_rejects_invalid_layout_before_reserve_and_returns_exact_inputs:
  one valid Full resident, checked N=isize::MAX/size_of::<CommandResident>()+1,
  actual input Vec capacity3. Layout wins before the resident reserve. N is a
  value-only witness; no huge valid allocation or allocator-OOM proof occurs.

Both compare every plan field before/after rejection, retain Busy exclusive
observers and blocked Full eligibility, then actually receive/reduce prefill,
rearm the returned command, send it once and reduce its original receipt.
Fresh also sends once. Empty cells/inboxes and observer release after command
destruction are explicit; this is command custody, not physical whole-zero.

Actual commands/evidence: pre-write fresh rustc-D warnings standalone6/6;
cargo test --offline [--all-features] --lib
'owned_upstream::quinn_leg_composition::owner::server_root::'
-- --nocapture --test-threads=1 is before57/57 and after59/59 in both modes.
Each new full-FQ exact test ran1/1 before proceeding; all-feature Root59/59
then ran ten serial rounds (590 passes). Final fresh standalone6/6;
Cargo offline/all-feature code_growth_governance6/6,
knife16_owned_leg_provenance162/162 and repository_test_module_provenance13/13;
cargo fmt --all -- --check, git diff --check and
cargo check --offline --all-features --lib pass. Only existing warnings remain.
No full-library green claim. Scratch binaries:
/tmp/mini-vpn-p3-cold-rejection.tTu0PI/governance and governance-final.

All348 enumerated Rust source hashes differ only at the authorized test leaf;
its untracked whitespace/final newline pass. The first inverse comparator
matched all4126 original characters but added a separator newline. Selecting
the exact new-function separator restores4126/4126 without normalization or
source edits; preserve that checker correction, not a fictitious Rust RED.

Same Heisenberg review submission01a084c4-f917-7223-9bec-7b35416d0179 completed
1788934076: P0/P1/P2=0/0/0, Approve; Module/Clean/System/DDIA9/9/8/8. Independent
source/inverse review, no reviewer Cargo rerun; Main runtime evidence included.
No necessary optimization or wording-only rereview. The limited test-scope/
coverage P1 closes. Both inner Layout checks, six remaining metadata recipes,
actor repair authority, actual Bound service, full A3 Layout and P3 remain
open. No commit/push/external action; HEAD remains532d766.

## 2026-09-09 — Remaining six metadata recipes accepted; all nine prepared

Previous goal turn was progress: two real cold-return tests closed the limited
scope/coverage P1. This turn implements the already-frozen section19–20 six
remaining metadata-private recipes; no actor scope grant is inferred from the
automatic goal continuation. Main is sole Writer; Heisenberg remains the same
independent read-only Reviewer. No new architecture sidecar or production gate
is substituted for the first complete Bound path.

Actual Rust changes, all existing private scheduler test files:

| Path below server_root/scheduler/tests/ | Before -> after |
|---|---|
| mod.rs | 85 -> 135 |
| ready_dispatch/publisher_fatal.rs | 708 -> 714 |
| ready_dispatch/publisher_fatal/notify_unwind.rs | 175 -> 187 |
| ready_dispatch/publisher_fatal/park_fatal.rs | 217 -> 233 |
| ready_dispatch/publisher_fatal/source_unknown.rs | 255 -> 355 |

Every file stays under its normal limit; no ratchet edit or new Rust path.
ready_dispatch.rs530, process_plan.rs448, every production source and
governance690 are unchanged. Full348-source hash comparison finds only these
five deltas and no deletion. Original12 tests/FQs remain unchanged.

Each extraction ran its original all-feature exact test1/1 before continuing:
poisoned_metadata_source_unwind_retains_claim_and_mailbox_disposition;
first_published_poison_survives_later_process_revision_exhaustion;
publisher_fault_during_send_preserves_admitted_completion;
notify_unwind::committed_publisher_fault_survives_root_notify_unwind;
park_fatal::fatal_during_cached_waker_replacement_prevents_repark;
source_unknown::healthy_source_unwind_quarantines_only_its_resident.
Commands use cargo test --offline --all-features --lib with each full prefix
owned_upstream::quinn_leg_composition::owner::server_root::scheduler::tests::
ready_dispatch::publisher_fatal:: and -- --exact --nocapture --test-threads=1.
These are green-to-green characterizations, not new behavior RED/GREEN.

Only expected tuples/epoch values leave metadata observations. Actual claim
comparisons are inherent ReadyClaim operations, avoiding any production type
visibility widening. Original claim address/slot/lane checks remain outer;
healthy claim comparison occurs only before recovery. Healthy-source per-slot
resident then metadata assertions keep the original A/B order; fixed test
operations share a private assertion implementation, not a generic public
state API. Guard release, phase/word/mask/cursor/tree observations, first-cause
checks and dirty-epoch readback preserve their original causal points.

Three native metadata-aware Wakes retain their exact implementations, Weak
references and original Arc construction sites/lifetimes. Poison's custom
wake_by_ref and guard-held panic stay; Notify/Healthy keep default wake_by_ref
and release guards before their original panic. Mailbox-only Wake/Drop bodies
also match byte-for-byte (seven total implementations); receipt/repeat/close,
Context, strong_count and try_unwrap order stay outer. No extra allocation,
callback, guard escape, counter getter, copied claim or cleanup wrapper.

Fresh pre-write standalone6/6 and both-mode Root59/59 passed. Post focused
publisher9/9, default/all-feature Root59/59 and ten all-feature Root59 rounds
(590 passes) passed. Final fresh rustc-D warnings standalone6/6;
Cargo offline/all-feature code_growth_governance6/6,
knife16_owned_leg_provenance162/162, repository_test_module_provenance13/13;
fmt/diff and offline all-feature lib check green. Only existing warnings;
no full-library, throughput, physical settlement or whole-zero claim.

Data-only evidence /tmp/mini-vpn-p3-six-recipes.de4a9k/before.json captures the
pre-write sources/hashes; transforms.json records all six inverse substitutions
and formatted expected-source checks. Each full inverse passes, and actual five
files exactly equal rustfmt of the proposed transformations. Untracked source
whitespace/final-newline checks pass. First inverse validator could not invert
an empty import replacement; its exact test-function anchor fixes the checker.
A later read-only audit command had one parenthesis syntax error. Neither
checker failure changed Rust or triggered a runtime RED; both were corrected.

Final SHA-256 in table order:
5e33c37bfe278dbbbbdd37277d97b87b7e8809f2a56d6ad79acedbec4c4d8589;
a5c330ee351369e8fb64d0d07f09ece5985ae77adcb5ed23b641f8aa4598b7b3;
fe8e51f70883a9f5cf4ba2925f89571162208c6847924d0c933a21f214c3f652;
b88bbe0da5ed10a0d1652ab38d511ec7a253800104fbc5558dbd04c06028fb26;
f16d59ab58d5faff34da40dbd7c0470f66369771b2ce54362ffff97876c67c2e.

Same Heisenberg submission01a084d5-1c79-7110-b6c9-20ce85caa59f completed
1788935365: P0/P1/P2=0/0/0, Approve; Module/Clean/System/DDIA9/9/8/8. Reviewer
independently recomputed inverses, scopes/claims/causal order and source hashes;
Main runtime evidence included, no reviewer Cargo rerun. No necessary repair
or optimization, and no wording-only status rereview required.

All nine metadata-private recipe preparations now close. Next separate gate is
the12 complete outer-root scenario/fixture Move while metadata recipes remain
inward; then remaining characterized Layout/metadata placement and actual Bound
implementation. Actor P1/repair authority, full A3 Layout, Bound/full P3, Case C,
R7 and Task4 stay open. No commit/push/external action; HEAD remains532d766.

## 2026-09-09 — Twelve complete Root scenarios moved outward and accepted

This is the frozen joint17 step3 / joint51 pure Move, following the accepted
nine metadata-private recipes. Main is sole Writer. Same Heisenberg first
confirmed that the five named outer destinations plus private-index routing
permit the necessary Root tests/mod.rs declaration; no new visibility or
write-scope exception was invented. No actor repair permission is inferred.

The new code-aware test
owner_bound_service::owner_root_dispatch_scenarios_have_one_outer_test_route
first compiled and failed exactly: Root tests/mod.rs had0 direct external
ready_dispatch declarations, expected1. It checked the route before reading
the missing destinations; no missing-file/privacy/compiler surrogate RED.
The same exact test passed after the Move.

Actual Rust placement, paths relative to server_root:

| Source | Before -> after | New outer destination / lines |
|---|---|---|
| tests/mod.rs | 1 -> 2 | private ready_dispatch declaration |
| scheduler/tests/mod.rs | 135 -> 124 | Root/command/protocol imports move outward |
| scheduler/tests/ready_dispatch.rs | 530 -> 7 | tests/ready_dispatch.rs / 540 |
| scheduler/tests/ready_dispatch/publisher_fatal.rs | 714 -> 150 | tests/ready_dispatch/publisher_fatal.rs / 572 |
| publisher_fatal/notify_unwind.rs, under the preceding scheduler tree | 187 -> 69 | outer corresponding child / 120 |
| publisher_fatal/park_fatal.rs, same | 233 -> 48 | outer corresponding child / 188 |
| publisher_fatal/source_unknown.rs, same | 355 -> 190 | outer corresponding child / 167 |

Repository provenance root418->421 only adds its private explicit test-root
route; the new owner_bound_service.rs is153 lines. Scanner is unchanged.
The old root file's full inverse is exact. The new tracer locks the five
private/unconditional/default-path outer routes and the12 designated direct
test declarations uniquely within the actual Root source tree, including
inward tests. It bans source include! there, not unrelated test-module shapes,
and does not claim whole-repository or arbitrary cfg/macro semantics.

All12 FQs change only by this prefix substitution:
owned_upstream::quinn_leg_composition::owner::server_root::scheduler::tests::ready_dispatch::
becomes
owned_upstream::quinn_leg_composition::owner::server_root::tests::ready_dispatch::.
Their exact suffix inventory is:

- ready_session_enqueued_during_cursor_handoff_is_not_lost
- scheduler_claim_corruption_stops_all_residents_without_discarding_commands
- unrelated_lane_rearm_registers_remaining_full_before_root_parks
- publisher_fatal::publisher_revision_exhaustion_closes_admission_before_another_resident_send
- publisher_fatal::process_revision_exhaustion_is_published_before_root_selection
- publisher_fatal::slot_poison_is_published_before_root_selection
- publisher_fatal::poisoned_metadata_source_unwind_retains_claim_and_mailbox_disposition
- publisher_fatal::first_published_poison_survives_later_process_revision_exhaustion
- publisher_fatal::publisher_fault_during_send_preserves_admitted_completion
- publisher_fatal::notify_unwind::committed_publisher_fault_survives_root_notify_unwind
- publisher_fatal::park_fatal::fatal_during_cached_waker_replacement_prevents_repark
- publisher_fatal::source_unknown::healthy_source_unwind_quarantines_only_its_resident

All49 original top-level fn/impl/struct/enum items are byte-identical and
uniquely retained, including12 complete test bodies and7 native Wake/Drop
implementations. Metadata recipes, their effective scopes and actual borrowed
claims remain unchanged inward. Real Root/mailbox/command helpers, receipt
reductions, Dequeue callbacks, catch scopes, guard releases and Arc/Context
lifetimes stay with the complete outer scenarios. No getter, re-export,
production visibility, callback or lifecycle authority was added.

Before-write fresh standalone governance6/6, default/all-feature Root59/59
and repository provenance13/13 passed. After Move, all-feature outer12/12
passed without an unpredicted repair. The new exact provenance1/1,
default/all-feature Root59/59 and ten all-feature Root59 rounds (590 passes)
all passed. Old scheduler test-prefix --list reports0 tests: this is removed-FQ
inventory, not a successful runtime regression.

Final executed commands use --offline --all-features for Cargo tests/check:
Knife16 provenance162/162; repository provenance14/14; code-growth6/6;
fresh rustc -D warnings --edition=2024 standalone code-growth6/6;
cargo fmt --all -- --check, git diff --check and cargo check --lib all green.
Tests use --test-threads=1; the exact route tracer also uses --exact --nocapture.
Only existing smoltcp22 and Knife scanner142 warnings; those sources unchanged.
All50 untracked paths passed safe whitespace checks. This is not a full-library
or physical lifecycle/throughput/whole-zero acceptance.

Data-only baseline /tmp/mini-vpn-p3-outward-tests.5GQ63I/before.json and
transform.json hold complete pre-Move sources/SHA inventory and intended
transforms. All12 resulting scenario/metadata files exactly match rustfmt
stdout of those transforms. Whole Rust inventory348->354 has exactly8 existing
path deltas and6 additions, no deletion. Production, Actor6769/index15818,
Root848/scheduler690, process_plan448, scanner and governance690 hashes match
the baseline. No grandfather shrink or Integrator edit is needed.

Same Heisenberg submission01a084ea-fba2-79a1-af38-1dec0c784fdd completed
1788936584: P0/P1/P2=0/0/0, Approve; Module/Clean/System/DDIA9/9/8/8.
Reviewer independently confirmed all49 items, exact transformations, unique
routes, old-provenance inverse and scope; Main's terminal gate evidence was
included, no Reviewer Cargo run. No necessary patch or optimization and no
wording-only status-flip rereview.

Outward placement closes; next separate gate is the two dominated foreign
Layout-check removals, then complete metadata placement. Actor test-repair
scope, actual Bound service/full A3 Layout/full P3, Case C/R7/Task4 remain
open. No production behavior, docs-only architecture detour, commit/push or
external action; HEAD remains532d766.

## 2026-09-09 — Dominated foreign Layout checks removed and accepted

Previous goal turn was progress: the12 outer scenario Move closed. This turn
executes joint15/17 step4/22/51's separate characterized refactoring, not new
capacity behavior or a raw unchanged-block Move. Main uses refactoring-patterns
and TDD's green-to-green phase; standing causal repair authority does not grant
the still-pending actor test scope. No further prerequisite sidecar is added.

Fresh source audit finds exactly one ReadyIndex::new call across src/tests:
server_root.rs675, inside OwnerServerRoot::from_commands. Its actual ordering:
resident validation -> Layout<CommandResident>(N) at645 -> other outer Layouts
-> residents/initial/wakers/identities try_reserve_exact(N) -> ReadyIndex::new.
The Waker vector's real type follows capacity_waker()->Waker and its actual
resident use, not a copied capacity witness. Its successful reserve dominates
the inner Waker Layout; the outer explicit resident Layout dominates the
inner CommandResident check. Earlier Reserve errors remain earlier. No new
caller, allocation, copied witness or relocated validation is introduced.

Only scheduler.rs changed:690->688, deleting exactly the adjacent
Layout::array::<super::CommandResident>(n) and Layout::array::<Waker>(n) lines.
The entire remaining file equals its baseline byte-for-byte. Initial masks,
N*2/N*SOURCE_COUNT, concrete Mutex<ReadySlot>, word-count/tree/atomic Layout,
allocation and failure ordering remain. The outer constructor, exact return
payload and all tests are untouched. Normal size limit applies; no ratchet edit.

Falsifiers checked: another caller would invalidate this narrow domination
proof; a changed early cause/returned Vec/Full Box/observer would fail the real
cold-return tests; an altered remaining branch would fail the full-source
inverse. None occurred. New cold consumers still must validate actual
foreign storage at their owning boundary. This is not full cold-peak/RSS,
legal-layout allocator-pressure coverage or complete A3 Layout acceptance.

Pre-write fresh standalone code-growth6/6 passed. Both existing real tests
ran all-feature exact1/1 before and after:
cold_root_rejects_duplicate_residents_before_layout_and_returns_exact_inputs;
cold_root_rejects_invalid_layout_before_reserve_and_returns_exact_inputs.
Their unchanged suffix prefix is
owned_upstream::quinn_leg_composition::owner::server_root::tests::ready_scheduler::process_plan::.
Both exercise original command payloads/Vec and Full Box identity, retained
exclusive observers, real receive/rearm/send/reduction and final release.
They do not allocate the large invalid requested capacity.

Before/after plan Module12/12 and default/all-feature Root59/59 passed.
After removal, ten all-feature Root59 rounds (590 passes) passed.
Final Knife16 provenance162/162, repository provenance14/14,
fresh rustc -D warnings --edition=2024 standalone growth6/6 and
Cargo growth6/6 passed; fmt/diff and all-feature lib check green.
Cargo commands were offline; tests used --test-threads=1 and exact cases
also --exact --nocapture. Only existing smoltcp22/Knife142 warnings.
All50 untracked-path whitespace checks passed. No synthetic expected RED
is claimed for this already-characterized behavior-preserving deletion.

Data-only /tmp/mini-vpn-p3-layout-removal.9KWmDL/before.json captures the
baseline scheduler and all354 Rust hashes. Final354-source comparison finds
exactly one changed path and no additions/deletions; all tests, Root848,
owner_ready55, Actor6769/index15818, scanner/provenance and governance690
are unchanged. Final scheduler SHA-256:
b90de3669773fc66b4e15af4b7fc98b5f24e5bdc4805f17c9cf226c03bba7ab8.

Same Heisenberg source review and final evidence closure
01a084f4-d0ab-7be3-8092-06d5f0330c34 completed1788937045:
P0/P1/P2=0/0/0, Approve; Module/Clean/System/DDIA9/9/8/8.
Main executed the tests; Reviewer independently reviewed source, not a Cargo
rerun. No necessary repair/optimization or wording-only status-flip rereview.

Section17 step4 now closes; next is the complete prepared metadata Move into
owner_ready, with its finite private recipes and unchanged real outer callers.
Actual Bound/full P3, full A3 Layout, independent actor repair scope,
Case C/R7/Task4 remain open. No commit/push/external action; HEAD532d766.

## 2026-09-09 — Complete metadata Implementation moved inward and accepted

Previous goal turn was progress: the separately characterized two Layout
deletions closed. This turn executes joint17 step5/51, not a new design phase
or partial metadata wrapper. Main is the sole Writer; same Heisenberg reviews.
The still-pending actor test-repair scope is neither inferred nor bypassed.

Two bounded pre-write symbol questions were resolved from existing §51:
the Root private direct-use alias is permitted, and the two existing
ReadyClaim slot/lane fields are already read by production Root callers.
Their pub(super) after movement reaches owned_upstream, the frozen narrowest
common ancestor; no pub(in sibling) is possible or invented. All original
private key/revision/state fields stay private. This creates no new getter,
re-export, pass-through Module or field exposed merely for tests.
Same Reviewer preflight01a084f8-39f0-75b1-835b-73815d9538a2 completed1788937352,
0/0/0 on both questions, not advance approval of the actual Move.

Actual production changes:
owner_ready.rs55->735 now holds the complete original geometry and metadata;
server_root.rs848->846 only removes mod scheduler and directly imports
owner_ready under the private scheduler name. Original declaration/impl bodies,
call expressions, Root policy, allocation/claim/finish/park ordering and actual
selector ownership are unchanged. The former688-line scheduler.rs is removed.
There is one implementation/failure gate, not a copied second scheduler.

Six metadata-private recipe files move verbatim from
quinn_leg_composition/owner/server_root/scheduler/tests/ to owner_ready/tests/:

| Relative suffix | Exact lines |
|---|---|
| mod.rs | 124 |
| ready_dispatch.rs | 7 |
| ready_dispatch/publisher_fatal.rs | 150 |
| ready_dispatch/publisher_fatal/notify_unwind.rs | 69 |
| ready_dispatch/publisher_fatal/park_fatal.rs | 48 |
| ready_dispatch/publisher_fatal/source_unknown.rs | 190 |

All six old files are removed along with scheduler.rs. They remain recoverable
from the data-only baseline and their unchanged new sources. No directory-wide
cleanup or unrelated deletion occurred. These files contain the fixed recipes
and native Wakes, not relocated Root test bodies. All59 outer tests and their
FQs remain unchanged in this Move; unlike the prior outward Move, no runtime
test filter name changes.

The existing provenance child153->352 adds exactly one structural tracer:
owner_bound_service::owner_ready_metadata_has_one_inward_implementation_route.
It uses already-present syn plus the unchanged scanner, not a new parser.
Its first all-feature exact command compiled and failed solely at line130:
owner_ready's direct ReadyIndex declaration count0, expected1. The existing
owner_ready file was parsed before any future child read, so this was the
frozen absence-of-ownership RED, not a missing-file/privacy compiler error.
After the complete Move, the same exact command passed1/1.

The tracer locks15 original direct/unconditional type declarations, their
frozen visibility and private storage (except the two existing claim values),
one private direct inward alias, the positive-cfg private default tests route,
all six private recipe-file routes and removal of the old Module/seven paths.
It distinguishes legal use-as-scheduler from forbidden mod-scheduler rather
than banning the word. Old outer12-source assertions and their helpers are
byte-identical. No scanner/governance/dependency changes or arbitrary-macro/
whole-repository-semantic claim.

Fresh pre-write standalone growth6/6, both-mode Root59/59 and repository14/14
passed. Post structural1/1, outer actual scenarios12/12, default/all-feature
Root59/59 and ten all-feature Root59 rounds (590 passes) passed.
Final Knife162/162, repository15/15, fresh rustc -D warnings standalone
growth6/6, Cargo growth6/6, fmt/diff and all-feature lib check all passed.
Cargo commands were --offline; test runs were --test-threads=1, with
--exact --nocapture for the structural tracer. Only existing smoltcp22 and
Knife142 warnings. No unpredicted compile/runtime repair was needed.
All49 final untracked paths passed safe whitespace checks.

Data-only /tmp/mini-vpn-p3-metadata-move.RQmZah/before.json captures354 Rust
hashes and complete relevant sources; transform.json holds intended content
and the six old/new mappings. Full original geometry/688-line metadata/Root
inverses pass. All24 original implementation items match byte-for-byte as a
multiset, and all six recipe files match completely. Every resulting file
equals rustfmt stdout of its intended transformation.
Rust inventory354->353 has exactly3 existing deltas,6 additions,7 removals;
all outer test files, Actor6769/index15818, scanner/provenance root and
governance690 remain unchanged. No file crosses a normal growth limit or
needs an Integrator ratchet.

Final owner_ready SHA-256:
af53cf3237f50160ecb6d75c7bf1a4406e38101ee3c47f27ed11a402241e8853.
Final Root SHA-256:
0a5d5029d92f03629941dcc319b12c6d196640417a2f28444fcd2e4a10c7d76b.
Six recipe hashes remain their accepted outward-Move values.

Same Heisenberg code closure01a08500-687a-73d1-aa9a-5ca539c4af3a completed
1788937935: P0/P1/P2=0/0/0, Approve; Module/Clean/System/DDIA9/9/8/8.
Reviewer independently reconstructed full inverses, hashes, private routing,
scope and old provenance retention; Main runtime evidence was included,
no Reviewer Cargo rerun. No necessary repair or wording-only status rereview.

Current documentation now names this accepted boundary and the remaining
already-frozen SessionCommands/35-test inward migration before A1–A4 Bound
implementation. Four joint19 scenario links follow the actual outer files;
their historical numeric spans remain explicitly pre-Move references.
The old claim that A1–A4 still needs another freeze is corrected in STATUS
using the accepted joint51 result, not a new design amendment.
Actor test scope, actual Bound/full P3/full A3 Layout, Case C/R7/Task4 remain
open. No commit/push/external action; HEAD532d766.

## 2026-09-09 — Command Move executability review: two finite plan gaps

The previous metadata Move is still accepted. This turn performs only bounded
symbol/caller/allowed-scope inspection and the same independent review; no
Rust, Cargo, test, formatting, inventory or external action occurred.

Same Heisenberg submission01a08508-ddfa-7082-8705-56c46c4a7a63 completed
1788938902, P0/P1/P2=0/2/0 Request Changes. This verdict targets the claim that
the full command/35-test inward Move can already be a standalone GREEN, not
the accepted metadata or earlier runtime results.

P1-1: Root604–670 still reads private commands for cold admission and initial
masks; Root724–734 quarantines metadata before commands and resets in-flight
state only in that successful order. The extracted selected visit at365 uses
outer private SessionCommandVisit/OwnerReadyUnit values. Moving everything to
private owner_target/commands requires actual nominal/result/operation closure;
the cfg(test)-only Prototype cannot serve those production callers.
Keeping the existing non-enum CommandResident was explicitly considered, not
rejected merely because Current service is absent. It can support local
extraction, but does not prove the complete Move: concrete inward nominal
placement, complete cold/quarantine operations and an authorized parent shrink
are still missing. owner_target9877 is a ratchet, not available line budget.
No public child/field, reverse Root import or temporary production Prototype
is approved as a workaround.

P1-2: outer ready_dispatch::full_pointer at123 and process_plan's
returned_cold_command at171 capture the original real Returned Box address
before the operation; the latter also validates it after rearm. The listed
full_control_is(pointer) returns unit and requires an existing pointer, while
drive_control_to_full preserves the original value disposition. Neither
provides the missing original observation. Keep original Vec/Box identities,
capacity/order, NoRunnable, independent rearm/send, receipts and observer Drop
order. A post-operation address compared with itself is not equivalent.
The35 inward command tests themselves are14+5+9+7 and have no production Root
dependency; the missing signatures concern retained outer Root/plan callers.

Decision: withdraw the mistaken independent-GREEN claim in current navigation
and joint51, without inventing a new sidecar or extending write authority.
Complete command migration must close with actual Current/Bound GREEN in the
same A1–A4 packet unless a concrete intermediate interface is separately
accepted; none is accepted now. Resolve these two finite placement/signature
findings there. An extra local extraction alone does not remove either blocker
and is not promoted as first-path completion.

Immediate next executable prerequisite requires the pending Actor test-only
scope grant recorded at the actor baseline result above: the old blocked-write
test's cancel/join callsite plus one private cleanup leaf. After authorization,
its exact regression/repeats, related gates and same Reviewer must close before
the characterized Actor Move. Do not reinterpret this review or automatic goal
continuation as that grant, exclude the red baseline or repair production join.

Only the existing joint/result, STATUS/HANDOFF/TODO and current learning/error
records are corrected. This is a next-gate correction, not Rust functionality,
runtime progress, rejection of prior acceptance or completion of full P3.
No commit/push; HEAD remains532d7660e3b862849aa7650589b182fd491df212.
Final checks: all353 current Rust hashes unchanged; seven edited records have
valid final newline/no trailing whitespace,48 local links resolve and scoped
git diff --check passes. These are documentation/scope checks, not test runs.

## 2026-09-09 — Actor fixture settlement prerequisite locally accepted

The previous goal was blocked awaiting the finite test-cleanup scope. The user
then reset the goal to continue and attempt full P3, explicitly directing
self-repair where possible. Main announced the original narrow fixture repair
and the subsequent causal test-only changes; this packet neither grants a
production join rewrite nor changes the full P3 completion requirements.
Main is the feature Writer, Mencius edits only the exact ratchet, and the same
Heisenberg reviews. No new architecture sidecar or test exclusion is created.

Fresh pre-write standalone governance passed6/6. The unchanged exact
data_writer_cancel_interrupts_an_actually_pending_ordered_quic_write compiled
and reproduced ResidualOwnership{messages:1,bytes:0}. The first test Adapter
used the real reported LocalCancelled terminal to retire its exact undelivered
wait; the original exact test then passed1/1 and ten repeats. No production
change was necessary, and its original real-QUIC Pending/no-DELIVERED proof
remained. Initial helper formatting was the only feedback-loop correction.

Related regression exposed three additional old cleanup assumptions:

| Original test suffix | Actual RED | Required disposition |
|---|---|---|
| r7_arrival::queued_b_ack_before_deadline_beats_delayed_root_expiry_and_keeps_actor_arrival_time | server B join1/0; group7/8 and exact0/1 | After all time assertions, the existing live helper waits for actual reverse DELIVERED and retires the FIFO wait before shutdown. |
| child_fatal_is_published_before_a_queued_ordered_record_can_acknowledge | join1/60, twice | Retain the queued, never-submitted frame until the real FatalIo terminal validates its exact FIFO cleanup. |
| closing_rejects_actor_ports_before_cancel_can_schedule_and_joined_stays_distinct | client join1/60 | Settle the queued frame under real LocalCancelled, retaining the original parallel peer join and port-state assertions. |

ProbeFresh does not itself prove the sender already received reverse DELIVERED.
The arrival repair adds one existing-helper call only; all deadline, arrival,
freshness, exact-key and peer cleanup assertions stay unchanged. Its exact test
and ten repeats pass, and both default/all-feature arrival Modules pass8/8.

The third inventory class triggered structural convergence rather than a copy
of the first helper. Same Reviewer execution freeze01a0854f-e595-71f1-8f7c-
06aa3474ea35 completed1788943203 at0/0/0. First, the complete already-GREEN
blocked cleanup body was behavior-preservingly extracted into its private
operation; pre/post exact tests each passed1/1. The existing fatal RED then
closed through the same operation. Closing added the third finite expected
case without another transaction implementation. No new production seam,
callback/provider, optional authority, dynamic dispatch, polling loop, sleep,
synthetic terminal or synthetic delivery was introduced.

The private Module now owns three fixed entries and one complete settlement:
first bounded join must fail with exact1/0 or1/payload; then the existing
runtime->LegIo->sole FIFO transaction checks the actual terminal reason,
None/Some original stamp and independently checked frame/byte/wait inventory.
Every precommit rejection returns the complete reported envelope. Only the
existing terminal cleanup guard commits. Second bounded join retains the
actual result/failure and original same-source stamp; its observation time
must match the handoff stamp. The real non-Clone terminal returns to the caller.
Joined's zero retained-terminal count is therefore not whole-fixture zero.

The fatal caller still compares the real retained first-cause address and
checks that no DELIVERED escaped. Closing still joins both sides concurrently
under its original outer timeout, checks the peer cause, and distinguishes
Closing from AlreadyJoined. A final rustfmt-only correction restores the
original separator while formatting the shorter join expression; the index
stays15814. The post-format Closing exact test passes1/1.

Final changed Rust files:

| Path below src/owned_upstream unless noted | Lines |
|---|---|
| quinn_leg_actor/tests/mod.rs | 15818 -> 15816 -> 15814 |
| quinn_leg_actor/tests/ordered_write_cancel.rs | new136 |
| quinn_leg_actor/tests/r7_arrival.rs | 512 -> 513 |
| tests/code_growth_governance.rs (repository root) | 690, only index ratchet |

Mencius integrated the two exact shrink steps separately, each with fresh
standalone and offline all-feature Cargo governance6/6. Final Main gates also
run against the last formatted source. No other debt/allowance changes.

Final runtime evidence: each of the three terminal cases passes ten serial
exact all-feature rounds; default blocked/fatal/Closing each1/1. Readiness3,
abort-before/aborted-outer/exit-subscription each1, fixed pressure1, submitted
FIFO1, join-FIFO2, terminal-stamp3 and arrival8 pass. Root59/59 passes. All
commands use --offline and tests use --test-threads=1; exact cases additionally
use --exact --nocapture. Previously recorded failing commands are not erased
or counted as green. The separately excluded real-loopback debt is unchanged.

Final Knife16 provenance162/162, repository provenance15/15, cargo fmt --check,
git diff --check, cargo check --offline --all-features --lib, fresh rustc
-D warnings standalone governance6/6 and Cargo all-feature governance6/6 all
pass. Only existing smoltcp22 and Knife142 warnings. No full-library, external
network, TUN, VPS, WAN, throughput or M2 acceptance is claimed.

Same Reviewer final formatted-code closure01a0855b-9d49-7492-8313-3ce3a50b21e1
completed1788943824: P0/P1/P2=0/0/0, Approve. Earlier combined scoped structure
score9/10 had no necessary extra optimization. Reviewer independently restored
the original index hash without general whitespace normalization, validated
the arrival one-line inverse and exact governance inverse, and kept all
ownership/receipt/first-cause assertions. Final gates actually completed after
that code review; no wording-only extra review was required or claimed.

Main's full inverse chain also restores original index30bd586f…6550 and the
arrival source. All354 current Rust hashes restrict change to the four paths
above; Actor6769 and every production source remain unchanged. Fifty untracked
paths pass whitespace checks. Compact data-only evidence and final hashes:
/tmp/mini-vpn-p3-ordered-cancel.Sm5xU1/evidence.json.

The current records and joint51 now reflect this executed finite test exception
and remove the stale pending-repair gate. Next is the previously frozen Actor
Readiness/Output pure Move, then the coupled A1–A4 real Bound path with its two
command placement/test-capture findings still open. No actual Bound service,
Actor implementation Move, commit or push occurred here. Full P3, Case C, R7
and Task4 are not complete; HEAD remains532d766.

## 2026-09-09 — Actor Readiness/Output pure Move accepted

After the finite fixture cleanup closed, Main resumed the already-approved
joint51 Move rather than creating another prerequisite architecture document.
Exact scope: actor parent, new private readiness/output children, existing
owner_bound_service provenance leaf, and Integrator-only measured ratchet.
No actor runtime test, caller, storage anchor, lifecycle or notification
behavior changed. The private children own complete operations, not wrappers.

Fresh standalone governance passed6/6 before writing. The frozen all-feature
actor baseline passed24/24: readiness3, abort-before/aborted-outer/subscription
each1, pressure1, submitted-wait1, FIFO2, terminal3, arrival8, fatal1, blocked1
and Closing1. The new exact provenance test
owner_bound_service::actor_readiness_and_output_implementations_have_one_private_route
compiled and failed only because the direct private readiness route was absent
(0 versus1,15 filtered); destination files were not read before that assertion.

The complete original Readiness impl80 lines, try_flush33 lines and
EncodedLegTransport impl72 lines moved. Parent storage/construction anchors
remain. Nine Readiness methods gain only pub(super) to recover their original
effective Actor-parent scope; try_flush stays pub(crate), trait/cfg methods
stay unchanged. Bodies, first-failure barrier, test pause, permit handling,
try_send and ordinal/error ordering are exact. Module docs name ownership,
non-ownership, entry operations and the real test location.

A pre-review P2 correctly found that declarations alone do not preserve an
old whole-parent negative-check domain. The same allowed provenance tracer
now applies bootstrap's original four naked-inbound bans to both real children;
the old parent checks remain untouched. No scanner, Knife count or new file
was needed for that repair. The only formatting repair was rustfmt layout in
the newly added tracer; production blocks did not need formatting changes.

Final lines: actor6769->6583; readiness88; output122; provenance352->507;
governance690. Mencius alone lowered the exact production entry6_769->6_583
and ran fresh standalone/Cargo governance6/6. All other inventory/allowance
bytes invert exactly. The new6583 ceiling remains shrink-only: this does not
permit later growth back to6769.

Post-Move exact provenance1/1 and all-feature24/24 passed. Default24/24 also
passed. Ten serial all-feature rounds of readiness3 plus pressure, submitted
wait, blocked write, child fatal and Closing each1 produced80 further passes.
Original tests/FQs/assertions are unchanged. Final Root default/all-feature
59/59 each, Knife162/162, repository provenance16/16, cargo fmt --check,
git diff --check, default/all-feature cargo check, fresh rustc -D warnings
standalone governance6/6 and Cargo all-feature governance6/6 all pass.
Cargo tests/checks use --offline; tests use --test-threads=1 and exact cases
also --exact --nocapture. Only existing smoltcp22 and Knife142 warnings remain.

Same Heisenberg final source review01a0856c-c690-7452-aed7-a340531ae21c,
completed1788945207: P0/P1/P2=0/0/0, Approve, scoped structure9/10.
The negative-check P2 is closed; no necessary optimization or broader parent
cleanup is requested. Reviewer did not run Cargo and correctly did not credit
then-unreported final gates. Main's terminal command evidence above supplies
that final acceptance; no extra wording-only review is required.

Both Reviewer and Main strictly invert the parent to824d7f8f...98a0, retain
all three blocks including their original separators, and preserve the
original352-line provenance prefix. All354->356 Rust sources, including
unchanged vendor sources in this audit, restrict change to the five allowed
paths with no deletion. Fifty-two untracked paths pass whitespace checks.
Data-only baseline/commands/final hashes are retained under
/tmp/mini-vpn-p3-actor-move.H4AxDx/before.json and evidence.json.
One initial read-only inventory comparison omitted vendor paths while its
baseline included them; correcting that enumeration, without a source edit,
proved the exact scope. This was an audit-script mismatch, not a runtime red.

The next executable packet is joint51 A1–A4's actual Current/Bound vertical:
close the two production-placement/pre-operation-capture command findings
inside its real implementation, not an independently-green command Move.
The first missing-path tracer remains
bound_active_owner_source_reaches_real_output_once. Later full-P3 verticals
and lifecycle obligations remain in the same checklist. No Bound behavior,
P3/Case C/R7/Task4 completion, commit/push or external acceptance is claimed.
HEAD remains532d7660e3b862849aa7650589b182fd491df212.

## 2026-09-09 — Active owning-fixture extraction accepted; Bound scope pending

This is the explicit preparatory extraction selected in joint51, not a new
feature, source dispatcher or Bound acceptance. Only two Rust paths changed:
physical_session_root/tests/current_only.rs744->754 and its
current_only/active_source.rs219->244. Both remain below800; no governance
inventory, production source, test name or public Interface changed.

The new private PreparedActiveSourceFixture owns the actual prepared owner,
client actor, listener, peer Supervisor, AttachedLeg, LocalFlow and payload.
Its parent-scoped test-only constructor contains the complete original real
OPEN -> OPEN_RESULT -> ordered delivery -> Target feed preparation. All
assertions stay in order. The original test destructures the extra resources
into named _peer/_attached/_local_flow/_payload bindings, retaining them through
the unchanged CurrentOnly constructor, actor joins and listener cleanup.
No wildcard drop, resource getter, clone, new callback or extra Driver exists.

Fresh rustc -D warnings governance compiled and passed6/6 before writing;
the original exact test passed1/1. After extraction the same exact passed1/1.
The only formatting difference was an overlong Blocked match arm after deeper
indentation; it was mechanically wrapped in the same allowed leaf, with no
behavior change, under the resumed self-repair instruction.

Final executed gates: CurrentOnly default/all-feature6/6 each; exact Active
test10 serial repeats, each1/1; Root default/all-feature59/59 each; Knife162/162;
repository provenance16/16; cargo fmt --all -- --check; git diff --check;
default/all-feature cargo check --lib; freshly recompiled rustc -D warnings
standalone governance6/6; Cargo all-feature governance6/6. Tests used
--test-threads=1 and --nocapture; exact cases also --exact. Only existing
smoltcp22 and Knife142 warnings appeared. Known unrelated real-loopback debt
was not run, fixed or reclassified; no full-library claim.

Same Heisenberg source review01a08580-1137-7c73-b733-2d2293dd5c01 completed
1788946275: P0/P1/P2=0/0/0, Approve, scoped refactoring9/10. Reviewer verified
the two files/inverses and source only, correctly leaving then-unreported final
gates pending. Main's actual terminal evidence above completes those gates;
no additional wording-only review is required.

Both Reviewer and Main verify all356 Rust sources: exactly these two paths
changed, no additions/deletions. The complete setup inverses exactly after
undoing indentation, listener mutability relocation and that one rustfmt arm.
The constructor/join tail is byte-identical. Final hashes:
current_only e3cbd570905b4922e829f32d2311138eba0f77d4ae5de8d86422af5eff19f6b3;
active_source e10ce28447f2183c657219a5548d67445330652ba30cbbcb277eea72ec937738.
Data-only before/expected/all-Rust hashes and exact command output are in
/tmp/mini-vpn-p3-bound-active.vkgTeZ/before.json and evidence.json.
An initial read-only inverse check applied the rustfmt undo at pre-dedent
rather than post-dedent indentation; correcting only that checker passed.
No Rust repair or weakened assertion was made in response.

Separate same-Reviewer executability review01a08579-6c95-7fa2-ad87-b6654ef55212
returns0/1/0: the selected fixed Root verification tail lacks its enumerated
private leaf, and the complete seven-method Target join/retirement Move needs
an explicit Move-axis allowance. Joint51 now records both proposed corrections
as awaiting user confirmation, not authorization. No Root test-route addition,
Target retirement Move or real Bound GREEN has started. Earlier acceptances
remain valid; the two command findings and full P3/Case C/R7/Task4 remain open.
No commit/push or external acceptance run. HEAD remains532d766.

## 2026-09-09 — Target join/retirement Move accepted

The user explicitly authorized both pending same-joint51 scope corrections.
Main synchronized that grant in the existing packet; no new sidecar or
wording-only review stage was introduced. Only the complete Target Move has
executed here. The Root bound_source test route is authorized for actual fixed
resident operations in the coupled Bound implementation, not empty scaffolding.

Fresh pre-write rustc -D warnings governance passed6/6; Target's existing
default/all-feature tests passed74/74 each. The new exact provenance tracer
owner_bound_service::target_join_retirement_has_one_private_completion_owner
compiled, ran1 test and REDed only on finish_target_flow's actual parent path
versus required completion path. No missing-file or fixture compile error.

The original contiguous seven-method block at4695–4935 plus its separator
moved to existing owner_target/completion.rs. Four original parent callers
retain equivalent pub(super) access: finish_target_flow, retire_target_tombstone,
try_join_target_flow and expire_next_target. The other three helpers remain
child-private. Storage, callers, all pre-existing tests/FQs/assertions and
original completion methods remain unchanged. Existing scans, Vec allocation,
Pending join, target tombstone comparison, retire_after_join and expiry/error/
final Flow removal order are preserved, not optimized or newly accepted.

Exact post-Move provenance passed1/1. Rustfmt required only the added imports,
one visibility-lengthened signature and the new test helper layout; formatting
was limited to those two allowed files. Final lines: owner_target9877->9635;
completion164->411; provenance507->591. No new Rust source or deletion.

Main's final actual commands (all Cargo operations offline):
- Target default/all-feature74/74 each; ten additional all-feature serial rounds
  each74/74 (740 test passes).
- CurrentOnly default/all-feature6/6 each; Controller default/all-feature44/44
  each; Root default/all-feature59/59 each.
- Knife16 provenance162/162; repository provenance17/17.
- cargo fmt --all -- --check, git diff --check and default/all-feature
  cargo check --lib pass. Tests use --test-threads=1; focused uses --exact
  --nocapture. Only existing smoltcp22 and Knife142 warnings remain.

Same Heisenberg source review01a085a0-66e7-7922-8363-129354f1f164 completed
1788948390: P0/P1/P2=0/0/0, Approve, scoped structure9/10. No necessary patch.
Reviewer independently verified the full parent inverse, complete method block,
unchanged three completion methods, original507-line provenance prefix and
356 Rust scope. It correctly left then-unreported final gates/ratchet pending;
Main's executed evidence and Integrator's result below complete those gates.

Mencius alone changed governance's one entry9_877->9_635. The file remains690
lines; full inverse restores c532f6b28ac3dee8ee52780449f6f6bf163dd295805f905a9cc19c68ec5e924d.
Current governance SHA is f4366589a2c1bf2884629e09dc959f7ea86867807ff26162af9edd9adadc5bfe.
Integrator submission01a085a0-66cc-7251-a692-5f633d2381a3 completed1788948497:
fresh standalone rustc --edition=2024 -D warnings compile exit0, standalone6/6
and offline Cargo all-feature governance6/6. Main separately verifies the exact
single-entry byte delta, final Rust scope and final diff check.

The parent inverse restores de646d370bb8103724232b7ea9a0630ec0e40079e3232f4bf902f6dd49620f57.
All356 Rust sources differ only in the three feature paths plus Integrator's
governance entry, with no additions/deletions. Data-only before/expected,
exact Main terminal commands, inverse checks and final fingerprints are in
/tmp/mini-vpn-p3-target-retirement.DyOQ3e/before.json, expected.json and evidence.json.
One initial large read-only capture exceeded the tool response budget, and one
patch-context mismatch applied nothing. Capturing the bounded method block
with whole-parent hash, and using unique context, resolved those tooling issues;
they were not Rust failures or permission to change unrelated content.

Next is the already-authorized actual Current/Bound tracer
bound_active_owner_source_reaches_real_output_once and its fixed Root-private
verification tail. The existing two command placement/pointer-capture findings
must close in that real route. No Bound behavior, whole P3, Case C, R7, Task4,
production entry or external acceptance is claimed. No commit/push; HEAD532d766.

## 2026-09-09 — First real Bound tracer reaches construction/connection RED

This is an in-progress TDD checkpoint, not Bound implementation acceptance.
The authorized Root test route now contains its three fixed resident FIFO/
one-flush/same-current-join operations; the physical private leaf contains
the actual opened-Target-to-peer tracer. No production behavior is implemented
by a fixture. Required Root/plan/result declarations and polling entry have
only the accepted owner-scoped reachability; their fields remain private.

Before adding RED, fresh standalone governance compiled with -D warnings and
passed6/6. The original Active fixture exact all-feature test passed1/1.
The complete original post-checked-construction transaction was then extracted
inside current_only.rs as finish_checked_current_only(actor, controller,
handoff). Removing only its11-line function boundary/call reconstructs the
original754-line file byte-for-byte. All six CurrentOnly tests passed with
all features; fmt passed. No handshake/assertion/protocol body was duplicated.

The first new exact command is:
cargo test --offline --all-features --lib
'owned_upstream::quinn_leg_composition::owner::physical_session_root::tests::current_only::bound_source::bound_active_owner_source_reaches_real_output_once'
-- --exact --nocapture --test-threads=1

Its first compile also exposed a test-feedback error: the inbound queue count
was invoked on StartedQuinnLegActor instead of its existing actor_snapshot().
That single test expression was corrected without any production API addition.
The corrected compile exits101 with14 errors exclusively naming the missing
OwnerReadySessionLimits, OwnerControllerPreparation, cold, actual OwnerResident/
resident.owner, and OutputAdmitted connection. There is no E0603, E0107,
remaining helper-call error or runtime test result. This is the expected
missing complete owning path, not an executed behavioral RED/GREEN.

Both new polling loops have a1024-step diagnostic ceiling: an always-Ready
loop cannot evade timeout forever. It is not a scheduler capacity/delay proof.
The test keeps the real peer, attachment and LocalFlow alive, checks actual
DATA generation/direction/offset/payload, and explicitly retains logical
authority after same-resident structural join. No fixture reducer, output
drain, manual Root wake, public resource getter or whole-root zero claim.

Current Rust delta is exactly three changed/two new paths: server_root846
(owner-scoped declarations only), its test index3/new bound leaf50,
current_only766/new bound leaf230. The prior356-file fingerprint inventory
plus both new leaves differs only there; no deletion or governance edit.
Fmt/diff and current-source standalone governance6/6 pass. No Cargo post-GREEN
matrix, runtime Bound test, completed implementation review or full P3
acceptance is claimed. Same Heisenberg's source-only review
01a085b5-edb5-7921-8e88-aaf3638a6d2e returns0/0/0, Approve for this RED tracer,
fixed tail and extraction only; no Reviewer compile/runtime execution is implied.
The finite exactly-once assertion is not a complete lifecycle proof, and its
outer visit ceiling cannot bound a broken single internal poll.
Next implement the accepted coupled A1–A4 owning route and its two command
findings; do not remove assertions, add a production Prototype, or report a
metadata-only GREEN as the path. No new authorization/design sidecar is needed.
No commit/push/external; HEAD remains532d766.

## 2026-09-09 — Cold custody implementation compiles; real Bound remains RED

Main began the accepted A1–A4 implementation, not another design prerequisite.
The actual ReadyIndex now has Cold construction, immutable checked F/O/M limits,
four Reserved class cells at one recorded occupancy, a bounded guarded N scan,
and short-borrow non-Clone custody. The shared strong admission RMW is last
before reservation publication. Explicit cancellation validates the same Shared,
correlation/key, all four unused charges and checked next generation; it may
settle known-empty metadata even when the Root gate already failed, without
clearing that gate. Associated reentry/return preserve the original owning form.
Failed cancellation can end the borrow into whole-custody Cold quarantine.
Abandonment only records the shared first fault/dirty epoch: no lock, allocation,
Notify, generation advance or Free. Bound installation/activation is NOT yet
implemented; their type anchors do not constitute a successful binding.

Same Heisenberg source audit01a085c6-b205-73c3-b93e-8752e3e64f39 found0/0/1:
cancel wrapped failure.fail(detected), whose return is the earlier Root fault.
With original RevisionExhausted plus actual generation exhaustion, this lost
SlotGenerationExhausted. Authorized self-repair separates recording from the
detected return in private ReadyShared::cold_fault. Stale/Poisoned/Corrupt and
generation branches all preserve their own cause, with the original gate intact.
The new real-metadata test sets both actual key halves at the arithmetic edge,
then checks both errors, same Shared, four Reserved charges and non-Free quarantine.
Same source re-review01a085cb-c134-74c2-8199-47c483fb986b returns0/0/0 for
this repair/test source only. Reviewer ran no compiler or runtime tests.

Executed evidence and its limits:

- Pre-write fresh standalone rustc -D warnings governance6/6; after final Rust
  changes the same current-governance executable scans current files6/6.
- cargo check --offline --lib passes before the review repair. After repair,
  cargo check --offline --all-features --lib passes. Only the existing vendored
  smoltcp22 warnings; no new non-vendored warning was reported.
- Bound exact after cold implementation exits101 with13 missing actual Root/
  preparation/resident/output connections (down from14 because limits now exist).
- Exact cold_cancel_retains_detected_generation_error_beside_original_root_fault
  attempt after repair also exits101 on the same13 Bound connection errors.
  No cold test error was reported, but no test executed. Neither runtime RED
  nor GREEN is established for this regression. Execute it when the coupled
  crate compiles; source review must not be substituted for that obligation.
- cargo fmt --all -- --check, tracked git diff --check and both new-file
  whitespace/newline checks pass. No broad/post-GREEN matrix is claimed.

Exact five-path Rust delta from the prior358-source boundary: owner_ready.rs
735->847; new owner_ready/cold.rs344; metadata test index124->125 and new
tests/cold.rs66; server_root846->847 only adds cfg(test) to old from_commands.
Its old metadata initializer is also cfg(test); real cold construction does
not publish send work. All other358-source entries are unchanged, with two
new sources making360; no deletion or governance edit. HEAD remains532d766.
Evidence: /tmp/mini-vpn-p3-cold-custody.myB0dy/evidence.json.

Next code work remains inside joint51: original-factory/correlation validation,
one actual checked inbox/command/backing installation, generic cold preparation
and whole Current resident, then the complete selected service/Output route.
Do not recreate the Bound tracer, stage a temporary success/Prototype, or add
another sidecar. A1–A4, full P3, Case C, R7 and Task4 remain incomplete.
No commit/push/external, no new authority or replacement Reviewer.

## 2026-09-09 — Identity and command coupling implemented; actual Bound remains compile RED

Actual code, not another design sidecar: Executor's private source validation
uses the original Supervisor role/health, actual factory origin/admission Arc,
actual gate binding and reservation's declared session. Capacity derivation
still belongs first at checked construction; the consuming installer is not
implemented, so this validation has no real install caller yet. Same Heisenberg
source review01a085db-e097-7032-960c-4a67d3b6656c returns0/0/0, not binding acceptance.

Complete SessionCommands now lives under private owner_target/commands.rs;
neutral Drive/Rearm/Unit/Visit values live in owner_ready, not outer Root policy.
The private fresh-pair operation creates one mailbox pair, one observer and two
Vacant cells, retaining commands-before-inbox destruction. It too still awaits
the real installer caller. No fabricated OPEN, independently injected endpoints,
command getter or production Prototype is used. The Root is now generic and
stores whole Current physical roots or cfg(test) Prototype; the actual Current
dispatch/quarantine calls are deliberately missing, not stubbed successful.

Four full test leaves are byte-identical at their new inward paths:
Control14, registration5, ordered9 and send-cells7 =35. The four old paths are
removed; their complete contents remain in the new private tree and before.json.
FQ prefix changes from owner::server_root::tests::ready_scheduler to
owner_target::commands::tests::ready_scheduler. Outer12 tests retain original
Box capture, real receipts, claim pointers and native callback lifetimes. Full
capture requires CapacityBlocked; cold-return capture also accepts real rearm.
Root cold masks/admission use the finite Prototype operation; metadata quarantine
still precedes command quarantine. Parent Executor stays9635 and Supervisor5682.

Same Reviewer01a085fa-09bc-7342-a6f7-79e8e328496b independently compared15 original
command method signatures/bodies, all35 test bodies, outer12 test symbols and
five Wake/Drop implementations:0/0/0, Approve SOURCE delta only. Current physical
quarantine is not implemented or accepted: when connected it must return typed
failure and retain actual Running claim/completion on failure before any Idle
transition. Prototype's old infallible ordering is not sufficient for Current.

Executed: identity-only all-feature library check PASS before command coupling.
Current all-feature library check exits101 solely on missing Current
visit_ready_source/quarantine_ready_source. First coupled test compilation
reported those/other real connection gaps plus four test-only generic Layout
omissions; an exact MemoryTarget CommandResident alias repairs the latter.
The full-FQ Bound exact compilation then exits101 with six references: missing
OwnerControllerPreparation, Root::cold, OutputAdmitted at two sites, and the
two physical methods. No test executed. The earlier short-name --exact command
also failed compilation and supplies no test-count result. Cold regression,
new35/runtime regressions and broad/Cargo provenance/governance remain unrun.
Fmt, git diff --check, all26 changed/new source whitespace checks and unchanged
current standalone code-growth executable6/6 pass. This is not coupled GREEN.

Scope from previous360-source checkpoint:17 changed,9 new,4 exactly relocated
old leaves removed, leaving365 governed baseline sources. Ready879/cold358,
commands476/Prototype156, generic Root401, physical722 (only root visibility),
inward index2/fixture88/leaves604/344/476/771; governance690/hash unchanged.
HEAD532d766 remains unchanged; no inventory edit, commit/push or external run.
Evidence: /tmp/mini-vpn-p3-command-coupling.2J4aon/before.json and /tmp/mini-vpn-p3-command-coupling.2J4aon/evidence.json.

Next: complete consuming checked installation with actual Flow/Target backing,
generic cold preparation/whole adoption and selected receipt/source/output
service under joint51. Six compiler messages are not six remaining tasks.
No new sidecar, temporary successful constructor, raw resource export or
independent command-Move promotion. A1–A4, full P3, Case C, R7 and Task4 remain open.

## 2026-09-09 — Consuming cold binding implemented; full Bound execution remains compile RED

Actual Rust now connects new_checked to the sole consuming install_owner_ready.
C derives its original full witness and progress before installation. The
installer validates the original source chain, imports existing F Flow records,
prepares the real service positions, eleven metadata hierarchies, actual immutable
publishers and one fresh mailbox pair/observer, then commits the same reserved
slot. No source/Target/inbox poll occurs during construction. Post-bind code
moves preallocated records, commands and binding into the same Executor and its
matching inbox into C. Modeled rejection returns clock, Executor, plan, original
reservation and precise cause. Actual Root::cold and owning Controller input
are also written; Root build_checked_controller/adopt/start are still missing.

The third installer parameter is existing inward OwnerSessionCapacityPlan,
configuration rather than a certificate. Same Reviewer01a0860e-d2ea-7c32-aebe-
666b3bff6969 approved this concrete dependency-preserving choice0/0/0. C retains
the full witness; importing C's witness back into Executor is not required.

Initial source review01a08626-c142-7882-9836-e650c615135e returned0/4/1:
legal source-less queued FlowFinished was rejected; asynchronous Output did not
notify; consuming Wake held its ticket Arc through publication; inactive errors
indexed/notified; and three old native callbacks lacked private try_lock.
Causal repairs preserve the actual pending/aborted effect queues through a
bounded cold index, record distinct PendingFinishEffect, release consuming Arc
before immutable old-generation publication, request retirement rather than
reopen DATA, classify Output as external, and preserve nonblocking try_lock.

The same single publication AtomicU8 now includes activation-committed ENABLED.
Same Reviewer01a0862f-c8f6-7fa3-9b79-ac8b3843562a approved this finite mechanism
repair0/0/0 under existing scope: activation sets it only after final RMW;
finish preserves it with fetch_and(ENABLED). Poison never recovers metadata;
the single fetch_or(PENDING) snapshot controls active indexing/notification.
Inactive errors retain the first fault, dirty state and custody but never
publish Reserved Session charges. Actual activation/finish are not yet written.
Source re-review01a08633-bd92-7f70-94ca-e7d2a9d26093 closes0/0/0, not runtime,
complete cold peak, Current service or Bound GREEN.

Five of18 checked fixture calls are migrated in C/tests/capacity.rs. Each owns
its real ReadyIndex; drift retry reuses the returned reservation, horizon
collision cancels it, and the two success Controllers remain named. Two guessed
snapshot identity receivers were compiler-disproved. The final helper captures
the actual CommittedLeg ID before its original Supervisor transfer; no second
Leg or production getter is introduced. Same test-source Reviewer01a08636-dd1b-
7443-b989-79940bd3bfa9 approves0/0/0. Thirteen call sites remain unchanged.
Four metadata tests cover inactive requests, foreign preparation exact return,
limit rejection/cancel and poisoned local publication; all are written/unrun.

Executed evidence:

- Fresh standalone rustc -D warnings compilation and current governance6/6.
- Final cargo check --offline --all-features --lib exits101 solely on missing
  Current visit_ready_source/quarantine_ready_source; no new local warning.
- Final full-FQ Bound exact command exits101 with20 errors:13 remaining old
  checked signatures and7 actual preparation/adoption/start/output/physical
  references. No fixture snapshot/try_lock error remains. Zero runtime tests
  executed; compiler silence about an edited function is not runtime GREEN.
- Fmt --all -- --check and git diff --check pass; current changed/new files
  retain final newlines and no trailing whitespace. Only vendored smoltcp22
  warnings; no Cargo governance/provenance or broad runtime matrix is claimed.

Scope:11 changed plus4 new Rust paths against the365-file checkpoint; zero
deletions and all other baseline hashes unchanged, now369 tracked/untracked
Rust sources including preserved vendors. The four new files are owner_ready/
flow.rs518, owner_ready/tests/flow.rs162, owner_target/service.rs41 and
server_root/construction.rs73. Other exact lines: ready976/cold455/test index126,
binding214/commands504, Executor9635, C3536/construction130/capacity tests473,
Root420. No oversized ratchet changes; governance remains690 and unchanged.
HEAD532d766 remains unchanged. Artifact: /tmp/mini-vpn-p3-cold-binding.rAQd1b/.

Remaining first-path mandatory work stays executable inside joint51: migrate
the13 callers with same-index lifetimes; finish original Root preparation and
whole adoption; implement activation and physical publication admission/finish;
actual Target readiness and five-source/thirteen-category selection; original
owning receipt/source/output service and Bound-aware cutovers; then execute
the real Bound tracer, metadata/caller regressions, lifecycle and final gates.
Full cold-peak/work bounds and exact retirement remain obligations, not copied
count proofs. Later full-P3 OPEN/TCP/DNS/all-class/expiry/B work and P4–P7 retain
their original checklist classification. No goal reduction, new sidecar,
commit/push/external, public TUN cutover or full P3/Case C/R7 acceptance.

## 2026-09-09 — All18 checked fixtures migrated; full Bound execution remains compile RED

The remaining13 new_checked calls now receive real reservations. Continuation
helpers borrow caller-owned domains; their18 helper uses retain independent
indexes, including the three simultaneous roots. The recovery carrier still
enters C only at its original begin_attached_recovery call. Eight initial calls
retain local indexes through wrong-clock return/retry, pending/delivered roots,
terminal/joined outcomes and LeglessParts. Physical attach/recovery and settled
CurrentOnly fixtures own their index last; destructuring binds named
_ready_index first so actual owners drop first. Active finish uses its caller's
index. No helper-local temporary domain escapes and no assertion is weakened.

Opaque initial acceptance/bootstrap have no SessionId getter. Same Reviewer
01a08641-63c4-7fc3-a43f-57e844b264d5 approved declaring the original fixture's
request configuration0/0/0, not substituting a scalar capability. The real
installer still verifies original Supervisor/factory identity. No bootstrap
decomposition, second identity, getter or oversized actor-helper change.

Same Reviewer source closure01a08646-a7dd-73c3-9635-88a5185cbd36 is0/0/0,
Approve; scoped Architecture/Clean/System9/9/8. Review independently verified
the ten-file hash delta, original continuation body tokens, initial rejection/
retry and fixture destruction order. Reviewer ran no Cargo/runtime test.

Main executed the offline all-features full-FQ bound_active_owner_source_reaches_real_output_once
command: compilation exits101 with only7 missing actual path references
(build_checked_controller, adopt_current_only, start, OutputAdmitted twice,
physical visit_ready_source/quarantine_ready_source). No migrated signature or
privacy error remains; zero tests ran. Only existing smoltcp22 warnings.
Pre/post standalone governance6/6, global fmt check, tracked diff check and
all10 changed files' newline/trailing-whitespace checks pass. Cargo governance,
provenance and broad runtime matrices remain unrun while coupled compilation
is RED. Compiler silence about migrated code is not runtime GREEN.

Exact lines: continuation330/462, initial649/647, physical attach685/196,
recovery400/107, CurrentOnly785/Active247. Against the369-source baseline,
exactly10 authorized test paths changed, none added/deleted; production,
previous five capacity calls, scanner and governance are unchanged. HEAD
532d7660e3b862849aa7650589b182fd491df212 is unchanged. Artifact:
/tmp/mini-vpn-p3-caller-migration.BFXd9r/evidence.json.

The next A1 implementation audit01a08647-ecb4-7552-a7a1-f4f19ee066a1 exposed
a real frozen-interface gap: quarantine_cold's Err(Self) retains the index
borrow; chaining into_associated still leaves a borrowed Err. No total Root
storage path exists without a contract correction. Same-Module total terminal
conversion retaining actual Cold/Associated tag, whole custody and exact cause
is under the same Reviewer's bounded review01a0864d-176a-79d0-9aee-41db8a032ef4.
No production repair is claimed here. Continue the existing A1–A4 packet and
full P3 obligations; no new sidecar, goal reduction, commit/push or external run.

## 2026-09-09 — Total constructor-quarantine exit implemented; actual Root caller remains open

Same Reviewer decision01a0864d-176a-79d0-9aee-41db8a032ef4 accepts the bounded
same-Module repair0/0/0 under existing A1–A4 auto-repair authority. Existing
joint sections31/33/51 now select one total into_constructor_quarantine exit,
not a new sidecar. Normal Associated return and new_checked's original four
arguments/five-field failure remain unchanged.

Actual ready code now returns opaque OwnerReadyConstructorQuarantine owning
the original UnboundReservation tag and complete custody plus exact cancel
cause. It ends the index borrow; same-gate first fault and epoch dirty precede
Cold-only abandonment disarm. Neither tag is relabeled; there is no lock,
allocation, notification, custody clone, second ledger or reusable exit.

The original Cold test now calls the total exit and checks its retained tag.
A second Associated metadata characterization uses actual reentry, exact
NotUnattached, retained earlier RevisionExhausted, unchanged keys/four charges
and an explicit ReadyIndex move while quarantine remains alive. It arranges
metadata only, not a P4 parent or production association claim.
The exact Cold command first compiled RED8: one predicted missing total-exit
method plus the seven existing Bound path references. After implementation it
returns only the original seven; no new local warning, existing smoltcp22.
Neither test executes while coupled compilation is RED: no runtime GREEN.

Same Reviewer source review01a08653-5e6f-76a1-be38-147fe211e55a finds Rust
0/0/0, Approve. Overall0/0/1 identified one stale §33 cold-only sentence; Main
corrected those exact sentences and checked the contract without another
wording-only review, as directed. Original cancel/into_associated remain exact.
Fresh fmt/diff and current standalone governance6/6 pass; three Rust files
retain newlines/no trailing spaces. Lines: ready976/cold453/tests138. No
oversized ratchet change. The preceding ten-file migration is unchanged.
Artifact: /tmp/mini-vpn-p3-caller-migration.BFXd9r/quarantine.json.

Next actual Rust remains Root::build_checked_controller with its one inline
OwnerConstructorQuarantine destination and exact original-input return, then
whole-current adoption/start and complete A1–A4 selected service. Root storage
and caller are not implemented or approved by this local source verdict.
All full-P3, Case C and R7 obligations remain; no commit/push or external run.

## 2026-09-09 — Root constructor/start and five-source selection checkpoint

Actual A1 code now invokes the checked constructor from the original Root:
require Idle and a coherent resident prefix, reserve the same index, and pass
the original Executor/clock/plan through new_checked. Known rejection returns
all original inputs after exact cancellation. Cancellation failure consumes the
total quarantine exit; Root stores original input, constructor cause and opaque
ready custody/cancel cause before returning its sticky fatal observation. No
borrow, duplicate owner or successful constructor validation gap escapes.
Consuming start checks the same gate, Idle, complete occupancy and resident
correlation, returning the original preparation on failure; success only moves
that same Root. Whole-current adoption/activation is still missing.

Same Reviewer found P1 in the cold resident audit: registered prefix/key/SID
contradictions and orphan Active suffix were ordinary InvalidResident. Repair
now records Corrupt in the original gate and preserves metadata/custody. A
legitimate unpublished binding remains a normal pending rejection, deferred
until the whole scan and final gate check. Source re-review
01a08667-b923-7460-a096-ebb15fc98af9 closes0/0/0, Approve. The new single test
uses real reserve/bind plus clearly marked metadata fault injection for Free
prefix, inactive prefix, orphan Active, SID/index/key disagreement. Existing
two cold tests are unchanged. Multi-slot pending-before-orphan has source
ordering proof only; none of these tests has executed yet.

A2's finite source type is now real: ControlSend/OrderedSend/ControlInbox/
OrderedInbox/OwnerContinuation map to0..4. ReadyClaim and Root Visited retain
that exact source; finish republishes all five returned runnable bits. Original
SendLane, SendCursor, two send cells/capacity operation and commands are unchanged.
Only cfg(test) Prototype maps the first two sources to its real send operation;
other sources are rejected, not synthesized as sends. Exactly22 lines in five
original outer test files migrate20 Visited and2 borrowed-claim assertions.
Inverting just those substitutions reproduces all five previous file hashes.
The existing provenance leaf adds the neutral enum and replaces the visible
claim lane whitelist with source, without widening its private storage.
Same-Reviewer source review01a0866a-3715-7573-9c30-b00dded0cb57 closes0/0/0,
Approve. Reviewer independently inverted all five tests, the provenance change
and ready metadata change; no residual claim.lane consumer or command/native
callback mutation. This is not runtime evidence for the additional three sources
and does not claim full A2 acceptance.

Executed compiler evidence: the original full-FQ offline/all-features Bound
command had7 missing references, then6 after build and5 after start. Following
the P1 repair and source migration it remains exactly5: adopt_current_only,
OutputAdmitted at two sites, physical visit_ready_source and
quarantine_ready_source. The new metadata exact command also cannot execute
because the same test crate is incomplete. No new local diagnostics; existing
vendored smoltcp22 warnings only. Zero runtime tests ran, no RED/GREEN or full
service claim. The missing references represent coupled owning implementations,
not permission to insert stubs merely to turn compilation green.

Fresh rustc -D warnings standalone governance compile/run passes6/6;
cargo fmt --all -- --check and git diff --check pass. All11 changed Rust paths
have final newlines and no trailing whitespace. Against the369-source prior
checkpoint, only these11 authorized paths change; no additions/deletions or
governance mutation. Exact lines: ready1000/cold534/tests236, Root462/
construction183, outer test files552/688/144/212/188, provenance592;
governance690 and Executor9635/C3536 stay exact. HEAD remains
532d7660e3b862849aa7650589b182fd491df212.
Artifact: /tmp/mini-vpn-p3-caller-migration.BFXd9r/root-construction.json.

Continue the same accepted A1–A4 packet: whole adoption with real actor/C/FIFO/
binding validation and final activation; actual selected receipt/output service;
Target/Actor readiness and complete publication/claim/completion lifecycle;
then the real Bound tracer and mandatory regressions. Cargo governance,
provenance/runtime and broad matrices remain unrun while compilation is RED.
All later full-P3, Case C/R7 and external readiness obligations remain open.

## 2026-09-09 — Whole-current adoption and original binding activation SOURCE checkpoint

Actual A1 code now moves the whole already-built Current into the real Root's
reserved resident Vec. Precommit checks cover the original gate/Idle/prefix and
actual destination capacity; rejection returns that exact whole Current plus
its cause. Inward activation uses the original actor first-failure transaction
and actual runtime/io/FIFO, settled handoff/physical binding, live vacant output,
checked C phase/recovery/receipt, and Active Supervisor attachment seal/factory
identity. Executor validates both directions of actual Flow-map/fixed positions,
full LocalFlow and metadata association/generation/retiring state. NoSource
phase is compared with the actual owner's completion/terminal/retirement state.
It does not remint or expose an identity certificate, inbox, actor or Driver.

The same binding validates its actual Arc/key/position and all six immutable
publishers, prepares controlled Capacity/Output Wakers, then reads current due
and checks revisions under the same slot lock. Original failure admission is
the final fallible edge; Err restores the exact BoundInactive payload. Success
only enables/stores/publishes metadata and returns owning values, followed by
physical Output-Waker retention and reserved whole-root placement. No allocation,
Notify, dependency poll or callback follows admission. Actual Actor Output
registration is still selected-visit work; it is not claimed by cold activation.

Same independent Reviewer submission01a08684-c496-7051-8f03-a86f9137ff9d
approved this SOURCE increment0/0/0. Closure self-audit then found a P1 error
classification gap: Flow-position preflight could return AlreadyActive for
BoundActive with ENABLED clear, before the final activation check saw corruption.
The same-lock preflight now checks Active iff ENABLED first and records original
sticky Corrupt without clearing custody. Same Reviewer final submission
01a0868f-195f-79a0-9451-074257ead3d6 closes0/0/0 and retains the original
15-file SOURCE approval. Old four metadata tests and original activation-test
assertions are unchanged, independently checked by inverse review.

Two metadata-only tests are written: original-domain/current-due activation
without Notify, and valid repeated rejection followed by actual ENABLED fault
injection with sticky gate and original BoundActive/custody retention. Neither
has executed. The latter was written before the classification repair; the
coupled crate still cannot reach runtime. Do not call this runtime RED/GREEN.

The exact offline/all-features real Bound tracer now compiles through adoption;
missing references fall5->4: OutputAdmitted at two test sites and the actual
physical visit_ready_source/quarantine_ready_source operations. Following the
last repair it remains exactly those4 E0599, no new local warning; only existing
vendored smoltcp22 warnings. Earlier local compile feedback required moving the
unchanged cold_fault body from child-private to its same-parent-private impl
and supplying the existing input_admission_block(None) argument. No visibility
workaround or missing-service stub was added. Zero new runtime tests ran.

Fresh rustc -D warnings standalone governance compile/run6/6, cargo fmt --all
-- --check and git diff --check pass. All15 changed Rust paths have final newline
and no trailing whitespace; against369-source baseline,13 existing paths and
two allowed new private children change, no deletion. C3536, Executor9635 and
Supervisor5682 remain exact; governance690/hash unchanged. Other exact counts:
ready1015/cold668/flow635/tests273, C service45, Executor service160,
Supervisor source_factory175, output continuation1004, physical741/service59,
Root474/construction241. HEAD is unchanged532d7660e3b862849aa7650589b182fd491df212.
Artifact: /tmp/mini-vpn-p3-caller-migration.BFXd9r/adoption.json, retaining the
prior checkpoint, complete source hashes, exact compiler/gate evidence and both
review verdicts. No commit/push, external operation or inventory edit.

Next execute the same joint51 A1–A4 packet: actual Target/Actor readiness and
release producers, complete selected receipt/output service, physical publication
and retained claim/completion on failure; then the real Bound test, reachable
lifecycle matrix and full regressions. Read-only Actor inventory identifies
the original Readiness allocation, explicit release/close producers and existing
nursery/result ledger; none of that Actor implementation changed here. Cargo
governance/provenance/broad runtime remain unrun. Later full-P3 and Case C/R7
obligations are unchanged; SOURCE approval is not runtime or full A1 acceptance.

## 2026-09-09 — Actor Output SOURCE checkpoint

The actual selected Actor port is written in the original output child. It
checks the existing physical incarnation/FIFO/non-transferred parts, registers
the controlled Output Waker and polls only the existing single-root nursery
through one stack-pinned primitive-only unconstrained future. Ready nested
outcomes move into original observed_root before classification/formatting.
The private JoinError now retains Tokio's exact error; the old join uses the
same plain owning conversion and restores exact custody before text conversion.
Completed blocks further output as Closing, not Joined or inventory zero.
Only real ActorPressure or ordered-delivery wait maps to Pending; one original
queue flush otherwise retains its original result categories.

Readiness deepens its original Arc to epoch plus one optional controlled Waker
under a mutex. The original watch/current/reserved/same_source behavior stays;
registration rejects overlapping legacy subscribers. Clone is before exclusion,
swap/take inside, old drop and invocation outside. Explicit Output edges follow
actual FIFO submission, non-ordered permit release, DELIVERED token/permit
release, Closing, all four fixed-child outcomes and both root child-join paths.
Generic legacy advance and ActorRootExitSignal Drop gain no Output notification.
No new task, nursery, watch receiver, observer or per-record allocation is added.

Only three existing Rust files change: actor parent6583, readiness130, output223.
Parent stays exact by the shared owning result conversion and compact adjacent
comments/separator; no governance edit. Pre/post standalone governance6/6,
fmt/diff pass. The original exact all-feature/offline Bound command still fails
only on its existing4 E0599 missing output/service references, no local warning;
existing vendored smoltcp22 remain. Zero runtime tests ran and no test assertion
was changed. The new port is not yet called by complete physical Bound service.

Same Reviewer submission01a0869b-a0f4-7760-a5af-716042fc8b61 approves this
three-file SOURCE increment0/0/0. Explicit poison review: LocalShutdownWon may
suppress the void cancellation record, but the original poison remains; new
fixed-child/root error edges reach original ActorError/nursery before handoff,
not normal Completed. Do not claim the void call itself stored that new cause.
Actual poison/pressure/close and deferred-publication tracers remain required;
no runtime or complete A3/P3 acceptance follows from this source proof.

Read-only supporting audit confirmed the existing Tokio1.52.0 primitive and
both prior build fingerprints exclude tracing/taskdump; it was not a new build.
Target inventory identifies one real MemoryTarget and three existing static
test adapters, named/shared release points, separate F versus Z lifetimes,
and the authorized memory_ready child. Target1445/memory_io582 remain unchanged.
Main's limited static assessment is Clean Architecture9/System8/DDIA8; reaching
10 requires the real physical caller and executed pressure/cancel/poison/outcome
tracers, complete backing accounting and lifecycle matrix, not more wrappers.
Next continue actual Target readiness and owning source/receipt/output/
publication under joint51. Full first-path and P3 runtime/lifecycle acceptance
remain open. Artifact: /tmp/mini-vpn-p3-caller-migration.BFXd9r/actor-output.json.

## 2026-09-09 — Target readiness and governance exclusion stop

Under the existing joint51 A3 write set, MemoryTarget now implements both
required readiness polls using its actual Flow/buffer/directive/tombstone state.
The old diagnostic readiness is unchanged. Fixed F_target positions, three
named registrations, one common registration and two waiting hierarchies own
no second business resource. Checked mutation plans precede business commit;
shared releases retain revision/cursor and notify at most one indexed candidate
per common quantum. Cold checks validate real Flow/position correspondence,
free chain, hierarchy shape/padding and ancestor consistency. Reset/cancel's
private terminalize result carries its prepared notification until the original
caller has committed its final snapshot counter; no callback parameter added.

The first real Adapter test was written before implementation: finite queued
WouldBlock allows one consuming attempt; then real global pressure waits until
another Flow releases a byte and one common quantum rearms that writer. Its
first exact all-feature/offline Cargo command produced10 new missing readiness
references plus the existing4 Bound references. After implementation and the
three real Adapter delegations, compilation has only the original4 E0599:
OutputAdmitted twice, quarantine_ready_source and visit_ready_source. No local
warning; existing vendored smoltcp22. No runtime test has executed, so neither
that compilation absence evidence nor the test source is runtime RED/GREEN.

Seven changed Rust paths, exact lines: target.rs1497, target/memory_io.rs709,
target/memory_ready.rs539, target/tests/owner_readiness.rs66, and the three
owner_target test Adapters568/744/151. All3 Adapter inverses exactly match the
pre-stage SHA after removing only new imports/delegations. Parent's nine old
tests and helpers match exactly after removing the new route and separator.
All373 current Rust hashes restrict this stage to seven paths; no deletion.
Fmt, tracked diff and all seven whitespace/final-newline checks pass.

Fresh standalone rustc -D warnings compilation succeeds, but governance is2/6.
All4 failures report that target.rs's owner_readiness route resolves outside
the governed inventory. Root cause is excluded_governance_directory matching
every basename target, not a module-context or Rust-resolution failure. It
excludes src/owned_upstream/target including existing production memory_io.
Prior standalone6/6 therefore did not establish coverage of those children.
The governance file remains690 lines with SHA
f4366589a2c1bf2884629e09dc959f7ea86867807ff26162af9edd9adadc5bfe.
No path rewrite, scanner edit, inventory change or altered limit was attempted.

Mencius's read-only audit confirms this scope blocker and finds no additional
definite normal-path Err-after-commit or dirty-pass lost wake. It expressly
does not prove unwind custody: read/take_written hold drained bytes locally
before publication, so the actual controlled Bound ACTIVE/deferred-notification
and external Adapter contracts still matter. No arbitrary synchronous-Waker
unwind retention or whole-root zero is claimed. Hierarchy/dirty pass/reuse/Z/
overflow/notification and complete Bound lifecycle tracers remain mandatory.
Same Heisenberg submission01a086c1-295d-7240-9713-64aa7e76b2b3 returns
P0/P1/P2=0/0/0, Approve only for conditional SOURCE. It independently confirms
the seven-path scope/inverses and distinguishes those two remaining boundaries.
No runtime, complete A3/Bound/P3 or stage-gate acceptance follows.

Next authority needed: governance predicate plus an adversarial discovery
fixture, preserving real build/vendor/archive exclusions and all growth/lint
limits. This exceeds Integrator's exact-inventory-only authority. Stop Rust
writes here, obtain that scope, then rerun actual governance and same Reviewer
before continuing coupled physical/receipt/output service. No commit/push,
external run, new architecture sidecar or P3 completion claim. Artifact:
/tmp/mini-vpn-p3-caller-migration.BFXd9r/target-readiness.json.

## 2026-09-09 — Governance repair and production-mode compile feedback

The user explicitly authorized Integrator to repair the directory predicate
and add a regression without changing limits or inventories. Mencius changed
only tests/code_growth_governance.rs690->752: root-relative build target
exclusion, preserving every other exclusion, plus one real inventory test.
The latter checks exact production membership for memory_io/memory_ready,
test membership for owner_readiness, route closure and excluded-tree negatives.
Removing only those two changes reproduces the original complete file SHA.

Actual sequence: prior standalone2/6; added focused test fails at memory_io
membership0!=1; minimal predicate repair gives focused1/1; final fresh strict
standalone7/7. Main independently rebuilds/runs7/7, including after the separate
source-factory feedback repair below. Global fmt and diff pass. Same Heisenberg
submission01a088f4-6619-7461-a820-bfdbff055c52 approves governance0/0/0.
The limits, debt inventories, scanner, Target code and routes are unchanged.
Final governance SHA is
bf0804edc9bafe1bbfc0ed618c58a7f916f369ecb48e14c7f04d59e132ff0022.
Integrator evidence lives in /tmp/mini-vpn-p3-governance-target.cEFFkO/:
governance.diff, final-source-audit.json, final-standalone.log and
cargo-all-features.log. The earlier authorization blocker is resolved.

Cargo all-feature governance did not execute its tests: non-test lib compilation
exposed three E0599, including a cfg(test)-only SessionModel::current_leg call
in the recently written supervisor/source_factory activation validator. Main
independently reproduced these3 with all-feature cargo check. This was hidden
by lib-test compilation, not caused by the governance predicate repair.

Under the already-authorized A1–A4 method scope, Main changed only that validator
in supervisor/source_factory.rs175->176. A single existing snapshot supplies
Active/generation; the original OwnerActiveContract.matches retains seal,
nonce, transport binding, version and features. Role/poison/expiry, factory,
reservation SID and admission checks/order remain. No Model getter or visibility
change. The exact-owner initialization and replacement transactions install
model/contract together; ordinary replacement events are rejected and client
replacement cannot change an Owner. Existing standby authentication uses the
same production identity pattern. Neither snapshot nor copied scalar is treated
as physical authority. Mencius finds no legal counterexample in this private
reachable-state invariant; Heisenberg independently reviews and approves
SOURCE0/0/0, submission01a088f9-3b05-71b0-9d2c-76299d0c2e24, with exact inverse.

Post-repair default and all-feature non-test cargo checks each retain only2
missing physical service methods: quarantine_ready_source and visit_ready_source.
The exact all-feature Bound test compile still has those two plus OutputAdmitted
twice (4 total), no local warning; existing vendored smoltcp22. No new runtime
test, Cargo governance GREEN or full Bound/A3/P3 acceptance is claimed. Current
Target/Actor conditional SOURCE obligations, deferred notification/external
return custody and all remaining first-path/full-P3 tracers remain required.
Continue actual selected receipt/output/physical publication and retain real
claim/completion on quarantine failure; no success stubs or new design sidecar.
No commit/push/external run. Main checkpoint:
/tmp/mini-vpn-p3-caller-migration.BFXd9r/governance-and-production-compile.json.

## 2026-09-09 — Admitted publication interval, conditional SOURCE closure

Main changed only owner_ready.rs1015->1041, owner_ready/flow.rs635->755 and
owner_ready/tests/flow.rs273->591 under joint51 A3. Exact inverse removal of
the new declarations/operations/tests and compact docs recovers each original
SHA-256; all other370 governed Rust paths are unchanged. No inventory edit.

The actual installed binding now validates first fault, key/correlation,
BoundActive/ENABLED, original Running phase and absent ACTIVE before its final
admission RMW. It returns a non-Clone metadata sequencing Turn or typed
NotEntered with original first fault. Turn owns no actor, command or claim;
its drop leaves ACTIVE sticky without locking/notifying. Normal finish borrows
the actual completed SessionCommandVisit which the future Root caller must
already store beside its unchanged claim. It validates healthy metadata,
does not recover poison or clear a healthy stale occupant, preserves ENABLED
while ending ACTIVE/taking PENDING, then notifies outside metadata exclusion.
Failure borrows that same completed value and preserves the original gate.
Existing owner-local/external publisher and notifier implementations are unchanged.

Five new metadata-only tracers cover normal deferred/external-after-finish
notification and owner-local silence; six admission rejection cases; source
unwind with sticky interval; poison/revision failure with exact completed-value
reference; and outer Notify unwind with original first cause. The observer
checks ACTIVE clear and no held metadata mutex at actual callback execution.
These are finite metadata recipes, not synthetic physical service evidence.
Actual first compile: missing begin_publication plus the four pre-existing
Bound references. After implementation, exact test compile reports only old4:
quarantine_ready_source/visit_ready_source and OutputAdmitted twice. No test ran,
so this is compile-absence feedback, not runtime RED->GREEN. Default and
all-feature non-test checks, and Cargo all-feature governance, each retain
only the two real missing physical methods. No new local warnings; smoltcp22.
Global fmt, tracked diff, scoped untracked whitespace and standalone7/7 pass.

Same Heisenberg review submission01a08907-44fd-7ac1-869c-a85a084ed9b6:
P0/P1/P2=0/0/0, SOURCE Approve only. Mencius independently closes the API
sequence: original Running/Admitted claim -> binding admission -> real physical
operation -> no-notify return -> Root stores Completed -> borrowed publication
finish -> original ready.finish even after a later publisher fault -> Idle only
after successful completion. Notify unwind/failure retains claim plus actual
completion. Source unwind has no completion and requires real local owning
quarantine before interval close. Those caller operations are NOT implemented
or accepted by this increment; no Bound/A3/full-P3 acceptance is implied.

Next executable work stays the same joint51 packet: actual selected receipt/
source/output service, original C/output ownership and Root completion/quarantine
integration, then run the real tests now blocked at compilation. No stubs or
new prerequisite design sidecar; no commit/push/external action. Checkpoint:
/tmp/mini-vpn-p3-caller-migration.BFXd9r/publication-turn.json.

## 2026-09-09 — Selected receive primitives and Owner DATA scope boundary

Main changed four paths under joint51 A2/section37: session.rs896->938,
tcp/driver_receive.rs148->167, commands/tests/mod.rs2->3 and new private
commands/tests/bound.rs470. The three existing-file inverse transformations
restore their exact original hashes. All other370 prior Rust paths and
governance752 are unchanged; no oversized shrink or inventory edit.

SessionOwnerInbox::poll_selected consumes only the requested original prefetch
or physical receiver. A new receipt enters that same prefetch before original
capacity epoch/Notify; notification unwind therefore preserves actual ownership.
Closing one lane does not consume or close the other. Each selected raw inbox
or TCP receive is a stack-pinned concrete unconstrained poll_fn, ending before
owning state/notification. RESET then Control remain bounded to one poll each;
OrderedSource polls once. Original mixed receives are byte-identical. The
private try-only producer graph has no async sender waiting on a raw permit.

Three tests assemble actual authenticated Owner/flow/Driver, real paired inbox,
original Held/Full transfer, DATA/RESET/ACK and exact permits. They cover lane
isolation, prefetch survival across capacity-notify unwind without duplicate
epoch, and real source/receiver registration after exhausting Tokio's current
cooperative budget. They do not implement C/Root business logic or fake a Bound
service. Cargo metadata resolves Tokio1.52.0 without taskdump/tracing in both
default and all-feature modes; current target is aarch64 macOS. This is source/
configuration evidence, not runtime or cross-platform acceptance.

First exact test compilation reported the expected finite mailbox visibility
and missing poll_selected references plus old4 Bound references. Implementation
removes all seven new errors. Final exact compile still exits101 with only
quarantine_ready_source, visit_ready_source and OutputAdmitted twice; local
warnings0, existing smoltcp22. No new test ran. Default/all-feature non-test
checks and Cargo governance retain only the two physical methods; governance
tests did not run through Cargo. Final global fmt, diff and standalone7/7 pass.

Same Heisenberg first SOURCE review found0/0/1: the unwind test recovered the
Ordered prefetch before selecting Control, so mixed-prefetch fallback could
escape detection. Within-scope repair selects/asserts real Control while
Ordered is prefetched, consumes its epoch, then recovers Ordered without a
second epoch. Final submission01a0891f-a2de-7071-af26-cce2b9dab862 approves
SOURCE0/0/0. Original two DATA/RESET/ACK settlement and production hashes remain.

Before C receipt consumption, Main/Mencius/same Heisenberg independently found
a distinct scope contradiction (submission01a08916-820f-7351-915e-74ed53bec1e7,
P0/P1/P2=0/1/0). Supervisor::apply_driver_input_observed splits event/staging;
its generic reducer Err at supervisor.rs1610 drops the payload and four actual
permits. A queued DATA held while real RESET retires its Flow can reach this
RetiredFlow branch. Existing Client test
terminal_superseded_queued_data_releases_staging_without_poisoning requires
no poison/quarantine and complete release. Owner A2 instead requires retained
staging. The exact joint51 Supervisor write axis freezes observation/reducer
semantics, so changing the generic branch would both exceed scope and change
Client compatibility. This is source diagnosis, not a new reproduced runtime
regression. Neither Supervisor nor C consumption was patched.

Next authority decision: allow only original Owner observed DATA entrance and
complete error settlement plus its private exact-ownership tests; retain the
actual staging/original typed error, preserve Client/legacy behavior, reducer,
factory/public Interface and growth limits. Amend the existing packet only
after explicit approval, not via another prerequisite sidecar. Then implement
the real C/Root receipt/output path and run the still-unexecuted tests. P3 is
incomplete, not redefined as these primitives. No commit/push/external action.
Checkpoint: /tmp/mini-vpn-p3-caller-migration.BFXd9r/selected-receive.json.

## 2026-09-10 — Owner-only DATA custody, conditional SOURCE closure

The user's confirmation authorizes only original Owner observed DATA settlement
and its private tests. The exception is recorded in existing joint51; no new
architecture sidecar. Main changed Supervisor5682->5597, new private
supervisor/driver_input.rs337, OwnerTarget9635/net-zero test route and new
owner_target/tests/owner_data_retention.rs207. First, the complete113-line
Driver reduction method moved with original block SHA
20b7c488424cec7e297baa150aa92530b7a53103f613a0bf0c0dee00701840b3;
inverse and compilation retained only old4. Later behavior is separate from
that Move. Full parent inverses restore both original file hashes. Client's
32-line late-RESET test is byte-identical, SHA
3697fd819a9df78929939270ef0c70453b218550287a85a415b0399bc792938c.

Only apply_owner_command_observed dispatches DATA to the retained path. Three
legacy/Client callers retain original release semantics; the reducer is not
duplicated or changed. Owner DATA's actual staging enters the original Vec
before healthy/provenance/reduction. The guard exclusively borrows Supervisor;
its index only locates that actual storage. bind_replay replaces the same slot
without a callback/fallible gap. The actual key/position are checked before
pop->original map insertion; subsequent observation errors/unwind preserve
the map. Drop only sets poison, without resource movement/allocation/Notify;
success explicitly disarms it. Original errors return unchanged. New private
custody contradictions fail closed as DataCustodyInvariant. First Vec push may
allocate; this is not allocation-free or full Bound capacity acceptance.

Three tests use actual source DATA/RESET, full LocalFlow, original Executor
observation gate and flow/session bytes/segments. They cover late DATA after
real RESET, second DATA after poison without model reduction, and single-use
root-local test unwind at staging, Replay binding and map publication. Original
permits remain until Executor destruction; this is not whole-root shutdown.
Reviewer P2 found the initial extractor backing-Drop counter invalid because
TCP copies/normalizes Bytes. The repaired test inspects actual queued payload
and permits, without claiming a post-retention payload getter or bypassing TCP.

Actual compile evidence: first test and pure Move each retain old4; unwind
recipe first adds exactly missing test enum/armer (6 total), then those disappear
after hooks (old4 only). No test ran. Default/all-feature lib checks each lack
only quarantine_ready_source and visit_ready_source; lib-test additionally
lacks OutputAdmitted twice. Local warnings0, existing smoltcp22. Global fmt and
diff pass. Integrator alone changes the exact Supervisor ratchet5682->5597;
Main independently rebuilds strict standalone governance and runs7/7.
Integrator's fresh standalone also passes7/7. Cargo offline all-feature
governance exits101 at the same two physical methods, before any test runs.
Governance remains752 lines; its sole numeric change is independently inverted.
Evidence: /tmp/mini-vpn-p3-supervisor-ratchet.RrzbmD/.

Same Heisenberg formal review01a08973-7f2b-7430-aad9-07e01d530f01:
P0/P1/P2=0/0/0, Approve SOURCE only; Architecture9/CA9/SD8. No additional
structural optimization requested. To complete acceptance, integrate actual
Bound receipt/output/Root, close capacity/first-allocation and lifecycle work,
and execute the outstanding runtime matrix. The former authorization blocker
is resolved, not full P3. No commit/push/external run.
Checkpoint: /tmp/mini-vpn-p3-owner-data.4eFW9v/owner-data.json.

## 2026-09-10 — Selected Controller receipt and original observed settlement, SOURCE only

Main implemented the actual consumer within joint51, not a new architecture
sidecar or a metadata substitute for the complete Bound path. Six feature files:
C3536/net-zero plus ready_service140; Executor9634 plus service367;
owner_ready/flow830 and continuation1037. No new test or test-body changes.
The existing bound_active_owner_source_reaches_real_output_once remains the
first complete-path tracer; it has not executed.

The existing ordinary output guard prepares unentered, calls only the paired
selected inbox with its original named controlled Waker, and restores itself
on genuine Pending/closed lane. C stores every returned command in Held before
full Flow/Driver/position/transit/phase validation and original clock sampling.
It marks Unknown before consuming the command through the original Executor
Owner observation gate. The original Supervisor checks DATA permit origin and
retains actual staging/replay on error; effects/observations and typed errors
use original C/output ledgers. Successful reduction settles exactly that transit
before effects may retire its Flow. Carrying tombstone pressure moves the same
OPEN/CLOSE/RESET/HalfClose feedback to original Flow pending completion and
admits an empty ordinary page. The existing private Failure name is preserved
for source-control feedback; reconstruction now accepts LocalClose/LocalReset.
No second reducer, output ledger, resource getter or legacy fallback was added.

Main source audit caught the cold-only comparison at the new live call site:
compare_flow_position intentionally rejects BoundActive. The repair introduces
active-only validate_receipt_flow with exact correlation, ENABLED/ACTIVE,
Running claim interval, association/generation and bidirectional retirement.
Cold comparison remains unchanged. Invalid metadata records the original fault
and deferred pending notification, without recovering poisoned exclusion.
A mechanical first patch placed ReadyReceipt in the wrong enum; compilation
reported six extra missing-variant errors, corrected at the intended error
anchor. No runtime failure or semantics inference is claimed from that repair.

Final executed checks: exact Bound lib-test compile exits101 at only the old4
references (two OutputAdmitted uses and two physical service methods); default
and all-feature non-test checks each exit101 at only the old2 physical methods.
Local warnings0, existing smoltcp22. Global fmt and diff pass. Integrator alone
ratchets OwnerTarget9635->9634 in governance752, with exact inverse and fresh
strict standalone7/7. Main independently recompiles current standalone and
passes7/7 after active validation. Cargo all-feature governance still exits101
before tests at old2; provenance/runtime suites were not run.
Integrator evidence: /tmp/mini-vpn-p3-owner-target-ratchet.rZmjhD/.

Same Heisenberg source feedback/re-review closes P0/P1/P2=0/0/0, Approve only
this consumer increment. The complete producer/transit increment, thirteen
categories, Bound read/RESET cutover, physical selected service and root-first
Completed/claim/publication/quarantine protocol remain mandatory prerequisites
of the first complete running path. Existing full-P3 capacity/pressure/credit/
retirement/fairness/lifecycle/runtime acceptance obligations remain unchanged;
no A1–A4 or P3 checkbox is closed. No commit/push/external operation.
Checkpoint: /tmp/mini-vpn-p3-ready-receipt.oOToAq/ready-receipt.json.

## 2026-09-10 — Selected actual Driver producer and retained send service, SOURCE only

Six existing paths changed under joint51. Neutral ready1059/flow1028/test-flow648;
commands694/bound test693/old send_cells771. Whole-Executor visit_ready_send
stays beside private command storage so it can borrow original actual resources
without exporting Driver/receipt/getter/callback interfaces. One active indexed
candidate is selected under the original key/domain/Running/ACTIVE interval;
its cursor advances before actual service. The original map/position/generation,
backing capacity and complete LocalFlow are checked bidirectionally before pull.

The private real Driver primitive preflights transit.checked_add before poll.
Ready(Some) moves its actual command to Held, assigns the real record charge
without callbacks, then validates full LocalFlow. Pending/None adds no charge.
An occupied cell is separately transferred/rearmed through the original
Full/Closed/Unknown/observer path; mailbox transfer never decrements transit.
Original C receipt settlement remains the consumer. The new finish operation
keeps a concurrent newer revision and requests slot-credit reconsideration only
after actual Ordered dequeue custody. No Target read, second poll or reducer
is fused into this send-source unit; no source/permit is returned to outer Root.

Independent review initially found0/1/0: imported Retiring sources were due in
Driver trees, but the new selector correctly required Live ordinary service.
The import repair preserves every original due/revision, source association,
ticket and retirement obligation while excluding only Driver eligibility for
Retiring. Existing category13 and same-generation publisher redirection remain.
No retirement completion/reclaim/zero or terminal-drain runtime proof is claimed.

Four added tests: real DATA transit-overflow/pull/transfer/Pending; equal numeric
IDs with foreign LocalFlow authority retain DATA on quarantine; actual metadata
binding/active dirty-wake clear/recheck; legal Retiring import remains correlated
and selects continuation instead of false Corrupt. The retirement wake recipe
is before activation, not evidence of activation-after-wake runtime closure.
One legacy test pattern mechanically follows the extended actual origin tuple;
its old send/unwind behavior and all other original assertions are unchanged.
Initial tracer compilation exposed the expected new missing pull method/variant
in addition to old4. Those new errors disappear. A new assertion's unwrap caused
E0277 on a non-Debug borrowed finish failure; changing to is_ok fixed only test
diagnostics, without production derive or Interface growth. No test has run.

Final actual gates: Bound test compilation exits101 only at old4; default and
all-feature --offline --locked library checks each exit101 only at old2 physical
methods. Cargo all-feature governance also stops at old2 before tests. Local
warnings0, existing vendored smoltcp22. Fresh strict standalone governance7/7;
global fmt and diff pass. Full376-path SHA audit shows exactly the six paths
above, no addition/deletion, HEAD unchanged. No oversized shrink and no ratchet
or governance edit. Intermediate Node hash-command parsing failures performed
no writes; git-ls-files/shasum supplied the final complete audit.

Same Heisenberg final SOURCE submission01a089a7-1cdf-75c2-8af2-e171093c0ecd:
P0/P1/P2=0/0/0, Approve only this increment, original P1 closed. Main's bounded
assessment remains Architecture9/CA9/SD8: inward ownership and exact custody
are explicit, but actual full-path/capacity/lifecycle evidence is still missing.
No full A/P3 acceptance, provenance/runtime result, commit/push or external run.

Next first-path prerequisites remain: guarded C source-only caller in original
recovery_service ledger; real Target read/category operations and Bound read/RESET
cutover; original physical selected/output operations; Root retains actual
Completed beside original claim before Notify and finishes admitted work after
later fault, with owning physical quarantine before interval close. Execute the
existing real Bound-to-FIFO tracer and accumulated regressions once wired.
Full-P3 credit/retirement/cold peak/pressure/fairness/lifecycle/runtime/review and
later-stage obligations in joint51 remain; no checklist item is removed.
Checkpoint: /tmp/mini-vpn-p3-driver-producer.cEj9Ex/driver-producer.json.

## 2026-09-10 — First real Active path, runtime GREEN with open review

This is implementation/runtime progress, NOT an accepted A1–A4 packet or P3.
The same original Bound tracer now compiles and executes the complete normal
Target read -> actual indexed Driver pull -> separate command/mailbox transfer
-> checked C held receipt/observed reduction -> original output page -> actual
actor FIFO path. Its original peer verification decodes DATA once; the real
selector subsequently parks without duplicate output. The fixture does not
perform reduction or substitute a success port. First exact1/1, serial10/10,
and post-capacity exact1/1 pass.

Main changed18 existing governed Rust paths, no new/deleted path; Integrator
changed only governance for the exact provenance shrink. Complete376-path
baseline/current hashes are in the checkpoint below. HEAD remains532d7660.
Original oversized Executor stays9634; C3536 unchanged; physical746/service404,
continuation1237/tests462, Root516; ready1126/flow1278/test697;
Executor service587/read266/completion420; C service221; commands694/test693.
Both outer claim-test migrations borrow the same actual claim; no address or
mailbox assertion was replaced. Their leaves remain688/188.

The original recovery_service ledger now covers finite Send/Target work as well
as its still-test-only B policy. Source work uses actual original Executor ports;
inbox service uses the existing ordinary guard; output and Effects use the
existing page/continuation transactions through actual actor parts. Output
admission and flush are separate charged units; Pending calls the actual Actor
registration/flush port. Unavailable lifecycle installation remains owned and
ineligible. This is not full credit, failure, cleanup or fairness acceptance.

Root stores actual Completed beside the original Running claim before outward
notification. On a modeled finish failure it still invokes original ready.finish
and preserves both values. Source quarantine owns the physical C/output state
before ending the publication interval. Same Reviewer found no additional
ordering P1 here, but identified the late-wake isolation defect below.

Capacity self-review produced exact runtime RED: an unrelated inbox finish
cleared Full demand from3 to0. The first narrow repair preserved the mask but
the fixture activated only Inbox/Continuation, so its capacity-wake assertion
correctly stayed red. A sibling-private metadata mutation failed E0616; the
final fixture instead uses original activation with all five eligible sources.
No visibility was widened. Typed optional replacement now distinguishes
Some(actual full commands summary), including Some(0), from None when commands
did not run. The additional deferred-Send test proves preserve then actual clear.
Both are in the green17-test metadata group, not a full physical pressure claim.

Independent same Heisenberg final progress review, submission
01a089c9-57eb-7f00-a887-280fb5f15873: P0/P1/P2=0/3/0, Request Changes.
The capacity P1 is SOURCE-closed. Three P1s remain executable repair work:

1. service.rs266 stages RESET in the same Failure phase used for reducer-pressure
   retry. completion.rs retry can replay before the queued Driver RESET and the
   receipt preflight can reject legitimate in-flight DATA. Distinguish those
   phases at the original Flow, preserve actual events/commands, and test order.
2. Pending failure/completion does not suspend the Flow's ReadReady/ReadAttempt
   eligibility. service.rs's legitimate pending-completion check then escalates
   retained work into C failure. Suspend/resume original metadata with a real
   selected-flow tracer, not a false successful Target poll.
3. After resident-local quarantine, the same generation's retained Target/Output
   registration can still wake. flow.rs1168 attempts publish on Quarantined cells
   and poisons the shared Root, stopping unrelated residents. Identify legitimate
   quarantined late publication without reopening DATA or hiding poison,
   identity mismatch or an existing root-fatal cause.

Review confirms normal DATA runtime evidence but does not accept these branches,
release-only credit pass, physical lifecycle matrix or full P3. No production
repair for the three remaining findings was made after this final review.

Final actual gates: metadata17/17, commands41/41, Root24/24, continuation16/16;
Bound1/1; default/all-feature offline locked library checks PASS; repository
provenance17/17; Knife162/162. The first Knife run was161/162 because its source
test still recognized the old guard literal/cfg Drop/enter() call. Only those
original source-shape/order assertions were migrated; focused1/1 then162/162.
Rustfmt caused Knife2626->2624. Integrator alone lowers that exact inventory;
its fresh strict standalone7/7 and Main post-ratchet Cargo governance7/7 pass.
Governance remains752; final global fmt, tracked diff and untracked Rust
trailing-whitespace checks pass. Post-review hash difference is that one
Integrator file only, matching its reported5569d895...9014b.

Warnings are NOT zero: successful library/test compilation exposes one
private_interfaces warning at the existing opaque CurrentOnly enum payload,
plus third_party smoltcp22. Knife's isolated imported sources additionally emit
one unused-import and141 dead-code warnings. The initial diagnostic filter
looked for "vendor" and miscounted third_party as local; corrected collection
classifies these separately. No warning suppression or visibility workaround
was introduced. Compilation errors from prior checkpoints hid this full result.

Next: repair the three P1s with causal focused RED/GREEN and same Reviewer;
resolve the opaque-seam warning without opening fields; complete release-only
credit rearm, actual output-Full/Target wake/observer-cancel/unknown/stop evidence,
then broader cold/capacity/fairness/lifecycle obligations in joint51. Normal-path
GREEN does not delete any checklist obligation. P3/Case C/R7 remain open.
The persistent goal remains active. No new sidecar, commit/push, external run,
P4–P7 implementation or full-root zero claim.

Checkpoint: /tmp/mini-vpn-p3-active-path.pZjTEW/active-path.json.

## 2026-09-10 — Bound late-wake isolation repaired; two Flow findings remain

This is a causal two-file repair, not A1–A4 or full-P3 acceptance. Main changed
only owner_ready/flow.rs1278->1297 and owner_ready/tests/flow.rs697->754.
All376 Rust paths remain present; comparison with the previous Active-path
checkpoint changes exactly these two hashes. No grandfather shrink/inventory
change, new source, production Interface or resource ledger was introduced.
HEAD remains532d7660e3b862849aa7650589b182fd491df212.

New exact test: quarantined_bound_late_wakes_preserve_the_other_live_session.
Initial diagnostic assertions demanded Debug on private ReadyKey/ReadyPhase,
so compilation failed E0277 before any behavioral test. Only test equality/
matches formatting was corrected, without adding a production derive. The
executed expected RED then observed Err(Corrupt) versus Ok after a retained
same-generation Output/Target notification. This is the actual fault evidence.

Minimum GREEN recognizes fully closed quarantine only after original lock/key
and active<->ENABLED validation. All fixed source/process cells must be Dormant
or Quarantined and both ordinary masks zero. Ignore that legitimate hint before
Flow due/revision, PENDING, index or Notify mutation. Preserve binding/charges
and any earlier root fault; poison or false quarantine still faults. There is
no Flow-map scan/allocation or physical resource settlement in this operation.
Exact1/1 passed. Extended characterization covers the interval before owning
finish (ACTIVE retained), finish-after late hints, another actual metadata
binding's claim/finish, and a deliberately inconsistent quarantine still giving
Corrupt. The final18-test metadata group passes all these cases.

Same Heisenberg submission01a089dc-2781-7202-83b7-d692a31a073a returns this
finding P0/P1/P2=0/0/0, SOURCE closed; entire packet remains0/2/0 Request Changes.
Reviewer did not run Cargo. Staged RESET versus reducer-pressure retry,
legitimate in-flight DATA and retained-completion Read eligibility remain open.
Metadata and ordinary Bound test success do not prove physical cleanup/zero,
credit-release fairness, failure pressure or the complete lifecycle matrix.

Main's executed all-feature/offline/locked library filters with nocapture and
one test thread: exact late-wake1/1; owner_ready::tests::18/18;
owner::server_root::tests::24/24; real bound_active_owner_source_reaches_real_output_once1/1.
Fresh rustc -D warnings --edition=2024 standalone governance7/7; Cargo
all-feature governance7/7, repository provenance17/17, Knife162/162. Global
cargo fmt --all -- --check and git diff --check pass. Default and all-feature
cargo check --offline --locked --lib pass. No full all-lib run or new10-repeat
claim; previous Bound10/10 remains its own earlier evidence. Warnings remain
third_party smoltcp22 plus existing private_interfaces1; Knife imported code
also reports unused_imports1/dead_code141. No suppression/widening repair.

Mencius independently audited next test placement twice, read-only/no Cargo.
Bound leaf693 has107 remaining lines, but lacks an actual bound Executor fixture;
its SourceFixture owns a bare Supervisor/factory/ports. Existing Executor helpers
are private in the sibling owner_target::tests tree; C service ports are visible
but require real physical handoff/runtime/LegIo/FIFO activation, and C cannot
call owner-private Root. Neither copied counters nor a cold-only binding tests
active receipt custody. Do not invent a getter or move new tests into a path
whose grant is only the old characterization Move.

Proposed minimum next scope, NOT authorized or written: add private
src/owned_upstream/owner_target/commands/tests/bound/flow_lifecycle.rs (<=800),
with its parent's private declaration and characterized fixture extraction.
Keep actual original Executor/Driver/pair/selected publication/receipt/transit
and complete RESET/read lifecycle in this test subtree. No new production
Interface, callback, Authority, governance entry or external gate. First trace
must distinguish queued RESET from reducer-returned completion and retain
already-in-flight DATA; then selected Read suspension/rearm. Existing two-P1
production change axes stay those already frozen in joint51. Seek only this
explicit new test-path scope; complete current repair before that decision.

Current docs and learning memory record this closure and pending scope; no new
architecture sidecar, commit/push, external run or later P4–P7 work. P3 remains
active and incomplete. Checkpoint includes376 previous/current source hashes:
/tmp/mini-vpn-p3-late-wake.bta0aB/late-wake.json.

## 2026-09-10 — Bound Flow lifecycle repair: four component tracers

User explicitly authorized the private commands/tests/bound/flow_lifecycle.rs
leaf, parent private declaration and characterized same-file fixture extraction.
The existing production repair axes remain within joint51. No getter, widened
existing visibility, second completion ledger or governance exception was added.
Main is the sole overlapping Writer. Same Heisenberg source re-review
submission01a08a0a-3fd5-7580-8dbc-44d2c54aefcf now returns SOURCE/full-packet
P0/P1/P2=0/3/0, Request Changes. This is component evidence, not physical
failure/retirement closure. No Rust was changed after that review result.

Fresh pre-write rustc -D warnings governance7/7. Original commands41/41 passed
before and after the same-Module source_leg fixture extraction. Original test
bodies were not changed. New private fixture uses the authenticated model,
original Executor/MemoryTarget/factory/Driver, actual consuming binding, pair,
selected publication and observed receipt. Its metadata activation deliberately
does not claim a physical handoff, checked C activation or Root cleanup.

Executed causality:

- staged_reset_waits_for_its_driver_receipt_after_terminal_capacity_release:
  actual old TerminalGraceExpired emitted RESET before its queued receipt, an
  exact runtime RED. StagedReset now resides at the same pending_completion
  field but cannot lower into a reducer command. Retry scans skip it. Only the
  original Driver RESET receipt can settle it; exact1/1 GREEN.
- in_flight_data_settles_without_erasing_the_staged_reset: the fixture initially
  attempted Ordered under the initial Control selection; only actual visit
  ordering was repaired. The subsequent real RED was PendingTargetCompletion
  rejecting already-owned DATA. Original receipt/association/transit preflight
  remains; DATA reaches the original reducer without clearing pending RESET.
  Four bytes/one segment remain in original ownership; exact1/1 GREEN.
- staged_failure_suspends_reads_even_after_a_real_target_notification: actual
  MemoryTarget read-interest registration followed by failure staging and real
  feed_read notification selected Read again (runtime RED). The fixed metadata
  now distinguishes eligible, suspended and EOF-closed read phases, gating late
  hints and ordered-credit rearm while preserving Control/Ordered settlement.
  The paired inbox remains alive; four late bytes remain at Target. Exact GREEN.
- received_reset_retries_once_after_real_terminal_pressure_releases: actual
  Driver RESET receipt hits the genuinely full Z=1 terminal capacity and is
  retained as reducer-returned Failure, not staged intent. Actual expiry frees
  capacity and yields exactly one RESET, no duplicate inbox receipt. Directly
  GREEN characterization; no fabricated RED or altered reducer pressure.

Final current Rust lines: owner_target.rs9634 (exact unchanged grandfather),
completion.rs446, service.rs634, owner_ready/flow.rs1387, bound.rs699, new
flow_lifecycle.rs404. Comparison against the fresh376-source baseline changes
exactly these six paths (including the one new leaf); all previous paths remain.
No ratchet edit. Formatting was restricted to these files before global check.

Executed all-feature/offline/locked gates with one test thread: four focused
tests4/4 and each10 serial repetitions; all Target122/122 (including commands45),
metadata18/18, real owner::server_root::tests24/24, continuation16/16, real Bound
DATA1/1 plus10/10 repeats. An initial wrong Root filter ran0 tests and is excluded;
the corrected namespace supplied24 actual tests. Repository provenance17/17,
Knife162/162, fresh strict standalone and Cargo governance7/7 each. Global fmt,
tracked diff and default/all-feature library checks pass. Library warnings remain
third_party smoltcp22 plus existing private_interfaces1; Knife also emits existing
unused_imports1/dead_code141. No warning suppression/visibility workaround.

Independent Mencius scope audit submission01a08a0f-f277-7c31-8d09-37d7658297ee
passes: baseline376 matches the prior checkpoint, current377/377 hashes and
Git Rust path set match, exactly six changes/one new path/no deletion. All line
counts above are independently verified. Governance752 is byte-identical to
baseline (5569d895af035fcca0f2d5158a2cee0aee875a5c5744d67d1c18a89af689014b),
and all73 untracked files (59 Rust) have no whitespace diagnostics. No ratchet,
write, Cargo, external or delegated review was performed by this audit.

Same Reviewer closes the staged-failure Read eligibility P1: Suspended/Closed
block real late read hints and ordered-credit reopening. He also confirms staged
RESET cannot manufacture retry feedback and DATA-first cannot clear it. The
generic nonterminal-HalfClose resume concern has no demonstrated reachable
counterexample: reducer checks Z only when send.final_accepted is already true.
No extra finding or speculative resume operation was added for that concern.

Remaining SOURCE findings:

1. completion.rs359 deletes the Flow after Target join, before queued/cell/inbox
   commands settle. RESET-first causes actual late DATA to lose its owner at
   service.rs335. Keep real FlowFinished plus full source/remaining transit;
   do not replay into the retired reducer or merely delay remove without a
   matching terminal disposition. Current DATA-first test does not cover this.
2. service.rs399 exempts only DATA. Actual EOF can already produce Ordered
   CLOSE before reverse write/HalfClose failure stages RESET. That CLOSE must
   settle independently, without clearing the exact pending RESET; non-DATA
   is not sufficient evidence of matching completion (service.rs417).
3. service.rs450 unconditionally decreases transit before checking the result,
   then service.rs502 retains the actual Z-rejected RESET. This inherited hole
   violates flow-completion-contract197: genuine pressure must keep1->1->0,
   including exact local-versus-Driver origin on retry. The green pressure
   characterization did not yet assert that charge.

Limited Architecture/Clean/System/DDIA scores are8/8/6/6. Main's structural
assessment is8/10: consolidate conditional admission and replace ambiguous
completion data with the original owning phase, rather than scattered special
cases. Reaching10 requires exact terminal/source/Target/command settlement,
closed identity/retirement invariants and both-order/pressure/lifecycle tests.
No performance tuning is proposed. Full P3 is not accepted. Target Adapter10/10
and Supervisor48/48 were additionally executed and green before this review.

Main applied the structural-convergence stop: no further Rust or behavioral
repair after the review. Existing accepted Flow completion sections3–6 already
specify the required single owning phase and bounded terminal transaction;
there is no new architecture sidecar. A bounded audit now checks the missing
Owner-only revocation against joint51's exact TCP write axes before proposing
any next implementation. The generic terminal operation at tcp.rs1580/1626
bulk-drains and publishes TUN terminal, so it cannot be silently reused.

Independent Mencius Driver audit submission01a08a16-b694-7293-a105-2af7780342d8
confirms there is no already-authorized non-bulk Owner revocation operation.
seal_for_local_abandon also bulk-drains/creates Client abandon feedback;
TcpFactoryAdmission::close affects future Flow admission, not existing ports.
Current selected Control/Ordered polls can drain after closure but cannot
revoke producer admission. Control drained means both receivers return None.
Minimum proposed scope extension is the existing167-line tcp/driver_receive.rs
behavior axis: whole Owner-only terminal-proof validation/admission revocation
at Driver Authority. Its child can access original private fields; no getter,
re-export, new Root/ledger or tcp.rs change is expected. A required new parent
storage anchor would need separate authority, not the factory-Move grant.

The pre-write exit must establish exclusive complete Owner Flow/Driver and no
split/export/mixed route; account for surviving source reservations, completion
permits and Sink/HalfClose deliveries; prove their commits respect the same
admission lock/revocation; and preserve controlled-wake ownership on close or
callback unwind. Then freeze one original-Flow transaction through revocation,
selected drain, exact terminal DATA/CLOSE/RESET disposition and charge release.
First-A may retain nonzero TerminalSettling but may not delete/reuse F or call
it completed retirement. Main awaits only this explicit new behavior-axis
authority; no second local Rust repair or new architecture sidecar was made.

Skills used: diagnose and tdd required separate real causal failures and
characterized fixture movement; clean-architecture, system-design and
ddia-systems kept actual custody/flow control at the owning Module; code-review
produced the independent0/3/0 stop rather than accepting four green tests;
improve-codebase-architecture and refactoring-patterns selected one original
Flow settlement deepening and prohibited scattered predicate/counter patches;
self-improving-agent recorded the retry/receipt/transit lessons locally. No
global skill, hook or external memory was modified. Deleting this settlement
Module would scatter its invariants back across C, Driver and readiness, so
the selected deepening increases Locality rather than adding a pass-through.

In particular,
normal DATA-before-RESET component evidence does not prove the reverse order's
physical disposition, full retirement, release-only credit fairness or the full
pressure/unknown/unwind/shutdown matrix. All joint51 A/B/C obligations remain;
P3/Case C/R7 are incomplete. No new sidecar, commit/push, P4–P7 or external run.
Checkpoint: /tmp/mini-vpn-p3-flow-lifecycle.OV2pdN/flow-lifecycle.json.

## 2026-09-10 — Owner-only revocation grant and complete-path feasibility

The user explicitly granted the previously requested behavior-axis extension
at existing tcp/driver_receive.rs: actual Owner terminal-proof/pair validation,
same-lock producer revocation and selected drain. tcp.rs is byte-frozen; new
parent storage needs separate authority. No getter/re-export, public Client/TUN
terminal change, new Root/parallel ledger, growth exception, external run or
commit/push follows from this grant. The existing joint51 table and compact
navigation record that exact grant; no architecture sidecar was added.

Entry verification compared all377 prior Rust fingerprints with the current
files: no differences. tcp.rs SHA-256 is
8e0af23c78b817929d725d48335143b306f2314e0998a21f990bd70ecc82710b;
governance remains752 lines, SHA-256
5569d895af035fcca0f2d5158a2cee0aee875a5c5744d67d1c18a89af689014b.
Fresh baseline executions, not new behavior evidence:

- `rustc -D warnings --edition=2024 --test tests/code_growth_governance.rs`
  compiled to this entry's fresh temporary directory; the binary passed7/7.
- Cargo `test --offline --locked --all-features --lib
  owned_upstream::owner_target::commands::tests::bound::flow_lifecycle::
  -- --nocapture --test-threads=1` passed4/4,1723 filtered, cargo exit0.

Mencius's two bounded source audits establish the following feasibility facts:

- Driver child can compare full LocalFlow, Owner TargetToClient direction and
  actual shared PortState identity without a TCP storage addition. Original
  checked factory identity remains necessary. Same-lock reservation/action
  checks supplement the non-escaping Owner producer call graph; counters alone
  cannot prove exclusivity. Outstanding DATA bytes are legitimate custody.
- Receiver close does not notify a parked receiver. It can wake sender waiters
  in general, but these closed Owner lanes only use try-send/try-reserve and
  expose no async sender/closed waiter. Keep selected Control/Ordered drain due.
  Factory progress may synchronously unwind; retain terminal/work/source first,
  release the admission lock before notification, and preserve unknown custody.
- LocalClose at resumable/session.rs1894 does not check Z or finish the Flow.
  The Z-full case in supervisor's final-ACK characterization accepts CLOSE;
  final ACK is the rejected event. No speculative second rejected-CLOSE slot
  is justified. Independent CLOSE cannot settle a pending RESET.
- retire_after_join means grace expired before actual Target join, not semantic
  Flow terminal. Current metadata Retiring reroutes wakes to Retirement and
  forbids ordinary Driver pulls. A terminal marker alone cannot enable drain.
  Completion/Join/Retirement trees already exist, but their real Bound service
  and eligibility are not implemented; old full-map retry/join is not one
  selected unit. Drained must also be distinguished from Pending at the Flow.

Main selected one original Flow settlement phase, failed/sink work custody and
origin-preserving feedback, rather than three independent predicate/count
repairs. The same Heisenberg was asked to challenge that executable choice
before Rust, including the physical output boundary. Main's additional source
check confirms an existing joint40 dependency: TerminalGraceStarted enters
the sole lifecycle port; pending lifecycle excludes ready sources and ordinary
receipt in continuation.rs239/499/607, while CurrentOnly has no normal action
installer/due consumer. OwnerPhysicalLifecycleSlots retains opaque actions,
not clock/deadline/due ownership. Retention is not completed terminal service;
no output-guard bypass or pretend timer was proposed.

At this interim record, no Rust edit or new RED/GREEN has occurred. The three
prior SOURCE P1s and all remaining joint51 A/B/C obligations remain open.
Same-Heisenberg executable-choice review concluded0/1/0 Request Changes for
the complete C/root terminal boundary. He explicitly permits original-Flow
component convergence within existing scope, but the actual lifecycle consumer
or an explicitly reviewed settlement-only admission amendment must be selected
before complete terminal acceptance. This does not authorize a guard bypass or
new physical owning fields automatically. The original three SOURCE P1s remain.
Checkpoint: /tmp/mini-vpn-p3-terminal.93j9Ua/entry.json.

### Preparatory completion implementation Move: accepted local boundary

Only the complete44-line PendingTargetCompletion impl was moved from
owner_target.rs to existing owner_target/completion.rs, with the two necessary
type imports. All actual calls to kind/into_command/from_carried_command were
already in this child. No visibility, forwarding function, new file, field,
test name, failure phase or runtime behavior changed. Parent9634->9590;
child446->491. Original/moved block SHA-256 is
eb288275dfe281ccd4c03a4d6092676c44e8ec6781cf977c18787fd3e2735342.
Parent inverse reconstruction restores its entry SHA exactly; parent symbol0,
child symbol1. The first fmt check found only required-import ordering; Main
applied that exact format repair under standing causal self-repair authority.

Actual gates:

- Before Move Owner Target122/122; after Move122/122, including all four current
  lifecycle tests. Knife16 provenance162/162; repository provenance17/17.
- Global cargo fmt --check and targeted tracked/untracked whitespace pass.
  Default and all-features offline/locked library check pass.
- Pre-ratchet standalone was6/7, only expected9634->9590 exact shrink. Mencius
  independently changed only the owner_target.rs entry9_634->9_590. Governance
  remains752 lines; full inverse replacement matches its original hash.
  Fresh strict standalone compile/run and Cargo all-features governance pass7/7.
  Integrator logs: /tmp/mini-vpn-p3-completion-ratchet.YDVIjq/.
- Same-Heisenberg bounded pure-Move SOURCE review:0/0/0 Approve. This does not
  approve a phase implementation, close the original three P1s or the complete
  physical lifecycle blocker. He did not rerun project tests; gates above are
  Main/Integrator executions.

No behavior RED/GREEN was added. The selected Flow settlement implementation
and full-P3 obligations are still open. Before claiming an end-to-end terminal
path, select the real owning installer/clock/deadline/due consumer and its exact
write scope, or obtain a reviewed explicit settlement-only admission amendment.
Main recommends the real lifecycle consumer because P3 already requires normal
expiry; no duplicated timer or weakening of lifecycle/output exclusion is
authorized. Component implementation remains within its existing authorization;
the end-to-end architecture choice is not implied by the Driver grant.

Skills used this turn: diagnose/tdd separated baseline from absent behavior
evidence; clean-architecture/system-design/ddia-systems and improve-codebase-
architecture kept source/work/terminal authority at the original Flow and
exposed the real output-consumer dependency. refactoring-patterns guided the
complete characterized impl Move with no delegate. code-review kept both
source-P1 and complete-path verdicts separate from Move approval;
self-improving-agent recorded producer/revocation/retirement lessons locally.
No global skill, external memory, commit/push, new sidecar or external gate.

## 2026-09-10 — Terminal-first late DATA: validated component RED

This is executed failure evidence, not a production repair or P1 closure.
Entry verified all377 Rust hashes against the accepted completion Move;
fresh strict standalone governance7/7 and original four lifecycle tests4/4.
Only existing commands/tests/bound/flow_lifecycle.rs changed404->507. Its
original404-line prefix SHA remains
cab28a677dd692f4371434eceac9c77d0e26ab520c76d27d136817075cf2c2af.

The new reset_first_keeps_the_original_flow_until_late_data_disposition uses
the actual Target/source/Driver/pair: DATA is produced first, original RESET
is pulled/sent on the actual Control claim, its received command stays owned
locally, then finish/claim switches to Ordered and moves real DATA into the
paired mailbox before RESET reduction. Genuine other-Flow grace expiry frees
Z. RESET reduction produces exactly one matching TerminalGraceStarted; the
next assertion fails because Target join removed the original Flow. The late
DATA remains mailbox-owned at this failure; subsequent source/byte-disposition
and retained-F assertions have not executed.

Two initial fixture attempts violated claimed-source selection, both failing
ReadyReceiptInvariant before the intended assertion. Read-only claim/selection
audit corrected actual turn and receipt order without production changes,
manual ready bits, transit writes, altered reducer or relaxed assertions.
Final exact command compiled and ran1 test,0/1 with1727 filtered; its causal
retention RED reproduced before and after rustfmt, now at leaf475. The four
original tests reran4/4 with the new test explicitly skipped. Global fmt and
git diff --check pass; post-change standalone governance7/7; source prefix,
final newline and trailing-whitespace checks pass. No post-change broad suite,
Cargo governance, implementation GREEN or production acceptance is claimed.

Same Heisenberg read-only test-validity review returns0/0/0 Approve for this
COMPONENT falsifier only. He verified original-prefix identity and actual
ownership/claim order; he did not run tests. Existing three SOURCE P1s and the
complete C/root lifecycle-consumer blocker remain open. Full377-path audit
shows only this test leaf changed; production and governance are unchanged.

Continue the already-authorized original Flow phase GREEN in the existing
joint51 scope. No independent predicate/counter exception or full-map Bound
retry may substitute for that model; no new lifecycle scope is inferred.
The goal remains active. Checkpoint:
/tmp/mini-vpn-p3-terminal-behavior.Jv22NZ/entry.json.
diagnose/tdd separated fixture failures from the real RED; architecture skills
kept the chosen single-Flow scope; code-review checked only falsifier validity;
self-improving-agent retained the claim-order lesson in existing local memory.
No production/governance/new sidecar/global skill/commit/push/external change.


## 2026-09-10 — Terminal-first retention and original Open disposition

Bounded SOURCE-approved first component step, not full Flow/P3 acceptance.
Entry matched all377 Rust hashes at the prior validated RED checkpoint.
Main remained sole overlapping Source Writer; Integrator alone edited the
exact governance inventory. No production caller/whole-root acceptance is
inferred from component activation.

### Actual changes and causal sequence

- Original Flow now owns TerminalSettling with actual TerminalGrace/reason,
  retained feedback and actual source. Bound Target join clears Target-owned
  presence, not the Flow/source record. The original late DATA is validated
  against full LocalFlow/Owner direction, paired physical state and the original
  sealed factory/permit origin, then disposed without re-entering the retired
  reducer or producing a second protocol output. This is only terminal proof
  checking in driver_receive; producer admission revocation is not implemented.
- Original RESET-first test changed from Flow-absent RED to GREEN. Its extension
  exposed an exact TerminalGraceExpired/PendingTargetCompletion RED; only the
  matching retained terminal now passes the phase-aware event gate, with
  global effect/output/attach exclusions preserved. Z expiry does not delete F.
- First SOURCE review0/2/0 also found legal NoSource Open regression. Actual
  MemoryTarget maxflows1, real occupied Z1 and failed Open3 produced exact
  MissingTargetReadSource on original completion retry. Original Open storage
  now retains full LocalFlow/request/result/actual Target disposition through
  Prepared/Reducing/Rejected/FinishPending. Rejection returns to this original
  proof; it cannot recreate LocalFlow from None or repeat Target.open.
- cold_source_less_open_keeps_its_disposition_across_real_z_retry then GREEN.
  cold_pending_finish_effect_consumes_its_original_resume_after_binding was
  directly GREEN characterization: real max_effect_steps1 leaves an actual
  FlowFinished in the original bounded queue; actual cold binding imports that
  phase and consumes only original NeedsResume tokens afterward. The immutable
  NoSource association remains unchanged; original OpenResult/terminal occur
  once, no Target reopen or fabricated Driver transit.
- Second SOURCE review0/1/0 closed those two findings but found Unbound
  AlreadyTerminal/join-pressure exclusion. Same-file fixture preparation was
  extracted and characterized7/7 before adding its test. Actual Target
  tombstone1, terminal Target2, model Open2, real capacity release and the
  original retry produced PendingTargetCompletion(flow2) RED. Matched actual
  Unbound FlowFinished now consumes completed Open proof while preserving
  Target/join custody. No general completion gate was relaxed. Exact GREEN.
- Final same-Heisenberg bounded SOURCE review0/0/0 Approve closes that new
  regression and preserves the earlier two closures. Reviewer ran no tests;
  Main/Integrator executed the gates below. Review approval does not cover
  unimplemented full-phase/physical obligations.

### Executed final gates and structural scope

All test commands used cargo test --offline --locked --all-features, --lib
for module/exact tests or --test for integration gates, with --nocapture and
--test-threads=1. There were no zero-test acceptance runs.

| Gate | Final result |
|---|---|
| owner_target::commands::tests::bound::flow_lifecycle:: | 8/8; ten serial repetitions each8/8 |
| owned_upstream::owner_target:: | 126/126 |
| owner::server_root::tests:: | 24/24 |
| owner::continuation::tests:: | 16/16 |
| bound_active_owner_source_reaches_real_output_once | 1/1 |
| knife16_owned_leg_provenance | 162/162 |
| repository_test_module_provenance | 17/17 |
| cargo check --offline --locked --lib; same plus --all-features | both PASS |
| cargo fmt --all -- --check; git diff --check | PASS |
| standalone rustc --edition=2024 --test -D warnings governance | compile PASS,7/7 |
| Cargo all-features code_growth_governance | 7/7 |
| all73 untracked files trailing-whitespace scan | PASS |

Target Adapter10/10 and Supervisor48/48 also passed after the first Open repair;
they are supplementary pre-final-Unbound-delta evidence, not mislabeled final
executions. Final library/governance warnings retain smoltcp22 plus existing
CurrentOnly private_interfaces1. No zero-warning build is claimed.

Final exact source sizes: owner_target.rs9581; completion.rs899; service.rs671;
binding.rs229; driver_receive.rs216; flow_lifecycle.rs787; governance.rs752.
Main touched only those six scoped Rust files, Integrator only governance.
The parent ratchet progressed9590->9588->9581; final known rustfmt enum
compression accounts for four lines, not a scope/Interface change.
Fresh377-path hash comparison isolates those seven changes; tcp.rs and every
other baseline source are unchanged. No new Rust path was introduced here.
Integrator final inverse-hash check proves only9_588->9_581 changed since his
prior ratchet. Final governance SHA:
a4a113e2703dea0863c9deed81665183ce66b5cf4d79bf386903c7634edeb8ed.
Logs: /tmp/mini-vpn-p3-terminal-open-ratchet.qvUNqA/
(compile.log, standalone.log, cargo-all-features.log).

### Remaining executable work, not waived acceptance

Continue the same joint51 checklist: independent CLOSE falsifier; exact Failed
work/cause plus independent sink custody; actual Driver-origin Z rejection
charge1->1->0; Owner-only admission revocation and both selected closed-lane
drains; selected completion/join/retry; true F/source/publisher retirement.
Cold/local completion has no Driver charge. The current full-map retry used
to reproduce cold behavior is not accepted as a bounded selected service unit.
The787-line leaf needs bounded preparatory extraction or explicitly granted
new test locality before further coverage exceeds800; do not compress ownership
into shallow interfaces to fit the limit. Fixture structure self-assessment8/10:
duplicate setup removed with before/after7/7, but remaining lifecycle coverage
and bounded future locality are unresolved; no whole-architecture10/10 claim.

Full C/root terminal acceptance additionally requires selection/authorization
of the real lifecycle action installer and original clock/deadline/due consumer,
or an explicit reviewed settlement-only admission amendment. Main recommends
the real mandatory P3 consumer. This is not granted by the Driver extension,
not a later-stage deferral, and not a reason to stop authorized component work.
P3/Case C/R7 remain incomplete; active goal remains open.
Checkpoint: /tmp/mini-vpn-p3-terminal-first-green.d5PPt1/entry.json.

diagnose/tdd separated each actual RED from direct characterization;
refactoring-patterns kept fixture extraction independently green;
code-review found and verified cold/Unbound regressions; self-improving-agent
recorded completed-proof versus feedback-ownership lessons in existing memory.
No new sidecar, global skill change, commit/push or external/TUN/VPS/WAN/M2 run.

## 2026-09-10 — Driver-origin rejection retains its transit charge

Bounded component SOURCE accepted, not full phase/selected/C-root acceptance.
Prior checkpoint: /tmp/mini-vpn-p3-terminal-first-green.d5PPt1/entry.json.
All377 prior Rust fingerprints matched before this four-file change.

The existing real RESET/Z-pressure test exposed transit0 versus expected1
after actual carrying rejection. Driver receipt charge had been decremented
unconditionally while its command remained retained. Minimum GREEN moves
ordinary decrement to observed success; actual returned Box and origin now
remain in the original Flow's DriverReturned disposition. Retry validates full
LocalFlow/source, original fixed position/association/generation and charge,
then publishes Reducing before command consumption. Rejection returns the
actual carrying Box; unknown leaves nonzero Reducing. Successful reduction
publishes Reduced and settles charge before effects/Target callbacks. No
second counter, charge mint, copied command, new allocation or event-syntax
inference distinguishes cold/local work. A subsequent reducer rejection may
return a different Box address; the invariant is actual ownership continuity.

submit/retry share the one original-Flow retry transaction. Existing global
selection remains reachable and is not accepted as selected work. A real
terminal retains Driver disposition; its selected terminal-disposal path is
still owed. Independent unexpected rejection returns its owning error and
does not overwrite another pending RESET.

At focused GREEN, a separately characterized same-file Extract Method replaced
three identical Bound/Live record lookups: lifecycle8/8 before and after,
original assertions unchanged. No new child/visibility or production getter.
Then actual pre-reduction1 and post-success0 assertions extended the existing
post-rejection1/one-RESET evidence. Exact1/1 and ten serial1/1 repeats pass.
No repeated-rejection or cold-otherDATA test was silently claimed by repeats.

Final commands use cargo test --offline --locked --all-features, --lib or
--test, with --nocapture --test-threads=1. Actual final evidence:

- lifecycle8/8; OwnerTarget126/126; Root24/24; continuation16/16;
  bound_active_owner_source_reaches_real_output_once1/1; late-wake isolation1/1.
- Knife provenance162/162; repository provenance17/17.
- Default and all-feature cargo check --offline --locked --lib PASS;
  cargo fmt --all -- --check and git diff --check PASS.
- Fresh strict standalone governance compile/run7/7; Cargo governance7/7.
- All73 untracked paths pass whitespace/final-newline check.
- Existing smoltcp22 plus CurrentOnly private_interfaces1 warnings remain.

Same Heisenberg production SOURCE0/0/0 Approve covered parent9581,
completion1097 and service675, not the older completion899 approval. Final
test-source797 separately closed0/0/0. Reviewer ran no Cargo. No currently
reachable silent no-op was found in finish_driver_reduction: None is local;
Driver preflight and the intervening Supervisor reduction exclusively borrow
the same Executor and cannot change its Flow/backing. Future callers must
preserve that closed precondition. Review did not provide a numeric score or
1097-version hash; neither is retroactively attributed to the Reviewer.
Main's limited structural assessment8/10: custody stays local, but full phase/
selected settlement and adversarial coverage remain before broader closure.

Exact lines: owner_target9581 net-zero; completion1097; service675; test797;
governance752 unchanged. Only those four Rust files differ among377 baseline
hashes; tcp.rs, Driver child, binding and inventory are unchanged. No ratchet
edit is required. Checkpoint contains Main's exact final377 hashes:
/tmp/mini-vpn-p3-close-charge.e9HHkL/entry.json.

Remaining executable checklist stays in joint51: repeated rejection/cold-other
charge, independent CLOSE, exact Failed work/cause/sink, Owner-only revocation/
both selected drains, selected completion/terminal disposition/join/retry and
true F/publisher retirement. Test leaf797 needs bounded preparation; proposed
private flow_lifecycle/cold_open.rs Move is not granted or performed. Full
C/root lifecycle installer/clock/deadline/due ownership still needs explicit
model/scope choice and remains mandatory P3. No complete A/B row is checked.
P3/Case C/R7 and the active goal remain open. No public/TUN/P4-P7 change,
new sidecar/global skill, commit/push or external execution.

diagnose/tdd isolated the actual counter RED; refactoring-patterns kept test
lookup preparation green-to-green; clean-architecture/system-design/ddia-systems
kept origin and settlement at the original Flow, not a second ledger;
code-review checked production and test deltas; self-improving-agent records
the bounded charge/custody lesson without modifying global skills.

## 2026-09-10 — Repeated rejection and cold-Open charge isolation

Test-only supplement to the preceding production checkpoint. All377 source
hashes matched /tmp/mini-vpn-p3-close-charge.e9HHkL/entry.json at entry.
No production behavior, original Interface, governance or new Rust path changed.
The proposed private cold_open test Move remains unanswered and unperformed.

Five duplicate selected-source transactions were extracted within the same
test leaf: the caller still owns the actual lane/claim/publication interval;
the helper performs the same two visits and SourcePulled/MailboxOwned assertions,
returns the actual Completed, and never claims/finishes/polls the inbox.
Lifecycle8/8 before and after;797->769 lines. A later cold-test local borrow
merely names its original full-source assertion; exact1/1 passed separately.

The existing actual RESET/Z test now makes three successive original retries
before releasing Z. Each retains actual DriverReturned command/full LocalFlow,
transit1 and no effects; real expiry then yields transit0 and exactly one RESET.
Direct GREEN1/1, not a new production RED/GREEN. Reviewer confirmed the only
eligible pending is actual Returned and each call reaches the original reducer
and capacity rejection; empty effects alone would not prove that reachability.
This remains the existing global selector, not selected-service acceptance.

The cold NoSource Open test now finishes its genuine Control visit and takes
the next Ordered claim. Actual MemoryTarget feed/read produces4-byte DATA,
Driver/cells transfer it into the paired mailbox, and the named inbox keeps it.
Original cold Open retry/expiry completes with the same single OpenResult and
terminal, no Target reopen, full source identity preserved. Before and after:
live transit1/cold transit0, original DATA4bytes/1segment. Direct GREEN1/1.
This intentionally retained nonzero custody proves isolation, not clean Drop
or whole-root zero, nor local/Unbound Failure alongside-charge coverage.

Final actual gates, all offline/locked/all-feature, serial test runners:
both exacts1/1; lifecycle8/8 with ten serial8/8 repeats; OwnerTarget126/126;
Knife162/162; repository17/17; standalone and Cargo governance7/7;
fmt/diff PASS; all73 untracked paths whitespace/final-newline PASS.
Standalone reused the strict binary built from the unchanged governance source.
Earlier Root24/C16/actual Bound1/library checks are prior production evidence;
they were not rerun or relabeled here. No new warning claim is made.

Same Heisenberg's782-line helper/repeated-rejection review and798-line cold
extension review both return P0/P1/P2 0/0/0 Approve, bounded test-source only.
The final combined retrieval explicitly confirmed both original reviews;
Reviewer ran no Cargo. Final798-line leaf is the sole change among377 baseline
Rust hashes; parent9581/completion1097/service675/tcp.rs/governance752 unchanged.
Main rates test locality8/10: duplicate transactions removed, but broader
failure/retirement coverage needs authorized deeper locality, not helper sprawl.
Checkpoint: /tmp/mini-vpn-p3-charge-coverage.wvu2SA/entry.json.

Next gate needs the private cold_open Move grant before substantial additional
tests in the798-line leaf. Independent CLOSE/Failed/sink/local-origin coverage,
Owner revoke/both drains, selected settlement/join/retry and actual F/publisher
retirement remain required. The real C/root lifecycle installer/clock/deadline/
due model/scope is independently ungranted and mandatory P3. No full A/B row
is checked; P3/Case C/R7 and the active goal remain open. No new sidecar,
global skill, external run, commit/push or public/TUN/P4-P7 expansion.

diagnose/tdd kept real assertions separate from new repair claims;
refactoring-patterns preserved the source/send transaction before test changes;
code-review audited actual retry reachability and retained DATA ownership;
self-improving-agent updates the existing scoped charge lesson.

## 2026-09-10 — Private cold Open Move and independent CLOSE

User authorized the proposed private test child and continued execution. All377
Rust fingerprints matched the preceding charge-coverage checkpoint at entry.
No production code, Interface, governance inventory or architecture changed.

First, pure Move only: parent flow_lifecycle.rs798->569; new private default-path
flow_lifecycle/cold_open.rs238. Original three cold Open tests and their sole
consume_original_resumes helper moved together, unchanged. The230-line block
SHA-256 is6fc7ff6c728b798163debe20cae24cc24eefcc3613d8169b9d0afb67b8606020;
parent inverse is exact. Child uses ancestor-private helpers without visibility
expansion. Five tests remain parent; three FQ names gain ::cold_open::.
Pre-write governance7/7, original lifecycle8/8 before and after, fmt/diff,
Knife162/repository17 and standalone/Cargo governance7/7 all passed.
Same Heisenberg submission01a08af0-3cb3-7ef1-87c3-bbe50c05a48b:0/0/0 Approve,
scoped structure9/10. No behavior was written before that Move review closed.

Then one separate test, in_flight_close_settles_only_itself_and_preserves_the_staged_reset:
actual MemoryTarget finish_read/EOF commits to the original ordered source;
Driver/cells transfer the actual CLOSE to the paired mailbox. Receipt is held
before failure staging, with full LocalFlow and transit1. Observed CLOSE
reduction emits exactly one CLOSE/offset0, reduces its own transit to0, and
preserves the same StagedReset. Real other-Flow Z expiry cannot manufacture a
retry from that unacquired RESET. EOF count1, byte/segment0 and empty Ordered
inbox are not a claim that source/RESET/F/Root custody is zero.

Initial focused compilation reported E0599 for nonexistent ByteOffset::ZERO;
no runtime executed. Scope-local feedback repair used existing ByteOffset::new(0)
with the identical assertion. Exact then directly GREEN1/1; production was
already correct for this branch, so this is characterization, not repair RED/GREEN.
Rustfmt changes remain inside the new test. Removing its77 lines exactly restores
the reviewed569-line Move parent. Final parent646, child238, both below800.

Fresh executed final gates: all-feature/offline/locked exact1/1; lifecycle9/9
plus ten serial9/9 repeats; all OwnerTarget127/127; Knife162/162;
repository17/17; newly compiled rustc -D warnings governance7/7 and Cargo7/7;
global fmt and tracked diff PASS; all74 untracked whitespace/final-newline PASS.
Earlier Root24/C16/actual Bound1/library checks were not rerun. Warning output
retains existing smoltcp22 and library-test private-interface1; Knife adds its
known unused/dead-code warnings. No suppressions or warning fixes were added.

Same Heisenberg submission01a08af3-b076-7ae3-b625-3615ef0e45d4 approves only this
new test-source0/0/0, scoped quality9/10. Reviewer ran no Cargo; Main owns the
executed evidence. Reviewer independently confirms actual source/receipt route,
original claims, charge1->0, exact retained RESET and source-nonzero boundary.

Checkpoint /tmp/mini-vpn-p3-cold-move.zRhPmx/entry.json contains378 current Rust
hashes and the prior reference. Only the test parent and new child differ from
the377-source baseline; production parent9581/completion1097/service675/tcp.rs/
governance752 are unchanged. No ratchet needed. Current status/navigation and
joint51 are faithfully advanced; prior result remains historical evidence.

Next authorized component work is exact Failed work/cause and independent sink,
including remaining local/Unbound Failure charge isolation, then exclusive
Owner revoke/both selected drains, selected completion/disposition/join/retry,
and actual F/publisher retirement. The real C/root lifecycle installer and
clock/deadline/due model still require scope choice; mandatory P3, not deferred.
No full joint51 A1-A4/B, P3, Case C or R7 is accepted. No new architecture sidecar,
global skill, commit/push, public Client/TUN, P4-P7 or external run.

refactoring-patterns separated unchanged movement from test growth; tdd/diagnose
kept the compile-feedback repair distinct from runtime behavior; code-review
checked both coherent boundaries; self-improving-agent records the reusable
test-locality and receipt-disposition lesson in existing project memory.

## 2026-09-10 — Owner revocation tracer: real terminal RED and scope stop

This is an unaccepted working RED, not a promoted implementation slice.
Prior cold Open Move/CLOSE acceptance and evidence remain unchanged.
Checkpoint: /tmp/mini-vpn-p3-owner-revoke.GOpDVc/entry.json.

The new real lifecycle test queues Target DATA and the sole staged RESET,
releases another actual Z, then sends peer RESET through Executor/Supervisor.
Its first runtime RED was PendingTargetCompletion at the real intake, not
the intended post-terminal admission assertion. Heisenberg diagnosed0/2/0:
per-Flow self-exclusion and a separate generic PeerReset effect that releases
pending_write before FlowFinished. General pending-completion bypass is unsafe.

The narrow completion.rs candidate admits only Bound StagedReset, original
source correlation, no pending_write/terminal/join. Global effect/attach guards
remain, and the original Supervisor still validates leg/generation/actual Z.
Initial E0026 used a nonexistent PeerFrame.record field; the causal repair
uses existing frame.record(). It does not alter the event or reducer.
Exact execution then reaches actual terminal and retains4bytes/1segment,
but fails on new source reservation. Formatting-only rerun reaches the same
RED at flow_lifecycle.rs378:0/1,1732 filtered. The later RESET/DATA/drain
assertions have not executed, so no disposition or revoke claim follows.

Heisenberg submission01a08b11-b975-7642-811d-d50035c42604 requires new explicit
scope: tcp.rs only one accurate Owner outstanding completion/action error
carrying two actual counts, no storage/getter/re-export/legacy change. Existing
FlowPortError cannot express that reason. Do not use an unrelated error,
inverse dependency or nested-result workaround. The parent remains2828 and
byte-frozen; any future grant must preserve its exact shrink-only limit.

Close-under-lock additionally requires original non-escaping try-only Owner
producers and no async send/closed observer or escaped reservation. General
Tokio close can synchronously wake; counters alone are not exclusivity proof.
Retain actual terminal/work/source before revoke, reject before mutation,
never dequeue, and unlock before potentially unwinding factory notification.
No revoke implementation was written while this scope decision was pending.

Failed work/cause testing has an independent scope prerequisite: MemoryTarget
has no Failed directive. Existing private FailOnceTarget is migration-only
under joint51. Reviewer permits requesting one private
owner_target/tests/r7_nested_abort/bound_failure.rs plus its private parent
route, keeping original injector behavior and visibility unchanged. The
async request has no user answer yet; it is not a write grant.

Only completion1097->1123 and flow_lifecycle646->769 changed against the378
Rust checkpoint. Removing the new123-line test restores prior646 byte exactly;
all376 other hashes, including tcp.rs/governance752, remain unchanged.
A fresh strict standalone governance compile/run7/7 completed before the
production policy write. Its post-format run also passed7/7; global fmt,
tracked diff and scoped untracked whitespace/final-newline checks passed.
An earlier baseline run overlapped initial test writing; it is not claimed
as a completed pre-write gate. No broad runtime/provenance/Cargo-governance/
library closure or repeated GREEN was run. Diagnostics include smoltcp22 and
the unchanged physical_session_root private_interfaces warning, not repaired.

Next: obtain the exact grants, finish the original owning revocation/feedback
transaction and challenge rejection/pressure/cancellation/notification;
then selected drains, exact Failed/sink and F/publisher retirement. The full
C/root lifecycle installer/original clock/deadline/due model remains a separate
mandatory scope decision. P3/Case C/R7 remain open. No new sidecar, governance
edit, commit/push, global skill edit or external run.

## 2026-09-10 — Owner revocation ordinary path GREEN; poison review P1 stop

User granted the preceding two exact scope requests. joint51 now records only
the counted completion/action error in tcp.rs, net-zero/shrinking, and private
r7_nested_abort/bound_failure test route with unchanged FailOnceTarget. The
error grant was used; the failure child/route was not written. This source is
unaccepted. Prior cold Open Move/CLOSE acceptance is unchanged.
Checkpoint: /tmp/mini-vpn-p3-owner-revoke-review.MxwvdE/entry.json.

All378 working-RED fingerprints matched before writing. Fresh strict rustc
governance7/7 completed first. The exact test again reached the actual runtime
RED at new source reservation0/1. Three-file minimum GREEN then added an Owner
operation over the original Driver/Flow/factory/terminal: full LocalFlow,
direction, shared-state/progress/origin checks; same-lock admission/reservation/
completion/action refusal; close without dequeue; unlock before notification.
completion.rs stores the actual TerminalSettling before this operation. The
new FlowPortError carries both actual outstanding counts, not an unrelated
queue error. tcp.rs remains2828 by removing two blank separators; legacy
implementations are untouched. One malformed apply_patch was rejected atomically
before any mutation; correcting patch syntax did not run or repair a test.

Exact runtime1/1 now includes retained DATA4bytes/1segment, refused new source,
actual Control RESET and Ordered DATA disposition through original receipts,
transit1->0, and physical lane Ready(None). It does not prove selected drain
ledger or F/publisher retirement. Test leaf remains769, unchanged this turn.
Exact three-file rustfmt yields tcp2828/driver_receive270/completion1130.

Executed gates, all offline/locked/all-features and serial where Cargo tests:
exact1/1; flow lifecycle10/10; OwnerTarget128/128; TCP68/68; owner server Root24/24;
continuation16/16; Knife provenance162/162; repository provenance17/17;
strict standalone governance7/7; Cargo governance7/7; global fmt and tracked
diff PASS. Cargo governance was already in flight when review P1 arrived and
completed; no subsequent gate started. Actual Bound physical test, cargo check,
ten-repeat set, poison/refusal negatives and Failed/sink tests were not run.
Warnings retain smoltcp22 and unchanged lib-test private_interfaces1; the Knife
library build also has its existing143 dormant warnings. No suppressions/fixes.

Same Heisenberg submission01a08e1d-c09d-7421-aa4f-8852ba01f0ab: P0/P1/P2 0/1/0,
Request Changes, scoped Module8/Clean8/System7/DDIA7. Reviewer ran no Cargo.
P1: driver_receive.rs43 calls admission_guard(), whose tcp.rs implementation
recovers PoisonError::into_inner. Thus a poisoned lock with zero counters can
still close/retire and report success. The new operation must check lock()
directly and refuse poison accurately before mutation/close/notify, with no
guard recovery. Old helper/legacy remain frozen. A counted {0,0} ownership
error cannot stand for poison. No poison repair has been written.

Reviewer confirms the terminal is retained before revoke, identity/ownership
checks precede mutation, there is no dequeue/TUN action, and notification is
unlocked. Close-under-lock relies only on the closed try-only Owner producer
route with no sender waiter/closed observer/escaped producer, not mere counters.
The narrow StagedReset event policy retains global and real Supervisor guards.
These source checks do not close the new negative/cancellation obligations.

Scope assessment01a08e23-dc20-7d32-92e0-6c2fc85934d2 supports requesting, not
executing: accurate tcp.rs Owner admission-poison error <=2828; private route
only in tcp/tests/mod.rs <=2975; private tcp/tests/owner_terminal.rs <=800 for
real poison and reservation/completion/action/identity negative tests. Construct
a real pristine Supervisor-bound factory before opening the flow; owner_fixture
is already non-pristine and isolated_pair lacks the production origin. Inject
poison by holding the actual mutex guard through caught panic, not an extractor
callback that runs outside the lock. Assert poison/custody persist and no
revoke/close/notify; never clear poison or fabricate an origin. No new getter,
export, production visibility or use of unrelated bound_failure scope.

Final378-hash audit has exactly the three reported production changes versus
the preceding RED, all375 others unchanged, including governance752. No ratchet
edit. Current result/status/handoff/TODO and concise project learning record
this P1 stop, not approval. diagnose/tdd isolated the actual RED/GREEN; same
code-review prevented poison recovery from being promoted as exact refusal;
self-improving-agent records that reusable limitation without global edits.

Next: obtain this minimal error/test-route extension, close P1 with real
negative evidence and same Reviewer, then selected drains/F retirement and
exact Failed/sink. Full C/root lifecycle installer/original clock/deadline/due
remains a separate mandatory P3 scope decision. No P3/Case C/R7 acceptance,
public cutover/P4-P7, new architecture sidecar, commit/push or external run.

## 2026-09-10 — Owner admission poison refusal and exact negative component closure

User explicitly authorized the previously requested poison error and TCP private
test route. No remaining C/root scope was opened. All378 preceding Rust hashes
matched at entry; fresh rustc -D warnings governance7/7 completed before Rust.
joint51 records the exact error-only/net-zero and private route exceptions.

First test used actual Supervisor factory mint before real Open/resolution,
queued DATA4bytes/1segment, actual terminal, and panic while holding the actual
PortState mutex. Identity/origin/zero-count assertions and poison=true passed;
the runtime RED was revoke returning Ok(()), exact0/1, not compile/fixture error.
Minimum GREEN replaces only this operation's admission_guard with checked
admission_gate.lock().map_err to new OwnerAdmissionPoisoned. No guard recovery,
mutation, close or notification on poison. Old legacy helper is untouched.
Same exact1/1 then passes with precise error, same custody and repeated refusal.

Five further tests were added/run individually, each directly GREEN without
additional production changes: actual source reservation retained then returned;
actual completion permit1/action0; real HalfClose action/completion1/1 then
original finish publishes completion while the action guard stays held0/1;
foreign terminal and same-LocalFlow wrong pair/factory; independent healthy
Owner proceeds while the poisoned Owner remains refused. The HalfClose test
is explicitly TCP defensive preflight, not a Bound TUN producer/call-path claim.
No fabricated origin, manual count store, poison clear or production API added.

After exact four-file rustfmt: tcp.rs2828, driver_receive.rs273,
tcp/tests/mod.rs2975, new private owner_terminal.rs341. Existing error insertion
uses two blank separators; index route uses one. All three old-file inverse
SHA-256 checks match the preceding baseline exactly. Other375 Rust hashes,
including completion.rs1130, original lifecycle769 and governance752 unchanged.
New checkpoint /tmp/mini-vpn-p3-poison.WZy55K/entry.json contains379 current hashes.

Final actual gates: all-feature/offline/locked six-test Module6/6; ten serial
repeats each6/6 (60 passes); TCP74/74; OwnerTarget128/128 including lifecycle10;
server Root24/24; continuation16/16; exact real bound_active_owner_source_reaches_real_output_once1/1;
Knife162/162; repository17/17; strict standalone/Cargo governance7/7 each;
global fmt, tracked diff, all75 untracked rs/md whitespace/final-newline and
cargo check --offline --locked --all-features --lib green. Repository provenance
and library check briefly overlapped read-only; Cargo governance ran afterward.
Warnings remain smoltcp22/private_interfaces1; Knife library dormant143. No
warning suppressions or unrelated repairs. No governance inventory change.

Same Heisenberg submission01a08e4a-650a-7d82-b923-237fe88e0c9a:0/0/0 Approve,
SOURCE component only; original poison P1 closed. Reviewer independently checked
actual poison refusal, true resource counts and identity, isolation and exact
inverses; ran no Cargo. Main owns the final runtime evidence above and supplied
it after review. Scoped Module9/Clean9/System8/DDIA8; further sealed/retired
priority characterization and progress-wake unwind would strengthen this seam.
The latter, selected drains/F retirement and whole-root lifecycle remain owed;
approval does not establish full P3/C/root, Case C or R7 acceptance.

Next authorized execution is the private bound_failure leaf/route using
unchanged FailOnceTarget to expose exact retained Failed work/cause and sink.
No new sidecar/Interface, synthetic success, global skill, commit/push, public
cutover/P4-P7 or external run. diagnose/tdd isolated the genuine runtime fault;
clean-architecture/system-design/ddia kept refusal local to its actual resource;
code-review closed the poison P1; self-improving-agent updates the existing
project lesson about legacy recovery and exact refusal, without global edits.

## 2026-09-10 — Bound Failed work/cause retention and terminal-order component closure

Continued the authorized original-Flow component under joint51 and the active
P3 self-repair goal. Entry379 Rust hashes matched the poison checkpoint;
fresh strict standalone governance7/7 passed before writes. The private
r7_nested_abort/bound_failure route uses unchanged FailOnceTarget, actual
capacity plan, original Supervisor factory, consuming Bound installation,
paired inbox and caller-held ReadyIndex. No new production visibility or test
destination. The first actual write-failure fixture ran1/1 before the exact
new work/cause test compiled RED solely for missing Failed (E0599).

Minimum GREEN adds Failed(work,cause,feedback) at the original completion
position, not a second ledger or the independent sink. Write, half-close, read
and readiness failure sites preserve the cause; read failure is stored before
reservation Drop can publish progress. Prepared/StagingUnknown/InTransit and
actual Driver Returned/Reducing/Reduced preserve work without reproducing RESET.
Carrying Z pressure changes feedback only. Reduction settles its one original
charge but retains work until actual FlowFinished; terminal feedback carries
the same non-recursive owning payload. Legacy StagedReset/local Failure remain
separate origins. MemoryTarget has no ReadyOutcome::Failed producer; that
call-site migration is source-reviewed, not a fabricated runtime tracer.

Nine focused tests were added/run with real resources: initial fixture; exact
Offer/segments/ConnectionLost; one-step RESET reduction before FlowFinished;
real Z4 pressure and genuine other grace release; read failure plus independent
WouldBlock sink and earlier peer terminal/late RESET; actual read-reservation
progress wake unwind; RESET-staging wake unwind; HalfClose capability and
ResourceExhausted; ambiguous once-only source RESET admission returning exact
TerminalLaneFull with work retained once, not duplicated into aborted work.
Read/RESET wake tests do not establish the separate terminal-revoke wake.
Global grace retry is component evidence, not selected retry acceptance.

Two fixture feedback-loop corrections preserved production semantics: a second
Control visit cannot blindly select the next round-robin claim, so paired
source/send operations use the same real publication interval; received CLOSE
already emits one nonfinal ACK before Target half-close, so the test asserts
that exact existing ACK rather than falsely requiring no output. No scheduler
gate or protocol change was made for either test correction.

First same-Heisenberg SOURCE review01a08e6f-3f38-7c92-b352-2a4e5093b124 found
0/1/0: newly admitted Failed+independent Write could run PeerReset and clear
sink before the following FlowFinished. The original128-step test hid this.
Changed that same test to a real constructor one-step budget. Exact runtime
RED at the original sink expect proved premature release before any repair.
Minimum GREEN only defers Bound+Failed PeerReset's Target/reset and release;
the real FlowFinished stays in the same bounded owning effect queue, and the
consumed step is still counted. Existing finish_target_flow first stores actual
TerminalSettling, then revokes, invokes Target and releases sink. Legacy and
StagedReset unchanged. Same exact1/1 now proves original Offer/backing pointer,
Target-present/terminal-None/Failed before real resume, then one terminal and
late RESET zero facts/transit1->0. No admission re-ban or synthetic terminal.

Same Reviewer re-review01a08e78-47b7-76a0-9b87-2526b2c2e393 returned0/0/0,
Approve for this minimal SOURCE repair, explicitly closing the sole P1.
He verified380 hashes, exact two-file repair and original test inverse; no
Cargo run by Reviewer. Prior findings checked the rest of this custody slice
without further issues. This does not approve full phase, C/root or P3.

Final Main gates, all offline/locked/all-features and serial:9/9 plus ten
repeats9/9 (90 passes), OwnerTarget137/137, TCP74/74, server Root24/24,
continuation16/16, exact real bound_active_owner_source_reaches_real_output_once1/1,
Knife162/162, repository17/17, freshly compiled rustc -D warnings governance7/7,
Cargo governance7/7, global fmt, tracked diff, all76 untracked rs/md whitespace/
final-newline, and library check green. Existing smoltcp22/private_interfaces1
and Knife dormant143 warnings remain. No warning/argument allowance added.

Final lines: owner_target.rs9581; completion1306; service734; read_source285;
r7_nested_abort746; private bound_failure722; governance752 unchanged. Parent
adds only the Failed anchor/cause call sites and12-line terminal guard, offset
by27 existing top-level doc separators. Original FailOnceTarget and old tests
have exact route-removal inverse. Against poison379, five old Rust paths and
one new leaf changed; other374 source hashes unchanged. No ratchet edit.
The rejected candidate entry and repair evidence stay in
/tmp/mini-vpn-p3-bound-failure.9DmOre/; accepted.json binds final380 hashes.

Remaining executable work: actual cold/local Failure charge isolation in the
authorized leaf if it fits; incoming independent Write|HalfClose and proper
DATA/CLOSE admission; terminal-revoke wake; both selected drains/completion/
join/retry and F/publisher retirement. New test destinations are not implied.
The separate real C/root lifecycle installer/original clock/deadline/due
decision remains ungranted and mandatory P3, alongside joint51 physical/capacity
obligations. No full phase/P3/Case C/R7 acceptance, public cutover/P4-P7,
commit/push or external run. diagnose/tdd isolated the real one-step fault;
code-review caught the gap hidden by aggregate GREEN; project-local
self-improving-agent records this boundary test lesson without global edits.

## Actual cold/local Failure Driver-charge isolation

2026-09-10. Test-source component closure under the continuing P3 goal.
Prior380 Rust hashes matched the accepted custody checkpoint. Fresh strict
standalone governance7/7 and unchanged bound_failure9/9 preceded writes.
Only owner_target/tests/r7_nested_abort/bound_failure.rs changed. Three repeated
Control source-pull/transfer/paired-receive transactions became one private
fixed-lane helper; caller claim/publication/assertions/finish remain. Separate
green-to-green boundary:9/9 before/after, fmt and governance7/7,722->683 lines.

The added tracer uses unchanged FailOnceTarget: actual Unbound write failure
under four real Z occupants retains exact Failure(LocalReset(original flow)).
A second Flow is actually opened before the same Executor/factory/plan binds.
Cold import probes the empty original source first (Waiting), then the other
Flow's real DATA (SourcePulled->MailboxOwned). Local transit0/othertransit1 and
bytes4 are observed. Actual predecessor grace expiry must produce exactly one
original-flow terminal while leaving that independent DATA charge/backing.
The original paired inbox then delivers DATA, settles only its own charge1->0
and emits exactly the original flow/payload. No count setters, raw producer,
fabricated completion, Target/Driver replacement or production edit.

Feedback corrections were test-only, not production RED/GREEN: initial compile
E0599/E0614 incorrectly called record() on transmitted()'s already-&Record;
then runtime Waiting vs SourcePulled revealed the actual cold source probe.
The source import requests both Driver lanes for every actual source and
advances the cursor before polling, so the test now requires the exact two
observations, not a permissive retry loop. Formatting initially yielded809
lines; named observations and an exact full transmitted-vector check retained
all assertions at800 before further testing. Do not plan with unformatted
line estimates. A mistaken Root filter ran0 tests; that is not acceptance and
was corrected to the actual owner::server_root path,24/24.

Same Heisenberg SOURCE review01a08e89-a508-74b2-b983-0c5ffcbb8825:
P0/P1/P2=0/0/0, Approve test-source only, bounded structure9/10. Reviewer
independently checked380 hashes, original cold origin, helper ordering,
non-skipped retry proof and exact800; no tests run by Reviewer.

Main final gates: exact1/1, full leaf10/10 and ten serial10/10 repeats (100
passes); OwnerTarget138, TCP74, server Root24, continuation16, actual Bound1;
Knife162, repository17, strict standalone/Cargo governance7 each; global fmt,
tracked diff, all76 untracked rs/md whitespace/newline and offline/locked/
all-features library check green. Existing smoltcp22/private_interfaces1 and
Knife dormant143 warnings remain. Only leaf differs from prior380 hashes;
production/adapter/parent/governance remain unchanged, no ratchet edit.
Checkpoint: /tmp/mini-vpn-p3-cold-failure.lgyx0R/accepted.json.

Next is incoming Write|HalfClose sink/command-aware DATA/CLOSE. Audit granted
destinations before another tracer: current leaf800 cannot grow. Terminal-revoke
wake, both selected drains/completion/join/retry/F/publisher retirement remain.
Global grace retry is not selected-service/whole-cleanup acceptance. Full
C/root lifecycle installer/original clock/deadline/due still needs separate
scope authority and remains mandatory P3; joint51 capacity/physical duties
remain. No full P3/Case C/R7 acceptance, public cutover, external or commit/push.

### Next sink test-path audit — request reviewed, no Rust authorization

2026-09-10. All380 accepted Rust hashes still match. Source audit identifies
the actual next gap: Failed peer DATA/CLOSE remains blocked by the generic
pending-completion gate; merely exempting it would still call Target write
inline. The original independent sink slot cannot hold HalfClose. Existing
joint51 already selects same-Flow Write|HalfClose custody, not a second ledger.
Retry/cold-import consumers must be adapted with the actual slot; no fake
Write wrapper or new public interface is selected. CLOSE's nonfinal ACK stays.

The real unchanged FailOnceTarget is private to r7_nested_abort. Its Bound
fixture leaf is800; other granted test leaves have neither the proper ancestor
nor this failure axis. Minimal proposed scope is private bound_failure/sink.rs
and parent mod, with a separate pure Move before any behavior TDD. Move only
the entire existing sink test489-610 (122 lines), whose SHA-256 is
f3a97c7bfd9b65d9643ec6de008ea4e7b7224fc63db497110d38e9ea96aacd48.
Read-only rustfmt stdout probe: parent679, child128, block unchanged. Existing
fixture/helper/PanicProgressOnce stay parent; use super::* needs no visibility.
Main reran the original exact test offline/locked, default1/1 and all-feature1/1.
No new incoming falsifier has run and no new production GREEN is claimed.

Same Heisenberg request-review01a08e91-993b-7440-8c50-b5d98f8ae638:0/0/0;
locality and executable counterexample confirmed, no extra production path or
external Interface contradiction found. This approves only submitting the
scope request. Await explicit user grant; do not create the child or silently
borrow another leaf. Next after grant: exact Move/inverse/gates/review, then
one real incoming DATA tracer, minimum same-Flow GREEN and separate HalfClose.
Only compact docs/plan/result/learning record this boundary; Rust unchanged,
no governance edit, global skill edit, commit/push or external action. Full
P3 and its separately ungranted C/root lifecycle obligations remain active.

## Independent sink test pure Move

2026-09-11. User explicitly granted the exact private sink.rs destination and
parent route, pure Move first then incoming Write/HalfClose falsifiers. Existing
production scope/invariants, unchanged FailOnceTarget and original test axes
remain. The old test-path blocker is resolved; manual work resumed. Scheduler
tool still reports blocked and provides no resume operation; this is not an
active/completed scheduler claim.

Only bound_failure.rs800->679 and new bound_failure/sink.rs128 changed.
Original122-line test body retains SHA-256
f3a97c7bfd9b65d9643ec6de008ea4e7b7224fc63db497110d38e9ea96aacd48.
Inverse exactly reconstructs the original800-line parent. Private unconditional
default path; no Interface changes, fixture/helper/adapter edits or behavior.
Original10 tests remain parent9+child1; only the moved FQ gains ::sink::.
Other379 source hashes unchanged; final381 match checkpoint:
/tmp/mini-vpn-p3-sink-move.JQ1rp2/accepted.json.

Main prewrite: fresh strict standalone7/7, bound_failure10/10 both modes.
Post: moved exact1/1, bound_failure10/10 and full OwnerTarget138/138 in both
configurations; Knife162, repository17, standalone/Cargo governance7 each;
fmt/diff/changed-file whitespace/newline and all-feature library check green.
A narrower tests:: filter also ran87/87; it is not the138-test full module.
Existing smoltcp22/private_interfaces1/Knife dormant143 warnings remain.

Same Heisenberg SOURCE review01a08efb-14f9-7d53-baa5-ff54e1311569:
P0/P1/P2=0/0/0, Approve pure Move, bounded structure9/10. Independently verified
all381 hashes, exact body/inverse and unchanged other tests. Reviewer ran no
Cargo; Main gates are separate evidence. No further wording-only review.
refactoring-patterns kept movement separate from behavior; code-review checked
actual source. Project-local self-improving-agent records the bounded test
partition lesson. No governance edits, global skill edits, external or commit.

Next authorized: ONE actual read-failure-before-peer-DATA tracer, minimal
original-Flow sink custody/no Target write/no premature DATA ACK, then separate
HalfClose with original nonfinal ACK. No incoming behavior is accepted yet.
Full P3 selected lifecycle, retirement and separately ungranted C/root duties
remain mandatory; no P3/Case C/R7 completion.

## Incoming DATA after actual read failure

2026-09-11 component closure, checkpoint:
/tmp/mini-vpn-p3-sink-data.evCOv8/accepted.json (381 hashes).
One new real read-first DATA tracer initially failed exactly with RejectedEvent
PendingTargetCompletion. Bound known-Failed/source-correlated incoming DATA is
now admitted through the unchanged Supervisor/model, while StagedReset stays
RESET-only and unknown/global blockers remain. Original empty Flow sink slot
owns the actual Offer/segments; Target.write and DATA ACK are not produced.
One-step peer RESET retains original Offer/backing until real FlowFinished;
late failure RESET gives0 facts and only own transit1->0. No second ledger,
new allocation, fixture/Adapter edit or external Interface.

Relative to the Move, only parent9581/completion1314/sink248 differ. Parent's
10 added formatted branch lines are offset only by10 separators between its
existing effect/completion helpers; no function or guard removed. Main:
exact1/1 plus10 serial repeats; OwnerTarget139 both modes, TCP74/Root24,
Knife162/repository17, standalone/Cargo governance7, fmt/diff/whitespace/check
green. Existing warnings remain. Same Heisenberg SOURCE0/0/0, Approve bounded
DATA increment, structure9/10; independent381 hashes match. No HalfClose or
whole-sink acceptance. Next authorized is real CLOSE and same-slot HalfClose;
write-only counts/readiness and retry error priority must remain accurate.

## Incoming independent Write/HalfClose sink custody

2026-09-11. Separate incoming CLOSE tracer compiled and produced exact runtime
RED: actual read Failure first, then legitimate peer CLOSE rejected by
PendingTargetCompletion. Minimal GREEN extends only the known Bound Failed
incoming command exception and original Flow sink representation to private
Write(PendingTargetWrite)|HalfClose(SinkHalfClose). No second owner/slot,
allocation, visibility, trait, public API or Target Adapter change.
The model still produces its nonfinal ACK first; one-step continuation keeps
the actual HalfClose effect and its exact session/flow/direction/final-offset/
completion identity. Resume retains that capability without Target half-close.
The test compares it after retry rejection and one-step PeerReset; actual
FlowFinished releases it, late failure RESET produces0 facts and owncharge1->0.

All producers, duplicate checks and releases use the one sink slot. Both
snapshots and cold write-interest recognize only Write. Retry preserves its
old admission/error priority, taking and immediately restoring any nonWrite
without an intervening fallible operation; the real tracer exercises the
earlier PendingTargetCompletion rejection, not that backup branch. The new
variant assertions strengthen exact custody; original two sink tests keep
their Offer/backing/Target/terminal/charge assertions.

Relative to DATA checkpoint only five files differ: parent9581,
completion1318, binding229, bound_failure679, sink377. Parent's18 formatted
new lines are offset solely by18 existing production method separators
(listed in entry.json), with no function/guard/code removed. Parent test leaf
has only three mechanical field references changed. Unchanged FailOnceTarget
and all other source hashes remain. No grandfather inventory edit.
Checkpoint: /tmp/mini-vpn-p3-sink-close.KIZhEU/accepted.json (381 hashes).

Same Heisenberg SOURCE01a08f0f-c716-76d1-b968-59d84479c3bb:
P0/P1/P2=0/0/0, Approve HalfClose increment. Bounded structure9/Clean9/System8;
no necessary patch. Independent381 hashes match. Main final actual gates:
three sink exacts1/1; full failure/sink12/12 repeated10 times (120 passes);
OwnerTarget140 both modes; TCP74/Root24/continuation16/real Bound1;
Knife162/repository17; strict standalone/Cargo governance7 each;
fmt/diff/all-feature lib check and77 untracked rs/md whitespace/newline green.
Existing smoltcp22/private_interfaces1/Knife dormant143 warnings remain.

refactoring-patterns separated the prior Move; diagnose/tdd reproduced actual
admission failures before GREEN. clean-architecture/system-design/ddia-systems
kept state and bounded settlement at one original Flow, without a new queue
or resource owner. code-review verified exact production consumers.
self-improving-agent records lessons only in project memory, not global skills.
Two read-only audit-command feedback failures (oversized JSON output and shell
quoting of a multiline probe) were corrected without source/test semantics.
The final HalfClose line-budget probe ran before applying its parent change.
No external, commit or push. This is not complete sink lifecycle/P3/Case C/R7.
Remaining mandatory work stays in joint51/TODO: revoke wake, both selected
drains/completion/join/retry/F retirement, full C/root lifecycle and capacity/
physical/credit acceptance. The latter C/root scope remains separately ungranted.

## Owner terminal-revoke progress-wake custody

2026-09-11. User continued the existing component scope. One test-only tracer
was added to the granted bound_failure/sink.rs; no production change or new
Interface. Failure prediction: a wake before terminal storage could lose the
real proof; a wake under the admission lock could poison it; premature release
could discard independent sink work or repeat Target operations on re-entry.
The existing terminal-first implementation directly GREENed. This is honest
characterization, not a manufactured RED or claimed new runtime functionality.

The test uses actual read Failure, incoming DATA Write, real staged failure
RESET transferred through the paired inbox, and peer RESET with a one-step
effect budget. It captures the original Supervisor-minted FlowFinished token
from its real queue; only then registers a fresh progress cursor/waiter using
unchanged PanicProgressOnce. Catch covers the real resume. After the wake
unwinds, same Flow remains TerminalSettling with Source/Target, original sink
Offer/payload backing and transit1. Real terminal admission discrimination and
actual RESET receipt classification identify the retained terminal/feedback;
the entire MemoryTarget snapshot stays unchanged. Repeated finish refuses
without consuming custody. Repeated revoke returns TerminalAlreadyDelivered,
not lock poison, and new source RESET returns Closed. Unknown publication and
the caller-held RESET are not promoted to clean completion.

Unchanged completion.rs retains Failed work/cause inside TerminalFeedback;
this is source evidence, not a new direct runtime assertion of private fields.
No getter, fake terminal, synthetic ACK, sleep, fallback Adapter or cleanup was
added. The three previous sink tests are byte-identical after deleting the
new test. Only sink.rs377->523 differs across381 baseline Rust hashes;
parent9581/completion1318/binding229/bound_failure679/governance752 unchanged.
No grandfather shrink or inventory edit.

Checkpoint: /tmp/mini-vpn-p3-terminal-wake.uoiT5d/accepted.json. It records exact
Cargo argument arrays and381 source hashes. The exact tracer is
owned_upstream::owner_target::tests::r7_nested_abort::bound_failure::sink::
terminal_revoke_wake_unwind_preserves_failed_sink_and_real_terminal.
Main actual evidence: exact1/1 before/after leaf-only rustfmt; related13/13
plus10 serial repetitions (130 passes); OwnerTarget141 default/all-feature;
TCP74/server Root24/continuation16/real Bound1; Knife162/repository17;
strict rustc -D warnings standalone governance7 pre/post, Cargo governance7;
fmt/diff/all-feature lib check and77 untracked rs/md whitespace/newline green.
Existing smoltcp22/private_interfaces1/Knife dormant143 warnings remain.

Same Heisenberg SOURCE review01a08f22-1501-7f03-a0e4-2fd0015571de:
P0/P1/P2=0/0/0, Approve this tracer; bounded structure9/Clean9/System8.
Independent381-hash and inverse checks agree. Reviewer ran no Cargo.
diagnose/tdd kept the wake causally isolated; code-review checked actual call
ordering and evidence limits; self-improving-agent updated project memory.
A checkpoint orchestration attempt used unavailable structuredClone and failed
before writes; JSON round-trip copying corrected that tool-only issue. No Rust
test failed, no second repair or scope expansion was required.

Next mandatory work is both selected drains/completion/terminal disposition/
join/retry/F-publisher retirement. Current selected Driver service maps Drained
and Pending to Waiting; the earlier direct Driver empty polls are not selected
phase acceptance. Full C/root lifecycle installer/original clock/deadline/due
remains mandatory P3 and separately ungranted. No P3/Case C/R7 completion,
public cutover, external run, commit or push. Compact status/checklist updated
without an additional wording-only review or new architecture sidecar.

## Selected terminal-source EOF recording

2026-09-11. Continued original full P3 objective; get_goal now reports active.
The previous turn produced accepted runtime evidence, not a no-progress turn.
Same Heisenberg bounded plan review01a08f2a-b733-75f2-a29b-c454592f2b74 was
0/0/0 for four existing paths only. Its crucial correction: Control/Ordered
must execute inside their respective actual claims, and a cleared candidate's
Waiting cannot establish the repeated physical-EOF branch.

Failure tree predicted lost EOF fact (Drained conflated with Pending), cross-
lane completion, or premature resource/credit release. Original exact baseline
was1/1 and strict standalone governance7/7. Strengthening the existing real
terminal lifecycle test first produced only missing SourceDrained E0599;
after the pure value variant was added it compiled and reproduced runtime
RED at409: Waiting versus SourceDrained. Minimum implementation then GREEN1/1.

Original TerminalFlowSettlement now owns private source disposition:
Absent only after existing actual source-less Open proof; Revoking before the
real source admission operation; Draining only after its successful return.
Any typed refusal or progress-wake unwind remains Revoking. Its two independent
lane facts live beside the same real terminal/feedback/Source, not in metadata
or another table. Original selected Driver EOF calls an owning Flow operation
which validates both full LocalFlow identities against actual terminal, rejects
nonterminal/Absent/Revoking, then records only the selected lane once. This
precedes summarize/finish. EOF neither increments transit nor sets pulled, so
it cannot release Ordered dequeue credit, Source, F, Target or publishers.

The test preserves original RESET/DATA receipt, byte/permit and no-F-removal
assertions. Live Control Pending yields Waiting. Each lane in its actual turn
reports SourceDrained; a genuine late wake through that Flow's original ticket
republishes its candidate, and reselection/physical EOF now yields Waiting.
The added late-wake assertions directly GREEN1/1 after scoped rustfmt. They
exercise the stored already-drained branch rather than an empty selector.
Original direct physical empty polls remain supplemental, not primary evidence.

Only four paths changed: owner_ready1126->1127, commands694->699,
completion1318->1379, lifecycle test769->791. No new path, Interface getter,
callback, queue, allocation, second resource owner or inventory edit. Parent
9581/bound_failure679/sink523/governance752 and other381-baseline paths unchanged.
Checkpoint: /tmp/mini-vpn-p3-selected-drain.EXTHDs/accepted.json, with hashes
and exact command argument arrays. Main final actual evidence: focused1/1
plus10 serial repeats; lifecycle10; OwnerTarget141 default/all-feature;
OwnerReady18; TCP74/Root24/continuation16/actual Bound1; Knife162/repository17;
strict standalone pre/post governance7, Cargo governance7, fmt/diff,
all-feature lib check and77 untracked rs/md whitespace/newline green.
Existing smoltcp22/private_interfaces1/Knife dormant143 warnings remain.

Same Heisenberg SOURCE01a08f34-8a51-7c61-9e92-99bfb6ef0566 is0/0/0,
Approve selected-EOF component. Independent381-hash checks and four inverse
transforms recover the accepted baseline; reviewer ran no Cargo. Limited
structure9/Clean9/System8; local DDIA transaction assessment9. Reaching full
acceptance still requires the actual parked-lane eligibility, Retiring,
completion/join/retry/F retirement and complete P3 lifecycle/failure matrix.

diagnose/TDD isolated a real missing result before implementation; architecture
skills kept authoritative physical facts in one Flow with bounded constant
state and no hot-path indirection. code-review caught claim/evidence limits;
self-improving-agent updates only project learning memory. Inspection output
truncation was resolved by separate complete skill reads; a nonexistent-file
read was corrected to the rg-located parent definition, with no source effect.
No unexpected Rust failure, external run, commit/push or new sidecar.
Automatic re-eligibility of parked empty lanes is the next executable gate,
not covered by this test's deliberate late wake. Full C/root installer/original
clock/deadline/due remains separately ungranted and mandatory P3. No P3/Case C/R7
completion claim; the full goal remains active.

## Automatic terminal-source drain re-eligibility

2026-09-11. Original full P3 goal remains active. This is a runtime component,
not design-only progress. Pre-write strict standalone governance7/7, original
selected-EOF1/1 and Bound failure13/13 were green.

The finite plan initially proposed active-only validate_receipt_flow before
terminal revoke. Same Heisenberg found P1: checked installation deliberately
leaves BoundInactive, and original event/effect close is legal before activation
(joint3402). That check would publish sticky Corrupt for a valid lifecycle.
Revised same-two-file plan01a08f47-653d-76e2-9d69-d401a6cc917b approved0/0/0:
validate original business Source/Driver/terminal full LocalFlow, F position/
association/nonzero generation and Live-Retiring agreement; obtain the two
existing tickets before take/revoke. Their one-installation origin and original
publisher validate key/generation/occupancy; this is not an independently
minted metadata certificate. Actual selected poll/receipt validation is unchanged.

Added one real Bound-failure tracer using unchanged FailOnceTarget/fixture.
Both Driver lanes first genuinely Pending/finish in their own claims; real
Target ReadReady Hint then ReadAttempt FailureStaged occur under continuation;
real peer terminal, RESET transfer/receipt and selected Control EOF follow.
Initial original-code RED was the final claim returning Corrupt because no
session remained published. A diagnostic refinement wrongly used select(0),
confusing slot0 with the admission class. Its persistent failure after the
first implementation attempt was NOT valid evidence of a production defect.
After correcting to actual Session select(3), Main temporarily reversed only
its own production delta and verified the original SHA exactly; that corrected
test then reproduced genuine RED None-vs-Some(0). Reapplying the same minimum
implementation made the same test GREEN1/1. No assertions/timing/Adapter were
weakened; no synthetic first Driver wake appears in the test.

Original finish_target_flow now acquires both original publishers before
terminal take/revoke, records Revoking, calls real revoke, records Draining
only on successful return, then publishes both. Inactive publishers retain
due without active wake; active publishers rearm actual work. The original
revoke error/unwind remains sticky and does not become Draining. No new
callback API, allocation, queue, owner, full scan or resource release.
Original selected EOF/charge/credit/repeated-observation semantics are unchanged.
This test directly proves automatic Ordered re-eligibility after real parked
lanes. Control's independent first-wake causality remains masked by real RESET
publication; both calls have source proof, not two independent runtime proofs.

Exactly two Rust paths changed: completion1379->1419; bound_failure679->799.
Both complete inverse transforms restore prior hashes; all381 source hashes
are frozen in /tmp/mini-vpn-p3-drain-rearm.a4oLT2/accepted.json. Original fixture/
FailOnceTarget, parent9581, owner_ready1127, commands699, lifecycle791, sink523
and governance752 are unchanged. No new path or ratchet. TDD refactor assessment
kept this fixed two-ticket transaction in its original owner instead of adding
a shallow helper; the near-limit test leaf is not permission for further growth.

Actual final Main evidence: formatted exact1/1 plus10 serial repeats; original
EOF10 serial repeats; Bound failure14/14; lifecycle10; OwnerTarget142 default
and all-feature; OwnerReady18; TCP74/Root24/continuation16/actual Bound1;
Knife162/repository17; strict standalone pre/post governance7 each and Cargo
governance7; fmt/diff/all-feature lib check and77 untracked rs/md whitespace/
newline checks green. Existing smoltcp22/private_interfaces1/Knife dormant143
warnings remain. Exact command arrays are in the checkpoint.

Same Heisenberg SOURCE01a08f4c-ae6e-72c2-b0fa-d4e8340ff294 approved0/0/0,
bounded structure9/Clean9/System8. Independent381 hashes and both whole-file
inverse checks agree; reviewer ran no Cargo. diagnose/TDD isolated the actual
missing scheduler event; architecture skills preserved inactive compatibility
and original ownership; code-review caught the phase restriction before code;
self-improving-agent records the phase and selector-namespace lessons locally.
No skill-global edits, new architecture sidecar, external run or commit/push.

Remaining mandatory P3: independent idle-Control wake evidence; actual Retiring
service, selected completion/terminal disposition/join/retry and F/publisher
retirement; remaining joint51 capacity/physical/credit/full review. Full C/root
lifecycle installer/original clock/deadline/due is still separately ungranted,
not removed from P3. No full sink/root/P3/Case C/R7 or public cutover claim.

## Independent idle-Control wake causality

2026-09-11. Previous goal turn changed real production behavior and completed
its review/gates, so it was progress. Fresh381 hashes match that accepted
checkpoint; full P3 remains active. Same Heisenberg finite plan
01a08f57-bbd2-7273-8cb6-c9ef46c5c5c9 approved the existing test change and
one explicitly bounded temporary negative control,0/0/0. No new path or fixture.

Pre-write strict standalone governance7/7 and original exact1/1 passed.
First refactored only the existing test's repeated selected-lane tail:
same two real lane claims/operations/finishes, original RESET/receipt/transit
and Source/terminal/read-byte assertions preserved, now checked for both lanes.
Local references name the original executor/ready, not new helpers or owners.
No statement-level minification or dropped invariant. File799->781;
original scenario exact1/1 and Bound failure14/14 green before extension.

The same FQ now executes two fresh original fixtures, true and false rows.
True retains actual ReadReady Hint->ReadAttempt FailureStaged, real early peer
terminal, late local RESET receipt/transit1->0 and both SourceDrained observations.
False performs actual ReadReady Pending but no ReadAttempt, no failure injection
and no local RESET/receipt; real peer RESET then requires ControlSend and
SourceDrained with read_calls=0, transit0 and read-owned bytes0. Source and
TerminalSettling remain owned in both rows. Every lane uses real select(3),
claim/publication/visit/finish; no test wake or metadata setter appears.
The extended test directly GREENed1/1; it is additional characterization, not
a new production fix. Final leaf799; unchanged prefix679 and original fixtures.

The causal counterfactual then temporarily changed only completion.rs's
existing loop to wakers.into_iter().skip(1), omitting Control while retaining
Ordered. The actual same exact test compiled, completed true first, then failed
at769 explicitly failed_read=false: OrderedSend != ControlSend. No other gate
ran under the mutant. Main immediately restored the loop and verified the
entire accepted production SHA; all381 hashes then differed only in the test.
The same exact test returned GREEN1/1 again. This targeted expected RED proves
Control is not being supplied by RESET, Target readiness or leftover fixture
bits. It is not an unexpected production regression or broad fault-injection
acceptance. Both causal lane obligations are now covered with prior Ordered
RED/GREEN and this new Control counterfactual.

Final Main gates on restored production: fmt/diff green; two-row exact10 serial
repeats (one test FQ, not two tests), Bound failure14; original EOF10 repeats/
lifecycle10; OwnerTarget142 default/all-feature; OwnerReady18/TCP74/Root24/
continuation16/actual Bound1; Knife162/repository17; strict standalone pre/post
governance7 each, Cargo governance7, all-feature library check and77 untracked
rs/md whitespace/newline checks. Existing smoltcp22/private_interfaces1/Knife
dormant143 warnings only. Production1419/ready-flow1387/service734 and all other
sources stay unchanged; no inventory edit. Exact args and381 hashes:
 /tmp/mini-vpn-p3-control-wake.wIKCwi/accepted.json.

Same Heisenberg SOURCE01a08f5c-32b6-7210-827a-ee1a063a2882 approved0/0/0,
limited structure9/Clean9/System8. Independent381 checks, original whole-test
inverse and restored production hash agree. Reviewer ran no Cargo or mutant;
Main owns their actual evidence. diagnose/TDD demanded the causal falsifier;
code-review verified both real rows and no invented first wake. Architecture
skills kept original dependency/ownership boundaries; self-improving-agent
consolidates the per-lane masking lesson in current project memory only.

Next actual work is genuine cold-Retiring service and its original terminal
proof, then selected completion/join/retry and actual F/publisher retirement.
A separate bounded read-only audit is running to select that first real seam;
no Retirement implementation or fixture expansion has been authorized by this
test closure. Full C/root lifecycle installer/original clock/deadline/due
remains mandatory P3 and separately ungranted. Remaining joint51 capacity/
physical/credit/full review is unchanged. No full P3/Case C/R7/sink/root zero,
public cutover, external run, commit/push or new architecture sidecar.

### Next executable gap: genuine cold-Retiring terminal custody

Same Heisenberg read-only audit01a08f61-8130-7822-9fc2-0869919d3d65 is
0/1/0 Request Changes for a metadata-only next plan, not a retraction of the
accepted Control test. Actual Unbound finish does not retain TerminalGrace;
Target tombstone pressure leaves join_pending, then that Flow's real grace
expiry sets retire_after_join. Existing importer admits the Source as Retiring
from that flag. The caller may still own the output proof, but the installation
API does not accept it; the original Flow has only TargetTerminalKind and no
TerminalSettling. LocalFlow/flags cannot reconstruct the missing authority.
Old post-import join retry can perform Target join/retire before it discovers
MissingPendingTargetCompletion in the Bound branch. No such runtime test was
executed by this audit; this is a source-proved reachable composition gap.

Existing Unbound characterization at owner_target.rs9502 already constructs
two real flows with Target tombstone capacity1 and second grace expiry before
first retirement. First new tracer must install at precisely that boundary,
using actual model Z>=2 and Target tombstone1. lifecycle_executor fixes model
Z1 and cannot reproduce it. Do not mutate grandfathered inline tests, borrow
child-private helpers, place tests into unrelated cold_open/sink leaves, or
invent a new Adapter/proof. Existing allowed commands/tests/bound.rs has699
lines/101 spare; a concrete full fixture and assertion candidate must fit.

Next production seam is original finish_target_flow: preserve the actual
terminal/reason at original Flow before potentially interrupting Target work,
while retaining Unbound normal output/join/removal/admission semantics. Merely
removing if bound would prematurely revoke Unbound Source and is not accepted.
Then real Retirement service must consume that proof; global join scanning and
ordinary Live Driver service are not substitutes. Completion1419 has81 lines,
ready/flow1387 has113, service734 has766; freeze the real transaction/test before
editing. This is within the original §51 convergence axis, not authority for
ungranted C/root lifecycle or new file paths. The current turn ends with all
accepted Rust hashes unchanged after the test closure; next implementation is
not yet frozen. Full P3 remains active/open.

## Genuine cold-Retiring terminal custody

Status: reviewed necessary-only component accepted on2026-09-11. Full P3,
selected Retirement/drain/F retirement and C/root lifecycle remain open.
Latest381 Rust hashes: /tmp/mini-vpn-p3-cold-retiring.bZTVtG/accepted.json;
prior checkpoint: /tmp/mini-vpn-p3-control-wake.wIKCwi/accepted.json.

The real two-Flow tracer uses model Z2 and Target tombstone1. Second RESET
really finishes while its Target join waits; second real grace expires before
the first tombstone retires. The original Executor/factory is then actually
installed, producing Retiring from the real retained Flow. Releasing the first
tombstone exercises the old retry path. That path is custody characterization,
not a selected Retirement unit or physical/root acceptance.

Exact new FQ:
owned_upstream::owner_target::commands::tests::bound::cold_retiring_keeps_original_terminal_through_ready_installation.
Command: cargo test --offline --locked --all-features --lib followed by that
FQ, --message-format=json -- --exact --nocapture --test-threads=1.
Compiled runtime RED0/1: bound.rs789 reported MissingPendingTargetCompletion
for Flow2 after the actual capacity-release join. No fixture/admission/import
failure preceded it. Minimum GREEN then1/1; ten serial repeats each1/1.

Original Unbound finish now validates Source/Driver identity and completed
Open disposition, then stores actual terminal/reason and original Open/None
feedback in the existing Flow's TerminalSettling before its Target work.
Cold means no revoke was attempted. Unknown completion is restored/rejected,
never overwritten. No new field, ledger, owner, accessor or production path.
Two original admission checks share one original-Flow predicate: only
Unbound+Cold+None/Open feedback is equivalent to old None. Bound, including
newly imported Cold, never receives that exemption. Global guard ordering,
duplicate checks, retry selector, snapshot, Bound settlement and old
Unbound successful join/removal are unchanged. Cold proof is not feedback
to replay and does not close its original Source.

Exact permanent scope: owner_target.rs9581 unchanged count (two admission
call substitutions); completion.rs1419->1465; existing commands/tests/bound.rs
699->800, original699-line prefix unchanged. All other378 Rust hashes match
the previous checkpoint; no governance inventory change or new Rust file.
Readonly formatted-candidate probes exceeded800 before insertion; retained
all observations using local named values and one exact two-element pattern.
Only the final800-line candidate entered the repo.

Evidence Main actually ran: pre/post strict rustc -D warnings standalone
governance7/7; original Unbound grace-before-join exact1/1 before/after;
new exact+10 repeats; Bound failure14/14; lifecycle10/10; OwnerTarget143/143
default and all-features; OwnerReady18/18; TCP74/74; Root24/24;
continuation16/16; actual Bound1/1; Knife162/162; repository17/17;
Cargo all-features governance7/7; fmt, git diff --check,77 untracked Rust/MD
whitespace/final-newline checks and all-features library check. All green.
Warnings remain existing smoltcp22, private_interfaces1 and Knife dormant143.

Same Heisenberg plan01a08f71-4c06-77d2-9218-97ab991bedc4 and independent SOURCE
01a08f76-6a9e-77f3-9898-01d075803de0 both0/0/0 Approve. SOURCE independently
verified381 baseline entries, exact three changed files, both production
inverses and original test prefix. Structure/Clean/System/DDIA9/9/8/8.
Reviewer ran no Cargo; test execution belongs to Main.

Next executable obligation: freeze actual selected Retirement using the
retained real Cold proof, exclusive revoke/real lane drain and original
completion/join/retry/F-publisher retirement. Do not relax ordinary Live
eligibility or use global retry as selected work. Current test file is800
and completion1465; preserve exact existing write scope and growth limits.
New test/production paths and full C/root lifecycle remain separately
ungranted. No commit/push/public cutover/external occurred. The original
full-P3 goal remains active, not complete.

## Selected Retirement executable-scope stop

2026-09-11 read-only next-package audit; no Rust edits, no test/Cargo execution.
Previous goal turn was progress: genuine cold-Retiring custody was repaired,
tested and independently accepted. At this turn's entry all381 current Rust
hashes matched /tmp/mini-vpn-p3-cold-retiring.bZTVtG/accepted.json.
That component remains accepted; full P3 remains active/incomplete.

Same Heisenberg review01a08f7e-5d79-7f90-ae25-fd1e0d2d938a returned0/2/0,
Request Changes for the proposed next selected-Retirement package only:

1. A complete selected transaction is absent. service_ready_target rejects
   retire_after_join after correlation; finish_continuation clears its due
   without a Retirement follow-up contract. Real grace release still invokes
   full-map join and completion retry before later selection. Bound release
   must migrate together with selected progression; preserve Unbound behavior
   and do not reopen ordinary Driver trees. The actual C/output eligibility
   and pending-lifecycle installer are separate, still-ungranted P3 obligations.
2. Placement is a real scope gate, not permission to compress code or spill
   tests. completion1465 has35 lines, ready/flow1387 has113. bound.rs800,
   flow_lifecycle791 and bound_failure799 have0/9/1. More decisively,
   tests/repository_test_module_provenance/owner_bound_service.rs556 currently
   locks seven terminal methods to completion.rs and their exact visibility.

The single proposed next action is a structure-only scope extension, NOT
authorization already received:

- Existing owner_target/completion.rs plus new private default-path
  owner_target/completion/retirement.rs: move the entire current970-1464
  eight-method terminal/join/receipt/retirement closure, behavior unchanged.
  Keep original phase/storage anchors; descendant reads ancestors, methods
  retain their original effective owner_target scope, private helpers remain
  private. No wrapper, callback, new getter, field or lifecycle owner.
- Existing owner_target/commands/tests/bound.rs plus new private default-path
  owner_target/commands/tests/bound/retirement.rs: move the complete current
  701-800100-line genuine characterization. Reuse its ancestor source_leg;
  keep all behavior/identity/lifetime assertions. Record the new FQ.
- Existing tests/repository_test_module_provenance/owner_bound_service.rs:
  update only target route/unique eight-method ownership/effective visibility
  for the actual Move. Existing scanner remains unchanged. No inventory edit.

After explicit grant: freeze exact extraction inverse and source routes,
baseline -> pure Move -> same exact/regression/provenance/governance gates ->
same Reviewer. Only then freeze one actual selected Retirement RED/GREEN
inside the original §51 Authority. No new behavior is mixed into the Move.
All normal1500/800/300 limits remain; Feature Writer never edits inventory.

The future first RED must obtain an actual OwnerContinuation claim and
original binding Retirement candidate, then observe its real owning operation
return BoundSourceRequiresSelectedService. A manufactured selection or a
global-retry-completed Flow is not evidence. Success eventually requires
original proof-driven Cold->Revoking->Draining, bounded actual dequeue/
disposition/EOF, Target join and exact receipt/feedback/transit, followed by
actual cached-registration/ticket release before metadata/F reclamation.
Rejection/pressure keep original work; cancellation/unwind/shutdown retain
sticky custody and real failures, not Cold rollback or synthetic EOF/zero.

Main judges current selected-Retirement executability4/10 (not a reassessment
of the accepted custody Module): a lawful test destination, provenance-preserving
Move and complete selected progression/reclamation still need closure.
Architecture/refactoring skills selected a complete Method Move rather than
a shallow forwarding Module; no new sidecar or substitute root was created.
No production/test/provenance/governance write, commit/push or external run
occurred. This is the first verified stop on this exact placement grant;
do not mark the full goal complete or blocked on this first occurrence.

Subsequent two automatic goal turns revalidated the same explicit scope stop,
both proposed paths absent and381 unchanged Rust hashes, with no new grant
or live worker. No safe in-scope implementation alternative became available.
On the third consecutive occurrence, update_goal returned blocked. Full P3
remains incomplete; resume only after the requested scope authorization.

## Retirement closure pure Moves

2026-09-11 user `继续` resumed the full-P3 goal and granted the exact preceding
two-private-path/provenance request. No C/root scope expansion follows.
Main used architecture/refactoring skills to move one complete Method closure;
this was a green-to-green structural stage, not selected Retirement behavior.

Allowed/actual Rust paths and exact final lines:
- owner_target/completion.rs:1465->971.
- new private default completion/retirement.rs:510.
- owner_target/commands/tests/bound.rs:800->700.
- new private default bound/retirement.rs:108.
- existing tests/repository_test_module_provenance/owner_bound_service.rs:592->618.

The original495-line production block owns all eight finish/join/receipt/
retirement methods. Five entries retain exact owner_target effective visibility
via pub(in crate::owned_upstream::owner_target); three helpers remain private,
all their calls inside the moved closure. Seven lexical super references move
one ancestor. No state/storage/caller/re-export/shim/getter/order changes.
Full production parent inversion plus rustfmt exactly matches its original.
Original block SHA:c63c2819859d52ae6716b8225178c316462ea2f79976338497d28bb83f98ac0b.
The moved100-line test is byte-identical, and its parent inversely exact.
Test SHA:630e54d3fb035aeec43374b9fb18a5e2b7f4a4a91b97b0b7bedeb3257aa3326b.
New exact FQ:
owned_upstream::owner_target::commands::tests::bound::retirement::cold_retiring_keeps_original_terminal_through_ready_installation.

Existing provenance now locks both private/unconditional/default routes,
all eight unique actual bodies (adding settle_terminal_receipt), five exact
effective scopes/three private helpers, and the direct test's unique location.
Original scanner and governance inventory remain untouched. First focused
provenance compile hit E0369 from Box<syn::Path> equality in the new assertion;
under standing in-scope self-repair authority Main replaced only that expression
with exact in-token/leading-colon/three-segment/no-arguments validation.
The same focused test then passed1/1; no production/dependency repair.

Main actually executed: pre-write strict standalone governance7/7; original
Unbound1/1, old focused1/1 and provenance1/1; after test Move new focused1/1;
after production Move focused1/1; ten serial focused repeats10/10.
Full final gates: Bound failure14/14, lifecycle10/10, OwnerTarget143/143 both
default/all-features; OwnerReady18/18, TCP74/74, Root24/24, continuation16/16,
actual Bound1/1; Knife162/162, repository17/17; post strict standalone
rustc -D warnings7/7 and Cargo all-features governance7/7. fmt/diff and
all-feature library check pass;79 untracked Rust/MD whitespace/final-newline
checks pass. Only existing smoltcp22/private_interfaces1 and Knife143 warnings.

Same independent Heisenberg:
plan01a08f94-2c50-7743-a481-b170b4585684 and
SOURCE01a08f99-f695-7290-90dd-1fde4609c68a both0/0/0 Approve.
SOURCE independently checked both complete inverses and the381 prior paths:
three existing changes plus two new children, no other Rust change/deletion.
Limited structural score9/10; no Move repair requested, no Cargo run by Reviewer.
Checkpoint /tmp/mini-vpn-p3-retirement-move.QUvYjm/accepted.json binds383 final
Rust hashes, evidence and reviews; original.json stores the three pre-Move files.

No ratchet needed: both shrinking parents were already within normal limits.
OwnerTarget parent9581 and governance752 remain unchanged. No runtime behavior,
public/legacy semantics, full C/root lifecycle, commit/push or external work.

Next executable gate is the selected Retirement transaction using original
Cold proof and actual ReadyIndex claim/correlated candidate. Reuse the two
children; couple Bound global retry removal to selected progression, preserve
Unbound semantics and retain actual Source/F/receipts/feedback/transit until
real retirement. Do not treat this placement approval as closing that P1,
C/root lifecycle installer/clock/deadline/due or full P3. Goal remains active.

## Selected Retirement/Completion — focused green, review/regression stop

2026-09-11. NOT ACCEPTED. Main continued the active full-P3 goal under standing
causal in-scope repair authority. No new Rust destination, C/root lifecycle
installer grant, scanner/inventory change, commit/push or external operation.
Prior accepted position remains "Retirement closure pure Moves" above.
WIP checkpoint: /tmp/mini-vpn-p3-selected-retirement.wiXjXT/stopped.json.
It binds383 current Rust hashes: twelve changed,371 unchanged, none missing.
Original entry/test source is in that directory's original.json.

The real first test uses authenticated original components, model Z2/Target
tombstone1, cold GraceExpired before join, same plan/factory Ready binding,
actual activation/Session claim/OwnerContinuation selection, exact position/
association/generation and4 queued Source bytes. It does not retire the first
tombstone or invoke global retry before its selected operation. Initial RED
was exactly BoundSourceRequiresSelectedService at the service call, not a
fixture/claim/compiler failure. The minimum implementation returned GREEN.

Initial production implementation, still under review:

- Original Source Cold->Revoking before actual close, then selected Draining;
  each actual receive is retained in the original private command cell before
  callback, transit is charged, and separate Send selection transfers it.
- Three bounded scheduling-only queues use existing F positions/generations;
  actual cell vacancy wakes one waiter. EOF or vacant Pending passes one
  opportunity onward; neither performs a loop over all waiters/Flows.
- Original named Join readiness and actual join run in separate visits;
  Source destruction runs under Releasing, then a later combined metadata
  finish releases F with checked next generation. Old keys remain immutable.
- Bound GraceExpired no longer globally drains joins/retries. Original
  Unbound behavior is retained. Selected Completion uses the existing per-Flow
  reducer transaction and returns actual observations/effects through C's
  original observed-page and ordinary output admission. The physical branch
  exists, but its actual eligibility is one outstanding P1.

Exact current UNFORMATTED source lines (no final format promotion yet):

| Existing Rust file | Lines |
|---|---:|
| owner_ready.rs | 1137 |
| owner_ready/flow.rs | 1432 |
| owner_target/binding.rs | 231 |
| owner_target/commands.rs | 749 |
| owner_target/service.rs | 855 |
| owner_target/completion.rs | 1028 |
| owner_target/completion/retirement.rs | 637 |
| owner_session_controller/ready_service.rs | 235 |
| quinn_leg_composition/owner/continuation.rs | 1251 |
| quinn_leg_composition/owner/physical_session_root/service.rs | 411 |
| owner_target/commands/tests/bound/retirement.rs | 412 |
| owner_target/commands/tests/bound/flow_lifecycle/cold_open.rs | 253 |

Bound failure parent799, OwnerTarget parent9581, governance752 and existing
provenance618 are unchanged from entry. Read-only rustfmt stdout probes predict
flow1475 and Retirement test580, not format gate acceptance or writes.

### Actual executed evidence

Commands used offline/locked all-features library tests with one thread and
nocapture; exact names below use --exact. Filters were actually nonempty.
Pre-write rustc -D warnings standalone governance7/7 and old cold test1/1 passed.

Under owned_upstream::owner_target::commands::tests::bound::retirement:::

1. selected_cold_retirement_revokes_source_without_global_retry_or_bulk_drain:
   runtime RED0/1, then GREEN1/1 and subsequent focused reruns1/1. The original
   Source is Closed to new admission, still owns4 bytes, and next work remains
   genuinely selectable. No whole transaction claim came from this first unit.
2. cold_retiring_keeps_original_terminal_through_ready_installation: migrated
   original assertions now follow real Retirement claims. It passes1/1 after
   Target join, then again1/1 after extension through separate actual Source
   destruction and F1->0 metadata retirement. The pre-release original proof/
   Source-pair/Target-zero assertions are retained, not weakened.
3. two_retiring_eof_waiters_pass_one_real_vacancy_without_another_transfer:
   new characterization directly GREEN1/1. Actual independent live Source
   sends two commands into a one-entry mailbox/Held Full; two closed Retiring
   Sources wait on the same cell. The real paired inbox frees one capacity;
   exactly one subsequent transfer vacates that cell, and both actual EOF
   visits advance by bounded handoff. Original live transit2/bytes8/segments2,
   three retained Flows and the blocking Target tombstone1 remain explicit.
   This is a component test, not C/root or whole-session cleanup acceptance.

Under ...::bound::flow_lifecycle::cold_open::,
cold_source_less_open_keeps_its_disposition_across_real_z_retry passes1/1:
finish original Ordered claim, then real OwnerContinuation/Completion claim;
same original OpenResult/TerminalGrace/no-reopen/other Flow transit1/bytes4/
segments1 assertions. An initial migration incorrectly predicted an OrderedInbox
claim; actual selection was OwnerContinuation. The test was corrected to that
real route, leaving original DATA mailbox-owned, not manufacturing a claim.

Original Unbound
owned_upstream::owner_target::tests::grace_expiry_before_a_capacity_blocked_join_retires_after_retry
passes1/1. The next full Bound failure filter ran14, passed12 and failed2:

- cold_local_failure_retries_without_borrowing_another_flows_driver_charge:
  bound_failure.rs:291 expected one terminal output, received0.
- real_z_pressure_retains_failed_work_and_driver_charge_until_actual_grace_release:
  bound_failure.rs:476 expected transit0, observed1.

Both still expect GraceExpired itself to retry globally. Main stopped later
gates at this first regression red; no production fallback was restored.
The batch did NOT run its subsequent lifecycle entry. No full OwnerTarget,
Root/C/TCP, provenance, fmt/diff, cargo check or post-governance ran afterward.
No ten-repeat series ran for this behavior stage. Existing smoltcp22 and
private_interfaces1 warnings only in executed successful compilations.
An intermediate missing Category alias caused one E0433; using the existing
Category alias restored compilation. A malformed Cargo-runner argument and
a read-only formatting-probe syntax error ran no tests and are not TDD REDs.

### Independent review, remaining work and exact scope stop

Same Heisenberg read-only reviews:
01a08fa1-abab-75e2-924e-473a7f785928 accepted the causal first RED, not complete
GREEN; 01a08fa8-34db-7900-9eb5-1194c595abe6 required both lawful GraceExpired
branches, owning Completion outputs and real wait/release edges. The next
plan correction required bounded EOF/vacant handoff, now characterized above.
Production SOURCE01a08fc5-0fdb-70f3-a855-86192f89fa21 is0/3/1 Request Changes:

1. Terminal-held Returned command is not another awaited inbox receipt. Settle
   its exact charge under original terminal proof; distinguish cold/local
   charge0. Settled/Reduced Failed still owns work/cause: add explicit
   FailedReleasing->FailedReleased before Free, sticky with cause on unwind.
2. request_retained_flow_work currently wrongly requires Active receipt
   admission for lawful BoundInactive owning expiry. Deepen that existing
   owning operation with same-exclusion identity/phase/revision checks;
   inactive only retains due. Keep validate_receipt_flow strictly selected.
3. Actual physical source-service eligibility omits Completion/Retirement.
   Enable only completed categories in original legal source-service states;
   keep Page/continuation/pending-lifecycle guards. Prove actual C/output route.
4. Preserve exact Join readiness Failed(cause), not generic Invalid or a new
   ordinary RESET on an already terminal Flow (P2).

The subsequent scope/repair review01a08fcd-2116-7360-a49f-5b47002f31b8 confirms
the full feedback/request repairs fit the existing owning seams, but the two
799-line-parent tests need a new explicit path grant. Existing sink.rs is
limited to a different frozen Write/HalfClose axis. Its free lines are not
permission. No Reviewer ran Cargo; source verdict remains unaccepted.

Request only:

- /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/owned_upstream/owner_target/tests/r7_nested_abort/bound_failure/completion.rs,
  private default child of the original parent;
- that parent's private mod declaration and complete removal of original
  tests239-323 (85 lines) and430-489 (60 lines), with fixture/FailOnceTarget/
  helpers/visibility unchanged;
- existing tests/repository_test_module_provenance/owner_bound_service.rs
  only for this route and two unique symbol ownership checks. Scanner and
  governance inventory remain forbidden.

Move first with exact body/inverse evidence and preserved known12/14 red,
not a claimed green baseline; then migrate real selected calls, retain all
original assertions, repair SOURCE findings, run focused/full gates and same
Reviewer. Parent predicted roughly654, child roughly152 before behavior;
exact rustfmt counts must be verified, no line compression or new debt entry.
This is the first verified turn at this exact new-path authority boundary.
Rust writes are frozen. Full P3 goal remains active/incomplete, not complete
or blocked yet. Full C/root lifecycle installer/clock/deadline/due and the
remaining joint51 capacity/physical/credit matrix remain mandatory later work.

Subsequent blocked audit: the next two automatic goal turns revalidated the
same scope restriction,799-line parent, absent requested child and383 unchanged
Rust hashes. The same Reviewer is completed, not a live process to wait for;
there is no new user grant or lawful alternative destination. Both follow-up
turns are no progress, not implementation or verified live waits. On the third
consecutive occurrence, update_goal returned blocked. No Rust write or test
rerun occurred during these audits. Preserve all current WIP; resume the full
objective after the precise test-path authorization, not by weakening its goal.

## Authorized Completion tests — reviewed local closure, next placement stop

2026-09-11: the user's explicit grant resumed the continuous P3 goal. All383
stopped source hashes matched; strict pre-write governance7/7. Only three Rust
paths changed relative to that stop: bound_failure.rs, its new completion.rs,
and existing repository provenance owner_bound_service.rs. Production, shared
fixture/FailOnceTarget/helpers, sink, scanner and inventory remained unchanged.

Pure Move first preserved the full85/60-line bodies and full-parent inverse.
Parent799->653, child153; original Bound failure still12/14 with exactly the
same output0vs1/charge1vs0 failures under new ::completion:: FQs. Focused route
test1/1; repository18/18 and Cargo growth7/7. Same Heisenberg SOURCE0/0/0,
submission01a08ff1-5e0e-7f91-9d71-eaf6327c33c7; local Move structure9/10.
Original texts and evidence: /tmp/mini-vpn-p3-selected-retirement.wiXjXT/completion-move.json.
This was preserved-RED movement, not green-to-green behavior acceptance.

Then migrate one test at a time to real selected service. Expiry must first
produce no unselected completion outputs. Finish original Send claim and
publication; Z case also visits the actually selected empty Ordered source.
Obtain original Continuation claim, real Completion candidate, owning Executor
operation, original metadata finish and outer publication/Ready finish.
Cold test retains other Flow transit1,4bytes and actual DATA output; Z test
retains original Offer/backing/cause/RESET and charge1 until actual reduction,
then exactly0 and one terminal, without a second Target.write. All original
assertions and fixture bodies survive an independent inverse check.
Both exact tests pass1/1; same Reviewer0/0/0 migration review
01a08ff5-78f4-7931-9796-2979d5037fbc. Final child rustfmt190->244 has only
layout/two trailing commas; same Reviewer0/0/0 format review
01a08ff7-b4af-7e91-b488-dd34688f89e2. Parent653/provenance646 are formatted.

Final local evidence actually executed:

- Both exact tests repeated serially10/10 each after formatting.
- Bound failure14/14 default and all-features after formatting.
- Focused repository route1/1; final full repository provenance18/18.
- Final standalone7/7 and Cargo all-feature code-growth7/7.
- Three-file scoped rustfmt, tracked targeted diff and all three actual source
  trailing-whitespace/final-newline checks pass. No global fmt/check claim.
- Only existing vendored smoltcp22 and existing private_interfaces1 warnings.

Full all-feature OwnerTarget separately ran145 tests:144 passed, one failed.
The exact failure was then reproduced0/1:
owned_upstream::owner_target::commands::tests::bound::flow_lifecycle::received_reset_retries_once_after_real_terminal_pressure_releases,
flow_lifecycle.rs:667, expected one RESET output but observed0 after expiry.
The batch stopped before default full OwnerTarget; full Root/C/TCP and global
fmt/check were not run. The local post-format gates above close only the two
granted test migrations and do not exclude this regression from P3 acceptance.
No third-test or production repair was made.

Read-only failure tree: the remaining test still assumes synchronous global
retry, and its three calls to retry_one_pending_target_completion now perform
only bounded waiter handoff, not actual reducer attempts. Repository search
finds this is the sole direct test caller. Keeping only three empty vectors
would erase its original repeated-Z-rejection obligation. Its formatted parent
is791 lines, too little for complete claim/selection/pressure lifecycle.

Same Reviewer confirms the smallest next authorization: new private
commands/tests/bound/flow_lifecycle/completion.rs, parent's default route and
whole70-line test Move, plus existing provenance route/one-symbol lock. Move
first preserving current0/1; predicted parent721/new child77. Then permit
child-local pre-bind assembly through unchanged lifecycle_executor at F4/Target4:
tested Flow plus three real imported competitors; actual expiry -> competitor
peer RESET refill -> selected Completion rejection three times -> final actual
release/success. Preserve actual returned RESET/full LocalFlow/charge and
all output assertions. Do not change shared fixtures/helpers/MemoryTarget
configuration or use post-bind OPEN: that path creates service_position=None
and cannot supply the required real imported F. No scalar or fake-wake bypass.

Checkpoint: /tmp/mini-vpn-p3-selected-retirement.wiXjXT/completion-test-stop.json
contains384 hashes and this turn's exact three Rust deltas. All pre-existing
production WIP is retained, not promoted. Earlier production SOURCE0/3/1 and
full C/root lifecycle/clock/deadline/due remain open. This is the first turn
at the new scope boundary; goal stays active, not complete or prematurely
blocked. No commit/push/external run, inventory edit or new design sidecar.

The first automatic continuation after this scope request is no progress, not
a verified wait: all384 Rust hashes still match, parent is791, the requested
child is absent, and the same Reviewer is authoritatively completed. No new
user authority arrived. The independent scope audit has already excluded both
alternative test destinations and post-bind dynamic OPEN. No Rust write/test
rerun or new plan was attempted. This is the second consecutive occurrence at
the resumed boundary; goal remains active, not prematurely marked blocked.

The following automatic continuation again made no progress: all384 Rust
hashes were unchanged, parent791 and requested child absent, scope still
ungranted, and the same Reviewer confirmed completed. No live job remained to
wait for. This is the third consecutive occurrence in the fresh resumed audit,
not reuse of the earlier blocked count. update_goal returned blocked; the
entire P3 objective remains intact and incomplete. No Rust/test/external work
was performed during either no-progress continuation. Resume after the exact
test path/routing/pre-bind assembly grant described above.

## Repeated-pressure selected Completion — test-source closure

2026-09-11 latest user confirmation grants the precise private pressure child,
parent route/whole test Move, existing provenance, and child-local pre-bind
competitors. The full P3 goal resumed; all384 stopped hashes matched first.
Strict pre-write standalone governance7/7. Production and shared fixtures/
helpers/config/scanner/inventory remained unchanged throughout this slice.

Pure Move: original70-line test retained byte-for-byte, parent791->721 and
child77; full-parent inverse PASS. Trimmed body SHA256
4ed9b894dd038402825117d22808e0bf2ae24942582c63c43adc5f3abcc264c8.
Exact new ::completion:: FQ compiled and preserved the original0/1 RESET0vs1
at child57. Repository19/19 and growth7/7 passed; this is preserved RED,
not green-to-green. Same Heisenberg Move SOURCE0/0/0 Approve,
submission01a0905a-0ff8-7172-bf97-42d64c245d84.

Child-only repair uses unchanged lifecycle_executor at original F4/Target4/Z1:
original Flow1 fills Z, then tested Flow2 and competitors3/4/5 are opened before
the real consuming install. Each round actually expires the current terminal,
refills Z through one imported competitor's peer RESET, finishes the real prior
claim/publication, services real dirty send sources, and selects the original
Completion. Before retry wait=None and original Returned remain; each of three
actual TerminalTombstoneCapacityExceeded reductions retains full LocalFlow,
original RESET and charge1, produces no RESET, and restores actual waiting.
Final release produces exactly one RESET, pending0 and charge0; paired inbox
and all claim/publication finishes remain asserted. The32-visit limit is only
a deterministic diagnostic bound, not a throughput/fairness acceptance claim.
No global retry test calls remain in the OwnerTarget test trees.

First exact repair1/1; scoped rustfmt then parent721/child268/provenance670.
Post-format exact10/10 serial repeats, full OwnerTarget145/145 both default and
all-features, repository provenance19/19, Knife16 provenance162/162, strict
standalone and Cargo all-feature growth7/7 each. Three-file rustfmt --check,
actual untracked no-index whitespace/final-newline checks and parent inverse
PASS. No oversized shrink/inventory change. Full Root/C/TCP, global fmt and
cargo check were not run. Library/repository/growth commands report existing
smoltcp22/local1 warnings; Knife16 separately reports smoltcp22/local143.
No diagnostic suppression or warning repair was attempted.

Same Heisenberg behavior/format SOURCE review0/0/0 Approve,
submission01a0905e-d538-7ff3-bf49-f6e28400f971. It independently follows the
actual retry branch: Returned plus wait=None must reduce; only the precise
capacity-rejection branch retains the command and returns empty effects.
It also verifies original assertion meaning, real pre-bind F and full
CommittedLeg value equality, private routing and unchanged production.
Reviewer ran no Cargo; test results above belong to Main.

Checkpoint /tmp/mini-vpn-p3-selected-retirement.wiXjXT/pressure-completion-closure.json
records385 hashes and the three Rust deltas; original Move text is beside it.
Only this test-source slice is accepted. Production SOURCE0/3/1 and full P3
remain open. Next resume existing owning repairs: terminal Returned/Failed
disposition, lawful inactive request, actual physical category eligibility and
exact Join failure cause. Read-only terminal audit still finds the None/Open
whitelist and missing Failed work-release phases; widening the whitelist alone
would lose owning work during final Free. Locality8/10, lifecycle/system and
DDIA safety6/10 for this unfinished production seam: reach10 only with exact
owning release/unknown phases and deterministic refusal/unwind/charge tracers.
No new design sidecar, production write, external run, commit or push.

## Inactive owning request — real runtime RED

The existing SOURCE0/3/1 repair continues within its original scope. Original
retiring-import exact baseline1/1 and pre-write strict standalone growth7/7
preceded the only Rust delta: owner_ready/tests/flow.rs754->795. It uses the
original metadata-only binding/LocalFlow, requests Retirement while inactive,
requires no epoch/root publication/Notify, and after real activation/claim
selects Retirement revision4 (initial due, owning request, two late hints).
This recipe proves no running TCP source, physical service or whole-root zero.

Exact command, also reproduced on2026-09-13:
`cargo test --offline --locked --all-features --lib owned_upstream::owner_ready::tests::flow::retiring_import_keeps_source_correlation_without_reopening_driver_service --message-format=json -- --exact --nocapture --test-threads=1`.
Compiled successfully,0 passed/1 failed/1755 filtered, exit101 at tests/flow.rs:751:
"the original inactive owner may retain work without a receipt claim: Corrupt".
No compiler errors; existing smoltcp22/local1 warnings. A reporting wrapper
first lost its captured output to its own undefined args variable; that is not
test evidence. The corrected read-only wrapper produced the exact result above.

Cause: request_retained_flow_work first calls validate_receipt_flow, whose
BoundActive/Running/publication requirement correctly rejects inactive receipt
service but incorrectly rejects a lawful owning request. Do not weaken that
receipt contract. The owning operation must retain same-key/correlation/Flow
identity and phase, checked revision, no partial destructive retirement and
original active publication under one exclusion; inactive due must never call
ReadyCell::publish or notify Root. Existing callbacks already distinguish those
phases, but advisory stale-hint ignore is not strict owning error disposition.

Production flow.rs is unchanged1432 raw/1475 rustfmt; tests/flow.rs is795.
One uncompiled in-memory duplicate-protocol draft formats to1552 and is rejected;
it neither authorizes line-limit relaxation nor proves no alternative fits.
The complete private typed-request/shared-publication candidate then formatted
to1571 in memory; rustfmt parsed it, but no candidate compilation/production
write occurred. It retains original ReadyCell transitions, strict receipt
validation and preflight before destructive replacement; no GREEN is claimed.
Main chooses complete publication extraction, not further compression. The
already-enumerated cold leaf could legitimately receive two whole preparation
tests (58 body lines), but that Move is unnecessary for the selected private
descendant test scope and was not performed. No new path is yet granted.

Same Heisenberg identity01a06bfa-4552-7ea3-b5ce-9d6b65f88992 is restored. App
inspection confirms prior submission01a0906b-d2b7-7620-8b9d-4236683d5a77 failed
on usage limits without a verdict. Resumed submission
01a09da8-3b15-7051-a1bc-e845c021ccf4 confirms existing P1:0/1/0, no finding
against the RED test; no inactive repair/source closure. Subsequent finite
scope review01a09db0-f077-7d33-96dc-7552b27b9369 returns0/0/0 only for the
proposed complete publication Move/test placement. Whole begin/Turn/notify
movement avoids visibility expansion; descendant tests access original private
metadata; provenance must qualify impl type/trait, not count ambiguous names.
Exact three new paths, existing parent/provenance changes, bounds and Move ->
behavior -> gates/review sequence are consolidated into existing joint51.
User authorization remains required. No replacement, self-approval or extra
design sidecar. Current owning Seam score6/10;10 requires the actual one-lock
repair and full negative/retention tests, not this source-placement verdict.
Checkpoint /tmp/mini-vpn-p3-selected-retirement.wiXjXT/inactive-request-red.json
retains all385 previous hashes except the one test delta; originals remain in
inactive-request-entry.json. No production GREEN, post-repair regressions,
post-repair provenance/governance or full P3 acceptance has occurred. Other
production0/3/1 obligations and full C/root remain open. No external run,
scanner/inventory change, commit or push.

The two subsequent automatic continuations made no implementation progress:
all385 stopped Rust hashes matched, all three requested paths remained absent,
and no actual user scope grant arrived. The remaining retirement and waiter
handoff both call the same request transaction; no alternate path was used to
claim its closure. The same Reviewer is authoritatively completed, not a live
wait. No Rust write or test rerun occurred in these two continuations. On the
third consecutive occurrence of this authority boundary (including its original
request turn), update_goal returned blocked. Full P3 remains unchanged and
incomplete; resume after the exact joint51 publication/test-path grant.

## 2026-09-13 Publication pure Move — accepted, original RED retained

The user explicitly granted joint51's finite publication/test paths and the
original parent/provenance scope. Main moved the complete owning-request,
admission, turn finish, shared publication/notification and controlled Wake
Implementation without behavior changes or visibility growth. Parent1095,
private child395, original test795 unchanged, existing provenance773. No
empty test scaffold, scanner/inventory write or other Rust change occurred.

Original three spans37/52/264 match exactly before formatting. Whole actual
parent/child equal scoped rustfmt of the original-source transformation;
raw originals/spans are in /tmp/mini-vpn-p3-publication-move.EfieqP/move.json.
Same Heisenberg submission01a09dc3-9459-7430-ba61-1f4cb4a44eaf independently
confirms inverses, private default route, effective scopes and complete
implementation ownership: P0/P1/P2 0/0/0, Approve Move/provenance SOURCE only.
Final386 Rust hashes: /tmp/mini-vpn-p3-publication-move.EfieqP/accepted.json.

Executed offline/locked, serial: fresh strict standalone governance7/7 before
and after; new provenance tracer exact missing-route0/1 ->1/1; OwnerReady
all-features pre/post17/18 and default post17/18 preserve only the original
inactive Corrupt at test751. Full Target145/145 in each feature mode,
repository20/20, Knife162/162, Cargo all-feature governance7/7; scoped fmt,
actual untracked whitespace, canonical inverses and all-feature library
check pass. Warnings smoltcp22/local1, Knife separately smoltcp22/local143.
Global fmt and full physical/Root/C/TCP matrix were not run for this Move.

This closes extraction, not owning behavior or existing production0/3/1.
The same user grant authorizes the next minimum GREEN and private tests:
typed owning request inside the original exclusion, exact identity/phase
checks, all fallibility before destructive Live->Retiring commit, inactive
due retention without root publication/Notify, original active protocol.
Strict receipt admission stays unchanged. P3/Case C/R7/Task4 remain incomplete;
no external, commit or push. Self-improving lesson is recorded in current
LEARNINGS; no separate architecture sidecar or further path grant is needed.

## 2026-09-13 Inactive owning repair — locally accepted

Only publication.rs and its two granted private test files plus the existing
provenance file change after the accepted Move. Final formatted lines are
515/1/555/792; parent1095 and original test795 remain byte-identical to Move.
The owning entry now sends a value-only Retained request through the original
shared publication exclusion. It validates exact binding/flow identity and
limits, expected Live/Retiring shape, legal active/inactive phases, inactive
unpublished state, checked Flow and original ReadyCell revisions before the
same first-fault admission RMW and metadata commit. Only derived scalar Flow
metadata is staged; no Source/lease/ReadyCell copy, allocation, temporary Free,
new visibility or second publication mechanism. Original ReadyCell publish
calls occur before destructive Flow replacement; notification remains outside
the lock and deferred to original finish for owner-local calls.

The old lawful inactive request's Corrupt RED is now GREEN. Seven new private
tests characterize inactive Live retirement, active Completion/finish Notify,
17 rejection cases, eight revision-exhaustion cases, actual metadata quarantine
with late advisory-ignore versus owning fault, source-less Completion and
strict receipt separation, plus three invalid active-interval/phase cases.
One additional causal RED found an inactive slot already in Root's index:
request returned Ok instead of Corrupt. Same-lock preflight of the fixed four
hierarchies closes it before mutation (bounded hierarchy selection, no Flow
scan). Inactive success changes only due/revisions/phase; no ReadyCell publish,
Root index/epoch or Notify. Failures preserve the original record/first fault.

Two test-only compile feedback repairs were scoped and analyzed: sibling-private
contains became the existing ReadyTree select; ReadyPhase's missing Debug was
handled with an exact matches assertion, not a production derive. Both exact
tests then passed. A read-only rustfmt-stdin shell-quoting probe initially failed
before Rust ran; corrected argument quoting passed without a source change.

Executed offline/locked/serial after final source formatting: OwnerReady25/25
and OwnerTarget145/145 in each feature mode; entire all-feature OwnerReady25/25
repeated ten times; repository provenance20/20, Knife162/162; fresh strict rustc
standalone growth7/7 and Cargo all-feature growth7/7; scoped five-source fmt,
actual untracked whitespace/newline, full baseline-delta check and all-feature
library check pass. Warnings remain smoltcp22/local1, Knife smoltcp22/local143.
Current388 hashes and command summaries are in
/tmp/mini-vpn-p3-publication-move.EfieqP/behavior.json.

Same Heisenberg behavior SOURCE submission01a09dda-48dc-7373-9e28-13a858cbe632
is0/0/0 Approve and explicitly closes the inactive P1. Reviewed publication
SHA c6ffb4a5633c3a6bc759405304fa4da2436519fc0e9c00e31ea86a9c33ba8bff matches.
Reviewer confirms original begin/finish/notify/Wake bodies unchanged and
only four granted post-Move paths differ. SOURCE scores: architecture9.5,
Clean Architecture9.5, System Design9, DDIA9; no necessary source patch or
new design phase. Reviewer did not execute tests; Main independently completed
the scoped combined Target/callback gates listed above. These are local
acceptance, not a substitute for the remaining whole-root/runtime evidence.
Full Root/C/TCP and global fmt were not run. Existing Returned/Failed release,
physical eligibility and exact Join-cause findings remain; P3/Case C/R7/Task4
are incomplete. No scanner/inventory, unrelated Rust, external or commit/push.

## 2026-09-13 Terminal Returned/Failed disposition — locally accepted

Same Heisenberg final coherent SOURCE review is0/0/0 Approve for this local
normal-disposition finding; full selected physical/P3 acceptance stays open.
The active P3 goal authorizes causal in-scope feedback repairs. Main changed
only completion.rs1143, its existing retirement.rs841 and the granted existing
bound_failure/completion.rs596. All385 other prior Rust hashes match; no new
Rust path, fixture/config/helper/visibility/scanner/inventory change.
Original3 sources and current388 hashes/commands:
`/tmp/mini-vpn-p3-terminal-disposition.vWWYVq/originals.json` and
`/tmp/mini-vpn-p3-terminal-disposition.vWWYVq/accepted.json`.

First actual tracer gets real Z4 RESET rejection, frees one predecessor Z,
receives an actual peer terminal and expiry, then drives original Ready
claim/Retirement. Its expected runtime RED was only transit1 instead of0.
Minimum GREEN validates original selected F/binding/Source/full terminal and
positive charge, publishes TerminalDisposing before consuming the already
Returned RESET, then decrements once and publishes TerminalDisposed. It does
not call a retired reducer or await a nonexistent second receipt.

Second tracer first used an invalid incoming Bytes Drop sensor. The TCP
receive boundary intentionally copies to normalized bounded backing, so that
sensor fired before the supposed measured release; this was test feedback,
not a production regression. Main removed that probe and reused unchanged
stage_write_failure. A clone of the actual Failed Offer segment observes
shared->unique on that original normalized backing. Corrected runtime RED:
the payload stayed shared after selected work parked. Minimum GREEN stores
the exact first cause in FailedReleasing, destroys original work only after
true settled feedback/zero transit, and records FailedReleased after success.
Source/F stay resident; their later release is separately selected. This
observes the Failed reference release, not destruction of the allocation still
owned by the test. FailedReleasing remains sticky on hypothetical unwind.

Full release test runs three real origins: ordinary Reduced; Z4 Returned plus
peer terminal; original InTransit plus peer terminal and its actual late
receipt. Each preserves its own charge, performs one Target write, releases
the actual Failed reference before Source/F, and ends at the original F Free.
Existing cold/local independent-Flow charge tests remain unchanged. New test
FQs under owned_upstream::owner_target::tests::r7_nested_abort::bound_failure::completion::
- selected_terminal_disposes_returned_reset_without_waiting_for_a_second_receipt
- selected_terminal_releases_actual_failed_work_before_reclaiming_its_flow

Actually executed after exact three-file rustfmt: child4/4 and ten serial
repeats of4/4; full OwnerTarget147/147 default and all-features; OwnerReady25/25
and TCP74/74 all-features; repository20/20, Knife162/162; freshly compiled
rustc -D warnings standalone growth7/7 and Cargo all-feature growth7/7;
scoped fmt, tracked diff and exact untracked whitespace checks; all-feature
library check. Cargo commands use offline/locked and single-thread tests.
The independent standalone process overlapped early Cargo provenance checks,
and its collected terminal result is7/7; no failures were bypassed.
Warnings remain smoltcp22/local1, Knife smoltcp22/local143.
Full Root/C, global fmt and external gates have not run.

Same Heisenberg's bounded Failed-destruction audit reports0/0/0: actual legal
work producers own normalized Offer segments, scalar HalfClose or read
invariant; no user callback is retained in this destruction seam. The
incoming-owner panic is therefore not a valid injection. Do not invent an
impossible SessionEffect or weaken backing normalization. No arbitrary Drop
panic runtime evidence is claimed; future callback-bearing work reopens this
audit. Full SOURCE submission01a09e01-2a5b-7da0-a2f5-cd158e8df636 now Approves
0/0/0 and explicitly closes the original Driver Returned/Failed normal
disposition finding. Reviewer matched all3 SHA values and385 unchanged files;
old test prefix remains exact. Scores Architecture9.5/Clean9.5/System9/DDIA9.5.
No necessary patch or new interface; Reviewer ran no Cargo. Main owns the
actually executed gates above, including the later all-feature TCP74/74.

Remaining independently required: actual physical Completion/Retirement
eligibility and real C/output selection; exact Join readiness failure cause;
full C/root lifecycle/original clock/deadline/due and joint51 capacity/physical/
credit/full review. Separate ungranted scope stays closed. P3/Case C/R7/Task4
remain incomplete; no public cutover, external/TUN/VPS/WAN/M2, commit or push.

## 2026-09-14 Physical Retirement — actual Root normal release locally accepted

Same Heisenberg SOURCE01a09e1e-9c66-7be3-ab40-7d52e7b6616d returns0/0/0 Approve
for this component. Main completed the actual runtime/growth gates below;
Reviewer ran no tests. Full P3/Case C/R7/Task4 remain active/incomplete.
Original2 sources, current388 hashes, exact commands and raw test summaries:
`/tmp/mini-vpn-p3-physical-retirement.GA2g7y/originals.json`, `candidate.json`,
`gates.json` and `accepted.json` in that same directory. Prior accepted checkpoint
is `/tmp/mini-vpn-p3-terminal-disposition.vWWYVq/accepted.json`.

The finite read-only scope audit found a legal tracer within existing joint51
current_only/bound_source.rs. No new fixture/path/Interface grant was needed.
The shared MemoryTarget config and both shared CurrentOnly/Active fixtures
remain unchanged; the original full Bound test and fixed FIFO/flush/join tail
are byte-exact. Only continuation.rs1254 and existing bound_source.rs600 changed,
with386 other Rust sources unchanged. Two child-private prebind adapters move
actual frames across actor FIFOs/attachments and invoke the original Executor;
they contain no reducer/scheduler/finalizer policy and never run after binding.

Preparation uses original hello DATA plus its real nonfinal ACK, peer held DATA
and CLOSE, Target EOF/CLOSE, and peer final ACK. Actual graceful termination
retains four written bytes, so original join returns PendingOwnedState. The
original TerminalGraceStarted proof is explicitly expired PREBIND; actual
take_written returns those four bytes without an Executor retry/join. Original
Flow and pending join remain1, Target joined0, buffers/read credit0 and pending
feedback0. Checked C/seed/CurrentOnly/adopt/start then consume the same owner.

Two in-scope test-feedback repairs preceded the intended RED. First, missing
hello ACK left59 bytes available for a64-byte read reservation; the actual DATA
ACK repaired that preparation without capacity changes. Second, the snapshot's
pending_target_completions counts needs_feedback, not terminal custody; its
correct expected value is0, alongside retained Flow/join1. Both were test
assumption errors, not production regressions. Then the exact runtime RED was
only Root retirement releases0 vs expected1 after successful preparation,
binding, activation and park. Exact FQ:
owned_upstream::quinn_leg_composition::owner::physical_session_root::tests::current_only::bound_source::bound_cold_retiring_flow_reaches_actual_selected_release_once

Minimum GREEN adds only Category::Retirement.mask() to original legal
source-service eligibility. Completion remains disabled; lifecycle.has_pending,
Page and continuation gates are unchanged. Scoped rustfmt also expands one
existing complete_selected_and_admit if into canonical layout, semantics
unchanged. Reversing that formatting and the single mask line exactly restores
the original production file.

After binding, only production Root polling runs. There are no live reads,
pending receipts, failure work or outputs; empty terminal lane reads yield Hint,
not SourceRead. Thus the sole OwnerContinuation SourceRead is actual
RetirementReleased, after original Source/Target join/publisher/F settlement,
physical continuation finish, publication finish and original ready.finish.
The Root then parks, actual FIFO stays0/0, and fixed same-resident actor joins
remain clean. Logical controller/executor/handoff/output authority remains;
the test and review expressly do not claim whole-root zero or postbind expiry.

Actually executed: exact GREEN1/1 then ten serial formatted1/1 repeats;
CurrentOnly8/8 and OwnerTarget147/147 both default and all-features;
all-feature continuation16/16, Controller44/44, Root24/24, OwnerReady25/25;
repository provenance20/20, Knife16 provenance162/162, Cargo growth7/7;
fresh rustc -D warnings standalone growth7/7 before writing and after GREEN;
scoped two-file rustfmt, git diff --check, exact untracked whitespace and
all-feature library check. Cargo commands use offline/locked and test-threads=1.
Cargo gates were serialized; the final independent standalone process briefly
overlapped cargo check, and its full7/7 result was collected. No red was bypassed.
Warnings remain smoltcp22/local1, Knife separately smoltcp22/local143.
Global cargo fmt, full-library suite, separate known actor-loopback debt and
external gates were not run. C44/Root24 are suite results, not full lifecycle
or P3 architecture acceptance.

Independent review matches all388 current hashes and the exact inverse/old-test
checks, confirms full protocol/ownership lineage and unique SourceRead origin,
and finds no required patch/optimization. Limited Architecture/Clean/System/DDIA
scores are9/9/9/9. Only this physical Retirement normal-path finding closes.
Completion eligibility and actual C/output proof, exact JoinReady cause,
pressure/fault variants, separately ungranted postbind lifecycle/original clock/
deadline/due and remaining joint51 capacity/physical/credit/full review remain
required. No shared fixture/config/getter, scanner/inventory, new Rust path,
public cutover, external/TUN/VPS/WAN/M2, commit or push change occurred.

## 2026-09-14 Completion feedback loop — TerminalGrace-only scope required

This is a source-audited scope stop, not an implementation or runtime failure.
The physical Retirement component above remains accepted. All388 Rust sources
match `/tmp/mini-vpn-p3-physical-retirement.GA2g7y/accepted.json`; this turn
changed no Rust and added no tracer. Audit details and actual baseline output:
`/tmp/mini-vpn-p3-completion-scope.jh3pDE/audit.json`.

Main executed only the existing all-feature CurrentOnly baseline using offline,
locked, nocapture and test-threads=1:8/8,1758 filtered. Expected caught failpoint
panics are not failures; warnings remain smoltcp22/local1. No new RED, fmt,
build, provenance or growth run is claimed for this source-only diagnosis.

The proposed cold HalfClose preparation fails as evidence. Original
owner_target.rs apply_event_observed, around2166, handles TerminalGraceExpired
through Target retirement and retry_one_pending_target_completion. In
completion.rs around1065, Unbound retries immediately while Bound hands off the
indexed waiter. Thus prebind expiry cannot simply free Z while leaving the
same sole completion for later Root/C selection. Fake actor arrival for a timer,
apply_command(Event) bypassing the expiry hook, raw state/capacity modification
or a widened fixed fixture would alter the path rather than prove it.

The output-order diagnosis is also precise: continuation.rs admit_ordinary
around1000 retains an ordered page, and owner_output_drain.rs drive_one
around1165 consumes its front. An ACK preceding TerminalGraceStarted can reach
FIFO before the later lifecycle action is installed. The pending-lifecycle
guard is real but does not inherently prevent that first ACK. The missing
lawful Bound expiry consumer is the blocker; neither that guard nor Page/
continuation exclusion may be weakened to obtain a mask-only GREEN.

Same independent Heisenberg SOURCE review01a09e25-99fe-79a2-b06a-7604bf8fa0f9,
including supplement01a09e2a-e087-7813-9046-6d0b4a33f910, returns P0/P1/P2
0/1/0 Request Changes for the current Completion success candidate. He found
no verified legal entry that releases Z after binding while retaining the
original completion for real Root/C selection. Reviewer made no writes and
ran no tests. This does not reopen accepted Retirement or approve a new API.

The finite requested extension is only the complete normal TerminalGrace
consumer in existing joint51/section40: original opaque action, genuine start,
same clock, positive grace policy and immutable deadline; Z-bounded timer/due
custody and registration; full owning expiry, Target retirement, observations
and indexed Completion wake; real C ACK/FIFO and successor terminal custody.
Rejection, pressure, cancellation, unknown interruption and shutdown must retain
the exact work or committed outcome. No copied marker substitutes for custody.

Before Rust, freeze producer/unique owner/call route, clock/configuration,
complete dispositions, checked capacity/work bounds and exact files/symbols
in the existing write table, with independent review. No additional sidecar,
replacement, Legless, other lifecycle implementation, new getter/event bus or
directory-wide write grant is requested. This extension is NOT yet authorized.
The next actual vertical must prove real deadline->Z release->original carrying
Completion->C ACK->FIFO->retained successor terminal. No count-only substitute.

Main's narrow Completion composition-readiness assessment is4/10, not an
overall P3 percentage or a regrade of accepted components: the missing clock/
expiry owner prevents the real success path. A complete owning transaction,
bounded due service and actual disposition tracers are needed to close it.
Full P3 remains active/incomplete; other checklist obligations remain required.
No commit/push, public cutover or external/TUN/VPS/WAN/M2 action occurred.

### Independent JoinReady alternative audit, no Rust changes

To exhaust safe existing work while TerminalGrace scope remains ungranted,
Main and the same Heisenberg checked the independent JoinReady cause finding.
Retirement service192 discards Failed(cause), while193 propagates a readiness
Err without storing its sticky terminal cause; actual target.join is a later
operation at653. All four TargetIo implementations were inspected: the three
test Adapters delegate readiness, and MemoryTarget569-646 has no operational
Failed producer. UnknownFlow or internal registry corruption would test a
different path. The frozen FailOnceTarget injection cannot be silently extended.

Review01a09e36-295a-72b0-ae23-fa0d7ae68e94 with supplement
01a09e38-b8c3-7bd0-b948-f16a987ad531 returns0/0/1, retains the existing P2 and
confirms the finite fixture-scope blocker. The repair is independent of
TerminalGrace; its whole transaction can remain at original terminal/retirement
symbols. Existing r7_nested_abort.rs746 and bound_failure/completion.rs596 are
the exact requested injection/test-axis files; no new path is needed. Preserve
default delegation, other faults, visibility, original Source/Target/F and the
distinct first readiness cause; prohibit subsequent join, second RESET and false
release. This request is consolidated in joint51, not authorized execution.

Main rechecked all388 Rust hashes, drift0. Neither Main nor Reviewer ran a test,
build or format command in this follow-up. The preceding8/8 is the prior
Completion-audit baseline, not a new run. The existing audit.json contains this
bounded alternative audit and exact source/reviewer evidence. Full P3 remains
active/incomplete; no autonomous repair exists at these frozen boundaries.

## 2026-09-14 JoinReady — first cause retained at the original terminal

The user explicitly granted both finite extensions above. JoinReady now closes
locally; TerminalGrace remains a granted next design/implementation gate, not
runtime acceptance. Full P3/Case C/R7/Task4 remain incomplete. Current388 hashes,
exact commands/results and original-source checkpoint:
`/tmp/mini-vpn-p3-join-readiness.ZFRrST/accepted.json` and `originals.json` in
the same directory. The prior audit's blocked scope statements are historical.

Only four existing Rust files changed: owner_target/completion.rs1143->1161,
completion/retirement.rs841->854, tests/r7_nested_abort.rs746->759 and its
bound_failure/completion.rs596->798. Other384 hashes are unchanged; original
four test bodies remain byte-exact. No new Rust path, getter, visibility,
allocation, governance inventory or parent-anchor edit.

Private TerminalJoinPhase replaces the original readiness bool with
AwaitingReadiness/Ready/OperationalFailure(TargetFailure)/InvariantFailure
(TargetIoError). The exact returned value moves into the same original
terminal before the existing generic scheduler error returns. Failed-work
cause is separate. An early failed-phase check rejects before disposition,
Target/Source work; final retirement also refuses it. Original Attempt->next
visit join and valid final paths that never needed Ready stay intact.

The existing Adapter gains one optional single-use Join-only outcome and two
call counters; default delegation, other faults and old tests are unchanged.
The new test disables the optional Write fault only on its own fixture instance.
It uses real Control/Ordered claims, DATA/CLOSE/EOF/final ACK, original opaque
terminal and original owning expiry, retaining four actual written bytes.
Both operational TimedOut and typed ReadinessInvariant outcomes then traverse
selected Join. Three subsequent calls under the same still-valid selection
reject without repoll/join; original Source/Target/F and nonzero backlog remain.
Only afterward is publication dropped. This is not physical quarantine cleanup.

TDD chronology is preserved: first setup hit ReadyReceiptInvariant because its
claim was Control, repaired by the actual Control-finish/Ordered-claim sequence.
The real intended runtime RED then showed re-entry after a consumed Join fault.
Minimal production repair passed those assertions; a later test expectation
incorrectly required owner.terminal=Graceful although the real Target held it.
Observing original Target.readiness fixed that test-only assumption. Exact1/1
GREEN followed; test-local fixture destructuring then preserved ready-owner
lifetime and all original test bodies at formatted798 lines, exact1/1 again.
No formatted808-line probe or rejected helper extraction was written.

Executed post-GREEN: ten serial exact1/1 repeats; OwnerTarget148/148 and
CurrentOnly8/8 in default and all-feature modes; all-feature continuation16/16,
Controller44/44, server_root24/24, OwnerReady25/25, Target10/10, TCP74/74;
Knife16 provenance162/162, repository provenance20/20, Cargo growth7/7;
default/all-feature library checks. Cargo used offline/locked and all tests
test-threads=1;25 serialized Cargo gates completed, none failed. Fresh strict
rustc standalone growth compiled and passed7/7 both prewrite and post-GREEN.
Four-file scoped rustfmt check, tracked git diff --check, actual untracked
whitespace and388-hash post-review check pass. Warnings remain smoltcp22/local1;
Knife separately local143. No global cargo fmt/full-library suite or known
legacy actor-loopback debt run is claimed.

Same Heisenberg plan01a09e87-8e41-7b60-a1d1-a54bbeb04361 and SOURCE
01a09e94-b503-7de0-9274-60b926f620e3 both return P0/P1/P2=0/0/0 Approve.
Reviewer independently matched388 hashes and old-test prefix, no writes/tests.
Architecture/Clean/System/DDIA9/9/9/9 apply only to this local slice.
Runtime evidence establishes sticky refusal/custody; SOURCE establishes exact
private typed-cause retention. Do not claim the test directly reads that cause.

Remaining: real TerminalGrace same-clock/deadline/due consumer, physical
Completion eligibility and actual C ACK/FIFO/successor custody, synchronous
target.join error dispositions where required, remaining capacity/physical/
credit/full P3 review. Arbitrary callback unwind, whole-root zero, public
cutover, external/TUN/VPS/WAN/M2, commit and push were not executed or accepted.

## 2026-09-14 TerminalGrace — original fact route audit, no Rust change

Same Heisenberg bounded factual exploration finalized after submission
01a09ea7-faf3-7b90-a675-adb897aebaa8; this is not executable-plan approval.
Main independently inspected the current producer, shared fact consumers,
observed reduction/merge/resume, C sampling, lifecycle slots and Root park.
All388 current Rust hashes still match the JoinReady accepted.json checkpoint.
No new Rust/test, Cargo, runtime tracer, format or external run occurred.

Action-changing findings: enrich existing ReducerFlowDirectionTerminal with
the original full terminal identity instead of adding a parallel start event;
preserve the existing Client wrapper's scalar observer behavior. First-produced
terminal observations and delayed execution of their original FlowFinished
must be distinguished: continuation now is not the original start. Use one
Z-bounded lifecycle entry for the early fact and delayed opaque action.

Supervisor consume_after_model_commit_observed can poison before returning
observations. Success-page registration alone therefore leaves a committed
failure/unwind custody hole. The complete freeze must address that same path,
not pretend an output timestamp closes it. Remaining design decisions are
original precommit time/policy, committed failure custody, real registration/
due service and complete owning observed expiry with explicit bounded costs.
Then the real delayed-effect tracer and end-to-end expiry/Completion/C/FIFO
path must compile and execute. This audit is not another permission request:
the finite consumer scope is already granted. Full P3 remains active/open.

## 2026-09-14 TerminalGrace — execution design accepted; Supervisor pure Move

Same Heisenberg revision2 plan01a09ec9-248c-7ac1-8c2b-93b2ea5c7eac plus
owning-transfer supplement01a09ecb-6c02-75b0-b3dd-a415bebd6582 return0/0/0,
Approve complete-consumer execution contract; scores9/9/9/9. The earlier
0/2/1 design findings close through real DueHeld/unblock request transitions,
explicit policy/input return and both Bound caller migrations, fixed Z/index/
journal Layout selection and charged original snapshots/process fairness.
The selected consuming observation operation is not a borrowed getter or
wrapper clone. These are executable design decisions, not runtime acceptance.

First characterized pure Move only: four original Supervisor postcommit
methods moved into private unconditional default-path owner_observation.rs.
Their pub(super) restores the original parent-effective scope; no new field,
state, policy, timer, observation journal or behavior. Parent5597->5439,
new child169. Main Integrator changed only that exact growth inventory entry;
governance remains752 lines. Existing tests/callers are unchanged.

Exact inverse four-method block SHA256:
`07999ae0fa6c5fe6fac8d61f55d2a3a564749d258c03ad8aceb6b51cf939fb2e`.
Inverse parent SHA256 equals the accepted original:
`a6b963f5a0673a4e842a25ca6c1c6d2a4d91a886fc3920f874c0b41f33446d3f`.
New389-path checkpoint and full commands/results:
`/tmp/mini-vpn-terminal-grace-move.jjJY2a/accepted.json`.
Of the prior388 recorded Rust paths, only Supervisor and governance changed;
the other386 remain exact. This snapshot includes vendored baseline sources;
it is not a claim that389 is the governance collector's source count.

Executed: Supervisor all-feature48/48 before and after Move, default48/48;
full OwnerTarget148/148 both modes; ClientController37/37 both modes;
all-feature Controller44/44 and CurrentOnly8/8; Knife162/162, repository20/20,
Cargo growth7/7, both library checks. Fifteen post-Move Cargo gates completed
(including narrower94-test OwnerTarget passes before the full148 runs).
Strict standalone compiled and passed7/7 before writing, reached the sole
expected shrink6/7, then compiled/passed7/7 after the exact Integrator update.
Three-file scoped rustfmt, git diff --check and touched-source whitespace pass.
An overbroad whitespace probe found unchanged vendored baseline whitespace;
only the scoped checker was corrected, no vendored file was edited. Warnings
remain smoltcp22/local1; Knife separately local143. No new behavior RED/GREEN.

Same Heisenberg SOURCE01a09ed0-eeaa-7123-8765-f7b443925f80 independently
verified exact inverse/scope/visibility/routes and returns0/0/0 Approve, scores
9/9/9/9 for this Move. Reviewer ran no tests; execution above is Main evidence.
Next are the remaining characterized Executor/C/output Moves under joint51,
then deferred original-fact timing and the complete expiry->Z->Completion->
C ACK/FIFO->successor terminal tracers. No new sidecar or repeated permission
request is needed. P3 remains active/incomplete; journal/consumer behavior,
global fmt/full-library, external, commit and push were not performed here.

## 2026-09-14 TerminalGrace — Executor pure Move and source-route closure

Locally accepted preparation only. Twenty-one complete reduction/resume/page/
error-retention methods moved from owner_target.rs9581 to private unconditional
default-path observed_turn.rs754; parent now8841. Two pub(crate) methods keep
scope, nineteen pub(super) methods restore the former parent-private scope.
No field, journal, policy, timer, I/O, admission or behavior changed. Exact
four-block inverse and full parent SHA256 match original
`57b8838205bf21aa241bf17bb1b438366f8a10761199bdbbe26d65cc6099bb38`.

Prewrite strict standalone7/7 and full OwnerTarget148/148 were green.
Post-Move focused observed-pages4/4 passed; full147/148 exposed only the old
unit source guard's parent-only mint count. An isolated RED confirmed0vs1.
Bounded diagnosis proved parent0/child1 and exact inverse; the authorized
source-route repair modifies only that first guard in r7_owner_ack392->403.
It keeps child mint/consume exactly1, adds parent absence0, and preserves old
private gate/Supervisor assertions and every behavioral test. Exact GREEN1/1
and full148/148 followed. No production repair was needed. Strict standalone
then reached only the expected shrink6/7; Main Integrator lowered9581->8841,
with governance remaining752, and strict standalone passed7/7.

Final executed gates: OwnerTarget148/148 default and all-features;
all-feature Supervisor48, ClientController37, Controller44, continuation16,
CurrentOnly8, Root24; Knife162, repository20 and Cargo growth7. Both library
checks, four-file scoped rustfmt, git diff --check and touched whitespace pass.
Existing warnings remain smoltcp22/local1; Knife separately local143.
Same Heisenberg SOURCE01a09edd-b03a-7121-a9c0-1b02784619bb returns P0/P1/P2
0/0/0 Approve, scores9/9/9/9. Reviewer independently verified method/root,
source-guard and governance inverses and scope; ran no Cargo/tests/formatting.

Current390-path checkpoint, exact commands, historical REDs and final results:
`/tmp/mini-vpn-terminal-grace-executor-move.vUBx4S/accepted.json`.
Of389 earlier recorded paths, only owner_target, its first guard's file and
governance changed;386 remain exact, none deleted, one new child. Snapshot
includes vendored baselines and is not the governance collector count.
Continue characterized C/output Moves, then the complete accepted original
timing->expiry->Z->Completion->C ACK/FIFO->successor path. No journal/consumer
runtime, global fmt/full-library, P3/Case C/R7 completion, external or
commit/push is claimed. The full unbudgeted P3 goal remains active.

## 2026-09-14 TerminalGrace — Controller pure Move and source-route closure

Locally accepted preparation only: seven complete observed-page/progress/error
methods moved to private/default/unconditional terminal_grace.rs280. Parent
3536->3268; pub(super) restores original effective scope. Storage, admission,
entry, callers, cfg and behavior remain. Exact block SHA is
`2d04cab26f3ba0161c9e756a00c4fc70bfeb315e3790ab82f18c35cef4298c72`;
complete parent inverse is
`c9312b29c52af7447c07431fef00fbe6dda6b9ea1aefe59cb975f5cee3aaaa04`.
Baseline C44 and Knife focused1 passed; post-Move C44 passed. Predicted source
RED0vs4 was closed by visiting both parent and child with the same AST audit:
four constructor calls/four checked installations/zero named literals, unchanged
parent storage/inventory checks. Six local blank separators offset six route
lines, leaving Knife2624. Capacity guard473->474 only adds child to forbidden
source checks. Both complete test files inverse-match their baseline hashes.

Exact source GREEN1/1 and C44 followed; strict standalone sole shrink6/7 led
Main Integrator to lower3536->3268, governance752 unchanged. Final C44 both
modes; OwnerTarget148, Supervisor48, Client37, continuation16, CurrentOnly8,
Root24, Knife162/repository20/Cargo growth7, strict standalone7, both library
checks, scoped five-file fmt/whitespace and git diff check all passed. Existing
smoltcp22/local1 warnings, Knife separately local143. No new behavior tracer.
Same Heisenberg SOURCE01a09ee9-9f43-7502-9641-836d458823c4 independently verified
scope/inverses and returns0/0/0 Approve, scores9/9/9/9; no Reviewer test runs.

Checkpoint391 paths, all commands/history/results:
`/tmp/mini-vpn-terminal-grace-controller-move.PKuQHU/accepted.json`.
Four existing changes, one new child,386 other paths unchanged, no deletions.
Snapshot includes vendored baselines, not governance source count. Only output
preparation remains before full accepted timing/expiry/Completion/C-output
tracers. No journal/policy/timer behavior, full P3, global fmt/full-library,
external or commit/push acceptance. The prior lesson on unit source guards
applies; this route failure was predicted rather than an unexpected repair.

## 2026-09-14 TerminalGrace — final output preparation Move accepted

All four required preparatory Moves are now locally accepted. This last slice
moved fourteen complete output lifecycle impl blocks, including rollback Drop
and ApplyFailure Debug, to private/default/unconditional lifecycle.rs698.
Parent1676->990 retains declaration/storage anchors and all FIFO/page I/O and
admission. Six pub(super) methods restore the original parent-private scope;
pub(crate), traits, cfg(test), callers and tests are unchanged. The only other
existing Rust edit removes the obsolete1676 grandfather entry; governance
752->751, with no limit relaxation or new inventory entry.

Fourteen visibility-inverse blocks and full parent restore original SHA256
`35bc95ed98f623a2a57aa3e21e65129a9d07f4d3c460cc4596a47558b9a4ec1e`.
Before writing, all391 baseline hashes matched; preceding same-Rust strict
standalone7/7 supplies the prewrite growth evidence. Output baseline17/17 and
post-Move17/17 passed; standalone's only6/7 failure required removing the
now990-line parent entry. After Integrator deletion, strict recompile/-D
warnings and standalone7/7 passed. Final output17 default/all-features;
all-feature OwnerTarget148, Controller44, Supervisor48, Client37,
continuation16, CurrentOnly8, Root24, Knife162/repository20/Cargo growth7;
both library checks, three-file scoped fmt/whitespace and git diff check pass.
Warnings remain smoltcp22/local1, Knife separately local143. No new runtime
behavior tracer or source-guard repair was needed for this Move.

Same Heisenberg SOURCE01a09ef1-1e7b-7893-8f53-e491d93a0be2 independently verified
inverses/scope/rollback/no omitted source guard and returns0/0/0 Approve,
scores9/9/9/9. Reviewer ran no Cargo/tests/rustfmt. Current392-path checkpoint:
`/tmp/mini-vpn-terminal-grace-output-move.YMWwGL/accepted.json`.
Only two prior paths changed, one child added,389 others unchanged, none
deleted. Snapshot includes vendored baselines, not governance source count.

No preparatory Move remains. Next is the accepted real deferred-FlowFinished
original-time RED/GREEN, then complete expiry->Target/Z->carrying Completion->
C ACK/FIFO->successor terminal and independent pressure/error/cancel tracers.
No journal, policy, timer or normal consumer behavior was added by these Moves.
Full P3/Case C/R7, global fmt/full-library, external and commit/push remain
unclaimed; the P3 goal remains active, with no new finite-scope question needed.

## 2026-09-14 TerminalGrace — real deferred-birth characterization and Interface stop

The new self-contained test builds real Attach proof/commit, original Executor
OPEN and consuming checked ready binding. A peer RESET with effect budget1
commits the terminal fact before its pending FlowFinished executes. It observes
terminal reverse progress while output is empty, advances the paused clock25ms,
then consumes the original continuation's sole matching TerminalGraceStarted.
This is installed BoundInactive checked-C evidence: no active Actor/Root
polling, original-window persistence, deadline or expiry is claimed.

First sibling-route characterization passed1/1. Adding the explicit50ms policy
and original-window assertions yielded the predicted compile RED: missing fifth
constructor argument, terminal_grace field and private state/slot types only.
No runtime timing failure was reached. Before production GREEN, independent
SOURCE exposed a genuine frozen Interface contradiction: into_terminal_fact
consumes the bundle but cannot cross the private Owner/Reducer wrappers to
supply full TerminalGrace. Its private tombstone cannot be reconstructed from
the four scalar methods. Review01a09f01-2fc7-72d3-a290-09587c618c1b, restated
01a09f05-0635-7a12-aeec-00684f80cd5a, is0/1/0 Request Changes. The smallest
proposal is borrowed `terminal(&self) -> &TerminalGrace` on those two original
wrappers, retaining non-Clone owning fact/window throughout. Literal getter
prohibition requires explicit authority; no exception or method was written.
Interface executability8/10 is a local design finding, not a product score.

Same Reviewer separately approved the test route clarification0/0/0 in
01a09f03-183a-7d80-bcee-d4f6e84b08e3. C's private terminal_grace.rs owns the sole
positive-cfg explicit-path declaration of the already-authorized test leaf.
Natural descendant scope permits eventual private window assertions; no getter,
public field or sibling helper export is needed. Its fixture uses actual APIs.
The old test index is restored byte-exact1355. Production child280->284 only
adds that four-line route; deleting it inversely matches the baseline. New
leaf208, governance751 unchanged. One prior path changed, one new source,
391 other baseline hashes unchanged; final checkpoint393 includes vendored
baselines and is not the governance source count.

The unimplemented timing assertion was removed from current Rust after its
RED was recorded; its exact candidate and compiler evidence remain in
`/tmp/mini-vpn-terminal-grace-time.YUaTFM/`. There is no ignored or cfg-hidden
timing test and no timing GREEN claim. Current descendant characterization1/1,
Controller45/45 in both modes, Knife162/162, repository20/20, strict recompiled
standalone7/7, Cargo all-feature growth7/7, scoped two-file fmt/whitespace,
git diff check and both library checks passed. Warnings stay smoltcp22/local1,
Knife separately local143. No broad unrelated suite or global fmt was run.

Same Heisenberg final SOURCE01a09f07-5ea0-7940-ae2e-02e2099b540b approves only
this characterization0/0/0 and independently confirms hashes/scope/cause. It
does not close the token Interface P1; Reviewer ran no Cargo/tests/rustfmt.
Exact current commands, RED history, hashes and bounded claims are in
`/tmp/mini-vpn-terminal-grace-time.YUaTFM/accepted.json`.

Next executable gate requires only the explicit borrowed-full-token exception,
not reauthorization of complete-consumer scope. After it, correct existing
joint51 in place and implement original time then the full lawful expiry/Z/
carrying Completion/C ACK/FIFO/successor chain with its accepted failure tests.
No preparatory Move remains, and no production policy/journal/timer behavior
was implemented here. P3/Case C/R7/Task4 remain incomplete; no external,
commit/push, public cutover or whole-root-zero claim.

## 2026-09-14 TerminalGrace — independent same-clock deadline preflight accepted

While the full-token Interface exception remains ungranted, the already-scoped
session_clock.rs time-preflight primitive was implemented as a mandatory C
admission dependency. It is not a substitute consumer. deadline_window(Duration)
returns value-only observed/deadline SimTimes and native Instant, with no state
mutation, allocation, policy store, timer or terminal authority. Zero interval
and u64 interval representation are checked first; exactly one actual sample
uses the original epoch; relative and native deadline additions are checked.

Before writing, all393 previous hashes matched and standalone7/7 passed.
Missing-operation exact compile RED preceded same-sample1/1 GREEN. Zero-window
test then reached a real runtime RED (an immediate deadline was wrongly returned
by the unfinished primitive); explicit ZeroInterval and exact assertion gave
1/1 GREEN. Interval representation overflow, relative addition overflow,
exact u64::MAX domain boundary and backwards sample were added sequentially,
each directly GREEN1/1. On this platform native Instant can represent that
max boundary; its checked-add None path is SOURCE-only evidence, not exercised
runtime coverage. Paused values do not independently prove sample count.

Final set6/6; original clock2/2, Controller51/51 and CurrentOnly8/8 in both
feature modes; Knife162/repository20, strict recompiled standalone7 and Cargo
all-feature governance7, scoped two-file fmt/whitespace, git diff check and
both library checks passed. Existing smoltcp22/local1 warnings, Knife separately
local143. Exact lines are clock100/test282; no new path or governance edit,
391 other Rust paths unchanged. Original clock and test fixture/characterization
inverse-match their previous versions after removing only the new additions.

Same Heisenberg SOURCE01a09f13-9975-70d2-ace3-2422688015c0 is0/0/0 Approve for
the primitive only; no Reviewer Cargo/tests/rustfmt. Exact393 source checkpoint,
all commands and RED/GREEN evidence:
`/tmp/mini-vpn-terminal-deadline.G56H7j/accepted.json`.
The first patch application encountered a stale formatted-import context and
made no changes; current lines were read and the same scoped patch reapplied.
This was a tooling feedback correction, not a Rust failure or scope expansion.

There is still no production C caller. Explicit constructor policy, durable
original terminal/window custody, journals, Z ledger, timer, normal expiry and
actual Completion/C-output/successor remain required. Borrowed-full-token P1
remains OPEN; this work did not authorize its exception. Full P3 goal remains
active/incomplete, no external/commit/push or whole-root-zero claim.

## 2026-09-14 TerminalGrace — original full-token borrowed fact seam accepted

The user explicitly authorized the previously frozen borrowed-full-TerminalGrace
exception. This slice implements exactly that route: the original Reducer fact
stores TerminalGrace plus generation; existing scalar methods derive the same
values, and both original wrappers expose the narrow pub(super) borrowed view.
Fields stay private and the Owner wrapper remains non-Clone/non-Copy. The sole
constructor copies the original FlowFinished token, not a scalar reconstruction.
No second observation kind, registry, callback, re-export or lifecycle owner.

Before writing, all393 previous source hashes matched and standalone7/7 passed.
The new exact test first failed only E0599, missing terminal() on the Owner
fact. Minimal GREEN passed1/1. It uses real protocol Attach proof/commit, original
Supervisor/MemoryTarget/Executor OPEN and peer RESET at effect budget1. The
original non-Clone observation bundle remains alive across25ms and original
continuations. The sole delayed TerminalGraceStarted is full-equal to its
original fact, including private LocalFlow/tombstone; subsequent pages contain
no second terminal fact. This proves an Executor success path, not Bound C
original time, active Root, error/unwind custody, timer or expiry.

Final exact1/1, full OwnerTarget148/148, Controller52/52, Supervisor48/48 and
Client37/37 passed in default and all-features modes. The initial Target
tests:: filter ran94/94 only; it was recognized as partial and both complete
Module148/148 runs followed. All-feature CurrentOnly8/8, continuation16/16,
Knife162/162, repository20/20 and Cargo growth7/7 passed. Fresh strict standalone
-D warnings compile/run7/7, scoped four-file fmt/whitespace, git diff check and
both library checks pass. Warnings remain smoltcp22/local1; Knife separately
local143. No new warnings, global fmt or full-library test claim.

Only four existing Rust files differ: supervisor.rs5439, owner_observation.rs167,
owner_target.rs8841, C tests/terminal_grace.rs406. Both oversized parents keep
their exact line counts. No inventory edit, new source or deleted source;
389 other hashes are unchanged. Removing only the added test and restoring
the five-line module header inversely matches the prior test282. Those header
lines were corrected after broad gates to distinguish BoundInactive C, Executor
and clock scopes; same Reviewer approved the comment-only inverse, and final
scoped fmt/whitespace/hash checks passed. Test bodies did not change after gates.

Same Heisenberg SOURCE01a09f27-d218-7f62-b306-303226a7def1 and header supplement
01a09f2a-7658-7130-a511-2f679fb8cdbe return P0/P1/P2=0/0/0 Approve for this
four-file seam. The prior borrowed-token Interface P1 is closed within this
scope; local Interface executability8->9/10 is not a P3 score. Reviewer ran
no Cargo/tests. The existing verdict was re-delivered under
01a09f2e-aba9-7502-a8a4-b057f925a66a, not a new review or expanded acceptance.
Exact393 hashes, commands, RED/GREEN history and final review:
`/tmp/mini-vpn-terminal-fact.gB8Hna/accepted.json`.

Important unchanged gap: the observation is still produced after fallible
replay receipt release. This narrow representation change does not protect
postcommit errors/unwind or implement into_terminal_fact(self). The next
already-authorized work is original producing-turn window and root-owned
Supervisor/Executor/C journals, then Z/install/timer/full expiry/Target/indexed
Completion/C ACK-FIFO/successor with pressure/cancellation/error tracers. No
preparatory Move or repeated scope request is needed. Full P3/Case C/R7/Task4
remain incomplete; no external/public cutover/commit/push or whole-root zero.

## 2026-09-14 TerminalGrace — original producing-window C slice accepted

This is the first actual C success-time vertical in the already-authorized
complete-consumer sequence. The full goal remains active and incomplete.
Checked construction now requires the supplied positive grace Duration, using
the exact fifth C/sixth Preparation argument and original rejection returns.
The private Bound ledger owns exactly Z cold-initialized slots, free/birth-order
links, intrusive AVL identity metadata and one original producing-turn window.
The fixed session domain comes from the original sealed C/Executor binding;
there is no redundant scalar identity cache or first-fact identity inference.
After original fallible progress/ledger checks, the original non-Clone fact
moves through into_terminal_fact into its AwaitingOutput entry. Inventory
counts that actual fact once; a healthy deferred output is not failed.

The real checked BoundInactive tracer uses original Attach/OPEN/RESET paths
at effect budget1. A terminal born at5ms with50ms grace retains relative55ms
and the corresponding native deadline across25ms-delayed FlowFinished output.
Its delayed opaque TerminalGraceStarted matches the original complete token;
there is only one entry. The tracer is not active Root or actual timer evidence.
A separate policy test rejects zero, unrepresentable Duration and timestamp
sum overflow, returns the original clock/executor/plan/reservation/Duration,
then binds those same returned authorities once with a valid50ms policy.

TDD/evidence sequence:

- Prewrite393 hashes matched; prior strict standalone growth7/7 passed.
- The first tracer produced the expected missing fifth argument/ledger types
  compile RED. The first implementation then failed E0599 on a redundant
  SessionSnapshot::session_id cache. Same SOURCE verified the sealed identity
  route; deleting only that candidate cache gave the real exact GREEN1/1.
- A new assertion found Missing continuation incorrectly retained the prepared
  window and reported failed. Exact runtime RED then one known-precommit clear
  yielded GREEN. No clock-error input or retained continuation was discarded.
- Policy rejection characterization directly passed1/1; ten serial original
  window repeats each passed1/1. C54/54, OwnerTarget148/148, Supervisor48/48,
  Client37/37, CurrentOnly8/8 and continuation16/16 passed both feature modes.
- All-feature Root24/24, physical attach9/9, initial acceptance8/8 and physical
  recovery2/2 passed. Knife162/162, repository20/20, Cargo growth7/7 and fresh
  strict rustc -D warnings standalone7/7 passed. Scoped16-file rustfmt/actual
  whitespace, git diff check and both library checks passed.

An initially misspelled Client filter ran0 tests and is not evidence; corrected
client_controller ran37 in both modes. An unanchored target exclusion falsely
omitted three existing source files from a read-only hash check; explicit
baseline paths and anchored build-target exclusion proved393 paths unchanged
in membership. No file was deleted. New warnings were eliminated by cfg-aware
match and value-error scope alignment; only existing smoltcp22/local1 remain
(Knife separately local143). Global fmt/full-library tests were not claimed.

Same Heisenberg SOURCE01a09f4e-b38f-7c70-9686-d7a24fb31649 initially found only
P2: child error visibility narrower than original BuildError. The permitted
equivalent value-error scope alignment and accurate module header were
independently closed by01a09f53-dc25-7393-95f2-e9f87abd3585:0/0/0 Approve.
Reviewer verified the known-success flow, original identity/arrival separation,
cold backing/index logic and unchanged migration assertions, and ran no tests.
Multi-birth AVL rotations have SOURCE reasoning, not dedicated runtime evidence.

Seven production and nine existing test paths changed;377 other Rust hashes
match. C3268, OwnerTarget8841 and continuation test-index330 remain exact;
terminal_grace implementation621/test660. The330-line migration uses one
private test TTL constant and a local controller-to-owner alpha rename, with
no new helper/interface or changed assertions. No new/deleted Rust source or
governance inventory edit. All precise file counts, command arguments and
RED/GREEN/source-review evidence are in the393-path checkpoint:
`/tmp/mini-vpn-terminal-window.dMNA9p/accepted.json`.

Mandatory next work remains in the existing revision2 checklist: original
Supervisor/Executor/C owning journals (including conflicting original facts),
opaque action installation, one retained physical Sleep, full owning expiry,
Target retirement/indexed Completion, actual C ACK/FIFO and successor terminal.
Current local bundle ownership during fallible calls does not survive unknown
unwind; the prepared flag cannot substitute for the actual journal. Ledger
removal, due exclusions/unblock, pressure/cancellation/error and full cold-cost
tracers are not closed here. No scope or acceptance obligation was removed.
No P3/Case C/R7/Task4 completion, external/public cutover, commit or push.

## 2026-09-14 — C returned-page journal: historical provenance scope stop

Working delta is NOT package-accepted. Accepted checkpoint remains
`/tmp/mini-vpn-terminal-window.dMNA9p/accepted.json`; current393 hashes,
preserved original sources, exact commands and failures are recorded in
`/tmp/mini-vpn-terminal-c-journal.aT5m5e/pending.json`.

Earlier read-only review rejected preactivation HalfClose staging: the real
submit_target_completion path reduces immediately, producing a terminal before
CurrentOnly exact-zero. Neither weakening construction nor prebind expiry is
valid Root evidence. Same Reviewer approved the independent C journal plan.

Only C terminal_grace.rs621->647 and its test leaf660->772 changed;391 other
Rust hashes match, no new/deleted path or inventory edit. C stores the original
returned page in its existing checked aborted_turn before continuation/ledger/
progress checks. Disjoint executor/progress/phase borrows leave the actual
bundle root-held; known error retains it, success takes it once at the end.
No second queue/allocation/interface. The new normal-path metadata audit costs
bounded O(E) count/encoded-length work; it is not zero cost or full Flow inventory.

Prewrite393 hashes and fresh strict standalone7/7 passed. Fully qualified real
Bound RESET interruption test reached predicted runtime RED: C lost the returned
page. An initial short --exact filter ran0 tests and is not evidence. Formatting
predicted823 test lines; extracting identical original-window OPEN preparation
into one private same-leaf helper yielded772. Existing exact passed before/
after and new exact retained its RED. Reviewer inverse SHA matched the old660
lines, including unchanged assertions. Minimal owning GREEN then passed1/1,
plus ten serial1/1 repeats. Both modes C55/Target148/Supervisor48/Client37/
CurrentOnly8/continuation16 passed; all-feature Root24/attach9/initial8/recovery2
passed. Only existing smoltcp22/local1 warnings (Knife separately local143).

Knife161/162 stopped at constructor_calls expected4, actual3. The subsequent
checked_slot_installations also expects4 and requires3. Neither was repaired.
No subsequent repository provenance, postchange governance, fmt CHECK/diff or
library check ran; only scoped formatting application preceded runtime gates.

Same Heisenberg SOURCE01a09f71-55b2-7d33-b863-2721edcc92ee and scope supplement
01a09f73-8289-7ca3-9de2-1e44be5ab38a: two-file0/0/0 Approve. Six sealed
production callers exclude a second page overwriting this journal. Package
still has one P2 provenance/scope blocker. Next: explicit authority for only
tests/knife16_owned_leg_provenance.rs function
controller_aborted_pages_use_one_checked_constructor_and_complete_cold_inventory;
change both counts4->3 and quantity text, retain literal/constructor/inventory
locks; then exact/full Knife, remaining gates and same Reviewer closure.
No fake fourth constructor, scanner modification or new sidecar is warranted.

This real test's ordinary outputs are empty: it proves original fact/resume/
origin/window custody, not nonempty Frame-byte interruption. Target-error/
recovery, Supervisor/Executor journals and complete timer/expiry/Completion/
C ACK-FIFO/successor remain mandatory. Goal stays active; no full P3/Case C/
R7/Task4/public/external/commit/push claim. Documentation records a stop only.

## 2026-09-14 — C normal returned-page owning journal accepted

User explicitly authorized the exact single-function Knife exception. Same
Heisenberg closure01a09f7a-f3cc-70e1-a459-cfb72cef2c7d: P0/P1/P2=0/0/0 Approve
for this limited component, not full C journals or complete consumer/P3.
Current393-path accepted checkpoint, preserved originals and exact evidence:
`/tmp/mini-vpn-terminal-c-journal.aT5m5e/accepted.json`.
The preceding pending artifact above is retained as historical RED evidence.

Fresh prewrite check matched all393 pending hashes. The exact existing source
test first reproduced3-vs4 RED1/1. Only three lines changed: constructor_calls
and checked_slot_installations both4->3, quantity description four->three
returned/aborted-page paths. Exact GREEN1/1 and full Knife162/162 followed.
Original zero-named-literal, unique constructor/Self materialization and full
inventory assertions remain unchanged. Whole-file inverse SHA matches baseline.

Remaining gates actually executed: repository provenance20/20; Cargo all-feature
growth7/7; fresh rustc -D warnings standalone7/7; scoped three-file rustfmt check;
actual three-file whitespace/final-newline check; git diff --check; cargo check
--lib in both feature modes. All pass. Only existing smoltcp22/local1 warnings;
Knife separately retains existing local143. No global fmt/full-library claim.
The C647/test772 hashes match the prior runtime and SOURCE review exactly, so
the previously executed RED/GREEN, ten repeats, both-mode C55/Target148/
Supervisor48/Client37/CurrentOnly8/continuation16 and all-feature Root24/attach9/
initial8/recovery2 evidence remains applicable without claiming a fresh rerun.
Reviewer independently confirmed these hashes and the exact three-line repair;
Reviewer did not run tests or write files.

Since the preceding accepted time-window checkpoint only C647, test772 and
Knife2624 differ;390 other Rust sources unchanged, no new/deleted source or
governance edit. This closure settles the normal returned-page owning journal
and specified interruption/provenance debt. Full Target-error/recovery and
upstream journals, nonempty Frame interruption, real action/Sleep/expiry/Target/
indexed Completion/C ACK-FIFO/successor and all remaining revision2 requirements
stay mandatory. Goal remains active. No P3/Case C/R7/Task4/public cutover,
external/TUN/VPS/WAN/M2, commit or push. Current docs transcribe this evidence;
no extra wording-only review or design sidecar is needed.

## 2026-09-14 — Actual nonempty ACK returned-page interruption accepted

Test-only characterization, same Heisenberg SOURCE
01a09f83-08f4-7ba1-81df-950658b7aa12: P0/P1/P2=0/0/0 Approve.
Current393-path checkpoint with exact commands, old source and inverse proof:
`/tmp/mini-vpn-terminal-frame-journal.V8Ix9v/accepted.json`.

Only C tests/terminal_grace.rs changed772->757;392 other Rust hashes, all
production and governance remain unchanged. Two old complete OPEN prefixes
exactly matched the existing same-leaf helper and now call it; old leaf11/11
passed before/after. Removing the new tracer/imports and expanding those calls
inverse-matches the entire accepted772-line file, including every assertion.

New real Bound C ClientToTarget DATA writes hello to MemoryTarget before a
one-step continuation. First page has no output and retains the original ACK
effect. After25ms, the existing post-return hook interrupts the actual ACK page.
The test checks unique ACK fields/generation, actual encoded content/length,
stored and independently summed frame bytes, and original Vec/Frame addresses
across two rejected reentries. There are zero facts/resume tokens; custody is
the actual nonempty page plus unfinished window. Actor origin5ms versus this
continuation turn30ms/deadline80ms is explicit, not a fabricated terminal birth.
Direct GREEN1/1 is characterization, not a new production RED/GREEN fix.

Ten serial1/1 repeats; both modes C56/Target148/Supervisor48/Client37/
CurrentOnly8/continuation16; Knife162/repository20; Cargo and fresh strict
standalone growth7/7; scoped rustfmt/actual whitespace/diff and both library
checks all passed. Only existing smoltcp22/local1 warnings, Knife local143.
Reviewer independently checked source/inverse hashes and ran no tests/writes.

This closes only the specified normal returned-page nonempty Frame evidence.
C Target-error/recovery, original Supervisor/Executor journals, physical FIFO,
complete action/timer/expiry/Target/indexed Completion/C ACK-FIFO/successor and
all other revision2 requirements remain. No production, scope or inventory
change; no full P3/Case C/R7/Task4, public/external, commit or push claim.

## 2026-09-14 — Real Bound Target-error source characterized; inventory scope stop

Same Heisenberg SOURCE01a09f91-e322-7430-b745-92cd46eba849, evidence update
01a09f94-6d1f-71c2-be8a-8c96cc7905d6: P0/P1/P2=0/0/0 Approve, test-source only.
Exact393-path checkpoint, original source/inverse and actual commands:
`/tmp/mini-vpn-terminal-error-source.HR1f5q/accepted.json`.

Only existing C tests/terminal_grace.rs757->789;392 other Rust hashes, all
production/Knife/scanner/governance unchanged. Four complete checked fixtures
share a same-leaf helper; it returns the actual ReadyIndex and original C/
clock/leg/flow/native epoch, preserving 1s versus50ms policy and lifetime.
Old leaf12/12 before/after; removing additions and expanding four calls exactly
restores the entire original757 lines. Reviewer independently checked hashes
and inverse. A candidate range initially captured the ACK DATA prefix; source
inspection restored it before any test run, and the final inverse proves lossless.

Real Bound RESET with E1 and existing resume-id exhaustion returns the exact
typed EffectResumeIdExhausted. No continuation is minted. C retains original
terminal fact/window5ms/55ms/native55ms, Executor retains one aborted effect
tail, and reentry after25ms refuses without clearing retained observations/tail.
This directly GREENed1/1; no synthetic error/manual finish call or production
RED/GREEN claim. Ten serial repeats, both-mode C57/Target148/Supervisor48/
Client37/CurrentOnly8/continuation16, Knife162/repository20, Cargo/fresh strict
standalone growth7, scoped rustfmt/whitespace/diff and both library checks pass.
Only existing smoltcp22/local1 warnings; Knife local143. Reviewer ran no tests.

Separate pre-implementation review01a09f8d-11fb-75d3-8e4f-3eecc6064ffa found
0/1/0: original OwnerTargetError may own nested work/observations/recovery,
so page-origin nonzero does not account for its new owning slot. The proposed
existing AbortedOwnerControllerTurn journal is feasible, but must name an actual
retained_target_errors cold-inventory owner. This also changes the exhaustive
Knife field whitelist, outside the consumed three-line count-only exception.
No error field, production journal, new hook or Knife edit has been written.

Next requested authority is ONLY the existing Knife function
controller_aborted_pages_use_one_checked_constructor_and_complete_cold_inventory:
add retained_target_errors to expected_inventory_fields; same-function whitespace
only if needed for <=2624 lines. Preserve constructor3/installation3/literal0,
unique checked constructor and every old AST/inventory check; no scanner,
threshold/governance or other-function edit. Existing production revision2
scope is already granted and need not be requested again. Proposed C error-seam
readiness is8/10, not runtime acceptance: reach10 only with actual owning-error
inventory and known/unknown-custody tests reviewed under the supplement.

Original C error+bundle custody, Supervisor/Executor journals and actual
action/Sleep/expiry/Target/indexed Completion/C ACK-FIFO/successor remain mandatory.
No full P3/Case C/R7/Task4/public/external/TUN/VPS/WAN/M2/commit/push claim.

## 2026-09-14 — C returned-error owning custody and explicit inventory accepted

User explicitly confirmed the narrow Knife field-list supplement. Same
Heisenberg SOURCE01a0a2c2-1c2b-7270-b9d5-c73486fe4a13, restatement
01a0a2c7-fabf-7213-ab1b-742b6c3f237f: P0/P1/P2=0/0/0, Approve, local9/10.
Current393-path checkpoint, originals/inverse evidence and actual commands:
`/tmp/mini-vpn-terminal-error-custody.fALxW9/accepted.json`.
This supersedes the preceding scope stop, not the complete-consumer obligations.

Four Rust paths differ from the preceding source-characterization checkpoint:
owner_session_controller.rs3268; terminal_grace.rs688; its existing test leaf786;
Knife2624.389 unchanged, no new/deleted Rust source or scanner/governance edit.
Two oversized counts remain exact. Six identical old RESET constructions share
one private same-leaf value helper; original leaf13/13 passed before/after and
pre-rustfmt inverse expansion recovers the original789-line source byte-for-byte.
Every old assertion survives; the helper owns no resource or public Interface.

Original AbortedOwnerControllerTurn gains a private Option<OwnerTargetError>
and initializes it in the unchanged four-argument checked constructor. The
original error moves into that existing vacant slot before executor snapshot,
observation transfer or fallible progress. The actual original bundle joins it
immediately. Actual C phase stays Failed through settlement; a local candidate
phase is published only with known completion. Unknown unwind leaves the same
error/bundle/window root-held and rejects reentry. Known progress failure returns
the original TargetAndProgress while keeping observations; known completion
returns the original Target error and preserves old phase/window rules. The
unused private preflight wrapper was removed after caller audit, not replaced
with another Interface. No new getter, callback, backing or owner was added.

Cold retained_target_errors counts the actual owning Option, participates in
exact-zero, and does not claim nested command/observation payload inventory.
Existing Layout::new::<OwnerSessionController<T>>() includes the enlarged inline
header; no claim that its byte size is unchanged. Eight cold-impl blank
separators plus compact docs preserve the exact parent line ratchet. Knife
adds exactly that inventory field name and removes its same-function boundary
blank. Inverse SHA recovers the whole prior Knife; old constructor3/installation3/
literal0/unique-constructor and every other inventory/AST guard remain intact.

The existing real returned-terminal unwind test now covers both normal and
actual Target-error outcomes. Real RESET/E1 with existing resume-id exhaustion
first compiled and failed at missing C page after caught interruption: runtime
RED0/1, not a fixture/compiler failure. Minimum owning implementation then
GREEN1/1; ten serial repeats each1/1. The normal row retains its old assertions.
The error row retains exact EffectResumeIdExhausted, original fact/full token,
5ms/55ms/native55ms window, zero fabricated resumes and actual error-owner1
through rejected reentry. Original Executor effect tail is not claimed as C's.

The existing known-error test additionally exercises repeated genuine Bound
input refusal with no observation: no false failed phase, slot or window;
error-owner count returns0. Its value-only cold-inventory discriminator proves
that count1 forbids zero, not that a copied snapshot owns the resource. Actual
fatal RESET still returns the exact error to the caller, retains original fact/
window and Executor tail, and does not leave a duplicate C error. Exact1/1.
Known TargetAndProgress behavior is source-reviewed; no new direct runtime
tracer for that specific progress-failure branch was executed here.

Actually executed closure gates: both feature modes C57/57, Target148/148,
Supervisor48/48, Client37/37, CurrentOnly8/8 and continuation16/16; all-feature
Knife162/162 and repository provenance20/20; Cargo code-growth7/7 and freshly
compiled rustc -D warnings standalone7/7; scoped four-file rustfmt check,
actual whitespace/final-newline check, git diff --check and cargo check --lib
in both feature modes. All green. Only existing smoltcp22/local1 warnings;
Knife separately retains existing local143. No global fmt/full-library claim.
Main executed these gates; Reviewer reviewed source and did not execute them.

Next mandatory executable work remains original Supervisor committed-batch/
current/observations, Executor original page/work/bundle custody, recovery
interruption and real action/Sleep/expiry/Target/indexed Completion/C ACK-FIFO/
successor. Their existing revision2 scope remains granted. This local approval
does not accept detailed nested error payloads, physical zero or full timer/
expiry. P3 goal remains active; Case C/R7/Task4/public/external/TUN/VPS/WAN/M2/
commit/push remain unclaimed. Current documents transcribe this evidence once;
no new prerequisite sidecar or wording-only review is required.

## 2026-09-14 — Supervisor original committed receipt journal accepted

Same Heisenberg SOURCE01a0a2d9-2ac3-7030-909f-d8957bbc3ccc:
P0/P1/P2=0/0/0, Approve, local architecture9/10. Limited receipt component,
not complete Supervisor DATA/Executor or P3.393-path checkpoint with originals,
exact fixture inverse, commands and runtime evidence:
`/tmp/mini-vpn-supervisor-receipt.g2BFGs/accepted.json`.
Initial seam review01a0a2cf-6233-7573-bc62-c874b7271cc7 found0/0/0 and identified
the real permit-drop progress callback plus the distinct DATA caller gap.

Only supervisor.rs5439, supervisor/owner_observation.rs192 and existing C
tests/terminal_grace.rs794 differ;390 Rust paths unchanged, no new/deleted source
or Knife/scanner/governance edit. Parent exact ratchet remains5439. A private
cold Executor fixture first removed duplicate identical capacity/Target assembly:
leaf786->768, original13/13 before/after, inverse expansion recovers the prior
source byte-for-byte. The checked rejection fixture stays pristine; the OPEN
fixture still executes original real OPEN/continuations. No public fixture seam.

The existing real Bound C unwind tracer now has a third Supervisor row, alongside
unchanged normal and Target-error rows. The actual Supervisor is pre-armed using
the existing test-only OwnerDataUnwindPoint/arming operation; the new variant
fires only inside real FlowFinished receipt processing. No effect or observation
is fabricated and no Executor test pass-through is added. Compilation succeeds;
after caught panic the first runtime RED observes retained effects/observations
(0,0), expected (2,0). The original PeerReset/FlowFinished batch was stack-local.

Minimum GREEN adds one inline SupervisorReceiptJournal owning the original
Vec and original observations. All current admitted callers prove health before
model commit; the receipt seam checks that premise and never clears old poison.
It installs the actual journal and sets poison before snapshot/receipt work.
In-place retain_mut keeps current item borrowed in original backing. A successful
internal ACK is filtered only after settlement; visible terminal effects remain.
First Err preserves current and untouched suffix, with no further settlement.
Duplicate facts are checked before assignment, never overwriting the first.
ACK derived fields and catch-up invalidation finish before the sole success
take; known errors retain journal and return original typed cause. Unknown
retains journal/poison and cannot replay partially released receipts.

No second K backing is allocated. Original replay-candidate Vec allocations,
replay scans, K+3/E work and payload costs are not erased by this change. Actual
generic C Layout includes the larger inline Supervisor header. New snapshot
effects/observations counts read actual journal values, not synthetic ownership
or detailed nested-byte/physical-zero proof. Existing parent observation-method
blank separators were removed mechanically to preserve5439; no old getter
signature/field visibility changed. Shared Client success/error gates stay green.

Exact GREEN1/1 and ten serial repeats each1/1 show actual effects2/observations0,
poison, failed C and stable Supervisor snapshot after rejected C reentry.
Both-mode related suites executed: Controller57/57, Supervisor48/48, Target148/148,
Client37/37, CurrentOnly8/8, continuation16/16. All-feature Knife162/162,
repository provenance20/20, Cargo growth7/7; freshly compiled strict standalone
growth7/7 before/after; scoped three-file rustfmt, actual whitespace/final-newline,
git diff --check and both library checks pass. Only existing smoltcp22/local1
warnings; Knife retains local143. Main ran gates; Reviewer only reviewed SOURCE.

Local installed Rust retain_mut implementation was inspected: current/unchecked
values survive predicate unwind, including its compaction guard. That is SOURCE
evidence: this new pre-terminal tracer has no previously filtered ACK, so it
does not exercise the compaction-guard branch or the actual permit-drop callback.
Those deterministically falsifiable interruption branches remain on the active
checklist; success of the three rows cannot discharge them.

Also unclosed: driver_input's pre-receipt effects/visible/outstanding fact around
bind/publication, post-return/caller-added facts, original Executor page/current/
conflict-bundle journals, recovery interruption and complete action/Sleep/expiry/
Target/indexed Completion/C ACK-FIFO/successor. Original Owner DATA settlement
and named private test-tree authority is in joint51; inspect its actual axis
before integration, without inventing a directory grant. No full DATA-journal,
nested-byte, physical-zero, P3/Case C/R7/Task4 or external/TUN/VPS/WAN/M2 claim.
Goal stays active; no commit/push. Existing current docs and learning memory
record the component once, without another prerequisite sidecar/status review.

## 2026-09-14 — Real filtered ACK and permit callback receipt custody

Same Heisenberg SOURCE01a0a2e9-6348-7bd3-aff8-245c1d748eb4:0/0/0 Approve,
local9/10; Reviewer ran no tests. Evidence/original source/exact commands:
/tmp/mini-vpn-receipt-suffix.5optQG/accepted.json.
Only existing Owner DATA test leaf446 changes;392 of393 Rust paths unchanged,
no production/new path/Knife/scanner/governance edit. Original three tests
inverse-restore exactly; their fixture retains the same original CommittedLeg.
Both new tracers directly GREEN: characterization, not production RED/GREEN.

Both use actual Unbound Executor gated DATA4, peer CLOSE ClientToTarget0,
MemoryTarget EOF and actual TargetToClient CLOSE4 before final peer ACK4.
The actual reducer emits ReplayAcknowledged followed by FlowFinished.
Existing ReceiptTerminal hook after ACK filtering preserves journal1 effect/
1 observation, replay map0, real four permits0, model flow0/tombstone1 and
Target flow1. Exact panic source, unchanged Target and poisoned retry are locked.

The second registers a real factory wait only after preparation; actual poll
Pending and wake0 exclude earlier edges. The final ACK releases original
UplinkPermits; its final FactoryProgressGuard wakes the single-use panicking
test Waker. Callback records all four real balances0 before panic.
Current ACK remains unfiltered with original terminal suffix: journal2/0.
Original map extent1/metadata bytes4 differ from real permits0; this distinction
is explicit, not a false leak or a physical-zero assertion. Reentry leaves
whole snapshots/Target unchanged; final Drop causes no second callback.

Executed: original3 baseline/fixture3; each exact1 and ten serial1/1 repeats;
both modes Owner DATA5, Target150, Supervisor48, TCP74, Controller57, Client37,
CurrentOnly8, continuation16; Knife162/repository20, Cargo growth7, newly
compiled strict standalone7, scoped leaf fmt/whitespace, git diff --check and
both library checks. All pass; only existing smoltcp22/local1, Knife143 warnings.
Full-library/global-fmt/external/commit/push were not run.
DATA pre/post-receipt/caller-added facts, original Executor journals, recovery
and complete action/Sleep/expiry/Target/indexed Completion/C ACK-FIFO/successor
remain mandatory. Goal stays active; no Bound/Root/physical-zero/fullP3 claim.

## 2026-09-14 — DATA committed-frame runtime RED and precise binding stop

Checkpoint: /tmp/mini-vpn-data-journal.38T44g/checkpoint.json.
Only existing Owner DATA test leaf446->508 changes from the immediately prior
accepted checkpoint;392 other Rust hashes match. No production/TCP/inventory
write. Added one exact real-input tracer with ReplayBound/ReplayPublished rows.
At the first row the existing single-use hook panics in actual DATA bind;
compilation succeeds, then retained_receipt_effects is0, expected1. This is
runtime RED0/1, not fixture/import/zero-test failure. Observation and Published
checks have not yet run. Original five tests remain unchanged in source.
Scoped leaf fmt and diff pass; post-RED broad/growth/production GREEN not run.

Same Heisenberg SOURCE01a0a2ef-7df0-73c2-84b7-40a495330286, supplemented by
01a0a2f1-d76f-7aa0-8712-37c32396e787, returns0/1/0 Request Changes for the
complete DATA proposal. Original receipt is non-Clone; UplinkOwnership's
by-value bind consumes/drops it on AlreadyBound, checked-range and mismatch
errors. Keeping Staging/permits is not keeping that committed current item.
Vec.remove plus old bind cannot satisfy the contract. Absence of a callback
does not close known Err; copied validation/reconstructed receipt is forbidden.

Only proposed amendment: one restricted Owner-only bind_owner_replay(&ReplayStored)
on original UplinkOwnership. Borrow original journal receipt throughout;
reuse complete validation/commit with old public by-value signature/error
ordering/Client/legacy consumption unchanged. No field/getter/clone/callback,
second backing or new production path; normal growth limits remain.
This requires explicit user authority under the existing frozen TCP row;
none was inferred, and no production GREEN was attempted.

After authority, one same-owner transaction must hold original Vec before
processing, reject duplicate receipts without overwrite, bind into original
Staging slot before filtering receipt, retain first-outstanding before both
hooks, and complete receipt/derived ACK/catch-up/SinkAccepted frontier before
the single success take. No second visible backing or hook-only acceptance.
Existing Executor/full timer/expiry/Completion/C-output duties remain.
Prior two characterization approvals remain valid; full P3 stays active,
with no completion/external/commit/push claim.

## 2026-09-14 — Authorized original Supervisor DATA journal accepted

The user explicitly authorized the restricted borrowed-bind supplement.
Same Heisenberg SOURCE01a0a329-6b89-71b2-bbfb-6f72b6f368fb approves0/0/0,
local9/10, limited to this Supervisor DATA/caller-fact component. Reviewer
inspected SOURCE only; Main owns all runtime and gate evidence. Checkpoint:
`/tmp/mini-vpn-owner-data-journal.Rb7JPH/accepted.json` includes original/current
sources,393 hashes,49 Cargo records, standalone checks and review. Exactly
five existing Rust files differ from the pending-RED checkpoint;388 match.
TCP2828/Supervisor5439 remain exact; driver_input422/owner_observation217/
OwnerDATA669 comply. No new/deleted path or inventory/Knife/scanner edit.

The original public by-value TCP bind delegates to one restricted Owner borrowed
bind. Original validation, error priority and commit body are identical modulo
whitespace; no state/getter/clone/callback/backing is added. Legacy still consumes
the receipt. Owner instead borrows the original non-Clone receipt in the same
effects Vec through all rejecting checks. Duplicate receipts cannot overwrite
one another. After successful binding, actual Replay is published in the original
staging slot before the settled receipt is removed. Original committed Transmit
and first-outstanding fact stay in the same Supervisor journal through both
bind and map-publication interruption points. The old by-value OwnerSlot arm
is currently unreachable after the Owner route's early return; it is not a
reachable bypass or a required second cleanup.

Original receipt filtering, ACK-derived fields and catch-up invalidation complete
before one success take. The actual event caller also installs SinkAccepted
frontier before that take; its presence participates in the existing observation
count and actual generic C Layout. No second K backing is introduced. Existing
O(K) scans/removal, replay-map scans/candidate allocation and aborting allocator
policy remain explicit, not a new constant-cost or throughput claim.

The authorized baseline reproduces the original real DATA runtime RED0/1:
retained Transmit0 vs1 after ReplayBound. Minimal GREEN passes both ReplayBound
and ReplayPublished rows with original Frame/fact, real four permits4/1 and
poisoned stable reentry. Ten serial repeats all pass. A separate real two-flow
TCP characterization rejects the wrong original receipt twice, then AlreadyBound
with old precedence; receipt address/payload and all four balances remain exact,
and both receipts can subsequently bind to their proper staging owners.
This is direct TCP evidence, not a fabricated Supervisor rejection.

The actual MemoryTarget accepts4 bytes from peer DATA; only then the conditional
SinkAccepted test hook interrupts before final take. Original ACK/frontier stay
1/1, reducer receive-owned bytes0, Target calls1/accepted4, and retry leaves whole
snapshots unchanged. Exact plus ten repeats pass. Previous filtered-ACK and real
permit-callback tests pass again; the prior508 test lines remain verbatim.

Executed both feature modes: OwnerDATA8/Target153/Supervisor48/TCP74/Controller57/
Client37/CurrentOnly8/continuation16. Knife162/repository20/Cargo growth7 pass;
fresh rustc -D warnings standalone growth7 passes. Scoped five-file rustfmt,
actual trailing-whitespace/scope checks, git diff --check and both library checks
pass. Only existing warning sets remain. No full-library/global-fmt, external,
TUN/VPS/WAN/M2, commit or push was run.

Next mandatory work remains original Executor page/current/work/observation and
conflict-bundle custody, including recovery; then complete C action/Z/Sleep/expiry/
Target/indexed Completion/C ACK-FIFO/successor and revision2 cost/pressure tests.
Those are not discharged by local Supervisor acceptance. Full P3/Case C/R7/Task4
remain open. No new prerequisite sidecar or wording-only review is required.

## 2026-09-14 — Original Executor real Target callback custody RED

Same Heisenberg SOURCE01a0a332-a856-73c2-a0e3-19be7c242a1a approves0/0/0
for the valid expected-RED test only; original Executor custody P1 remains open.
Evidence: /tmp/mini-vpn-executor-journal.L4vAOl/checkpoint.json. Only existing
owner_target/tests/r7_observed_pages.rs206->318 changes from the accepted
Supervisor journal checkpoint;392 other hashes match. Original206-line prefix
is byte-exact. No production/new-path/inventory change or GREEN attempt.

The new exact test uses real OPEN and original MemoryTarget, registers a real
Read Waker after preparation and proves Pending/wake0. Original Executor receives
an authentic peer RESET; Supervisor commits the terminal and finishes its receipt
handoff. MemoryTarget actually terminalizes and commits reset_flows1 before
publishing readiness. The registered one-shot callback panics; exact payload,
wake1/reset1/actual terminal establish that real interruption boundary, not a
receipt stub, fabricated successful adapter or Drop callback. Model flow0/
tombstone1 and Supervisor journal0/healthy precede the first failing assertion:
Executor aborted_effect_guards0, expected1. Runtime1-test RED is reproducible
in the recorded command; subsequent effects2/original fact/token/rejected-retry
checks have not run and are not accepted evidence.

The first attempt stopped at test E0277 because unwrap_err required Debug on
the success type; an explicit match corrected only the test and the next exact
run reached the expected runtime RED. No production derive or Interface change.
Scoped fmt/diff and source/whitespace checks pass; no post-RED broad/growth or
production acceptance claim. Reviewer ran no tests or writes.

SOURCE identifies stack-local current/suffix/outputs/observations, with ordinary
Err retention only after Target returns. The next already-granted implementation
must keep the whole original Executor journal and conflicting incoming bundle
through actual callbacks, including Source-taking branches. No local poison,
catch-unwind wrapper or synthetic inventory can substitute. This is Unbound
component evidence, not Bound Root; full revision2/P3 remains active.

## 2026-09-15 — Complete Executor caller preparation Move

Same Heisenberg scope clarification01a0a338-b32f-7823-a61f-b8b8adc2f531 confirms
the existing revision2 grant covers complete original caller integration; the
earlier raw-body placement restriction applied to that preparatory Move alone.
No new authority or architecture sidecar was required. Same Move SOURCE
01a0a33c-7603-7c32-9dca-8a4bfc893568, restated
01a0a33e-bba3-7643-8549-c50e1fe2ebbc, approves0/0/0 Move-only.

Current evidence/original sources/393 hashes/exact commands:
`/tmp/mini-vpn-executor-callers-move.lUd8El/accepted.json`.
Four complete methods moved from original parent to existing observed_turn.rs:
apply_event_observed78, execute_attached_recovery_observed84 including docs,
retry_target_write_observed27, attempt_target_write131. The first three keep
pub(crate); attempt_target_write uses pub(super) for original parent-effective
scope. No caller, parameter, storage, policy, getter or re-export changed.
Parent8841->8518, child754->1077; three blank separators were removed by rustfmt.
Restoring those separators and the one visibility spelling, then reinserting
the four original blocks, exactly restores the parent and child baselines.

Main acting only as Integrator changed the one measured governance entry
8_841->8_518; reverse replacement restores the full original governance file
(751 lines). No new/deleted source paths. Exactly these three Rust files differ
from the preceding RED checkpoint;390 hashes match. The original test318 is
unchanged. No behavior GREEN has started.

The old Target153 tests pass before/after, and in both modes, explicitly using
--skip only for the fully named newly added real RESET/readiness-callback RED.
That exact test is separately rerun and still reaches the same actual Target
reset1 followed by Executor guard0-vs1 failure. This is a characterization
boundary, not a passing full Target suite or an accepted regression exclusion.
Default/all S48/C57/Client37/CurrentOnly8/continuation16, Knife162/repository20/
Cargo growth7 and both library checks pass. Fresh strict standalone passes7
before Move, reports only expected shrink6/7, then passes7 after exact ratchet.
Scoped fmt/diff, whitespace, unique source and full inverse checks pass; existing
warnings only. Reviewer inspected SOURCE and did not run Cargo or write.

Next is the whole original Executor journal implementation: keep actual page/
current/suffix/outputs/observations and conflict reserve before callbacks; cover
expiry preamble, recovery guard, pending Offer/segments, nested completion and
temporary OPEN Source ownership. No poison-only guard, copied receipt/counter,
second K backing, unsafe aliasing or changed Target/legacy policy is selected.
Original K+3/E and actual header/capacity costs remain mandatory. At final
behavior acceptance the current RED must pass without exclusion.
Full P3/Case C/R7/Task4 remains active/incomplete; no external/commit/push.


## 2026-09-15 — Original Executor entered journal accepted

Same independent Heisenberg SOURCE01a0a36a-82c8-78d3-811e-c1268afdc0ee:
P0/P1/P2=0/0/0, Approve, local architecture9/10. SOURCE-only review did not run
Cargo. Main subsequently recorded the completed frozen regression matrix.
Exact original sources, failed attempts, repairs, commands and393 Rust hashes:
`/tmp/mini-vpn-executor-full-journal.jttB1H/accepted.json`.

Production now places the original popped work directly in the same journal's
inline current before any Target callback. Its original queue holds the suffix;
append uses the old physical pending length. The page owns actual output prefix,
observations, one incoming bundle and recovery until the final owning handoff.
Expiry preamble, attached-recovery/resume, pending-write and original selected
receipt/completion calls enter before their callbacks. Existing Source/Flow and
Target policy remain at their original authorities. Known conflicting incoming
facts return through the original owning error; unknown retains entered state.

OPEN stores actual TargetOpenCompletion before explicit Source Releasing/drop.
Normal destruction alone clears Releasing; unknown keeps original OPEN/facts/
outcome and refuses reentry, without claiming the Source survives its own Drop.
Accepted Source moves directly into the existing Flow. Independent retry keeps
the original Offer/segment vector inline, not in a newly allocated work queue.
There is no second K backing, cloned fact, unsafe alias, new callback or new path.

Review-driven corrections: first stale private completion argument caused
E0425/E0061 and was removed; real RESET then passed. The first full Target run
was152/154: old tests expected inner Supervisor rejection or open admission
after unknown Source revocation. They now require exact outer quarantine while
preserving original event/terminal/payload identity. Same Reviewer explicitly
approved those migrations. Initial SOURCE0/2/0 required fixing Source shadow
lifetime and single-element retry allocation. Subsequent capacity SOURCE0/1/0
rejected queue-front retention: K+3 accounting did not prove actual backing/
growth costs. Universal inline current closes that issue. One additional old
layout test required three exact lines: count current+suffix and inspect actual
inline Offer, preserving all original error/segments/no-duplicate assertions.
The new retry fixture initially watched Read; actual directive consumption
publishes Write. Its test-only correction uses the real Write registration and
AcceptAtMost directive. No production wake policy was changed.

Final measured sources versus the accepted four-caller Move:
owner_target.rs8518->8515; observed_turn1077->994; completion1161->1166;
service855->866; r7_observed_pages318->505; OwnerDATA669->684;
r7_nested_abort759 unchanged; bound_failure/sink523->531; governance751.
Exactly9 Rust paths differ/384 of393 match, no new/deleted paths.
Integrator alone lowered the original parent inventory8518->8515. Fresh strict
standalone reports only expected shrink before ratchet, then7/7 after it.
Unrelated rustfmt changes to the service prefix were restored byte-exact;
its touched suffix equals rustfmt output. Other changed files pass scoped fmt.

Final actual evidence, with no Target skips: real RESET RED->GREEN, actual
rejected Source Drop and retry Write-wake characterization1/1 each; each10/10
serial repeats on the final universal-current source. Full Target156 in default
and all-features; S48/C57/Client37/CurrentOnly8/continuation16 in both modes.
Knife162/repository20, fresh standalone/Cargo growth7, scoped fmt/whitespace/
diff and both library checks pass. Only prior smoltcp22, physical CurrentOnly
private_interfaces1 and prior provenance warnings remain. Detailed failed and
superseded front-held runs remain evidence, not claims about final code.

This accepts the original Executor journal component, not complete Bound Root,
nested physical accounting, full expiry or P3/Case C/R7/Task4. Next is the
already-granted real C action/Z/Sleep/expiry/Target/indexed Completion/C ACK-FIFO/
successor path with original pressure/cancellation/failure duties. No further
Executor Move or repeated grant request; no full-library/global-fmt, external,
TUN/VPS/WAN/M2, commit or push was run.


## 2026-09-15 — Complete consumer DRAFT: terminal-read scope stop

NOT accepted. Existing full P3 goal and original Executor acceptance remain.
Exact original sources,394 hashes and all commands/failures:
`/tmp/mini-vpn-terminal-consumer.gnAZqG/checkpoint.json`.
Same Heisenberg causal SOURCE01a0a38a-bebb-71c1-b909-26bee81caa4c:
P0/P1/P2=0/1/0 Request Changes; diagnosis, not implementation approval.

Pre-write strict standalone governance7/7. The new physical leaf uses real
loopback handshake, MemoryTarget, checked Z=1 C, original seed/CurrentOnly and
ReadyIndex selection. A private ingress Adapter transfers real peer delivery
and arrival into the original C ordinary guard; it does not prove the later
production process ingress route. No test calls expiry or wakes Completion
during the eventual expiry interval. That interval has not executed yet.

Two test-only compile errors (arrival path and unwrap's unwanted Debug bound)
were repaired. Exact then hit the predicted runtime absence: C holds actual A
terminal fact1, but its uninstalled action prevents the next legal B CLOSE.
Draft Port exact install and C-held Installing/Installed/AVL pairing compile.
Scalar hint uses original transact(kind;retain) with explicit outcome handling.
Same SOURCE entry clarification0/0/0 forbids repeated audit-mutating eligibility
queries; corrected draft probes only inside selected owning installation.
The output root caches unsupported-action ineligibility. No new getter,
re-export, callback API, second Z registry or Target policy is implemented.

The next real RED is ReadReady for already-terminal A flow1, returning original
BoundSourceRequiresSelectedService after TargetCommon. Temporary category/error
probes established this and were removed. The exact error remains root-held;
C snapshot failed=false/progress1/observations1. Final formatted-draft exact is
0 passed/1 failed/1774 filtered; no later gate is represented as green.

Reviewer confirms original finish_target_flow holds TerminalSettling then
revokes/notifies/resets/joins without closing read candidates. Correct repair
is terminal-owned read suspension after custody and before revoke/wake.
Existing helper is Active-only; legitimate BoundInactive needs same-lock
identity/state checks without publish/Notify, while Active retains strict
Running/publication checks. Clear only Credit/ReadReady/ReadAttempt; preserve
Source/Driver/transit/Completion/Join/Retirement. Do not relax the receipt
validator or replace all terminal/identity errors with Pending/Hint.

Revision2 restricts service/completion changes to journal integration, so this
new behavior axis requires explicit narrow authority: completion/retirement.rs
finish_target_flow, owner_ready/flow.rs suspend_flow_reads, only if needed
owner_target/service.rs suspend_bound_reads. None changed in this draft.
Next evidence: current real Root RED, BoundInactive, late-read wake cannot reopen,
and independent other Flow still progresses. No new sidecar is needed.

Five old Rust paths changed, one selected new leaf,388 original paths unchanged,
none deleted: C terminal_grace800, lifecycle718, continuation1284, physical
service416, current_only791, new terminal_grace685. No shrink ratchet/inventory
edit. Five-file scoped fmt and targeted diff pass; physical service formats
only new install lines, preserving unrelated old formatting. No trailing
whitespace. Post-growth, related suites, provenance, library/global-fmt and
closure review not run. Sleep/expiry/Z release/Completion ACK/successor, costs,
pressure/cancel and full physical inventories remain owed. No external,
TUN/VPS/WAN/M2, commit or push. Writer stops for the new axis, not withdrawal of
the already-granted consumer or the active unbudgeted full-P3 goal.


## 2026-09-15 — Authorized terminal-read prerequisite locally closed

Local prerequisite only; complete consumer remains DRAFT/runtime RED.
Same Heisenberg SOURCE01a0a3ab-62be-74e0-a6ee-3eb7fa065a56:
P0/P1/P2=0/0/0 Approve. Reviewer ran no tests; commands below are Main's.
Exact original sources,394 hashes,47 command records and review:
`/tmp/mini-vpn-terminal-consumer.gnAZqG/terminal-read-repair.json`.

User explicitly authorized three original function axes; only two were needed.
finish_target_flow retains actual TerminalSettling, suspends sourceful read
candidates, reborrows the original owner, then performs revoke/drain notification
and remaining Target work. Proven NoSource/Unbound branches stay unchanged.
suspend_flow_reads owns one lock for capacity/key/correlation/nonzero generation/
Live-or-Retiring comparison. Inactive requires no publication, no local/source/
process runnable state and absence in the fixed four Root trees; success does
not publish. Active preserves exact publication+Running requirements. Only
Credit/ReadReady/ReadAttempt clear. Existing service helper/global receipt
validator and all Source/Driver/Completion/Join/Retirement ownership stay.

Actual full-path test advanced beyond A stale ReadReady into fixture B EOF
double-read. B EOF now remains unconsumed until the real bound selected service
consumes it and emits CLOSE; actual peer ACK follows. Next60s virtual advance
expired real QUIC;100ms positive grace isolates the intended expiry gate.
New private focused characterization proves actual RESET-wake candidate
clearing, independent B's exactly-one matching nonfinal ACK under Z=1, retained
A fact1 and both actor inventories0/0, explicitly not whole-root zero.
One mechanical test helper E0596 was repaired by mut self. No production
callback/getter/Debug/importer workaround was introduced.

Reviewer P2 corrected the coverage claim: actual PeerReset invokes Target.reset
and its registered Waker BEFORE later FlowFinished suspension. Delayed Root
Notify is not delayed request_flow. Test name/comment now say RESET-wake
clearing, with executable assertions unchanged. Post-suspension late wake has
only existing SOURCE-gate proof; its runtime duty remains open.

Focused1/1 and10 serial repeats; renamed/formatted final focused1/1 both modes.
Both modes: Target156/156 (no exclusions), Ready25, C57, Supervisor48, Client37,
continuation16. CurrentOnly9 = existing8+new focused1; the known DRAFT expiry
tracer was explicitly excluded only from this scoped regression group.
Incorrect client_session_controller filters matched0 and are not evidence;
correct client_controller37/37 ran in both modes. Knife162/repository20,
fresh strict standalone growth7/Cargo all-feature growth7, seven-file scoped
rustfmt, tracked diff/all changed-source whitespace and both library checks
pass. Warning counts are preserved in the artifact; no warnings-clean claim.
Final complete-consumer exact still runs1/fails1 with expected missing-expiry
Elapsed after all A RESET/B CLOSE/full-Z assertions pass, now test line645.

Relative to preceding draft only retirement861, ready flow1130 and selected
leaf713 changed;391/394 Rust paths unchanged. Relative to accepted Executor,
seven old paths plus this one new leaf differ,386/393 old paths unchanged.
No new/deleted path in this repair, no shrink/allowance/inventory edit.
C800/lifecycle718/continuation1284/physical service416/current_only791 retain
their prior draft shapes. Physical service's unrelated formatting debt remains;
no full-library/global-fmt acceptance is claimed.

Next already-granted gate: actual unique Sleep registration, owning original C
expiry, Z release/indexed Completion/C ACK-FIFO/successor, with failure/pressure/
cancellation/cost/nested physical inventory and post-suspension runtime duties.
Do not repeat resolved scope requests or reclassify this component as full P3,
Case C, R7 or Task4 completion. No external/TUN/VPS/WAN/M2/commit/push.

## 2026-09-15 — First actual terminal expiry/ACK chain GREEN; matrix still open

Real implementation, not architecture-only progress. The consumer remains DRAFT.
Same Heisenberg SOURCE01a0a3de-077d-7213-9c25-cdedefff5fe3:0/0/0;
SOURCE passes, but missing runtime matrix is explicitly NOT approved.
Reviewer independently matched394 hashes and inverses for11 changed files;
383 others unchanged. Main ran tests; Reviewer ran none.
Exact hunks, hashes,73 command records (including failed attempts), format
audit and remaining obligations:
`/tmp/mini-vpn-terminal-consumer.gnAZqG/terminal-expiry-draft.json`.

One cold physical Pin<Box<Sleep>> is prepared before output-root publication.
Its private Dormant/Armed/DueHeld state owns registration/correlation only;
C owns the original fact, immutable window and exact installed action.
Selected Output fairly services frame/flush, installation or timer. Actual
exclusions suppress due requests, and successful owner transitions recompute
the request from original state. Completed Sleep is not polled again.
C marks Entered before original TerminalGraceExpired reduction, preserving
Target retirement, observations and indexed Completion handoff. Exact carrying
rejection restores Waiting; unknown does not. Original returned output enters
its ledger before C removes the old AVL entry and returns its Z position.
The actual peer then observes B's unique final ACK and original successor expiry.
Timer storage and pending registration stay explicit in physical inventories;
actor cleanup remains explicitly NOT whole-root zero.

The first implementation still timed out after C had released A: original
HalfClose capacity rejection kept its completion but lacked its derived waiter.
The original helper now owns complete Inactive/Active validation, read suspension
and waiter linking; global receipt validation stays strict. Same-resource wait
is source-idempotent; conflicting membership remains fail-closed, not rollback.
SOURCE P1 on Active-only validation was repaired in that existing seam.
Inactive/duplicate-wait runtime proof remains required.

Two validation repairs preserved old assertions. Tokio1.52 advance does not
await timer-driver delivery (local clock.rs207–215); successor observation now
waits actual selected settlement within the original timeout, with no supplied
timer wake or expiry. Final ACK/no-repeat/zero assertions remain. Newly eligible
empty Completion initially counted as SourceRead, making unchanged cold
Retiring observe2 instead of1. Returning original output admission classification
fixes work reporting; the original test again proves one actual release.

Cold process/session test produced exact missing-method RED then GREEN for
factor3 bounds using the actual Root Session-class quantum bound and typed F/Z.
Overflow rejects the original controller-preparation input before ready reserve.
Checked cold Layout includes actual registration and Sleep payload. Allocator
abort and missing time-driver panic are not typed exact-return guarantees.
The complete expiry still costs2*M+S+X plus Target/progress/ledger and original
preflight/E/replay/encoded-byte charges; only the new index is O(log Z).
Full Layout/call-count/pressure boundaries are still unproven.

Final SOURCE-frozen chain:10/10 default AND10/10 all-features.
Both modes: Target156, Ready25, C57, Supervisor48, Client37, continuation16,
CurrentOnly11 including the full expiry tracer, with no test exclusions.
Knife162/repository20/Cargo growth7; final strict standalone growth7;
both library checks and tracked diff pass. Scoped rustfmt has no new debt;
14 untouched old owner_target/service.rs hunks remain, so no global-fmt claim.
Warnings and exact command statuses are retained, not described as warning-free.
No full-library run or external acceptance.

Exact lines: output parent1009/lifecycle811; C terminal1084; continuation1336;
physical root789/service459/test leaf743; Target completion1172/service891;
server root544/construction262. All normal limits hold. C test parent794,
current_only parent791 and governance751 are unchanged. No new/deleted path,
ratchet/allowance edit, commit/push or external run.

Executable remaining work for this consumer (all mandatory before acceptance):

- Test placement: proposed private C tests/terminal_grace/expiry.rs plus the
  existing parent's private declaration. Parent794/800 cannot absorb the
  boundary matrix. Same Reviewer confirms default route/private ancestry;
  USER AUTHORITY IS PENDING, so the child has NOT been created.
- Original Inactive carrying pressure and duplicate waiter evidence; do not
  infer access to another Authority's private fields from the new test route.
- Actual output-excluded deadline plus real FIFO pressure relief must resume
  original install/expiry and park, without manual wake.
- Observer cancellation and unknown/postcommit ownership, multi-Z AVL
  rotations/removal/reuse, actual Layout/cost/capacity boundaries, late wake.
- Rerun the full related/provenance/growth matrix after those changes and ask
  the same Reviewer for consumer closure. SOURCE-only approval is insufficient.

Existing production scope remains granted. No new architecture prerequisite,
Executor Move or terminal-read grant is required. Later full-P3 obligations,
production ingress, Case C/R7/Task4 and whole-root shutdown remain separate/open.

## 2026-09-15 — Shutdown-observer retention characterized, consumer still DRAFT

Only the existing physical current_only/terminal_grace.rs test leaf changed:
743->796 lines, original743 prefix byte-exact; other393 of394 Rust paths
unchanged, no production edit/new path/governance change. Appended exactly one
test: shutdown_observer_cancellation_retains_pending_terminal_and_timer.
Same Heisenberg SOURCE01a0a3ed-69c5-7263-ad67-63fa29259138:0/0/0 Approve
for this test only. Reviewer independently checked prefix,394 hashes and lines;
Reviewer ran no tests. Main's exact32 Cargo records, pre/post strict standalone,
hashes, added source, scoped checks and review are retained in
`/tmp/mini-vpn-terminal-consumer.gnAZqG/terminal-cancel-characterization.json`.
Its prior reference preserves the original consumer implementation evidence.

Real RESET/drain leaves C fact1 and an actual pending timer. Both actors are
cancelled before yielding; the borrowed root join is polled until the original
join block confirms entry, then only that observer is dropped. Same root is
Joining; actual C/timer readbacks remain1/(1,1), separately from admitted
inventory. Advancing100ms without selected service/expiry/manual timer wake
does not turn custody into zero. Existing actor-only cleanup re-enters the
same actual join, proves actor messages/bytes0/0 and explicitly not whole-root
zero. This does not establish DueHeld, expiry under pressure, normal admission
restoration, unknown outcome, fixture Drop settlement or complete shutdown.

Direct characterization GREEN1/1, without a manufactured RED; then10/10
default and10/10 all-features. Both modes CurrentOnly12 (including the original
full expiry tracer, no exclusions), C57 and continuation16 pass. Knife162,
repository20, Cargo all-features growth7, strict standalone7 and both library
checks pass. Exact leaf rustfmt/stdin equality and whitespace/diff pass.
Existing warning counts retained in command records; no warning-clean or
full-library/global-fmt claim. The14 old service.rs fmt hunks remain untouched.

The consumer matrix remains mandatory: Inactive pressure/duplicate wait,
real output-excluded due and pressure relief, other cancellation/unknown
ownership, multi-Z AVL, real Layout/call-count costs and post-suspension wake.
C test parent794/800, physical leaf796/800. Proposed private C expiry.rs and
its private parent route still await USER authority; not created. No new
production scope, external/TUN/VPS/WAN/M2, commit or push. P3/Case C/R7/Task4
remain incomplete. No new runtime failure was encountered in this slice.

## 2026-09-15 — Real C multi-Z expiry/index lifecycle; P3 scope clarified

The user continued after the exact private C child/parent permission request.
Only parent tests/terminal_grace.rs794->797 and new private
tests/terminal_grace/expiry.rs284 changed; production/governance unchanged.
Parent inverse is exact;393 of394 old paths unchanged, one new path,395 total.
First exact compile E0583: the already path-routed parent resolves bare mod
expiry at tests/expiry.rs. The same-scope explicit private relative #[path]
closes lookup; no new visibility or changed child destination. Removed only
the child's extra final blank line before final repeats. The real behavior
directly GREENed; this compile feedback is not a predicted behavioral RED.

One actual C component test creates four real prebind OPENs, then three RESETs
in each of four insertion orders. Original C pages enter OwnerOutputDrain;
drive_one(None, lifecycle port) transfers only the original lifecycle actions,
and C's original install_exact_terminal path checks full TerminalGrace identity.
The single actual cold Sleep drives each original expiry. C retains its fact
through return; output admits the original returned page before C returns its
own Z slot; the original continuation is then drained. This does not imply
the model tombstone is held until output admission. Read-only AVL heights/
ordering, birth FIFO and free-list partition checks follow every transition.
A fourth actual RESET reuses the just-returned slot; the backing address and
exact eight-slot allocation remain fixed. No direct test insert/remove/state
writes. Final timer(1,0), C/Ready/Executor remain, explicitly not whole-root zero.

Same Heisenberg SOURCE01a0a407-3c09-7243-8892-fdf8a9173224:0/0/0 Approve
ONLY this normal C characterization. Reviewer independently checked scope,
identity/ordering and inverse; Reviewer ran no tests. Main exact1/1 plus10/10
default and10/10 all-features; both modes C58/Target156/CurrentOnly12/
continuation16. Knife162/repository20/Cargo growth7 and strict standalone7;
both library checks, exact two-file rustfmt/whitespace/scoped diff green.
Warnings remain recorded; no full-library/global-fmt claim. The14 old Target
service.rs format hunks are untouched. Exact35 Cargo records including E0583,
395 hashes, original parent, child, strict checks and SOURCE verdict:
`/tmp/mini-vpn-terminal-consumer.gnAZqG/c-expiry-index.json`.

The same joint51 executable checklist is now factually synchronized: missing
expiry/runtime-RED statements are superseded by actual success evidence, not a
fresh prerequisite sidecar. Five current groups remain: Inactive/duplicate
wait, actual FIFO-excluded due/pressure relief, other cancel/unknown retention,
real Layout/cost/capacity, and post-suspension late wake. Normal multi-Z passes;
capacity-exhaustion and pressure obligations are not inferred from it.
Full P3 additionally retains five section51B packages: new OPEN/static Target
progress, concrete TCP/DNS, all scheduler/capacity, Legless/full association,
and final lifecycle matrix/review. The actual TargetIo still exposes synchronous
open; there is no claim that P3 is only missing tests. Detailed later callable/
write freezes remain, while P4 table/bootstrap/P5 fused publication/P6 real
Case C caller are separate later stages. No production expansion, new physical
test path, external/TUN/VPS/WAN/M2, commit or push in this slice.

## 2026-09-15 — BoundInactive HalfClose pressure retains original completion

Only existing private C tests/terminal_grace/expiry.rs284->522 changed;
parent797 and production/governance unchanged.394/395 Rust paths unchanged,
no added/deleted paths. Prior284-line helpers/test remain exact after restoring
two module-doc lines. No new getters, visibility or private-state mutation.

Fresh real F4/Z1 plan and MemoryTarget; prebind OPEN A/B, B actual Target EOF
through original read/continuation and peer final ACK close only TargetToClient.
Checked C remains BoundInactive. A's actual RESET consumes Z1 and its original
output/lifecycle action is installed. B's actual peer CLOSE returns one nonfinal
ACK and its original continuation executes HalfClose exactly once. The original
carrying completion remains1 under pressure; A's full terminal/deadline stay
identical. Actual frames remain test-consumer-held, not physical peer acceptance.

First exact compiled/ran then failed target_flows: actual2/expected1.
Source audit and same Reviewer P2 confirmed this counts original Executor
records, not unjoined Targets. Bound join retains A TerminalSettling/source/
Retirement; original expiry does not execute selected source drain/F release.
Both pre/post test expectations corrected to2, and original Target tombstone1
asserted before expiry. No production fix or weakened pressure/frame/identity.
Next exact1/1 GREEN, including actual timer expiry -> original page admitted to
output -> C slot return -> continuation drain. Model/Target tombstones and C
fact0; Executor Flow records2/completion1 and actual returned frames4 remain.
HalfClose still1; four Root class selections None and root check succeeds.
Not proof of waiter membership, duplicate registration, selected retry, Active
publication, wire acceptance or whole-root cleanup.

Same Heisenberg SOURCE01a0a41a-0f2e-7550-90d3-47f9415ec8ed:0/0/0 Approve
ONLY this test; counter P2 closed. Local Architecture/Clean/System/DDIA9/10 each.
Reviewer ran no tests. Main exact1/1 plus10/10 default and10/10 all-features
after format; both-mode C59/Target156/Ready25/CurrentOnly12/continuation16.
Knife162/repository20/Cargo all-feature growth7, strict pre/post standalone7,
both library checks and scoped parent/child format/whitespace green.
Tracked diff exit0; new-child no-index check output empty, exit1 for new-file
difference. Existing22 vendored/1 local test warning unchanged; diagnostics
recorded. No full-library/global-fmt/warning-clean claim; old14 service.rs
format hunks unchanged. Exact37 Cargo records include first runtime failure,
baseline/final395 hashes, original/final child, checks and SOURCE review:
`/tmp/mini-vpn-terminal-consumer.gnAZqG/c-inactive-pressure.json`.

Joint51 separates completed Inactive pressure from still-open actual waiter/
index/duplicate/selected retry. Matrix2-5 and five full-P3 packages remain.
New physical fixture/path is not granted by C child scope. No architecture
sidecar, external/TUN/VPS/WAN/M2, commit or push.

## 2026-09-15 — Reuse actual duplicate-wait coverage before adding tests

No Rust writes: all395 hashes remain identical to c-inactive-pressure.json.
The existing received_reset_retries_once_after_real_terminal_pressure_releases
fixture consumes the actual Executor ready installation, then activates its
original binding/tickets. Three real competitor RESETs reoccupy Z after expiry;
the selected original Completion retries the carried Driver RESET each time.
Current inner rejection restores the receipt and calls the wait helper; the
outer still-waiting path then calls wait_for again for the same resource.
Existing assertions check actual LocalFlow/ReturnedReset/transit1, membership
restored, next real handoff and finally one RESET with transit/completion0.
Thus the single-waiter same-resource duplicate path already has a real test.
This double-call conclusion is SOURCE reachability, not runtime instrumentation.

Same Heisenberg SOURCE01a0a420-6b18-7a51-b1c1-dc2b36445df4:0/0/0.
Main existing exact1/1 plus10 repeats EACH mode; existing physical HalfClose
normal expiry/retry exact1 EACH mode; Target156/Ready25 both modes.27 Cargo
records and unchanged baseline/final395 hashes plus same SOURCE verdict:
`/tmp/mini-vpn-terminal-consumer.gnAZqG/existing-waiter-coverage.json`.
No new full-suite, code-growth or global-fmt run is claimed for this audit.
Do not infer multi-waiter topology, other-resource rejection or HalfClose's
specific repeated-pressure payload/association from this generic RESET case.
The same checklist records its narrow coverage instead of creating another
equivalent test. Actual output-excluded deadline/physical pressure is next;
new physical test path remains outside the existing grant. Full P3 stays open.

Next matrix2 candidate SOURCE review rejected two false pressure premises:
ordinary ACK does not require peer application consumption; duplicate DATA
does not emit a repeated ACK. The corrected finite plan uses four actual
consecutive DATA ranges/ACK frontiers1..4, existing actor DataWriterBlock,
original four message permits, then actual A RESET and B CLOSE offset4.
The fifth output must stay in the original FIFO under ActorPressure; observe
real deadline exclusion, then release the existing block and drive the actual
root/peer to completion. Keep real Target4 bytes and whole-root nonzero.
Same Heisenberg SOURCE01a0a42f-841e-7741-8236-2ca893d13f9c:0/0/0
Approve finite test plan only. No implementation/runtime acceptance claimed.
Exact proposed scope: new private current_only/terminal_grace/pressure.rs and
parent private mod route only, expected796->798; original fixture untouched.
New path authorization requested but not received. No further Rust writes.
Candidate/history/review: physical-pressure-candidate.json in checkpoint dir.

## 2026-09-15 — Actual output pressure across the terminal deadline

User confirmed the exact parent-route/new-pressure-child grant. Only parent
current_only/terminal_grace.rs796->798 and new terminal_grace/pressure.rs232
changed; inverse parent removes the sole private route and exactly matches.
394 other original Rust paths unchanged;396 final paths. No production,
fixture parameters/behavior or governance edit, external, commit or push.

One real test uses unchanged TimedFixture and existing actor writer gate.
Consecutive DATA offsets0..3 create four real ACK records retaining original
message permits and independently counted encoded bytes. Each prior C page
finishes and FIFO empties. Actual A RESET occupies Z1; B CLOSE4 produces a
fifth ACK that remains in the sole FIFO under actual ActorPressure/Registered.
Original same-clock advance101ms exceeds grace. Thirty-two bounded selected
observations retain the exact C snapshot, timer inventory and actor/FIFO
counts/bytes; original plan excludes sources/timer. No manual wake or expiry.
Only writer release resumes actual selected/peer work: exact five nonfinal
ACKs(1,2,3,4,4), unique final ACK4, then successor expiry and park. Actual
MemoryTarget writes4 bytes; actor join0/0 explicitly is not whole-root zero.

Feedback loop: first compile E0596 at three existing mutable plan queries.
Only test receivers changed to &mut; no production Interface/behavior change.
Runtime directly GREEN; an added original-clock assertion and scoped rustfmt
then GREEN. One stale actor test filter ran0 and is not accepted; actual source
names production_root_retries_exact_fifo_after_fixed_actor_pressure and
data_writer_cancel_interrupts_an_actually_pending_ordered_quic_write both pass.

43 Cargo records: exact runs plus10 EACH mode; both-mode CurrentOnly13,
C59/Target156/Ready25/continuation16 and two actual actor exact regressions;
Knife162/repository20/Cargo all growth7; both library checks. Strict standalone
pre/post7, scoped rustfmt and tracked/untracked diff checks pass. Existing22
vendored+1 local test warnings remain;14 prior service.rs fmt hunks unchanged.
No full-library/global-fmt/warning-clean acceptance.

Same Heisenberg SOURCE01a0a456-24a9-7572-94da-75ada5378d4f:0/0/0 Approve
ONLY this actual test, local Module/Clean/System/DDIA9. Reviewer ran no tests.
Final child SHA2564a20b2f18733d66879eca60ad1f652a3851de06510f8bedc379bed422b9bd2aa.
Exact commands, failures, both snapshots and inverse/scope/scoped checks:
`/tmp/mini-vpn-terminal-consumer.gnAZqG/physical-pressure.json`.
Matrix2 is now narrow runtime coverage, not internal DueHeld, HalfClose-specific
repeated waiter, complete budgets or whole-root shutdown. Remaining matrix1
per-origin/3-5 plus five full-P3 packages are retained in joint51. No P3 completion.

## 2026-09-15 — Actual HalfClose repeated pressure and Source association

The subsequent explicit user grant permits only new private preparation/test
in existing owner_target/commands/tests/bound/flow_lifecycle/completion.rs.
Final640 lines; original268-line prefix SHA256 stays
39642418cac278cc51a080709d5699ba78a1f38ec1fc61ea8ee9d26f17ba47cb.
396-path inventory: only that file changed,395 unchanged, no new/deleted path.
No production/governance/old fixture change or external/commit/push.

Actual prebind OPEN/TargetEOF/original CLOSE/peer finalACK precede real consuming
install/activate. B CLOSE performs HalfClose and retains original SinkHalfClosed.
Each of three rounds releases the original captured terminal, then an actual
pre-imported competitor RESET refills Z before the real selected Completion.
Assertions distinguish wait=None before invocation from wait=Some after real
capacity rejection; an empty selector/skip cannot satisfy them. Whole
SinkHalfClose Eq includes completion_id; original Source/Driver LocalFlow,
association, position/generation also match. Local origin never receives Driver
charge(transit0), Target HalfClose counter stays1. Final whole Frame ACK and
original B terminal appear once. Original outputs remain in held_outputs;
TerminalSettling/Source authority is not normalized to F or whole-root zero.
This is component expiry/capacity input, not a physical timer delivery claim.

First exact directly GREEN; no Rust repair. After scoped formatting,10 exact
repeats EACH mode. Both-mode Target157/Ready25/C59/CurrentOnly13/continuation16;
Knife162/repository20/Cargo all growth7; both library checks.36 Cargo records.
Strict standalone pre/post7 and scoped fmt/diff green. Existing22 vendored+
1 local test warning and14 previous service.rs fmt hunks remain; no full-suite,
global-fmt or warning-clean claim. One documentation patch context mismatch
made no write; exact full-line retry repaired only that navigation text.

Same Heisenberg SOURCE01a0a467-ae9d-7063-b918-94178b816e98:0/0/0 Approve
ONLY this test; Module/Clean/System/DDIA9. Reviewer ran no tests.
Final file SHA256131f11897074018cfc6c737b23d2ae7819b22d007cd7f1c3a9a1c66e82843234.
Commands, snapshots, original-prefix/scoped checks and review are in
`/tmp/mini-vpn-terminal-consumer.gnAZqG/half-close-pressure.json`.
Joint51 matrix1 HalfClose row is now locally covered; other topology/capacity,
cancellation/unknown and terminal late-wake duties remain, along with the five
full-P3 packages. This does not approve the complete consumer/P3.

## 2026-09-15 — Executable terminal late-wake boundary selected, no Rust

The user requested continuation. Bounded source audit finds that the existing
staged-failure notification test is not the real terminal-FlowFinished case;
the physical RESET tracer delivers its wake before suspension. Neither is
relabelled as matrix5 proof. All396 Rust hashes stay identical to the accepted
HalfClose snapshot. No test/Cargo/external run or Rust write occurred.

Same Heisenberg SOURCE01a0a478-e895-7d83-b2ff-59cf5cd2f1fd:0/0/0 finds
a complete component route for the actual matrix5 invariant. §51 does not
independently require physical Root injection; later combined consumer/physical
acceptance remains mandatory. Original ReadReady service registers the same
publisher Arc exposed by its existing tickets. Capture this real Waker only
after actual Pending; real PeerRESET/FlowFinished must retain terminal/Source
and suspend reads, then finish publication before old notification delivery.
Legal B Read->Driver->inbox->reduction/output must demonstrate continued DATA
with original identity/resources. No new production getter/Target wrapper,
fabricated wake bit, direct Unbound read or synthetic ownership zero.

Prospective exact scope: commands/tests/bound/flow_lifecycle.rs private route
only721->723; new private flow_lifecycle/terminal_read.rs complete fixture/test
<=800. Preserve old tests/fixtures. User two-path grant is not yet received.
Frozen finite plan/gates: /tmp/mini-vpn-terminal-consumer.gnAZqG/terminal-late-wake-plan.json.
This removes an unnecessary physical-injection assumption; it is not runtime
acceptance, an architecture amendment or closure of matrix3/4/fullP3.

## 2026-09-15 — Matrix5 actual terminal late-wake component accepted

User explicitly authorized the two test paths. Only parent private default-path
mod terminal_read was added (721->722); new private terminal_read.rs431 owns
the complete actual two-Flow fixture/test. Parent inverse byte-exact; all395
other existing Rust hashes match the396-path prior baseline; one new child,
no deleted path, production/governance/old fixture or inventory change.

Both real OPENs precede consuming install and metadata activation. Original
ReadReady polls for A/B return Pending; A tickets clone the publisher actually
registered with Target. PeerRESET reaches FlowFinished/TerminalSettling and
publication/claim finish before that original Waker is invoked. Before feeding
B, legal claim dispatch requires no read candidate or park and a byte-exact
Target snapshot value. B cannot hide an erroneously eligible A. B then really
reads11bytes, passes its original Driver and paired inbox, and reaches reduction
with exact LocalFlow/payload; the full generation/direction/offset/DATA Frame
is unique. Original Source/Driver identities and B association/position/generation
stay exact. A TerminalSettling/Source/Z remain; B transit settles but original
Replay retains11bytes/1segment. No synthetic F, terminal or whole-root zero.

Four Cargo failures were test-feedback only: incorrect protocol names/private
field access; needs_feedback count mistaken for all settlements; unACKed Replay
mistaken for released permits; a strengthened pre-B query under the wrong
claim source. Corrected only the new test after source diagnosis. No production
RED/GREEN is claimed. Final exact1/1 plus10 repeats EACH mode; both-mode
Target158/Ready25/C59/CurrentOnly13/continuation16; Knife162/repository20/
all-feature growth7; both library checks pass.41 total Cargo records preserve
the failures. Strict standalone pre/post7; scoped child rustfmt, tracked diff
and both untracked whitespace checks pass. Warning counts equal the preceding
same commands; known14 service.rs fmt hunks unchanged. No global-fmt/full-library
acceptance. Read-only scope-probe syntax/context errors made no file changes.

Same independent Heisenberg SOURCE01a0a7ec-8cc3-7c51-b560-7077ba7bb844:
P0/P1/P2=0/0/0, Approve only this complete shared-component matrix5 invariant;
Module/Clean/System/DDIA each9. Reviewer ran no tests. Child SHA256
1dd0d5092a3db1bb3a349110ab5acfabbd64b599030e29371d4552c0e2203b22.
Exact commands, scope hashes, failure trail, checks and review:
/tmp/mini-vpn-terminal-consumer.gnAZqG/terminal-late-wake.json.

Joint51 matrix5 is checked locally. Matrix3 cancellation/unknown/postcommit
and matrix4 actual Layout/cost/capacity/topology remain; next is bounded source
gap mapping against existing tests before freezing a different write scope.
Combined consumer/physical acceptance and five full-P3 packages remain. No
P3/CaseC/R7 completion, public cutover, external/TUN/VPS/WAN/M2 or commit/push.

## 2026-09-15 — Matrix3/4 source gap map; next exact scope pending

Bounded source inspection and the same Heisenberg review completed without
Rust writes or tests. All397 current Rust hashes match the preceding accepted
baseline. SOURCE01a0a7f2-d901-7733-99b2-bf259e358cd8 reports P0/P1/P2=0/1/0:
the P1 is missing executable coverage, not a confirmed ownership-loss bug.
Exact source axes and next-gate plan are retained in
/tmp/mini-vpn-terminal-consumer.gnAZqG/matrix3-gap-map.json.

Checked Bound C already has ordinary returned-page/error and receipt-unwind
custody tests. Existing recovery errors use Legacy C or the shared Executor;
they do not prove checked Bound unknown interruption. Recovery's current
LegActivated/Transmit alphabet has no Target callback, so a Target panic cannot
stand in for that boundary. Nested duplicate-completion fixtures likewise do
not establish natural boxed C payload custody. Existing capacity/witness tests
do not establish actual generic Layout, cold peak or complete call charges.

Next finite candidate: actual checked C RESET/resume-ID exhaustion produces
the original Target error and observations, then one existing test-only flag
injects progress failure after preflight in finish_target_error. Expected RED
is the ordinary Target error rather than TargetAndProgress. GREEN must retain
both exact typed errors, the original fact/window/effect tail and stable failed
reentry, without duplicate error ownership or non-test behavior changes.

Four exact paths require a new user grant: controller terminal_grace.rs hook
only; oversized controller root's existing hook-doc comment only/net-zero3268;
test terminal_grace.rs private explicit route797->800; new private
terminal_grace/returned_error.rs <=800. No new flag, storage or public Interface.
The completed two-path matrix5 grant does not authorize these writes. Planned
exact/both-mode repeats, related gates and same Reviewer closure have not run.
Recovery unknown, nested payload, matrix4, combined consumer review and all
five joint51B packages remain open. No Rust functionality was added in this
source-only stage; no external operation, commit or push occurred.

## 2026-09-15 — Matrix3 direct combined-error fault injection accepted

User explicitly authorized the four frozen paths. The new checked Bound C
tracer uses the original parent fixture, real same-clock RESET at5ms and real
effect-resume-ID exhaustion. Its first exact command compiled and failed only
because C returned Target(EffectResumeIdExhausted), not TargetAndProgress.
Four cfg(test) lines in finish_target_error reuse the existing one-shot flag
after terminal preflight and before progress consume. The same exact test
then passed. No unexpected RED, follow-up repair, fake bundle or new flag/API.
Non-test behavior is unchanged; no naturally occurring second-error claim.

The caller retains both typed errors while C retains the original full terminal
fact, producing origin, birth5ms/deadline55ms/native epoch and reserved-but-vacant
slot. Failed phase prevents partial ledger publication. Cold inventory keeps
one observation and one original Executor aborted guard/effect; no duplicate
Target error owner in C. Two late rejected continuation/RESET attempts preserve
full original terminal, window, slot, snapshots, tail and original retry input.
This is not nested-payload enumeration, recovery unknown or whole-root zero.

Exact scope: terminal_grace.rs1084->1088; controller root3268 with only four
doc-comment replacements; test parent797->800 with private explicit route;
new tests/terminal_grace/returned_error.rs140. Three inverse transformations
reproduce exact preceding source. All394 other old Rust paths unchanged;
398 total, no deletions/old fixture/governance inventory change.

Evidence: exact RED->GREEN,10 serial repeats EACH mode; both-mode Target158,
Ready25,C60,CurrentOnly13,continuation16; all-feature Knife162,repository20,
code-growth7; default/all library checks; strict standalone pre/post7 pass.
Scoped rustfmt checks all four paths; tracked diff and three untracked no-index
whitespace checks pass.37 Cargo records retain the expected RED; remaining36
pass. Existing warning counts match prior successful comparable commands.
Known14 old service.rs format hunks remain; no global-fmt/full-library claim.
An initial read-only warning comparator selected a prior failed compilation;
matching successful same-mode categories resolved its false mismatch with no
source edit or new warning. The learning is comparison provenance, not a Rust bug.

Same independent Heisenberg SOURCE01a0a81b-9117-7261-b0d6-edc28a989ead:
P0/P1/P2=0/0/0, Approve ONLY this direct fault-injection component. Reviewer
independently verified398 hashes/three inverses and read the Main gate records;
no tests were rerun by Reviewer. Full command/failure/scope/review evidence:
/tmp/mini-vpn-returned-error.405z5b/evidence.json.

Next executable work is source-first selection of actual checked Bound recovery
interruption and nested-payload seams, retaining the existing matrix3 gap map.
Matrix4 actual Layout/cold peak/call charges/capacity/topology, combined consumer
review and the five joint51B packages remain. No extra Rust write scope follows
from this completed grant. P3/CaseC/R7/Task4 are not complete. No external,
public cutover, commit or push.

## 2026-09-15 — Next nested-observation test selected; authorization pending

Source-only follow-up to the accepted combined-error slice. Current recovery
effects are LegActivated/Transmit (resumable/session.rs3045); observed execution
starts empty and these branches do not call Target. Returned recovery progress
consumes an empty bundle, so a Target RESET Waker panic is not evidence of that
boundary. PostCommitRecoveryRetentionInvariant's helper is called only by the
legacy nonempty-observation publication adapter; checked C uses the observed
execute/explicit publish route. The legacy read-source observation-retention
branch is rejected before entry in Bound mode. No concrete legal checked-Bound
witness for natural nested-error production was found; this is not a universal
unreachability theorem and does not waive future applicable acceptance.

Same Heisenberg SOURCE01a0a823-6e24-7b72-a5db-b6ad3f38bc60 with supplements
01a0a824-734a-7750-b368-efb5c23b8a7d and01a0a827-015b-7593-b172-50363f75b452:
P0/P1/P2=0/0/0 for a finite defensive-test plan, not runtime acceptance.
Selected next gate needs only the existing returned_error.rs new test axis.
No production decoration hook, new route/storage/getter/Interface is needed.
Original combined-error test body must remain byte-exact; compact leaf docs
may describe the added test. Expected resulting leaf about300-400, hard cap800.

For each known/unknown case, use a fresh real checked C fixture. Prove original
active admission/empty slots/no existing turn before beginning the real terminal
window. Actual captured arrival and sampled producing time own the origin.
Real RESET/resume exhaustion produces the original error and observation; take
the original observation once, then explicitly wrap both in the existing nested
variant. This wrapper is injected, not a claimed second production conflict.
Reuse finish_target_error and the existing pre-progress panic flag only.
Known branch returns the original inner boxes/fact to caller, clears C's error
page, but retains Failed phase/window because the Executor tail remains.
Unknown branch retains the original nested error in C. Compare Box pointee
addresses/full fact, window/reserved slot/tail and stable rejected reentry;
ordinary C observations remain empty. Error-owner count is not recursive
fact/byte inventory, and neither branch may claim whole-root zero.

Expected test is direct GREEN characterization. Exact/both-mode10 repeats,
related/provenance/growth/check/scoped checks and same Reviewer closure remain
future work. Exact plan and source limits:
/tmp/mini-vpn-returned-error.405z5b/nested-observation-plan.json.
No Rust or tests changed/run in this audit; all398 Rust hashes stay accepted.
The completed four-path grant is not extended to this new test axis without
confirmation. Recovery callback interruption, boxed recovery cursor, complete
nested inventory, matrix4 and full P3 remain open. No external/commit/push.

## 2026-09-16 — Single-leaf nested-observation custody accepted

User authorization: add the selected test only in existing
`src/owned_upstream/owner_session_controller/tests/terminal_grace/returned_error.rs`.
Main was sole Writer. Same Heisenberg SOURCE
01a0a98f-df19-76a3-b110-1237761c6eb3: P0/P1/P2=0/0/0, Approve ONLY this defensive
characterization. Reviewer read source and execution evidence, independently
verified all398 hashes/old test identity, and ran no tests or writes.

Fresh baseline/full37 Cargo records/scope audit/review:
`/tmp/mini-vpn-nested-observation.9o5AQA/{baseline,evidence}.json`.
The preceding /tmp plan/result files were absent; the accepted plan was recovered
from the durable2026-09-15 entry above. No old execution was reconstructed.
This section retains the new exact test identity, counts and limitations even
if the fresh temporary files later disappear.

Only the existing test leaf changed140->326;397 other Rust sources unchanged,
398 total, no added/deleted Rust path. No production, parent route, storage,
visibility/API, old fixture, governance inventory or behavioral repair.
The original combined-error test block remains byte-exact:
SHA256 `02883865bb634450995fe029ace58d6b3dafea077eb3602eaaab8a06f2e28cbe`.
Final whole leaf SHA256:
`4c0ee35eeb816550755d126a41267abf45e7b9004edb8c722d109514172604a5`.

New exact test:
`owned_upstream::owner_session_controller::terminal_grace::tests::returned_error::bound_nested_observation_error_keeps_exact_payload_on_return_and_unwind`.
Each branch creates a fresh actual checked C/MemoryTarget, verifies Active,
empty admission/turn slots and original same-clock arrival, then begins the
real5ms window. Actual RESET/resume exhaustion yields EffectResumeIdExhausted
and the original terminal bundle. The test takes that bundle once and moves
both originals into PostCommitObservationRetentionInvariant; only this wrapper
is injected, not a claimed naturally occurring second observation conflict.

Known return leaves the entire nested error with caller and clears C's page.
Unknown unwind uses the existing pre-progress hook and leaves it in C's page.
Both compare the two Box pointee addresses, full TerminalGrace and generation.
Ordinary C observations remain empty, while Failed, original5ms/55ms/native
epoch window, reserved vacant slot and Executor guard1/tail1 remain held.
Cold retained_target_errors is0/1 by owner, not recursive fact/byte inventory.
Two25ms late retries return untouched actor input/arrival, reject continuation,
and retain the same snapshots, window and nested payload. No partial commit,
duplicate error owner, reconstructed terminal or synthetic clean result.

All37 new Cargo records succeeded; direct GREEN, no deliberate RED or repair:

| Actual command group | Default | All features |
| --- | --- | --- |
| New exact (`--exact --nocapture --test-threads=1`) |1/1 plus10/10 repeats|1/1 plus10/10 repeats|
| `--lib owned_upstream::owner_target::` |158/158|158/158|
| `--lib owned_upstream::owner_ready::` |25/25|25/25|
| `--lib owned_upstream::owner_session_controller::` (both leaf tests included) |61/61|61/61|
| `--lib owned_upstream::quinn_leg_composition::owner::physical_session_root::tests::current_only::` |13/13|13/13|
| `--lib owned_upstream::quinn_leg_composition::owner::continuation::` |16/16|16/16|
| `--test knife16_owned_leg_provenance` |not run|162/162|
| `--test repository_test_module_provenance` |not run|20/20|
| `--test code_growth_governance` |standalone below|7/7|
| `cargo check --lib` |PASS|PASS|

Cargo used `--message-format=json`; all test groups were serial/nocapture.
Strict pre/post compilation used `rustc -D warnings --edition=2024 --test
tests/code_growth_governance.rs -o <fresh-dir>/code-growth-{pre,post}`; each
binary `--test-threads=1` passed7/7. Scoped leaf rustfmt check used edition2024,
`skip_children=true`; tracked diff check and95 untracked whitespace checks pass.
Warning counts match the preceding durable successful categories: vendored
smoltcp22; test-lib local1; Knife local149; repository/growth/check local7.
No full-library test/global-fmt/warning-clean claim; old14 service.rs formatting
hunks were not edited. No commit/push, external/TUN/VPS/WAN/M2 or cutover.

Scope limit: this closes only the defensive known/unknown nested-observation
custody component. Natural nested-conflict production, applicable recovery
interruption/boxed cursor/full recursive inventory, matrix4 and combined consumer
review remain open, as do all five joint51B packages. Next is bounded source
reconciliation and selection of one actual falsifier/exact write axis, not an
implicit new Rust grant. P3/Case C/R7/Task4 and whole-root zero remain incomplete.

## 2026-09-16 — Matrix3/4 source reconciliation and next real cold test

Previous goal turn was progress: the one-leaf nested-observation runtime result
above is accepted. This follow-up was source-only: Rust/tests/Cargo/external
actions zero; all398 Rust hashes still match that reviewed evidence, no deletes.
Only compact status/checklist/result/learning documents were updated.
No new architecture sidecar or wording-only approval round was introduced.
Working record: `/tmp/mini-vpn-nested-observation.9o5AQA/next-gate-source.json`.

Same Heisenberg SOURCE01a0a996-27b5-7452-9908-b58799cc1bdf0/0/0:
current recovery emits LegActivated/Transmit only, and
owner_target/observed_turn.rs898 starts its observed page with an empty bundle.
C root2765 consumes it synchronously; no actual Target callback witness was
found for unknown interruption there. This is not a universal unreachability
theorem, nor proof of production loss because a cursor is temporarily local.
PostCommitRecoveryRetentionInvariant's generating helper is reached through
legacy nonempty-observation publication at owner_target.rs3646; it cannot
establish checked-Bound natural nested failure. Real queue pressure is different:
drain_attached_recovery_outputs at3547 restores the original failed Frame to
the cursor; C2849 restores that drain to Publish. A future checked-bootstrap,
nonempty-recovery, repeated-pressure/release tracer is applicable without a new
panic hook. Existing attached_recovery fixtures are Legacy, not that proof.
Recursive cursor payload/bytes and applicable unknown obligations remain open.

Supplement SOURCE01a0a998-7dd5-7573-a6b5-3e3f3dff9ced0/1/0 identifies a
**P1 acceptance-proof gap**, not a confirmed memory-safety/OOM defect:

| Actual storage/check | Missing relation |
| --- | --- |
| C terminal_grace.rs486: TerminalSlot[Z] plus generic C Layout | Other simultaneous original backing is not included by inline size. |
| Target binding.rs69–82: slots(F), imports(L), effects(L) | Their simultaneous preparation, Flow backing and mailbox phases need a peak ledger. |
| Physical root509: CurrentOnly<T> plus Sleep | The separately allocated Z/K+3/E backing and transfer phases remain distinct. |
| Production server_root/construction.rs225: generic resident array | Per-session peak and outer ready backing cannot be inferred from this one array. |

Required next aggregate decision is either an actual-type phased sum/check or
a proven stronger upstream bound. Count moved backing once; embedded journals
already inside C/CurrentOnly are not separate allocations. Validate proposed
overflow inputs against earlier plan constraints before predicting a RED.
Timer cold explicitly distinguishes reportable Layout error, process allocator
abort and missing-time-driver panic; this audit does not invent a typed OOM
return guarantee. Original expiry still owes2*M+S+X plus Target/progress/ledger,
preflight/E/replay/encoded-byte work and runtime call-count evidence.
Scoped review System/DDIA8/8; Main Clean9 for existing inward/local owners.
10/10 requires the aggregate proof, reachable boundary witnesses and association
of rejection with the actual constructors, not more partial Layout assertions.

Selected independent next gate: same SOURCE01a0a99a-84ad-7ca0-8a94-a3bcbafac698
0/0/0 approves finite feasibility only. Earlier two findings were re-delivered
under01a0a99c-52b6-7422-ba95-cb60fe25cdd7 so the aggregate P1 is not lost when
wait returns only the latest submission. Main and Reviewer performed no tests.

Exact requested write axis is ONLY existing
`src/owned_upstream/quinn_leg_composition/owner/server_root/tests/ready_scheduler/process_plan.rs`
(415 lines), appending one test while keeping its old prefix byte-exact:
`production_cold_rejects_actual_resident_layout_before_reserve_and_returns_plan`.
Predicted440–465 lines, hard800. Existing ancestor aliases instantiate both
OwnerServerRoot and CommandResident with MemoryTarget; do not add type arguments
to these non-generic aliases. Set N from actual resident size:
`isize::MAX as usize / size_of::<CommandResident>() + 1`. Require invalid array
Layout but successful ReadyProcessPlan::checked(N,[1;4]); call real production
`OwnerServerRoot::cold`, not cfg(test) from_commands. Explicitly match
OwnerServerColdBuildFailure, avoiding a new Debug requirement. Verify exact
ReadyFault::Layout and complete returned geometry/budgets/turn/quantum bounds
through the existing assert_cold_plan helper. Premature reserve yields Reserve
and fails this oracle; no enormous valid allocation is requested. Small-N
construction is not a prerequisite of this narrow rejection test.

Expected direct GREEN; no fabricated RED. Future gates: fresh scope baseline,
strict pre-governance, exact+10 each mode, both-mode server-root/Ready/CurrentOnly,
all-feature Knife/repository/growth, both library checks, scoped fmt/diff and
strict post-governance, then same Reviewer. Mechanical test-only repairs may
use standing authority; production/new path/API need or changed semantic scope
stops for a new explicit grant. No production, routes, new helper or inventory.

Execution remains unauthorized pending the user single-file grant. This next
test covers the actual cold rejection under test-build MemoryTarget layout;
it does not cover every T/release ABI, legal-layout allocator failure, aggregate
P1 or full matrix4. Matrix3/4/combined review and five51B packages remain open.
No full P3/Case C/R7/Task4/physical-zero, external, cutover or commit/push claim.

### Bounded upstream implication follow-up (source only)

No new write grant arrived. Before pausing the new test, Main checked whether
earlier scalar limits alone could discharge the aggregate P1. Target config67
only requires K=max_pending_effects nonzero; Target construction146 and C
capacity152 only require K>=derived pending-work bound. E's checked encoded
byte product at controller159 is a separate axis. K=usize::MAX passes those
K-specific comparisons while checked K+3 is unrepresentable. This is algebra
over the inspected branches, NOT a run of a complete checked constructor and
NOT proof of a physically reachable enormous queue. The upstream alternative
must prove effective work-DAG/backing limits from actual resources, or add the
required cold peak check under an explicit future scope. P1 remains open.
`cold-peak-upstream-note.json` in the fresh artifact directory records this
finite source check. No Rust/Cargo/test/external action, no permission expansion.

## 2026-09-16 — Production cold resident Layout rejection

User grant: ONLY append the selected test in existing
`src/owned_upstream/quinn_leg_composition/owner/server_root/tests/ready_scheduler/process_plan.rs`.
Same Heisenberg SOURCE01a0ad1d-21b4-7b22-96ec-caba625f586b: P0/P1/P2=0/0/0,
Approve ONLY this characterization. Reviewer independently checked398 hashes
and original-prefix identity; no Reviewer tests or writes.
Main is sole Writer. The old415-line prefix is byte-exact; final433 lines.
All398 baseline Rust paths rechecked: only this leaf changed,397 unchanged,
no added/deleted path, production/route/API/helper/governance change.
Original prefix SHA256: `25ac6dd0056712447b08840dff6f79dcdef5ee563c71b090fa542e54347efbac`.
Final leaf SHA256: `cef72d0d8fbb24ca5ca217cf66c1220404cde7ca2da36e0b54092c90f634c61f`.
Fresh baseline/full execution evidence: `/tmp/mini-vpn-process-cold.YMBNAt/{baseline,evidence}.json`.

Exact test: `owned_upstream::quinn_leg_composition::owner::server_root::tests::ready_scheduler::process_plan::production_cold_rejects_actual_resident_layout_before_reserve_and_returns_plan`.
Actual MemoryTarget resident size selects N just beyond the array Layout limit.
ReadyProcessPlan::checked succeeds first; both original and returned plan checks
cover every geometry/budget/turn/quantum field. The production cold constructor
returns exactly Layout, not Reserve. No cfg(test) from_commands adapter is used.
Source confirms Layout validation precedes reserve; the runtime oracle checks
error precedence/input return, not an allocator-call counter. No huge valid
allocation, new Debug requirement, fake RED or production repair.

All33 Cargo invocations passed: exact1/1 and10 repeats EACH default/all-feature
mode; both-mode server_root25/25, owner_ready25/25 and CurrentOnly13/13;
all-feature Knife162/162, repository20/20, code-growth7/7; check --lib both modes.
Tests use --lib with the exact/full Module filters above or --test with the
three named integration targets, --nocapture --test-threads=1; the new exact
also uses --exact. All except initial default exact use --message-format=json.
Strict standalone pre/post rustc -D warnings --edition=2024 --test governance
compile and each7/7 pass. Scoped rustfmt --edition=2024 --config skip_children=true
--check, tracked git diff --check and all95 untracked whitespace checks pass.

Warnings remain vendor smoltcp22 plus local test-lib1/Knife149/other7, matching
preceding durable categories. Known14 old service.rs fmt hunks are untouched.
No full-library/global-fmt/warning-clean or release-ABI acceptance claim.
This test covers current test-build MemoryTarget's production cold entry only,
not every T, legal-layout allocator failure or complete phased cold peak.

Separate SOURCE aggregate-proof P1 remains open. Matrix3 applicable checked
recovery/cursor/unknown/full inventory, matrix4 total Layout/peak/call charges/
capacity/topology and combined consumer review remain mandatory. The next
technical decision is the already-required actual phased peak or proven
upstream-bound implication; no new Rust axis is implicitly granted here.
All five joint51B packages/P3/Case C/R7/Task4 remain incomplete.
No external/TUN/VPS/WAN/M2, cutover, commit or push.

## 2026-09-16 — Capacity-discipline SOURCE decision, aggregate P1 still open

The preceding single-file production-cold test remains accepted. This follow-up
is source inspection and existing-record maintenance only: no Rust write,
Cargo/test/build/format run, external access, commit or push. It does not claim
another runtime boundary or a legal-layout OOM reproduction.

Same independent Heisenberg consolidated SOURCE
01a0ad2a-fa99-7e50-b365-f80a228cac16: P0/P1/P2=0/1/0, Request Changes on the
original aggregate proof gap. Earlier source submissions were01a0ad25-e9c8-7500-94d9-bc94bec3a05d
and01a0ad26-f1fe-75d1-813a-2b364e2af6af. The final reply consolidates completed
inspection, not another test or architecture approval.

Actual call chain/live-phase map (paths below are under src/owned_upstream):

| Phase / real entry | Simultaneously held backing and distinction |
| --- | --- |
| server_root/construction.rs226 cold | Root, actual resident[N], Ready slots/four trees/shared notification; N alone does not provide per-session Target/work bounds. |
| server_root/construction.rs127 -> controller/construction.rs64 checked C | Existing accepted Root prefix plus caller's original Executor; Z allocated and progress initialized before binding. The sequential preparation gate forbids an unrepresented occupied suffix. Actor/seed is not owned by this C builder. |
| owner_target/binding.rs40 installation | Original Executor/C Z/progress, F service slots, L imports/effect scratch (L<=F), prepared Flow metadata/tickets/11 trees/publishers and mailbox pair. flow.rs118 already aggregates its local pieces. |
| owner_ready/cold.rs445 bind | Metadata/tickets transfer to original Ready/Executor owners; count the same backing once, then release actual scratch. No new post-bind fallible operation. |
| physical_session_root.rs509/546 publication | Original actor/handoff/bound C/Executor, one Sleep and existing Root prefix. CurrentOnly+Sleep Layout does not include every separately allocated backing. |
| server_root/construction.rs40/100 adopt/start | Same whole CurrentOnly moves into pre-reserved resident storage; not a second copy of its heap. N slots do not imply N identical session configurations. |
| observed_turn.rs / terminal_grace.rs828 / output_drain.rs483 | Original work, generated increments, raw page, replay scratch and output transfer form runtime envelopes; logical K+3/E is not a proof of actual retained capacity. |

The table's controller path is owner_session_controller/construction.rs; physical
and server-root paths are under quinn_leg_composition/owner/. It is an actual
phase map, not a completed byte sum or an assertion that all maxima overlap.

Independently confirmed source findings:

- Executor construction.rs195 starts an empty VecDeque. observed_turn.rs123
  converts SessionEffect into OwnerTargetWork with collect;167–199 extend before
  logical generated/K checks. These checks do not specify growth or reuse.
- observed_turn.rs609 appends NeedsResume after up to E output-producing steps;
  terminal_grace.rs839 pops it without shrinking. Application E is unchanged;
  raw storage may require E+1, and allocator capacity can exceed raw length.
- supervisor/owner_observation.rs103 retains original Vec allocation during
  filtering;124/176 allocate separate sequential candidate scratch.
  owner_output_drain.rs483 transfers the raw
  Vec into its existing deque while lengths scratch and source backing live.
- owner_ready/flow.rs118 aggregates local metadata, tickets, trees and publisher
  allocations already. It lacks the cross-owner live-phase relation, not its
  own local sum. Embedded C journals must not be charged twice.
- Installed Rust1.95.0 (59807616e1fa2540724bfbac14d7976d7e4a3860,
  aarch64-apple-darwin) RawVec uses amortized growth/minimum nonzero capacity;
  Vec's public contract does not promise a fixed collect/growth strategy.
  No repository rust-toolchain file was found. Recording this version is not
  a toolchain guard or a proof that cross-type collect reuses allocation.

Main-only supplemental source finding (Reviewer explicitly did not independently
re-read it): target.rs734/769 store per-flow written/readable deques;
take_written962–984 drains logical bytes while retaining capacity, as does the
readable path in target/memory_io.rs. Its cold_inventory counts len, correctly
for logical ownership but not allocated capacity. A shared live-byte quota
therefore cannot by itself bound retained per-flow backing after sequential
use. This is not a runtime leak/OOM finding or permission to modify Target.

Executable decision needed before freezing Rust scope:

1. Recommended: explicit original-owner capacity, reservation and growth/transfer
   discipline. Account for raw continuation, conversions, scratch and failure
   custody; forbid unaccounted implicit growth. A lone reserve(K+3) or checking
   capacity after allocation does not close the obligation.
2. Alternatively preserve current growth paths, with an explicit toolchain guard
   and per-container/per-conversion capacity recurrence proof. No unexplained
   2*K factor, assumed reuse or logical-byte substitution is acceptable.

Neither alternative is approved. No evidence yet requires replacing every
container or expanding Interface. Executor/Supervisor/Output and construction
are implicated, but that list is not a proven complete repair scope. Next exit:
select the discipline, derive the actual-type phased peak/stronger bound and
freeze a real falsifier plus complete allowed files; then resume TDD, not another
standalone sidecar. Preserve K/E semantics, one original work owner, exact
failure custody, growth gates and allocator-abort policy. Do not add a second
K queue, global allocator/lock, per-packet inventory or fake capacity token.

Scoped assessment remains Main Clean9/10 and independent System/DDIA8/10 each;
10 requires truthful backing/overlap bounds and relevant real-constructor
validation, not documentation promotion. Matrix3/4, combined consumer review,
five51B packages, P3/Case C/R7/Task4 and external gates remain open.
Final source audit: all398 Rust paths match the accepted direct-cold checkpoint,
no added/deleted path; process_plan.rs433 and original415-line prefix remain
exact. Six existing records pass newline/trailing-whitespace and tracked diff
checks. This documentation maintenance adds no runtime acceptance evidence.

## 2026-09-16 — Explicit original-owner capacity direction selected; scope audit

User confirmed the recommended capacity/reservation/growth/transfer direction.
The previous direction blocker is resolved and the full P3 goal is active.
This is not approval of a guessed file list, blanket container rewrite or unsafe
allocator implementation. No Rust, Cargo/test/build/format, external, commit or
push action occurred. Existing six records are updated; no new design sidecar.

Same Heisenberg consolidated SOURCE01a0ad48-5cb8-7ed0-81b7-bf821b6188b3:
P0/P1/P2=0/1/0 on the original aggregate proof gap. Submissions
01a0ad40-b4ca-7ed0-87a3-75f002c2927c,01a0ad43-b744-7d02-a7c4-47aea88c08e8
and01a0ad45-bc27-7342-9d80-e17d83ca97ba covered Target, shared core and sparse
preparation. Final reply only consolidates actual completed source inspection.

Mandatory per-owner capacity contract map (paths relative to src):

| Original owner / source anchors | Required discipline, not blanket replacement authority |
| --- | --- |
| resumable/session.rs1107/1475/3045 | Flow/tombstone map nodes and ordinary/recovery effect backing, including producing and transferring original batches. |
| resumable/tcp.rs323/867/942 | Send descriptors, receive map, two reservation vectors and commit overlap; retain existing normalized payload/sliding-compaction contract. |
| owned_upstream/supervisor.rs658, supervisor/driver_input.rs, supervisor/owner_observation.rs88 | Original replay/quarantine/receipt and candidate scratch, preserving entered/unknown custody; candidate stages are sequential where source proves it. |
| owned_upstream/owner_target.rs1764/1947 and construction.rs/observed_turn.rs | Original pending/entered/current queue, effect conversion, outputs and returned errors. Reserve a possible burst before the producer commits, not at stage_entered_reduction159. |
| owned_upstream/owner_output_drain.rs372/483 and owner_session_controller/terminal_grace.rs828 | Raw E+1 versus application E; source page, destination FIFO and lengths scratch overlap, including stop/return conversions. |
| owned_upstream/reverse_switch.rs65 | Actual progress entry/node storage for2*(F+Z), not only the logical entry count. |
| owned_upstream/target.rs136/734/769/962 and target/memory_io.rs15/280 | Retained buffers/directives/maps/address and return-buffer storage; deepen the actual capacity witness with enforcement at its owner, not a scalar getter. |
| Existing checked C/binding/CurrentOnly/server-root constructors | Compose original live backing and new scratch by phase before binding/publication; transferred backing is not a new copy. |

The table identifies obligations and real owners, not an exact allowed-files
list. Main source counts: SessionModel6442, Tcp window2469, Supervisor5439,
OwnerTarget8515, progress3231; these remain grandfathered, unchanged. Small
construction204/observed_turn994/owner_observation217/output1009 and Target1497
also remain unchanged. Any future structural Move is separate from behavior,
with characterization/provenance first and Integrator-only shrink ratchets.

Independently confirmed counterexample sequence, SOURCE prediction not executed:
MemoryTarget F=2/per-flow=8/global=8; write8 to A, fully drain/drop returned Bytes,
then write8 to B and fully drain/drop its result. Logical bytes are0 but both
written deques retain total capacity at least16. It falsifies treating the
global live-byte quota as a backing bound, not existing logical quota behavior.
The previous Main-only finding is now independently source-checked.

TargetIo's actual implementations are MemoryTarget plus three test wrappers.
No production TCP/DNS/TUIC Adapter implements that Interface today. TUIC/REALITY
use ProxyUpstream instead; this is not a claim that the project lacks TCP/DNS.
The real Target Adapter remains the original51B duty, not a newly invented
prerequisite of this audit and not removed from P3.

Reuse, do not erase: owner_ready/flow::check_layout, MemoryReadiness fixed
positions/local checks, actual C TerminalSlot[Z], binding scratch checks,
CurrentOnly+Sleep and Root resident validation. Their local results are useful;
the missing item is truthful cross-owner peak composition. No evidence requires
rewriting all std containers, Tokio/Quinn internals or global allocation.

Stable-Rust feasibility note from installed1.95 source: try_reserve_exact only
promises at least requested capacity; Box::try_new_uninit_slice remains behind
allocator_api. A checked-Layout -> allocation -> strict initialization -> Box
handoff is a reviewable candidate, not selected/approved necessity. If proposed,
alignment, zero length/ZST, allocation failure, initialized subset and unique
Drop all need proof; Option::None cannot be assumed all-zero. An allocation
primitive alone does not implement a bounded map/ring or its transaction.

Selected operating constraints: avoid dense cold allocation of the entire K
work-DAG (which includes2*receive_bytes+5*F+recovery+3). Bounded on-demand/block
preparation may be used, with actual chunk/slack, retained free blocks, directory
growth and relocation peak counted. Keep original complexity and one original
work queue; no global lock or per-record allocation merely for this audit.
Fallible preparation must precede reducer/Target side effects. Expected rejection
returns original input/work; already-entered failures retain original nonzero
custody. Consumption, ACK, drain or clear does not release backing by assertion.

Design exit still to execute in this same repair package: settle each owner's
retain/deepen/replace representation, block/directory growth, capacity source,
precommit error exit and transfer peak; consolidate complete Rust scope here
and joint51, then same Reviewer before writes. Do not open another sidecar or
repeat the accepted direction question. No expected RED is frozen before these
details yield a compilable actual constructor/consumer test.

Required first complete test boundary: legal K larger than the first block,
non-dense checked construction; real protocol/Target/completion/continuation
crosses the actual growth boundary; inspect physical capacity and original
work/output identity; repeated consume/reuse cannot accumulate uncharged backing.
Actual unrepresentable Layout must reject before new binding/Target side effects
or publication and preserve the original input. A standalone Layout error or
storage-helper-only test does not complete this boundary or close P1.

Assessment: Main Clean9/10, structural/refactoring8/10, same Reviewer System/DDIA
8/10; reach10 through enforced representation/peak and real-path tests. The
architecture/refactoring skills kept ownership local and structural Move separate;
self-improvement records the precommit/sparse-capacity lesson, not global skills.
No throughput/OOM/runtime acceptance. All matrix3/4 and full P3 duties remain.
Final checks: all398 Rust hashes unchanged from the accepted direct-cold
checkpoint, no added/deleted Rust path; all six updated records pass newline,
trailing-whitespace and tracked diff checks. No project test was run this turn.

## 2026-09-16 — Contiguous backing selected; first complete package converged

Architecture-only continuation of the confirmed capacity direction. Same
Heisenberg SOURCE01a0ad58-2d62-7b31-80c4-b7701cdaa081 concludes0/1/0: original
aggregate P1 remains, but original-owner controlled Vec/VecDeque backing is an
acceptable selected representation. No new map/ring/chunk directory is required
before this package. Main narrowed the call/ownership boundary below; this is
not code approval, implementation evidence or a new sidecar.

### Allocation and growth decision

Keep standard container operations within explicitly prepared capacity. The
candidate checked Layout::array<T> -> std::alloc::alloc -> null check -> one
Vec::from_raw_parts(ptr,0,capacity) handoff is viable. Nonzero/non-ZST allocation
must use exactly the type alignment/Layout and allocator later used by Vec.
Handle zero capacity/ZST separately. No exit/callback may intervene between a
successful allocation and owning handoff. Afterwards Vec owns initialization
and Drop; no handwritten element destruction or all-zero Option assumption.
Only same-type Vec->VecDeque has the public allocation-free O(1) guarantee;
cross-type SessionEffect->OwnerTargetWork collect does not inherit it.

This is exact requested Layout and observable capacity, not allocator metadata,
RSS or successful-OOM recovery. Existing allocator-abort policy stays. No stable
safe std alternative satisfying all three requirements (advance exact upper
bound, recoverable Reserve, no private-std dependency) was verified in this
bounded review. That does not prove unsafe is uniquely necessary. Small unsafe
implementation is not write-authorized; no global allocator replacement.

Main's concrete growth proposal, not yet reviewed/implemented: each original
buffer owns a maximum M, first capacity b and retained actual capacity c. Reuse
when need<=c. Otherwise choose the first member of b,2*b,4*b,...,M meeting need,
clipping to M using c>M-c before doubling; reject need>M before allocation.
Cold work starts at min(3,M), not K. Work's candidate M is checked K+3; output
has separately checked M=E+1. Neither changes logical K/application E.
Before moving, check both exact array Layouts and old+new live-byte sum. Only
then allocate and transfer items with prepared single-element operations, no
callback/Clone/element Drop in the move region. Clear/drain retain capacity.
For a cap sequence ending at M with preceding p, its largest growth overlap is
p+M, not an unexplained2*K. Main's non-Rust arithmetic examples: M17 yields
3,6,12,17 and peak29 slots; M1027 yields last768->1027, peak1795 slots. These
examples test only the proposed arithmetic, not source behavior or phase totals.
Original producer/source/raw-page backing is additional when actually live.

### First complete package and real precommit boundaries

The package is original producer effects -> Supervisor original receipt journal
-> Executor current/suffix -> raw output Vec. Do not call a storage-helper or
Executor-only change a complete repair. It need not rewrite all maps/Target
backing first; those original P1 obligations remain mandatory and unchecked.

| Actual source boundary | Required preparation / unchanged ownership disposition |
| --- | --- |
| resumable/session.rs1536,1304,1672,3045 | Original ordinary/recovery producer backing prepared before its own commit; preserve public Vec semantics and one reducer implementation. Recovery already materializes before install/CAS: reuse that transaction, not a new late reserve. |
| supervisor.rs1094/1204 and original driver_input/owner_observation | Original DATA/event admission and batch/facts journal retain their exact inputs and allocation; a downstream queue reservation cannot cover this producer. |
| owner_target/observed_turn.rs852; service.rs659 | Before reducing event/command and before feedback is marked Reducing. Known rejection carries the exact original input through its proper typed error. |
| observed_turn.rs96/930 | Prepare before consuming the exact resume or destructuring attached recovery; rejection returns that same capability/cursor. |
| observed_turn.rs334/671; completion.rs1145 | Already-entered current stays root-held. Prepare possible fanout/output before its next Target/reducer side effect. Failure here is not rollback of the already-committed outer turn. |
| read_source.rs150-175/273 | Prepare before actual Target read/source-input consumption, not after a real read has already committed. |
| observed_turn.rs123/167/942 | Replace cross-type collect/unprepared extend with prepared transfer; retain original incoming Vec AND facts on generated-bound failure or occupied/conflicting incoming. Existing error precedence and whole original ownership must survive. |
| observed_turn.rs612; terminal_grace.rs828 | Prepare raw E+1 including resume publication. C's later pop/move does not shrink the source allocation; prove source/destination/scratch overlap without gratuitous C behavior changes. |

Concrete error gap: OwnerTargetConfigError currently has no storage error;
OwnerTargetInputBlock and ReducerAdmissionBlock have no allocation rejection.
Do not disguise reserve failure as an unrelated invariant. Freeze the real
config/admission/entered error signatures and original batch carrier together;
any returning error must still own the input or original generated batch.
Unwind keeps actual entered work/facts nonzero; no synthetic empty fallback.

Next executable exit is that package's signatures, precise files/symbols and
real constructor -> growth -> reuse/rejection tracer, then same Reviewer before
Rust. No expected RED or exact write list is falsely declared frozen here.
Checked construction must use real provenance, not the old recovery fixture's
accept_owner_attach_for_test compatibility route as production acceptance.
No dense K allocation, changed K/E, copied reducer, second business queue,
per-record audit allocation, test getter/re-export or new unrelated sidecar.

Scores remain Main Clean9/10, structural8/10; prior System/DDIA8/10 is not
raised by this source-only decision. Reach10 with enforced backing/error/peak
and real lifecycle evidence. Capacity/architecture skills kept producer and
consumer custody together; no implementation or runtime gate ran. P1/P3 and
all remaining matrix3/4/51B obligations remain open. No external/commit/push.
Validation: all398 Rust hashes still match the accepted direct-cold checkpoint,
with no new/deleted Rust path. Six edited records pass final-newline/trailing-
whitespace/reviewer-reference checks and tracked git diff --check.

## 2026-09-16 — Effects-storage packet reviewed; explicit Rust authority pending

Architecture-only continuation. Same Heisenberg consolidated SOURCE
01a0ad69-8e9a-7790-b4b8-5ab781313ad7 resolves the actual error/custody choices;
it consolidates completed submissions01a0ad5f-9cf4-7420-bf0d-e7f530a7127a,
01a0ad63-7a4c-7391-8f4f-773fbff1d109,01a0ad64-8549-7610-8e1d-4fa5fff59cb2 and
01a0ad67-1640-78e1-97ae-29baf572121b. Main did not receive all intermediate
answers through the tool; the final resend performed no additional inspection.

Decisions now explicit in joint51D, not another sidecar:

- Prepare ordinary producer3 at construction and recycle only the same empty
  successful backing. This preserves DATA staging-before-Healthy/origin rather
  than inventing a reverse staging->UplinkData API or TCP getter.
- Keep complete original batch/overflow/facts through entered/conflict failure;
  retain both old Box identities. Public Clone SessionError remains value-only;
  the proposal gives its impossible overflow a Model-owned failed position.
  C's existing error owner retains the whole nested error; that count does not
  claim to recursively inventory every new payload. C production need not change
  merely for an expanded payload; its existing returned_error fixture must adapt.
- Recovery preparation already precedes CAS, but leg.rs470 destructures the
  original received and currently returns only SessionError on model rejection.
  The selected leg-owned carrying enum returns exact seal/frame only for the
  new storage errors, preserving existing mappings and authentication/CAS.
- Registered restore failure currently discards the new carrying rejection;
  retain both rejection and exact restore cause. Main also checked catch-up
  owner_target.rs2466 and leg_io.rs4629: release failure returns the original
  reservation, so this path must retain it and the original rejection together.
  This is required handling for the proposed new failure, not a claim that a
  production OOM/restore counterexample has been executed. No standby rewrite.

Main placed one finite152-line proposal at joint51D:14 production paths,
enumerated test paths, one unsafe site in resumable/effect_storage.rs, normal
1500/800/300 limits and unchanged oversized ceilings. No C/standby/auth/TCP
production expansion or new directory grant. Governance remains Integrator-only.
The packet's first full fixture/expected RED must be frozen before its test is
written; acceptance cannot shrink to an allocation-helper or cold-only test.

Same Reviewer SOURCE01a0ad6e-ea3c-7270-8a07-0a0f2da40f62 inspected those152 lines:
P0/P1/P2=0/0/0, Approve ONLY the authorization-range proposal. No new source,
hash, line-count, arithmetic or runtime audit was performed in that final review.
This does not close the original aggregate P1 or authorize the actual edits.
Next: request explicit user authority for the named packet/new files/limited
unsafe; then freeze the real fixture/RED and implement under the standing
within-scope repair authority. Do not repeat the accepted capacity-direction
question, infer the unsafe grant, or add another prerequisite sidecar.

Additional executed evidence is only candidate arithmetic:593437 BigInt
transitions for maxima1..256 and seed1/3 had no monotonic/bound/sequence/peak
counterexample.32/64-bit maximum requests terminate, over-maximum requests reject;
old+new peaks exceed usize at those extrema and must be checked, not wrapped.
This is not Rust, actual Layout/type size, allocator or pipeline acceptance.
Prior structural/System/DDIA8/10 and Clean9/10 are not raised by plan review;
enforcement and the real lifecycle matrix remain necessary for10/10.

No Rust/test/Cargo/build/format/external/commit/push ran. Fresh398-path SHA-256
check still matches the accepted direct-cold checkpoint with no new/deleted
Rust path. Only the existing six records were edited. Full P1/P3/Case C/R7/Task4
and all remaining matrix3/4/51B obligations remain open. Architecture/capacity
skills preserved original-owner settlement; self-improvement records the new
carrying-error lesson locally, without global skill changes.

## 2026-09-17 — Authorized51D implementation, full-path RED and TCP Move

The user explicitly grants continuous work to P3, including this packet's new
paths/limited unsafe and in-scope repairs without another permission pause.
All architecture, custody, growth/review and external prohibitions remain.

Main pre-write standalone governance:7/7. Added one real test in the private
initial_bootstrap_handoff/effect_storage.rs child: checked Owner construction,
production Client bootstrap, actual MemoryLegTransport initial authentication,
three real OPENs/MemoryTarget opens, registered B recovery, four E1 pages and
original acceptance/output publication, followed by another ordinary command.
After test-feedback import/type/idle-flush corrections it ran exactly one test
and reached the intended cold-work capacity0-versus3 RED at leaf226. Previous
protocol/Target/page assertions passed; later growth/reuse assertions did not
execute. Component LegLost is not physical terminal/cleanup evidence. Same
Reviewer approved only the fixture and expected RED, P0/P1/P2=0/0/0.

Added original-container allocation/growth primitive at the sole permitted
unsafe site; six focused tests pass: actual alignment/zero/ZST/Layout, clipped
levels, wrapped queue order/no premature Drop, injected Reserve/original backing,
logical ZST bounds and retained first overflow. This is not real OOM/null
allocator execution, complete producer integration or aggregate peak proof.
Initial Executor cold work3/output1 preparation and entry preparation helpers
are implementation-in-progress; the complete packet is not accepted.

Same Reviewer's bounded preflight audit found six inner TCP commits still own
validation after preview. Recorded one narrow correction in joint51D, adding
tcp.rs + private send.rs/receive.rs, preserving public signatures/preview order.
Pure Move:218 send lines and112+147 receive lines copied intact; parent2469->1995,
children226/267. Main TCP regressions27/27 before and after; rustfmt three files
and tracked diff check pass. Integrator changed only tcp ratchet2469->1995;
standalone and Cargo all-features governance each7/7. Same Reviewer independently
compared all six method blocks byte-exact and approved Move plus primitive
local SOURCE0/0/0, not the later prepared behavior or entire P1/P3.

Following that Move, append and ACK tests each first failed only with E0599 for
their missing prepared operation. Minimal original-window prepare/commit then
passed each exact tracer1/1. Injected effect Reserve leaves the original window
unchanged; retry commits exact payload/prefix/partial suffix. Send CLOSE and
receive operations, Model/Supervisor original-buffer reuse, Executor incoming/
output transfer, carrying recovery failures and complete related review remain
work in progress. New unused-item warnings reflect unconnected implementation;
existing vendored22/private-interface warnings are not described as warning-free.
No external, cutover, commit or push. Full P1/P3/Case C/R7 remain open.

### Six inner TCP prepared operations: local reviewed closure, not51D closure

All six exact prepared-operation tracers first produced missing-method E0599
and then GREEN1/1. Main's subsequent default and all-features groups each passed
TCP27/27, effect-storage12/12 and Session44/44. Same send SOURCE then found one
P1: prepare_append dropped the original Bytes owner before normalized window
publication. Main checked and exposed the same problem in commit_receive with
an equal-byte but distinct-owner reservation. Two real Bytes::from_owner Drop
panic tracers compiled and ran, both RED at normalized inventory0-versus3.
Prepared append/receive now also hold the original input; commit publishes all
normalized ownership/accounting before original disposal. The same two tests
then passed2/2 (the disposal panics are intentional and caught).

Final scoped rustfmt produced tcp/send328, tcp/receive392, effect_storage220,
capacity leaf419; tcp parent remains1995. Main then reran, in both modes:
TCP27/27, effect-storage14/14, Session44/44. Strict standalone governance7/7 and
scoped formatting/tracked whitespace checks pass. Command/summary evidence is
in /tmp/mini-vpn-p3-effects.AGjIGL/tcp-regressions.json; earlier fixture/tool
feedback remains described above, not reconstructed as raw logs.

Same Heisenberg SOURCE01a0ada9-2907-7023-ba47-ad9d8d292ee9:0/0/0 Approve only
these six transactions and primitive; independently verified exact lines and
original Drop/epoch/accounting/partial-suffix/public signature semantics.
Local structural score9/10. Commit adds no recoverable error, but existing
container allocations and original input Drop remain: not alloc-free or
unwind-free. Reviewer ran no tests. Full producer/Supervisor/Executor wiring,
original received-error propagation and actual aggregate P1/P3 are still open.

The in-place shared-Vec probe would format to6523 versus6442; it was not applied.
Same SOURCE01a0adb0-fc5d-7723-a66a-439374bf9a86 approved the single ordinary
producer child and unpromoted internal Move sequence0/0/0. Main moved reduce
plus24 complete helpers, including begin_half_close: parent6442->5534,
private child918. All25 method SHA-256 values remain individually identical;
default and all-features Session44/44 pass. No new visible method or forwarding.
Final complete51D promotion must lower the parent ratchet to the actual final
count; intermediate shrink is not a green governance claim. Shared-buffer
conversion and whole-chain wiring remain unaccepted. The full fixture's last
observed verdict is still the original expected RED.

### Ordinary producer convergence and precommit tracers (implementation continues)

Same SOURCE01a0adb7-b531-7ef0-b470-0a2129d027e6 approved the pure25-method
Move, including a complete inverse parent comparison. Shared single-Vec
conversion then passed Session44 in both modes and SOURCE01a0adb9-e7c6-7c80-
ac1c-dfdc937c34c8 approved only that shape. No nested effect Vec/extend remains
in ordinary helpers; special LocalOpen/recovery still use their old paths.

Actual public Client Model tests use verified CommittedLeg. The first fixture
mistakenly requested current generation1 and failed before the target; fixing
only the request to next generation2 reached the intended LegLost runtime RED.
LegLost then reserves after original validation/no-op, before phase change.
LocalData had a separate runtime RED returning ReplayStored+Transmit despite
the injected Reserve; it now uses the reviewed TCP prepared append before
reserve/commit. Tests prove exact snapshot preservation and EmptyData priority.
Same SOURCE01a0adbe-f8ee-7241-915e-1520408e3220 approved these two local seams.

Approved test-only Move restored capacity.rs to its original419-line SHA
3dc6cfbc67aba184e7860a5469439577c2633f41264eed25173a55ff01037807; new sibling
model.rs held the exact two tests/fixture (block SHA70e03472d6048175bd0c2ffbdd35e
090fe20617398edcf3128af5a339e8a0aa0), private index2. Both-mode16 tests passed
after Move. Model-specific tests continue in this bounded leaf, not capacity.

One sender lifecycle tracer next reached FIN RED, then after minimum FIN GREEN
reached ACK RED, then fully passed: original errors precede reserve; partial
ACK preserves exact state on Reserve/retries once; final CLOSE no-op allocates
nothing. Pure shared finish checks are evaluated without allocation and their
errors deferred until after original TCP/global ACK checks. Equal exact delta
subtraction preserves terminal-release inequalities. Same SOURCE01a0adc5-
34d6-7e81-b464-abf01c80e2e3 approves only this seam0/0/0; the tracer does not
enter finishing=true, which remains a required test in the complete packet.

Expiry's own real RED returned SessionExpired and cleared live replay before
Reserve. Preparation now precedes clearing; exact retry passed. At this point
Main ran both modes: primitive/TCP/Model18/18 and Session44/44. Session parent
5551; ordinary child1046 before subsequent receiver work. New value-only
SessionError Layout/Reserve/Overflow variants preserve Clone and expose no
private resource payload. These are still transitional shared-Vec operations,
not Batch overflow/root custody or Supervisor backing-reuse acceptance.
Receiver gap/duplicate/no-output preparation is the next executable tracer.
Whole51D/fullP3 remains open; final parent ratchet is still deliberately pending.

### Ordinary producers and actual failed Batch custody

Receiver DATA, acceptance, FIN and half-close completion each reached their
actual missing-reserve runtime RED, then GREEN at the original validated
precommit seam. Gap/duplicate/legless-no-output paths remain allocation-free
for this effect container. Both finishing=true graceful orders and legless
partial/adjacent/gap acceptance are direct-GREEN characterizations, not newly
fabricated REDs. Same SOURCE01a0add2-47c1-7141-8f99-3fe075d9b3f6 approves the
five receiver/expiry seams0/0/0; SOURCE01a0add0-2821-7703-89cd-8be0999737e7
approves the added characterizations. The two resumable-scoped prepared bool
queries are recorded in joint51D and introduce no window getter.

Owner OPEN, resolution and replay then each exposed a real runtime Reserve
failure before the corresponding fix. Every remaining ordinary send/terminal
caller was audited together so no new fallible allocation occurs only after
its state commit. Same SOURCE01a0add7-6109-71d0-87c1-3f592dedd9aa approves
that production state0/0/0 (parent5549, effects1113, receive408). The intact
graceful test moved from model664 to model574 plus private terminal96;
block SHA67b10a2b9b6ee34cdea416978c733ec50488f136c0d0e1cf5182c057cc2b7fb4.
Post-Move both-mode22/22 passed. Two more direct-GREEN tests cover Client
first OPEN results and actual local/peer reset, abandon and half-close failure.
Default/all-features then passed effect-storage24, Session44, TCP27 each.

The next exact test challenges the same private owned wrapper used by public
reduce with deliberately undersized capacity1 and real LocalData. Initial
test scaffolding accidentally copied a partial following test; after removing
that fragment, RED contained only missing reduce_owned and failed_effects.
After implementation, an opaque iterator's test borrow needed an explicit
drop before mutable retry checks. The test then passed1/1: identical original
allocation, prefix ReplayStored and first overflowing Transmit are retained;
subsequent reducer/prepared-entry mutations reject without state change.
This is an impossible-capacity safety check, not proof of a natural >3 burst.

Public reduce now delegates through the sole borrowed Batch producer; every
append immediately propagates its first error. Nonempty failed public batches
remain Model-held, separate from the future Owner journal-borrowed path.
Special LocalOpen/recovery still preserve their prepared Vec pathways.
Current exact source: session5567, effects1128, private effects/tests96;
model574, terminal261. Both-mode effect-storage24 and Session45 are green.
Same Reviewer SOURCE review of the latest tests/Batch code is pending.
Supervisor cold/journal transfer, recovery carrying errors, Executor complete
incoming/work/output transfer, final ratchets and full51D/P3 remain unfinished.
The full-path fixture's last observed verdict remains cold0-versus3 RED; it
has not been rerun after partial integration. No external/commit/push occurred.

### 2026-09-17 — Connected original backing/fault matrix locally accepted

This supersedes the preceding partial-integration position. Exact commands,
repeats, scope and411 Rust hashes: `/tmp/mini-vpn-p3-effects.AGjIGL/progress.json`.
Whole51D SOURCE closure is Approve, P0/P1/P2=0/0/0 by same Heisenberg. The
Reviewer independently matched411 Rust hashes/31-path scope,30 Cargo records,
20 repeats and108 untracked whitespace checks, but ran no tests. Both packet
P1s and E1-constructor P2 are closed. Aggregate P1, remaining matrix3/4 and
five51B packages remain open. No external, cutover, commit or push.

The checked three-OPEN fixture now crosses ordinary production, registered R4,
four E1 pages and reuse: original ordinary capacity3/pointer returns unchanged,
work grows3->6 and remains6, raw outputs hold2/2/2/1. Recovery prepares its exact
allocation before generation CAS. Real injected Reserve returns the same received
ATTACH for successful retry. Separate checked catch-up coverage delivers actual
status through MemoryLegTransport, arms its permit, injects pre-CAS Reserve,
preserves received/permit/FIFO, retries that input and delivers actual acceptance
and replayed OPEN result. Real wrong-queue registered restore and catch-up release
plus manually composed opaque errors retain original errors/rollback/reservation
and Box identities. These are defensive primitive/composed-error tests: a legal
outer caller exclusively holds one queue and cannot swap it mid-call. Neither
outer double-failure branch is claimed naturally executed.

Supervisor now owns the original ordinary3 spare through Idle/Entered/Transferred;
DATA retains staging-before-Healthy/origin. Executor incoming holds full batch
and facts before bounds checks, then moves into prepared original work without
collect/extend allocation. Only completed empty transfer recycles. Recovery
allocation never replaces the ordinary spare. Same-batch monotonic mutation
evidence covers empty gap/Legless/expiry commits; only explicit returned errors
with false marker, empty effects and no journal facts/frontier restore Idle.
Unknown/unwind/DATA remain sticky. Earlier15 OwnerTarget regression failures
localized missing entry preparation, unreclaimed terminal-attach reduction and
over-poisoned precommit ACK rejection; original-owner repairs restored159/159.

Characterized pure Moves preceded behavior: complete Supervisor observed entry
to owner_observation (48/48 before/after, exact body), complete attached-recovery
resume to observed_turn (exact acceptance-queue test before/after), then three
fault tests to private effect_storage/faults.rs (four tests before/after, block
SHA256 `37605977e7b2920369bd9a55b76e742452aac5c6e02f5498a3411786627c81ea`).
Later scoped formatting is separate, not a current body-hash claim.

Same Heisenberg's packet review found two local P1s. Actual terminal-batch test
REDded on missing-page payload-free rejection; both consuming guards now return
the original batch/facts/error. It also covers occupied incoming and real
terminal-fact merge conflict, keeping first work and second batch/moved work.
An actual MemoryTarget reset/read fault REDded on missing inline current;
direct abort now transfers original backing and retains current inline rather
than allocating with push_front. Existing-page rejection precedes all takes.
Both GREEN, OwnerTarget163 passed. The Target/Executor inconsistency is explicit
test fault injection, not ordinary protocol termination. Final whole-packet
review subsequently accepted both repairs as recorded at this entry's start.

Additional checked-path evidence:

- K=usize::MAX-3 passes addition but fails actual Work Layout. Original bootstrap,
  Target identity/config/source/FIFO return, drop0; normal-K retry then drop1.
  An early legacy unproven fixture was replaced, not counted as checked proof.
  All new fixtures use production prepare_owner(plan); E1 is constructor-set.
- Entry output Reserve returns the exact normalized DATA Frame before mutation,
  then same-event retry accepts four bytes. Initial pointer assertion incorrectly
  observed the buffer before Frame normalization; only observation was repaired.
- Entered output growth Reserve retains actual committed OPEN/current/backing,
  runs no Target callback and rejects reentry with its original input.
- Real installed Source/Driver DATA reuses ordinary3; a deliberately armed effect
  allocation refusal stays armed until a test probe. No claim about allocation
  outside that seam, replay maps, normalization or whole-call allocation freedom.

Knife's stale flat #[path] assembly could not resolve new default children and
effect_storage, producing230 cascade compile errors. Loading actual production
resumable/mod.rs fixes the route with all non-Clone guards/uninhabited shims
unchanged. Suite190 includes newly loaded real tests, not a weakened162 oracle.

Post-format gates, each default AND all-features: storage25, Session47, innerTCP27,
Supervisor48, OwnerTarget166, Controller61, Client37, CurrentOnly13, continuation16,
server-root25, owner-ready25; all pass. Eight new packet fixtures repeat10/10 EACH
mode. Knife190, repository20, Cargo governance7 and library check pass EACH mode.
Fresh standalone rustc -D warnings compile/governance7, scoped31-path rustfmt,
tracked diff and all untracked text whitespace checks pass. Global fmt/full-lib/
external acceptance not claimed. Library7/test-library1 existing warnings remain;
standalone source assembly emits149 unused/dead-code warnings; no suppression.

Integrator ratchets: Session6442->5613, Supervisor5439->5409, OwnerTarget8515->8498,
Leg2705->2699, Knife2624->2592; prior innerTCP1995 retained. Packet:18 changed old
Rust paths+13 new, no removal. Against398-path pre-process-test baseline379 are
unchanged; the additional old change is prior accepted process_plan433, still
SHA256 `cef72d0d8fbb24ca5ca217cf66c1220404cde7ca2da36e0b54092c90f634c61f`.
Continue remaining P3 without repeated scope grants; this packet's review is closed.

### 2026-09-17 — Checked-Bound recovery publication survives actual FIFO pressure

Locally accepted component characterization, same Heisenberg SOURCE0/0/0.
Only effect_storage.rs450->706 changed; original450-line prefix and410 other
Rust files are unchanged. Final SHA256:
`cb6360ec71224d506e1a4d64d0152e8f0cc38c3d308ee970105345730e0cbd92`.
Exact commands/repeats/411 hashes: `/tmp/mini-vpn-p3-recovery.HLxB8L/evidence.json`.

Production prepare_owner/initial bootstrap and real Memory wire establish one
OPEN, standby registration and registered exact-next ATTACH. The same Executor
crosses production C::new_checked with a live ReadyIndex, remaining BoundInactive.
E1 first emits LegActivated; second page holds original OpenResult. Acceptance
fills the actual FIFO1, so three publication retries return real queue pressure;
attempted execute cannot skip the held page. C restores the original boxed drain
to Publish; Box allocation addresses need not remain stable between retries.

Actual FIFO submission frees its slot; the test then completes peer receipt and
ordered-wait retirement before retry. The peer validates one original acceptance
and one generation3/flow1/Opened result. Repeated completed publication adds no
record; C retains HandoffReady and A retains its acceptance authority. This is
not whole-root zero or a claim that admission waits for peer acknowledgment.

First exact test directly GREENed; no production repair. Both modes: exact10/10,
fixture9/9, full Controller61/61, Knife190, repository20, code-growth7 and lib
check pass. Standalone pre/post7 with strict compilation, scoped rustfmt, tracked
diff and108 untracked text whitespace checks pass. Same Reviewer independently
verified scope/prefix and SOURCE, ran no tests. Existing warning categories
remain. No inventory edit, external run, commit or push.

This discharges only the applicable checked C FIFO/cursor tracer. Remaining
matrix3 unknown/recovery interruption/full nested inventory, aggregate matrix4,
combined consumer review and all five51B packages remain mandatory P3 work.

### 2026-09-17 — Original nested-error facts reach the actual C/Root cold audit

Locally accepted finite joint51E family, same Heisenberg P0/P1/P2=0/0/0.
Not complete nested resource inventory, aggregate P1, matrix3 or full P3.
Artifacts: `/tmp/mini-vpn-p3-nested-inventory.q0LWgl/move.json` (accepted pure
Move414 hashes), `behavior.json` (415 frozen hashes) and `behavior-gates.json`.

Pure Move first: complete original C inventory predicates106 lines and audit/
helpers154 lines moved into private cold_inventory; owner inventory202 lines
moved as one implementation with four parent-effective builder visibilities.
Original143-line provenance test moved to its explicit private child. Rebasing
its relative include_str fixed the first compile-route error; AST source routing
then followed the real child, retaining constructor3/literal0/field/byte guards.
Integrator lowered C3268->3009, owner2062->1861, Knife2592->2454. Both-mode
C61/owner9/cont16/Knife190/repo20/growth7/check and strict/scoped gates passed;
independent source review0/0/0. Baseline coverage includes173 vendor paths.

Behavior RED: existing original RESET error/fact wrapper test compiled only
after the missing Target/C payload audit was implemented; its exact result is
known caller nodes2/fact1 with C0, or existing-hook unknown C nodes2/fact1 with
ordinary facts0. Original Box identity, terminal/window/tail and two rejected
reentries remain. Iterative borrow adds checked counts, not a second ledger;
TargetAndProgress visits the unique target once. Explicit conflict-chain8 and
empty-but-capacity/mutated/nonempty batch counterexamples pass; unsupported
carrying payloads return typed Uncovered without a partial successful result.

Actual checked Root failure ledger was a second missing-method compile RED,
then GREEN through original ReadyReceipt rejection and WrongActorClock carrying
input. Its returned error audit and Root aggregate agree across two rejected
reentries; explicit Invariant fault fails audit/zero and preserves the original
error for restoration. C-owned unknown and Root-owned returned payloads combine
without adding snapshot summaries. The Root tracer is not a naturally nested
error or unknown recovery test. Unsupported domains remain mandatory follow-up.

Feedback repairs were test-only: unwrap_err required Debug for an owning success
value, fixed by explicit match. Changing the fixture to same-clock exposed that
BoundInactive alone does not reject direct actor admission; restored deliberate
wrong clock with matching session/leg identity and exact WrongActorClock check.
No production behavior was altered for either defect. Provenance's expected
field-set RED was resolved by adding the actual typed field and distinguishing
the C and opaque-error methods by their exact impl owners, not relaxing counts.

Final exact lines: Target8498; C3007/cold332; owner1860/inventory220;
error_inventory280; continuation1355; returned_error464/transactions618;
provenance child205/parent2454; governance751. Two additional shrink ratchets
3009->3007 and1861->1860. Compared with Move baseline,10 old paths changed,
one new error child,404 unchanged/no missing. Reviewer independently verified
all415 hashes, original byte audits/old continuation bodies and exact ratchets;
Reviewer ran no tests and found no required optimization.

Final default AND all-features: C64/owner9/continuation17/CurrentOnly13/server25,
Knife190/repository20/growth7 and library check all green. Fresh strict rustc
standalone7; two exact behavior tracers each10 repeats in each mode (40 runs);
eleven scoped rustfmt checks, tracked diff and112 untracked-text whitespace
checks pass. Existing vendor22/lib7/libtest1/provenance-assembly149 warning
families remain. No global fmt/full-library/external/commit/push claim.

### 2026-09-17 — Recovery error cursors retain exact values through real FIFO pressure

Same Heisenberg SOURCE P0/P1/P2=0/0/0, locally accepted finite extension only.
Artifacts: `/tmp/mini-vpn-p3-nested-inventory.q0LWgl/{cursor,cursor-gates}.json`.
Reviewer independently checked416 hashes and inverse preservation of the old
fixture/C tests; no test execution by Reviewer. Three old paths changed, one
approved private test child added,412 unchanged; no production-parent/governance
edit. Exact lines: error_inventory510, real protocol leaf771, new diagnostic
leaf258, C returned_error503. All normal source limits remain satisfied.

The existing production-checked bootstrap/actual registration/three-OPEN/E1
fixture now has B FIFO3. Acceptance and two original OpenResults fill it;
the final third OpenResult returns actual AttachedRecoveryQueueBlocked. First
execution reached precisely Err(UncoveredDomain(Recovery)), not compile/fixture
failure. Minimal borrow visitor GREENed; numeric assertions prove node1/cursor1,
one original frame and exact checked wire length, pending capacity1/ready0,
one attached and one acceptance, no resume. Consuming the original returned
error/drain after actual acceptance receipt permits the same publication retry.
The original acceptance validation, three peer results and all backing/reuse
assertions remain; no extra fabricated queued frame or synthetic pressure hook.

Nested RecoveryPayloadInventory distinguishes cursor/resume/output/token counts,
two actual array capacities and their checked Layout bytes, and frame wire bytes.
Arrays exclude pointees/shared Arc/allocator overhead and are not aggregate peak.
Weak/shared references do not become additional actor/FIFO/endpoint/session owners;
there is no invented stored-byte counter or transport-correlation assertion.
Single resume errors count cursor0; actual empty/malformed cursor counts1.
Every pending/ready variant is visited, including receipt/attached/resume tokens
inside ready values. Original wrapper chains remain iterative. Unsupported inner
and queue causes fail the whole audit, not a partial successful inventory.

Four new diagnostic tests pass: empty retained capacity/nonzero cursor; nested
original frame with supported then uncovered scalar cause; actual E1 ordinary
resume placed in explicitly defensive pending/ready holders (not valid recovery
correlation); unsupported queue cause. Ten arithmetic axes individually reject
overflow and participate in nonzero detection/checked composition. Invalid-frame
and impossible-capacity Layout guards are fail-closed code, not fabricated live
failure witnesses. No new unsafe, clone, allocation during audit or raw getter.

Both default/all-features: Target171, C65, owner9, continuation17, CurrentOnly13,
server25, Knife190, repository20, growth7 and library check pass. Fresh strict
standalone governance7; actual registered-pressure tracer10/10 EACH mode;
four scoped rustfmt checks/tracked diff/113 untracked text whitespace checks
pass.22 gate commands and20 repeat commands, no post-GREEN repair. Existing
warning families unchanged. No global fmt/full-library/external/commit/push.

Full nested effects/input/command/reservation/status/terminal inventory, C normal
recovery, applicable natural unknown reachability, aggregate matrix4, combined
consumer review and five51B/P3 remain open. Continue under standing authority.

### 2026-09-17 — Complete current carrying-error audit, not normal C recovery

Same Heisenberg SOURCE P0/P1/P2=0/0/0 Approve, limited to joint51F. Reviewer
independently matched423 hashes:11 old paths changed/7new/405 unchanged. No
governance ratchet edit. Exact artifacts in
`/tmp/mini-vpn-p3-nested-inventory.q0LWgl/remaining{,-gates,-repeats}.json`.
Main owns all executed tests; Reviewer ran no Cargo or external operations.

Eight focused RED/GREEN steps cover actual original DATA refusal, effect batch,
Driver permit custody, received ATTACH, status/reservation, unexecuted recovery,
C/Root returned input and terminal cursor. Lower Session summary counts actual
payload views, String/Vec capacity/Layout, effects/overflow/mutation, segment
backing charges and codec wire lengths. Leg/TCP owner-local borrowed diagnostics
do not expose original resources; four permit counts use actual permit values.
The original Target error chain is iterative. Current carrying variants are
explicitly handled; checked count/Layout/invalid-frame failure rejects the whole
audit. No allocation, clone, resource transfer or DATA-path audit was added.

Tests preserve original holder/backing/bytes/receipt and retry assertions; all52
new nested axes compose into root checked sum/zero. Five terminal-tail holders
are explicitly defensive, not valid terminalization claims. The C recovery
return/retry test uses legacy admission only for its opaque returned error;
it does not establish normal checked-Bound recovery. Real Session reducer tests
prove receipt/transmit share normalized backing and original sink segment views
survive audit. Shared views, recovery-frame classification and token counts are
not additive unique heap, physical endpoints or aggregate peak.

Feedback: one test-only sum type annotation; one corrected pre-normalization
identity baseline, without removing bytes/shared-pointer/state checks. A wrong
module filter ran0 tests and was excluded; corrected returned-error filter ran6.
Knife first found missing child resolution, then nominal owner-scope absence
in its standalone crate. Explicit Leg path plus equivalent `super::super`
scope resolved both without test edits, new public Interface or line growth.

Final both modes: Target176, C66, owner9, continuation17, CurrentOnly13, server25,
Knife195 (five new lower tests), repository20, growth7, library check; lower
payload5/protocol11/TCP74/Supervisor48/Session47.32 final gate invocations green,
including strict standalone compile/run7. Four actual error/retry tracers each
10 repeats in each mode:80/80. Scoped18fmt/tracked diff/120 untracked text
whitespace pass; final423-hash audit has no drift. Existing vendor22/libtest1/
lib7 remain; standalone Knife warnings149->152 from new unused diagnostic ports,
without suppression. No global fmt/full-library/warning-clean claim.

Production exact lines: lower payload361/codec20/Leg child24/TCP child131;
Target error680/payload child435/C cold341. Oversized protocol1992/Leg2699/TCP2828
stay exact. Tests faults531/recovery295/diagnostic260+449/C632/Root631/index221.
All normal limits pass. No external, TUN/VPS/WAN/M2, commit or push.

Next mandatory dependency: normal checked-Bound C owning recovery/status/
high-water inventory through the same borrowed payload seams. Applicable
interruption/reachability, aggregate matrix4, combined consumer review and
five51B remain. Full P3/Case C/R7/Task4 are not complete.

### 2026-09-17 — Normal C recovery inventory rejects the actual false zero

Same Heisenberg SOURCE P0/P1/P2=0/0/0, Approve only joint51G. Independently
matched424 hashes (7old changed/1new/416unchanged), no deleted path. Reviewer
ran no tests. Module/Clean/System/DDIA9/10; no required repair. Main's artifacts:
`/tmp/mini-vpn-p3-nested-inventory.q0LWgl/normal-c{,-gates,-repeats}.json`.

First moved the original255-line checked FIFO-pressure test without changes:
SHA-256701ea21dc6ce9773d4eea4f5c115af6333242ca76eec14600fcdfff54014a74d,
parent771->516/new259. Baseline and moved exact1/1, component18, Knife195,
repository20/growth7/scoped checks pass before behavior. The first new assertion
then failed at actual C-held Publish: old ordinary/error audit returned zero.
Minimal borrowed inventory made this same real checked FIFO1/E1 test GREEN.

C now traverses six recovery/two status phases, high-water, exact terminal
quarantine and its three cause payloads, aborted pending/results/facts at the
actual holders. Target-owned inherent value walkers replace previous private
free walkers, preserving their error callers and lower nominal direction.
Standalone result arrays retain actual capacity/Layout without a fake cursor.
Normal payload is a plain value; typed outer RecoveryPayload failure feeds the
existing Root audit_failed/nonzero branch. Error-only payload stays separate.
No audit allocation, clone, recursion, resource move, new owner or DATA-path work.

Numeric assertions cover Initial/Execute/Publish/HandoffReady, original frame
wire length/capacity and complete C cold stability through three pressure
returns and rejected reexecute. Original real peer delivery remains once.
C-local defensive interrupted storage moves the original pending value once;
result backing and empty guard stay nonzero. Direct high-water/quarantine and
frame/backing identity cases are explicitly defensive, not runtime phase claims.
An attempted outer malformed Frame::new fixture hit E0624 before tests; repair
preserved protocol privacy, using legal original-frame identity, existing lower
invalid-frame tests and code-aware typed C outer-propagation guard instead.
No production visibility or unchecked constructor was added.

Final default/all-features: Target178/C67/owner9/continuation17/CurrentOnly13/
server25/Knife195/repository20/Cargo-growth7/library checks/lower payload5;
strict standalone compile/run7.24 gate commands pass. Three exact tracers each
10 repeats in both modes:60/60. Scoped8fmt/global tracked diff/121untracked text
whitespace pass; final424-hash recheck drift0. Existing vendor22/libtest1/lib7/
Knife152 warnings unchanged; no globalfmt/full-library/warning-clean claim.

Exact lines: C3007/cold396, Target error694/payloads467, provenance267,
test parent516/new501/C693. No ratchet/other production/external/commit/push.
51G closes normal C payload audit only. Remaining matrix3 applicability/
interruption review, aggregate matrix4, combined consumer and all five51B
packages remain mandatory. P3/Case C/R7/Task4 remain incomplete.

### 2026-09-17 — Checked fatal return closes current first-path applicability

Same Heisenberg SOURCE P0/P1/P2=0/0/0, Approve finite test SOURCE; prior missing
checked-Bound fatal-return evidence P2 closed. Reviewer independently matched
424 hashes, one changed test/423 unchanged, and inverse restoration of original
501-line successful fixture (SHA-256
f0e32dbcb7e059019cc6ebe66f2653a33d906a1c1c8a016c1bfc9fbd7f07c1a0).
No production/hook/API/new path/governance/external/commit/push change.

Existing controller_inventory.rs501->575 shares the synchronous original
fixture. New checked_bound_recovery_resume_exhaustion_retains_original_fatal_tail
uses the existing exhaustion seam after actual OPEN, before checked C. Actual
execute returns the exact Target resume-ID exhaustion; Executor keeps one
original output prefix, one effect suffix, cursor and attached authority.
C Failed retains its recovery marker, no C-owned normal/error payload duplicate;
caller owns cause. FIFO still contains original acceptance. Two execute/publish
reentries return Failed and preserve complete C snapshot/cold/FIFO.

Direct GREEN; both default/all-feature Target179/C67/owner9/continuation17/
CurrentOnly13/server25/Knife195/repository20/growth7/library checks; strict
standalone compile/run7, scoped fmt and tracked diff:24 closure commands green.
Original success and new failure each10 repetitions per mode:40/40. Existing
warning categories unchanged; no globalfmt/full-library/warning-clean claim.
Artifacts /tmp/mini-vpn-p3-nested-inventory.q0LWgl/fatal{,-repeats}.json own
full command output, snapshot, original inverse hash and independent review.

Current first-path matrix3 applicable obligations are now covered. Synchronous
replacement LegActivated/Transmit has no identified natural callback/await
unknown seam: SOURCE N/A-now, not runtime unknown PASS. Existing defensive
injection, Legacy B-only post-bind and prototype scheduler evidence remain
separate. New51B producers/callbacks reopen applicability; P4–7 parent lifecycle,
whole-root shutdown/zero, aggregate matrix4/P1 and all five51B remain open.

### 2026-09-17 — Original TCP sender descriptors reserve before commit

Same Heisenberg SOURCE P0/P1/P2=0/0/0, Approve only finite joint51I. Independent
review covers frozen426 hashes/eight paths;6 old changed/2 new/418 unchanged.
Reviewer ran no tests. Artifacts:
/tmp/mini-vpn-p3-descriptors.FVL93c/{baseline,source,gates,repeats}.json.

Pre-write strict standalone7/7. First original append with max_segments1
failed exactly capacity4 versus1. Shared allocation extraction first ran
original storage25/25. Minimum GREEN uses prepared Reuse/Replace on the same
exclusive sender borrow. Original accounting validation precedes descriptor
allocation; clipped actual growth and old+new Layout are checked before new
backing. Original window remains untouched through preparation Drop or later
effect refusal. Commit moves original descriptors into exact prepared capacity,
same-type Vec->VecDeque without allocation, publishes original accounting,
then preserves original Bytes Drop order. No new unsafe site or getter.
Shared primitive rechecks safe Layout entry and does not consume effect TLS;
old effect Layout/zero/ZST/fault priority stays unchanged. Two new value-only
TCP errors are handled explicitly by the original exhaustive payload audit.

Private actual tests cover1/2/4/5 growth, wrapped original deque and exact
payload identities, replacement pointer reuse, real old+new Layout, ACK retained
capacity and allocation-free reuse; nonempty guard Drop/effect refusal, descriptor
Reserve and old validation order. Overflow/corrupt-accounting tests are defensive,
not enormous real-allocation claims. Existing real Model first DATA then second
growth/effect Reserve preserves TCP/global state; retry yields exact offset3 DATA.
Unchanged real Executor effect-reuse fixture passes without altered semantics.

Both modes sender5/storage26/TCP32/Session47/Supervisor48/Target179/C67/owner9/
continuation17/CurrentOnly13/server25/Knife201/repository20/growth7/library check;
strict standalone7/scoped8fmt/diff:34 final commands green. Real Model and
unchanged Executor cases each10 per mode:40/40. SOURCE recheck426 drift0.
Knife195->201 reflects embedded original resumable new unit tests, not changed
provenance assertions. Existing warnings remain; no globalfmt/full-library claim.

Exact lines: tcp.rs1995 net-zero, send397, effect_storage270, cause audit469,
new tests186/index1, existing capacity440/model665. No governance mutation,
external/TUN/VPS/commit/push. Receive/map/Target backing, phased aggregate peak,
call charges/combined consumer and all five51B remain mandatory; P3 is incomplete.

### 2026-09-17 — Remaining map proof requires explicit storage representation

One bounded whole-map review, same Heisenberg P0/P1/P2=0/1/0 retaining the
original aggregate gap; no new memory-safety/OOM finding. No Rust/test/Cargo
change after accepted51I. BTreeMap public element bounds do not reveal actual
node Layout/preparation; neither header size nor guessed private std constants
can close P1. MemoryTarget's drained logical bytes omit per-flow retained
capacity. Source operation map is consolidated in joint51J, not a new sidecar.

Selected next complete transaction: original TCP receive range storage plus
overlap/uncovered/normalized preparation, accept/partial-key/abandon, contiguous
Offer descriptors and affected Session receive/accept/OPEN preparation. A
scratch-only patch would leave insertion and Offer allocation at commit.
Preserve ordered complexity: no implicit sorted-Vec or moving-arena relaxation.
Main selects nonmoving original-owner ordered nodes/recycled slots; same Reviewer
is checking exact feasibility and write scope, no code is authorized by this
draft. Node allocation calls/slack/free custody and allocation-free bounded
iteration still need proof; no global allocator/new unsafe/callback shortcut.

Main read installed rustc1.95 public allocation guarantees: alloc/src/vec/mod.rs
394–398 allows unchanged Vec->Box<[T]> when len==capacity; boxed/convert.rs
255–275 permits in-place checked Box<[T]>->Box<[T;N]> with original error return.
This supports the existing exact one-node Vec primitive candidate without a
second unsafe site. It is not evidence of implemented or tested ordered storage.
Main also traced all three production peek calls: effects.rs726 exhaustion
length,945 actual Offer, session.rs2136 OPEN precheck; preparation must preserve
those error/custody contracts rather than adding postcommit Reserve.

Architecture/refactoring skills drove this whole-transaction selection. Remaining
design scores from Reviewer: Clean8/System6/DDIA7; reach closure through node
representation, bounded growth and real complete preparation tests. Aggregate
P1/matrix4/P3 remain open; current source remains426 hashes. No external/git work.

### 2026-09-17 — Complete receive design exit and preparatory Move accepted

Same Heisenberg selects concrete u64/OwnedPayload safe owning-node AVL, plus
the complete Session Offer preparation transaction. SOURCE0/1/0 retains the
unimplemented receive/aggregate P1; allocation conversion itself is valid.
With L live/F free/A=L+F/U new ranges, prepare max(0,U-F) missing nodes and
check actual peak max(A,L+U). No old free-chain extraction before commit;
partial accept reuses its node. Rotations move links, fixed iterator stack
must respect proven height, free/prepared chains require iterative release.
Keep both original input Bytes owners through Session accounting and actual
Offer publication before custom Drop. Public peek keeps its Vec/allocator-abort
contract; production queries borrow, owning Offers prepare exact backing first.
Joint51J records full six-production-path and test-tree scope. No node-only
implementation, new unsafe or equal-performance claim is approved.

Executed only preparatory behavior-preserving Move: whole preview_receive docs
and body195 lines from tcp.rs to existing receive.rs inherent impl. Block hash
0e58936587cc6a8c3b00137b072411b2751acde6eee980333f6d59ad450ed33d retained;
parent1995->1800, child408->603. Original pub(crate), validation/error/allocation/
ownership order and caller/route unchanged. Parent/child inverse hashes both
match51I. Writer stopped at exact standalone6/7 shrink-only boundary; Main as
Integrator changed only the one debt entry1995->1800, inverse hash also exact.

Prewrite standalone7/7; exact bridge before/after1/1. Both modes TCP32/storage26/
Session47/Target179/C67/Knife201/repository20/growth7/check plus scopedfmt/diff:
20 final commands green. Strict standalone compile/run7/7. Final independent
SOURCE0/0/0 approves only Move:426 hashes,3changed/423unchanged, no added/deleted
path. Reviewer ran no Cargo/tests/writes. Existing warning categories unchanged.
Artifacts /tmp/mini-vpn-p3-descriptors.FVL93c/receiver-move{,-gates}.json own
commands, exact snapshots/inverses/shrink and review. No external/commit/push.

Next: freeze complete Model gap/bridge/Offer/partial/reuse and fault phases,
then implement51J as one complete transaction under ongoing user P3 authority.
No tree/Offer behavior has been implemented. Receive/aggregateP1/matrix4/P3,
combined consumer and five51B remain open; no fresh permission pause required.

### 2026-09-17 — Complete receiver/Session Offer locally accepted after SOURCE repair

Same Heisenberg final SOURCE0/0/0 Approve, limited to this receiver transaction.
`/tmp/mini-vpn-p3-receive.T55xyv/closure-{source,gates,repeats}.json`
owns430 hashes/12changed paths/418 unchanged,31 post-repair green commands and40 module
repeats (new Model7/storage4,10 each per feature,220 test executions).
Original source/sequence/gates/repeats retain pre-review evidence separately.
review.json records sole P1 and closure: sufficient Vec capacity does not give
bulk extend a stable no-reallocation guarantee. Replace only its three lines
with per-item push; receive.rs stays819, inverse SHA matches original review,
other429 hashes unchanged. Installed public Vec docs373–378 support the fix.
All31 gates/40 repeats were actually rerun after repair; no borrowed GREEN.
Reviewer ran no tests and reported no remaining P0/P1/P2 in the bounded closure.
Initial SOURCE scores were Architecture8/Clean9/System8; final repair closure
did not issue a new numeric rating and is not a full-project10/10 claim.

Real Model bridge/node/Offer/reservation tracers first fail only at their named
missing preparation/error/fault seams, then GREEN. Gaps2:cd/6:gh followed by
0:abcdefgh publish original four-segment Offer; accept1/reoffer7/full accept
and warm reuse succeed. Private safe owning-node AVL replaces only the original
receive ranges. Preparation owns new missing nodes; old tree/free stay intact;
native keys/rotations/partial rekey do not relocate allocations. Requested
peak=max(live+free,live+added) checks real Node Layout. Shared original exact Vec
allocator and safe one-node Box conversion add no unsafe/global allocator.

Original logical validation/global budget/IDs precede newly recoverable arrays.
Actual projected Offer segments, normalized ranges, nodes and effect storage
prepare before receive/accept/both OPEN-result mutation. Old Owner duplicate
replay and Client duplicate no-op differ as before. Checked whole global usage
is prepared before mutation and assigned once; obsolete single-call per-field
mutators are removed. Both original input Bytes owners stay held until TCP,
Session accounting and actual original batch publication. An externally-owned
reduce_into batch retains its Offer during input Drop unwind. No claim for the
public compatibility wrapper's own destruction or whole-root lifecycle.

Private tests audit rotations/height/order/address/live-free partition; partial
node/slice identity; mixed old free/new preparation and intermediate allocation
refusal;8192 prepared nodes released on128KiB stack. Same Reviewer caught O(NlogN)
clear; actual visit count1410 versus256 RED, then one destructive traversal256
GREEN. New Model tests preserve OPEN-result state on Offer refusal, actual
global129/128-byte and33/32-range errors ahead of allocator fault, and original
ACK prefix/full Offer on defensive journal overflow. get() observes only normal
backing; its initial test assertion was corrected to iter() to include overflow.
Two other feedback-only compile errors used existing finish() and one reference
dereference; no production Interface was widened for diagnostics.

Both modes TCP36/storage33/Session47/Supervisor48/Target179/C67/owner9/
continuation17/CurrentOnly13/server25/Knife212/repository20/growth7/check;
strict standalone7,12-file scopedfmt/diff/untracked checks:31 commands GREEN.
Knife count201->212 includes the11 embedded original new unit tests, not changed
provenance assertions.430 hash drift0. Existing vendor22/lib7/lib-test1 warning
categories remain; no globalfmt or full-library-all-tests claim.

Exact lines: tcp1770/receive819/new storage386/Session5585/effects1242;
Model parent666/new leaf521,capacity440,cause audit471,new storage index1/leaf191,
governance751. Main Integrator lowers only tcp1800->1770 and Session5613->5585.
No other maps, external/TUN/VPS/commit/push. Aggregate matrix4 physical peak,
remaining map/Target ownership, combined consumer and all five51B/P3 remain open.
Next executable boundary: original Session flow/tombstone OPEN/terminal
preparation and ownership, symbol-first; no blanket map rewrite or new grant pause.

### 2026-09-17 — Session flow/terminal storage locally accepted

51K is implemented within9 Rust paths; same Heisenberg independent SOURCE
0/0/0 Approve, local Module/Clean/System/DDIA9/9/9/9. Pure Move has0/0/0
Approve:432 hashes/6paths/426 unchanged, six original block hashes and both
whole parent inverses exact. PreparedLocalOpen nominal anchor/recovery/validation
stay parent; existing complete transaction methods restore only Session scope.
Twelve Move gates pass. True retire3-before1 still recovers terminal1,terminal3,
live2; the initial oracle's extra empty-flow ACK was corrected against source.

Final candidate /tmp/mini-vpn-p3-session.xmyomq/{source,sequence,gates,repeats}.json
owns434 hashes/9paths/425 unchanged,31 green gates and40 repeats/160 executions.
review.json owns closure and caller scope. Initial320 was a doubled tally;
actual40 command summaries are6*10*2 +2*10*2=160 passed test executions.
Nonmoving native-key AVL has distinct live/terminal roots plus original vacant
pool. OPEN prepares only a newly missing node before Adapter; old free pool
is untouched through refusal/drop. Real Node Layout and checked F+Z/current
peak precede node allocation; no dense cold allocation, generic map, new unsafe,
or new DATA indirection. Finish moves that key's actual Box, not successor
payload, and retains original FlowState until counters/tombstone/original
FlowFinished publication (including inline overflow). Terminal expiry recycles
that node; Session expiry detaches roots/pool, publishes Expired/effect then Drop.
Terminal/vacant nodes pay the larger live-sized header; other pointees/aggregate
peak remain distinct, not absorbed into this node count.

First named storage RED was compile-only missing allocation arm/typed Reserve.
Minimum implementation first hit two missing EffectStorageError-to-SessionError
conversions, fixed with the original From mapping; focused then1/1. Mechanical
field/query formatting temporarily grew parent5380->5395; no ratchet was raised.
At focusedGREEN, the original31-line snapshot/flow_snapshot family moved byte
exact to the same lifecycle impl, before/after exact1/1; parent now5364. Future
Move envelopes must allow actual rustfmt/query growth before writing behavior.

Six real Model tests cover original Target backing on Reserve/Adapter rejection,
retry IDs, Adapter unwind with original vacant reuse, peer duplicates/terminal
replay/high-water, independent F/Z refusal and checked F+Z overflow, terminal/
Session effect-before-test-disposal and ordered recovery. Test-only FlowState
destructor injection is explicitly not a natural normalized production callback
or a proof of public Vec-wrapper/whole-root destruction safety. Two storage
tests cover256 keys/three orders, balance/full original addresses/two-child-node
moves and reused allocation, and8192 live/free nodes dropped on128KiB stack.

Both modes TCP36/storage39/Session49/Supervisor48/Target179/C67/owner9/cont17/
CurrentOnly13/server25/Knife220/repo20/growth7/check; strict standalone7 and
scoped9fmt/diff/untracked whitespace checks pass. The40 module repeats run six
Model tests plus two storage tests ten times per feature mode. No globalfmt,
full-library-all-tests or warning-clean claim; old warning categories remain.
Current exact lines: Session5364/lifecycle859/effects1005/Model index667/leaf380/
storage index1/leaf168/cause audit473/governance751. Integrator alone lowered
5380->5364. No other map, external/TUN/VPS/commit/push. Reviewer independently
confirmed434 hashes and31-line query Move; ran no tests. New FlowStorage errors
stay Supervisor Reducer; uncommitted empty original batch may return Idle, but
outer Peer OPEN input is consumed, not carrying-retried. No postcommit flow/ID/
Target ownership loss requires a synchronous caller expansion in this packet;
ATTACH recovery does not call node preparation. Retain outer OPEN/retry51B duties.
Matrix4 aggregate, combined consumer, five51B/fullP3/CaseC/R7/Task4 all remain open.
Next: original Supervisor replay/receipt/ACK-release complete owning transaction.

## 2026-09-17 — Supervisor transaction preparation: M1/M2 pure Moves accepted

Same Heisenberg independently approves each SOURCE boundary, P0/P1/P2=0/0/0.
M1 moves seven original DATA/quarantine/ACK tests to a private default child;
only four-space unindent changes the487-line block. Supervisor5409->4924,
test493,435 hashes/3paths/432 unchanged. Whole-parent and governance inverses
match the prior434 baseline.17 actual commands are green: pre strict7/Sup48,
both-mode moved7/Sup48, post strict7, both-mode growth7/Knife220/repository20/
check and scopedfmt/diff. Runner's substring0 false-matched220; the project
command remained exit0. Numeric result parsing resumed only unrun gates.

M2 moves whole snapshot/test-correlation/receipt-consumption families into
the existing driver_input implementation and the complete TCP replay-permit
impl into private tcp/uplink_replay.rs. Nominal types/fields and callable
operations remain unchanged. The first compile returned E0624 twice because
pub(super) now named TCP instead of owned_upstream; only its original effective
scope was restored, with rustfmt wrapping. There was no behavior-test failure.
Four original block hashes and whole-file inverses match after that one exact
scope normalization and Driver's first-line documentation inverse. Governance
inverse is exact, with only4924->4849 and2828->2766 changes.

M2 final436 hashes/6paths/430 unchanged;21 successful commands: pre TCP74 and
real Owner retention8; both modes Supervisor48/TCP74/OwnerTarget179/Controller67;
strict standalone7, both-mode growth7/Knife220/repository20/check, six-file
scopedfmt/diff. Earlier compile failure is explicitly retained in the gate
artifact. Main ran these commands; Reviewer ran no Cargo/tests/formatting.
Final lines: Supervisor4849, driver633, observation264, TCP2766, new TCP72,
moved test493, governance751. All scoped whitespace/final-newline checks and
436-source drift audit pass. No globalfmt or full-library-suite claim.

/tmp/mini-vpn-p3-supervisor.73tG3G/{move,implementation-move}-{plan,source,gates,review}.json
owns these two closed boundaries; design-review.json records only M3's
candidate-scratch design approval. Existing vendor/library warning categories
remain. No storage bound, allocation failure, ACK scratch or expiry behavior
was implemented in M1/M2, and no external/TUN/WAN/commit/push occurred.

The next executable packet is joint51L M3: actual Owner DATA node/scratch
preparation refusal, then complete K_r+1 admission, retained prefix/permit
release/terminal/expiry/reuse. Original Legacy/Client outer health policy is
not moved; only inputs reaching storage admission get its exact-return promise.
Design exit is selected, not a new sidecar prerequisite. Aggregate matrix4,
combined consumer, all five51B and full P3/CaseC/R7/Task4 remain incomplete.

## 2026-09-17 — Supervisor original DATA storage and receipt transaction accepted

Joint51L M3 is independently accepted by the same Heisenberg, final P0/P1/P2
0/0/0, Module/Clean/System/DDIA9/9/9/9. This replaces the original Supervisor
replay map/quarantine backing with native owning AVL nodes and a prepared
candidate Vec at the same DATA/receipt authority. K_r+1 is explicit admission
policy, not a natural one-quarantine proof. Missing node plus geometric scratch
replacement are prepared together before DATA is split; Capacity/Layout/Reserve
returns original input without changing the Model, old free pool or prefix.
Owner admitted custody and original Legacy/Client outer health policy remain
distinct. Legacy reuses its original effects Vec. No audit-only hot-path work.

ACK visits in native order while borrowing original nodes, keeps its completed
scalar prefix and removes only after full success. Terminal precollects then
releases/removes; expiry releases every original owner before linear recycle.
Real second-permit wake leaves current/suffix/nodes/prefix retained; later
poisoned scratch refusal and growth preserve that prefix. Cold values separate
actual four permits, retained metadata, node allocations and empty-but-capacious
scratch. Shared Bytes views are not additive unique physical memory.

Twelve new tests cover original input refusal/retry, two quarantines plus
third-input carrying Capacity, Layout defense, node Reserve, reusable backing,
partial ACK, real ACK/terminal/expiry wake interruption,128-node original-address
recycle, three rounds of128 native removals/reinsertions, and20,000 vacant nodes
on a64KiB stack. Independent factory ledgers and damaged cached-count Layout
are explicit defensive fixtures; they are not natural Bound/Root acceptance.
Expected compile REDs were missing storage error/hook, cold audit, node hook.
The capacity fixture initially forked after OPEN's monotonic gate; the terminal
fixture initially caught RESET publication instead of permit release. Repairs
only changed test ordering. One independent expiry test was queued before
inspecting that terminal red; all final gates were subsequently restarted.
No production failure or strict uninterrupted-first-green sequence is invented.

Final439 source hashes:8 original paths changed,3 new,428 unchanged. Exact
lines: Supervisor4834 (4849 ratcheted down), driver1169, TCP replay84/cold159,
error audit709, old replay tests487, Owner test parent686, new leaves621/398,
new test index1, governance751. Old Owner parent inverse, duplicate-key test
inverse and governance inverse independently match. No outside source drift.

Main executed21 final green gate commands: default/all-feature Supervisor54,
TCP74, OwnerTarget185, Controller67, growth7, Knife220, repository20 and lib
check; strict standalone compile/run7,11-file scopedfmt/diff/text and439-hash
check.40 module repeats at6 tests each total240 actual executions, all green.
Existing22 smoltcp warnings and existing physical-root private_interfaces
warning persist. No globalfmt or full-library-suite claim. Reviewer ran no Cargo,
tests or writes; they independently reviewed all12 bodies and exact artifacts.

/tmp/mini-vpn-p3-supervisor.73tG3G/behavior-{source,sequence,gates,repeats,source-review,review}.json
owns the final evidence. M1/M2 move-* artifacts remain historical prerequisites.
No external/TUN/VPS/WAN/commit/push. This closes only the Supervisor component:
matrix4 Target/progress/aggregate, combined consumer and all five51B remain
mandatory for full P3. Next is actual Target retained-backing characterization
and its original-owner storage discipline, not a generic map-only substitution.

## 2026-09-17 — Target retained-backing characterization and complete impl Move accepted

Full P3/Case C/R7/Task4 remain incomplete. This packet executes joint51M's
preparation, not its remaining Target storage behavior. Same Heisenberg
SOURCE reviews each returned P0/P1/P2=0/0/0 Approve; Reviewer ran no tests,
Cargo, writes or external commands. Source-only approval is distinguished
from Main's executed gates below.

First, one private leaf uses actual MemoryTarget F2/P8/G8, successive eight-byte
write/drain on A then B, cold inventory and reset/join/retire. It directly GREENs:
logical stored/summed bytes0 coexist with retained deque capacity>=16. Returned
Bytes have different pointers from their still-allocated original deque backing,
retain exact content and survive Target cleanup. This neither calls the logical
counter wrong nor claims unique whole-root memory or readiness backing zero.
No production behavior, visibility, error or governance edit. Initial parent
1497->1498 plus leaf58;440 hashes,2 changed paths,438 unchanged. Exact baseline
10 and new exact1 pass. characterization-{source,gates}.json records19 actual
command executions including baselines: both Target11/OwnerTarget185/Knife220/
repository20/growth7 and lib checks; scoped fmt/diff and strict standalone7.

Then Move the complete208-line inherent impl MemoryTarget to existing private
memory_io.rs: new/snapshot/directive/terminalize/feed/finish/drain. Six methods
keep pub(crate); terminalize stays private with its only two callers already
child-local. Nominal storage/config/types, callers, default private route and
tests stay unchanged. No wrapper, re-export, visibility widening or new Module.
Block SHA-256 bf41a65819f2ae66402e7f634eca684036c5bf61756a394df00b52846bc8cb32;
whole-parent and whole-child inverse hashes both match (one separate child
ownership-doc line is normalized). Parent1289/child918; leaf58 unchanged.
No ratchet edit because parent was already below its normal1500 ceiling.
440 hashes,2 changed/438 unchanged against characterization. Across the full
packet,2 old source paths+1 new test changed versus the accepted439 baseline.

Main's16 post-Move green gates: scoped rustfmt; default/all-feature Target11,
OwnerTarget185, growth7, repository20, Knife220, cargo check--lib; targeted
diff; standalone rustc-D warnings compile and run7.20 additional exact repeats
(10 each feature mode) all1/1. Hash/inverse/whitespace checks remain green.
Warning headings match the previous accepted baseline exactly: existing
vendored22 plus existing non-vendored irrefutable-pattern/private_interfaces
families; do not claim all warnings are vendored. No globalfmt/full-library
suite, WAN/TUN/VPS/M2, commit or push was run.

Artifacts: /tmp/mini-vpn-p3-target.z6G5fA/characterization-{source,gates}.json,
move-{plan,source,gates,review}.json and repeats.json. No unexpected Rust RED
or behavior repair occurred. A read-only evidence accessor used .map on an
object and two guessed inspection paths were absent; neither touched source
or counted as a Rust failure. Checks resumed only after correcting inspection.

Source audit and review identify next concrete work: write consumes a directive
before uncontrolled extend; feed/directive growth is likewise unprepared;
read/take_written drain into local output then run controlled Waker publication;
OPEN clones/inserts and join removes live before joined insertion. Complete
original-owner storage preparation, output custody across unwind, actual
requested backing/addresses and coupled readiness mapping remain to implement.
These are source findings, not a claimed executed panic/allocator failure.
Progress ordered scans, aggregate phase peak/call charges and all five51B are
still mandatory. Preparation is accepted; no Target/P3 closure is inferred.

## 2026-09-17 — Original Target output-return custody transaction accepted

Joint51M's limited return transaction is implemented and accepted. Same
Heisenberg independent SOURCE P0/P1/P2=0/0/0 Approve, no blocking finding;
Reviewer did not write source or run Cargo/tests. Main executed evidence below.
Full Target retained backing/maps/addresses, aggregate matrix4 and full P3/
Case C/R7/Task4 remain incomplete. No external/TUN/VPS/WAN/M2/commit/push.

First real test fed8 readable bytes, registered the actual common Waker and
caught its specified notification panic. Original code compiled and failed
at cold summed0 versus expected8: queue drain's local Bytes had been dropped
before return. The five-file minimum GREEN adds Idle|Publishing{direction,
Bytes} to the original flow, not a token, extra queue or global owner. Exact
backing preparation, counter/readiness checks and Bytes conversion precede
drain. Pending stays charged in original per-flow/global bytes; cold scans
queues+pending once. Only successful publish allows delivered counting and
the final infallible output move. Same-flow ordinary entry refuses replacement;
other flows may use remaining G. No production catch_unwind or new unsafe site.

The allocator is the existing exact primitive with separate Target fault
domain. Each u8 push is capacity-proven. Pinned bytes1.11.1 uses boxed-slice
ownership for exact len=capacity, without a Shared allocation; conversion is
still precommit. Checked source-capacity+copy overlap is local arithmetic,
not full Target/Root peak or allocator RSS. Original growth/map/address work
is not silently treated as covered by this output-copy preparation.

Five new behavior tests plus the old backing test cover real entry points:

- Read full output remains concrete across notification unwind; delivered0,
  same-flow refusal, unrelated B I/O, exact reset read8/written0 and join/retire.
- Written count3 and8 retain prefix/residual, including pending-only8 with
  empty queues. Graceful reset/cancel stays AlreadyTerminal/nondiscarding.
  Original expiry releases written8, creates a new shared pass and wakes B's
  actual waiting writer; B then writes. Join before expiry is nonzero.
- Allocation refusal in each direction preserves both queue pointers,
  capacities/content, cold counts and snapshot except original read attempt.
  Exact retry returns the original prefix and cleanup releases the remainder.
- Empty return avoids allocation; Zero/Unknown/counter priorities precede
  Reserve. read_calls retains its original policy; successful delivered3 only
  follows actual return. No production counter semantics are loosened.
- Partial Read keeps the exact Bytes address/content through three rejected
  entries and cancelled_flows overflow. Successful cancel releases read6,
  written2 and directives; joined/retired cold state is quiescent.

This last test initially expected AlreadyTerminal(Cancelled), but original
cancel deliberately returns AlreadyCancelled. All preceding retention checks
passed. Read-only branch audit established a test-only mistake; only that
assertion changed and exact rerun passed. Its next fmt check requested a
single-line assertion layout; rustfmt changed only the test leaf. Both are
retained in output-sequence.json; no random production follow-up repair.

Final440 Rust hashes:five paths changed/435 unchanged, no new source path or
governance edit. Exact lines: target.rs1370, memory_io.rs980, memory_ready.rs534,
owner_target/error_inventory.rs712, tests/retained_backing.rs470. The cause
audit adds only exhaustive value-only error cases. Readiness Terminal now
counts pending output when deciding shared-capacity release. No new exported
API, getter, callback, worker or permission exception. Source remained frozen
during all gates/repeats/review and final rehash.

Main's20 final commands passed: default/all-feature Target16, OwnerTarget185,
Supervisor54, Controller67, Knife220, repository20, growth7 and lib checks;
five-file scopedfmt/diff and strict standalone compile/run7.20 further module
repeats (10 EACH mode,6 tests per run) total120 actual executions, all green.
Warning headings exactly match the accepted Move baseline:22 vendored smoltcp
and existing non-vendored irrefutable-pattern/private_interfaces warnings
(lib7, lib-test1). No globalfmt or full-library-suite claim. The file/text
audit includes untracked source; outside440-path hash drift is zero.

/tmp/mini-vpn-p3-target.z6G5fA/output-{source,sequence,gates,repeats,review}.json
owns exact source, failure sequence, commands and independent verdict.
The earlier characterization/Move artifacts remain their own accepted history.
Per code-review, no further repair is required for this limited transaction.
Main's local Module/Clean/System/DDIA assessment is9/9/9/9; full Target10 needs
prepared original write/feed/directive growth, actual flow/address/node Layout,
live->joined ownership and a composed backing peak with deterministic tests.
Those and progress/aggregate plus all five51B remain mandatory, not optional
optimizations or a reason to redefine P3 as a prototype.

## 2026-09-17 — Three original Target buffer growth transactions accepted

Same Heisenberg final SOURCE P0/P1/P2=0/0/0 Approve; local Module/Clean/System/
DDIA scores9/9/9/9. This follows the accepted output-return transaction above.
Only original write/feed_read/push_write_directive buffer growth is accepted.
Target maps/addresses, checked retained/returned backing aggregate, matrix4 P1,
full P3/Case C/R7/Task4 remain open. No external/TUN/VPS/WAN/M2/commit/push.

The design's sole P1 was the existing owned Bytes iterator in feed_read. Copy
candidates alone do not prevent user input Drop. Same three-file scope was
retained and the condition added before TDD: borrow input during copy; keep it
until queue/count/high-water/readiness publication commit; then destroy it.
Do not pretend feed's Err carries the input, or that it survives its own Drop.

Implementation prepares a private T:Copy replacement from the borrowed original
deque after all old count/completion/readiness checks. Reuse returns before
scan, allocation or BUFFER fault consumption. Shared next_capacity/growth_peak/
allocate_exact_vec provide exact checked backing; same-T Vec->VecDeque does not
reallocate. First capacity=min(8,maximum), checked doubling clamps at maximum.
Byte directions use M=min(P,G); directives use D. Commit installs a prepared
candidate and only uses capacity-proven single pushes; AcceptAtMost is not
popped until preparation succeeds. No new unsafe, public Interface, helper
layer or effect/output fault-domain change. Two nested value-only cause mappings
remain independent; Reviewer explicitly found no need for a new abstraction.

Four focused tests followed vertical steps:

- Write's first RED was only E0425 for the missing BUFFER test hook, not an
  executed ownership failure. GREEN actually exercises wrapped8 backing,
  Reserve preserving both slice pointers/content/snapshot/directive, retry
  clamping to9/16, exact drain, and A reuse leaving the fault for real B growth.
- Real Bytes::from_owner Drop panic produced the specified runtime RED,
  StoredMismatch buffered_bytes summed4/stored0. Borrowed copy/post-publication
  Drop GREEN checks actual4 bytes, high-water, real readback and one destruction.
- Directive's real runtime RED accepted a ninth item instead of Reserve.
  Prepared growth GREEN keeps wrapped FIFO and exact9 backing, consumes the
  original WouldBlock/Zero/AcceptAtMost sequence, and proves reuse through a
  later real read-buffer refusal before cleanup.
- Feed refusal directly GREENs against the prepared implementation: two original
  slice pointers/content/cold/snapshot unchanged, caller explicitly keeps a
  cloned input for retry, growth clamps9 and real readback preserves FIFO.
  Reuse leaves the BUFFER fault for the opposite direction, then exact reset.

No unexpected Rust red or second repair occurred in this packet. Each focused
command completed before the next write/test. The accepted output helper and
its fault domain are unchanged. Independent review confirms removing the four
new test bodies restores the original470-line leaf byte-exact.

Final440 source hashes:three changed/437 unchanged, no new path or governance
edit. target.rs1370 only generalizes two Storage error display strings;
memory_io.rs1074; retained_backing.rs732. Strict source/text and final rehash
show no drift. Values like2FM/FD*sizeof(directive) are algebraic upper bounds
for later composition, not an already-checked whole Target/Root witness.

Main ran20 final green commands: both-mode Target20, OwnerTarget185, Supervisor54,
Controller67, Knife220, repository20, growth7 and lib checks; three-file scoped
fmt/diff plus strict standalone compile/run7.20 additional module repeats,
10 each mode with10 tests per run, total200 actual executions all pass.
Both libcheck warning-heading sets equal the accepted output baseline:22
vendored smoltcp plus existing non-vendored warnings (lib7/libtest1). No global
fmt or full-library-suite claim. Reviewer verified artifacts, not rerun tests.

/tmp/mini-vpn-p3-target.z6G5fA/buffer-{baseline,source,sequence,gates,repeats,
source-review,review}.json owns exact evidence; buffer-design-review.json
records the original P1 and its condition. Earlier output-* remains separate.
Next mandatory work is original flow/tombstone/address preparation and actual
retained backing Layout, preserving readiness's concrete bidirectional map
check and monotonic admission policy. Shared TargetAddr accepts arbitrary
String whereas the protocol validates253 bytes; that observation is not yet
a proof for every Target entry. Do not invent a shared-type restriction or
guess standard-map internal allocation. Progress/aggregate and all five51B
remain unchanged obligations; this is not complete Target or P3 acceptance.

## 2026-09-17 — Original Target address preparation and transfer accepted

Same Heisenberg final SOURCE P0/P1/P2=0/0/0 Approve; local Module/Clean/System/
DDIA9/9/9/9. Only address allocation preparation/refusal and original address
transfer are accepted. BTreeMap owning nodes, complete retained/address/return
peak, progress/aggregate and all five51B remain mandatory; P3 remains incomplete.
No external/TUN/VPS/WAN/M2/commit/push or governance change.

The original OPEN calls private prepare_address after opened-count and ready
preflight, before highest-flow-ID/map mutation. IP copies by value. Domain
checks the actual caller capacity plus new length with the existing Layout
primitive, prepares exact requested backing, copies bytes with proven capacity,
then performs checked String::from_utf8 before commit. The independent ADDRESS
test fault never shares BUFFER/OUTPUT/effect state. Empty addresses allocate
nothing and leave the fault untouched. No shared-type validation, new unsafe,
getter, runtime dispatch or global allocator hook. Raw long/Unicode/empty/
scoped-IP/port0 semantics remain unchanged. Node allocation is still the old
BTreeMap operation: this does not claim complete fallible OPEN is closed.

The real caller proof is Session effects::peer_frame -> validate_record ->
receive_open -> PeerOpenRequested -> observed_turn::Target.open. Protocol's253
limit holds there, not for arbitrary raw/preloaded MemoryTarget addresses.
The future aggregate must account for that distinction instead of assuming
every Target address is already normalized by the protocol.

Three tests each executed through original Target operations:

- First compile RED was only E0425 for the missing ADDRESS hook, not a runtime
  ownership bug. GREEN injects Reserve into real OPEN; caller4096-cap Unicode
  pointer/content/capacity and cold inventory/snapshot (apart from attempt)
  stay unchanged. Same-ID retry owns exact-length backing; real join keeps that
  same allocation, duplicate terminal OPEN does not recopy, retire removes it,
  and unrelated-flow read plus original write/drain work normally.
- Raw scoped IPv6/port0 and empty String with4096 caller capacity succeed
  without consuming the allocation fault. A real900-byte Unicode domain then
  receives Reserve, retries without ID loss and retains exact900 backing.
- Duplicate/conflict/stale/full precede allocation; full still consumes ID
  even with opened counter atMAX. Joined duplicates, counter overflow and a
  deliberately inconsistent readiness fixture precede Reserve, followed by
  exact retry. The fixture is defensive error-priority evidence only.

No unexpected Rust red/second repair. Each test finished before the next write.
Three-file scoped formatting preceded final gates.441 hashes:two existing
files plus one new leaf changed,438 unchanged. target.rs1371 only adds one
private test route; memory_io1108; address_storage206. Independent reverse
comparison restores both original files byte-exact; accepted buffer/output
code remains unchanged. No oversized shrink or inventory update.

Main ran20 final green commands: default/all-feature Target23, OwnerTarget185,
Supervisor54, Controller67, Knife220, repository20, growth7 and lib checks;
three-file scoped fmt/diff and strict standalone compile/run7.20 additional
module runs (10 each mode) execute60 tests, all passing. Warning headings equal
the accepted buffer baseline (existing vendored22; lib7/libtest1). No global
fmt/full-library-suite claim. Reviewer independently checked source/inverses,
hashes and artifacts, and ran no Cargo/tests or writes.

/tmp/mini-vpn-p3-target.z6G5fA/address-{baseline,source,sequence,gates,repeats,
source-review,review}.json owns exact evidence. The next executable obligation
is the native original owning-node transaction across OPEN/live->joined/retire,
preserving both-way readiness correspondence, per-domain ordered complexity,
bounded retained vacancies, address transfer and precommit refusal. Complete
address/backing peak remains attached to that work, not waived or moved to P4.

## 2026-09-17 — TargetIo complete Implementation Move accepted

Same independent Heisenberg final SOURCE P0/P1/P2=0/0/0 Approve. Only existing
target/memory_io.rs and new private target/memory_io/flow_io.rs changed. Parent
1108->372, child745. The entire737-line impl TargetIo for MemoryTarget is
byte-exact: SHA2564ebd4eab27c532190fc934ae8171b6645c9b065e99753291bc58b0912b7a0610.
Removing the private declaration, restoring module docs/separator and replacing
the child body restores the complete original parent exactly.442 Rust hashes,
two changed paths/440 unchanged, no removed path or governance edit.

The parent retains its complete inherent implementation and original address/
buffer/output preparations. Child calls ancestor-private methods directly;
there is no re-export, forwarder, caller/signature/visibility change or second
resource owner. No test body/FQ route changed. Deleting child removes the actual
TargetIo Implementation. This is preparatory structure, not a node-storage fix.

Current standalone prewrite governance7 and accepted before-Move Target23 in
both modes form the source-identical baseline. First post-Move Target23 passed.
20 final green commands cover both Target23/OwnerTarget185/Supervisor54/C67,
Knife220/repository20/growth7/libcheck; two-file scopedfmt/diff plus strict
standalone compile/run7.20 additional Target-module runs (10 each mode) execute
460 tests, all green. Warning headings equal the accepted address baseline.
No unexpected red or repair. New untracked source whitespace/newline checks and
final442-hash recheck show no drift. Reviewer reviewed code/inverses/evidence,
and ran no tests/Cargo or writes.

/tmp/mini-vpn-p3-target.z6G5fA/node-move-{baseline,source,focused,gates,repeats,
review}.json owns evidence. The selected next native direction is one real Node
Layout with Live/Joined/Vacant, separate ordered roots and bounded vacancies.
Exact behavior/test scope still must freeze: checked F+Z and preparation peak,
same-node transition before callback, original-address disposition, bidirectional
readiness lookup and iterative vacancy destruction. A generic map facade,
temporary map rebuild, Index compatibility shim or253 raw-address assumption
is not authorized by this Move. Complete Target peak, progress/aggregate and
all five51B remain mandatory; P3/Case C/R7/Task4 remain incomplete.
