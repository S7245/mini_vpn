# Knife16 R7 P2T bootstrap test/provenance Move local results

Date: 2026-09-05.

Status: **P2T LOCALLY ACCEPTED — IMPLEMENTATION, GATES, RATCHETS AND FINAL HANDOFF CLOSED.**

The user authorized completing the independently reviewed public TUIC capacity
repair, its review and inventory closure, then continuing the complete P2T
node. The [public result](2026-09-05-public-path-capacity-repair-local-results.md)
now records local closure. Its Writer Herschel and Integrator confirmed that
all repository writes stopped before P2T resumed. No public implementation,
Quinn root repair, external acceptance, commit or push belongs to P2T.

The [frozen entry packet](2026-09-05-knife16-r7-p2t-bootstrap-test-move-entry.md)
owns the six exact block hashes, routes, allowed files and failure predictions.
Its historical pre-write `4/6` stop remains unchanged; the public dependency
was resolved by its own reviewed work, not by waiving that gate. The
[accepted implementation plan](2026-09-04-knife16-r7-case-c-owner-server-root-implementation-plan.md),
Sections 6 and 12, owns the P2T/P2A/P2B boundaries.

## Scope and ownership

Change axis: behavior-preserving movement of exactly five existing unit tests
and one existing integration-provenance test. Mencius is the sole Feature
Writer for the four Rust paths in the entry packet. The coordinating Integrator
owns this result, later exact inventory changes and compact-state promotion.
The existing independent P2T Reviewer is Heisenberg, not public Reviewer Popper.

No test body, assertion, production behavior, effective visibility, helper
Interface, callback or parameter changes are allowed. New leaves remain below
800 lines, and both grandfathered parents must shrink. Only the Integrator
lowers their exact ratchets after the characterized Move. Any unexpected red,
missing test, hash mismatch or expanded scope stops the next repair attempt.
P2A, P2B, P3, Case C and R7 completion are not claimed by this node.

## Resumed pre-write evidence

Shared HEAD remains `a7c9a95438386739e467061ac961fecf3bbe9d60`.
Existing public-path changes are preserved. The Integrator independently
revalidated these complete source hashes before any P2T Rust write:

| Source | SHA-256 |
|---|---|
| Actor production | `04917d3a7b7052627b54906b69532c91089f04291d2b263d32d5be6b379738b4` |
| Actor test parent | `14ff8c18a200345c0519d0a703950a4bfbb4bfeaffec9213bf20c76adee34333` |
| Knife16 provenance parent | `b988c0655eca940d884f925fa95c59aa50def818df3701114937e8357ec8d7aa` |
| Governance after public closure | `631cbd210d37876cb2812ee8841d06517a6553bdd28dcd49fc167f42e0d271ee` |
| Frozen P2T entry packet | `a473ed8e54eb76add38cb8109c440560c3b581622a30249832bc5059ce432bc9` |

Fresh evidence directory: `/tmp/mini-vpn-p2t-9SlYdnx8`.
`rustc -D warnings --edition=2024 --test tests/code_growth_governance.rs -o
/tmp/mini-vpn-p2t-9SlYdnx8/code-growth-governance` exited 0. Running that
executable with `--test-threads=1` passed **6/6**. Logs are
`pre-governance-compile.log` and `pre-governance.log` in that directory.

The following pre-Move baselines ran serially, each selected exactly one test,
and each exited 0 with **1 passed / 0 failed**. Unit filters use the prefix
`owned_upstream::quinn_leg_actor::tests::`.

| Leaf name | Log |
|---|---|
| `cancelled_start_caller_leaves_registered_bootstrap_cleanup_to_completion` | `baseline-unit-1.log` |
| `admitted_slow_bootstrap_does_not_head_of_line_block_a_second_connection` | `baseline-unit-2.log` |
| `abnormal_bootstrap_guard_poisons_before_releasing_its_live_slot` | `baseline-unit-3.log` |
| `prepublication_start_failure_releases_reservation_without_poisoning_listener` | `baseline-unit-4.log` |
| `unclaimed_server_handoff_preserves_structural_failure_poison` | `baseline-unit-5.log` |
| `quinn_actor_inbound_and_admission_cleanup_seams_stay_typed_and_single_use` | `baseline-provenance.log` |

Exact command shapes were:

```text
cargo test --lib <unit-prefix><leaf-name> -- --exact --nocapture --test-threads=1
cargo test --test knife16_owned_leg_provenance quinn_actor_inbound_and_admission_cleanup_seams_stay_typed_and_single_use -- --exact --nocapture --test-threads=1
```

Each unit command reported 1,608 filtered tests; the integration command
reported 158 filtered tests. The abnormal-bootstrap test's intentionally caught
panic is expected characterization evidence, not a new failure. The separate
known ordered-delivery residual fixture was neither run nor repaired.

## Mechanical Move and coordination stop

Mencius completed only the four allowed Rust writes, then stopped before the
first post-Move test. Its additional shared-document hash check included
`HANDOFF.md`, `TODO.md` and `docs/current/STATUS.md`; the Integrator had updated
those documents within its separately assigned coordination scope. The check
therefore reported a false positive, not a compiler or behavior-test failure.
No post-Move test, rustfmt, broad gate or ratchet update has run.

The Writer reported all six immutable blocks unchanged, exact parent absence
and child presence, private routes/imports, the unchanged `104 + 5 = 109`
inventory, and exact reconstruction of both original parent hashes after
undoing only the authorized Move/import/route delta. The five unit blocks
remain 468 lines / 18,143 bytes with aggregate SHA-256
`b6a46bd402e0ce0b3317158821fce19b7ff318ba268eb0262a156695a1d1fc1e`.
The integration block retains its frozen SHA-256
`9ad34d9f4d8f6712438fd98bf5fb433ea33605000c42664df696504b41744fab`.

The Integrator independently recomputed and matched these whole-file hashes:

| Source | Current lines | SHA-256 |
|---|---:|---|
| Actor test parent | 15,818 (was 16,288) | `8e1fe2d82e40ba8dc2ca6439533abc4d06064497d51d18f6df6820243ad69e84` |
| New `bootstrap_custody.rs` | 480 | `3f2f6cdb10978e4241f1e1b5ef2e0648ee072e79b93cd8595e5ea10f9d686699` |
| Knife16 provenance parent | 2,626 (was 2,988) | `15338e43145901a3b98cdf64fb4d1f41b0c30d079e879f9e17db6c2ef5bd2206` |
| New integration `bootstrap.rs` | 374 | `7394c24321d5b4770f2b3b316ba9f82d634c078a748c7eb6debf0f2086f20a49` |

Actor production, governance and the frozen entry packet still match their
pre-write hashes above. The Integrator confirmed ownership of the three
coordination-document updates; no rollback or Rust repair is needed for that
audit false positive. The same Heisenberg confirmed its existing P2T review
context and remains on standby, without approving the untested Move.

The user explicitly confirmed recovery under the first-red stop contract.
Only the audit ownership boundary was corrected; the four Rust files were
preserved. The Integrator independently rechecked all six block hashes,
aggregate hash, parent absence and child uniqueness without freezing its own
coordination documents.

## Resumed post-Move exact evidence

The five unit names above now run under the exact prefix
`owned_upstream::quinn_leg_actor::tests::bootstrap_custody::`:

```text
cargo test --lib <new-unit-prefix><leaf-name> -- --exact --nocapture --test-threads=1
cargo test --test knife16_owned_leg_provenance bootstrap::quinn_actor_inbound_and_admission_cleanup_seams_stay_typed_and_single_use -- --exact --nocapture --test-threads=1
```

All six ran serially and passed **1/1**, preserving the pre-Move filtered
counts of 1,608 for each unit and 158 for integration. Logs are
`post-unit-1.log` through `post-unit-5.log` in the same name order as the
baseline table, plus `post-provenance.log`, in the fresh evidence directory.
The Integrator independently read all six result summaries. The intentionally
caught guard panic remains expected; no code repair followed recovery.

## Integrator gates and first format stop

After the Writer confirmed final source/test freeze, the Integrator ran:

| Command | Result | Log in evidence directory |
|---|---|---|
| `cargo test --test knife16_owned_leg_provenance -- --nocapture --test-threads=1` | exit 0; 159/159 | `full-knife16.log` |
| `cargo test --test repository_test_module_provenance -- --nocapture --test-threads=1` | exit 0; 13/13 | `full-repository-provenance.log` |
| `cargo fmt --all -- --check` | exit 1; exactly one import-wrap hunk | `fmt.log` |

The format hunk is confined to the allowed actor test parent at lines 16–17.
Rustfmt moves `build_client_endpoint` to the following line beside
`export_binding`. Both versions use the same symbols in the same order and
occupy exactly two lines. It is outside all six immutable test blocks. No
format fix was applied, and no post-fmt diff/check, standalone governance,
Cargo governance or independent closure review ran after this first red.
Current counts remain parent 15,818 / leaf 480 and integration parent 2,626 /
leaf 374; governance remains 699 lines with both old P2T ratchets unchanged.

Bounded failure tree:

1. Import layout differs from the active formatter: the exact two-line hunk
   predicts that applying only that wrap makes global fmt pass without changing
   symbols, order, line count or block hashes.
2. Route/privacy or behavioral regression: unsupported by the six post-Move
   exact passes and the full 159/159 plus 13/13 provenance results; those gates
   do not replace final review.
3. Unrelated formatter/source drift: the complete fmt log names no other path
   or hunk. No out-of-scope source repair is justified by this evidence.

The proposed repair at that stop required user confirmation: only apply the reported
two-line wrap in `quinn_leg_actor/tests/mod.rs`, retain all other Rust bytes,
then rerun fmt and revalidate hashes/counts. Resume the focused/full provenance
and remaining diff/check/governance sequence on that formatted tree; stop at
any unexpected red. Only after the expected two-parent shrink boundary may the
Integrator lower their exact ratchets and rerun both governance routes, then
request the same Heisenberg's independent closure review. The separately
confirmed recovery and resulting evidence follow; this stop record is retained.

## Authorized format repair and final gate tree

The user confirmed that exact repair. Mencius applied only the two-line wrap
with `apply_patch`; parent length remains **15,818**. The Integrator reversed
only that wrap in memory and recovered the prior whole-parent SHA-256
`8e1fe2d82e40ba8dc2ca6439533abc4d06064497d51d18f6df6820243ad69e84`,
proving there was no additional parent edit. The final parent SHA-256 is
`2e16c0abd04e06677f11a077fc2b70f481fcfc05b6459938863fae6a11be3d9e`.
The other three Rust files retain their whole-file hashes above. All six
immutable blocks, exact identities and the `104 + 5 = 109` inventory remain
unchanged. The Writer then confirmed complete source/test freeze.

All logs below are under `/tmp/mini-vpn-p2t-9SlYdnx8`; earlier logs are preserved.

| Gate on formatted tree | Result | Log |
|---|---|---|
| `cargo fmt --all -- --check` | exit 0; no diff | `final-fmt.log` |
| Five new unit exact identities, same commands/order above | each exit 0; 1/1 | `final-unit-1.log` through `final-unit-5.log` |
| New integration exact identity, same command above | exit 0; 1/1 | `final-provenance.log` |
| Full Knife16, same command above | exit 0; 159/159 | `final-full-knife16.log` |
| Full repository provenance, same command above | exit 0; 13/13 | `final-full-repository-provenance.log` |
| `git diff --check` | exit 0; no diff | `final-tracked-diff.log` |
| Safe `git diff --no-index --check /dev/null <path>` for all six untracked files | no whitespace diagnostics; expected new-file exit 1 is not a failure | Integrator tool evidence |
| `cargo check --lib` | exit 0; only existing vendored smoltcp 22 warnings | `final-cargo-check.log` |

### Exact shrink boundary and Integrator promotion

The Integrator compiled standalone governance with `rustc -D warnings
--edition=2024 --test tests/code_growth_governance.rs -o
/tmp/mini-vpn-p2t-9SlYdnx8/shrink-code-growth-governance` (exit 0), then ran it
with `--test-threads=1`. It returned exactly the anticipated **5/6**: only
`rust_test_source_line_debt_is_exact_and_shrink_only` failed, naming both and
only these lower counts. Logs: `shrink-governance-compile.log` and
`shrink-governance.log`.

| Test parent | Prior ratchet | Final ratchet / actual lines |
|---|---:|---:|
| `src/owned_upstream/quinn_leg_actor/tests/mod.rs` | 16,288 | 15,818 |
| `tests/knife16_owned_leg_provenance.rs` | 2,988 | 2,626 |

Only then did the Integrator change those two inventory values. No normal
limit, allowance, path, source scanner or public-path ratchet changed.
Reversing just those two numbers in memory reproduces the public-closure
governance SHA-256 `631cbd210d37876cb2812ee8841d06517a6553bdd28dcd49fc167f42e0d271ee`.
Final governance is 699 lines, SHA-256
`6dc56a98781c09a3a317dfd61d3ce9458ea3f547b26c49261c3c328f1426de4f`.

| Post-ratchet gate | Result | Log |
|---|---|---|
| Same standalone compile with output `final-code-growth-governance` | exit 0, `-D warnings` | `final-governance-compile.log` |
| `/tmp/mini-vpn-p2t-9SlYdnx8/final-code-growth-governance --test-threads=1` | exit 0; 6/6 | `final-governance.log` |
| `cargo test --all-features --test code_growth_governance -- --nocapture --test-threads=1` | exit 0; 6/6 | `final-cargo-governance.log` |
| `cargo fmt --all -- --check` | exit 0 | `post-ratchet-fmt.log` |
| `git diff --check` | exit 0 | `post-ratchet-diff.log` |

The two new leaves remain **480** and **374** lines, both below 800. The
Integrator also checked 26 unchanged sources against the public-closure
manifest, excluding only the two P2T parents and governance whose authorized
changes are recorded here. Actor production and the frozen entry packet
retain their pre-write hashes. The excluded residual fixture was not run.

## Independent review and accepted boundary

The same replacement Reviewer **Heisenberg**
(`01a06bfa-4552-7ea3-b5ce-9d6b65f88992`) completed independent implementation
and evidence review: **P0/P1/P2 `0/0/0`, Approve; findings none**. This is the
P2T Reviewer, not historical Darwin or public-path Reviewer Popper.

It independently verified all six immutable block hashes and aggregate,
parent absence/child uniqueness, `104 + 5 = 109`, exact FQ changes, private
ancestor imports, the test-root path exception and two-source `concat!`.
It reconstructed both original parent hashes, the pre-format parent hash and
the pre-P2T governance hash, and matched all 26 non-P2T public manifest sources.
Actor production and the entry packet remain unchanged. It inspected the
pre/post/final gate results and relevant log excerpts, and the authorization/stop
chain; it did not rerun tests, edit files, access external systems, commit or push.

Scoped scores: **structural quality 9/10, Module Depth 9/10, Authority Locality
10/10**. Remaining large ancestors/shared fixtures explain the first two
scores; no additional refactor is required or proposed within this frozen
scope. These are not whole-repository or Case C production-readiness scores.

Accepted: this four-file behavior-preserving test/provenance Move, both exact
shrink ratchets and the associated local gates. The parents shrink by a combined
832 lines; new leaves are bounded at 480 and 374. No production behavior changed.
The independent Reviewer explicitly allowed compact status promotion; the
status-flip stop and its subsequent closure are recorded below.

Next independent boundary: **P2A**, the accepted plan's separately authorized
bootstrap parameter-object convergence. It has not started. P2B/P3, production
owner/caller work, Case C/R7/Task 4/6 completion, public TUN cutover, external
acceptance and commit/push remain open or out of scope. The old ordered-delivery
residual fixture remains separate debt and was not run or repaired here.

## Final status-review stop and authorized closure

The same Heisenberg returned **P0/P1/P2 `0/0/1`, Request Changes**, limited to
the former statement that it read the complete gate logs. That description was
overstated: the Reviewer inspected gate results and relevant log excerpts,
not every complete log line. Its implementation `0/0/0 Approve` remains valid.
The final handoff and persistent P2T goal remained open at that stop.

The six document fingerprints, seven frozen source/entry fingerprints, 14 local
links, historical evidence, acceptance boundary and separate P2A authorization
were otherwise confirmed. No Rust or test repair is required. The Integrator
stopped without changing the disputed sentence or running more gates. It
requested confirmation to replace the complete-log claim with "inspected the
pre/post/final gate results and relevant log excerpts".

The user then confirmed that single-sentence repair. The Integrator changed
only its three-line layout at lines 249–251; no Rust or tests changed or ran.
The same Heisenberg checked only those three corrected lines and the resulting
document SHA-256 `2f2a44657decd4f038b1c026f9a6c646638ed17642a45d1c2729f73ae32fb2e2`,
returned **P0/P1/P2 `0/0/0`, Approve**, and explicitly closed the sole P2 while
preserving its implementation approval. It authorized final factual status
synchronization and handoff, without another implementation or review scope.

The Integrator synchronized that closure in the current documents and existing
error record. **P2T is complete.** All historical stops remain documented.
This final documentation closure did not rerun tests or change Rust, inventory,
production behavior, or any external state. No commit/push occurred. P2A still
requires separate authorization; Case C/R7 and the other frozen non-goals remain
open.
