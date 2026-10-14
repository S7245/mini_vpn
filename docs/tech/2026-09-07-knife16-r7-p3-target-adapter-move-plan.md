# P3 — complete private Target test Adapter Move

Date: 2026-09-07.

Status: **PURE MOVE AND DOCUMENT CLOSURE ACCEPTED; NO BINDING BEHAVIOR**.

## Goal and authority

Prepare the two oversized inline TargetIo test implementations before the
accepted [Target readiness contract](2026-09-07-knife16-r7-p3-target-readiness-contract.md)
can change the same static trait. This is green-to-green movement, not a new
behavior, interface, readiness fallback or partial Owner binding. Full P3 user
authority permits causal in-scope mechanical repair; architecture/growth/review
hard stops remain. Main owns the overlapping source; Mencius alone owns ratchet
promotion. No commit/push, external run, P4+, production behavior or new tests.
Closure metadata is limited to this plan, current P3 result, STATUS/HANDOFF/TODO
and project LEARNINGS/ERRORS. This adds no Rust or behavioral write scope.

Move each whole fixture plus all callers, not just trait methods. Retain the
existing r7_nested_abort route to avoid an unnecessary file/test rename; compact
docs explicitly broaden its ownership to the seven failure/terminal tests.
Do not export a fixture so that a sibling test can continue borrowing it.

## Frozen source and deterministic transformation

Pre-Move [owner_target.rs](../../src/owned_upstream/owner_target.rs) is10965 lines,
SHA `801a88613f8c0bb92653bfdeb532769e77d5ae159895e226756630666b3fc3a5`.
Existing [nested leaf](../../src/owned_upstream/owner_target/tests/r7_nested_abort.rs)
is98 lines, SHA `0cd3e0eff123ab17ec188191e419c724a393b6378ddf3adb9028708b325b56b1`.
Immutable entry and all raw block hashes are in
`/tmp/mini-vpn-p3-target-adapters-yCzYFb/entry.json`; run.mjs owns the exact
deterministic candidate, rustfmt preview, inventories, logs and named audits.

| Raw parent block, including LF | Original lines | Destination |
|---|---:|---|
| OverAcceptingTarget struct |8103–8105|new target_overacceptance|
| FailOnceTarget struct/new/full TargetIo impl |8107–8238|existing r7_nested_abort|
| full OverAcceptingTarget TargetIo impl |8240–8316|new target_overacceptance|
| six complete failure/terminal tests |9746–10240|existing r7_nested_abort|
| complete invalid-write acceptance test |10307–10354|new target_overacceptance|

Only three source paths may change:

1. `src/owned_upstream/owner_target.rs`: remove the three complete continuous
   ranges8103–8316,9746–10240,10307–10354 and each following blank separator;
   add `mod target_overacceptance;` after r7_owner_ack. Existing nested route,
   imports, ancestor helpers, production prefix and all other tests unchanged.
   Exact rustfmt-preview size10206; full retained-parent comparison required.
2. `src/owned_upstream/owner_target/tests/r7_nested_abort.rs`: six compact doc
   lines, separators and use super; complete FailOnceTarget fixture followed
   by six moved tests, then the original nested-abort test. Exact formatted726.
3. New `src/owned_upstream/owner_target/tests/target_overacceptance.rs`: same
   nine-line prefix shape, OverAcceptingTarget struct/full impl/unique test.
   Exact formatted133; private default route, no debt entry.

Parent blocks lose precisely one indentation level. The sole lexical rebase
is OverAcceptingTarget expire_session_flow return type from
`super::super::target::TargetExpireCompletion` to the existing ancestor import
`TargetExpireCompletion`. Apply rustfmt layout after dedent; raw post-Move
hashes cannot be claimed equal to indented originals. Verify the complete
formatted leaves against this frozen recipe, not a whitespace-insensitive
comparison that could hide string changes. Original nested test stays exact.
No visibility, callbacks, helpers, re-exports, cfg, control/error/ownership or
assertion changes. Existing ancestor helpers and production event_flow_id stay.

## Test identities and failure tree

Common prefix is `owned_upstream::owner_target::tests::`. These six tests
gain `r7_nested_abort::`:

- operational_open_failures_map_exactly_once_without_target_ownership
- operational_read_failure_is_one_urgent_target_failure_reset_and_other_flow_progresses
- operational_write_and_half_close_failures_reset_but_invariant_errors_stay_fatal
- terminal_capacity_retains_failed_read_completion_without_repeating_target_io
- successive_terminal_retirements_advance_pending_completions_in_flow_order
- terminal_capacity_retains_successful_half_close_without_repeating_target_io

`target_cannot_ack_more_bytes_than_the_slice_it_was_given` gains
`target_overacceptance::`. The existing nested_target_failure_abort test's FQ
is unchanged. Compare the entire actual library --list before/after with only
these seven substitutions. Expect leaf7/1 and Owner74; zero tests is not green.

Predictions: ancestor privacy compiles without interface expansion; all eight
test bodies survive dedent/format with exact assertions; shared helpers remain
available; only seven FQs change; both leaves remain under800. Missing caller,
relative-path/type failure, extra FQ difference, semantic delta or growth
disproves this candidate. No source-provenance assertion currently targets
these fixtures/test names; production-prefix provenance must still pass.

## Execution gates and stop boundary

Before source writes: fresh standalone governance6; actual library inventory;
Owner74/Target9/Controller44/continuation16 serial baselines. Independent same
Reviewer must approve this plan before the Move. Existing tests characterize
unchanged behavior; no artificial behavior RED or synthetic success witness.

After Move: exact two leaf sets7/1, complete library FQ comparison; repeat the
four baselines plus server_root56/mailbox18/TCP68/client-progress3. Then fmt,
tracked/untracked whitespace and whole-source/other-Rust scope audit. Writer
stops at shrink. Integrator alone changes only owner_target10965→10206 in
code_growth_governance; preserve all allowance/inline records and run fresh
standalone-D warnings6 and offline Cargo all-features governance6. Then
Knife16/repository provenance162/13 and all-feature library check. Same
Reviewer verifies implementation and document closure before acceptance.

Unexpected failures require a causal evidence tree. Only mechanical repairs
within these paths/transformations can proceed autonomously; no second semantic
repair, fixture visibility expansion, unrelated actor debt repair or production
change. Do not auto-revert or commit under the skill's generic recipe; preserve
the dirty worktree. Full P3/Case C/R7 remain incomplete, and binding/generation/
release-only guard/K/Layout freeze is still required before behavior work.

Structural source estimate8/10: complete closed test ownership, no new Interface.
Exact executable closure and independent review remain needed. The historical
nested filename is documented rather than expanding scope just for a rename.
The [P3 result](2026-09-07-knife16-r7-p3-owner-progress-local-results.md) owns
acceptance; this plan does not close any readiness/binding ledger item.

First same-Reviewer plan review returned0/0/1, solely the audit script's
stage ambiguity: audit-final could previously accept an absent new leaf and
old ratchet. It now distinguishes prewrite (original sources/new leaf absent/
old ratchet), every post-Move audit (all three exact candidate files required),
and final (new ratchet mandatory). The Move itself was found feasible with
exact10206/726/133, no other necessary optimization. No source write occurred.
Same-Reviewer narrow re-review then passed0/0/0 Approve for the plan. An actual
negative audit-final invocation before the Move rejected the old source and
created no final snapshot. The three-file Move now matches the frozen complete
candidate; the production prefix and original nested test body remain exact.

Before-source baselines and every post-Move gate passed. Complete library FQ
comparison changes only the seven listed names; leaves7/1 and total Owner74
are exact. Integrator lowered only owner_target10965→10206; governance remains
698 lines and both routes pass6/6. Knife16/repository162/13 and all-feature
check also pass. Complete source-proof.json and audit-review-packet.json bind
the final candidate/334 other frozen Rust and closure metadata. A second
negative final audit after the Move rejected the old ratchet. Final same
Reviewer implementation/document closure passed0/0/0 Approve, scoped8/10,
no necessary optimization. It permits only factual status/evidence promotion
without another review. Full P3 and the next behavior freeze remain open.
