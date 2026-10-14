# P3 Owner source preparation — Move freeze

Date: 2026-09-07.

Status: PURE MOVE IMPLEMENTATION AND DOCUMENT CLOSURE ACCEPTED.

Same Reviewer Heisenberg: P0/P1/P2 0/0/0, Approve of this two-file pure Move
only. Subsequent implementation/document closure is also0/0/0 Approve,
Module/Clean/System/DDIA9/9/9/9, with inverse hash and all planned gates green.
No future source binding Interface or full-P3 completion is implied.

## Authority and purpose

The full-P3 goal permits causal repairs in scope, not architectural bypass.
The accepted [scheduler amendment](2026-09-04-knife16-r7-target-open-ready-scheduler-amendment.md)
and [Case C plan](2026-09-04-knife16-r7-case-c-owner-server-root-implementation-plan.md)
remain controlling. This plan authorizes no P4 table/bootstrap, P5 adoption,
P6 physical caller integration, public cutover, commit/push or external run.

The [current P3 result](2026-09-07-knife16-r7-p3-owner-progress-local-results.md)
accepts the re-park/fatal packet, not full P3. Further isolated notification
tests cannot substitute for actual Owner source service. This preparatory
Move creates a bounded read context at the real Authority before that design.
It changes no behavior and makes no persistent Driver/mailbox binding claim.

## Placement audit and unresolved behavior freeze

Read-only audits by Mencius and the same Reviewer Heisenberg agree:

- Prefer deepening the existing OwnerTargetExecutor source Implementation.
  Its actual TargetReadSource owns the Driver/Flow; the Executor owns Target,
  Supervisor, flow records and completion retention. Do not extract these.
- Preserve Controller observations, reverse progress and exact continuation,
  and the outer controller/output root's page-drain/permit transaction.
- Amendment sections 2 and 8 permit transitive root ownership of the same
  per-session two command cells. They do not permit per-flow pairs, duplicated
  outer/inner buffers, process scheduling policy inside the Executor, or an
  inward dependency on server_root. Placement feasibility is not a frozen
  replacement Interface or an authorization to move the cells in this packet.
- Putting only commands/inbox outside the Executor cannot close its private
  source transaction. A bare Executor resident bypasses the frozen whole-root
  owning chain. Neither is an acceptable shortcut.

Before any source behavior change, freeze the actual cold mailbox pairing,
unique two-cell custody and all read/failure/continuation call routes. The
current conditional Driver borrow, matching SessionId and exclusive capacity
observer do not prove this association. Both legacy Owner mixed dequeues must
be accounted for; the Client's sealed mixed receive is a separate route.
Peer ACK must retain its real actor-arrival witness; a mailbox Event cannot be
retimestamped or treated as local input to bypass Controller validation.
Shutdown must still enumerate and rebind the actual sources with custody.

## Exact Writer scope and transformation

Main is the sole overlapping Writer. Rust scope, after this freeze is approved:

1. `src/owned_upstream/owner_target.rs`:
   - add exactly one private unconditional default-path `mod read_source;`;
   - delete the existing two-method block at entry lines 4220–4365;
   - no other import, method, field, test, visibility or Interface change.
2. New `src/owned_upstream/owner_target/read_source.rs`:
   - compact ownership/invariant/entry/test module docs;
   - imports of the exact ancestor types and TargetReadCompletion;
   - one inherent `impl<T: TargetIo> OwnerTargetExecutor<T>` containing the
     existing `try_read_target` and `try_read_target_observed` methods.

Both methods retain `pub(crate)`, signatures, ordering, match arms, errors and
return values. No parent pass-through, getter, re-export, callback, new trait,
new public type, cfg split or generic parameter is introduced. The impl has
the same Authority as the old impl, not a second storage object.

The entry block is 146 lines, SHA-256
`367f818e14f2339aa31bf8bf95c5a8d4c1cd2d5e5838ff4bdab4452c11145445`.
Its four `super::target::TargetReadCompletion` paths need lexical rebasing in
the child: import `crate::owned_upstream::target::TargetReadCompletion` and
refer to `TargetReadCompletion`. This is the only permitted body text change.
Inverse-normalize those four paths and require the complete original block
hash, including its docs. Count both method definitions exactly once in the
child and absent from the parent, preserving all parent test calls; restore
the block and remove the
module declaration to require an exact whole-parent inverse hash.

Baseline parent: 11110 lines. Delete146/add2 predicts10966 before formatting;
the read-only rustfmt probe and Reviewer agree that removal of the one doubled
separator gives10965. Reverse that exact separator change for the inverse hash.
The child probe is164 lines, below200 and1500. The Integrator alone
lowers the existing parent ratchet to the verified final count in the same
promoted change. Writer never edits `tests/code_growth_governance.rs`.

## Complete transaction and unmoved dependencies

The moved read transaction retains preflight, source reservation before Target
read, DATA/EOF commit, actual Driver dequeue, exact source-fact validation,
observed reducer application, bounded effects and failure observation retention.
WouldBlock releases the reservation without claiming progress. Existing read
failure continues to call the exact existing `apply_target_failure` helper.

That shared helper remains parent-private and still serves read, write and
half-close failure; RESET publication/dequeue/completion are not changed.
`submit_target_completion`, retry, abort/error/observation helpers and all
storage remain in the parent. A descendant inherent impl may use them without
new visibility. This is a complete existing read-method Move, not a claim to
extract the entire future source/RESET settlement model.

The observed-apply gate mint/consume and checked Controller aborted-page
construction provenance remain at their original parent routes. Existing
source-inspection tests must pass unchanged; no scanner/provenance edits are
authorized. All caller paths and test names remain identical.

## Failure tree and acceptance

1. Before Move, standalone governance and focused characterization must pass.
   A baseline failure is not a Move regression; inspect before any repair.
2. After Move, path/import/privacy compile errors falsify this freeze. Diagnose
   causally; only equivalent local rebasing within these two files may be
   repaired autonomously. No visibility workaround or caller change.
3. A behavior failure contradicts the pure-Move hypothesis. Compare the exact
   inverse hash and existing transaction before considering a repair. Do not
   change behavior, assertions or tests in this packet.
4. The sole expected governance red is the exact oversized shrink request.
   Stop Writer changes; Integrator verifies scope/count and lowers that entry.
5. Any unexpected source/provenance debt or need for a third Rust file stops
   implementation for a new explicit scope freeze. No silent test weakening.

Run all local commands offline where Cargo supports it and serially:

- Before/after exact characterization, each1/1:
  `owned_upstream::owner_target::tests::target_read_reserves_before_consumption_and_eof_follows_owned_data`;
  `owned_upstream::owner_target::tests::terminal_capacity_retains_failed_read_completion_without_repeating_target_io`;
  `owned_upstream::owner_target::tests::one_step_turn_drains_data_close_chain_without_repeating_target_io`.
- Full Owner74/74 (including ACK observations) and Controller44/44 (including
  actual actor-arrival/progress and continuation retention); continuation16/16.
- Existing P3 root56/56, mailbox18/18, TCP68/68 and Client progress/provenance.
- Knife16 provenance162/162 and repository provenance13/13, unchanged.
- fmt, tracked/untracked whitespace and exact scope/hash audits;
  `cargo check --offline --all-features --lib`.
- Standalone `rustc -D warnings --edition=2024 --test` governance and Cargo
  all-features governance, each6/6 after Integrator shrink ratchet.
- Same independent Reviewer findings-first closure of the exact Move and
  evidence; no broad P3 acceptance inferred from a scoped Approve.

This pure movement does not introduce a new RED behavior or require race
repeats. Every command must report a nonzero exact inventory; freeze additional
module counts from the pre-Move listing rather than guess them.

## Next boundary

After reviewed Move closure, update the current result/status/navigation and
concise reusable learning. Then freeze the real owning Session service:
actual DATA/RESET/CLOSE over repeated source visits, lane pressure, same-reducer
delivery, ACK permit release, output/continuation exclusion and shutdown
inventory. New Interface/behavior files remain unfrozen by this Move plan.

Current source-service seam readiness is approximately6/10, not a full-P3
score. Reaching10 requires the above owning association/turn contract and
deterministic runtime evidence. A hash-identical Move alone changes neither.
