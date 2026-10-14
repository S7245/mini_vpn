# Knife16 R7 P2T bootstrap test/provenance Move entry

Date: 2026-09-05.

Status: **P2T ENTERED — STOPPED AT PRE-WRITE GOVERNANCE; NO RUST MOVE**.

The user's goal is to proceed until entering P2T. P1 architecture and its
status promotion are independently approved by replacement Reviewer Heisenberg
(`01a06bfa-4552-7ea3-b5ce-9d6b65f88992`), both P0/P1/P2 `0/0/0`, Approve.
The status review verified the unchanged architecture-body hash and independent
public-path task stop sections. It was not compilation or runtime evidence.

Sources: [accepted implementation plan](2026-09-04-knife16-r7-case-c-owner-server-root-implementation-plan.md),
Sections 6 and 12; [accepted P1 amendment](2026-09-04-knife16-r7-target-open-ready-scheduler-amendment.md).
Case C/R7/Task 4/6 and public TUN remain open. This entry is not P2T completion.

## Scope and ownership

Change axis: behavior-preserving Move of exactly five existing bootstrap
characterizations and one existing integration-provenance test. One Writer
owns the two parents and two destination leaves; reviewers remain read-only.
The original R7 Writer was verified idle before this entry.

| Role | Repository-relative path | Current lines |
|---|---|---:|
| Unit-test parent | `src/owned_upstream/quinn_leg_actor/tests/mod.rs` | 16,288 |
| Future private unit leaf | `src/owned_upstream/quinn_leg_actor/tests/bootstrap_custody.rs` | absent |
| Integration parent | `tests/knife16_owned_leg_provenance.rs` | 2,988 |
| Future private integration leaf | `tests/knife16_owned_leg_provenance/bootstrap.rs` | absent |

These are the only future P2T Writer Rust paths. No production, scanner,
repository-provenance, unrelated fixture repair, or governance write belongs
to the Writer. Only the Integrator lowers the two exact parent ratchets after
the Move reaches its expected shrink boundary; no entry is changed in advance.
Normal limits remain test leaf 800 and test index 300; old parents must shrink.
The 468-line unit closure and 366-line provenance closure fit their leaves
with compact docs/imports. Exact post-rustfmt sizes are not yet evidence.

## Frozen identities and bytes

Unit old prefix: `owned_upstream::quinn_leg_actor::tests::`.
Unit new prefix: `owned_upstream::quinn_leg_actor::tests::bootstrap_custody::`.
Leaf names remain exact:

1. `cancelled_start_caller_leaves_registered_bootstrap_cleanup_to_completion`
2. `admitted_slow_bootstrap_does_not_head_of_line_block_a_second_connection`
3. `abnormal_bootstrap_guard_poisons_before_releasing_its_live_slot`
4. `prepublication_start_failure_releases_reservation_without_poisoning_listener`
5. `unclaimed_server_handoff_preserves_structural_failure_poison`

Integration old identity:
`quinn_actor_inbound_and_admission_cleanup_seams_stay_typed_and_single_use`.
New identity: the same name prefixed by `bootstrap::`.

Each block begins at its complete test attribute and ends with the function's
closing brace plus LF. Separating blank lines, Module docs, imports and route
declarations are outside the hash. All bodies and assertions remain unchanged.

| Block | Original lines | Lines | SHA-256 |
|---|---|---:|---|
| Unit 1 | 11329–11458 | 130 | `885fbf69b893cbaee3c4ade0d1ff1137cac51348243093bc1f9ce378dad6d969` |
| Unit 2 | 11460–11564 | 105 | `e342ca18e7e5acccd25cd117a129dbeb45b71a778ae2188b6699f99ea2131e46` |
| Unit 3 | 12256–12294 | 39 | `5ce367e9db45b5aed2620cec4c95f2ec336ae7ddd371fae02685d8bd63c03936` |
| Unit 4 | 12296–12410 | 115 | `d861ab3573811a8f38f5cc3a96e1eb1e68c102f0038a5ffe4858d41245fdcf19` |
| Unit 5 | 13429–13507 | 79 | `501df092683866899dcccb1ecc4eb150eee6f72cf1d754d690842b42724cbbdd` |
| Integration | 1911–2276 | 366 | `9ad34d9f4d8f6712438fd98bf5fb433ea33605000c42664df696504b41744fab` |

The five unit blocks concatenated in the order above contain 18,143 bytes /
468 lines, SHA-256
`b6a46bd402e0ce0b3317158821fce19b7ff318ba268eb0262a156695a1d1fc1e`.

Pre-entry full-source hashes:

- Actor production: `04917d3a7b7052627b54906b69532c91089f04291d2b263d32d5be6b379738b4`.
- Unit parent: `14ff8c18a200345c0519d0a703950a4bfbb4bfeaffec9213bf20c76adee34333`.
- Integration parent: `b988c0655eca940d884f925fa95c59aa50def818df3701114937e8357ec8d7aa`.
- Governance (699 lines): `1c59983c02e93b7da4104ab0a31921884fe1ce1f0b108a89ced7f15941239080`.

## Route/dependency freeze and falsifiable predictions

- Unit parent declares one private `mod bootstrap_custody;`. Its leaf uses
  `use super::*;`. Unchanged `super::` expressions require private imports at
  the test parent for `build_client_endpoint`, `export_binding`,
  `binding_confirmation`, `ActorRole`, `bootstrap_streams`, `ListenerRegistry`,
  `BootstrapTaskTerminationGuard`, and `ServerBootstrapHandoff`. All are
  existing actor-private ancestors; no visibility change or re-export.
- Integration root uses the explicit crate-root path exception:
  `#[path = "knife16_owned_leg_provenance/bootstrap.rs"] mod bootstrap;`.
  The leaf borrows existing source constants/helpers through `use super::*;`.
  Bare `mod bootstrap;` would resolve to the wrong sibling file and is forbidden.
- `QUINN_ACTOR_TESTS_SOURCE` currently reads only the actor test parent.
  Its future source-route definition must concatenate exactly that parent and
  the new leaf via `include_str!`, outside frozen test blocks. This preserves
  the existing 109-test assertion: 104 parent + 5 leaf. Its current consumers
  are string/count checks, not `syn::parse_file`; no assertion is relaxed.
- Old test-body presence becomes zero in each parent and exactly one in its
  named child. New fully qualified commands must execute one test, not zero.
- Existing detached bootstrap behavior is characterized, not endorsed as Case C
  custody. The unrelated actor ordered-delivery residual fixture is not moved,
  repaired or used as this baseline.

If existing private imports do not preserve body resolution, the source-route
change alters an invariant, another file is needed, or rustfmt changes a hashed
block, stop and present evidence before repair. Do not substitute public helpers,
forwarding functions, macros, new assertions, or an altered test body.

## Entry and later Move gates

Entry first runs standalone governance, compiled with
`rustc -D warnings --edition=2024 --test tests/code_growth_governance.rs`
to a fresh temporary executable, then runs it with `--test-threads=1`.
Only after it passes, run each of the five exact pre-Move unit identities:

```text
cargo test --lib <old-unit-prefix><leaf-name> -- --exact --nocapture --test-threads=1
cargo test --test knife16_owned_leg_provenance quinn_actor_inbound_and_admission_cleanup_seams_stay_typed_and_single_use -- --exact --nocapture --test-threads=1
```

After the later Move: use the exact new identities, compare all hashes and
old/new symbol counts, run full Knife16/repository provenance, fmt/diff/check,
then standalone/Cargo all-features governance. Stop at the first unexpected RED.
The anticipated exact shrink-ratchet RED belongs to the Integrator; after its
inventory update, both governance gates must pass and independent closure is
required. P2A/P2B/P3 do not start from a focused GREEN or from this entry record.

## Evidence at entry

P1 final status review: `0/0/0`, Approve. P2T symbol/route/hash audit is complete.
Standalone governance compilation with `-D warnings` passed. Its first execution
returned exit 101: **4 passed / 2 failed**. The failed production/test line-debt
checks report exactly four shrink-only mismatches outside P2T:

| Independent public-path source | Recorded | Actual |
|---|---:|---:|
| `src/client_tun.rs` | 16,613 | 16,561 |
| `src/tuic.rs` | 8,172 | 7,989 |
| `src/client_tun/tests/mod.rs` | 14,301 | 14,230 |
| `src/tuic/tests/mod.rs` | 5,499 | 5,390 |

The other four governance tests pass. Read-only `wc -l` independently confirms
these counts; actor production, both P2T parents and governance retain all four
pre-entry full-source hashes. This is a shared-worktree acceptance dependency,
not an observed failure of a moved test: no Move has occurred.

The [public-path result](2026-09-04-public-path-review-repair-local-results.md)
still says partial/not promoted, with an open sender review finding. Therefore
R7 must not lower those four ratchets merely to unblock itself. The public-path
Integrator must establish its authorized coherent acceptance boundary and own
any inventory promotion; alternatively, a separately chosen isolated accepted
baseline is needed. Neither action was performed by this entry task.

Per first-RED stop: no focused Cargo test, later governance, fmt, behavior repair
or Rust write followed. All six pre-Move focused baselines remain **NOT RUN**.
No inventory, test body or Module route changed. P2T entry has occurred; P2T
Move acceptance, Case C and R7 have not. No commit/push or external run.

Resume only after the shared governance prerequisite is legitimately resolved:
revalidate hashes/current boundaries, rerun standalone governance, then the six
exact baseline commands above. Do not skip the gate or silently rebase a frozen
block. If another failure appears, stop before any repair.

Final independent entry/stop review by the same replacement Reviewer
Heisenberg: P0/P1/P2 `0/0/0`, Approve, **entry/stop evidence only, not Move
approval**. The Reviewer independently verified six block hashes, aggregate and
four source hashes, the proposed routes/imports, all four mismatch counts and
compact status consistency. No tests were rerun. This confirms the user's
entry-level goal only; it authorizes neither a second attempt nor the separate
public-path repair.
