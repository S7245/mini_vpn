# Knife16 R7 CurrentOnly direct-join local results

Date: 2026-09-03.

Status: **LIVING-ROOT A/B/ERROR AND CURRENTONLY CONSTRUCTOR LOCALLY AND
INDEPENDENTLY ACCEPTED AS NECESSARY-ONLY; CASE C AND PRODUCTION EXPOSURE REMAIN
OPEN**.

## Outcome

The CurrentOnly root now retains the exact current actor in one non-optional,
private custody and drives the actor's existing cancel-safe borrowed join
directly. The extra root-local spawn, `JoinHandle`, actor-transfer gap, outer
task result, abort/detach cleanup claim, and second lifecycle Authority are
absent.

This document is the authoritative dated evidence ledger for the implemented
living-root A/B/error slice and the constructor behavior promoted with it. The
[direct-join amendment](2026-09-03-knife16-r7-current-only-direct-join-amendment.md)
remains the architecture contract.

## Scope and non-goals

The accepted Rust behavior is private to:

- `src/owned_upstream/quinn_leg_composition/owner/physical_session_root.rs`;
  and
- `src/owned_upstream/quinn_leg_composition/owner/physical_session_root/tests/current_only.rs`.

The existing private Module route in
`src/owned_upstream/quinn_leg_composition/owner.rs` remains at exactly `2062`
lines. Its final change in this closure was only rustfmt-stable Module ordering.

This result does not close or authorize:

- ultimate root destruction, called Case C;
- a production caller or initial-A Authority cutover;
- whole-root exact zero or a root-owned no-ACK finalizer;
- production terminal preservation;
- authenticated physical B topology or runtime restart credit;
- R7a-d, Task 4, or public TUN replacement; or
- macOS TUN, VPS, WAN, throughput, M2, or another external run.

## Exact implemented ownership model

`OwnerPhysicalSessionRoot<T>::CurrentOnly` always owns the same concrete
`StartedQuinnLegActor`. Its private actor phase has exactly four dispositions:

```text
Running
Joining { admitted_inventory }
Joined { admitted_inventory, exact_joined }
Failed { admitted_inventory, exact_error }
```

The actor is never optional and is never extracted into a root-local task.
`Joining` retains the exact admitted inventory while the actor remains in the
root. `Joined` retains the complete `QuinnLegActorJoined`. `Failed` retains the
complete `QuinnLegActorError` exactly once; repeat observations borrow that
error and return the stable root-level `ActorJoinQuarantined` category without
moving, cloning, or retrying actor join.

## Constructor and runtime rejection

The accepted constructor remains one exact-return transaction:

1. the initial seed is reversibly decomposed into actor, handoff, and
   controller;
2. all modeled precommit validation occurs over those exact parts;
3. every modeled rejection reconstructs the exact seed;
4. `tokio::runtime::Handle::try_current()` still gates the
   `RuntimeUnavailable` exact-return branch;
5. controller/output-root construction remains the final fallible operation;
   and
6. success moves immediately into the closed CurrentOnly root.

Removing the extra join task removed the stored Tokio handle, not the runtime
preflight or retained Target-executor inventory.

## Living-root settlement behavior

### B: monotonic pre-await unwind

After all fallible shutdown and FIFO checks pass, the root first publishes
sticky `Joining` while still holding the same actor. The deterministic
failpoint runs after that publication and before cancellation/direct join.
Unwind therefore leaves the same concrete actor and the same nonzero inventory
in root state. Re-entry can only roll that same actor forward; it cannot restore
`Running`, rebuild a seed, create another admission, or spawn another task.

### A: observer cancellation and re-entry

The root synchronously requests cancellation and then directly awaits
`StartedQuinnLegActor::join(&mut self)`. Its only await is Tokio `JoinSet`'s
cancel-safe `join_next`. Dropping a Pending observer ends only the mutable
borrow. The root still owns the same actor and actor-owned nursery; a later
observer re-enters the same join and consumes its result once.

### Root-first result retention

When direct actor join returns, the complete
`Result<QuinnLegActorJoined, QuinnLegActorError>` is synchronously moved into
`Joined` or `Failed` as the first root-side state action. There is no root-side
await, callback, logging, allocation, validation, or observer return between
the return and state write.

The deterministic inner-error tracer uses real actor/FIFO residual ownership
to produce a modeled error. It proves the exact error remains in `Failed`,
repeat observation is stable, and listener teardown ends with the exact
fail-closed `ListenerShutdownUnclean` result rather than an invented clean
shutdown.

Unknown unwind without a modeled error remains sticky and nonzero in
`Joining`; it does not fabricate `Failed` or zero.

## Focused and repeated evidence

Each focused tracer passed independently at `1/1`:

1. direct-join structure has no root-local task or actor extraction;
2. B retains the same actor after Joining/pre-cancel unwind;
3. A retains join custody across observer cancellation and re-entry;
4. the real inner join error is retained once and observed stably; and
5. the settled initial seed enters CurrentOnly with structural shutdown
   Authority.

The A, B, and inner-error tracers each passed ten serial exact repetitions:

```text
A           10/10
B           10/10
inner error 10/10
```

The structural lexical tracer is not a general Rust parser and is not treated
as standalone proof. It is accepted only together with these behavioral tests
and the bounded manual source audit for this exact revision.

## Regression, provenance, and governance evidence

- owner-initial accepted-root/transfer tests: `2/2`;
- continuation tests: `16/16`;
- actor join FIFO inventory tests: `2/2`;
- safe structural-shutdown regression: `1/1`;
- Knife16 provenance: `159/159`;
- repository test-Module provenance: `13/13`;
- format check: green;
- tracked and safe untracked diff/whitespace checks: green;
- `cargo check --lib`: green;
- standalone code-growth governance: `6/6`; and
- Cargo all-features code-growth governance: `6/6`.

Library commands emitted only the existing vendored smoltcp `22` warnings.
Knife16 provenance also emitted its existing harness-local unused/dead-code
warning set; none referenced the changed physical-session root or CurrentOnly
test leaf. No governance inventory changed.

The known old real-loopback actor test with
`ResidualOwnership { messages: 1, bytes: 0 }` was deliberately excluded. It is
separate ordered-delivery fixture debt and is not a direct-join regression or
an accepted clean result.

## Reproducible closure command ledger

### Environment and immutable input snapshot

The evidence repair reran the accepted local gates sequentially from:

```text
/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn
```

The final snapshot was recorded at `2026-09-03T22:35:40-0400` (`EDT`) with:

```text
rustc 1.95.0 (59807616e 2026-04-14)
cargo 1.95.0 (f2d3ce0bd 2026-03-21)
```

SHA-256 and line counts were captured before the focused run and repeated
after all behavior, provenance, format, check, and governance commands. The
three Rust hashes and line counts were identical at both boundaries:

```text
523afd841faddb5e482bb82d57174acf1339f56c686d3da94e46ba55da78cda1  owner.rs                                         2062
4f781ff970a89f5cde41c31372dad3d7d79442c5d46f74a01bce830f02cc16d6  owner/physical_session_root.rs                    722
930acfd7abc2cbba7632ebefa7c837f260a2f1e836bd27042e76d763bbed5ad7  owner/physical_session_root/tests/current_only.rs 691
74447883ad53e0c6dd05e8800839cb3efaf04f7e221d5aaf5cd3e53c31b6503a  Cargo.lock
1c59983c02e93b7da4104ab0a31921884fe1ce1f0b108a89ced7f15941239080  tests/code_growth_governance.rs
b988c0655eca940d884f925fa95c59aa50def818df3701114937e8357ec8d7aa  tests/knife16_owned_leg_provenance.rs
a8ddd615f4cb58f376ae78850d54a7899e892461e551920e93e92dedb27f9d6b  tests/repository_test_module_provenance.rs
```

### Ordered focused commands

The five focused commands ran in this order. Each selected exactly one test,
passed `1/1`, and reported `1590 filtered out`:

```sh
cargo test --lib 'owned_upstream::quinn_leg_composition::owner::physical_session_root::tests::current_only::current_only_direct_join_has_no_root_local_task_or_actor_extraction' -- --exact --nocapture --test-threads=1
cargo test --lib 'owned_upstream::quinn_leg_composition::owner::physical_session_root::tests::current_only::current_only_joining_pre_cancel_unwind_retains_same_actor_for_roll_forward' -- --exact --nocapture --test-threads=1
cargo test --lib 'owned_upstream::quinn_leg_composition::owner::physical_session_root::tests::current_only::current_only_observer_cancellation_retains_admitted_join_custody_for_reentry' -- --exact --nocapture --test-threads=1
cargo test --lib 'owned_upstream::quinn_leg_composition::owner::physical_session_root::tests::current_only::current_only_inner_join_error_is_retained_once_and_observed_stably' -- --exact --nocapture --test-threads=1
cargo test --lib 'owned_upstream::quinn_leg_composition::owner::physical_session_root::tests::current_only::settled_initial_seed_enters_current_only_root_with_exact_shutdown_authority' -- --exact --nocapture --test-threads=1
```

Console projection used `set -o pipefail` and retained `running`, final
`test result`, and `error` lines. Therefore a failing Cargo process could not
be hidden by the output filter.

### Exact serial-repeat wrapper

The repeat order was B, A, then inner error. This wrapper checked the Cargo
exit status and the exact nonzero summary on every iteration:

```sh
set -e
repeat_exact() {
  label="$1"
  test_name="$2"
  for iteration in {1..10}; do
    rc=0
    output=$(cargo test --lib "$test_name" -- --exact --nocapture --test-threads=1 2>&1) || rc=$?
    if [ "$rc" -ne 0 ]; then
      printf '%s\n' "$output"
      return "$rc"
    fi
    summary=$(printf '%s\n' "$output" | rg '^test result:' | tail -n 1)
    printf '%s\n' "$summary" |
      rg -q '1 passed; 0 failed; 0 ignored; 0 measured; 1590 filtered out'
  done
}
repeat_exact 'B' 'owned_upstream::quinn_leg_composition::owner::physical_session_root::tests::current_only::current_only_joining_pre_cancel_unwind_retains_same_actor_for_roll_forward'
repeat_exact 'A' 'owned_upstream::quinn_leg_composition::owner::physical_session_root::tests::current_only::current_only_observer_cancellation_retains_admitted_join_custody_for_reentry'
repeat_exact 'inner-error' 'owned_upstream::quinn_leg_composition::owner::physical_session_root::tests::current_only::current_only_inner_join_error_is_retained_once_and_observed_stably'
```

All `30/30` underlying Cargo invocations passed `1/1`; no loop relied only on
the final process status.

### Related regression and provenance commands

The related regressions then ran in this exact order:

```sh
cargo test --lib 'owned_upstream::quinn_leg_actor::tests::r7_initial_acceptance_settlement::initial_physical_root_adopts_the_prebuilt_controller_before_bounded_service' -- --exact --nocapture --test-threads=1
cargo test --lib 'owned_upstream::quinn_leg_actor::tests::r7_initial_acceptance_settlement::accepted_initial_root_parts_transfer_the_current_actor_once' -- --exact --nocapture --test-threads=1
cargo test --lib 'owned_upstream::quinn_leg_composition::owner::continuation::tests::' -- --nocapture --test-threads=1
cargo test --lib 'owned_upstream::quinn_leg_actor::tests::join_fifo_inventory::' -- --nocapture --test-threads=1
cargo test --lib 'owned_upstream::quinn_leg_actor::tests::r7_physical_attach_permit::r7_physical_attach_settlement_precedes_cancelled_structural_shutdown' -- --exact --nocapture --test-threads=1
```

Their selected results were respectively `1/1`, `1/1`, `16/16`, `2/2`, and
`1/1`. The two repository-level commands followed:

```sh
cargo test --all-features --test knife16_owned_leg_provenance -- --nocapture --test-threads=1
cargo test --all-features --test repository_test_module_provenance -- --nocapture --test-threads=1
```

They selected and passed `159/159` and `13/13`; neither relied on a substring
filter with zero selected tests.

### Format, diff, library, and governance commands

The source and library checks were:

```sh
cargo fmt --all -- --check
git diff --check
git diff --no-index --check /dev/null src/owned_upstream/quinn_leg_composition/owner.rs
git diff --no-index --check /dev/null src/owned_upstream/quinn_leg_composition/owner/physical_session_root.rs
git diff --no-index --check /dev/null src/owned_upstream/quinn_leg_composition/owner/physical_session_root/tests/current_only.rs
cargo check --lib
```

Format, tracked diff, and library check returned success. Each safe no-index
check returned the expected status `1` because the untracked source differs
from `/dev/null`, with empty diagnostic output; the wrapper rejected any other
status or any whitespace diagnostic. Cargo reported:

```text
warning: `smoltcp` (lib) generated 22 warnings
Finished `dev` profile
```

The accepted governance wrapper was:

```sh
set -e
set -o pipefail
evidence_tmp=$(mktemp -d /tmp/mini-vpn-r7-governance.XXXXXX)
rustc -D warnings --edition=2024 --test tests/code_growth_governance.rs \
  -o "$evidence_tmp/code-growth-governance"
"$evidence_tmp/code-growth-governance" --nocapture --test-threads=1
cargo test --all-features --test code_growth_governance -- \
  --nocapture --test-threads=1
```

The standalone and Cargo forms each selected six tests and passed `6/6`. An
earlier wrapper containing a temporary `rm -f` cleanup was rejected by command
policy before process creation. It ran no compiler or test and contributes no
evidence. The accepted wrapper above performed no repository write and left
only its isolated `/tmp` binary for normal temporary-directory reclamation.

### Warning fingerprints

The post-provenance Cargo fingerprint was:

```text
path   target/debug/.fingerprint/mini_vpn-76025782c4b5802d/output-test-integration-test-knife16_owned_leg_provenance
sha256 5f5bd90d0089670796e3b02cc97702af79a4285e6d4f226eb150991eadb26c22
```

It contained `143` warning diagnostics: `141` coded `dead_code`, one coded
`unused_imports`, and one final warning summary. A source-name scan found zero
`physical_session_root` or `current_only` matches. The two active smoltcp
feature fingerprints had identical content hash
`4f6e2e1169bd665013530c595faf83ecd577b01e9cbbd88a0fe69f4ecb4d6af6`;
each contained `23` diagnostic lines, comprising the `22` warnings and their
summary reported by Cargo.

No external environment, public TUN path, commit, or push contributed to this
ledger.

## Bounded source audit

The production root contains no extra `runtime.spawn`, `JoinHandle`,
`AdmissionUnknown`, actor-transfer quarantine, root-level outer task result,
optional actor slot, actor-moving `mem::replace`, abort, detach, or second join
Interface.

The audit also confirmed:

- the constructor retains `Handle::try_current()` and exact
  `RuntimeUnavailable` seed return;
- production calls direct actor join exactly once;
- actor join contains one await and a synchronous Ready tail;
- root stores the complete result before observation;
- repeat failure observation only borrows the retained exact error; and
- unknown unwind leaves the actor in sticky, nonzero `Joining`.

## Independent implementation review

The same independent Reviewer reported:

```text
P0/P1/P2 = 0/0/0
Verdict = Approve
Clean Architecture = 10/10
System Design = 10/10
Reliability/lifecycle = 10/10
```

The verdict is scoped only to the CurrentOnly constructor and living-root
A/B/error transaction described here. It does not waive Case C or promote this
private root to production reachability.

## Exact accepted line inventory

```text
src/owned_upstream/quinn_leg_composition/owner.rs                              2062
src/owned_upstream/quinn_leg_composition/owner/physical_session_root.rs         722
src/owned_upstream/quinn_leg_composition/owner/physical_session_root/tests/current_only.rs
                                                                                691
```

Both new leaves remain below their `1500/800` production/test limits.

## Acceptance boundary and next gate

This result closes the local constructor and living-root A/B/error subgate only.
Case C remains open because dropping an unjoined actor or its `JoinSet` aborts
work without awaiting settlement or retaining the exact result.

The next candidate is an architecture-only, symbol-first audit to identify the
real longer-lived production parent that could receive the whole CurrentOnly
root or its unique private actor-owning custody through one consuming move.
That audit and every later Rust change remain separately gated and are not
authorized by this result.

R7 and Task 4 remain incomplete and necessary-only. The public TUN route remains
`LegacyOwnedUpstream`.

## Source links

- [Current status](../current/STATUS.md)
- [Direct-join architecture amendment](2026-09-03-knife16-r7-current-only-direct-join-amendment.md)
- [CurrentOnly join-custody amendment](2026-09-03-knife16-r7-current-only-join-custody-amendment.md)
- [Post-extraction owner-root cutover amendment](2026-09-02-knife16-r7-post-extraction-owner-root-cutover-amendment.md)
- [Current physical-session root](../../src/owned_upstream/quinn_leg_composition/owner/physical_session_root.rs)
- [CurrentOnly focused tests](../../src/owned_upstream/quinn_leg_composition/owner/physical_session_root/tests/current_only.rs)
- [Quinn actor implementation](../../src/owned_upstream/quinn_leg_actor.rs)
- [Code-growth governance](../../tests/code_growth_governance.rs)
