# P2A bootstrap parameter convergence — local result

Status: P2A locally accepted; same independent Reviewer Approve, P0/P1/P2
`0/0/0`. P2T remains independently accepted. P2B/P3 and Case C/R7 remain open.

## Authority and boundary

The user's persistent P2A goal authorizes autonomous diagnosis and repair of
in-scope compile, test, format and review failures until this node closes.
The older routine first-red confirmation pause is waived only for P2A; retain
every causal failure and verification result. Architecture, exact line limits,
single-Writer ownership and separate inventory promotion remain mandatory.

Accepted contract: [Case C implementation plan](2026-09-04-knife16-r7-case-c-owner-server-root-implementation-plan.md)
§6/§11/§12. Predecessor: [P2T result](2026-09-05-knife16-r7-p2t-bootstrap-test-move-local-results.md).

Main owns feature source, tests and this result/compact status. Mencius owns
only the P2A governance inventory promotion after an exact Writer report.
Heisenberg remains independent Reviewer. Existing public/TUIC/vendored dirty
changes are frozen, not P2A work. No commit/push/external operation is authorized
by this goal. No P2B bootstrap extraction, P3, custody repair or Case C/R7 claim.

## Frozen source axis and acceptance

- `src/owned_upstream/quinn_leg_actor.rs`: private non-Clone bootstrap input,
  `run_server_bootstrap` and its sole `start_with_clock` caller only; preserve
  every input identity, evaluation boundary and existing cleanup/publication.
  Parent starts at 7,001 lines and must not grow; remove its function allowance.
- `src/owned_upstream/quinn_leg_actor/tests/bootstrap_custody.rs`: one real
  start/failure identity characterization; five existing bodies stay unchanged.
  Starts at 480 lines, limit 800. Actor test index stays byte-identical.
- `tests/knife16_owned_leg_provenance/bootstrap.rs`: code-aware allowance guard
  and adversarial fixtures, using existing scanner/syn machinery. Starts 374,
  limit 800. Integration parent may change only exact actor test count 109→110
  without line growth (2,626). Shared scanner is read-only.
- Mencius alone changes `tests/code_growth_governance.rs`: allowance 7→6 and
  any exact production line shrink; no new allowance or grandfathered path.

Failure predictions: the real characterization passes before and after the
refactor; source tracer first fails on the existing bootstrap allowance, then
passes without relocating it; production remains ≤7,001; governance rejects
only accurately reported shrink until Integrator ratchets it. An honest shape
that cannot fit requires a separately characterized headroom Move, never
unrelated compression or silently starting P2B. Ownership/interface divergence
requires architectural reassessment, not weakening the test.

Closure requires exact and existing bootstrap tests, full Knife16/repository
provenance, fmt/diff/check, standalone/Cargo governance, independent review and
factual compact status. Known unrelated real-loopback ordered-wait residual
debt is excluded; no whole-root zero or production readiness is inferred.

## Evidence

Logs: `/tmp/mini-vpn-p2a-9J9YQPEA/`.
Pre-write standalone `rustc -D warnings` governance: 6/6 passed.
Read-only rustfmt probes of naive aggregate shapes produced 7,009/7,010 lines;
neither candidate was written to the repository.

### Design feedback and resolved P1

Whole-input retention was rejected: an isolated compiler probe in
`drop-order.rs` observed entered-function parameter drops as
`clock/reservation/connecting`, versus whole-struct field drops as
`connecting/reservation/clock`. Full parameter destructuring restored the
former order. This is a language-shape probe, not a production ownership test.

Heisenberg's bounded candidate audit initially returned P0/P1/P2 `0/1/0`,
Request Changes: a line-count solution did not prove cold-state destruction.
The actual candidate keeps the original guard rebinding and completely
destructures input on callee entry. Its sole caller creates and directly awaits
the callee in the same running poll, without storing it or introducing an
intervening failure/cancellation point. The unpolled top-level start has not
constructed input. Cold outer-task destruction is safe independently of
unspecified closure capture drop order: input-first leaves the guard's slot
reference; guard-first leaves the reservation's reference. Sticky poison and
termination-counter release precede final slot release in either case.

The same Reviewer verified the actual two-hunk diff and closed that P1:
P0/P1/P2 `0/0/0`, Approve for bounded design only, with no test rerun or writes.
No architecture waiver or headroom Move was needed. The separate final
implementation/test review is recorded below.

### Implemented candidate and frozen evidence

The local enum-variant import keeps the two original `take()?` expressions
readable inside the named initializer. Connecting, reservation, Copy fields and
clock-last are evaluated in the original order. Guard/channel/runtime/spawn,
guard rebinding, result mapping and cleanup are unchanged. The private data
aggregate and its sole consumer are adjacent; no unrelated code or whitespace
was compressed. There is no derive, new forwarding Interface or heap allocation.

`run_server_bootstrap` body is byte-identical to the pre-P2A source, SHA-256
`d9197c7177f7c768825a96100f22962b845a44d0d97c8099d117fd2c5d0b1270`.
All five P2T unit blocks retain their prior individual hashes. Actor test index
is unchanged; integration parent changes only inventory `109→110` for the new
real test. The P2T provenance body also remains unchanged.

| Source | Lines | SHA-256 |
|---|---:|---|
| `src/owned_upstream/quinn_leg_actor.rs` | 7001 | `6910b8d47a71333d8f8b8c89fd7ba9e4f3c3459e28954f7d95404e43488dc103` |
| `src/owned_upstream/quinn_leg_actor/tests/bootstrap_custody.rs` | 614 | `78def6610d8490753e873a343bd1f61e082c63a81e6023ad5321a5a3a895ee5d` |
| `tests/knife16_owned_leg_provenance/bootstrap.rs` | 604 | `39c6d4f10a8a223ee5628de557323ec0fe2b1aa14dfc3f62149c9c8406efa9a6` |

New real test: `owned_upstream::quinn_leg_actor::tests::bootstrap_custody::server_bootstrap_parameter_aggregate_preserves_start_and_failure_identity`.
It exercises real leg B prepublication failure followed by leg A success on
the same one-slot listener, original reservation identity, endpoint retirement,
clock source, typed actor capacity, joined messages/bytes and clean listener.
It reads no parameter-aggregate fields and adds no production test Interface.

Source tracer: `bootstrap::server_bootstrap_parameter_aggregate_has_no_relocated_allowance`.
The first run compiled and failed only on the existing bootstrap allowance.
It now locks one private named aggregate, complete parameter destructuring,
the six remaining old function allowances, no inherited/broad allowance and no
relocation into any actor child. Adversarial fixtures distinguish comments/raw
strings from real direct/expect/cfg_attr, inherited and broad suppressions.
They reuse the existing shared scanner and `syn`; scanner source is unchanged.

| Gate / log | Result |
|---|---|
| `characterization-before.log`, `characterization-after.log` | new real test 1/1 before and after |
| `characterization-set-before.log`, `bootstrap-set-after.log` | all six bootstrap tests 6/6 before and after |
| `bootstrap-all-features.log` | all-features bootstrap regression 6/6 |
| `identity-repeat-1.log` through `identity-repeat-10.log` | exact serialized repeat 10/10 |
| `source-red.log` | causal allowance RED, 0/1; 160 filtered |
| `source-adversarial.log`, `source-green.log` | adversarial 1/1; full bootstrap provenance 3/3 |
| `knife16.log`, `repository-provenance.log` | 161/161; 13/13 |
| `fmt.log`, `diff.log`, `check.log` | global fmt, tracked diff, library check passed |
| `allowance-shrink.log` | predicted 5/6: only actor allowance inventory 7→6 |

`gates.sh` records the serial commands, including exact repeat identities.
Library commands retain 22 vendored smoltcp warnings; integration commands also
retain 142 existing embedded-source warnings. No new warning was observed.
Printed bootstrap-guard panic is expected and caught by the existing test.

### Independent inventory closure

Mencius changed only the actor allowance inventory `7→6` at governance line 58.
Inverse reconstruction restored the pre-P2A governance hash, proving all other
bytes, public/P2T debt and the 7,001-line production entry stayed unchanged.
Governance remains 699 lines / 25,227 bytes, SHA-256
`e31b587e5265088a8074747d7de947ac18873a0d2935e545f3ad61c0c34ca537`.

- `inventory-standalone-compile.log`: fresh `rustc -D warnings --edition=2024`
  test compile passed.
- `inventory-standalone.log`: standalone governance 6/6.
- `inventory-cargo-all-features.log`: Cargo all-features governance 6/6.

The independent Integrator stopped writing/testing. Main separately checked
all 26 unmodified public closure source hashes against the preserved manifest;
all matched. New/untracked P2A source whitespace and final newlines passed.
The five old unit blocks and old provenance block retain P2T hashes, and the
oversized unit index remains byte-identical.

### Final independent implementation review

Same Heisenberg: findings none, P0/P1/P2 `0/0/0`, Approve. The earlier design
P1 remains closed. Scoped structural quality/Module Depth/Authority Locality
are `9/9/10`; there is no further scoped optimization recommendation. These
are P2A scores, not whole-repository or Case C readiness scores.

Reviewer inspected the actual production/test diff, causal RED, before/after
characterization, repeats, regressions, provenance, governance and fmt/check
results and relevant log excerpts. Reviewer independently recomputed source
and immutable-block hashes, inverse count/allowance restoration, public 26/26
freeze and whitespace. Reviewer did not rerun Cargo/Rust tests or write files.
No complete-log-read claim is made.

Final acceptance: one private move-only parameter aggregate, entered-callee
drop semantics and cold-task lease invariants preserved, all five prior test
bodies unchanged, production still 7,001 lines, allowance debt lowered 7→6 by
the independent Integrator, all authorized gates green, no review findings.
The two active leaves are 614/604, both below 800. No new test index or public
Interface was introduced. Compact STATUS/HANDOFF/TODO name P2B as the next
separately authorized pure-Move boundary; this goal does not execute it.

No commit/push, public routing change, external run, headroom extraction,
production owner root, Case C or R7 completion is claimed.

Final light status-flip review: same Heisenberg returned findings none,
P0/P1/P2 `0/0/0`, Approve. Six documents, 15 local links and the approved source
hashes were rechecked without Cargo/tests/writes. Reviewer authorized final
handoff and P2A goal completion; this factual verdict transcription needs no
additional review cycle and authorizes no subsequent implementation.
