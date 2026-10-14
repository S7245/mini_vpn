# Owned-upstream two-leg harness test Module extraction local results

Date: 2026-08-26.

## Outcome

All `9` inline `owned_upstream::two_leg_harness` tests now live behind one
private external child Module seam:

```rust
#[cfg(test)]
mod tests;
```

The normalized test body, fully qualified test identities, private parent
access, and behavior are preserved. The production file no longer embeds the
test Implementation, and the repository's shrink-only legacy inline-test
backlog advances from `44` to `43`.

The parent file also contains intentional R7 controller-API adaptations from
the same working stage. Therefore this result does **not** claim that the whole
HEAD-to-current production prefix is byte-identical; it proves the extraction
body and Module/test Interface independently.

## Exact boundary change

| Artifact | HEAD / before | Current / after |
|---|---:|---:|
| Parent file | `2,559 lines / 99,302B` | `2,070 lines / 80,121B` |
| Inline Module / external test file | `500 lines / 19,629B` | `494 lines / 17,676B` |
| Fully qualified tests | `9` | `9` |
| Legacy inline test Modules | `44` | `43` |

The parent file is smaller by `489` lines and `19,181B`. All nine tests are
synchronous, non-ignored tests and retain the logical path
`owned_upstream::two_leg_harness::tests::*`.

## Move-integrity evidence

- Parent HEAD SHA-256:
  `7266eb18cde9c538363bae8ee5081373c4a115a1683732cd73445fbaa6609180`.
- Parent current SHA-256:
  `066be3239c321ff32493ab46fb75dc7f1bf54ce80adfb18f84eb4fb20ee3c2bf`.
- Removing the old wrapper/one-level indentation and formatting with the
  project's Rust 2024 edition yields a body byte-identical to the current
  external file: `494 lines / 17,676B`, SHA-256
  `74d7b604342fe8b809e8757b586cf11c8ab8f50f9363555d690a778f7f8d812c`.
- The sorted before/after test-name inventories are exact: `9` entries,
  SHA-256
  `6e6629280999867beca28746614fe906144fe159890c1f6794990c510ecf0efc`.
- The parent has one terminal `#[cfg(test)] mod tests;` declaration and no
  inline `mod tests {}` or direct test item.
- The child begins with `use super::*;` and contains no nested Module,
  `include!`, `#[path]`, `file!`, or `module_path!` dependency.
- The repository ratchet removes exactly
  `src/owned_upstream/two_leg_harness.rs::tests`; the exact current legacy list
  has `43` entries.

For clarity, the old production prefix was `2,059 lines / 79,673B`, SHA-256
`7e891c943fe072aa213397d196387736bd4d38f5cb71a296d3bb4ea20497873c`;
the current prefix is `2,068 lines / 80,097B`, SHA-256
`f963fa0c9289f175dabd70bf3841db4a88ad09df4913f047d8803213e0b55dde`.
The `13` added and `4` removed lines are the separately tested R7 controller API
adaptation, not extraction-body drift.

## Validation

| Gate | Result |
|---|---:|
| Two-leg harness focused, debug | `9/9` |
| Two-leg harness focused, release | `9/9` |
| Repository test-layout provenance | `6/6` |
| All targets/all features | library `1323 passed; 3 ignored`; main `2/2`; concurrency `10/10; 4 ignored`; Knife16 provenance `145/145`; protocol `25/25`; session API `1/1`; repository provenance `6/6` |
| Release checks/build, current-baseline Clippy, rustdoc | PASS |
| Rustfmt and `git diff --check` | PASS |
| Independent read-only extraction review | unresolved P0/P1/P2 `0/0/0` |

## Scope limits

This extraction reduces routine production-source context and enforces the
project's test-layout rule. It does not by itself change two-leg behavior,
complete physical R7, finish Knife16 Task 4, switch public TUN, or solve WAN,
throughput, or M2. The adjacent R7 API changes are accepted only under the
separate R7 checkpoint result.

No macOS TUN, VPS, WAN, throughput, M2, M3, user traffic, commit, or push is
claimed.
