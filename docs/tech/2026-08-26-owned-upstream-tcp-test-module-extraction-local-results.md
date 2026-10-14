# Owned-upstream TCP test Module extraction local results

Date: 2026-08-26.

## Outcome

This repository-maintenance stage is locally accepted as a behavior-preserving
Move Implementation refactor. All `65` inline `owned_upstream::tcp` tests now
live behind the unchanged private child Module seam:

```rust
#[cfg(test)]
mod tests;
```

The production prefix, three parent-local `cfg(test)` helpers, private
visibility, and fully qualified `owned_upstream::tcp::tests::*` Interface are
unchanged. No TCP relay, replay, queue, lifecycle, capacity, ownership, TUN, or
runtime behavior was modified.

## Exact boundary change

| Artifact | Before | After |
|---|---:|---:|
| `src/owned_upstream/tcp.rs` | `5,823 lines / 220,800B` | `2,812 lines / 98,389B` |
| External test Implementation | inline | `2,973 lines / 110,738B` |
| Fully qualified TCP tests | `65` | `65` |
| Legacy inline test Modules | `45` | `44` |

The routinely read production file is smaller by `3,011` lines and `122,411B`.
The external file remains a private `cfg(test)` child. Its size may later be
organized by ownership responsibility, but that is separate from this exact
move.

## Move-integrity evidence

- The original combined file was `5,823 lines / 220,800B`, SHA-256
  `8084091b0d1887182459074dc4c0181da9a858074666508e92bd6e6b1945a759`.
- The old production prefix and final first `2,810` lines are byte-identical:
  `2,810 lines / 98,365B`, SHA-256
  `07451c356c65793cff89a341c651d49f82e73b7118cae3001742cf3f12b29d72`.
- Before rustfmt, the extracted body was byte-for-byte identical to the old
  inline body: `3,010 lines / 122,408B`, SHA-256
  `6911b14040c96104fd4e97dc0f88781a84c03acd469e99fd07952d6b63a33d1f`.
- The final parent has SHA-256
  `d2781f3fd878766776fb1167ec1c9110f9d1fe2a4bcd3862c8e8d2e7815a1aaa`.
- The syntax-aware formatted external body has SHA-256
  `a64013365c65b58b163b3c21bcf45ed185388ec648dee69a2fbb9a8cbe411871`.
- The normalized sorted Cargo test-name inventories before and after are exact:
  `65` entries, SHA-256
  `6079288299d0f87164d4edcab9901b9748c05b7b4a4d07c9cecf0afd6b3b7fdb`.
  The inventory contains `61` synchronous and `4` current-thread Tokio tests.
- The three parent-local test-support helpers at the old prefix lines `473`,
  `490`, and `567` remain in the byte-identical production prefix. They are not
  test bodies and remain reachable from the private child Module.
- The moved body contains no `include!`, `#[path]`, nested Module, `file!`,
  `module_path!`, or relative source-file dependency. Its logical path remains
  `owned_upstream::tcp::tests::*` and `use super::*` retains private access.

Final source SHA-256 values:

```text
d2781f3fd878766776fb1167ec1c9110f9d1fe2a4bcd3862c8e8d2e7815a1aaa  src/owned_upstream/tcp.rs
a64013365c65b58b163b3c21bcf45ed185388ec648dee69a2fbb9a8cbe411871  src/owned_upstream/tcp/tests/mod.rs
074f2c1376df45cca74aca4f8d0f8296ae40bebd241760a59805d267e0bbf6c6  tests/repository_test_module_provenance.rs
```

## TDD and local validation

The repository-layout tracer first required one terminal external private
Module and failed RED while the production source still contained its inline
body. The exact move then passed GREEN. The global shrink-only allowlist retired
only `src/owned_upstream/tcp.rs::tests`, from `45` entries to `44`.

| Gate | Result |
|---|---:|
| External/private Module provenance tracer | expected RED, then GREEN |
| Repository-layout provenance | `5/5` |
| TCP focused tests, dev | `65/65` |
| TCP focused tests, release | `65/65` |
| Pre/post fully qualified name inventory | exact `65/65` match |
| All tests compile, all features | PASS |
| All targets/all features | library `1301 passed; 3 ignored`; main `2/2`; concurrency `10/10; 4 ignored`; Knife16 provenance `145/145`; protocol `25/25`; session API `1/1`; repository provenance `5/5` |
| Release all-target/all-feature check and release build | PASS |
| Rustdoc | PASS with the established `15` mini_vpn warnings |
| Six-class current-baseline library and provenance-target Clippy | PASS |
| Rustfmt, scoped diff, whitespace, and secret-shape checks | PASS |
| Independent final read-only review | unresolved P0/P1/P2 `0/0/0` |

The six-class Clippy runs used `-D warnings` plus the currently disclosed
`too_many_arguments`, `enum_variant_names`, `manual_clamp`, `collapsible_if`,
`result_large_err`, and `large_enum_variant` command-line allowances. This is a
controlled current-baseline diagnostic, not a claim that the historical
four-class strict gate was reproduced. No source-level lint allowance or
unrelated production repair was added.

## Scope limits

This result improves source locality and preserves the executable test-layout
ratchet. It does not implement R7, complete Knife16 Task 4, switch public TUN
from the legacy upstream, create the owner/server or second ingress, solve WAN
or throughput, or fix the M2 one-second uninterrupted acceptance failure.

No macOS TUN, VPS, WAN, throughput, M2, M3, user traffic, or external resource
was used. No commit or push is claimed.
