# Client TUN test Module extraction local results

Date: 2026-08-26.

## Outcome

This repository-maintenance stage is locally accepted as a behavior-preserving
Move Implementation refactor. The oversized inline `client_tun` test
Implementation was moved behind the unchanged private child Module seam:

```rust
#[cfg(test)]
mod tests;
```

The production Module Interface, visibility, logical test path, and production
behavior remain unchanged. This stage does not implement R7, switch the public
TUN route, change lifecycle or capacity behavior, or solve throughput, WAN, or
the M2 one-second uninterrupted acceptance failure.

## Exact boundary change

| Artifact | Before | After |
|---|---:|---:|
| `src/client_tun.rs` | `31,059 lines / 1,180,957B` | `16,613 lines / 648,986B` |
| External test Implementation | inline | `14,301 lines / 476,826B` |
| Fully qualified client-TUN tests | `338` | `338` |

The routinely read production file is smaller by `14,446` lines and
`531,971B`. The new file remains a private `cfg(test)` child; no public
Interface or runtime branch was added. The external test file is intentionally
still large and is a future test-organization concern, not part of this exact
move.

## Move-integrity evidence

- The original combined file had SHA-256
  `04970912b0643240bc018f0fc0586ec447c78cbebd63e9535225130ee47c1980`.
- The old production prefix and the final first `16,611` lines have the same
  SHA-256:
  `c5a0e9d436bd947aa16a53ff1ddeb421b6e24cd65dfa622ee2ef256b9d97ad98`.
- Before rustfmt, the extracted body was byte-for-byte identical to the old
  inline body: `14,445 lines / 531,968B`, SHA-256
  `a6f1671319526d7ee0465e9d6a3c7cde1f1c813398e31a053defdefa7b6f4fb9`.
- After the byte-identical extraction, rustfmt applied syntax-aware formatting
  and dedentation, including wrapper-adjacent blank-line normalization. The
  final external test Implementation has SHA-256
  `5c3e99d9b4d23cca3a05372d892e07b39b8f3530f316e5e3a64bbb88741342e2`.
- The normalized fully qualified test-name lists before and after are
  byte-identical: `338` entries, SHA-256
  `0e875ecf84b9eaf34533dd09f21ea35c17703b068a1528df2c28966fc7c37441`.
  The inventory contains `271` synchronous tests and `67` Tokio tests, with no
  ignored or conditionally compiled test in this Module.
- The moved body contains no nested test Module, `include!`, `#[path]`,
  `file!`, `line!`, `module_path!`, or relative source-file dependency. The
  logical path remains `client_tun::tests::*`.

Final source SHA-256 values:

```text
fb03df034ec66f6bea0f06bae9f0047bd1807a8e9c410d47e690f762a54ea0e1  src/client_tun.rs
5c3e99d9b4d23cca3a05372d892e07b39b8f3530f316e5e3a64bbb88741342e2  src/client_tun/tests/mod.rs
354dfd70ff287a98190cc06f4adecc6779b3d20dcf7ada39a6c45f336898fae1  tests/repository_test_module_provenance.rs
```

## TDD and local validation

A repository-layout tracer first required one terminal external private Module
and failed RED because the production source still contained the inline body.
After the exact move it passed GREEN. Review then deepened it to reject every
nested Rust Module declaration, preserving the fully qualified path as well as
the test inventory.

| Gate | Result |
|---|---:|
| External/private Module provenance tracer | expected RED, then `1/1` GREEN |
| Client-TUN focused tests | `338/338` |
| Pre/post fully qualified name inventory | exact `338/338` match |
| All tests compile, all features | PASS |
| All targets/all features | library `1301 passed; 3 ignored`; main `2/2`; concurrency `10/10; 4 ignored`; Knife16 provenance `145/145`; protocol `25/25`; session API `1/1`; repository provenance `1/1` |
| Release all-target/all-feature check and release build | PASS |
| Rustdoc | PASS with the established `15` mini_vpn warnings |
| Six-class current-baseline Clippy diagnostic | PASS |
| Rustfmt and `git diff --check` | PASS |
| Independent final review | unresolved P0/P1/P2 `0/0/0` |

The six-class Clippy run used `-D warnings` plus the currently disclosed
`too_many_arguments`, `enum_variant_names`, `manual_clamp`, `collapsible_if`,
`result_large_err`, and `large_enum_variant` command-line allowances. It is a
controlled current-baseline diagnostic, not a claim that the historical
four-class strict gate was reproduced. No source-level lint allowance or
unrelated production repair was added.

## Review and scope limits

The production prefix is byte-identical and every focused and broad local gate
passes. This result improves source locality and reduces routine context cost;
it does not reduce the total test corpus or change any data-plane behavior.

R7-R9, public TUN composition, owner/server, UDP two-leg delivery, WAN,
throughput, and M2 remain open. No macOS TUN, VPS, WAN, throughput, M2, M3,
user traffic, or external resource was used. No commit or push is claimed.
