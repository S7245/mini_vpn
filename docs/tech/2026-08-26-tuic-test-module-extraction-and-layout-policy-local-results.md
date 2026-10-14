# TUIC test Module extraction and layout policy local results

Date: 2026-08-26.

## Outcome

This repository-maintenance stage is locally accepted as a behavior-preserving
Move Implementation refactor. All `162` inline TUIC tests now live behind the
unchanged private child Module seam:

```rust
#[cfg(test)]
mod tests;
```

The production prefix, private visibility, fully qualified `tuic::tests::*`
Interface, and separate `tuic::direct_probe::*` seam are unchanged. No TUIC,
QUIC, TCP, UDP, lifecycle, capacity, TUN, or runtime behavior was modified.

The project now also has an executable two-tier test-layout rule:

- private unit-test bodies live in `src/<module>/tests/mod.rs` and child Modules;
- root `tests/` owns integration, public-contract, and repository-provenance
  tests;
- `src/lib.rs` and `src/main.rs` use distinct explicit `#[path]` seams when
  externalized, because both crate roots otherwise resolve a bare `mod tests;`
  through the shared `src/tests.*` location; and
- the exact legacy directly declared positive-cfg inline-test Module allowlist
  is shrink-only.

## Exact boundary change

| Artifact | Before | After |
|---|---:|---:|
| `src/tuic.rs` | `13,696 lines / 520,893B` | `8,172 lines / 311,114B` |
| External test Implementation | inline | `5,499 lines / 189,112B` |
| Fully qualified TUIC tests | `162` | `162` |
| Legacy inline test Modules | `46` | `45` |

The routinely read production file is smaller by `5,524` lines and `209,779B`.
The new external file remains intentionally private and `cfg(test)`-only.

## Move-integrity evidence

- The original combined file was `13,696 lines / 520,893B`, SHA-256
  `5ac9dff0f4a1410332486d8154f1e500939671ecbc5b124352dfe8908eabe15a`.
- The old production prefix and final first `8,170` lines are byte-identical:
  `8,170 lines / 311,090B`, SHA-256
  `4900c6573e21bf39747dded9fca100dfa5b665a73b6c90e18212c36a2c3f684d`.
- Before rustfmt, the extracted body was byte-for-byte identical to the old
  inline body: `5,523 lines / 209,776B`, SHA-256
  `bea01e23fc43f435a13d57b96cd1777a7bd29414eb26eb8aadc117d38802f55f`.
- The final parent has SHA-256
  `c39d248e9c3ec9108f194a38319e9ee200580b92d9caf3a6f36558d91540d330`.
- The syntax-aware formatted external body has SHA-256
  `416a5b68e1a351900ae1d7f42998863062fad09944a6e272fbb4ef407956affc`.
- The normalized sorted Cargo test-name inventories before and after are exact:
  `162` entries, SHA-256
  `26f3abf30419da0e0e3d7fe97e247626fa086fe2b6d43496ca3c383be5217cdc`.
  The inventory has `131` synchronous and `31` Tokio tests, with no ignored or
  conditionally excluded test in this Module.
- `#[path = "tuic_direct_probe.rs"] mod direct_probe;` remains in the unchanged
  parent prefix, so its logical path and file resolution did not move.

Final source SHA-256 values:

```text
c39d248e9c3ec9108f194a38319e9ee200580b92d9caf3a6f36558d91540d330  src/tuic.rs
416a5b68e1a351900ae1d7f42998863062fad09944a6e272fbb4ef407956affc  src/tuic/tests/mod.rs
2437880a9da85ec80ad20ece7da6b63e0ce420d6393192a74d06da89d5742e50  tests/repository_test_module_provenance.rs
```

## Executable layout ratchet

The repository provenance target recursively scans production Rust sources and
excludes every `tests` subtree. It recognizes inline Modules behind exact,
composite, multi-line, and same-line positive `test` cfg expressions, including
double negation. Focused cases reject `not(test)`, a feature string named
`"test"`, external `mod ...;`, and apparent attributes inside strings or
comments.

The sorted actual inventory must equal the exact `45`-entry legacy list. A future
extraction removes one entry; a new directly declared positive-cfg inline test
Module is unexpected and fails the gate. The inventory deliberately includes
nonstandard legacy names such as `test_support`, `queue_classifier_tests`, and
`standby_control_tests`.

The cfg scanner tracer first failed RED because composite and same-line forms
were missed. A final review then found that the extracted-body nested-inline-
Module guard was line-based: it missed attributed same-line declarations and
counted apparent declarations inside raw strings. A second focused tracer
reproduced that failure RED; the repaired Rust-aware scanner distinguishes code
from attributes, comments, strings, byte/C strings, and their raw forms. It
rejects an extra inline wrapper while continuing to allow deliberate external
child Modules under `src/<module>/tests/`. The complete repository-layout target
passes `4/4`.

This executable gate intentionally claims only directly declared positive-cfg
inline Modules. Standalone test items and `cfg_attr`- or macro-generated test
code remain prohibited by repository policy and review; this lexical gate does
not claim to prove their absence.

## Local validation

| Gate | Result |
|---|---:|
| TUIC extraction/layout tracer | expected RED, then GREEN |
| Repository-layout provenance | `4/4` |
| TUIC focused tests, dev | `162/162` |
| TUIC focused tests, release | `162/162` |
| Pre/post fully qualified name inventory | exact `162/162` match |
| All tests compile, all features | PASS |
| All targets/all features | library `1301 passed; 3 ignored`; main `2/2`; concurrency `10/10; 4 ignored`; Knife16 provenance `145/145`; protocol `25/25`; session API `1/1`; repository provenance `4/4` |
| Release all-target/all-feature check and release build | PASS |
| Rustdoc | PASS with the established `15` mini_vpn warnings |
| Six-class current-baseline library and provenance-target Clippy | PASS |
| Rustfmt | PASS |
| Scoped diff/whitespace/link/file/secret-shape checks | PASS |
| Independent final read-only review | unresolved P0/P1/P2 `0/0/0` |

The six-class Clippy runs used `-D warnings` plus the currently disclosed
`too_many_arguments`, `enum_variant_names`, `manual_clamp`, `collapsible_if`,
`result_large_err`, and `large_enum_variant` command-line allowances. This is a
controlled current-baseline diagnostic, not a claim that the historical
four-class strict gate was reproduced. No source-level lint allowance or
unrelated production repair was added.

## Structural assessment

- Production seam depth/locality: `9/10`; the private Interface is one stable
  declaration and routine production reads no longer load 5,499 test lines.
- Behavior-preservation confidence: `9/10`; prefix, path, visibility, test names,
  focused behavior, and broad targets are independently checked.
- External test cohesion: `5/10`; the file is still large, so later organization
  by TUIC responsibility may help, but that is a separate behavior-preserving
  stage rather than part of this exact move.

## Scope limits

This result improves source locality and makes the future layout enforceable. It
does not complete Knife16 Task 4, implement R7, switch public TUN from the legacy
upstream, create the owner/server or second ingress, solve WAN/throughput, or fix
the M2 one-second uninterrupted acceptance failure.

No macOS TUN, VPS, WAN, throughput, M2, M3, user traffic, or external resource
was used. No commit or push is claimed.
