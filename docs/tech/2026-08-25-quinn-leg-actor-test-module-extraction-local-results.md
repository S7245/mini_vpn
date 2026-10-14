# Quinn leg actor test Module extraction local results

Date: 2026-08-25.

## Outcome

This repository-maintenance stage is locally accepted as a behavior-preserving
Move Implementation refactor. The oversized inline test Implementation was
moved behind one stable private child Module seam:

```rust
#[cfg(test)]
mod tests;
```

The production Module Interface, visibility, logical test Module path, and all
production behavior remain unchanged. This stage implements no R7 policy,
transport, lifecycle, capacity, public-TUN, UDP, owner/server, or WAN behavior.

## Exact boundary change

| Artifact | Before | After |
|---|---:|---:|
| `quinn_leg_actor.rs` | `21,627 lines / 906,140B` | `5,762 lines / 199,915B` |
| External test Implementation | inline | `15,794 lines / 645,712B` |
| Actor tests | `106` | `106` |

The production-facing actor file is therefore smaller by `15,865` lines and
`706,225B`. The new test file remains a private `cfg(test)` child; no `pub`
Interface or runtime branch was added.

## Move-integrity evidence

- The old production prefix and the final first `5,760` lines have the same
  SHA-256:
  `9fb47be5c5da822dc7978de6a7e681582a176e2bbd75b469d814ab10e184a662`.
- Before rustfmt, the extracted body was byte-for-byte identical to the old
  inline body. Both had SHA-256:
  `267987d010c1368af4fcede4f82de268e1070849e5419ab64575b1eebe1e61e2`.
- Rustfmt then removed the obsolete wrapper indentation through syntax-aware
  formatting. The final test Implementation has SHA-256:
  `fe9de63864a091e9bb60d78fe8c5c76564c75f1cbfdc92613b8b105548be7031`.
- The normalized fully qualified actor test-name lists before and after are
  byte-identical: `106` names, SHA-256
  `3d9f25ae4dd7d06b8fef96a2fcfd9292b490a3efe9c454c4e874e70f2ddb4d82`.
  The only difference in the raw Cargo `--list` logs is the reported compile
  duration (`0.33s` versus `0.07s`).
- No `include!`, relative source include, `#[path]`, `file!`, `line!`, or
  `module_path!` dependency existed inside the moved Implementation. Certificate
  fixtures use `CARGO_MANIFEST_DIR`; the two sibling fixture references remain
  at the same logical Module path and compile successfully.

Final source SHA-256 values:

```text
1a2cb5d8cfcecf3a0ca1326aef055ea2ad4fe744549b285a6c7cd4abc6a79de7  src/owned_upstream/quinn_leg_actor.rs
fe9de63864a091e9bb60d78fe8c5c76564c75f1cbfdc92613b8b105548be7031  src/owned_upstream/quinn_leg_actor/tests/mod.rs
434aaa089429d68f0c006b6d430e6ad9da137f2b39ed5ddf4ea1012a5754cddc  tests/knife16_owned_leg_provenance.rs
```

## TDD and local validation

The provenance tracer was first changed to require the external private Module.
Its exact focused run failed RED because the terminal declaration did not yet
exist. After the mechanical move, the same tracer passed GREEN.

| Gate | Result |
|---|---:|
| External/private Module provenance tracer | RED as expected, then `1/1` GREEN |
| Quinn actor tests | `106/106` |
| Typed provenance | `145/145` |
| All targets/all features | library `1301 passed; 3 ignored`; main `2/2`; concurrency `10/10; 4 ignored`; provenance `145/145`; protocol `25/25`; session API `1/1` |
| Release all-target/all-feature check and build | PASS |
| Rustdoc | PASS with the established `15` mini_vpn warnings |
| Six-class current-baseline Clippy diagnostic | PASS |
| Rustfmt, `git diff --check`, link/file checks, and secret-shape scan | PASS |
| Independent final review | unresolved P0/P1/P2 `0/0/0` |

The provenance suite gained one structural guard, so its current count is
`145/145`. The R6d result's `144/144` remains correct historical evidence and
must not be rewritten.

## Strict Clippy baseline discrepancy

The recorded R6d gate says production-library Clippy passed with exactly four
allowances: `too_many_arguments`, `enum_variant_names`, `manual_clamp`, and
`collapsible_if`. That exact gate could not be reproduced on the current source
with Clippy `0.1.95` / rustc `1.95.0`: it reports ten
`result_large_err`/`large_enum_variant` sites in `client_controller.rs`,
`leg_io.rs`, `quinn_leg_composition.rs`, and `tcp.rs`.

This is not attributed to the test extraction:

- a Cargo fingerprint from approximately ten hours before the extraction
  records the same ten lint sites;
- the accepted R6d hashes of `client_controller.rs`, `leg_io.rs`, and
  `quinn_leg_composition.rs` exactly match the current files;
- none of the ten sites is in either file changed by this maintenance stage;
- the actor production prefix is byte-identical; and
- adding only those two observed baseline classes to the diagnostic command
  makes `-D warnings` pass with no additional mini_vpn lint class.

No unrelated production type was boxed, and no source-level or accepted-baseline
lint allowance was added; the two extra command-line allowances belong only to
the disclosed diagnostic. The historical four-allowance gate is therefore
recorded as **not reproducible** for this maintenance stage, while the six-class
run is diagnostic evidence only, not a replacement strict-gate claim. A separate
authorized lint-baseline audit is required before future work treats the
four-class command as authoritative.

## Review and scope limits

Independent read-only reviews found no unresolved P0/P1/P2 issue. The private
child Module retains the same `owned_upstream::quinn_leg_actor::tests::*`
identity and sibling access, and the production Interface is unchanged.

This result reduces production-source context cost; it does not reduce the total
repository test corpus and makes no throughput or continuity claim. R7-R9,
public TUN composition, owner/server, UDP two-leg delivery, WAN, throughput, and
the M2 one-second acceptance remain open. No macOS TUN, VPS, WAN, throughput,
M2, M3, user traffic, or external resource was used.
