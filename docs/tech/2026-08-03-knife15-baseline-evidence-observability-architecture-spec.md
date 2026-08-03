# Knife15 Direct Baseline Evidence Observability Architecture Spec

Date: 2026-08-03

Status: **LOCAL IMPLEMENTATION ACCEPTED — observer repair only; M2/M3 blocked**

## 1. Accepted Evidence

The failed HK baseline directory is:

```text
/tmp/mini_vpn_knife15_macos_baseline_20260803_055550
```

Its files are byte-identical on the test Mac and analysis Mac:

```text
4fcfef103b8d1ba0524c470dd0163097b77234bc4eb7ef170cb825403805c65d  direct-forward.json
6659bf17f0725c9d71a072c5cf0c79bd4c0d8ef7ded41ac6e397f632d33530b5  direct-reverse.json
```

The test Mac used exact source `6231048`, runner SHA-256
`befb14c9...`, `jq-1.7.1-apple`, and a passing runner self-test. The analysis
Mac has the same runner hash and jq version. Replaying the complete production
baseline predicate against the synchronized bytes passes both directions.

The evidence itself is receiver-positive:

```text
forward receiver: 8.107 Mbit/s, no zero intervals
reverse receiver: 11.800 Mbit/s, no complete zero intervals
```

The reverse server output contains one final `0.305442s` zero row after the
20-second command boundary. It satisfies the already accepted numeric
partial-tail exception and is not a continuity failure.

This rejects old source, changed upload bytes, malformed JSON, a complete
receiver stall, low-rate rejection, and a broken current predicate. The
remaining failure is an unclassified validator invocation: the current runner
suppresses all jq stderr/status detail and writes no baseline manifest, so it
cannot distinguish evidence rejection from validator execution failure after
the fact.

## 2. Goal

Deepen the existing direct baseline evidence validator module so one small
interface owns:

1. the exact direction-aware receiver predicate;
2. structured `ok`, `invalid_evidence`, and `validator_error` classification;
3. forward/reverse pair classification;
4. an immutable-at-completion manifest with hashes and provenance;
5. a public read-only replay action using the same interface.

Formal baseline, direct-discriminator admission, fixtures, and operator replay
must not carry separate copies of the predicate.

## 3. Non-Goals And Frozen Decisions

This stage does not change:

- baseline duration, parallelism, Target, direction, iperf block sizes, or
  receiver-positive requirement;
- the final sub-`0.5s` numeric partial-tail rule;
- the 300-second direct discriminator, its derived 50% offered rate, freshness,
  route, or receiver-continuity contract;
- M2 workload, SLOs, IPv6/full-tunnel transaction, cleanup, or evidence counts;
- mini_vpn Rust, TUN, pool, MTU, H10d16/D16, Endpoint pacing, QUIC windows,
  Cubic, GSO, chunking, or self-wake.

There is no automatic retry, fixed sleep, evidence waiver, or acceptance of a
validator error. Every non-`ok` class remains fail-closed.

## 4. Deepened Module And Interface

The existing predicate remains the single implementation. Its internal jq
adapter returns its real exit status and stderr instead of discarding both.
The module exposes two shell interfaces:

```text
baseline_file_validation_reason(file, Target, reverse)
  -> ok | missing_or_symlink | invalid_evidence | validator_error_rc_<n>

baseline_pair_validation_reasons(directory, Target)
  -> <forward_reason> <reverse_reason>
```

The boolean `validate_m0_baseline_file/pair` wrappers remain for existing
callers but delegate to this interface. This preserves locality: predicate,
classification, production use, replay, and fixtures change together.

The public `baseline-check` action selects exactly one stage baseline through
the existing `M0_BASELINE_DIR` / `M1_BASELINE_DIR` / `M2_BASELINE_DIR`
interface. It performs no network command or mutation. It prints schema,
current source/runner, Target, file hashes, both reasons, and the receiver
summary, then passes only for `ok/ok`.

Formal `baseline` writes `manifest.txt` after both commands and before its
terminal result. The manifest records:

- schema/status/reason and completion time;
- current source, runner hash, Target, route, duration, and parallelism;
- both JSON hashes and both structured validation reasons;
- receiver summary when available.

Command failures also attempt a manifest with the corresponding reason and
unknown/missing fields. The raw JSON remains authoritative; the manifest makes
the exact invocation class and provenance replayable.

## 5. Safety And Evidence Invariants

1. `baseline-check` is read-only and never starts TUN, iperf, routes, DNS, or
   another VPN process.
2. Validator execution failure cannot collapse into `invalid_evidence` or
   PASS.
3. Invalid evidence cannot become PASS through a second implementation.
4. A final proven partial tail remains accepted; a nonterminal, `>=0.5s`,
   missing-timing, malformed, or complete zero interval remains rejected.
5. File hashes and current source/runner provenance are printed even when
   validation fails, when the files are readable.
6. Formal baseline writes the terminal manifest once per run result and does
   not modify the JSON produced by iperf.
7. Existing baseline directories without a manifest remain readable by the
   explicit replay action and direct validator; this observer repair does not
   retroactively rewrite evidence.

## 6. TDD And Acceptance

Focused RED/GREEN fixtures must prove:

1. the exact uploaded forward/reverse files classify `ok/ok`;
2. a complete receiver zero classifies `invalid_evidence`;
3. malformed JSON classifies `validator_error_rc_5` with the pinned jq, not
   invalid evidence, while the interface preserves any actual non-1 status;
4. a missing/symlink file classifies separately;
5. a final `0.305s` tail remains `ok`;
6. public help and the external wrapper expose `baseline-check`;
7. a formal fixture writes a parseable status/reason/hash/provenance manifest;
8. baseline-check failure remains nonzero and read-only.

Local acceptance requires Knife15 internal/external self-tests, Bash 3.2
syntax, formatting/diff/secret checks, and review with no unresolved P0/P1.
No Rust or VPS gate is required because no data-plane code changes.

## 7. Stop Rule

This repair explains future invocations; it does not fabricate the missing
status from the already completed run. The uploaded JSON may be classified as
valid data, but if IPv6 was restored after the pre-start failure it cannot be
reused across another disable/link-reset boundary. A new M2 attempt still
requires fresh baseline/direct evidence in one qualified network window.
