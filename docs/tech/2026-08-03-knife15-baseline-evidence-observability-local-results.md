# Knife15 Direct Baseline Evidence Observability Local Results

Date: 2026-08-03

Status: **LOCAL COMPLETE — observer repair accepted; M2/M3 blocked**

## 1. Frozen Evidence

The uploaded HK directory
`/tmp/mini_vpn_knife15_macos_baseline_20260803_055550` used source
`6231048`, runner SHA-256 `befb14c9...`, and `jq-1.7.1-apple`. Its synchronized
forward and reverse files were byte-identical to the test Mac:

```text
4fcfef103b8d1ba0524c470dd0163097b77234bc4eb7ef170cb825403805c65d  direct-forward.json
6659bf17f0725c9d71a072c5cf0c79bd4c0d8ef7ded41ac6e397f632d33530b5  direct-reverse.json
```

The complete production predicate replayed `ok/ok`. Forward and reverse
receiver rates were `8.107/11.800 Mbit/s`; neither direction contained a
complete receiver-zero interval. The reverse final `0.305442s` zero row was
the already accepted numeric partial tail.

The original command nevertheless reported the generic baseline error and
preserved neither the jq status nor a terminal manifest. The raw evidence
therefore rejects a continuity failure, but the historical invocation remains
unclassified. This stage repairs future observation; it does not relabel the
old command result or authorize evidence reuse across a network/IPv6 change.

## 2. Implemented Observer Contract

One direction-aware predicate now supplies structured reasons:

```text
ok
invalid_evidence
missing_or_symlink
validator_error_rc_<n>
```

The existing boolean file/pair validators delegate to the same interface, so
formal baseline, direct admission, fixtures, and replay cannot drift into
separate predicates.

Formal `baseline` writes `manifest.txt` for forward-command failure,
reverse-command failure, evidence rejection, validator failure, and PASS. It
records source/runner, Target/route, duration/parallelism, both hashes,
directional reasons, and receiver summary without changing the raw JSON.

The public `baseline-check` action selects exactly one configured stage
directory and performs a read-only replay. It starts no TUN, iperf, route,
DNS, or other network mutation and passes only for `ok/ok`.

## 3. TDD And Local Gates

Focused fixtures cover:

- valid forward/reverse pair -> `ok/ok`;
- complete receiver-zero interval -> `invalid_evidence`;
- malformed JSON -> the real pinned jq validator error;
- missing and symlink evidence -> `missing_or_symlink`;
- proven final partial tail -> `ok`;
- replay schema/status/hashes/reasons;
- parseable terminal manifest;
- public help and external wrapper exposure.

Local gates:

```text
bash scripts/knife15-macos-soak.sh --self-test       PASS
bash scripts/knife15-macos-soak-self-test.sh         PASS
bash -n scripts/knife15-macos-soak{,-self-test}.sh   PASS
git diff --check                                     PASS
```

The expected internal `ERROR: command exceeded hard timeout of 1s` line is a
self-test fixture; the terminal
`passfailpassknife15 macOS runner self-test passed` proves the runner test
completed successfully.

## 4. Review

No unresolved P0/P1 remains. Validator non-execution cannot become PASS,
invalid evidence cannot be reinterpreted by a second predicate, symlinks stay
rejected, and the replay action is read-only. Bash 3.2-compatible shell
constructs and the existing `/tmp` path restrictions are preserved.

No Rust data plane, M2 workload, SLO, pool, MTU, D16, Endpoint pacing, QUIC
window, Cubic, GSO, chunk, or self-wake value changed.

## 5. Next Position

The observer repair is complete, but it cannot recover the missing historical
validator status. A later M2 attempt still requires a fresh qualified
`m2-ipv6-check -> baseline -> direct-discriminator` transaction. This result
does not unblock M3 and does not authorize reuse of the old baseline after
IPv6 or the physical link was restored.
