# Knife15 Formal M2 Observer State Binding Repair — Local Results

Date: 2026-08-12

Status: **PASS locally — formal M2 traffic was not started; M3 remains blocked**

## Trigger Artifact

The exact-source `166c390` Mac artifact is
`/tmp/mini_vpn_knife15_macos_20260812_030150.tar.gz`, SHA-256
`692c6286e5cc9c107a20dbafe80824c6023ab67a9a2bee536d7398c095467bdd`.
It passed `start`, target-only smoke, the baseline/direct and network gates
that precede observer admission, then stopped at the formal M2 observer
preflight:

```text
ERROR: formal M2 requires a healthy matching 26-hour Exit observer; start it
before m2 and inspect .../m2-exit-observer-status.txt
```

The referenced `m2-exit-observer-status.txt` was exactly zero bytes. No M2
schedule, phase, DNS, real-client, or full-tunnel traffic started;
`formal_m2_acceptance` remained `NOT_RUN`. The requested `status/snapshot/stop`
cleanup completed and Endpoint conservation ended at `61,414/0/0B`.

## Root Cause

The observer itself was not the failure. The matching `.33` v2 observer was
active, healthy, configured for `93,600s`, and bound to Target
`43.130.32.77`, iperf port `5201`, and TUIC port `8443`. It was started about
three seconds after smoke and remained healthy after the Mac preflight
failed.

The Mac runner had an exact state-contract omission:

1. `parse_server()` derived `SERVER_PORT=8443` and the immutable manifest
   correctly recorded `exit_port=8443`.
2. `start_runner()` persisted Target, DNS Target, Exit host, and iperf port,
   but did not persist `server_port` in the root-owned runner state.
3. Formal `run_m2_action()` later read the absent `server_port`. Because the
   runner intentionally does not use shell `errexit`, the failed read left an
   empty value and preflight continued.
4. `m2_exit_observer_call()` rejected that empty TUIC port before invoking
   the observer script. Its old input guard emitted no diagnostic, producing
   the exact zero-byte evidence file and misleading generic error.

This rejects wrong observer timeout, observer process death, SSH-key loss,
Target/port mismatch, VPS failure, and operator ordering as causes.

## TDD Repair

- RED: the runner self-test used the same start-state boundary and could not
  read a persisted TUIC port.
- GREEN: `write_start_network_state()` now validates and persists Target, DNS
  Target, Exit host, TUIC server port, and iperf port as one startup binding.
- RED: an empty TUIC port failed observer admission but left a zero-byte
  evidence file.
- GREEN: invalid observer boundary inputs now leave a fixed, sanitized
  `<missing>` or `<invalid>` diagnostic without echoing untrusted state.
- The formal source floor moves to repair commit `1cdb17c`; exact source
  `166c390` and older descendants of the observer stage cannot start formal
  M2.

The unused remote observer was frozen and bundled at
`/tmp/mini_vpn_knife15_exit_target_observer_20260812_030240.tar.gz`, SHA-256
`785427201944d168327dfbb51630183a88ec660df551b4573c1c9b36d20450a5`.
It observed no M2 workload and left no active observer ownership. A final
`.33` check found sing-box active with zero restarts and both observer state
and the dedicated nftables table absent.

## Local Gates And Review

- Root library: `717 passed; 3 ignored`.
- Main binary: `2 passed`.
- Integration harness: `10 passed; 4 ignored`.
- Release build, established non-dependency Clippy, `cargo fmt --check`, shell
  syntax, Mac runner self-test, Exit observer self-test, and diff checks: PASS.
- Review verified one production startup writer for the network binding,
  sanitized pre-SSH error evidence, no observer ownership before successful
  status admission, and deliberate rejection of old-run compatibility. No
  unresolved P0/P1 remains in the changed runner/docs scope.

## Scope And Decision

No Rust production code, D16, MTU, pool, QUIC windows, chunk, Cubic, GSO,
Endpoint pacing, self-wake, workload rate/duration, UDP payload, or SLI
changed. This is a deterministic test-runner repair, not an architecture or
parameter change.

Do not reuse the stopped TUN run or the frozen observer. Pull the reviewed
pushed descendant, rebuild release, and take one fresh
`m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
observer start -> m2 -> status -> stop`. Start the observer only after smoke
and invoke `m2` within 900 seconds. Formal M2 still requires about 25 hours;
M3 remains blocked until formal M2 and cleanup pass.
