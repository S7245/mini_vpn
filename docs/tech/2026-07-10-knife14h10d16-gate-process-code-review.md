# Knife14 H10d16 Gate Process Code Review

Date: 2026-07-10

## Trigger

A fresh mature sing-box MTU1500 reverse P1 control again failed the Gate A
capability precondition at `12.843/10.905 Mbit/s` sender/receiver. Routing was
correct, both client UDP socket buffers were `16 MiB`, and socket drop was `0`.
Gate A was not run.

The requested review therefore examined whether the remaining plan could test
and deploy the wrong profile even after the external TUIC window recovers.

## Verdict

**Request changes to the gate proof and deployment process. Keep the D16
production architecture.**

The byte-owned queue, actor ownership, phase state machine, bounded TUN RX, and
EOF ordering remain coherent and locally exercised. The high-risk gaps are
between the tests, runtime profile, runner scripts, and isolated deployment.

## Findings

### P1 - Full real-Quinn test does not construct the Gate A runtime profile

`d16_real_quinn_full_tun_path_sustains_capacity_and_clean_eof` creates
`TunRuntimeConfig::from_sources(Some("2"))`. That constructor intentionally
uses default TUN MTU `1500` and leaves `h10d16_byte_owned_egress`, D3, and D5
flags false. The mock upstream directly returns `NativeByteOwned`, so the
D16 queue and auto-detected actor are reached, but the test is not the approved
safe1200 Gate A profile and does not prove its exact derived thresholds.

The existing `local_egress_actor_enabled` D16 queue check explains why the test
still proves real actor behavior. This is a gate-fidelity defect, not evidence
that the production actor is bypassed.

### P1 - The isolated commit and the acceptance runner are not one reproducible unit

A detached worktree at `1bf1f78` compiles successfully, so the committed D16
production code is deployable. However, that commit's
`knife14b-usclient-tunnel-suite.sh` has no H10d16 option, command export, or
profile report. The current working scripts contain the required D3-D6/D11/D16
flags, parser fields, Target out-of-band evidence, and self-tests only as
uncommitted diffs (`211+8` and `337+12` lines).

Consequently, running the suite entirely from a clean `1bf1f78` worktree would
not select the approved D16 profile. Copying the current scripts ad hoc would
select it, but would make the result depend on unversioned files.

### P2 - The mature-control precondition is not versioned

The temporary sing-box control correctly streams credentials through a FIFO
and cleans up, but it has been recreated for each run. That allowed MTU1200 and
the historical MTU1500 capability shape to be mixed during diagnosis. A Gate A
precondition must be a checked-in, secret-free script with a self-test, fixed
defaults, route assertions, socket-buffer/drop capture, artifact output, and a
cleanup trap.

### P2 - Real Quinn and TUIC profile selection are proven in separate tests

The full-path test starts from an already-open bare Quinn stream. Separate TUIC
tests prove that the H10d16 environment gate selects the direct ordered native
relay. No single deterministic test currently composes profile selection,
Gate A runtime values, real Quinn read service, the D16 actor, bounded TUN RX,
and EOF.

A full sing-box server fixture is unnecessary. The smallest correct seam is a
Gate A config constructor plus the existing real-Quinn `NativeByteOwned`
boundary, with a separate assertion that the production selector maps the
single H10d16 gate to that boundary.

## Architecture Decision

Do not replace the D16 architecture and do not reopen packet budgets, credit,
MTU/PLPMTUD, QUIC windows, pooling, chunk size, or self-wake. The review found
proof/deployment gaps, not a contradictory ownership design.

Production algorithm changes are allowed only if the exact-profile real-Quinn
tracer bullet becomes RED for capacity, drop, actor bypass, or EOF tail.

## Revised Plan

### R1 - Exact Gate A profile tracer bullet

Add one test-only Gate A config constructor that produces the approved profile:
pool `2`, TUN MTU `1200`, automatic derived watermarks, H10d16 enabled, and the
same non-legacy feature state used by the suite. Change the full real-Quinn
test to use it and assert the profile fingerprint before traffic.

RED must prove the old default config is not the Gate A profile. GREEN must
retain `>=170 Mbit/s`, 24-payload-packet bound, zero modeled drop/bypass, and
clean EOF. If this capacity assertion fails, diagnose that exact seam before
any VPS run.

### R2 - Version the acceptance runner

Review and commit the two current script diffs as a dedicated test/operations
change. Before staging, list the older included changes: stream/local-egress
parsers, D3-D6/D11/D16 flags, Target proxy evidence, help text, and self-tests.
Add a fail-fast startup fingerprint check and record the source commit, binary
hash, runner hashes, non-secret profile values, and actual TUN properties.

### R3 - Version the mature-control harness

Add a secret-free sing-box control script with `--self-test`. Use the historical
MTU1500 control as the capability precondition, stream configuration via FIFO,
assert target/exit routes, capture both UDP socket buffers/drop counters, and
always restore sysctls/TUN state. Never persist or print credentials.

### R4 - Clean deployment rehearsal and gates

From one clean detached worktree containing R1-R3, run local gates and a remote
build/smoke that validates startup fingerprints without spending Gate A. In a
new external window, require mature control receiver `>150 Mbit/s`; then run
exactly one `20s` safe1200 mini_vpn Gate A. Gate B remains conditional on a
clean Gate A.

## Stop Rule

Do not run another unchanged mature control in the current window. Do not run
Gate A until R1-R4 are versioned and the external capability precondition is
green.
