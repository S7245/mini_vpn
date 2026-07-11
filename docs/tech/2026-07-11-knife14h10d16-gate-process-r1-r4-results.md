# Knife14 H10d16 Gate Process R1-R4 Results

Date: 2026-07-11

## Verdict

R1-R4 are complete. The review defects were proof/deployment drift, not a
contradiction in the byte-owned egress architecture. No new production tuning
was required, and no Gate A iperf run was consumed.

The next allowed action is one versioned mature sing-box capability control in
a genuinely new external TUIC window. It must exceed `150 Mbit/s` receiver with
zero client and Exit UDP socket drops before one safe1200 Gate A may run.

## R1 - Exact local Gate A profile

Commit: `c544e49`

The full real-Quinn TCP/smoltcp/TUN test now constructs the exact approved
profile: pool `2`, MTU `1200`, production automatic watermarks, H10d16 enabled,
and legacy D3-D6 diagnostics disabled. It retained complete `32 MiB` delivery
above `170 Mbit/s`, the 24-payload-packet flush bound, zero modeled drop, zero
actor bypass, and clean EOF in `30/30` repeats.

## R2 - Versioned mini_vpn acceptance runner

Commit: `0d964ac`

The accumulated H4/H10/D3-D6/D11/D16 runner and parser changes are now in the
same versioned unit as the binary. The suite fails before traffic unless it sees
the exact safe1200 startup fingerprint, and its artifact records full source
commit plus binary, suite, and probe SHA-256 values. A startup-only mode verifies
profile, MTU, and routes without iperf.

## R3 - Versioned mature-client control

Commit: `ec112a9`

The mature control fixes the historical capability shape at MTU1500, reverse,
P1, and 20 seconds. It asserts target-only routing, captures both client and Exit
UDP socket buffers/drops, and fails closed below `150 Mbit/s` or on any socket
gate failure. TUIC configuration is streamed through a mode-0600 FIFO; cleanup
restores socket sysctls and removes the FIFO/TUN. Artifacts are deleted if a
credential value is ever detected.

The script self-test passed locally and on `.27`. The actual 20-second control
was deliberately not repeated in the already-proven incapable window.

## R4 - Clean local and remote rehearsal

Clean local worktree at `ec112a9`:

- default library: `586/586`;
- harness library: `595/595`;
- concurrency harness: `10 passed`, `4` existing ignored;
- default and harness checks: pass;
- clippy: pass with existing warnings;
- focused D16 Rust formatting and diff-check: pass;
- low-RTT probe, suite, and control self-tests: pass.

The full repository format check remains red in previously committed
Reality/DNS/failover files outside this stage. No unrelated formatting was
mixed into the intentionally dirty primary worktree.

Remote `.27` clean-worktree rehearsal:

- source commit: `ec112a9de64140687468da64463ee60a420e6f4c`;
- source dirty: `0`;
- release build: pass;
- exact safe1200 H10d16 fingerprint: verified;
- pool: `2`, TUN MTU: `1200`, tx queue length: `500`;
- target `.77`: routed through `tun0`;
- Exit `.33`: remained on `eth0`;
- idle TUN TX drop: `0`;
- cleanup: mini_vpn stopped, TUN removed, both routes restored to `eth0`.

The remote artifact is under
`/tmp/mini_vpn_h10d16_r4_ec112a9_artifacts/`. No credential value is recorded
in this document.

## Final review

No new correctness, ownership, lifecycle, bounded-backpressure, TCP/TUN, UDP,
or cross-platform-core blocker was found in the R1-R4 diff. The runner's
best-effort `--help` snapshot logs a handled panic because the binary exposes no
help subcommand; it does not change the rehearsal result. Full-repository format
debt is separate from D16 and should be handled only with an explicit strategy
for the overlapping user changes.

Gate B remains frozen. Do not change MTU/PLPMTUD, broad QUIC windows, pool size,
chunk size, self-wake, or VPS configuration before the new-window mature
control discriminator.
