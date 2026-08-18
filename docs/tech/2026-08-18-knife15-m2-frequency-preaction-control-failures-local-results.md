# Knife15 M2 Frequency Pre-Action Control Failures And Local Repair

Date: 2026-08-18

Status: **TWO PRE-ACTION ATTEMPTS INVALID; LOCAL TDD/REVIEW PASS; FRESH SOURCE REQUIRED**

## Outcome

No Tier-B epoch started and no quality verdict was produced. Two exact-source
`ce164e621019addecab329465d64b3d842440704` attempts reached `start` and
`smoke`, then failed closed before the first six-hour epoch. Both controllers
sent the requested completion notice and completed Mac status/snapshot/stop,
Exit observer finalization, route/TUN cleanup, and IPv6 restoration.

Attempt 1 artifacts:

- Mac: `/tmp/mini_vpn_knife15_macos_20260818_021340.tar.gz`, SHA-256
  `18dd409b87641b9d3ba3532a0ad7b23e422bd4d55c597b7a4c963d5c09782646`;
- Exit: `/tmp/mini_vpn_knife15_exit_target_observer_20260818_021432.tar.gz`,
  SHA-256
  `72c7fec7d737c5d61dcfdb1abd6cc5f825b93f250bb7c3888b7be480c6b0a40f`.

Attempt 2 artifacts:

- Mac: `/tmp/mini_vpn_knife15_macos_20260818_022707.tar.gz`, SHA-256
  `dc5c83cde3d1f519a2f9e00a27bf65db92f95c6025057c19d536c5bcd17cae9b`;
- Exit: `/tmp/mini_vpn_knife15_exit_target_observer_20260818_022759.tar.gz`,
  SHA-256
  `b066908ad0d718baeda8cacbf5a63cf80d8d583eafca463383f1f9e58939995f`.

Final environment evidence has physical `en0` routes for Exit and Target,
`IPv6: Automatic`, no Mac controller/workload/caffeinate/mini_vpn process, no
Exit observer state or nftables table, and `sing-box active`, `NRestarts=0`,
`ExecMainStatus=0`.

## Attempt 1: root SSH identity was stale

The observer started and reported healthy under the `xiaoou` identity. The
root runner then rejected its own observer admission because
`/var/root/.ssh/known_hosts` retained the destroyed predecessor ECS ECDSA key.
Strict SSH reported the already out-of-band accepted replacement fingerprint:

```text
SHA256:km4qtBz/r+jNuPv4WKoCP3LoIyw1U6nfRNMIWpW9K24
```

An independent HK `ssh-keyscan -t ed25519` reproduced that exact fingerprint.
Only the `47.89.211.4` entries in root's host-key file were replaced, with a
root-owned backup retained. Exact root strict SSH then proved `sing-box`
active with zero restarts and zero exit status.

The detached controller now performs the same `sudo -n -E` observer status
call that the runner will use and requires exactly one `status=active` plus
one `observer_healthy=1` before creating the workload process. Transport
failure and successful-but-inactive reports are both locally rejected before
`sudo:m2-frequency`; once-only notice and cleanup remain intact.

## Attempt 2: the new action was absent from PID identity policy

All source/resource/observer admission passed. The runner then printed:

```text
ERROR: cannot establish identity-verified M2 workload state
```

`register_m0_workload()` records the current shell PID and calls
`workload_command_matches()`. The new `m2-frequency` action had been wired
through admission, schedule, evidence, interruption, summary, cleanup, and
the public action switch, but was omitted from this exact command allowlist.
It was therefore deterministically impossible for a real frequency workload
to begin. The existing self-test also omitted that command.

The focused RED was:

```text
ERROR: self-test: M2 frequency workload command rejected
```

The minimal GREEN adds exact `m2-frequency` ownership and rejects the
non-exact `m2-frequency-replay` suffix. Concentrated action-list review also
adds frequency to the generic sleep-inhibitor gate and to M0/M1 same-TUN
evidence exclusion, closing adjacent control-plane omissions without changing
the workload.

## Gates

- runner RED reproduced only the missing frequency command; complete runner
  self-test then passed, including expected hard-timeout and TERM fixtures;
- controller RED proved root observer failure previously launched workload;
  transport-failure and inactive-report GREEN tests now stop before workload;
- controller, frequency reducer, continuity ledger, resource profile, and
  resource preflight self-tests pass;
- Exit observer self-test passes in its required process-visible permission
  domain; its restricted-sandbox `ps` false failure remains expected;
- Bash syntax, Python entrypoints, `git diff --check`, and secret scan pass;
  `shellcheck` is unavailable;
- concentrated review has no unresolved P0/P1.

No Rust production code, mini_vpn release behavior, workload duration or
shape, SLI, Endpoint/D16, pool, MTU, QUIC, pacing, congestion control, server,
or frozen traffic value changed.

## Next

Commit and push this control-plane repair, sync the HK Mac to the new exact
source, rebuild, and rerun all script self-tests. Because the source identity
changes before the first valid epoch, take one new baseline, one fresh direct
discriminator, and one fresh resource preflight. Then launch the first exact
four-epoch run. The invalid artifacts above consume no epoch or ledger
sequence. M3 remains blocked until twelve valid epochs pass.
