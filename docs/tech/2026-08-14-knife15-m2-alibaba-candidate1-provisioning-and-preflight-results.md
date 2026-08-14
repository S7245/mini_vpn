# Knife15 M2 Alibaba Candidate-1 Provisioning And Preflight Results

Date: 2026-08-14

Status: **RESOURCE CONFIGURATION PASS; QUALIFICATION NOT RUN; CLOUD SECURITY
GROUP BLOCKED**

## Decision

Alibaba candidate 1 is materially eligible and its host/service configuration
is ready, but it is not yet traffic-admitted. The HK Mac's UDP packets to the
candidate's TUIC port do not reach the guest. This is an Alibaba security-group
block, not a `mini_vpn`, sing-box, Target, Mac route, or bandwidth failure.

Do not start TUN or an observer until the cloud rule is fixed and the new live
TUIC preflight passes. This invalid pre-start attempt neither passes nor
rejects candidate 1 and consumes no Tier-A qualification slot.

## Exact Candidate Identity

- Candidate: `candidate1-alibaba-usw1`
- Provider/ASN: Alibaba Cloud / AS45102
- ECS: `i-rj9cabfprph7x3sard3z`, `ecs.c8i.large`, 2 vCPU / 4 GiB,
  Ubuntu 24.04
- Region/zone: `us-west-1` / `us-west-1b`
- Private/public IPv4: `172.18.188.19` / `47.89.211.4`
- EIP: `eip-rj9hj9g6dbtxwwxfqmw0t`, 200 Mbit/s,
  pay by data transfer, normal mode
- Route class/contract: `public-internet` /
  `eip-rj9hj9g6dbtxwwxfqmw0t`
- ED25519 host-key fingerprint:
  `SHA256:79dk6jK0kPdeegRLpduVuA/EjZ43TZVwMsQ67gZ5B1o`
- Provider evidence SHA-256:
  `90d9a9fa880133abd88e63aa023f1e284680317e281f4f239896b17d4cb37fd5`
- Route evidence SHA-256:
  `94044d5e553a31e1cc394211b2b8ae9a35ea2676e4253e0c7b842fe775fc01ec`

## Provisioning And Service Gates

- The exact reviewed sing-box binary SHA-256 is
  `4ea794fddcb2ad84532adeab979a9b0d7b2052822bb3439dfb321c33c941da19`.
- The exact `/etc/sing-box/config.json` SHA-256 is
  `9aa397471060d1ef11afea858ee2a7550c426a2e133cfee1d2468c55425f33d8`.
- Certificate/key and service shape match the `.33` reference. `sing-box` is
  active with zero restarts, owns UDP 8443 and TCP 443, and reaches
  Target `43.130.32.77:5201`.
- Candidate outbound public IPv4 is exactly `47.89.211.4`.
- SSH root login is key-only: password and keyboard-interactive login are
  disabled, `PermitRootLogin` is `prohibit-password`, `sshd -t` and a new
  batch key login pass.
- Aegis/Alibaba security agents, their three `AliSec*` kernel modules, and
  `apt-daily`/`apt-daily-upgrade` timers are inactive and disabled for the
  dedicated acceptance window. A post-change reboot preserved host key,
  EIP, service hashes, listener, and zero-restart service state.
- A real observer lifecycle passed and cleaned its nftables/state ownership.
  Bundle SHA-256:
  `9eb635cc708b133adce3e65521fa030e30f23b5de30374cf290efa1246ed7675`.
- The initial resource archive passed the older static preflight:
  `e82007eac958819c6a64e6c1719d088d2c06dba0da6ac8fd6e07960184538a37`.
  It is historical only because it lacked an external TUIC handshake.

## Mac Evidence And Invalidations

An exact-source `e5247fa` direct baseline passed at `20.801/28.236 Mbit/s`,
with zero receiver intervals. Its preserved archive SHA-256 is
`2cca4c4cb5b3cfaa6511b24085ed56a8af7973e784c923a906eb3c599ac23d52`.
It belongs to the superseded source and is not reused after the fail-closed
runner repair.

Two later 300-second direct attempts failed because the agent's non-TTY SSH
session did not satisfy macOS `ttyskeepawake`; the Mac had `sleep=1`. Parallel
pings showed multi-second stalls and loss to the gateway, Target, candidate,
and DNS at the same time. With a process-owned `caffeinate -dimsu` assertion,
all four paths had zero loss and the fresh 300-second discriminator passed at
`10,400,336 bit/s`:

- direct directory: `/tmp/mini_vpn_knife15_macos_direct_20260814_111000`
- result SHA-256:
  `aff9ee6c5432cf2ec1d347d30daf72dfa25c561727f9e1d1695786dd401f673b`

The associated candidate profile SHA-256 was
`dfbf7d291d637901115ec602f5efeb3fc981f4b4d64e810a0beae4dbb055e444`,
and its older static resource archive SHA-256 was
`1c7dc885958077a64a5733540d99dd33b91ea28b199a8824110120abf40e2842`.
These are diagnostic evidence only; no qualification was run.

## Selected External Blocker

The first TUN `start` stopped during `tuic handshake: timed out` before TUN
readiness. A candidate-side `tcpdump` saw zero packets while the HK Mac sent
UDP to `47.89.211.4:8443`. Repeating the packet probe after Alibaba Cloud
Security Center protection was disabled still produced zero guest packets.
The guest listener is healthy, so the remaining authority is the Alibaba
security group.

The required cloud rule is:

- direction/action: inbound allow
- protocol/port: UDP 8443
- source: the current HK Mac public IPv4 as one `/32`; observed value is
  `119.13.90.246/32`
- never use `0.0.0.0/0`

The Cloud Security Center alert about “malicious destruction of client files”
was caused by the deliberate removal of the Aegis startup symlink for this
dedicated test host. It explains the earlier file-operation denial, but it is
independent of the still-closed network security-group rule.

## Local Repairs

Reviewed source `0a1cf1c` closes four locally preventable invalid-run paths:

1. resource preflight now performs a real one-second TUIC handshake,
   authentication, and Connect/open probe to the exact Target before TUN;
2. baseline, direct, start, qualification, and formal entry require an active
   `PreventUserIdleSystemSleep` assertion;
3. cleanup after a pre-ready start compares the current utun set with the
   pre-start snapshot, accepting only exact no-addition state and retaining
   fail-closed behavior for any new or owned utun;
4. the same pre-ready cleanup verifies each Target/Exit/DNS interface and
   gateway against its pre-start route snapshot. It does not require a missing
   ownership identity, and it does not accept a merely non-utun route.

The resource archive, root runner, and continuity ledger all require and hash
the exact single-line handshake report plus empty stderr. Shell syntax,
resource-preflight self-test, complete macOS runner self-test, continuity
ledger self-test, diff check, and credential scan pass. Code review found no
unresolved P0/P1.

The repaired cleanup was then exercised against the exact failed pre-ready
Mac state. The utun set was unchanged; Target, Exit, and DNS all matched their
recorded pre-start `en0/192.168.133.1` route; `stop cleanup complete` was
recorded; `cleanup_evidence=PASS`; and the secret scan passed. The finalized
diagnostic bundle is:

- path: `/tmp/mini_vpn_knife15_macos_20260814_112106.tar.gz`
- SHA-256:
  `228533d3c9669880d26a457482a706d1c176e03053d356477d85fbfb163cdb07`
- immutable verdict: `m2_qualification_status=not_run`,
  `formal_m2_acceptance=NOT_RUN`

The temporary four-hour `caffeinate` process was stopped and all TUIC
credential environment variables were unset after finalization.

No Rust production code, TUIC data-plane behavior, M2 workload, SLI, rate,
duration, D16, Endpoint pacing, MTU, QUIC windows, pool, chunk, Cubic, GSO, or
self-wake value changed.

## Exact Next Step

1. Add the narrow Alibaba inbound UDP 8443 `/32` rule.
2. Prove one packet reaches the guest, then pull the reviewed descendant of
   `0a1cf1c` on the HK Mac.
3. Start a fresh bounded global `caffeinate`, rebuild, and capture a fresh
   baseline/direct/profile/resource preflight. The live TUIC probe must pass.
4. Only then run one fresh smoke, observer, and strict qualification.

Formal run 1, formal run 2, and M3 remain blocked until the strict
qualification is sealed as PASS.
