# Knife15 M2 Strict Candidate 2 Qualification Results

Date: 2026-08-15

Status: **STRICT QUALIFICATION PASSED; FORMAL 1 AUTHORIZED; TIER A PENDING;
M3 BLOCKED**

Mac artifact:
`/tmp/mini_vpn_knife15_macos_20260815_102736.tar.gz`

Mac SHA-256:
`71a61c55c185b5137f45ed22cee4ea14db3865f1a997bb019e1377905db24b96`

Exit artifact:
`/tmp/mini_vpn_knife15_exit_target_observer_20260815_102824.tar.gz`

Exit SHA-256:
`516ff6d4d2a6d16d0f02fcf0082f1869434f7c7a6481465c924dddff16c8b009`

Exact source: `b4244a7c3fb58efe9fbfc832cda402d6ed4e96d7`.

## Verdict

Alibaba Tokyo candidate 2 passed the one permitted strict qualification. The
exact paired run completed baseline, bounded direct discrimination, resource
admission, smoke, two cycles/eight mixed phases, two DNS checks, two
real-client checks, safety, observer coverage, and cleanup. Every completed
TCP result had zero sender and Target receiver-zero intervals. The strict
ledger seals sequence 3 as `pass/strict_pass`, moves candidate 2 to
`awaiting_formal`, and retains `TIER_A_PENDING`.

This qualification authorizes Formal 1 only. Candidate 2 still needs two
consecutive clean formal runs under the same source, release binary, workload,
resource, server, and observer contract. It is not a Tier-A acceptance and
does not reopen M3 yet.

## Exact Identity And Admission

- candidate: `candidate2-alibaba-tokyo`;
- Alibaba ECS: `i-6weckus0r7voaarxz2k3`, `ap-northeast-1c`;
- EIP / ASN: `8.211.176.98` / AS45102;
- exact ED25519 fingerprint:
  `SHA256:r7JYHgl+fH36CCVXrb7fc5ADsg1fnM9Z8j6KowZZYYM`;
- client release SHA-256:
  `5e946af25ac05e74fa54d607fe4a67d7b22a1305c3b0a2e042fffb3d3cbf3032`;
- sing-box package SHA-256:
  `4ea794fddcb2ad84532adeab979a9b0d7b2052822bb3439dfb321c33c941da19`;
- server configuration SHA-256:
  `0c48b68369253dfa427a953f7f05a0945a9a7023073713b2a10423390d288c40`;
- observer SHA-256:
  `ed550b968978df549d9b37b86e6f4f423ddbd142fc3abd458c92eab6e4edc1da`;
- canonical resource profile SHA-256:
  `f7aca9899e5c65dfea9e7098685db61d6b94b231273f66e3101565fee78a8b2c`;
- workload contract SHA-256:
  `bebff85c39b8e48c056e5882a4b77521dffea956c0e5653131dd555480e81382`.

The live preflight classified the candidate as eligible relative to the frozen
Tencent `.33` reference through distinct provider and ASN. Candidate 2 shares
Alibaba/AS45102 with rejected candidate 1, so this result does not claim
provider/ASN independence between the two new resources.

## Completed Envelope

- direct baseline: `22.809/36.695 Mbit/s` forward/reverse, zero
  sender/receiver-zero intervals;
- bounded 300-second direct forward: `11.404318 Mbit/s`, PASS;
- qualification window: `2026-08-15T10:28:36Z` through `10:55:30Z`;
- two cycles, eight phases, two DNS checks, and two real-client checks;
- six TCP results and two UDP results;
- maximum TCP sender/receiver gap: `7,077,888B`, below `16MiB`;
- maximum UDP loss: `0.145685%`, below `3%`;
- TCP sender-zero intervals: 0;
- Target receiver-zero intervals: 0;
- Endpoint maximum conservation: `61,440B`;
- Endpoint terminal available/live/outstanding: `61,402/0/0B`;
- socket would-block, pacing abandon, and interface errors: 0;
- process, route, DNS, TUN, observer, and Exit cleanup: PASS.

The Exit observer captured `7,517,117` packets, received `7,517,121` through
its filter, and dropped zero in the kernel. All four Target/TUIC nftables
directions were nonzero. Observer state/table ownership was removed after
freeze and bundling; sing-box remained active with zero restarts.

## Transport And Lifecycle Review

The data connection experienced one native Quinn PLPMTUD black-hole event and
ordinary congestion contraction, ending around `64ms` RTT and `59,959B` cwnd.
All eight phases retained the strict continuity SLI. There was no mini_vpn
`path_changed()` execution, Endpoint rebind, generation replacement, ordered
gap recovery event, or gap-ACK reinforcement. This is qualification evidence
for this route, not proof that the rare replacement branch was exercised.

The summary's `internal_failure_scan=REVIEW` is explained by three exact
`Stopped(0)` writes at timed-transfer termination. Each matching D16 close
record had queued/leased/reserved ownership `0/0/0B`, a closed queue, and no
stalled-write timeout. They are completed transfer tails, not unresolved
data-plane failures.

## Invalid Setup Attempts

Three pre-qualification controller attempts were invalid and decision-neutral:

1. a GNU-only `find -maxdepth` expression on macOS produced no selected
   directory and a 29-byte junk archive;
2. resource-preflight `OUT_DIR` leaked into `start`, whose exact-output guard
   correctly rejected the ambiguous path;
3. the controlling SSH connection timed out during self-test before TUN
   ownership.

None entered qualification or consumed a candidate evidence slot. The final
controller uses Bash-3-compatible selection, unsets `OUT_DIR`, executes inside
a persistent macOS `screen` session, keeps the interactive sudo timestamp
alive through bounded noninteractive checks, and records an independent
controller log. The active runbook now carries the missing `unset OUT_DIR`
contract.

## Ledger And Decision

The immutable evidence root is:

```text
/Users/liushan/knife15-evidence/candidate2-alibaba-tokyo/
  b4244a7c3fb58efe9fbfc832cda402d6ed4e96d7
```

`attempt-003.json`, `knife15-tier-a-ledger-003.json`, and
`evaluation-003.json` revalidate the complete paired artifact content and
emit:

```text
status=TIER_A_PENDING
candidate1-alibaba-usw1.state=rejected
candidate2-alibaba-tokyo.state=awaiting_formal
candidate2-alibaba-tokyo.formal_passes=0
```

Freeze source, binary, baseline-derived workload, candidate identity, server
binary/configuration, observer, workload, and every data-plane value. Take one
fresh direct/resource preflight, start, smoke, fresh matching observer, and
Formal 1 while reusing the preserved qualification baseline. A genuine quality
failure rejects candidate 2 and exhausts Tier A; a clean Formal 1 authorizes
exactly one Formal 2. Do not tune or repeat a valid failure.
