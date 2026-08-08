# Knife15 M2 Successor Authentication-Flight Ownership Implementation Plan

Date: 2026-08-08

Architecture:
`docs/tech/2026-08-08-knife15-m2-successor-authentication-flight-ownership-architecture-spec.md`.

## Task 1: Code-Review Reachability

- [x] Trace TUIC Authenticate through Quinn Data-space transmission and the
  later successor service-turn start.
- [x] Trace ordinary and PLPMTUD ACK/loss terminal paths.
- [x] Reject parameter, retry, Target-probe, and second-turn branches.

## Task 2: Authentication Ownership RED To GREEN

- [x] Withhold one real pre-turn authentication STREAM packet.
- [x] Prove the old turn starts with `sent_bytes=0`.
- [x] Adopt existing ack-eliciting, congestion-accounted Data-space packets at
  turn start.
- [x] Prove withheld authentication loss fails closed and delivered
  authentication succeeds with exact ACK ownership.

## Task 3: PLPMTUD Terminal RED To GREEN

- [x] Reactivate test-only PLPMTUD and withhold its exact in-flight probe.
- [x] Prove the old special loss branch leaves the owned turn nonterminal.
- [x] Publish the existing `PacketLost` result without changing MTU discovery
  or congestion behavior.

## Task 4: Complete Local Gates

- [x] Run root, main, integration, release, established Clippy, shell,
  vendored Quinn/quinn-proto, docs, fmt, diff, vendor, and secret gates.
- [x] Require the exact 32MiB Endpoint capacity `>170 Mbit/s`, exact EOF,
  zero socket would-block, and final ownership `<=61,440/0/0B`.

## Task 5: Reverse Review, Memory, Commit, And Push

- [x] Review authentication ordering, path generation, ordinary/PLPMTUD loss,
  cancellation, boundedness, Endpoint conservation, and TCP/UDP/TUN/D16 risk.
- [x] Resolve every P0/P1.
- [x] Update Quinn patch manifest, result docs, and project learning memory.
- [x] Commit coherent implementation/evidence changes and push the branch.

## Task 6: One Paired Mac Qualification

- [ ] Start one fresh bounded `.33` Exit observer only when the Mac has pulled
  and rebuilt the reviewed descendant.
- [ ] Run exactly `m2-ipv6-check -> baseline -> direct-discriminator -> start
  -> smoke -> m2-qualification -> status -> stop`.
- [ ] Preserve failure evidence, stop/bundle the observer, and do not run
  formal M2 or repeat/tune unchanged.
