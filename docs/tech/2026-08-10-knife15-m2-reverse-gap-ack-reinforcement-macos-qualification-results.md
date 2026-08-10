# Knife15 M2 Reverse Gap ACK Reinforcement macOS Qualification Results

Date: 2026-08-10

Status: **PAIRED QUALIFICATION PASS; FORMAL M2 REOPENED ON REVIEWED TREE; M3 REMAINS BLOCKED**

Mac artifact:
`/tmp/mini_vpn_knife15_macos_20260810_101221.tar.gz`

SHA-256:
`6121b8b86dfcea853ef7bc578e8324a33280158170c4442a50312a66f09b91e6`

Exact source:
`c6ffa3a2fbd6284e77c97d9589a0ed86ac7a8d2a`

Paired Exit artifact:
`/tmp/mini_vpn_knife15_exit_target_observer_20260810_095205.tar.gz`

SHA-256:
`03d513708b715c188024242108ddb624f18c53bace0bb93f9309fec5b00435f7`

Implementation:
`300fb16`

## Qualification Boundary

The exact-source run passed baseline `12.791/44.901 Mbit/s`, the 300-second
direct discriminator at `6.393 Mbit/s` with zero sender/receiver intervals,
start, smoke, IPv6/full-tunnel/controlled-drain preflights, two exact mixed
cycles, two DNS checks, two real-client checks, status, and cleanup. Its
public verdict is correctly `PASS_NON_ACCEPTANCE`; formal M2 remained
`NOT_RUN`.

All eight mixed phases passed:

| Phase | Sender / receiver bytes | TCP zero intervals / UDP loss |
| --- | ---: | ---: |
| cycle 1 forward TCP | `239,861,760 / 239,861,760` | `0 / 0` |
| cycle 1 reverse TCP | `842,362,880 / 841,895,936` | `0 / 0` |
| cycle 1 reverse UDP | `505,715,920` received | `1.830894%` |
| cycle 1 short forward | `12,845,056 / 11,141,120` | `0 / 0` |
| cycle 2 forward TCP | `239,861,760 / 239,599,616` | `0 / 0` |
| cycle 2 reverse TCP | `842,388,480 / 841,919,488` | `0 / 0` |
| cycle 2 reverse UDP | `505,612,680` received | `1.458682%` |
| cycle 2 short forward | `10,616,832 / 4,849,664` | `0 / 0` |

The aggregate TCP maximum sender/receiver gap was `5,767,168B`; maximum UDP
loss was `1.830894%`. Every client and server TCP interval independently had
nonzero bytes.

## Exact Reinforcement Reachability

Conn1's transmitted `gap_ack_reinforcements` advanced `0 -> 8 -> 22` during
the bounded smoke pressure and then remained exactly `22` through both mixed
cycles and final drain. Conn0 remained zero. This proves the exact
`STREAM_DATA_BLOCKED` plus same-stream ordered-gap branch was reachable on the
real mature-server path. The counter's stable tail rejects a persistent or
self-sustaining ACK stream.

The branch consumed only normal Endpoint Control traffic. Endpoint high water
remained the frozen `61,440B`; final available/live/outstanding was
`61,414/0/0B`, with zero interface errors and zero socket would-block.

## Paired Exit Evidence

The observer was active from `09:52:05Z` through `10:42:36Z`. Its bounded
ring retained every qualification TCP data socket from smoke traffic at
`10:12:24Z` through the final Target traffic at `10:40:08Z`. It captured
`2,971,792` packets with exactly zero kernel drops; every internal file hash
passed.

Exact payload continuity was:

| Exact socket phase | Maximum positive supply gap | Gaps >=500ms / >=1s | Longest zero receive window |
| --- | ---: | ---: | ---: |
| smoke forward | `216.771ms` | `0 / 0` | n/a |
| smoke reverse | `674.640ms` | `2 / 0` | `656.203ms` |
| cycle 1 forward | `166.523ms` | `0 / 0` | n/a |
| cycle 1 reverse | `251.174ms` | `0 / 0` | `249.901ms` |
| cycle 1 short forward | `167.013ms` | `0 / 0` | n/a |
| cycle 2 forward | `226.316ms` | `0 / 0` | n/a |
| cycle 2 reverse | `342.575ms` | `0 / 0` | `341.525ms` |
| cycle 2 short forward | `231.996ms` | `0 / 0` | n/a |

Both qualification reverse flows therefore stayed far below the previous
`1,158.373ms` Exit supply pause and had no half-second supply gap. The Target
sender reported only `2` and `3` TCP retransmissions across the two 300-second
reverse phases.

## Lifecycle And Review

Two peer `Stopped(0)` writes explain `internal_failure_scan=REVIEW`: one was
the timed smoke close tail and one the cycle-2 short-forward close tail. Both
occurred after the workload boundary with D16 queued/leased/reserved
`0/0/0B`; every phase and cleanup completed. They are not active-transfer
failures.

The process ended dead; DNS, fake/low/high/full-tunnel and Exit routes were
released; qualification cleanup and endpoint conservation passed. Transient
three-probe Exit samples reached `66.7%` loss, while all gateway samples had
zero loss and the paired TCP flows retained continuity. This is a useful
adverse-path qualification rather than a reason to tune a constant.

The first local SCP of the finalized Exit bundle was interrupted and produced
a smaller archive whose SHA did not match the authoritative remote checksum.
It was rejected before extraction. A complete retransfer matched
`03d51370...`; only that copy was analyzed.

## Decision

Retain the exact ACK reinforcement architecture. The real path reached the
new branch, remained bounded, completed all eight phases without a TCP zero
interval, and avoided the previous second-scale reverse supply stall.

Do not repeat or tune qualification. Formal M2 is now reopened for the
reviewed `c6ffa3a` production-code tree (implementation `300fb16`) and a
docs-only pushed descendant, with the frozen workload/config. Take one fresh
`m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke -> m2 ->
status -> stop`. M3 remains blocked until that formal result and cleanup pass.
