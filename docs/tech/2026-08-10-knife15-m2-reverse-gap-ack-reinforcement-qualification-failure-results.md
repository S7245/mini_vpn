# Knife15 M2 Reverse Gap ACK Reinforcement Qualification Failure Results

Date: 2026-08-10

Status: **QUALIFICATION FAILED; REVERSE QUIC ORDERED-GAP PRESSURE SELECTED; FORMAL M2 AND M3 REMAIN BLOCKED**

Mac artifact:
`/tmp/mini_vpn_knife15_macos_20260810_075621.tar.gz`

SHA-256:
`25847808201fde5f2083f97936e0829ceebb4d517edc3039ac9c2f9b68470314`

Exact source:
`673d13d1b42e3eb03797615a609db0ad8ac9f3ff`

Paired Exit artifact:
`/tmp/mini_vpn_knife15_exit_target_observer_20260810_073524.tar.gz`

SHA-256:
`5c26edfd08155123fb8f95e7ca774f1394c19ca3f5ba9cc9443289ba4a04e911`

## Qualification Boundary

The Mac passed baseline `23.502438/64.235511 Mbit/s`, the bounded 300-second
direct discriminator, start/smoke, every preflight, cycle 1, and cycle 2
forward. Cycle 2 reverse TCP then recorded exactly one complete Target
receiver-zero interval at interval 98 (`98.005s..99.010s`). The transfer
continued to `1,204,496,384B` received versus `1,205,171,200B` sent, and
cleanup passed. Formal M2 was not run.

Endpoint high water was the frozen `61,440B`; final
available/live/outstanding was `61,414/0/0B`. Interface errors and socket
would-block were zero. Routes, DNS, TUN, and process ownership were removed.

## Exact Client Stream

The reverse owner was conn1 stable identity `39880097808`, generation 2,
stream 2. Its successor service certificate remained `Ready`; no replacement
or path migration occurred.

The ordered reader stopped at `385,286,941B` behind a missing `1,386B`
prefix. Same-stream buffered tail grew from `5,876,229B` to `7,552,771B`
while Quinn received another `2,982` datagrams and `2,981` STREAM frames.
The prefix became readable after `1,759ms`. Received
`STREAM_DATA_BLOCKED` frames increased from 3 to 37 around the episode.

## Paired Exit Boundary

The exact reverse Target socket was
`172.26.0.2:36210 -> 43.130.32.77:5201`. The capture reported zero kernel or
capture drops. Target-to-Exit data stopped only after the Exit TCP advertised
window fell from three segments to zero; the zero-window advertisement was at
`04:17:32.932655`, it reopened at about `04:17:34.049`, and payload resumed at
about `04:17:34.050`. The maximum positive supply gap was `1,158.373ms`.

This means sing-box stopped reading the Target TCP socket while its QUIC
stream was flow-control blocked behind the client's missing ordered prefix.
When the prefix recovered, application consumption, receive credit, the Exit
TCP window, and Target supply resumed together.

## Decision

The paired evidence rejects operator sequencing, Target service,
Exit-to-Target delivery, successor readiness, pool replacement, D16, TUN,
Endpoint capacity/conservation, routes, and cleanup. Physical samples did
show transient `.33` probe loss and RTT growth from roughly `162ms` to
`212ms`, so WAN ACK/loss remains a contributor rather than a code-only claim.

The next architecture may reinforce one ACK only when the peer reports
`STREAM_DATA_BLOCKED` for the exact receive stream while that stream has a
buffered ordered gap. Packet-number range splitting alone is not authority.
Take one new paired qualification after local gates and review; recurrence
rejects this mechanism as sufficient without tuning constants.
