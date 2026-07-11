# Knife14 H10d16 Alternate-Exit Gate A Results

Date: 2026-07-11
Source: `5884ac0b2ee604eabe6acac4511f0d3bf714f538`

## Verdict

The replacement external TUIC service restored the expected capacity window.
The mature control reached `163.786 Mbit/s` receiver and unlocked the one
authorized safe1200 Gate A. mini_vpn then reached `183 Mbit/s` receiver with
zero TUN drop, zero actor bypass, stable sub-second service, and clean QUIC
loss/congestion/blocking surfaces.

The literal Gate A still failed its close-tail clause. The iperf data socket
changed directly from `Established` to terminal `Closed` at the 20-second
boundary while the byte-owned reservoir held `524288B` and smoltcp held
`27840B` of unacknowledged egress. This was a local peer reset/abort, not clean
remote EOF. Gate B remains frozen.

## External service diagnosis

The earlier `.33` mature-client failure was isolated to the Exit service side,
not the `.33 <-> .27` network path or mini_vpn:

- bilateral captures for the same `.33` control contained exactly `14984`
  packets on each side and identical downlink per-second bins;
- the Exit capture itself stopped emitting during every long idle gap;
- reversing client and target roles through the same `.33` TUIC service still
  reached only `15.308 Mbit/s` receiver, while `.33 -> .27` direct reverse was
  `229.946 Mbit/s`;
- disabling QUIC GSO worsened the control to `0.262 Mbit/s` receiver;
- a transient Cubic server improved the control to `36.752 Mbit/s` receiver,
  still far below the gate;
- the original `.33` BBR service was restored after each discriminator.

A temporary sing-box TUIC Exit on `.77`, using the same service version and a
mode-0600 FIFO without persisting credentials, provided the independent
external change:

- `.77 -> .33` direct reverse: `232.355 Mbit/s`;
- mature `.27 -> TUIC .77 -> target .33`: `165.884/163.786 Mbit/s`;
- client and Exit UDP buffers: `16 MiB`;
- socket drops: `0`.

This passes the versioned capability precondition and proves that replacing or
rebuilding the `.33` Exit is a viable external solution.

## Gate A result

The clean source, binary, and versioned safe1200 runner used:

- pool `2`;
- TUN MTU `1200`;
- H10d16 byte-owned egress enabled;
- one reverse-first P1 iperf run for `20s`;
- `.27 -> TUIC .77 -> target .33`.

Measured throughput:

- sender: `187 Mbit/s`;
- receiver: `183 Mbit/s`;
- interval shape: stable high, `113-251 Mbit/s`;
- data-stream maximum ordered-read gap: `416ms`.

Clean invariants:

- `tun_rx_dropped_delta=0`;
- `tun_tx_dropped_delta=0`;
- actor bypass `0`;
- send-slice zero/errors `0`;
- queue pending at close `0`;
- terminal pending reap `0`;
- QUIC lost bytes, congestion events, and data/stream blocking `0`.

Failed close invariants for data `SocketHandle(1)`:

- `Established -> Closed` directly at the iperf 20-second boundary;
- no data-flow `remote_eof` was observed before the local terminal edge;
- `permit_terminal_drop_bytes=524288`, exactly one full per-flow reservoir;
- `close_egress_class=terminal_closed_no_send`;
- `close_egress_bytes=27840`, exactly one maximum accepted actor batch;
- remote `rx_bytes=457868515` minus actor-admitted `457344227` equals the
  recorded `524288B` terminal reservoir drop.

The separate iperf control flow closed through the normal remote-EOF path with
queue, pending, inflight, terminal-drop, and close-egress bytes all zero.

Remote artifact:

`/tmp/mini_vpn_h10d16_gate_a_alt_exit_5884ac0/`

## Code review

### What the run proves

The 170 Mbit/s architecture has sufficient capacity and remained stable under
the approved safe1200 production profile. The single-owner actor, byte ledger,
bounded queue, local TUN service, and pressure path did not regress. The current
failure is not a reason to retune MTU, Quinn windows, pool size, read chunk,
self-wake, or D16 cadence.

The byte ledger also behaved conservatively on terminal cancellation: the
exact remote-read surplus was bounded by the `512 KiB` per-flow cap and released
once. Once smoltcp has processed a peer RST and entered `Closed`, neither the
owned reservoir nor unacknowledged send queue can be delivered to that peer.
Requiring those values to become zero after the RST is not an implementable TCP
lifecycle contract.

### Actionable defect

`drop_d16_owned_for_terminal` records the exact terminal byte count, but the
D16 relay supervisor can later emit
`terminal_direction=none terminal_reason=clean_queue_lifecycle`. Closing the
local uplink channel and closing the queue erase the reason before the relay
summary is produced. This violates the architecture requirement that a
terminal local socket report one terminal reason with its exact byte count.

### Gate-contract mismatch

The timed reverse iperf capacity flow ended with a local reset, while the strict
Gate A simultaneously required clean remote EOF and zero terminal bytes on that
same flow. The newly observed terminal edge makes the mismatch concrete: a
steady-state throughput generator that aborts at its time boundary cannot also
prove graceful EOF drain.

## Accepted correction and implementation

1. RED: add a deterministic production-seam test for a local peer reset while
   one full D16 reservoir and one admitted batch are outstanding. Require exact
   ownership conservation, one terminal event, and no double release.
2. GREEN: preserve an explicit local terminal cause from smoltcp/TUN lifecycle
   detection through queue termination and the D16 relay summary. Keep the
   existing bounded drop behavior; do not attempt to drain into a `Closed`
   socket.
3. Separate the acceptance evidence:
   - capacity gate: timed reverse P1, receiver `>150 Mbit/s`, stable gaps, zero
     TUN drop/bypass/error, and exact bounded accounting for any peer reset;
   - clean-close gate: an EOF-terminated finite reverse payload whose queue,
     pending, inflight, terminal-drop, and close-egress values must all be zero.
4. Amend the architecture spec and versioned runner, then run the focused local
   lifecycle tests, full D16 tests, checks, and review.
5. Recreate the temporary alternate Exit only for the approved focused close
   proof or replacement acceptance. Gate B remains frozen until both capacity
   and clean-close evidence pass.

The user approved this correction. Commits `879e904` and `7a7ca04` remove the
dead close-reap plumbing, preserve the first terminal cause, classify it in the
versioned runner, and add a fixed-byte reverse `iperf3 -n 64M` clean-close
window. Gate A remains a strict AND gate: timed capacity and fixed-byte clean
EOF must both pass on one build/profile/tunnel.

## Cleanup

- `.77` temporary TUIC service stopped;
- `.77` temporary binary/FIFO removed;
- `.77` socket sysctls restored to their original values;
- `.27` mini_vpn/TUN/route cleaned and detached worktree removed;
- `.33` original BBR sing-box service active with persistent high buffers;
- no credential, private key, or environment value was written into this
  result.
