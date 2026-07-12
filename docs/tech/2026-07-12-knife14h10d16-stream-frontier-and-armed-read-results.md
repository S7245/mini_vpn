# Knife14 H10d16 Stream Frontier And Armed-Read Results

Date: 2026-07-12

## Outcome

The follow-up closed two deterministic reader-service defects but did not
authorize composite Gate A. Gate B remains frozen.

- `4bc847b` restores cancellation-safe, transport-owned ordered Quinn chunks.
- `bdaa19c` preserves an already armed RAII read reservation across
  non-pausing `Running` credit changes.
- The D16 actor, leased queue, DrainOnly/Recovery state model, and EOF gates
  were not changed.

## Discriminators

The `99ff0f4` service-sized application `AsyncRead` implementation passed local
capacity but a clean scoped VPS run delivered only `18356B`; connection-level
STREAM frames continued arriving while the ordered reader remained pending.
The implementation was therefore necessary-looking local capacity evidence,
not a sufficient product reader.

An existing bounded unordered diagnostic produced a `170 Mbit/s` first second
and exposed a two-byte offset gap. A reservation-owned D16 frontier prototype
then failed deterministically in Quinn loopback: later offsets consumed the
entire `524288B` per-flow ledger before the missing offset arrived. The
prototype was removed. D16 does not revive the old `4 MiB` staging path.

`4bc847b` replaced the application buffer with direct ordered
`read_chunk(max_len, true)`. Local real-Quinn tests delivered `32 MiB` above
`170 Mbit/s`, kept progress gaps below `250ms`, and preserved native chunk-sized
handoff. Default library tests passed `591/591`; harness library tests passed
`600/600`; default and harness checks passed.

## Scoped Capable-Window Result

After rebuilding the temporary `.77` Exit, the versioned mature control passed:

- direct receiver: `217.429 Mbit/s`;
- sing-box receiver: `172.167 Mbit/s`;
- client and Exit UDP socket drops: `0`.

The clean `4bc847b` safe1200 pool-2 scoped run then reached:

- sender: `40.2 Mbit/s`;
- receiver: `38.5 Mbit/s`;
- TUN RX/TX drop delta: `0/0`;
- actor bypass, `send_slice_zero`, `send_slice_errors`, and flush failures: `0`;
- maximum pending bytes: `27840`;
- maximum ordered read gap: `5.219s`.

During several gaps, connection-level STREAM frame counters advanced while the
local D16 queue, actor, and TUN surfaces remained unpressured. The run therefore
failed the scoped `>150 Mbit/s` gate upstream of local egress.

## Armed-Read Fix

Review found that every non-pausing quantitative credit update canceled the
pending read and refunded its reservation. The red test observed a full
`524288B` reservation being replaced by `131072B`. `bdaa19c` keeps that owned
read armed; a pause, channel close, stop, or read completion remains able to
cancel it. Full local regression and checks pass.

The required VPS A/B for `bdaa19c` was not run. A freshly rebuilt temporary
Exit produced a mature-control receiver of only `18.873 Mbit/s` despite a
`217.011 Mbit/s` direct receiver and zero socket drops. That window cannot
validate mini_vpn.

## Decision

Composite Gate A was not run. In a genuinely capable temporary-Exit window,
run the versioned mature control once and require receiver `>150 Mbit/s` with
zero socket drops. Only then run one clean `bdaa19c` scoped safe1200 reverse P1.
Composite Gate A is authorized only if that scoped run also exceeds
`150 Mbit/s` with all local invariants clean.

## Current Requalification

A later clean `ce5a87c` control used the same sing-box `1.13.14` binary on a
temporary `.77` Exit and the role-reversed `.33` iperf target. The source and
runner were clean/versioned, target-only routing was verified, and both client
and Exit UDP sockets had `16777216B` receive/transmit buffers with drop `0`.

- `.27 -> .33` direct receiver: `216.801 Mbit/s`;
- `.77 -> .33` direct receiver: `212.398 Mbit/s`;
- mature TUIC sender/receiver: `0.996/0.192 Mbit/s`;
- one-second TUIC intervals: `3.143`, nine zeros, `0.695`, then nine more
  zeros.

The mature client logged no error. The temporary Exit stayed active and only
reported the expected remote stream cancellation after the timed test. This
reproduces the external burst/idle discriminator and does not measure
`bdaa19c`; the scoped run and composite Gate A remain forbidden in this
window. Local artifact:
`/tmp/mini_vpn_h10d16_bdaa19c_control_current.tar.gz`.

## Isolated `.33` Service Port Discriminator

A same-version minimal sing-box service was started independently on
`.33:9443` while the original `.33:8443` service remained active. The new unit
was active, listened on UDP `9443`, inherited the same TUIC inbound, and used
the existing high socket-buffer settings. Clean `8cc0f15` control setup and
target-only routing passed, with a `217.011 Mbit/s` direct receiver, but the
TUIC open timed out after five seconds with no recent network activity.

This was a reachability failure rather than a low capacity result. A bounded
packet-arrival A/B sent one UDP byte from `.27` to each port while `.33`
captured both ports. The host received the `8443` packet and did not receive
the `9443` packet. Therefore an upstream firewall/security policy blocks the
alternate port before `.33`; service configuration and mini_vpn were not
measured. The temporary unit was removed and original `8443` remained active.
Artifact:
`/tmp/mini_vpn_h10d16_alt9443_control_current.tar.gz`.

The next discriminator requires a maintenance swap on the already-open
`8443`: stop the original unit, let the minimal isolated unit own `8443`, run
one mature control, then restore the original unit on every exit path. This is
an external service-state change and must be confirmed before execution.

## Allowed-Port Maintenance Swap

The confirmed maintenance swap installed a `45m` automatic restore watchdog,
stopped the original `.33:8443` unit, and let a minimal same-version sing-box
service own the already-open UDP port. Before the swap, clean `7f69fd0` passed
the focused armed-read regression, both runner self-tests, release build, and
source-clean check. The isolated service was active with the expected socket
buffers before traffic.

The mature control still failed capability:

- `.27 -> .77` direct receiver: `218.688 Mbit/s`;
- `.33 -> .77` direct receiver: `214.285 Mbit/s`;
- mature TUIC sender/receiver: `13.525/11.953 Mbit/s`;
- client and server UDP socket drops: `0`;
- thirteen of twenty one-second intervals: exactly zero.

The client emitted no error. The service stayed active and only logged the
expected remote cancellation after the timed run. Nonzero intervals were
isolated bursts (`94.337`, `22.020`, `15.729`, `6.291`, `4.194`, `45.089`,
and `51.380 Mbit/s`). The original sing-box service was then restored, the
watchdog and isolated unit were removed, and all three VPS cleanup checks
passed. Artifact:
`/tmp/mini_vpn_h10d16_isolated8443_control.tar.gz`.

This falsifies the old-process and full-config hypotheses. Current evidence
places the blocker at the `.33` host or Client-to-Exit QUIC path, before any
mini_vpn reader/egress comparison. `bdaa19c` scoped and composite Gate A remain
unspent. A new independent capable Exit is the shortest path to acceptance; a
host-local network-namespace mature control can further distinguish server/
kernel behavior from the external UDP path but cannot itself authorize Gate A.

## Independent `.111` Exit Result

The newly authorized independent Exit `.111` was prepared with the exact
sing-box `1.13.14` binary hash, FIFO-only configuration/certificate/key input,
temporary high socket buffers, a `60m` restore watchdog, and a verified UDP
`8443` host-arrival probe. Clean `559f7a8` passed the focused armed-read test,
both runner self-tests, release build, source check, and direct-path preflight.

- `.111 -> .77` direct receiver: `212.013 Mbit/s`;
- control direct receiver: `214.092 Mbit/s`;
- mature TUIC sender/receiver: `7.129/4.928 Mbit/s`;
- client and Exit UDP socket drops: `0`;
- thirteen of twenty one-second intervals: exactly zero.

The client emitted no error and the Exit only logged the expected timed remote
cancellation. The temporary service, watchdog, binary, FIFO, and socket sysctl
changes were removed; `.111` retained only its authorized public key and the
installed iperf3 client. Artifact:
`/tmp/mini_vpn_h10d16_exit111_control.tar.gz`.

This falsifies an Exit-host-specific root. The common remaining surfaces are
the `.27` Client-to-Exit UDP/QUIC window and the capability-control shape.

## Capability-Control Code Review

Verdict: **request a Gate-process correction; keep D16 unchanged.**

The sole authorization script hard-codes sing-box congestion control `bbr`
and TUN MTU `1500` (`scripts/knife14h10d16-singbox-control.sh`), whereas the
actual D16 Gate runner requires `MINI_VPN_TUIC_CC=cubic` and TUN MTU `1200`.
`src/tuic.rs` documents Cubic as the production default and BBR as an
experimental override that can materially underperform on affected paths.

The historical control was deliberately versioned and has produced capable
results before, so its observations remain valid. However, after the same
burst/idle false negative across independent Exits, it is not a sound sole
necessary predicate for a different product profile.

The recommended TDD correction is:

1. preserve the historical `BBR + MTU1500` profile as a named diagnostic;
2. add a fixed `gate-aligned` mature profile using `Cubic + MTU1200`;
3. extend self-tests to assert the exact profile label, MTU, congestion control,
   route shape, and fail-closed floor;
4. authorize one clean `bdaa19c` scoped run only when the gate-aligned mature
   receiver exceeds `150 Mbit/s` and both socket drops are zero.

This does not relax the capacity floor or authorize Gate A from the failed
historical result. It removes a control-to-product mismatch before the next
external measurement.

External artifacts:

- `/tmp/mini_vpn_h10d16_service_batch_scoped_99ff0f4/`
- `/tmp/mini_vpn_h10d16_frontier_diag_99ff0f4b/`
- `/tmp/mini_vpn_h10d16_transport_owned_scoped_4bc847b/`
- `/tmp/mini_vpn_h10d16_armed_read_bdaa19c_control_export/`
