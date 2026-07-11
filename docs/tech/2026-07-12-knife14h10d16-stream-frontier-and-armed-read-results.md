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

External artifacts:

- `/tmp/mini_vpn_h10d16_service_batch_scoped_99ff0f4/`
- `/tmp/mini_vpn_h10d16_frontier_diag_99ff0f4b/`
- `/tmp/mini_vpn_h10d16_transport_owned_scoped_4bc847b/`
- `/tmp/mini_vpn_h10d16_armed_read_bdaa19c_control_export/`
