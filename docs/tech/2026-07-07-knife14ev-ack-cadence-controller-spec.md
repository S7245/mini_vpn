# Knife14ev ACK cadence controller spec

Date: 2026-07-07

Branch: `codex/knife14d-downlink-reap-open`

## Goal

Implement the smallest split-controller step after Knife14eu:

- keep payload egress pressure bounded by pending and smoltcp `send_queue`;
- give reverse TCP ACK/window cadence a separate, short-lived relay-read boost
  when a data relay reports sustained remote read gaps and local egress is not
  at the hard pause edge.

## Evidence

Knife14eu failed at `20.8/19.8 Mbit/s`, but rejected FIN deferral:

- data stream `local_finish_events=0`;
- `remote_after_local_finish_bytes=0`;
- `data_max_read_gap_before_finish_ms=7002`;
- `data_max_read_gap_after_finish_ms=0`.

The remaining failure shape is a pre-FIN stream gap with local pressure:

- `may_recv_false=14046`;
- `headroom_deferred_bytes=41455269`;
- `send_queue_max=557386`;
- QUIC loss/congestion/blocking stayed `0`.

## Non-goals

- Do not change QUIC MTU/PLPMTUD, sing-box, iperf3, stale pool, or TUN qlen.
- Do not add FIN deferral.
- Do not remove payload backpressure or enlarge unbounded staging.
- Do not make timer-driven background TUN RX drain the default.

## Design

Add a per-flow ACK cadence boost inside the existing
`DownlinkCreditController`.

Trigger:

- main loop receives a current-epoch `RelayEvent::AckDrainHint`;
- the flow is still `Relaying` or `Closing`;
- local egress is below the hard pause edge and TUN feedback is not paused.

Effect:

- boost only the ACK/window read floor used under pressure;
- cap the boost at the existing pressure floor;
- decay it after it is consumed by a read-credit publish;
- clear it quickly when hard pause, TUN feedback pause, or no-progress
  headroom debt appears.

Safety invariants:

- `DownlinkCreditController::read_credit_for` still enforces bounded staging;
- if pending is at the staging limit, read credit is still paused;
- near the hard pause edge the boost collapses to the one-MTU ACK floor.

## Acceptance

Local:

- focused unit tests prove gap hints increase pressure ACK/window credit while
  healthy;
- focused unit tests prove hard pause / no-progress pressure clears the boost;
- full lib, harness, clippy, script self-tests, release build, and
  `git diff --check` pass.

Remote:

- `.27` focused tests and release build pass.

VPS:

- run one safe1200 reverse-first P1 with explicit exit-to-target preflight;
- target remains receiver `100+ Mbit/s` with zero TUN drops, zero QUIC
  loss/blocking, and clean terminal pending accounting.
