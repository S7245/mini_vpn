# Knife14h10d16 Reverse P8 Restart Architecture Spec

Date: 2026-07-13
Status: **GO for one fresh frozen 60-second reverse P8**
Source baseline: `87c15c7`

## Stage Goal

Resume Task 12 step 4 after the accepted zero-drop forward P1. Run one fresh
reverse-only, eight-flow, 60-second TCP gate on `.27 -> .33 -> .77` and decide
whether the accepted H10d16 chain remains bounded, loss-free at the TUN edge,
and stable under concurrent downlink service.

This is a regression/scale gate for the existing architecture, not a new
throughput mechanism. The only required local change is a backward-compatible
runner seam that stops hard-coding reverse-first parallelism to one.

## Frozen Inputs And Non-Goals

Preserve:

- H10d16 and EndpointWindowV1 at `30,720,000 wire B/s`, `61,440B` burst,
  `10,240B` control reserve, and `20,480B` quantum;
- TUN MTU `1200`, kernel queue `500`, pump FIFO `500`, and existing 48/240
  ingress service bounds;
- physical smoltcp RX/TX storage `1 MiB` and H10d16 local receive credit
  `368,640B`;
- pool `2`, QUIC windows, `64 KiB` application chunk, Cubic, GSO enabled,
  Quinn sender, driver work bound `20`, D3/self-wake disabled;
- target-only routing and the accepted `.33`/`.77` service preflight.

Do not tune any capacity or enable a rejected bounded-sender, cap64, GSO-only,
old actor, or self-wake branch. Do not run macOS TUN. UDP/live-streaming,
full-tunnel fake-IP DNS, and stop/rearm remain later gates.

## Capacity And Reachability Gate

The stable target is aggregate receiver throughput strictly above
`170 Mbit/s = 21.25 MB/s` for 60 seconds. Eight flows do not multiply that
aggregate target; they test sharing and lifecycle. The frozen D16 global byte
budget is `64 MiB`, or more than three seconds of target-rate payload, while
each flow owns at most `512 KiB` before actor admission. Physical smoltcp TX
storage remains `1 MiB` per flow. These are necessary bounded reservoirs, not
a claim that buffering alone supplies throughput.

The reverse hot path is:

```text
TUIC open_tcp_relay / D16DirectOrdered
-> run_relay_d16 + run_d16_native_reader
-> per-flow D16 owned queue + global D16 byte budget
-> actor process_dirty_relay
-> drain_d16_owned_queue_into_pending
-> flush_downlink / tcp_socket.send_slice
-> iface.poll
-> flush_tx_and_release_downlink_permits / TunIo::flush_tx
-> tun0 -> eight local iperf TCP receivers
```

Continuous progress comes from Quinn stream wakers, D16 reader ownership,
dirty-handle scheduling, the existing bounded local-egress service, smoltcp
polling, and TUN flush completion. EndpointWindowV1 remains active for
connection control/ACK datagrams but does not manufacture downlink payload
capacity.

## Old-Path Audit

- `D16DirectOrdered` remains the selected relay; thin, continuous, permit,
  unordered, and native-chunk diagnostic engines remain disabled.
- D3 actor, D4/D5/D6/D11 diagnostics, buffered downlink, bounded global RX,
  cap64, bounded UDP sender, and self-wake remain disabled.
- The accepted continuous TUN reader and batch/poll/flush/relay service remain
  active. The `368,640B` local receive-credit service governs uplink TCP
  admission; it is not presented as a reverse-path fix.

## Runner Seam And TDD Contract

Add `REVERSE_FIRST_PARALLEL`, default `1`, with positive-integer validation.
One helper must pass it unchanged to `run_lowrtt_probe` and name the artifact
`reverse_first_p<parallel>`. Existing callers remain byte-for-byte equivalent
at the default. The suite self-test must prove:

- default/help value is `1`;
- `8` becomes `-P 8` through the reverse-only probe handoff;
- zero, negative, empty, and nonnumeric values are rejected before tunnel
  mutation.

No product Rust path changes for this orchestration seam.

## P8 Acceptance And Failure Discriminators

Pass requires all of:

- reverse P8 receiver strictly above `170 Mbit/s`, `60/60` nonzero one-second
  intervals, no tail collapse;
- TUN RX/TX drop deltas `0/0` and runtime TUN drop counters zero;
- no `send_slice` error/zero loop, TUN flush failure/defer, terminal permit
  drop, actor bypass, unbounded pending, or unexpected relay reap;
- all eight data flows open and make useful remote-read/local-write progress,
  with pool attribution and no reconnect/stale slot;
- D16 per-flow/global queues remain within frozen bounds and finish with zero
  queued/leased/reserved/pending/terminal-reap bytes;
- pump high water below `500`, full waits/read errors zero;
- endpoint conservation remains exact and final live/outstanding bytes are
  zero; aggregate client QUIC lost-byte delta stays within `16 MiB`;
- cleanup removes the client and restores the target route to `eth0`.

Failure attribution is selected as follows:

- low receiver plus remote-read gaps/idle streams: QUIC/TUIC read service;
- growing D16 ownership or global pressure: relay admission/service;
- smoltcp send queue, pending, or `may_recv=false`: local TCP/TUN egress;
- pump saturation or TUN drops: ACK/uplink ingress service;
- QUIC loss or reconnect: path/pool behavior;
- nonzero close ownership: lifecycle/close-tail behavior.

## Stop Rule

An orchestration RED may enter the minimal runner implementation. An
unexpected local repair/regression failure is analyzed before another edit.
If the frozen P8 fails a product gate, archive it as a real failure and design
the next architecture-level repair; do not tune constants or rerun a modified
profile. If it passes, proceed to the separately bounded UDP/live-streaming
gate without changing the frozen profile.
