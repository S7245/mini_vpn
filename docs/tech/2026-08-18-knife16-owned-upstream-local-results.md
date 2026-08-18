# Knife16 OwnedUpstream Local Results

Date: 2026-08-18

Status: **TASK 3 PASS; TASK 4 NEXT; M3 BLOCKED**

## Outcome

Knife16 Task 3 is complete as a local branch-by-abstraction stage. The new
crate-private `owned_upstream` layer lets the existing TUN/smoltcp loop drive
either the unchanged legacy relay variants or a typed resumable TCP flow
without exposing transport-leg, replay, application-ACK, or session-reducer
mechanics to the socket hot path.

The public production entry still selects `LegacyOwnedUpstream`; the
resumable branch is exercised by deterministic in-process adapters and the
real smoltcp loop. There is no production two-leg transport, server owner,
blackout recovery, WAN result, or throughput result yet. Task 4 owns the
deterministic two-leg transport harness. No macOS TUN or VPS run is authorized
by this result.

No D16, Endpoint, MTU, pool, QUIC window, chunk, Cubic, GSO, self-wake,
Knife15 workload, or quality threshold changed.

## Deep adapter boundary

`OwnedUpstream` is an object-safe facade whose TCP open returns exactly one of:

- `Legacy(OpenedTcpRelay)`, preserving `Generic`, `Native`, and
  `NativeByteOwned` without coercion; or
- `Resumable(ResumableTcpFlow)`, a typed port into the single-owner session
  reducer.

`LegacyOwnedUpstream` delegates `open_tcp_relay` exactly once. It does not fall
back to `open_tcp`, create tasks, reinterpret EOF, or change TUIC, Reality, or
Failover selection. UDP uplink is a borrowed `UdpUplink<'_>` view and is
encoded exactly once by the legacy adapter. The bounded downlink source owns
the existing receiver/reassembler and consumes one item per poll, preserving
the old select-loop fairness and TUIC-only Failover UDP policy.

The adapter-aware event loop is crate-private and the ordinary public loop
wraps its upstream in the legacy adapter. This keeps the parallel branch
reversible until Task 4 supplies a real leg and Task 6 supplies the owner.

## TCP byte ownership and application ACK

Every logical session constructs one `ResumableTcpPortFactory`; all of its
flows share one exact global uplink-byte ledger in addition to their per-flow
bounds. The producer reserves both message and byte capacity before the
smoltcp receive callback may extract bytes. Per-flow, message-lane, and global
byte exhaustion are all backpressure: the local socket bytes remain
unconsumed and the flow remains open.

Reducer DATA ownership is transferred with a non-cloneable `ReplayStored`
receipt that binds the exact local flow, direction, byte extent, and payload.
Capacity is released only by an exact reducer-minted
`ReplayAcknowledged` receipt or by terminal ownership. A caller-constructed
wire ACK cannot release replay bytes.

DATA and CLOSE share one ordered source lane. RESET has a separate reserved
terminal lane, while ordinary control uses bounded-burst priority so neither
control nor bulk DATA can starve forever.

Downlink `SinkDelivery` and `HalfCloseDelivery` retain opaque reducer
capabilities. Decode, leg receive, mailbox admission, D16 state, and queueing
emit no application ACK. Only the exact positive byte count returned by
`TcpSocket::send_slice` becomes `SinkAccepted`; zero writes retain ownership,
and errors/terminal paths abandon or reset without ACK. Partial acceptance
invalidates the old offer and only the reducer may issue the remaining
suffix.

Sink write, half-close, and terminal retirement share one liveness gate, so a
terminal transition cannot race a stale delivery into the local socket.
Half-close success follows socket lifecycle authority rather than transmit
capacity, and the final ACK is emitted only after the local half-close
completion succeeds.

## Existing D16/Endpoint actor preserved

Resumable downlink is classified as an owned local-egress actor and uses the
existing phase, origin, credit-clock, pressure/debt, headroom, send-window,
and flush-feedback decisions. `ControlOnly`, `DrainOnly`, hard pressure, and
active debt retain the sink capability and emit no ACK. A clean
`EgressActor` turn admits only the existing bounded allowance.

The resumable path does not create a synthetic D16 queue or permit, and it
does not redefine frozen D16 gauges. After smoltcp accepts bytes, packet
ownership continues through the unchanged D16/Endpoint conservation chain.

## Leg, session, and flow lifecycle

Decoded transport frames first become `LegBoundFrame` values sealed to one
exact transport-leg instance. Attach validation returns a non-cloneable
`AttachedLeg`; only that same seal can convert later DATA/ACK/CLOSE/RESET into
a reducer event. Two TLS connections with identical exporter inputs cannot
cross-wire their frames by reusing a bare generation value.

One bounded session mailbox owns reducer mutation. Local and peer DATA/CLOSE
ordering is preserved; urgent reset and completion control remain bounded.
Only the reducer's opaque `FlowFinished` capability can mint a terminal TUN
action. Informational `PeerReset` cannot terminate a flow by flow id alone.

The TUN action lane is separate from the legacy bulk relay lane, so unrelated
data-pressure pause cannot indefinitely block sink completions, half-close,
terminal, or owner-loss handling. Socket incarnation and epoch checks reject
late actions after rearm. An unexpected port closure emits one flow-local
owner-loss event; a normal terminal ends the bridge without synthesizing a
second owner-loss event.

Clean FIN supports both local-first and remote-first state sequences. Exact
terminal delivery has a bounded five-second wait for the local TCP tail; a
missing terminal or closed owner fails closed instead of leaking a slot.
Every stale, mismatched, or otherwise uninstalled async resumable open is
explicitly abandoned before its senders are dropped, leaving one
`LocalReset(LocalAbandon)` for the reducer. This also covers a closed
handshake-completion channel and an unexpected resumable result on the legacy
open seam.

## Deterministic real-smoltcp tracer

The harness runs a fake resumable upstream through the actual client TUN event
loop and smoltcp sockets. It proves:

- async open consumes no local TCP bytes before the resumable flow installs;
- one session-level `65,535B` ledger is shared by its flows;
- port/message/byte saturation leaves the final `97B` in smoltcp rather than
  dropping or resetting it;
- reducer storage and exact application ACK release replay ownership;
- two echo segments deliver exactly `65,632B`;
- every `SinkAccepted` byte corresponds to a real `send_slice` acceptance;
- LocalClose, SinkHalfClosed, and graceful FlowFinished occur exactly once;
- flow-port and global-byte ownership return to zero; and
- a zero-byte CloseWait completed by an async open is re-dirtied and emits one
  clean close rather than leaking the listener slot.

The focused tracer passes `2/2`; its frozen binary also passed 100 consecutive
runs.

## Review findings closed

Concentrated ownership, lifecycle, client-integration, and final acceptance
reviews found and closed these P0/P1 classes before Task 4:

- DATA could be reordered behind CLOSE by an over-broad control lane;
- sink acceptance could occur outside the existing D16/local-egress actor;
- local FIN used send capacity instead of lifecycle authority and missed
  remote-FIN-first TCP states;
- best-effort reset, detached bridge closure, and graceful terminal races
  could strand or prematurely abort one flow;
- legacy bulk backpressure could starve resumable lifecycle actions;
- replay capacity was per-flow rather than session-global;
- a same-length or caller-constructed frame could release the wrong replay
  ownership;
- sink, half-close, terminal, and post-attach frames lacked exact direction,
  flow, or transport-leg provenance;
- global replay exhaustion closed a flow instead of backpressuring it; and
- stale or undeliverable async open results could disappear without terminal
  authority.

Final independent reviews report `P0=0` and `P1=0` for the Task-3 local seam.

## Local gates

- owned-upstream focused unit tests: `43/43` PASS;
- all-target harness build: root `875/875` PASS, `3` ignored; main `2/2`;
- concurrency harness: `10/10` PASS, `4` ignored;
- typed leg/protocol provenance integration: `101/101` PASS;
- protocol integration: `19/19` PASS;
- public resumable session API: `1/1` PASS;
- real smoltcp fake-resumable adapter: `2/2` PASS and 100-repeat PASS;
- release build: PASS;
- strict root Clippy with only the four documented legacy allowances: PASS;
- rustdoc: PASS with existing unrelated dependency warnings;
- vendored Quinn: `40/40` PASS, `3` ignored; doctest `1/1` PASS;
- vendored quinn-proto: `330/330` PASS; docs `3/3` PASS;
- `cargo fmt --check` and `git diff --check`: PASS.

## Deferred contracts

Task 4 must retain two explicit contracts:

1. ownership already dequeued from a port is outside the port's queue-drain
   reach and must be dropped or released by the supervisor on exact ACK or
   `FlowFinished`; and
2. one logical session must share one `ResumableTcpPortFactory`; constructing
   multiple factories for one session would defeat the global byte bound.

UDP profiler attribution and deterministic supervisor task joining remain
later implementation work; neither is a Task-3 ownership blocker.

## Next

Proceed to Task 4 only: build the deterministic in-memory two-leg transport
harness with authenticated attach, blackout, reorder, duplicate, delayed ACK,
stale-leg, and fairness injection. Prove exact TCP continuity and bounded
ownership before implementing a production transport or asking for any WAN,
macOS TUN, VPS, or throughput run.
