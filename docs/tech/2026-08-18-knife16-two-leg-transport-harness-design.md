# Knife16 Task 4 Deterministic Two-Leg Transport Harness Design

Date: 2026-08-18

Status: **R1-R5 FOUNDATION ACCEPTED LOCALLY; R6-R9 NOT IMPLEMENTED;
TASK 4 NOT PASS; M3 BLOCKED**

## Decision

The implemented R1-R5 tracer bullets did not extend `FakeResumableUpstream`
into a nominal two-leg test. That fake remains useful as a Task-3 TUN/smoltcp
adapter tracer, but it constructs reducer inputs and effects inside one
process. R1-R5 instead established the real wire codec, exact live-leg seal,
attach exchange, owner/client supervisors, typed ownership binding, bounded
leg I/O, deterministic scheduler/fault substrate, `TargetIo`, and one complete
byte-level baseline. A blackout test built on the old fake shortcut could
still pass while the production design remained unable to resume a flow.

The remaining Task-4 work must extend the existing production-shared
deterministic seam rather than introduce a second controller path:

```text
smoltcp / ResumableTcpDriver                     TargetIo
              |                                    |
       client supervisor                       owner supervisor
              |                                    |
       encode exact Frame                     encode exact Frame
              |                                    |
          LegIo A <-> opaque ingress A <-> LegIo A
          LegIo B <-> opaque ingress B <-> LegIo B
              |                                    |
       decode_owned_exact                    decode_owned_exact
```

The in-memory transport and Target are adapters to the same supervisor and I/O
contracts that Tasks 6 and 7 must use with real transport and sockets. Faults
exist only at those I/O boundaries. Harness code may inspect snapshots and
counters, but it may not manufacture reducer authority, inspect the fault
oracle to make a production decision, or call a test-only switch operation.

This stage is intended to be sufficient for deterministic TCP correctness and
continuity under the modeled single-leg failures. It is not sufficient for
UDP, real-socket throughput, process ownership, security closure, WAN path
independence, or macOS acceptance.

## Current implementation position

R1-R5 are implemented as foundation, not merely proposed design:

- R1 composes lost `ATTACH_ACCEPTED` recovery through the client model with a
  typed, exact-leg catch-up capability;
- R2 binds owner ATTACH commit and subsequent DATA authority to the exact live
  leg seal;
- R3 serializes attach preflight, authority commit, model installation,
  acceptance publication, and opaque recovery publication;
- R4 binds dequeued driver DATA ownership to the reducer-minted
  `ReplayStored` receipt in one supervisor turn; and
- R5 crosses encode, bounded memory transport, decode, exact-leg binding,
  owner supervision, and `TargetIo` for one complete byte-level flow.

The original seven foundation P1s and the later source-admission,
reducer-input-carry, sole-factory-provenance, and category-budget findings are
closed and independently re-reviewed. R1-R5 now pass as a production-shared
local foundation: scheduler turns are single-event and checked, every physical
wire reservoir is bounded, source extraction owns byte and segment capacity
first, transient reducer admission returns the exact input, Target work is
typed and resumable, and one pristine supervisor is the sole factory mint.

This is not yet a Task-4 result. R6/R7 standby registration, probes,
controller arbitration, ordered acceptance-before-recovery, attach/replay,
stale-A retirement, and authenticated reverse-only switch hints are not
implemented. R8/R9 fault-matrix and real-smoltcp parity acceptance are also
open. The presence of R1-R5, or a passing R5 baseline, must not be reported as
Task-4 PASS.

## Stage goal

Prove, under a deterministic clock and bounded adversarial scheduling, that a
single logical TCP session can move from primary leg A to healthy leg B while:

- opening each Target TCP flow exactly once;
- delivering byte-exact, non-duplicated TCP streams in both directions;
- retaining and releasing application bytes through the existing typed
  ownership chain;
- rejecting stale-leg authority after the replacement commit;
- bounding replay, receive, wire, queue, task, and tombstone ownership;
- preserving unrelated-flow progress and lifecycle isolation; and
- keeping every complete positive-acceptance interruption below one second
  when the owner, leg B, and destination sink remain ready.

The main fault profile covers `300ms`, `500ms`, and `800ms` primary-leg
blackouts. An `800ms` A blackout does not imply an `800ms` replay horizon:
leg B remains healthy, and the switch plus its first application ACK must
complete inside the configured retained-coverage horizon. A scenario that
also withholds B for `800ms` must explicitly allocate an `800ms` horizon; it
may not silently reuse the `500ms` plan.

## Frozen scope and non-goals

The following remain frozen:

- D16 ownership, batching, and gauges;
- Endpoint pacing and conservation;
- MTU/PLPMTUD, pool size, QUIC windows, chunk size, Cubic, GSO default, and
  self-wake behavior;
- the legacy `TuicUpstream`, `FailoverUpstream`, Reality, and UDP behavior;
- Knife15 workload and quality thresholds; and
- the existing public production selection of `LegacyOwnedUpstream`.

Task 4 does not:

- claim a production transport or server;
- implement or qualify UDP path switching;
- open sockets, run macOS TUN, or contact a VPS;
- claim `>170 Mbit/s`; that is Task 7's real-socket gate;
- hide a missing switch trigger with test orchestration; or
- treat two independent session owners as sharing one Target socket.

## Reachability and old-path audit

The required uplink path is:

```text
smoltcp receive callback
  -> ResumableTcpPortFactory shared session capacity
  -> ResumableTcpDriver::try_recv_next / recv_next
  -> typed supervisor command retaining UplinkOwnership
  -> SessionModel::reduce(LocalData)
  -> exact ReplayStored receipt bound to UplinkOwnership
  -> SessionEffect::Transmit(Frame)
  -> Frame::encode
  -> selected LegIo
  -> opaque ingress
  -> peer LegIo
  -> Frame::decode_owned_exact
  -> exact EstablishedLeg seal
  -> owner supervisor / SessionModel::reduce(PeerFrame)
  -> TargetIo write
  -> exact positive Target acceptance
  -> ACK encode and reverse LegIo path
  -> reducer-minted ReplayAcknowledged
  -> UplinkReplayOwnership release
```

The downlink path reverses the session roles:

```text
TargetIo read
  -> owner SessionModel local DATA ownership
  -> encode / selected LegIo / opaque ingress / decode / exact leg seal
  -> client SessionModel sink offer
  -> ResumableTcpDownlink
  -> unchanged D16/local-egress admission actor
  -> exact positive smoltcp send_slice acceptance
  -> typed SinkAccepted completion
  -> client ACK over the selected leg
  -> owner replay release
  -> iface.poll -> flush_tx -> Endpoint reservation -> TUN
```

The Task-3 fake remains only a parity tracer at the final acceptance layer. It
is not the transport oracle. The legacy relay, old TUIC pacing and QUIC paths,
and existing local egress path remain active for legacy selection. Therefore
Task 4 is a parallel resumable-path proof, not a full data-plane replacement or
a measured throughput result.

## Required production-shared seams

### Session supervisor

There is exactly one mutating supervisor per endpoint of one logical session.
The owner-side supervisor additionally owns the session's `AttachAuthority`
and every `TargetIo` flow. Leg tasks, flow drivers, timers, and Target adapters
send typed commands; none holds mutable `SessionModel` access.

R1-R5 already exercise this supervisor for attach, catch-up, replay ownership,
the byte-level baseline, and Target execution. R6/R7 must place one
production-shared controller outside it; they must not duplicate reducer,
attach, replay, or Target ownership inside the harness.

The supervisor is the only component allowed to:

- call `SessionModel::reduce`;
- commit and install a replacement generation;
- translate reducer effects into encoded transport output, Target operations,
  or typed client/TUN completions;
- retain the map from `ReplayStored` to port/Target byte ownership;
- release that ownership after exact reducer-minted `ReplayAcknowledged` or
  `FlowFinished`; and
- declare a leg active, stale, lost, or joined.

The deterministic harness and future async runtime drive the same supervisor
state machine. They may differ only in how readiness and I/O are polled.

### Typed supervisor command

The R4 foundation gives `SessionOwnerMailbox` a typed driver-DATA path rather
than reconstructing a bare `SessionEvent` after dequeue. This remains a hard
contract: `DriverInput::Data` owns both bytes and a non-cloneable
`UplinkOwnership`; reducing a separately reconstructed `LocalData` event and
binding the receipt later would reintroduce a cancellation gap outside the
port's queue drain. Any internal event variant is not an alternate public leg,
driver, timer, Target, or harness ingress.

The closed command/envelope boundary must retain at least these semantic
variants as R6-R9 are added:

- exact flow-driver input, with `DriverInput::Data` and its ownership intact;
- exact leg receive bytes plus the live `EstablishedLeg` identity;
- leg send completion/failure and hard-liveness observations;
- exact Target open/write/read/half-close/reset completions;
- sink acceptance/half-close/terminal completions; and
- deterministic timer expiry and shutdown/join facts.

For a DATA command, one supervisor turn must reduce the local DATA, locate the
one matching `ReplayStored` effect, bind it to the dequeued ownership, and
store the resulting replay owner. If any step fails, that same turn releases
or terminally accounts for the ownership. No `.await`, queue handoff, or
second actor may intervene between dequeue and binding.

RESET remains urgent. DATA and CLOSE from one source remain one FIFO.
Continuously ready ordinary control may run at most eight times before one
ready DATA/CLOSE command. These are the actual production mailbox/driver
rules; the harness may not simulate a friendlier policy.

### `LegIo`

`LegIo` is a bounded, message-framed byte interface. A sender gives it the
result of `Frame::encode`; a receiver obtains exact encoded bytes and must run
`Frame::decode_owned_exact` before the live `EstablishedLeg` seals the decoded
frame. The real transport adapter may assemble stream chunks internally, but
the supervisor boundary receives one exact bounded frame.

R5 already proves that baseline byte path and exact local seal. R6/R7 extend
it with lifecycle/probe inputs and independent per-leg controller queues; they
must not add a semantic-frame shortcut beside `LegIo`.

Each leg has independent:

- control and DATA queue accounting;
- send/receive readiness;
- cancellation and join ownership;
- hard-liveness/path-probe state; and
- physical-send, delivered, duplicate, drop, and stale counters.

The in-memory fault injector sits after encode and before decode. It can omit,
delay, reorder, duplicate, truncate, or corrupt byte messages according to
leg, direction, send ordinal, and deterministic schedule. It may not inject a
`Frame`, `Record`, `SessionEvent`, `CommittedLeg`, `SinkAccepted`, or terminal
capability. Record-selective scenarios may classify the already encoded bytes
for scheduling, but the delivered authority must still come only from the
receiver's real decode and exact-leg bind.

Opaque in-memory ingress A and B forward only bytes and transport metadata.
They never authenticate the session, decode records, own replay, or open the
Target. This preserves the production trust boundary.

### `TargetIo`

`TargetIo` is the sole Target boundary used by the owner supervisor. Its
in-memory implementation and Task-6 socket implementation share the same
semantic interface for:

- one open attempt and one open result per logical flow;
- bounded partial, zero, would-block, and positive write acceptance;
- bounded reads and EOF;
- half-close, reset, and terminal completion; and
- cancellation and join ownership.

Target faults are injected only through `TargetIo`. The fake Target cannot
construct `PeerOpenResolved`, `SinkAccepted`, ACK, close, reset, or finished
events directly. `target_open_count == 1` is a hard invariant, including after
duplicates, replacement, resynchronization, and late old-leg output.

R5 already crosses this boundary. Operational Target failures now become
flow-local typed completions, partial effect turns retain one bounded resume
obligation, invariant failures poison and retain ownership, and expiry can
force-discard adapter buffers before exact join and retirement. Those
foundation P1s are closed; R6 must preserve their tests unchanged.

### Production-shared session controller

R6/R7 require two role-specific outer controllers:

```text
ClientSessionController
  -> client SessionSupervisor
  -> exact active/standby/retired LegIo capabilities

OwnerSessionController<T: TargetIo>
  -> OwnerTargetExecutor<T>
  -> exact active/standby/retired LegIo capabilities
```

The controller is the sole switch-orchestration mutator. Per logical session
it owns exactly one active `AttachedLeg`, at most one authenticated
`RegisteredStandby`, at most one bounded `RetiredLeg`, any pending
attach/catch-up, independent per-leg outbound queues and cancel/join
obligations, liveness/probe clocks, application-ACK clocks, one switch epoch,
and the checked recovery budget. The reducer, replay receipts, Target handle,
and application ownership remain inside the existing supervisor/executor.

The controller accepts only closed, provenance-bearing inputs:

- encoded bytes decoded and bound by the exact receiving `LegIo`;
- an exact-seal transport close/error observation from that leg adapter;
- an opaque controller-minted timer token carrying its leg/switch epoch;
- existing typed driver, Target, and sink completions; and
- controller-internal ordered-send, cancel, and join completions.

A numeric leg ID, fault interval, dropped-message count, scheduler time alone,
or bare `SessionEvent` is not a switch capability. The memory harness supplies
the same byte delivery, actual close, and timer-expiry inputs that a future
runtime adapter supplies. It cannot call `switch_to_b`, inspect
`MemoryFaultScript` from the controller, or convert non-delivery directly into
a liveness event.

Each controller turn is finite. It returns bounded actions such as queueing an
exact record on A or B, arming/canceling an opaque timer, and canceling/joining
a retired leg. Per-leg queues are required: pressure or cancellation on A
cannot head-of-line block B registration, probe, hint, ATTACH, or acceptance.
There remains one logical replay owner in the reducer; canceling physical A
copies after B commits releases only their separately accounted queue/wire
ownership.

The supervisor may expose bounded, read-only observations to its controller,
but no replay receipt or mutation capability. The minimum application-ACK
observations are:

- replay became outstanding for exact `(flow, direction, start, end)`;
- reducer-accepted application ACK advanced to an exact offset, with final and
  released-byte facts; and
- flow/direction finished.

They must be derived from reducer-minted `ReplayStored`,
`ReplayAcknowledged`, and `FlowFinished` facts. A decoded ACK record, QUIC ACK,
wire delivery, duplicate DATA/replay, or non-advancing application ACK cannot
start or refresh the application-ACK progress clock.

### Standby registration and leg-control wire records

R7 cannot reuse `ATTACH` as standby registration. A valid ATTACH advances the
owner generation and makes A stale; using it to prewarm B would perform the
switch before any legitimate trigger. The resumable profile therefore needs
one named negotiated feature, `STANDBY_CONTROL_V1`, and five new fixed-size or
strictly bounded wire records:

| Record | Direction | Required semantics |
|---|---|---|
| `STANDBY_REGISTER` | client -> owner on B | Session, active generation, fresh standby nonce, negotiated contract, and exact B TLS binding are proof-bound; no generation change |
| `STANDBY_ACCEPTED` | owner -> client on B | Echoes the exact session/active generation/standby nonce and mints only the client-side exact-B registered-standby capability |
| `LEG_PROBE` | either endpoint on exact A or B | Carries an exact leg-epoch nonce and monotonic probe sequence |
| `LEG_PROBE_ACK` | peer on that same exact leg | Echoes leg epoch and sequence; only the matching pending probe refreshes health |
| `SWITCH_HINT` | owner -> client on registered B | Binds reverse ACK-stall cause and evidence to the exact session, active generation, standby nonce, and monotonic hint sequence |

Existing v1 frame/header semantics and existing record encodings remain
byte-identical. The new record IDs must come from the unallocated v1 space and
are accepted only after `STANDBY_CONTROL_V1` was negotiated as required by the
Knife16 resumable profile. A compatibility profile that does not negotiate the
bit cannot send or act on these records. They are leg-control records and must
never enter `SessionModel::PeerFrame` or mint DATA, ACK, CLOSE, RESET, Target,
sink, terminal, replay-release, or generation authority.

`STANDBY_REGISTER` uses a proof type and HMAC domain distinct from ATTACH. Its
transcript binds at least the exact session ID, current active generation,
fresh registration nonce, selected session protocol version, negotiated
feature set, owner identity, ALPN, TLS exporter, and device principal. The
owner verifies it against the exact B `AttachTransportBinding`; verification
does not call the attach CAS, advance generation, or mint `CommittedLeg`.
Successful verification mints only the owner-side exact-B registered-standby
capability. Secret and exporter material remain non-wire and redacted. A
probe's leg-epoch nonce is the attach nonce on active A and the registration
nonce on standby B.

After registration, `STANDBY_ACCEPTED`, probes, probe ACKs, and hints are
authenticated by the end-to-end TLS channel plus their exact process-local B
seal and correlation fields. This relies on the frozen opaque-ingress trust
boundary: if ingress terminates the authenticated session TLS instead of
forwarding it end to end, this design is invalid and Task 4 stops. A
same-nonce retransmission on the same B seal is idempotent; a conflicting
nonce, seal, session, generation, or negotiated contract is rejected before
replacing the one bounded standby slot.

`SWITCH_HINT` additionally carries a monotonic `hint_seq`, exact flow ID,
`Direction::TargetToClient`, oldest unacknowledged offset, and the closed cause
`ReverseApplicationAckStall`. The owner may emit it only while its reducer has
replay outstanding for that exact flow/direction and B has a fresh matching
probe ACK. The client requires the correlated live flow plus the exact control
binding; wrong seal, session, generation, nonce, direction, cause, offset,
duplicate/lower sequence, idle/terminal flow, or stale B health is
snapshot-inert. The hint grants permission only to start the client's existing
exact-next ATTACH transaction. The subsequent correlated `ATTACH_ACCEPTED` is
the switch acknowledgment; no separate `SWITCH_ACK` record is required.

### Hard liveness, ACK stall, and switch arbitration

Hard liveness is not generic send pressure. A queue-full, would-block, or
retryable encoded-send failure retains its frame and cannot mark A dead. The
only hard-liveness capabilities are:

1. a terminal close/error reported by the exact authenticated A transport; or
2. expiry of a controller-owned probe/PTO token that was armed after an exact
   `LEG_PROBE` and was not canceled by the matching exact-seal
   `LEG_PROBE_ACK`.

A silent A blackout is therefore detected through the missed probe budget,
not by reading fault state. B is a healthy standby only after a current exact
probe ACK within the configured freshness bound; registration acceptance is
not indefinite health evidence. A direction-selective A DATA/ACK blackout may
leave A probes healthy. That is intentional: client-to-Target outstanding
replay lets the client act on its own application-ACK stall, while
Target-to-client outstanding replay requires the authenticated owner hint over
B.

For a hard A failure, the client controller consumes the exact failure once,
applies the existing `LegLost`/grace transition, and starts one exact-next
ATTACH on the already authenticated B. The owner uses the existing
preflight/CAS/install transaction and preserves the same Target executor and
Target handle. It orders `ATTACH_ACCEPTED` before recovery; the client validates
acceptance on exact B and installs it. Committing B atomically consumes A into
the bounded `RetiredLeg` state. Late A DATA, ACK, CLOSE, RESET, acceptance, or
status bytes can then be counted and discarded or reducer-rejected as stale,
but cannot mint a new `SessionEvent` or mutate flow, sink, Target, terminal, or
ownership state.

Only the client initiates generation change. The owner can send a hint but
cannot attach on the client's behalf. Hard failure, local application-ACK
stall, and authenticated hint race through one switch epoch and one pending
nonce; simultaneous triggers cannot create two ATTACH transactions. Stale
timer tokens and retired-leg completions are epoch checked and snapshot-inert.

## Attach and generation correctness gates

### Owner-side exact seal

R2 now provides exact-leg paths on both sides. The client uses
`EstablishedLeg::begin_attach`, `PendingAttach::validate_response`, and
`AttachedLeg`. The owner consumes an ATTACH decoded and bound by one exact
`EstablishedLeg`, verifies it against that leg's exact transport binding, and
returns a non-cloneable owner attach outcome. A successful outcome contains
the `AttachedLeg` capability for that same seal and the correlated accepted
response. A resynchronization outcome contains only the correlated
authenticated status response and no DATA authority. R6/R7 must reuse this
path for B rather than exposing a bare-frame controller API.

The following must be impossible or rejected:

- validating bytes received on leg A using leg B's binding;
- committing an ATTACH through a bare `Frame` after the transport boundary;
- using a status outcome to accept DATA;
- accepting the first post-attach DATA on a different connection with the
  same numeric generation; and
- cloning one commit into two live `AttachedLeg` capabilities.

### Serialized attach barrier

R3 makes attach verification, generation commit, owner-model installation,
active-leg publication, and accepted-response publication one supervisor
transaction with no await point. Before mutating generation, the supervisor
preflights session identity, semantics, exact-next generation, and the model
transition. After commit, model installation is infallible under that
preflight; otherwise the entire request fails before authority changes.

Concurrent `n+1` requests serialize: exactly one can install. A simultaneous
`n+2` request cannot overtake `n+1`. No DATA/CLOSE/ACK from the candidate leg
is dequeued as authoritative until its install barrier completes. If response
publication fails after installation, the lost-acceptance recovery below is
the only permitted path; the supervisor must not roll generation backward.

For R6/R7, queue admission of `ATTACH_ACCEPTED` must also precede recovery
publication on B. If a future real transport maps control and replay to
independently reorderable streams, FIFO insertion into two queues is not a
sufficient barrier: the adapter must expose one ordered message boundary or a
send-completion capability that proves acceptance is ordered before the first
recovery record.

The R1-R5 publication value currently exposes acceptance and recovery as
sibling non-cloneable values; that proves at-most-once ownership, not queue
order. No non-test controller consumes it yet. Therefore the first R6
controller slice must make recovery consume a successful ordered-enqueue/send
receipt, or combine acceptance and recovery in one atomic queue operation.

### Lost `ATTACH_ACCEPTED` typed catch-up

Before R1, isolated authentication resynchronization was not compositional
with `SessionModel`. The concrete historical RED was:

1. both endpoints start at generation 1;
2. the owner commits and installs generation 2;
3. the generation-2 `ATTACH_ACCEPTED` is lost, so the client model remains at
   generation 1 and becomes legless;
4. a fresh proof-valid generation-2 request receives authenticated status
   saying the owner is at generation 2;
5. `GenerationResynchronization::next_request` asked for generation 3; but
6. the pre-R1 client reducer rejected generation 3 because
   `ReplacementAttached` only accepted local-current `+1`, which was 2.

R1 closes that RED with a non-forgeable catch-up capability minted only by
`PendingAttach::validate_response` from the exact correlated, authenticated
status on the same leg. Applying it while the client is legless advances an
`observed_generation_floor`; it does not mint an active `CommittedLeg`, allow
DATA, release replay, or grant Target authority.

Required invariants are:

```text
active_generation <= observed_generation_floor
next_attach_generation == observed_generation_floor + 1
accepted_generation == observed_generation_floor + 1
```

An accepted replacement sets both values to the committed generation. Catch-up
is same-session, same-semantics, request-correlated, monotonic, legless-only,
and overflow checked. Stale/lower status, active-session catch-up,
cross-session status, wrong-leg status, or uncorrelated nonce fails closed.
The generation-2 status followed by generation-3 attach must compose through
the real supervisors and encoded-byte path.

## Legitimate switch triggers

The harness knows when it starts a blackout, but that knowledge is not a
production signal and may never call `switch_to_b` or enqueue an attach.
Only these production-shared observations may trigger replacement:

1. **Hard leg liveness:** a client-observable transport close/error or missed
   authenticated path-probe/PTO budget on A. The client controller initiates
   an exact-next ATTACH on ready leg B.
2. **Client-to-Target application-ACK stall:** the client is the direction's
   sender, has outstanding replay, and its application ACK age crosses the
   configured bound while B's authenticated standby probe remains healthy.
   The client controller initiates the attach.
3. **Target-to-client application-ACK stall:** the owner is the direction's
   sender and observes outstanding replay with stale application ACK. The
   owner controller sends a session- and generation-bound switch hint over the
   authenticated standby control path. Only after the client verifies that
   hint does it initiate the exact-next ATTACH on B.

The standby control binding and switch hint are production protocol behavior,
not an out-of-band test callback. They grant no DATA, ACK, CLOSE, Target, or
generation authority by themselves. They must be bounded, replay protected,
and tied to the same session/device/owner identity and the direction whose
application ACK stalled. An idle direction has no outstanding replay and
cannot trigger on age alone.

The implemented R1-R5 protocol has no end-to-end standby hint path. Therefore
reverse-only DATA blackout remains an expected RED until that real
authenticated mechanism exists. A harness that switches at
`blackout_start`, reads injector state, or uses a privileged timer callback is
a false PASS and blocks Task 4 acceptance.

Every switch records one cause from a closed enum, the oldest unacknowledged
offset/age, A and B liveness, attach start/commit/first-ACK times, and the old
and new generations. Numeric timeout selection is not tuned per scenario.

## Capacity and event budget

### Combined recovery budget

The `500ms` value is the complete retained-coverage horizon, not a standalone
probe, stall, attach, or replay timeout. A checked `RecoveryBudget` must reject
overflow and any configuration for which either complete path exceeds the
horizon:

```text
hard path:
T(last application ACK -> exact hard-liveness signal)
  + T(exact-next ATTACH -> installed B)
  + T(B replay -> first B application ACK)
  <= 500ms

reverse-only path:
T(last application ACK -> reducer-observed ACK stall)
  + T(authenticated hint over healthy B)
  + T(exact-next ATTACH -> installed B)
  + T(B replay -> first B application ACK)
  <= 500ms
```

Every component is measured in deterministic monotonic controller time and
carried by epoch-bound timer/transition evidence. Unused budget in one
component may cover another, but no scenario may independently assign `500ms`
to detection, attach, and replay or tune a component after seeing the fault.
The first positive B application ACK, not queue insertion, byte delivery,
decode, or transport ACK, closes the recovery interval.

An `800ms` A blackout remains admissible under a `500ms` retained horizon only
when B is healthy and the corresponding combined inequality reaches its first
B application ACK within `500ms`; A may remain black for the rest of the
fault. Withholding B itself for `800ms` is a different capacity contract and
must fail the `500ms` constructor rather than enlarge a queue during the test.

### Retained application bytes

For directional application rate `R` and total retained-coverage horizon `H`:

```text
B = ceil(R * H_ns / 8,000,000,000)
```

At `H = 500ms`:

| Rate | Bytes/direction | Full duplex |
|---|---:|---:|
| 100 Mbit/s | 6,250,000 | 12,500,000 |
| 170 Mbit/s | 10,625,000 | 21,250,000 |
| 240 Mbit/s | 15,000,000 | 30,000,000 |

`H` includes normal application-ACK age plus detection, attach, replay, sink
acceptance, and the first application ACK on B. A main `800ms` A-blackout case
may use the `500ms` plan only when B is healthy throughout and that first B ACK
arrives within `500ms`; A may remain black after ownership has moved. If B
recovery itself is held for `800ms`, the raw minimum becomes
`10,000,000/17,000,000/24,000,000B` per direction at
`100/170/240 Mbit/s`, or `20/34/48MB` full duplex, before adding normal ACK
age.

The main 100-Mbit/s workload uses a `5ms` traffic quantum:

```text
100,000,000 bit/s * 0.005s / 8 = 62,500B
```

This fits under the `65,536B` DATA payload bound. One hundred quanta represent
`6,250,000B` and exactly `500ms` of application bytes per direction.

### Frame and physical-delivery bound

For application bytes `B`, maximum DATA payload `W = 65,536`, and `F` forced
tail fragments:

```text
tail = min(F, B)
S = floor((B - tail) / W) + tail
encoded DATA bytes = B + 37S
```

The `37B` term is the exact v1 DATA frame overhead. With one final tail
fragment:

| Rate | `B` | `S` | Encoded once |
|---|---:|---:|---:|
| 100 Mbit/s | 6,250,000 | 96 | 6,253,552B |
| 170 Mbit/s | 10,625,000 | 163 | 10,631,031B |
| 240 Mbit/s | 15,000,000 | 229 | 15,008,473B |

With 64 forced tail fragments, `S` is `159/226/292` and encoded bytes are
`6,255,883/10,633,362/15,010,804B`. One switch may emit at most one original
and one replay copy, or `2 * (B + 37S)`. An injector may duplicate each send
at most once, so delivered/decode work is bounded by
`4 * (B + 37S)`. For the one-tail table that is
`25,014,208/42,524,124/60,033,892B` per direction.

Logical replay ownership is counted once in application bytes. Original,
replay, and fault-injected physical copies have separate counters and never
inflate or release logical ownership.

### Partial acceptance and action bound

`4S` is not a sufficient scheduler bound because a sink is allowed to accept
one byte at a time. Every scenario declares:

- `P`: maximum positive acceptance pieces per unique DATA frame;
- `Z`: maximum zero/would-block completions before a readiness transition;
- `S`: the derived logical DATA frame count; and
- a fixed bound for attach, probe, timer, close, reset, and join actions.

Per direction, the harness enforces at least these category bounds:

```text
logical DATA sends including one replay <= 2S
fault-delivered DATA decodes             <= 4S
positive sink/Target accepts             <= P*S
zero/would-block attempts                <= Z*S
ACK records constructed                  <= P*S + 1 final ACK
fault-delivered ACK decodes              <= 2*(P*S + 1)
```

The main capacity scenarios use a small fixed `P` and `Z`. A separate bounded
micro-scenario uses a small payload and one-byte acceptance to exhaustively
prove partial-acceptance ownership. It must not turn the full `6.25MB` case
into an accidental millions-of-events liveness test. Exceeding any declared
counter is a deterministic failure, not permission to enlarge an unbounded
queue.

Global replay capacity derives from aggregate directional rate, not
`per_flow * flow_count`. Two-flow tests share one
`ResumableTcpPortFactory` and one exact global ledger.

## Deterministic scheduler

No wall-clock timeout, Tokio scheduling luck, `sleep`, or host load is
acceptance evidence. A virtual scheduler owns time and every runnable actor.
Its stable key is:

```text
(deadline, phase, actor_id, sequence)
```

The phases cover fault release, transport receive, supervisor command, Target
or sink completion, timer expiry, and join cleanup. One turn performs one
bounded action. Work created at the same deadline cannot re-enter ahead of
already-ready peers in the same micro-round. A rotating actor cursor provides
round-robin service among same-deadline runnable actors.

For each deadline, a continuously ready actor receives a turn within
`N_ready` eligible turns, where `N_ready` is the number of actors ready in
that round. An actor cannot consume a second same-deadline turn before every
eligible peer has consumed one, apart from a prerequisite phase that creates
the peer's readiness. The test records maximum ready-to-service turns for
control, DATA/CLOSE, both legs, each flow, Target, and sink.

Acceptance runs:

- one canonical schedule;
- boundary orders that put loss, ACK, attach, FIN, reset, and timeout at the
  same virtual timestamp; and
- a bounded set of seeded adversarial permutations whose seeds are printed on
  failure and replayable exactly.

The production lane rules are asserted inside these schedules: RESET wins the
next eligible driver service; ready ordinary control gets the next owner
opportunity unless the bounded source turn is due; after at most eight
continuous controls, one ready DATA/CLOSE item runs; DATA cannot pass an
earlier CLOSE or vice versa within the source FIFO.

## Fault model

Each injected fault is finite and has a declared bound:

- omission and crash-stop of leg A;
- delayed delivery with exact release time;
- adjacent and bounded-window reordering;
- at most one physical duplicate per send;
- first, middle, and last DATA/ACK omission;
- full A blackout and direction-selective A DATA or ACK blackout;
- late A output after B commits;
- simultaneous generation requests and lost accepted/status responses;
- one-direction leg task cancellation;
- bounded control, DATA, replay, and Target/sink pressure;
- local FIN, remote FIN, and RESET immediately before/during/after switch;
- one flow blocked while another remains ready; and
- both legs unavailable, for safety and cleanup only.

No liveness promise is made while the sole owner, both legs, or the relevant
Target/sink is unavailable. Safety, bounds, stale-authority rejection, and
cleanup still apply.

## Safety invariants

Every scenario asserts:

1. `TargetOpen(flow) == 1` and there is one live Target owner.
2. Each receiver output is always a prefix of its exact source and equals the
   source at graceful completion.
3. Duplicate wire delivery never duplicates Target or local delivery.
4. Application ACK equals exact positive Target/smoltcp sink acceptance; wire
   delivery, decode, buffering, zero acceptance, or abandon grants no ACK.
5. A stale generation grants no new DATA, ACK, CLOSE, RESET, Target, sink, or
   terminal authority after B commits.
6. Replay, receive ranges, queues, pending opens, commands, timers, joins,
   tombstones, and physical copies remain within their declared bounds.
7. DATA and CLOSE preserve source FIFO; RESET is urgent and terminally owns
   abandoned source bytes.
8. One failed leg or flow does not close or starve an unrelated ready flow.
9. Logical application ownership and physical retransmit/duplicate accounting
   are distinct and reconcile exactly.
10. Graceful cleanup reaches zero live flows, Target handles, leg tasks,
    queued commands, replay bytes, receive bytes, and transport bytes, with
    only the configured count-bounded terminal tombstones allowed before
    their deterministic expiry.

## Conditional liveness invariants

When the session owner, leg B, Target/sink, and scheduler remain ready and
fair:

- from the last application ACK on A to the first application ACK on B is no
  more than the configured retained-coverage horizon;
- the maximum gap between complete positive receiver acceptances is strictly
  less than `1s`;
- the active byte stream makes positive progress after replacement;
- source DATA/CLOSE and control each make progress under sustained pressure;
- a ready unrelated flow receives positive service within its declared fair
  turn bound;
- capacity exhaustion backpressures and later recovers without byte loss or
  flow reset; and
- all tasks and owned bytes clean after terminal input.

The one-second assertion is made over complete deterministic acceptance
events, not partial log lines or wall-clock buckets. Baseline idle time with no
outstanding source bytes is not counted as an interruption.

## Acceptance matrix

| ID | Scenario | Required proof |
|---|---|---|
| T4-01 | Baseline A plus authenticated registered/probed B, full duplex | Registration does not advance generation or grant B DATA authority; exact bytes, one Target open, zero replay after ACK, clean FIN |
| T4-02 | A actual close and silent full blackout for `300/500/800ms`, B healthy | Exact close or missed-probe trigger selects B without oracle access; first B application ACK within horizon; positive-accept gap `<1s` |
| T4-03 | Asymmetric A DATA blackout and asymmetric ACK blackout | Direction-local application-ACK trigger is reducer-derived; reverse-only uses authenticated standby hint; exact bytes/ACK |
| T4-04 | First/middle/last drop, adjacent reorder, max-one duplicate | Exact dedup/reassembly; no false ACK; all event bounds hold |
| T4-05 | Delayed A DATA/ACK/CLOSE/RESET/control after B commit | Bounded retired A cannot mint a session event; every stale mutation is rejected; output remains exact |
| T4-06 | Simultaneous attaches plus lost `ATTACH_ACCEPTED` and status retry | One commit per generation; typed catch-up reaches generation 3; first DATA waits for install barrier |
| T4-07 | Two active flows sharing one factory/global ledger | Block one flow/leg; unrelated flow makes measured positive progress; Target opens once per flow |
| T4-08 | Continuous control plus bulk replay | Actual max-eight lane rule and round-robin actor bound prevent control/source starvation |
| T4-09 | Partial, zero, would-block, per-flow and global capacity pressure | ACK equals positive acceptance; source is retained on backpressure; one-byte micro-test passes |
| T4-10 | Local-first and remote-first FIN around switch | Final offsets exact; half-close completes once; replay ownership reaches zero |
| T4-11 | RESET before/during/after attach and replay | RESET wins bounded service; no post-terminal delivery or ownership leak |
| T4-12 | Both legs fail or owner unavailable | No liveness claim; safety, bounded grace, terminal ownership, and cleanup pass |
| T4-13 | Same in-memory adapter through real `run_owned_event_loop`/smoltcp | Byte/ACK/lifecycle/capacity results match the pure supervisor harness |

T4-13 is a parity tracer, not a second fake implementation. It must use the
same supervisor, `LegIo`, fault-free MemoryIngress, `TargetIo`, attach, and
ownership code as T4-01; only the client source/sink is replaced by the real
smoltcp event-loop boundary.

## Required observability and discriminators

Snapshots are read-only and contain no mutation capability. At minimum record:

- per generation and leg: encoded frames/bytes, physical sends, deliveries,
  drops, delayed releases, duplicates, decode rejects, stale rejects, queue
  high-water, liveness events, cancel, and join;
- per direction and flow: source bytes, next sent, application accepted/ACKed,
  retained/replayed bytes, deduplicated bytes, gaps, overlaps, final offsets,
  positive-accept gaps, and first ACK per generation;
- attach: request/status/accepted nonce and generation, catch-up floor,
  preflight, commit, model install, response publication, and switch cause;
- controller: standby registration/acceptance nonce and exact leg, probe/ACK
  sequence and freshness, application-ACK clock source, hint sequence/cause/
  flow/direction/oldest offset, switch epoch, each combined-budget component,
  B attach start/install/first application ACK, retired-A stale records, and
  ordered-send/cancel/join completion;
- Target: open attempts/successes, reads, positive/zero/would-block writes,
  accepted bytes, EOF, half-close, reset, and live handle count;
- scheduler: turns per actor/class, longest ready-to-service wait, pending
  events, final virtual time, and adversarial seed; and
- ownership: per-flow/global port bytes, reducer replay/receive bytes, queued
  wire bytes, pending commands, live tasks, and terminal tombstones.

These counters select the failure boundary:

- encoded but not delivered is `LegIo`/injected transport;
- delivered but not decoded/bound is codec or leg provenance;
- bound but not reduced is supervisor ordering/capacity;
- reduced but not Target/sink accepted is `TargetIo` or local D16/sink service;
- accepted but not application-ACKed is reducer/effect/return-leg service;
- application ACK advances but replay ownership remains is typed receipt
  release;
- reverse-only ACK age crosses the bound without an authenticated switch hint
  is missing production control architecture; and
- duplicate Target/local bytes is an immediate protocol correctness failure.

## Minimal implementation order

Implementation must deepen the production seam in dependency order. It must
not write a harness-only controller first and backfill protocol authority
later. The accepted vertical order is:

| Slice | Files | Smallest behavior and mandatory retained regression |
|---|---|---|
| **P — protocol** | `src/resumable/protocol.rs` | Add the named feature and five bounded leg-control records, codec, malformed-input rejection, and golden vectors. Existing v1 record vectors remain byte-identical; the protocol integration and public session API remain green. |
| **A — authentication** | `src/resumable/auth.rs` | Add the distinct standby HMAC transcript and proof verification without generation CAS. Preserve authenticated/concurrent/stale attach, lost-response catch-up, transcript mutation, uniform rejection, and known-answer tests. |
| **L — exact leg capability** | `src/owned_upstream/leg.rs`, `src/owned_upstream/leg_io.rs` | Add pending/registered standby, authenticated leg control, exact hard-liveness, and retired-leg capabilities; classify leg control away from reducer frames. Preserve byte encode/decode/seal tests, exact provenance, bounded queue retry/cancel, and one-leg cancellation isolation. |
| **C1 — hard close** | new `src/owned_upstream/controller.rs` plus the smallest supervisor/executor observation plumbing | An actual exact-A transport close causes one B ATTACH and exact replay, with one Target open and no fault-oracle read. Preserve attach barrier, simultaneous attach, catch-up, replay ownership, exact Target open, and R5 baseline tests. |
| **C2 — silent failure and retirement** | controller, optionally split `liveness.rs` only after the interface is stable | Exact probe deadline detects silent A loss; checked `RecoveryBudget` rejects overcommit; B health is fresh; old A becomes bounded and stale. Preserve all C1 and scheduler/wire ownership tests. |
| **C3 — reverse-only hint** | controller plus bounded read-only observations in `supervisor.rs` and `owner_target.rs` | Reducer-owned reverse application-ACK stall emits one authenticated hint on B; client verifies it and starts one attach. Preserve Target partial/zero/would-block ACK semantics, flow-local terminal behavior, and all C1/C2 tests. |
| **H — harness matrix** | `src/owned_upstream/two_leg_harness.rs` | Adapt the harness to the production controllers and add R6/R7 scenarios. Preserve the R5 production byte baseline, pressure retry, global scheduler, full outbound queue, attach byte budget, physical fault counters, deterministic replay, and cancellation accounting. |

Each row is one or more tracer bullets, never a horizontal batch. Run one
behavioral RED, add only its minimum production implementation, restore the
row's regression floor, and only then add the next RED. The foundation P1s are
closed and re-reviewed; P is now the next slice. Controller work cannot begin
before P, A, and L are green.

At final H acceptance, rerun the entire accepted floor plus additions:
owned-upstream focused tests, root all-target tests, concurrency harness, typed
provenance, protocol integration, public session API, real-smoltcp parity and
repeat, release, strict Clippy, rustdoc, vendored Quinn/proto and docs,
`cargo fmt --check`, and `git diff --check`. The pre-R6 accepted counts are a
floor, not a substitute for the new assertions.

## TDD sequence and expected REDs

Implement one vertical RED/GREEN slice at a time:

1. **R1 — lost acceptance composition (foundation present):** reproduce generation
   `1 -> owner 2 / client 1 -> status 2 -> attach 3`. GREEN only through the
   typed legless catch-up capability and real reducer.
2. **R2 — owner exact-leg attach (foundation present):** prove wrong-seal and bare-frame commits
   cannot mint owner DATA authority. GREEN adds the smallest seal-bound owner
   outcome.
3. **R3 — attach install barrier (foundation present):** race `n+1`, `n+2`, first DATA, and failed
   response publication. GREEN serializes preflight/commit/install/publication
   without rollback or partial authority.
4. **R4 — dequeued DATA ownership (foundation present):** cancel at every boundary from
   `DriverInput::Data` dequeue through `ReplayStored` binding. GREEN leaves
   exactly one retained or terminal owner and no permit leak.
5. **R5 — byte-level baseline (foundation accepted):** run one full flow through encode,
   MemoryIngress, decode, exact leg bind, owner, and `TargetIo`; direct reducer
   construction is forbidden. R1-R5 pass as a local foundation only and do not
   satisfy Task-4 blackout/switch acceptance.
6. **R6a — actual hard close:** an exact A transport close reaches the
   production controller, causes exactly one B attach/replay, keeps one Target
   open, and never consults the fault oracle.
7. **R6b — silent hard failure:** an A byte blackout without a close switches
   only after the exact outstanding probe epoch expires; a fresh exact B probe
   is required.
8. **R6c — retired A:** late A DATA, ACK, CLOSE, RESET, acceptance, and status
   after B commit are snapshot-inert and every physical queue/task byte joins
   or releases exactly.
9. **R6d — recovery capacity:** a component sum above `500ms` is rejected;
   the 100-Mbit/s, 5ms-quantum case admits exactly 100 quanta per direction.
10. **R7a — no oracle shortcut:** a reverse-only A DATA/ACK blackout may leave
    A's probe healthy; with no authenticated B hint the client does not switch.
11. **R7b — authenticated reverse switch:** reducer-owned
    Target-to-client application-ACK age causes one B hint, one client attach,
    exact replay, and a first B application ACK inside the combined horizon.
12. **R7c — hint rejection:** wrong seal, session, generation, nonce,
    sequence, direction, cause, offset, idle flow, or stale B health cannot
    trigger a switch; duplicate/lower hints cannot start a second attach.
13. **R7d — trigger race:** simultaneous hard failure, local stall, and hint
    consume one switch epoch and one nonce and commit at most one generation.
14. **R8 — bounded fault/fairness matrix:** add reorder, duplicate, pressure,
   multi-flow, FIN, RESET, both-leg loss, and seeded schedules incrementally.
15. **R9 — real-smoltcp parity:** reuse the accepted in-memory transport through
   `run_owned_event_loop` with no alternate reducer path.

Expected REDs above authorize the corresponding smallest architectural
implementation. An unexpected repair or regression failure requires a causal
analysis and written modification plan before editing the failed invariant.
No timeout, queue, replay, D16, Endpoint, MTU, pool, QUIC, Cubic, GSO, or
workload constant may be adjusted merely to turn a RED green.

## Stop rules

The following are immediate architectural stops (P0). Task 4 remains
incomplete if any one is needed for a passing test:

- the controller reads a fault oracle, calls a raw `switch_to_b`, or the
  harness constructs reducer events/effects or liveness authority;
- ATTACH is reused as standby registration, or standby/hint authority is not
  bound to exact end-to-end TLS, leg seal, session, active generation, nonce,
  and negotiated contract;
- B can send authoritative DATA, ACK, CLOSE, or RESET before its attach install
  barrier, or recovery can overtake `ATTACH_ACCEPTED` publication;
- two supervisors/Target executors own one logical session, a replacement
  reopens the Target, or `TargetOpen(flow)` exceeds one;
- lost-acceptance catch-up no longer composes, owner attach loses exact-seal
  provenance, attach authority can diverge from model installation, or
  dequeued DATA crosses an unowned cancellation gap;
- retired or stale A can mutate flow, ACK, close, reset, sink, Target,
  terminal, replay, or ownership state after B commits;
- a decoded/wire/QUIC ACK rather than reducer-minted application acceptance
  grants replay release or refreshes progress authority;
- retained bytes, event work, queues, registrations, nonces, probes, hints,
  timers, retired legs, tasks, joins, or tombstones require an unbounded
  allowance; or
- either checked combined-recovery inequality exceeds the retained horizon,
  a healthy ready B cannot keep the complete positive-acceptance gap below one
  second, exact payload/single Target/unrelated-flow/cleanup fails, or a frozen
  local architecture/tuning branch must reopen.

The following are P1 stop guards. They may be repaired by the next authorized
vertical slice, but Task 4 cannot be declared PASS while any remains:

- queue full, would-block, retryable send pressure, or an arbitrary elapsed
  timer is classified as hard leg death;
- any inbound byte or registration acceptance substitutes for a fresh exact
  B probe ACK;
- duplicate DATA/replay, duplicate or non-advancing ACK, or wire/transport ACK
  refreshes application-ACK age;
- a stale probe, stall, switch, or grace timer token can act after progress,
  standby replacement, retirement, or generation change;
- a hint on active A, unregistered B, or a wrong seal/session/generation/nonce/
  flow/direction/offset/sequence can start ATTACH;
- A and B share one outbound queue or actor such that A pressure blocks B
  registration, probe, hint, attach, acceptance, or replay;
- `ATTACH_ACCEPTED` and first recovery use independently reorderable lanes
  without an ordered-send barrier;
- canceling old A loses physical queue bytes, a held fault slot, task/join
  ownership, or changes logical reducer replay ownership;
- hard failure, local ACK stall, and owner hint can allocate more than one
  switch epoch, nonce, attach, or generation;
- standby registration retry is not idempotent on the same exact seal, or a
  conflicting registration silently replaces the bounded standby;
- probe, hint, ATTACH, acceptance, ACK, RESET, or terminal control can starve
  behind bulk DATA/replay; or
- any closed foundation invariant regresses, including checked underflow,
  category-owned work capacity, exact transient-input return, post-mutation
  ownership quarantine, flow-local Target failures, bounded continuation, or
  exact expiry cleanup.

An expected R6/R7 RED authorizes only its corresponding minimum P/A/L/C/H
implementation. An unexpected repair or regression failure requires causal
analysis and a written modification plan before changing the failed
invariant. No timeout, queue, replay, D16, Endpoint, MTU, pool, QUIC, Cubic,
GSO, or workload constant may be adjusted merely to turn a RED green.

A Task-4 PASS authorizes Task 5 only. It does not authorize Task 6 production
deployment, Task 7 throughput claims, macOS TUN, VPS, or M3.

## Architecture score

The currently implemented system remains **8/10** for the Knife16 target.
R1-R5 now provide a reviewed codec/seal/supervisor/Target byte path, exact
attach and catch-up, typed replay ownership, a bounded deterministic wire
substrate, category-owned work budgets, and one complete production-shared
baseline. The score does not rise because there is still no accepted
production-shared controller exercise of standby registration, genuine
liveness, ordered acceptance-before-recovery, switch arbitration, stale-A
retirement, and replay across two byte-level legs.

Task 4 reaches **9/10** only if every gate in this document passes through the
same supervisor, `LegIo`, `TargetIo`, attach transaction, and ownership code
intended for later production adapters, and R6-R9 pass without a harness-only
switch path. The document itself does not raise the score. Reaching **10/10**
still requires Task 5 UDP behavior, Task 6 real
single-owner process, Task 7 `>170 Mbit/s` real-socket capacity, Task 8
security/abuse closure, Task 9 independent-path WAN qualification, and Task 10
long macOS acceptance.
